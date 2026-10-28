"""Alignment + tree for a finished search (the results page's Alignment tab).

The aggregator starts this Lambda in the background right after it writes a
search result (`key = "default"`), so the default tree is usually ready by the
time anyone asks; the orchestrator's `align` action reads its status, and
starts a custom one for an explicit `ids` list. A search never waits on it and
never fails because of it.

Sequences come from the result itself (`query_sequence` + every hit's
`target_sequence`), so nothing is read from the corpus:

    default   the query, every Plasticome hit, and the best-ranked hit of each
              90 % cluster (`metadata.c90`), in rank order, capped at MAX_HITS
    ids       an explicit list (<= MAX_HITS) of hits from that result

FAMSA aligns, FastTree (-lg -gamma) builds the tree. Outputs, next to nothing
else so `history` never sees them:

    alignments/{sessionId}/{jobId}/{key}.json  status: running | ready | failed
    alignments/{sessionId}/{jobId}/{key}.afa   aligned FASTA, headers = petadex_id, query = ">query"
    alignments/{sessionId}/{jobId}/{key}.nwk   Newick, same leaf names, branch lengths kept

ISPETase (125 sequences, 660 columns, 2 threads on watson): FAMSA 1.9 s,
FastTree 6.9 s; ~85 KB .afa, ~5 KB .nwk.
"""
import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "petadex")
FAMSA_BIN = os.environ.get("FAMSA_BIN", "famsa")
FASTTREE_BIN = os.environ.get("FASTTREE_BIN", "FastTree")
MAX_HITS = 200
STALE_RUNNING_S = 300   # a "running" status older than this is treated as failed (the Lambda died)
TOOL_TIMEOUT_S = 240
MAX_THREADS = int(os.environ.get("ALIGN_THREADS", "4"))   # Lambda reports its 2-6 vCPUs; a big host must not spawn hundreds
s3 = boto3.client("s3", region_name="us-east-1")


def key_for(ids):
    """`default` for the default selection, else a stable hash of the id list (order-free)."""
    if ids is None:
        return "default"
    return "ids-" + hashlib.sha256("\n".join(sorted(set(ids))).encode()).hexdigest()[:16]


def validate_ids(ids):
    """None, or a non-empty list of <= MAX_HITS distinct strings; raises ValueError otherwise."""
    if ids is None:
        return None
    if not isinstance(ids, list) or not ids or not all(isinstance(i, str) and i for i in ids):
        raise ValueError("ids must be a non-empty list of petadex_ids")
    if len(set(ids)) > MAX_HITS:
        raise ValueError(f"at most {MAX_HITS} ids")
    return ids


def select(result, ids=None):
    """[(name, sequence)] for the alignment: the query first, then hits in rank order."""
    hits = [h for h in result["results"] if h.get("target_sequence")]
    if ids is None:
        seen, pick = set(), []
        for h in hits:
            c90 = (h.get("metadata") or {}).get("c90")
            if h["target_id"].startswith("PL") or c90 is None or c90 not in seen:
                pick.append(h)
            if c90 is not None:
                seen.add(c90)
        pick = pick[:MAX_HITS]
    else:
        by_id = {h["target_id"]: h for h in hits}
        missing = sorted(set(ids) - set(by_id))
        if missing:
            raise ValueError(f"not hits of this result: {', '.join(missing[:5])}" + (" …" if len(missing) > 5 else ""))
        want = set(ids)
        pick = [h for h in hits if h["target_id"] in want]
    return [("query", result["query_sequence"])] + [(h["target_id"], h["target_sequence"]) for h in pick]


def build(seqs, threads=None):
    """FAMSA then FastTree on [(name, seq)] -> (afa text, newick text, timing)."""
    threads = threads or min(os.cpu_count() or 1, MAX_THREADS)
    timing = {}
    with tempfile.TemporaryDirectory() as d:
        src, afa = os.path.join(d, "in.fa"), os.path.join(d, "out.afa")
        with open(src, "w") as f:
            f.writelines(f">{n}\n{s}\n" for n, s in seqs)
        t0 = time.monotonic()
        subprocess.run([FAMSA_BIN, "-t", str(threads), src, afa], check=True, capture_output=True, timeout=TOOL_TIMEOUT_S)
        timing["famsa_ms"] = round((time.monotonic() - t0) * 1000, 1)
        t0 = time.monotonic()
        tree = subprocess.run([FASTTREE_BIN, "-lg", "-gamma", "-quiet", afa], check=True, capture_output=True,
                              text=True, timeout=TOOL_TIMEOUT_S,
                              env={**os.environ, "OMP_NUM_THREADS": str(threads)}).stdout.strip()
        timing["fasttree_ms"] = round((time.monotonic() - t0) * 1000, 1)
        with open(afa) as f:
            afa_text = f.read()
    if not tree.endswith(";"):
        raise RuntimeError("FastTree returned no tree")
    return afa_text, tree + "\n", timing


def prefix(session_id, job_id):
    return f"alignments/{session_id}/{job_id}"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _put_status(session_id, job_id, key, doc):
    s3.put_object(Bucket=S3_BUCKET, Key=f"{prefix(session_id, job_id)}/{key}.json",
                  Body=json.dumps(doc), ContentType="application/json")


def read_status(session_id, job_id, key):
    """The stored status doc, or None. A `running` doc older than STALE_RUNNING_S reads as failed."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix(session_id, job_id)}/{key}.json")
    except s3.exceptions.NoSuchKey:
        return None
    doc = json.loads(obj["Body"].read())
    if doc.get("status") == "running":
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(doc["started_at"])).total_seconds()
        if age > STALE_RUNNING_S:
            doc.update(status="failed", error="timed out")
    return doc


def read_outputs(session_id, job_id, key):
    """(afa, newick) text of a ready alignment."""
    p = prefix(session_id, job_id)
    get = lambda ext: s3.get_object(Bucket=S3_BUCKET, Key=f"{p}/{key}.{ext}")["Body"].read().decode()
    return get("afa"), get("nwk")


def run(session_id, job_id, ids=None):
    """Build and store one alignment + tree; returns the final status doc. Never raises."""
    key = key_for(ids)
    base = {"session_id": session_id, "job_id": job_id, "key": key, "started_at": _now(),
            "aligner": "famsa", "tree": "fasttree -lg -gamma"}
    _put_status(session_id, job_id, key, {**base, "status": "running"})
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"results/{session_id}/{job_id}.json")
        result = json.loads(obj["Body"].read())
        seqs = select(result, ids)
        if len(seqs) < 3:
            raise ValueError("fewer than 3 sequences to align")
        afa, nwk, timing = build(seqs)
        p = prefix(session_id, job_id)
        s3.put_object(Bucket=S3_BUCKET, Key=f"{p}/{key}.afa", Body=afa, ContentType="text/plain")
        s3.put_object(Bucket=S3_BUCKET, Key=f"{p}/{key}.nwk", Body=nwk, ContentType="text/plain")
        doc = {**base, "status": "ready", "finished_at": _now(), "n_sequences": len(seqs),
               "ids": [n for n, _ in seqs[1:]], "n_columns": len(afa.split(">")[1].split("\n", 1)[1].replace("\n", "")),
               "database_version": result.get("database_version"), "search_version": result.get("search_version"),
               "timing": timing}
    except Exception as e:   # recorded for the poller, never raised to Lambda (no retries of a bad input)
        print(f"WARN align failed for {session_id}/{job_id}/{key}: {type(e).__name__}: {e}")
        doc = {**base, "status": "failed", "finished_at": _now(), "error": f"{type(e).__name__}: {e}"[:300]}
    _put_status(session_id, job_id, key, doc)
    return doc


def handler(event, context):
    """Lambda entry: {sessionId, jobId, ids?} (async invoke from the aggregator or the orchestrator)."""
    return run(event["sessionId"], event["jobId"], event.get("ids"))
