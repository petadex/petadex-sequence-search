#!/usr/bin/env python3
"""
Live check of the export action against the deployed functions.

For each query: run a normal search pinned to VERSION, then an export of the
same query, wait for both, and check the export file against the search:
  - the summary is complete and its total equals the rows in hits.tsv.gz
  - rows are ranked (bitscore desc, evalue asc, id)
  - every search hit is in the export with the same bitscore, and the export's
    top N is at least as strong rank by rank (DIAMOND's -k is a heuristic, so
    the IDs need not be identical)
  - every hit is a v260701 petadex_id (no cross-version leak)
  - hits.fasta.gz (streamed, never held in memory) has one record per TSV row
    in the same order, each sequence as long as its `slen`, and the search's
    own target sequences for the hits both returned
  - a second export call returns the cached copy as "ready"
Time and cost come from each job's own timing.json: Lambda bills memory x
duration, so cost = sum(worker total_ms) x worker GB + aggregator ms x
aggregator GB, at the arm64 price. (Excludes cold-start init and requests,
both small.)

Usage: AWS_PROFILE=default validate_export.py VERSION OUT.tsv NAME=SEQ [NAME=SEQ ...]
       (a bare NAME takes the sequence from data/example-sequences.txt)
"""

import gzip
import json
import re
import sys
import time
import uuid
from pathlib import Path

import boto3

BUCKET = "petadex"
ORCH = "petadex-diamond-orchestrator"
PRICE_GB_S = 0.0000133334  # Lambda arm64, first pricing tier (us-east-1)
EXAMPLES = Path(__file__).resolve().parents[2] / "data" / "example-sequences.txt"
PETADEX_ID = re.compile(r"^(PL|NR|LG)\d+$")

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
MEM_GB = {f: lam.get_function_configuration(FunctionName=f"petadex-diamond-{f}")["MemorySize"] / 1024
          for f in ("worker", "aggregator")}


def examples():
    recs, name = {}, None
    for line in EXAMPLES.read_text().splitlines():
        if line.startswith(">"):
            name = line[1:].strip()
            recs[name] = ""
        elif name:
            recs[name] += line.strip()
    return recs


def invoke(body):
    out = json.loads(lam.invoke(FunctionName=ORCH, Payload=json.dumps(body).encode())["Payload"].read())
    return json.loads(out["body"]) if isinstance(out.get("body"), str) else out


def get(key):
    try:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None


def wait(session, job, timeout=1200):
    t0 = time.time()
    while time.time() - t0 < timeout:
        doc = get(f"results/{session}/{job}.json")
        if doc is not None:
            return json.loads(doc), time.time() - t0
        timing = get(f"results/{session}/{job}/timing.json")
        if timing and json.loads(timing).get("status") == "failed":
            raise RuntimeError(f"job failed: {session}/{job}")
        time.sleep(5)
    raise TimeoutError(f"{session}/{job}")


def check_fasta(key, data, search_hits):
    """Stream the export FASTA from S3 and check it against the TSV rows."""
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
    want = {h["target_id"]: h["target_sequence"] for h in search_hits if h.get("target_sequence")}
    n = ok_order = ok_len = ok_head = ok_seq = 0
    header = None
    with gzip.GzipFile(fileobj=body) as f:
        for raw in f:
            line = raw.decode().rstrip("\n")
            if line.startswith(">"):
                header = line
                continue
            if n < len(data):
                d = data[n]
                hid, *attrs = header[1:].split(" ")
                ok_order += hid == d[1]
                ok_len += len(line) == int(d[9])
                ok_head += attrs[:3] == [f"rank={d[0]}", f"evalue={d[10]}", f"bitscore={d[11]}"]
                ok_seq += hid not in want or want[hid] == line
            n += 1
    rows = len(data)
    return {"fasta records == rows": n == rows, "fasta same order": ok_order == rows,
            "fasta lengths == slen": ok_len == rows, "fasta headers": ok_head == rows,
            "fasta seqs == search target_sequence": ok_seq == rows}, n


def cost(session, job):
    """(total_wall_s, worker GB-s, aggregator GB-s, dollars) from timing.json.
    The aggregator writes timing.json just AFTER the result, so wait for it."""
    for _ in range(60):
        raw = get(f"results/{session}/{job}/timing.json")
        if raw is not None:
            break
        time.sleep(2)
    t = json.loads(raw)
    worker_s = sum(s.get("total_ms") or 0 for s in t["shards"]) / 1000
    agg_s = (t.get("aggregator") or {}).get("total_ms", 0) / 1000
    gbs_w, gbs_a = worker_s * MEM_GB["worker"], agg_s * MEM_GB["aggregator"]
    return t["total_wall_ms"] / 1000, gbs_w, gbs_a, (gbs_w + gbs_a) * PRICE_GB_S, t


def main():
    version, out = sys.argv[1], Path(sys.argv[2])
    ex = examples()
    queries = [(a.split("=", 1)[0], a.split("=", 1)[1]) if "=" in a else (a, ex[a]) for a in sys.argv[3:]]
    rows, ok = [], True
    for name, seq in queries:
        sid = f"validate-export-{name}-{uuid.uuid4().hex[:8]}"
        body = {"sessionId": sid, "sequence": f">{name}\n{seq}", "version": version}

        r = invoke({**body, "action": "search"})
        search, _ = wait(sid, r["job_id"])
        s_wall, s_w, s_a, s_usd, _ = cost(sid, r["job_id"])

        t0 = time.time()
        r = invoke({**body, "action": "export"})
        assert r.get("status") == "running", r
        xsid = r["export_session_id"]
        summary, _ = wait(xsid, r["job_id"])
        x_e2e = time.time() - t0
        x_wall, x_w, x_a, x_usd, xt = cost(xsid, r["job_id"])

        t0 = time.time()
        again = invoke({**body, "action": "export"})
        cache_s = time.time() - t0

        raw = get(summary["files"]["hits_tsv_gz"]["key"])
        lines = gzip.decompress(raw).decode().splitlines()
        header = [l for l in lines if l.startswith("#")]
        data = [l.split("\t") for l in lines[len(header) + 1:]]
        keys = [(-float(d[11]), float(d[10]), d[1]) for d in data]
        exp = {d[1]: float(d[11]) for d in data}
        hits = search["results"]
        checks = {
            "summary complete": summary.get("status") == "complete",
            "total == rows": summary["total_hits"] == len(data),
            "ranked": keys == sorted(keys),
            "search hits in export, same score": all(abs(exp.get(h["target_id"], -1) - h["bitscore"]) < 1e-6 for h in hits),
            "export top N >= search": all(float(d[11]) >= h["bitscore"] for d, h in zip(data, hits)),
            "all petadex_ids": all(PETADEX_ID.match(d[1]) for d in data),
            "second call is cached": again.get("status") == "ready" and again.get("job_id") == r["job_id"]
                                     and again.get("fasta_key") == summary["files"]["hits_fasta_gz"]["key"],
        }
        fasta_checks, _ = check_fasta(summary["files"]["hits_fasta_gz"]["key"], data, hits)
        checks.update(fasta_checks)
        recovered = len({d[1] for d in data[:len(hits)]} - {h["target_id"] for h in hits})
        slow = max(xt["shards"], key=lambda s: s.get("total_ms") or 0)
        row = {
            "query": name, "search_hits": len(hits), "export_hits": summary["total_hits"],
            "file_mb": round(len(raw) / 1e6, 2),
            "fasta_mb": round(summary["files"]["hits_fasta_gz"]["bytes"] / 1e6, 1),
            "recovered_top_n": recovered,
            "search_wall_s": round(s_wall, 1), "search_usd": round(s_usd, 4),
            "export_e2e_s": round(x_e2e, 1), "export_wall_s": round(x_wall, 1),
            "export_slowest_shard_s": round((slow.get("total_ms") or 0) / 1000, 1),
            "export_slowest_search_s": round((slow.get("search_ms") or 0) / 1000, 1),
            "export_merge_s": round((xt.get("aggregator") or {}).get("merge_ms", 0) / 1000, 1),
            "export_agg_s": round((xt.get("aggregator") or {}).get("total_ms", 0) / 1000, 1),
            "export_usd": round(x_usd, 4), "export_vs_search_cost": round(x_usd / s_usd, 2),
            "cache_hit_s": round(cache_s, 2),
            "by_evalue": json.dumps(summary["by_evalue"]),
            "failed_checks": ",".join(k for k, v in checks.items() if not v),
        }
        rows.append(row)
        ok &= all(checks.values())
        print(json.dumps(row), flush=True)

    with open(out, "w") as f:
        f.write("\t".join(rows[0]) + "\n")
        for row in rows:
            f.write("\t".join(str(v) for v in row.values()) + "\n")
    print("EXPORT VALIDATION " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
