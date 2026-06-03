#!/usr/bin/env python3
"""
PETadex DIAMOND Worker Lambda — Phase 2 of the scale-out plan.

One worker searches ONE shard of the sharded DIAMOND database and writes its
partial result to S3. The orchestrator (Phase 3) fans out one worker per shard
via Step Functions Map; the aggregator (Phase 4) merges the parts.

This handler is deliberately self-contained — it does NOT import
`lambda_function`, because that module eager-downloads the 3.2 GB MMseqs2 DB at
import time (for the legacy single-Lambda path). A worker never touches MMseqs2.
One container image serves both roles; the worker Lambda just binds its CMD to
`worker.handler` instead of `lambda_function.handler`.

Input event (query pre-validated and version pinned by the orchestrator):
    {
        "sessionId":  "...",
        "jobId":      "...",
        "shardIndex": 0,                                  # for part naming + logs
        "shardKey":   "diamond/{version}/shard_00.dmnd",  # exact S3 key (manifest)
        "queryFasta": ">query\\nMKLL...",                 # already validated
        "maxResults": 50
    }

`shardKey` is the authoritative locator (copied straight from the manifest's
per-shard `key`), so the worker never has to reconstruct the zero-pad width that
the build script chose. `version` + `shardIndex` are accepted as a fallback for
manual testing.

Output:
    s3://petadex/results/{sessionId}/{jobId}/parts/shard_{shardIndex}.tsv
    Raw DIAMOND outfmt-6 TSV (the aggregator parses/sorts/enriches). An empty
    file is still written when a shard has no hits — a completed-with-no-hits
    shard must be distinguishable from a shard that never ran (fail-fast).

pident NOTE (silent-corruption guard, Phase 0 Check 8): DIAMOND's `pident`
column is already a percentage in [0,100]. The legacy MMseqs2 code multiplies
`fident` (0–1) by 100; that ×100 must NOT be applied to DIAMOND output. The
worker emits the raw column untouched; the assertion lives in the merger.
"""

import glob
import json
import os
import subprocess
import time
from datetime import datetime, timezone

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig

S3_BUCKET = "petadex"

# Locked Phase 0 parameters (see CLAUDE.md settled-parameter table).
SENSITIVITY = os.environ.get("DIAMOND_SENSITIVITY", "--very-sensitive")
BLOCK_SIZE = os.environ.get("DIAMOND_BLOCK_SIZE", "1")   # -b; Check 5: 0.5–1
# Lambda allocates ~1 vCPU per 1769 MB; os.cpu_count() reflects that.
THREADS = int(os.environ.get("DIAMOND_THREADS", os.cpu_count() or 2))

# DIAMOND outfmt-6 columns, in the exact order the aggregator expects. Mirrors
# the legacy MMseqs2 field order (target, qstart, qend, tstart, tend, alnlen,
# ident, evalue, bits) so the merge logic stays uniform — except `pident` here
# is already 0–100 (see module docstring).
OUTFMT = ["6", "sseqid", "qstart", "qend", "sstart", "send",
          "length", "pident", "evalue", "bitscore"]

# Mirror the multipart tuning the legacy download path uses.
S3_DOWNLOAD_CONCURRENCY = 32
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=32 * 1024 * 1024,
    max_concurrency=S3_DOWNLOAD_CONCURRENCY,
    use_threads=True,
)

# The connection pool must be >= the transfer's max_concurrency, otherwise the
# surplus download threads thrash a too-small pool ("Connection pool is full,
# discarding connection") and throughput collapses (~74 MB/s seen vs ~356 MB/s).
s3 = boto3.client(
    "s3",
    region_name="us-east-1",
    config=BotoConfig(max_pool_connections=S3_DOWNLOAD_CONCURRENCY),
)


def resolve_shard_key(event):
    """Return the exact S3 key of the shard .dmnd for this worker."""
    key = event.get("shardKey")
    if key:
        return key
    # Fallback for manual testing: reconstruct from version + index. Uses the
    # build script's default zero-pad (width = digits in shard_count-1). When
    # shard_count is unknown we cannot know the width, so require shardKey in
    # production; this branch assumes the common 20-shard / width-2 layout.
    version = event["version"]
    idx = int(event["shardIndex"])
    return f"diamond/{version}/shard_{idx:02d}.dmnd"


def download_shard(shard_key):
    """Download the shard .dmnd to /tmp, cached across warm invocations.

    Version-pinned (not LATEST-following): the key already names a frozen
    version, so a cached file is always valid for that key. DIAMOND adds the
    `.dmnd` suffix itself, so the local db path passed to `-d` omits it.
    """
    basename = os.path.basename(shard_key)                # shard_00.dmnd
    local_path = f"/tmp/{basename}"
    db_path = local_path[:-len(".dmnd")]                  # /tmp/shard_00

    # A shard .dmnd is ~6 GB and /tmp is 10 GB, so only ONE fits at a time.
    # Warm-container routing is NOT shard-affine — Lambda may hand this warm
    # container a different shard than its last invocation — so a naive
    # "cache across warm invocations" leaves the previous 6 GB shard on disk
    # and the next download overflows /tmp with [Errno 28] No space left on
    # device. Evict every other cached shard before proceeding; the wanted
    # shard (if present) is kept, so same-shard reuse is still a cache hit.
    for other in glob.glob("/tmp/shard_*.dmnd"):
        if other != local_path:
            print(f"evicting stale cached shard: {other}")
            os.remove(other)

    if os.path.exists(local_path):
        print(f"shard cached: {local_path}")
        return db_path

    print(f"downloading s3://{S3_BUCKET}/{shard_key} -> {local_path}")
    t0 = time.time()
    s3.download_file(S3_BUCKET, shard_key, local_path, Config=S3_TRANSFER_CONFIG)
    print(f"TIMING shard_download: {time.time() - t0:.2f}s "
          f"({os.path.getsize(local_path) / 1024 / 1024:.0f} MB)")
    return db_path


def run_shard_search(query_fasta, db_path, max_results):
    """Run `diamond blastp` for this shard; return the raw outfmt-6 TSV text."""
    query_file = "/tmp/query.fasta"
    with open(query_file, "w") as f:
        f.write(query_fasta if query_fasta.endswith("\n") else query_fasta + "\n")
    out_file = "/tmp/part.tsv"
    # Remove any stale part from a previous warm invocation.
    if os.path.exists(out_file):
        os.remove(out_file)

    cmd = [
        "diamond", "blastp",
        "-q", query_file,
        "-d", db_path,
        "-o", out_file,
        "-b", str(BLOCK_SIZE),
        "-k", str(max_results),
        "--threads", str(THREADS),
        SENSITIVITY,
        "--outfmt", *OUTFMT,
    ]
    print(f"$ {' '.join(cmd)}")
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    print(f"TIMING diamond_blastp: {time.time() - t0:.2f}s")
    if proc.returncode != 0:
        print(f"DIAMOND stderr:\n{proc.stderr}")
        raise RuntimeError(f"diamond blastp failed (rc={proc.returncode})")

    # DIAMOND writes nothing when there are no hits; treat that as an empty
    # (but completed) result rather than an error.
    if os.path.exists(out_file):
        with open(out_file) as f:
            return f.read()
    return ""


def write_shard_timing(session_id, job_id, shard_index, timings, status, error=None):
    """Persist this worker's timing as a standalone sidecar object.

    Written for EVERY shard — success or failure — from the handler's `finally`
    block, so a job that later aborts under fail-fast still leaves a per-shard
    breadcrumb the aggregator can roll up. Telemetry must never be the thing that
    fails a search, so this logs and swallows any error rather than raising: a
    failed timing write degrades to "no timing for this shard," not a crash.

    Sidecar key (beside the result part, under parts/):
        results/{sessionId}/{jobId}/parts/shard_{shardIndex}.meta.json
    """
    try:
        doc = {
            "shard_index": shard_index,
            "download_ms": timings.get("download_ms"),
            "search_ms": timings.get("search_ms"),
            "total_ms": timings.get("total_ms"),
            # Dual-purpose telemetry (Phase 7): size + seq count let real traffic
            # feed the diminishing-returns / shard-count benchmark, not just ops.
            "shard_size_bytes": timings.get("shard_size_bytes"),
            "shard_seq_count": timings.get("shard_seq_count"),
            "num_hits": timings.get("num_hits"),
            "status": status,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        key = f"results/{session_id}/{job_id}/parts/shard_{shard_index}.meta.json"
        s3.put_object(Bucket=S3_BUCKET, Key=key,
                      Body=json.dumps(doc).encode(),
                      ContentType="application/json")
        print(f"wrote timing sidecar -> s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"WARN write_shard_timing failed (shard {shard_index}): {e}")


def handler(event, context):
    """Search one shard and write its partial TSV to S3. Fail-fast: any error
    raises so the Step Functions Map branch fails the whole job."""
    print(f"Worker event: sessionId={event.get('sessionId')} "
          f"jobId={event.get('jobId')} shardIndex={event.get('shardIndex')}")

    session_id = event["sessionId"]
    job_id = event["jobId"]
    shard_index = int(event["shardIndex"])
    query_fasta = event["queryFasta"]
    max_results = int(event.get("maxResults", 50))

    # Per-shard timing, captured with monotonic() and emitted as a sidecar in the
    # `finally` below so it fires whether the search succeeds or throws (a failed
    # shard still leaves a breadcrumb under fail-fast). `shardSeqs` is threaded
    # from the manifest by the orchestrator/Map for the shard-count benchmark.
    timings = {"shard_seq_count": event.get("shardSeqs")}
    status = "success"
    error = None
    job_t0 = time.monotonic()
    try:
        shard_key = resolve_shard_key(event)

        d0 = time.monotonic()
        db_path = download_shard(shard_key)
        timings["download_ms"] = round((time.monotonic() - d0) * 1000, 1)
        local_path = db_path + ".dmnd"
        if os.path.exists(local_path):
            timings["shard_size_bytes"] = os.path.getsize(local_path)

        s0 = time.monotonic()
        tsv = run_shard_search(query_fasta, db_path, max_results)
        timings["search_ms"] = round((time.monotonic() - s0) * 1000, 1)

        n_hits = tsv.count("\n") if tsv else 0
        timings["num_hits"] = n_hits
        part_key = f"results/{session_id}/{job_id}/parts/shard_{shard_index}.tsv"
        s3.put_object(Bucket=S3_BUCKET, Key=part_key, Body=tsv.encode(),
                      ContentType="text/tab-separated-values")
        print(f"wrote {n_hits} hits -> s3://{S3_BUCKET}/{part_key}")

        return {
            "sessionId": session_id,
            "jobId": job_id,
            "shardIndex": shard_index,
            "partKey": part_key,
            "numHits": n_hits,
        }
    except Exception as e:
        status = "failed"
        error = str(e)
        raise  # fail-fast: propagate so the Map branch fails the whole job
    finally:
        timings["total_ms"] = round((time.monotonic() - job_t0) * 1000, 1)
        write_shard_timing(session_id, job_id, shard_index, timings, status, error)


if __name__ == "__main__":
    # Local test: python worker.py '<event json>'
    import json
    import sys
    evt = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    print(json.dumps(handler(evt, None), indent=2))
