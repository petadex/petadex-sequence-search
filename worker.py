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
import math
import os
import subprocess
import time
from datetime import datetime, timezone

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig

S3_BUCKET = "petadex"

# Phase 0 parameters (see CLAUDE.md settled-parameter table). An empty value or
# "default"/"none" omits the sensitivity flag entirely → DIAMOND's default (fast)
# mode, which is distinct from (and slower than) --fast.
SENSITIVITY = os.environ.get("DIAMOND_SENSITIVITY", "--very-sensitive").strip()
# -b reference-block size (Check 5: 0.5–1 for the index-bearing .dmnd path).
# ⚠️ For the zstd/FASTA-as-DB path, -b must be large enough that the WHOLE shard
# is a SINGLE reference block: with --dbsize the count is skipped (sequences: 0),
# so multi-block runs emit per-block top-k with no global merge and OVER-REPORT
# (doc 06 / CLAUDE.md §9.1). The build pipeline shards finely (~1 G letters) so
# that -b1 already yields one block at ~1.9 GB RSS — keep shard letters <= one
# block at this -b, or raise -b (which raises RSS, possibly onto the 10 GB tier).
BLOCK_SIZE = os.environ.get("DIAMOND_BLOCK_SIZE", "1")
# Lambda allocates ~1 vCPU per 1769 MB; os.cpu_count() reflects that.
THREADS = int(os.environ.get("DIAMOND_THREADS", os.cpu_count() or 2))
# Search-flag overrides for the Search Optimization 07 benchmarks (Benjamin
# Buchfink's suggestions). Both default to empty → flag omitted → DIAMOND's own
# default (= current production behavior), so this is a no-op until the worker
# env sets them.
#   DIAMOND_CHUNKS=1   → `-c1`        (S1: one index chunk, single reference pass)
#   DIAMOND_MASKING=0  → `--masking 0` (S2: skip per-call repeat masking)
INDEX_CHUNKS = os.environ.get("DIAMOND_CHUNKS", "").strip()
MASKING = os.environ.get("DIAMOND_MASKING", "").strip()
# DB-format streaming benchmark ("06"): the worker reads either the native
# `.dmnd` (default / production) or a compressed-FASTA DB (`.fa.zst`/`.fa.gz`),
# chosen purely by the shard key's extension — so a production switch is just a
# manifest key change and a benchmark is a direct invoke with the other key.
# A FASTA-as-DB has no prebuilt seed index, so it MUST run as a single reference
# block or DIAMOND emits per-block top-k without a global merge and over-reports
# hits (root-caused in doc 06). The single block's `-b` must be >= the shard's
# residue count in billions, and `-b` drives peak RSS — so a 20-shard zstd shard
# (~5.25 Gletters -> -b6) OOM-kills the 10 GiB Lambda tier, while a 32-shard
# shard (~3.28 Gletters -> -b4) fits (db-format memory note, 2026-06-09). The
# worker therefore sizes `-b` to the actual shard via `fasta_block_size()` from
# the per-shard `letters` threaded in the event (`shardLetters`); this env is
# only the fallback when that figure is absent (e.g. a pre-letters manifest).
FASTA_BLOCK_SIZE = os.environ.get("DIAMOND_FASTA_BLOCK_SIZE", "6")
# DEV-BUILD ONLY override. When set (>0), forces this exact `-b` on the FASTA-as-DB
# path, bypassing the single-block `fasta_block_size()` sizing. Exists solely to
# exercise Benjamin's cross-block-merge fix (diamond_dev `dev`@4b2ae056) at `-b1`
# on Lambda — the fix makes multi-block top-k merge correctly, so the single-block
# requirement no longer applies and `-b1` (~2 GB RSS) replaces the OOM-prone `-b4`.
# ⚠️ MUST stay UNSET on the production worker: the prod 2.2.1 build OVER-REPORTS at
# `-b1` (no merge → per-block top-k; doc 06/08). Default "" → prod behavior unchanged.
# See doc "08 Compressed-FASTA Merge Dev Build Validation" Stage 2.
FASTA_BLOCK_OVERRIDE = os.environ.get("DIAMOND_FASTA_BLOCK_OVERRIDE", "").strip()
_FASTA_SUFFIXES = (".fa.zst", ".fa.gz", ".fasta.gz", ".fasta.zst", ".fa", ".fasta")
# Effective reference-DB size (residues) for e-value calibration. Normally
# threaded from the manifest via the event (`dbSize`); this env is a manual
# override fallback. See docs/evalue-calibration.md.
DBSIZE_ENV = os.environ.get("DIAMOND_DBSIZE")

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


# Manual-test fallback only: when no shardKey is supplied, reconstruct one from
# version + index. Production ALWAYS passes the authoritative shardKey from the
# manifest, so the pad width/suffix here never bind in production — they are
# env-overridable for finer-shard / zstd manual tests rather than hardcoded.
SHARD_PAD_WIDTH = int(os.environ.get("SHARD_PAD_WIDTH", "3"))
SHARD_SUFFIX = os.environ.get("SHARD_SUFFIX", ".dmnd")  # ".dmnd" | ".fasta.zst"


def resolve_shard_key(event):
    """Return the exact S3 key of this worker's shard artifact."""
    key = event.get("shardKey")
    if key:
        return key
    version = event["version"]
    idx = int(event["shardIndex"])
    return f"diamond/{version}/shard_{idx:0{SHARD_PAD_WIDTH}d}{SHARD_SUFFIX}"


def shard_db_path(shard_key, local_path):
    """The path to pass to `diamond -d` for a downloaded shard.

    `.dmnd`: DIAMOND appends the `.dmnd` suffix itself, so strip it.
    Compressed FASTA (`.fasta.zst`/`.fa.zst`/`.fa.gz`/`.fa`): DIAMOND reads the
    file directly as the DB (zstd needs the WITH_ZSTD=ON build), so pass it
    verbatim. See doc 06 / CLAUDE.md for the zstd-as-DB path.
    """
    if shard_key.endswith(".dmnd"):
        return local_path[:-len(".dmnd")]
    return local_path


def is_fasta_db(path):
    """True if `path` is a (optionally compressed) FASTA used directly as the DB,
    vs a native `.dmnd`. Drives the `-d` arg form and the single-block requirement."""
    return path.endswith(_FASTA_SUFFIXES)


def fasta_block_size(shard_letters):
    """`-b` for a FASTA-as-DB: the smallest value that keeps it a SINGLE reference
    block, sized to this shard.

    A FASTA-as-DB has no prebuilt seed index, so it must run as one block (else
    DIAMOND over-reports per-block top-k). `-b` is measured in *billions of
    letters*, so the minimal single block is `ceil(shard_letters / 1e9)`. Sizing
    it to the shard — rather than a fixed `-b6` — is what lets finer shards fit
    Lambda's 10 GiB tier: 20 shards (~5.25 Gletters -> -b6) OOMs, 32 shards
    (~3.28 Gletters -> -b4) fits. Falls back to DIAMOND_FASTA_BLOCK_SIZE when the
    letter count is unknown (a manifest/event without per-shard `letters`).

    DEV-BUILD escape hatch: DIAMOND_FASTA_BLOCK_OVERRIDE (when >0) wins outright —
    forces that `-b` regardless of shard letters, so the dev build can be tested at
    `-b1`. Unset in production, so the single-block sizing below is unchanged."""
    if FASTA_BLOCK_OVERRIDE:
        try:
            if int(FASTA_BLOCK_OVERRIDE) > 0:
                return FASTA_BLOCK_OVERRIDE
        except (TypeError, ValueError):
            pass
    try:
        n = int(shard_letters)
    except (TypeError, ValueError):
        n = 0
    if n > 0:
        return str(max(1, math.ceil(n / 1_000_000_000)))
    return FASTA_BLOCK_SIZE


def download_shard(shard_key):
    """Download the shard DB to /tmp, cached across warm invocations. Returns the
    local file path (full filename, including extension).

    Version-pinned (not LATEST-following): the key already names a frozen
    version, so a cached file is always valid for that key. Works for any DB
    format (`.dmnd` or `.fa.zst`/`.fa.gz`); run_shard_search derives the DIAMOND
    `-d` argument from the returned path.
    """
    basename = os.path.basename(shard_key)                # shard_00.dmnd / .fa.zst
    local_path = f"/tmp/{basename}"

    # A shard is ~1–6 GB and /tmp is 10 GB, so only ONE fits at a time. Warm-
    # container routing is NOT shard-affine — Lambda may hand this warm container
    # a different shard (or format) than its last invocation — so a naive "cache
    # across warm invocations" leaves the previous multi-GB shard on disk and the
    # next download overflows /tmp with [Errno 28]. Evict every OTHER cached
    # shard (any format) before proceeding; the wanted file (if present) is kept,
    # so same-shard reuse is still a cache hit.
    for other in glob.glob("/tmp/shard_*"):
        if other != local_path and os.path.isfile(other):
            print(f"evicting stale cached shard: {other}")
            os.remove(other)

    if os.path.exists(local_path):
        print(f"shard cached: {local_path}")
        return local_path

    print(f"downloading s3://{S3_BUCKET}/{shard_key} -> {local_path}")
    t0 = time.time()
    s3.download_file(S3_BUCKET, shard_key, local_path, Config=S3_TRANSFER_CONFIG)
    print(f"TIMING shard_download: {time.time() - t0:.2f}s "
          f"({os.path.getsize(local_path) / 1024 / 1024:.0f} MB)")
    return local_path


def run_shard_search(query_fasta, db_local_path, max_results, dbsize=None,
                     shard_letters=None):
    """Run `diamond blastp` for this shard; return the raw outfmt-6 TSV text.

    `db_local_path` is the downloaded file (full name). DIAMOND wants the bare
    basename for a native `.dmnd` (it appends `.dmnd` itself) but the FULL
    filename for a FASTA-as-DB, and the FASTA path must use a single reference
    block for a correct global top-k — sized to this shard via
    `fasta_block_size(shard_letters)` so finer shards fit memory (see is_fasta_db
    / doc 06).
    """
    query_file = "/tmp/query.fasta"
    with open(query_file, "w") as f:
        f.write(query_fasta if query_fasta.endswith("\n") else query_fasta + "\n")
    out_file = "/tmp/part.tsv"
    # Remove any stale part from a previous warm invocation.
    if os.path.exists(out_file):
        os.remove(out_file)

    if is_fasta_db(db_local_path):
        db_arg = db_local_path                       # FASTA: full filename
        block = fasta_block_size(shard_letters)       # single block, sized to shard
    else:
        db_arg = db_local_path[:-len(".dmnd")]       # native: bare basename
        block = str(BLOCK_SIZE)

    cmd = [
        "diamond", "blastp",
        "-q", query_file,
        "-d", db_arg,
        "-o", out_file,
        "-b", block,
        "-k", str(max_results),
        "--threads", str(THREADS),
    ]
    # Omit the flag for an empty/"default"/"none" value → DIAMOND default mode.
    if SENSITIVITY and SENSITIVITY.lower() not in ("default", "none"):
        cmd.append(SENSITIVITY)
    # S1/S2 search-flag overrides (empty ⇒ omit ⇒ DIAMOND default = prod today).
    if INDEX_CHUNKS:
        cmd += ["-c", INDEX_CHUNKS]
    if MASKING != "":
        cmd += ["--masking", MASKING]
    # Calibrate e-values against the FULL corpus, not this ~1/20 shard. Without
    # --dbsize, DIAMOND uses the shard's own residue count, so e-values come out
    # ~SHARD_COUNT× too significant (E ∝ database size). dbsize = total corpus
    # residues (manifest.total_letters), threaded from the orchestrator. Bit
    # scores are unaffected. See docs/evalue-calibration.md.
    if dbsize:
        try:
            n = int(dbsize)
        except (TypeError, ValueError):
            n = 0
        if n > 0:
            cmd += ["--dbsize", str(n)]
    cmd += ["--outfmt", *OUTFMT]
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
                      ContentType="application/json",
                      Tagging="reapable=true")  # reaped with the parts (see part_key)
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
    # Effective full-corpus DB size for e-value calibration (manifest-derived,
    # threaded via the Map). 0/None ⇒ omit --dbsize (legacy per-shard behavior).
    dbsize = event.get("dbSize") or DBSIZE_ENV
    # Per-shard residue count (manifest-derived, threaded via the Map). For a
    # FASTA-as-DB this sizes the single reference block so finer shards fit Lambda
    # memory; for a `.dmnd` it is unused. None ⇒ fall back to FASTA_BLOCK_SIZE env.
    shard_letters = event.get("shardLetters")

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
        db_path = download_shard(shard_key)          # full local file path
        timings["download_ms"] = round((time.monotonic() - d0) * 1000, 1)
        if os.path.exists(db_path):
            timings["shard_size_bytes"] = os.path.getsize(db_path)

        s0 = time.monotonic()
        tsv = run_shard_search(query_fasta, db_path, max_results, dbsize=dbsize,
                               shard_letters=shard_letters)
        timings["search_ms"] = round((time.monotonic() - s0) * 1000, 1)

        n_hits = tsv.count("\n") if tsv else 0
        timings["num_hits"] = n_hits
        part_key = f"results/{session_id}/{job_id}/parts/shard_{shard_index}.tsv"
        # reapable=true: parts are write-once-then-dead once {jobId}.json exists
        # (web app never re-reads them), so a tag-scoped lifecycle rule can expire
        # them without ever touching the authoritative result. Requires
        # s3:PutObjectTagging in the worker IAM (see infra/iam/worker-policy.json).
        s3.put_object(Bucket=S3_BUCKET, Key=part_key, Body=tsv.encode(),
                      ContentType="text/tab-separated-values",
                      Tagging="reapable=true")
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
