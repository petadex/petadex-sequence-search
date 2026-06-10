#!/usr/bin/env python3
"""
PETadex DIAMOND shard builder — Phase 1 of the scale-out plan.

Streams the full Logan corpus FASTA from S3, partitions it into SHARD_COUNT
shard FASTAs, runs `diamond makedb` on each to produce `.dmnd` shards, uploads
them to `s3://{bucket}/diamond/{version}/shard_{i}.dmnd`, writes a manifest, and
*last* bumps `s3://{bucket}/diamond/LATEST` (atomic versioning — workers that
read LATEST mid-build never see a half-written version).

Source of truth is the S3 corpus object, NOT the RDS `enzyme_fastaa` table:
that table holds only the ~1M nr subset and would defeat the scale-out (see
Scale-Out Architecture Plan §9.3). Each record keeps its native corpus header
as the sequence ID.

  s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa   307,155,746 seqs

=== WHERE TO RUN ===
This is NOT a laptop job. Producing ~144 GiB of `.dmnd` from a 116 GB corpus
needs an arm64 box with fast network and a large scratch volume. The Phase 0
spike used an r6g.2xlarge with EBS scratch. Disk budget on --scratch-dir:

  * shard FASTAs (one streaming pass) .......... ~116 GB  (deleted as built)
  * .dmnd shards (~144 GiB total) .............. peaks near the FASTA total
  -> provision ~300 GB scratch to be safe.

Partitioning (--partition):
  * round-robin (default): record r -> shard r % N. Evens out BOTH seq count
    and total residues across shards, so every worker's /tmp footprint and
    fan-out search time are ~uniform. Under the fail-fast Step Functions Map a
    job is gated by its slowest branch, so uniform shards tighten WORKER_TIMEOUT.
  * contiguous: record r -> shard r // shard_size. Simplest, but the corpus is
    not uniformly ordered (Phase 0: FASTA bytes/seq drifts 502 -> 460), so shard
    sizes vary. The head shard measured 6.58 GiB; still under the ~8-9 GB /tmp
    ceiling, but round-robin is preferred for the uniformity reason above.

The mmseqs2/ path and update_sequence_index.py are untouched — this writes only
under the diamond/ prefix.
"""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

DEFAULT_BUCKET = "petadex"
DEFAULT_KEY = "logan/petadex.catalytic_orfs.v1.1.fa"
CORPUS_COUNT = 307_155_746  # Phase 0 Check 0 ground truth
DIAMOND_PREFIX = "diamond"  # s3://{bucket}/diamond/...

# --- Sharding granularity for the zstd path ----------------------------------
# zstd-as-DB is correct only when each shard is a SINGLE DIAMOND reference block:
# with --dbsize the letter count is skipped (sequences: 0), so per-block top-k are
# NOT globally merged and a multi-block shard OVER-REPORTS (doc 06 / CLAUDE.md
# §9.1). So shard size is bounded by what fits ONE block in RAM:
#
#   block letters ≈ -b × 1e9;   peak RSS ≈ 1.9 GB @ -b1 ... 8.8 GB @ -b6 (measured)
#
# Crucially, the 10 GB Lambda tier is ALREADY in use for vCPUs (search is CPU-
# bound, §10.6), so minimizing RSS buys nothing — the only memory question is
# "does the single block fit under 10 GB?". It does even at the current 20 shards
# (5.25 G letters → -b6 → 8.8 GB). So MEMORY PERMITS AS FEW AS ~20 SHARDS; it does
# not push toward many. (/tmp is a non-issue for zstd: a shard is ~1 GB, streamed.)
#
# The TOP is capped by reserved concurrency, NOT memory: one search fans out one
# worker per shard, so shard_count must stay <= WORKER_RESERVED_CONCURRENCY /
# peak-concurrent-searches (§10.6: reserved=100 → 20 shards = 5 concurrent searches;
# 32 shards ≈ 3; >100 shards can't run even ONE search). Sharding finer only trades
# per-shard search latency for fan-out/concurrency budget (the §9.1 tradeoff).
#
# Reasonable window: ~20 (memory floor, -b6, tight RSS) to ~40 (comfortable RSS,
# 2–3 concurrent). Default 32 ≈ 3.3 G letters/shard → one block at -b4 (~6 GB RSS)
# with ~3 concurrent searches under the existing reserved=100. ⚠️ Workers MUST set
# DIAMOND_BLOCK_SIZE so the whole shard is one block (-b4 at 32 shards), and raising
# shard_count REQUIRES bumping WORKER_RESERVED_CONCURRENCY in
# scripts/provision_diamond_infra.sh first. Pin the exact count after benchmarking.
CORPUS_LETTERS_ESTIMATE = 104_940_484_545   # live-manifest total_letters
LETTERS_PER_B = 1_000_000_000               # block ≈ -b × 1e9 letters
RSS_BASE_GB = 1.9                           # measured peak RSS at -b1
RSS_PER_B_GB = 1.38                         # measured slope (1.9 @ -b1 → 8.8 @ -b6)
LAMBDA_MEM_GB = 10                           # the tier already provisioned for vCPUs
WORKER_RESERVED_CONCURRENCY = 100            # CLAUDE.md §10.6 (provisioned)
DEFAULT_SHARD_COUNT = 32                     # ~3.3 G letters/shard, -b4, ~6 GB RSS, ~3 concurrent
DEFAULT_ZSTD_LEVEL = 19                      # Benjamin's rec; benchmark-validated (1.06 GiB)

# Mirror the multipart tuning the Lambda download path uses (lambda_function.py).
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=32 * 1024 * 1024,
    max_concurrency=32,
    use_threads=True,
)

# Bytes pulled per streaming read of the corpus body. Large reads keep the
# split-on-record-boundary loop cheap relative to the network.
STREAM_CHUNK = 64 * 1024 * 1024


def human(n):
    """Bytes -> human string (binary units)."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.2f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.2f} TiB"


def shard_for(record_index, partition, shard_count, shard_size):
    if partition == "round-robin":
        return record_index % shard_count
    # contiguous: clamp the final partial shard into the last bucket
    return min(record_index // shard_size, shard_count - 1)


def stream_and_partition(s3, bucket, key, shard_paths, partition, shard_count,
                         max_records=None):
    """Single streaming pass: split the corpus into SHARD_COUNT FASTA files.

    Reads the S3 body in STREAM_CHUNK slices and splits on b"\\n>" record
    boundaries (the leading '>' of the very first record has no preceding
    newline, so it is handled explicitly). The trailing fragment of each chunk
    is held back as `leftover` until the next chunk completes it.

    Returns (per-shard sequence counts, per-shard residue counts, total records).
    Residues are counted exactly here (bytes after each record's first newline,
    minus internal newlines) so the manifest's `letters`/`total_letters` is exact
    for ANY format — the zstd path has no `.dmnd` to run `diamond dbinfo` on, and
    exact beats the live manifest's striped-sample estimate for --dbsize anyway.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    obj_size = obj["ContentLength"]
    body = obj["Body"]
    shard_size = math.ceil(CORPUS_COUNT / shard_count)

    counts = [0] * shard_count
    letters = [0] * shard_count
    handles = [open(p, "wb", buffering=1024 * 1024) for p in shard_paths]
    record_index = 0
    read_bytes = 0
    t0 = time.time()

    # Invariant: `pending` holds the bytes of the in-progress record INCLUDING
    # its leading '>' (empty only before the first '>' is seen). `combined`
    # therefore always starts with '>' for well-formed FASTA. Splitting on
    # b"\n>" yields: parts[0] = a complete record (keeps its '>' from pending),
    # parts[1:-1] = complete records that lost their '>' to the delimiter,
    # parts[-1] = the next in-progress record body (also missing its '>').
    class _Done(Exception):
        pass

    def emit(rec):
        nonlocal record_index
        sh = shard_for(record_index, partition, shard_count, shard_size)
        handles[sh].write(rec)
        handles[sh].write(b"\n")  # the '\n' that the split delimiter consumed
        counts[sh] += 1
        # Residues = bytes after the header line (first '\n'), minus the internal
        # newlines of wrapped/multi-line sequences. The trailing delimiter '\n'
        # was consumed by the split, so it is not in `rec`.
        nl = rec.find(b"\n")
        if nl >= 0:
            seq = rec[nl + 1:]
            letters[sh] += len(seq) - seq.count(b"\n")
        record_index += 1
        if max_records and record_index >= max_records:
            raise _Done  # --max-records smoke-test cap reached

    pending = b""
    capped = False
    try:
        try:
            while True:
                chunk = body.read(STREAM_CHUNK)
                if not chunk:
                    break
                read_bytes += len(chunk)
                combined = pending + chunk
                parts = combined.split(b"\n>")
                if len(parts) > 1:
                    # parts[0] already carries its leading '>'; the rest don't.
                    emit(parts[0])
                    for p in parts[1:-1]:
                        emit(b">" + p)
                    pending = b">" + parts[-1]
                else:
                    pending = combined  # no boundary yet; keep accumulating

                pct = 100 * read_bytes / obj_size if obj_size else 0
                mbps = read_bytes / (time.time() - t0 + 1e-9) / 1e6
                print(f"  streamed {human(read_bytes)} / {human(obj_size)} "
                      f"({pct:4.1f}%)  {record_index:,} recs  {mbps:.0f} MB/s",
                      end="\r", flush=True)

            # Flush the final in-progress record (only on a full read).
            tail = pending.strip()
            if tail:
                emit(tail if tail.startswith(b">") else b">" + tail)
        except _Done:
            capped = True  # stopped early at --max-records; pending dropped
    finally:
        for h in handles:
            h.close()

    print()  # finish the progress line
    print(f"  partitioned {record_index:,} records in {time.time() - t0:.0f}s")
    if capped:
        print(f"  NOTE: stopped at --max-records={max_records:,} "
              f"(smoke test, not a full build — do NOT publish).")
    elif record_index != CORPUS_COUNT:
        print(f"  WARNING: parsed {record_index:,} records but expected "
              f"{CORPUS_COUNT:,} (Check 0). Investigate before publishing.")
    return counts, letters, record_index


def build_shard(fasta_path, dmnd_base, threads):
    """`.dmnd` arm: diamond makedb --in fasta -d dmnd_base. Returns (path, size).

    Letters are no longer read back via `diamond dbinfo` — they are counted
    exactly in the streaming pass (works for any format, one fewer subprocess).
    """
    cmd = ["diamond", "makedb", "--in", str(fasta_path),
           "-d", str(dmnd_base), "--threads", str(threads)]
    print(f"  $ {' '.join(cmd)}")
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise RuntimeError(f"diamond makedb failed for {fasta_path}")
    dmnd_path = Path(f"{dmnd_base}.dmnd")
    if not dmnd_path.exists():
        raise RuntimeError(f"expected {dmnd_path} after makedb, not found")
    size = dmnd_path.stat().st_size
    print(f"    -> {dmnd_path.name}  {human(size)}  in {time.time() - t0:.0f}s")
    return dmnd_path, size


def compress_shard_zstd(fasta_path, level, threads):
    """zstd arm: zstd -<level> the shard FASTA -> shard.fasta.zst. (path, size).

    DIAMOND built WITH_ZSTD=ON reads the `.zst` directly as the DB — no makedb,
    no on-disk decompression (it streams the FASTA out of the compressed file).
    The smaller artifact is the whole point: doc 06 measured shard_00 at 1.06 GiB
    vs 5.60 GiB for `.dmnd` — a 5.3× smaller transfer. Workers MUST pass --dbsize
    (manifest total_letters) or DIAMOND re-runs a ~92 s letter-count pass on open.
    """
    zst_path = Path(f"{fasta_path}.zst")  # shard_NN.fasta.zst
    cmd = ["zstd", f"-{level}", f"-T{threads or 0}", "-f",
           "-o", str(zst_path), str(fasta_path)]
    print(f"  $ {' '.join(cmd)}")
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise RuntimeError(f"zstd compression failed for {fasta_path}")
    if not zst_path.exists():
        raise RuntimeError(f"expected {zst_path} after zstd, not found")
    size = zst_path.stat().st_size
    print(f"    -> {zst_path.name}  {human(size)}  in {time.time() - t0:.0f}s")
    return zst_path, size


def database_release(corpus_key):
    """Semantic corpus release (e.g. "v1.1") parsed from the corpus filename.

    s3://.../petadex.catalytic_orfs.v1.1.fa -> "v1.1". This is the human-facing
    DATABASE version (distinct from the timestamped build `version` tag), stamped
    into the manifest so the orchestrator/result can label searches by it without
    re-parsing. Returns None if no vN[.N...] token is present.
    """
    m = re.search(r"v\d+(?:\.\d+)*", corpus_key or "")
    return m.group(0) if m else None


def diamond_version():
    try:
        out = subprocess.run(["diamond", "version"], capture_output=True,
                             text=True, check=True).stdout.strip()
        return out.split()[-1]  # "diamond version 2.1.11" -> "2.1.11"
    except Exception:
        return "unknown"


def upload_shard(s3, bucket, artifact_path, version):
    """Upload one shard artifact (`.dmnd` or `.fasta.zst`). Key = its filename,
    so the manifest `key` (and the worker's `shardKey`) carries the format."""
    key = f"{DIAMOND_PREFIX}/{version}/{artifact_path.name}"
    size = artifact_path.stat().st_size
    print(f"  uploading {artifact_path.name} ({human(size)}) -> s3://{bucket}/{key}")
    t0 = time.time()
    s3.upload_file(str(artifact_path), bucket, key,
                   Config=S3_TRANSFER_CONFIG,
                   ExtraArgs={"ServerSideEncryption": "AES256"})
    print(f"    done in {time.time() - t0:.0f}s")
    return key


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--key", default=DEFAULT_KEY,
                    help="corpus FASTA object key (NOT the RDS table)")
    ap.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT,
                    help="number of shards (= workers per search; MUST be <= "
                         "worker reserved concurrency, see CLAUDE.md §10.6)")
    ap.add_argument("--format", choices=["dmnd", "zstd"], default="dmnd",
                    help="shard artifact format. dmnd = native (seed index, the "
                         "current production format); zstd = compressed FASTA-as-DB "
                         "(5.3x smaller transfer, needs finer shards for -b1 "
                         "correctness — see the granularity note up top).")
    ap.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL,
                    help="zstd compression level (only used by --format zstd)")
    ap.add_argument("--partition", choices=["round-robin", "contiguous"],
                    default="round-robin")
    ap.add_argument("--version",
                    help="version label (default: catalytic_orfs_v1.1_<UTC ts>)")
    ap.add_argument("--scratch-dir", default="/mnt/scratch",
                    help="local dir for shard FASTAs + .dmnd (needs ~300 GB)")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--keep-fasta", action="store_true",
                    help="don't delete each shard FASTA after its makedb")
    ap.add_argument("--skip-upload", action="store_true",
                    help="build shards + write local manifest, but no S3 writes")
    ap.add_argument("--no-bump-latest", action="store_true",
                    help="upload shards + manifest but do NOT move LATEST")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan and exit without touching S3 or disk")
    ap.add_argument("--max-records", type=int, default=None,
                    help="SMOKE TEST: stop after N records (a truncated, "
                         "non-publishable DB). Requires --skip-upload or "
                         "--no-bump-latest so it can never bump LATEST.")
    args = ap.parse_args()

    # A --max-records run produces a truncated corpus; never let it become the
    # version workers resolve via LATEST.
    if args.max_records and not (args.skip_upload or args.no_bump_latest):
        ap.error("--max-records is a smoke test and must not publish; "
                 "pass --skip-upload or --no-bump-latest too.")

    version = args.version or (
        "catalytic_orfs_v1.1_"
        + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    scratch = Path(args.scratch_dir)
    shard_size = math.ceil(CORPUS_COUNT / args.shard_count)

    print("=" * 64)
    print("PETadex DIAMOND shard builder (Phase 1)")
    print("=" * 64)
    print(f"corpus      s3://{args.bucket}/{args.key}")
    print(f"version     {version}")
    print(f"format      {args.format}"
          + (f" (zstd -{args.zstd_level})" if args.format == "zstd" else ""))
    print(f"shards      {args.shard_count}  ({args.partition})")
    print(f"shard_size  {shard_size:,} seqs/shard (contiguous target)")
    est_letters_per_shard = CORPUS_LETTERS_ESTIMATE // args.shard_count
    need_b = max(1, math.ceil(est_letters_per_shard / LETTERS_PER_B))
    est_rss = RSS_BASE_GB + RSS_PER_B_GB * (need_b - 1)
    print(f"~letters/sh {est_letters_per_shard:,}")
    print(f"scratch     {scratch}")
    print(f"threads     {args.threads}")
    print(f"diamond     {diamond_version()}")

    # zstd single-block requirement: a shard must be ONE reference block or it
    # over-reports (see the granularity note up top). Report the -b workers need
    # for that, and its estimated RSS; warn only if that RSS won't fit the tier.
    if args.format == "zstd":
        print(f"single-block: workers need DIAMOND_BLOCK_SIZE >= {need_b} "
              f"(-b{need_b}); est peak RSS ~{est_rss:.1f} GB / {LAMBDA_MEM_GB} GB tier")
        if est_rss > LAMBDA_MEM_GB - 1:
            finer = math.ceil(CORPUS_LETTERS_ESTIMATE
                              / ((LAMBDA_MEM_GB - 1 - RSS_BASE_GB) / RSS_PER_B_GB + 1)
                              / LETTERS_PER_B)
            print(f"  ⚠️  ~{est_rss:.1f} GB leaves <1 GB headroom under the "
                  f"{LAMBDA_MEM_GB} GB tier — shard finer (~{finer}+ shards) so the "
                  f"single block is smaller, or it may OOM.")
    # Concurrency coupling: one worker per shard per search (see top-of-file note).
    max_concurrent = WORKER_RESERVED_CONCURRENCY // args.shard_count
    if not (args.no_bump_latest or args.skip_upload):
        if max_concurrent < 1:
            print(f"  ⚠️  {args.shard_count} shards > reserved concurrency "
                  f"({WORKER_RESERVED_CONCURRENCY}): a SINGLE search can't get enough "
                  f"workers → guaranteed throttle. Raise WORKER_RESERVED_CONCURRENCY "
                  f"in provision_diamond_infra.sh FIRST.")
        else:
            print(f"concurrency : {args.shard_count} shards → ~{max_concurrent} "
                  f"concurrent searches under reserved={WORKER_RESERVED_CONCURRENCY} "
                  f"(raise it in provision_diamond_infra.sh to allow more)")
    print(f"dest        s3://{args.bucket}/{DIAMOND_PREFIX}/{version}/")
    print(f"upload      {'NO (--skip-upload)' if args.skip_upload else 'yes'}")
    print(f"bump LATEST {'NO' if (args.no_bump_latest or args.skip_upload) else 'yes (last)'}")
    if args.max_records:
        print(f"max_records {args.max_records:,}  ⚠️  SMOKE TEST — truncated, non-publishable")
    print("=" * 64)
    if args.dry_run:
        print("dry run — exiting before any disk or S3 work.")
        return

    scratch.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", region_name="us-east-1")

    width = len(str(args.shard_count - 1))
    fasta_paths = [scratch / f"shard_{i:0{width}d}.fasta"
                   for i in range(args.shard_count)]
    dmnd_bases = [scratch / f"shard_{i:0{width}d}" for i in range(args.shard_count)]

    # [1/4] Stream + partition the corpus in a single pass (counts residues too).
    print(f"\n[1/4] Streaming corpus and partitioning into {args.shard_count} shards...")
    counts, letters, total = stream_and_partition(
        s3, args.bucket, args.key, fasta_paths, args.partition, args.shard_count,
        max_records=args.max_records)
    for i, c in enumerate(counts):
        print(f"  shard_{i:0{width}d}: {c:,} seqs / {letters[i]:,} letters "
              f"({human(fasta_paths[i].stat().st_size)} FASTA)")

    # [2/4] Build each shard artifact (dmnd makedb or zstd -19), then delete the
    # FASTA right after to bound scratch use. artifact_paths drives both upload
    # and the manifest `key`, so the format is carried by the filename.
    print(f"\n[2/4] Building {args.shard_count} shard artifacts ({args.format})...")
    artifact_paths = []
    artifact_sizes = []
    for i in range(args.shard_count):
        print(f"shard_{i:0{width}d}:")
        if args.format == "dmnd":
            art_path, size = build_shard(fasta_paths[i], dmnd_bases[i], args.threads)
        else:  # zstd: compress the shard FASTA in place
            art_path, size = compress_shard_zstd(
                fasta_paths[i], args.zstd_level, args.threads)
        artifact_paths.append(art_path)
        artifact_sizes.append(size)
        if not args.keep_fasta:
            fasta_paths[i].unlink(missing_ok=True)
    print(f"  total {args.format} bytes: {human(sum(artifact_sizes))}")

    # [3/4] Upload shards.
    shard_keys = []
    if args.skip_upload:
        print(f"\n[3/4] --skip-upload: leaving {args.format} shards on scratch, no S3 writes.")
    else:
        print(f"\n[3/4] Uploading {args.shard_count} shards to s3://{args.bucket}/"
              f"{DIAMOND_PREFIX}/{version}/ ...")
        for i in range(args.shard_count):
            shard_keys.append(
                upload_shard(s3, args.bucket, artifact_paths[i], version))

    # [4/4] Manifest, then (last) LATEST.
    manifest = {
        "version": version,
        # Semantic corpus release (e.g. "v1.1") — the human-facing DATABASE
        # version, parsed from the corpus filename. Distinct from `version`, the
        # timestamped build tag that pins an exact build of that release.
        "database_release": database_release(args.key),
        "build_date": datetime.now(timezone.utc).isoformat(),
        "corpus": f"s3://{args.bucket}/{args.key}",
        "diamond_version": diamond_version(),
        # Shard artifact format ("dmnd" | "zstd"). The worker also infers it from
        # the shardKey extension; recorded here for human/tooling clarity.
        "format": args.format,
        "zstd_level": args.zstd_level if args.format == "zstd" else None,
        "partition": args.partition,
        "shard_count": args.shard_count,
        "total_sequences": total,
        "expected_sequences": CORPUS_COUNT,
        # On-disk bytes of the primary artifact (`.dmnd` or `.fasta.zst`). Name kept
        # as *_dmnd_bytes for backward compat; it is the artifact size for any format.
        "total_dmnd_bytes": sum(artifact_sizes),
        # Total corpus residues, counted EXACTLY in the streaming pass (no longer a
        # striped-sample estimate). Workers pass this as --dbsize so e-values
        # calibrate against the whole corpus, not one shard (docs/evalue-calibration.md).
        "total_letters": sum(letters),
        "shards": [
            {
                "index": i,
                # The exact uploaded key (artifact filename carries the format),
                # so the worker's shardKey never reconstructs a pad width or suffix.
                "key": f"{DIAMOND_PREFIX}/{version}/{artifact_paths[i].name}",
                "sequences": counts[i],
                "dmnd_bytes": artifact_sizes[i],
                "letters": letters[i],
            }
            for i in range(args.shard_count)
        ],
    }
    manifest_blob = json.dumps(manifest, indent=2)
    local_manifest = scratch / f"manifest_{version}.json"
    local_manifest.write_text(manifest_blob)
    print(f"\n[4/4] manifest -> {local_manifest}")

    if args.skip_upload:
        print("--skip-upload: manifest written locally only; LATEST untouched.")
    else:
        manifest_key = f"{DIAMOND_PREFIX}/{version}/manifest.json"
        s3.put_object(Bucket=args.bucket, Key=manifest_key, Body=manifest_blob,
                      ContentType="application/json", ServerSideEncryption="AES256")
        print(f"  s3://{args.bucket}/{manifest_key}")

        if args.no_bump_latest:
            print("  --no-bump-latest: LATEST NOT moved. To publish later:")
            print(f"    aws s3api put-object --bucket {args.bucket} "
                  f"--key {DIAMOND_PREFIX}/LATEST --body <(echo -n {version})")
        else:
            # LAST step — only now is the version discoverable to workers.
            s3.put_object(Bucket=args.bucket, Key=f"{DIAMOND_PREFIX}/LATEST",
                          Body=version.encode(), ContentType="text/plain",
                          ServerSideEncryption="AES256")
            print(f"  LATEST -> {version}")

    print("\n" + "=" * 64)
    print("DONE.")
    print(f"  shards built : {args.shard_count}  ({args.format})")
    print(f"  sequences    : {total:,}")
    print(f"  letters      : {sum(letters):,}")
    print(f"  total bytes  : {human(sum(artifact_sizes))}")
    print(f"  version      : {version}")
    if not args.skip_upload and not args.no_bump_latest:
        print("  LATEST now points here — workers will pick this up.")
    print("=" * 64)


if __name__ == "__main__":
    main()
