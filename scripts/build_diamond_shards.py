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
CORPUS_LETTERS = 104_940_484_545  # total residues (backfilled estimate, §10.8)
# 32 (was Phase 0's 20): a 20-shard zstd shard (~5.25 Gletters) forces a single
# `-b6` reference block that OOMs the 10 GiB Lambda tier; 32 shards (~3.28
# Gletters -> -b4) fit. Finer sharding also shortens each worker's download+search
# and adds /tmp headroom for the .dmnd path. See the db-format memory note and §10.13.
DEFAULT_SHARD_COUNT = 32
DIAMOND_PREFIX = "diamond"  # s3://{bucket}/diamond/...

# A FASTA-as-DB (zstd) must search as a SINGLE reference block; that block's
# peak RSS scales with `-b`. -b6 OOM-killed Lambda's 10 GiB max tier (measured
# 2026-06-09), so the build refuses any shard count whose single block needs
# -b >= this. ceil(CORPUS_LETTERS / ((LIMIT-1) * 1e9)) shards stays at or below it.
FASTA_BLOCK_LIMIT = 5

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

    Returns (per-shard sequence counts, total records, per-shard residue counts).
    The residue counts are summed cheaply during this pass (count non-newline
    bytes after each record's header line) so the zstd path has per-shard
    `letters` for the manifest without a `diamond dbinfo` (it builds no `.dmnd`).
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    obj_size = obj["ContentLength"]
    body = obj["Body"]
    shard_size = math.ceil(CORPUS_COUNT / shard_count)

    counts = [0] * shard_count
    letters = [0] * shard_count
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
        # Residues = bytes after the header line, minus its internal newlines.
        nl = rec.find(b"\n")
        if nl != -1:
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
    return counts, record_index, letters


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


def compress_shard(fasta_path, base, level, threads):
    """zstd-compress a shard FASTA into `{base}.fa.zst` — the artifact DIAMOND
    2.2.x reads directly as `-d` (built WITH_ZSTD=ON). Returns (path, bytes).

    No `diamond makedb` runs for the zstd format: the compressed FASTA *is* the
    shard. It carries no prebuilt seed index, which is why the worker must search
    it as a single reference block (see worker.fasta_block_size)."""
    out = Path(f"{base}.fa.zst")
    cmd = ["zstd", "-q", "-f", f"-{level}", f"-T{threads}",
           str(fasta_path), "-o", str(out)]
    print(f"  $ {' '.join(cmd)}")
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise RuntimeError(f"zstd failed for {fasta_path}")
    if not out.exists():
        raise RuntimeError(f"expected {out} after zstd, not found")
    size = out.stat().st_size
    print(f"    -> {out.name}  {human(size)}  in {time.time() - t0:.0f}s")
    return out, size


def require_zstd():
    """Fail early (before the long stream) if the `zstd` CLI is missing."""
    try:
        subprocess.run(["zstd", "--version"], capture_output=True, check=True)
    except Exception:
        raise SystemExit(
            "ERROR: --format zstd needs the `zstd` CLI, not found on PATH.\n"
            "  Amazon Linux 2:  sudo yum install -y zstd\n"
            "  Debian/Ubuntu:   sudo apt-get install -y zstd")


def shard_letters(dmnd_base):
    """Residue (letter) count of a built .dmnd, via `diamond dbinfo`.

    Recorded in the manifest as total_letters and passed to workers as
    --dbsize, so per-shard e-values calibrate against the full corpus instead
    of a single shard (see docs/evalue-calibration.md). Returns 0 if it can't
    be parsed — the build still proceeds; dbsize just won't be recorded.
    """
    try:
        out = subprocess.run(["diamond", "dbinfo", "-d", str(dmnd_base)],
                             capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            if "letters" in line.lower():
                digits = "".join(ch for ch in line if ch.isdigit())
                if digits:
                    return int(digits)
    except Exception as e:
        print(f"    WARNING: diamond dbinfo failed for {dmnd_base}: {e}")
    return 0


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
    ap.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    ap.add_argument("--format", choices=["dmnd", "zstd"], default="dmnd",
                    help="shard artifact: native .dmnd (default, production) or "
                         ".fa.zst compressed-FASTA-as-DB (smaller download; needs "
                         "DIAMOND built WITH_ZSTD and finer sharding to fit Lambda "
                         "memory — see §10.13)")
    ap.add_argument("--zstd-level", type=int, default=19,
                    help="zstd compression level for --format zstd (default 19)")
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

    # zstd memory guard: a FASTA-as-DB must search as a single reference block,
    # whose -b (= ceil(letters/1e9)) drives Lambda RSS. -b6 OOM'd the 10 GiB tier,
    # so refuse a shard count whose block would need -b >= FASTA_BLOCK_LIMIT.
    if args.format == "zstd":
        est_shard_letters = CORPUS_LETTERS / args.shard_count
        rec_b = math.ceil(est_shard_letters / 1e9)
        min_shards = math.ceil(CORPUS_LETTERS / ((FASTA_BLOCK_LIMIT - 1) * 1e9))
        if rec_b >= FASTA_BLOCK_LIMIT:
            ap.error(
                f"--format zstd with {args.shard_count} shards needs a single "
                f"reference block of -b{rec_b} (~{est_shard_letters/1e9:.1f} "
                f"Gletters/shard); -b>={FASTA_BLOCK_LIMIT} OOMs the 10 GiB Lambda "
                f"tier. Use --shard-count >= {min_shards}.")
        print(f"zstd single-block size ~ -b{rec_b} "
              f"(~{est_shard_letters/1e9:.1f} Gletters/shard) — fits the 10 GiB tier.")

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
          + (f"  (zstd level {args.zstd_level})" if args.format == "zstd" else ""))
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

    if args.format == "zstd":
        require_zstd()  # fail before the long stream if the CLI is absent
    scratch.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3", region_name="us-east-1")

    ext = ".fa.zst" if args.format == "zstd" else ".dmnd"
    width = len(str(args.shard_count - 1))
    fasta_paths = [scratch / f"shard_{i:0{width}d}.fasta"
                   for i in range(args.shard_count)]
    dmnd_bases = [scratch / f"shard_{i:0{width}d}" for i in range(args.shard_count)]

    # [1/4] Stream + partition the corpus in a single pass (counts residues too).
    print(f"\n[1/4] Streaming corpus and partitioning into {args.shard_count} shards...")
    counts, total, counted_letters = stream_and_partition(
        s3, args.bucket, args.key, fasta_paths, args.partition, args.shard_count,
        max_records=args.max_records)
    for i, c in enumerate(counts):
        print(f"  shard_{i:0{width}d}: {c:,} seqs / {letters[i]:,} letters "
              f"({human(fasta_paths[i].stat().st_size)} FASTA)")

    # [2/4] Build each shard artifact; delete the FASTA right after to bound
    # scratch use (for zstd the .fa.zst is the artifact, so the .fasta still goes).
    print(f"\n[2/4] Building {args.shard_count} {args.format} shards...")
    dmnd_sizes = []
    dmnd_letters = []
    artifact_paths = []
    for i in range(args.shard_count):
        print(f"shard_{i:0{width}d}:")
        if args.format == "zstd":
            # No makedb: compress the FASTA; letters come from the streaming pass
            # (a .fa.zst has no .dmnd to run `diamond dbinfo` on).
            out, size = compress_shard(fasta_paths[i], dmnd_bases[i],
                                       args.zstd_level, args.threads)
            letters = counted_letters[i]
            artifact_paths.append(out)
        else:
            size, letters = build_shard(fasta_paths[i], dmnd_bases[i], args.threads)
            artifact_paths.append(Path(f"{dmnd_bases[i]}.dmnd"))
        dmnd_sizes.append(size)
        dmnd_letters.append(letters)
        if not args.keep_fasta:
            fasta_paths[i].unlink()
    print(f"  total {args.format}: {human(sum(dmnd_sizes))}")

    # [3/4] Upload shards.
    shard_keys = []
    if args.skip_upload:
        print(f"\n[3/4] --skip-upload: leaving {args.format} shards on scratch, no S3 writes.")
        print(f"\n[3/4] --skip-upload: leaving {args.format} shards on scratch, no S3 writes.")
    else:
        print(f"\n[3/4] Uploading {args.shard_count} shards to s3://{args.bucket}/"
              f"{DIAMOND_PREFIX}/{version}/ ...")
        for i in range(args.shard_count):
            shard_keys.append(
                upload_shard(s3, args.bucket, artifact_paths[i], version))
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
        # Shard artifact format — "dmnd" (native) or "zstd" (.fa.zst FASTA-as-DB).
        # The worker also infers it from the shard key extension; recorded here for
        # at-a-glance manifest inspection.
        "format": args.format,
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
                "key": f"{DIAMOND_PREFIX}/{version}/shard_{i:0{width}d}{ext}",
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
    print(f"  shards built : {args.shard_count} ({args.format})")
    print(f"  sequences    : {total:,}")
    print(f"  total bytes  : {human(sum(dmnd_sizes))}")
    print(f"  version      : {version}")
    if not args.skip_upload and not args.no_bump_latest:
        print("  LATEST now points here — workers will pick this up.")
    print("=" * 64)


if __name__ == "__main__":
    main()
