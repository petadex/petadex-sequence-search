#!/usr/bin/env python3
"""
Striped corpus sampler — Phase 1 pre-build size check.

Pulls a sample of FASTA records spread evenly across the *whole* Logan corpus
object on S3 (not just the head), so the .dmnd size projection that sets
SHARD_COUNT is based on the true length distribution rather than a head sample.

Why striped: the Phase 0 spike sampled the head of the file and measured
~502 B/seq, drifting to ~460 B/seq by the first 15M sequences — i.e. the corpus
is not uniformly ordered, and head-sampling overestimates. This walks N evenly
spaced byte offsets, aligns each to a record boundary, and collects a handful of
records at each, reading only a tiny fraction of the 116 GB object.

Output: a sample FASTA plus FASTA-level byte/seq and length stats. Feed the
sample to `diamond makedb` to get the definitive .dmnd bytes/seq, then project:

    projected_total_dmnd = (dmnd_bytes / sampled_seqs) * CORPUS_COUNT
    SHARD_COUNT           = ceil(projected_total_dmnd / 8 GB)
"""

import argparse
import concurrent.futures
import statistics
import sys

import boto3

DEFAULT_BUCKET = "petadex"
DEFAULT_KEY = "logan/petadex.catalytic_orfs.v1.1.fa"
CORPUS_COUNT = 307_155_746  # Check 0 ground truth
SHARD_TMP_BUDGET = 8 * 1000**3  # 8 GB target per-shard .dmnd ceiling (decimal)


def fetch_records_at(s3, bucket, key, offset, obj_size, per_stripe, window):
    """Return up to `per_stripe` complete FASTA records starting at/after `offset`.

    A ranged GET lands mid-record, so we drop bytes before the first record
    boundary, and drop the final (possibly truncated) record. If the window
    holds too few complete records, extend it until satisfied or EOF.
    """
    records = []
    start = offset
    leftover = b""
    first = offset == 0
    while len(records) <= per_stripe and start < obj_size:
        end = min(start + window, obj_size) - 1
        body = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")["Body"].read()
        chunk = leftover + body
        if not first:
            # Align to the first record boundary in this chunk.
            nl = chunk.find(b"\n>")
            if nl == -1:
                start = end + 1
                leftover = b""
                continue
            chunk = chunk[nl + 1:]  # keep the '>' that starts the next record
            first = True
        # Split into records on '>' at line start; the last piece is incomplete
        # unless we hit EOF, so hold it back as leftover for the next loop.
        pieces = chunk.split(b"\n>")
        hit_eof = end + 1 >= obj_size
        complete = pieces if hit_eof else pieces[:-1]
        leftover = b"" if hit_eof else b">" + pieces[-1]
        for i, p in enumerate(complete):
            rec = p if (i == 0 and p.startswith(b">")) else b">" + p
            if rec.startswith(b">") and b"\n" in rec:
                records.append(rec.rstrip(b"\n") + b"\n")
                if len(records) >= per_stripe:
                    break
        start = end + 1
        if hit_eof:
            break
    return records[:per_stripe]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--num-stripes", type=int, default=1000)
    ap.add_argument("--per-stripe", type=int, default=1000,
                    help="records to collect at each stripe (total ≈ stripes × per-stripe)")
    ap.add_argument("--window", type=int, default=1_500_000,
                    help="bytes fetched per stripe before extending (default 1.5 MB)")
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--out", default="/tmp/striped_sample.fasta")
    args = ap.parse_args()

    s3 = boto3.client("s3")
    obj_size = s3.head_object(Bucket=args.bucket, Key=args.key)["ContentLength"]
    print(f"Object: s3://{args.bucket}/{args.key}")
    print(f"Size: {obj_size:,} B ({obj_size / 1000**3:.2f} GB)")
    print(f"Whole-file FASTA bytes/seq (size / {CORPUS_COUNT:,}): "
          f"{obj_size / CORPUS_COUNT:.2f}\n")

    stride = obj_size // args.num_stripes
    offsets = [i * stride for i in range(args.num_stripes)]
    print(f"Sampling {args.num_stripes} stripes × {args.per_stripe} records, "
          f"stride {stride:,} B, {args.threads} threads...")

    # Thread-per-stripe client reuse is fine: botocore clients are thread-safe.
    def worker(off):
        return fetch_records_at(s3, args.bucket, args.key, off, obj_size,
                                args.per_stripe, args.window)

    all_records = []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
        for recs in ex.map(worker, offsets):
            all_records.extend(recs)
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{args.num_stripes} stripes, "
                      f"{len(all_records):,} records", end="\r")
    print(f"  {args.num_stripes}/{args.num_stripes} stripes, "
          f"{len(all_records):,} records collected.")

    # Stats
    sample_bytes = sum(len(r) for r in all_records)
    seq_lens = []
    for r in all_records:
        nl = r.find(b"\n")
        seq_lens.append(len(r) - nl - 2)  # minus header line, minus 2 newlines
    n = len(all_records)
    if n == 0:
        sys.exit("No records collected — check window/offsets.")

    with open(args.out, "wb") as f:
        f.writelines(all_records)

    fasta_bps = sample_bytes / n
    print("\n=== Striped sample stats ===")
    print(f"Records collected:        {n:,}")
    print(f"Sample FASTA bytes:       {sample_bytes:,}")
    print(f"FASTA bytes/seq (sample): {fasta_bps:.2f}")
    print(f"Seq length (aa)  min/mean/median/max: "
          f"{min(seq_lens)} / {statistics.mean(seq_lens):.1f} / "
          f"{statistics.median(seq_lens)} / {max(seq_lens)}")
    print(f"Wrote sample → {args.out}")

    proj_fasta = fasta_bps * CORPUS_COUNT
    print("\n=== Projection (FASTA-level; confirm with diamond makedb) ===")
    print(f"Projected whole-corpus FASTA: {proj_fasta / 1000**3:.2f} GB "
          f"(actual object = {obj_size / 1000**3:.2f} GB → "
          f"sampler vs truth Δ {100*(proj_fasta-obj_size)/obj_size:+.1f}%)")
    print(f"\nNext: diamond makedb --in {args.out} -d /tmp/striped_sample")
    print(f"Then: projected_dmnd = (.dmnd bytes / {n}) * {CORPUS_COUNT:,}")
    print(f"      SHARD_COUNT     = ceil(projected_dmnd / {SHARD_TMP_BUDGET:,})")


if __name__ == "__main__":
    main()
