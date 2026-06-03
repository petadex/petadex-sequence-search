#!/usr/bin/env python3
"""Backfill `total_letters` into an existing DIAMOND manifest (for --dbsize).

The e-value calibration fix (docs/evalue-calibration.md) needs the corpus
residue count in the manifest. Manifests built before that change lack it, so
the orchestrator sends dbSize=0 and workers omit --dbsize (leaving the ~20×
e-value deflation). Re-building records `total_letters` exactly; this script
populates it for the *current* live version without a 307M rebuild.

Two modes:
  --estimate  (default, runs anywhere, no diamond): striped-sample the corpus
              FASTA from S3, measure mean residues/sequence, multiply by the
              manifest's total_sequences. Accurate to ~1% — far tighter than
              e-values need (they're log-scale), and tagged as an estimate.
  --exact LETTERS : write a known exact value (e.g. the sum of per-shard
              `diamond dbinfo` letters computed on the build box).

DRY-RUN by default; pass --apply to write the patched manifest back to S3.

Usage:
  ./backfill_manifest_letters.py                     # dry-run, estimate
  ./backfill_manifest_letters.py --apply             # write estimate
  ./backfill_manifest_letters.py --exact 95000000000 --apply
"""
import argparse
import io
import json
import sys

import boto3

BUCKET = "petadex"
REGION = "us-east-1"
DIAMOND_PREFIX = "diamond"
AA = set(b"ACDEFGHIKLMNPQRSTVWYBJOUXZ*acdefghiklmnpqrstvwybjouxz")
STRIPES = 50               # evenly spaced sample windows across the corpus
STRIPE_BYTES = 2 * 1024 * 1024


def resolve_version(s3, requested):
    if requested:
        return requested
    body = s3.get_object(Bucket=BUCKET, Key=f"{DIAMOND_PREFIX}/LATEST")["Body"].read()
    return body.decode().strip()


def count_window(raw):
    """Count (residues, sequences) among COMPLETE records in a byte window.

    Drop the leading partial record (before the first '\\n>') and the trailing
    partial record (after the last '\\n>'), so every counted record is whole.
    """
    i = raw.find(b"\n>")
    j = raw.rfind(b"\n>")
    if i == -1 or i == j:
        return 0, 0
    body = raw[i + 1:j]                      # from first '>' .. just before last '\n>'
    residues = seqs = 0
    for line in body.split(b"\n"):
        if line.startswith(b">"):
            seqs += 1
        else:
            residues += sum(1 for c in line if c in AA)
    return residues, seqs


def estimate_letters(s3, corpus_uri, total_sequences):
    assert corpus_uri.startswith("s3://")
    bkt, key = corpus_uri[5:].split("/", 1)
    size = s3.head_object(Bucket=bkt, Key=key)["ContentLength"]
    step = max(1, (size - STRIPE_BYTES) // STRIPES)
    tot_res = tot_seq = 0
    for n in range(STRIPES):
        start = n * step
        end = min(start + STRIPE_BYTES - 1, size - 1)
        raw = s3.get_object(Bucket=bkt, Key=key,
                            Range=f"bytes={start}-{end}")["Body"].read()
        r, s = count_window(raw)
        tot_res += r
        tot_seq += s
    if tot_seq == 0:
        raise RuntimeError("sampled no complete records — check corpus URI")
    per_seq = tot_res / tot_seq
    print(f"  sampled {tot_seq:,} seqs over {STRIPES} stripes; "
          f"mean {per_seq:.2f} residues/seq")
    return round(per_seq * total_sequences), per_seq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", help="version dir (default: diamond/LATEST)")
    ap.add_argument("--exact", type=int, help="write this exact letter count")
    ap.add_argument("--apply", action="store_true", help="write back to S3 (else dry-run)")
    args = ap.parse_args()

    s3 = boto3.client("s3", region_name=REGION)
    version = resolve_version(s3, args.version)
    mkey = f"{DIAMOND_PREFIX}/{version}/manifest.json"
    manifest = json.loads(s3.get_object(Bucket=BUCKET, Key=mkey)["Body"].read())
    print(f"version: {version}")
    print(f"existing total_letters: {manifest.get('total_letters')}")
    print(f"total_sequences: {manifest.get('total_sequences'):,}")

    if args.exact is not None:
        letters, source = args.exact, "exact"
    else:
        letters, per_seq = estimate_letters(
            s3, manifest["corpus"], manifest["total_sequences"])
        source = "striped-sample-estimate"

    print(f"\n  -> total_letters = {letters:,}  (source: {source})")
    manifest["total_letters"] = letters
    manifest["total_letters_source"] = source

    if not args.apply:
        print("\nDRY-RUN — re-run with --apply to write the manifest back to S3.")
        return
    s3.put_object(Bucket=BUCKET, Key=mkey,
                  Body=json.dumps(manifest, indent=2).encode(),
                  ContentType="application/json")
    print(f"\nwrote s3://{BUCKET}/{mkey}  (total_letters={letters:,})")


if __name__ == "__main__":
    main()
