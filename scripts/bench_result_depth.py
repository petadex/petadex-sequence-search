#!/usr/bin/env python3
"""Measure what raising the search depth (`-k`) costs on the REAL worker Lambda.

The gate on `common.RESULT_DEPTH` (vault note "12 Variable Result Count" §9).
Direct-invokes `petadex-diamond-worker` against one live shard with different
`maxResults`, so the measurement uses production hardware (Graviton, 10240 MB
≈ 6 vCPU), the production `.fa.zst` codec and the production DIAMOND build —
none of which a local box can reproduce (Homebrew DIAMOND has no WITH_ZSTD).

Method: one warm-up invoke to cache the shard in /tmp (so `download_ms` drops
out and we isolate search), then interleaved arms so drift hits every arm
equally. Timings are read from the worker's own `TIMING` log lines via
`--log-type Tail`, plus Lambda's REPORT line for billed duration and peak RSS.

Artifacts: the worker hardcodes its part key as `results/{sessionId}/{jobId}/
parts/...`, so a benchmark's scratch parts land in the live results area whether
we want them or not. This script therefore ARCHIVES them when it finishes —
moving them under `diamond/benchmarks/result-depth/` and stripping the
`reapable=true` tag the worker sets. Both halves matter: the bucket's
`reap-search-parts` lifecycle rule is scoped to that TAG with no prefix filter,
so a moved-but-still-tagged object is deleted after 14 days regardless of where
it now lives. `--no-archive` leaves them in `results/` to expire normally.

    python3 scripts/bench_result_depth.py --depths 50 250 500 --runs 3
"""
import argparse
import base64
import json
import re
import statistics
import sys
import time
import uuid

import boto3
from botocore.config import Config

BUCKET = "petadex"
FN = "petadex-diamond-worker"

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, connect_timeout=15, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name="us-east-1")


def load_manifest(version):
    body = s3.get_object(Bucket=BUCKET, Key=f"diamond/{version}/manifest.json")["Body"].read()
    return json.loads(body)


def resolve_version():
    return s3.get_object(Bucket=BUCKET, Key="diamond/LATEST")["Body"].read().decode().strip()


def read_query(path, name):
    seqs, cur, hdr = {}, [], None
    for line in open(path):
        line = line.strip()
        if line.startswith(">"):
            if hdr:
                seqs[hdr] = "".join(cur)
            hdr, cur = line[1:], []
        elif line:
            cur.append(line)
    if hdr:
        seqs[hdr] = "".join(cur)
    if name not in seqs:
        sys.exit(f"query {name!r} not in {path}; have {list(seqs)}")
    return seqs[name]


def invoke(shard, query_fasta, depth, db_size, session):
    event = {
        "sessionId": session,
        "jobId": uuid.uuid4().hex[:8],
        "shardIndex": shard["index"],
        "shardKey": shard["key"],
        "shardSeqs": shard.get("sequences"),
        "shardLetters": shard.get("letters"),
        "queryFasta": query_fasta,
        "maxResults": depth,
        "dbSize": db_size,
    }
    t0 = time.monotonic()
    r = lam.invoke(FunctionName=FN, Payload=json.dumps(event).encode(), LogType="Tail")
    wall = round((time.monotonic() - t0) * 1000)
    payload = json.loads(r["Payload"].read())
    log = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")
    if r.get("FunctionError"):
        print(log[-2000:])
        sys.exit(f"worker failed: {payload}")

    def grab(pat, cast=float):
        m = re.search(pat, log)
        return cast(m.group(1)) if m else None

    return {
        "depth": depth,
        "hits": payload.get("numHits"),
        "search_s": grab(r"TIMING diamond_blastp: ([\d.]+)s"),
        "download_s": grab(r"TIMING shard_download: ([\d.]+)s"),
        "billed_ms": grab(r"Billed Duration: (\d+) ms", int),
        "max_mem_mb": grab(r"Max Memory Used: (\d+) MB", int),
        "invoke_wall_ms": wall,
    }


def archive_artifacts(session, dest_prefix):
    """Move this run's worker parts out of results/ and into the benchmark area.

    The worker writes `results/{sessionId}/{jobId}/parts/*` tagged
    `reapable=true`. We relocate them under `dest_prefix` and drop the tag
    (`TaggingDirective="REPLACE"` with no Tagging = no tags), because the
    `reap-search-parts` lifecycle rule matches on that tag alone — a relocated
    object that keeps it still disappears in 14 days.
    """
    src = f"results/{session}/"
    moved = total_bytes = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=src):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            dest = dest_prefix.rstrip("/") + "/" + key[len(src):]
            s3.copy_object(Bucket=BUCKET, Key=dest,
                           CopySource={"Bucket": BUCKET, "Key": key},
                           TaggingDirective="REPLACE")   # no Tagging => untagged
            s3.delete_object(Bucket=BUCKET, Key=key)
            moved += 1
            total_bytes += obj["Size"]
    if moved:
        print(f"\narchived {moved} artifact(s) ({total_bytes:,} B) -> "
              f"s3://{BUCKET}/{dest_prefix.rstrip('/')}/ (untagged, will not be reaped)")
    else:
        print(f"\nno artifacts found under s3://{BUCKET}/{src}")
    return moved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--depths", type=int, nargs="+", default=[50, 250, 500])
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--query", default="IsPETase")
    ap.add_argument("--fasta", default="example-sequences.txt")
    ap.add_argument("--version", default=None)
    ap.add_argument("--session", default="benchdepth")
    ap.add_argument("--out", default=None)
    ap.add_argument("--archive-prefix", default="diamond/benchmarks/result-depth",
                    help="where to move this run's worker parts when it finishes")
    ap.add_argument("--no-archive", action="store_true",
                    help="leave parts in results/ (they expire via the reapable tag)")
    a = ap.parse_args()

    version = a.version or resolve_version()
    man = load_manifest(version)
    shard = next(s for s in man["shards"] if s["index"] == a.shard)
    db_size = man.get("total_letters") or 0
    seq = read_query(a.fasta, a.query)
    qfasta = f">{a.query}\n{seq}\n"

    print(f"version   {version}  ({man.get('format')}, {man.get('shard_count')} shards)")
    print(f"shard {a.shard}   {shard['key']}  ({shard['sequences']:,} seqs, {shard['letters']:,} letters)")
    print(f"query     {a.query} ({len(seq)} aa)   dbsize {db_size:,}")
    print(f"arms      {a.depths}  x{a.runs} runs, interleaved\n")

    print("warm-up (caches the shard in /tmp so download drops out)...")
    w = invoke(shard, qfasta, a.depths[0], db_size, a.session)
    print(f"  cold: download {w['download_s']}s  search {w['search_s']}s  hits {w['hits']}\n")

    rows = []
    for r in range(a.runs):
        for d in a.depths:
            m = invoke(shard, qfasta, d, db_size, a.session)
            rows.append(m)
            dl = "cached" if m["download_s"] is None else f"DOWNLOAD {m['download_s']}s"
            print(f"  run {r+1} -k {d:<4} search {m['search_s']:>6.2f}s  hits {m['hits']:>4}  "
                  f"mem {m['max_mem_mb']} MB  [{dl}]")

    print("\n" + "=" * 74)
    print(f"{'-k':>6} {'hits':>6} {'search median':>15} {'vs -k 50':>12} {'peak RSS':>10}")
    print("-" * 74)
    base = None
    summary = []
    for d in a.depths:
        xs = [r["search_s"] for r in rows if r["depth"] == d and r["search_s"] is not None]
        hits = [r["hits"] for r in rows if r["depth"] == d]
        mem = [r["max_mem_mb"] for r in rows if r["depth"] == d and r["max_mem_mb"]]
        med = statistics.median(xs)
        if base is None:
            base = med
        delta = f"{med - base:+.2f}s ({(med/base - 1) * 100:+.1f}%)" if base else ""
        print(f"{d:>6} {max(hits):>6} {med:>14.2f}s {delta:>12} {max(mem) if mem else '-':>7} MB")
        summary.append({"depth": d, "hits_max": max(hits), "search_median_s": round(med, 3),
                        "delta_s": round(med - base, 3), "peak_rss_mb": max(mem) if mem else None,
                        "runs": [round(x, 3) for x in xs]})
    print("=" * 74)

    exhausted = [s for s in summary if s["hits_max"] < s["depth"]]
    if exhausted:
        print(f"\nNOTE: at -k {exhausted[0]['depth']} the query returned only "
              f"{exhausted[0]['hits_max']} hits — the e-value floor bound before the depth cap, "
              f"so deeper -k is free for this query.")

    out = {"version": version, "shard": shard["key"], "query": a.query,
           "query_len": len(seq), "warmup": w, "summary": summary, "rows": rows}
    if a.out:
        json.dump(out, open(a.out, "w"), indent=2)
        print(f"\nwrote {a.out}")

    if not a.no_archive:
        archive_artifacts(a.session, f"{a.archive_prefix}/{a.session}")


if __name__ == "__main__":
    main()
