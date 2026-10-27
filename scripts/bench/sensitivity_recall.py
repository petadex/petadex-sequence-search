#!/usr/bin/env python3
"""
How much does production's `default` sensitivity miss, vs `--very-sensitive`?

The live endpoint can't answer this below ~50 % identity: it returns the top 500
hits, and those are all close homologs for PETase-like queries. Offline, on one
real production shard, run each query uncapped (`-k 0`, e <= 1e-3, the
production --dbsize) at both sensitivities and treat the very-sensitive hit set
as the reference. Reports, per query and pooled, the fraction of reference hits
that `default` also finds, by percent-identity bin, and how much longer the
very-sensitive run takes.

Usage:
  sensitivity_recall.py SHARD.fa.zst QUERY_DIR OUT_DIR [--threads N] [--dbsize N]
QUERY_DIR holds one FASTA per query (*.fa). DIAMOND 2.2.2 built with zstd on
PATH (or $DIAMOND).
"""

import argparse
import csv
import os
import subprocess
import time
from pathlib import Path

BINS = [(0, 30), (30, 40), (40, 50), (50, 60), (60, 101)]
COLS = ["sseqid", "pident", "evalue", "bitscore", "length", "slen"]


def run(diamond, query, shard, out, sensitivity, threads, dbsize):
    cmd = [diamond, "blastp", "-q", str(query), "-d", str(shard), "-o", str(out),
           "-b1", "-k", "0", "--threads", str(threads), "--dbsize", str(dbsize),
           "--quiet", "--outfmt", "6", *COLS]
    if sensitivity != "default":
        cmd.append(f"--{sensitivity}")
    t0 = time.time()
    subprocess.run(cmd, check=True)
    return time.time() - t0


def load(path):
    """sseqid -> pident (best hit per target)."""
    hits = {}
    with open(path) as f:
        for r in csv.reader(f, delimiter="\t"):
            hits[r[0]] = max(hits.get(r[0], 0.0), float(r[1]))
    return hits


def bin_of(pid):
    for lo, hi in BINS:
        if lo <= pid < hi:
            return f"{lo}-{min(hi, 100)}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("shard")
    ap.add_argument("query_dir")
    ap.add_argument("out_dir")
    ap.add_argument("--threads", type=int, default=24)
    ap.add_argument("--dbsize", type=int, default=88181580142)
    args = ap.parse_args()
    diamond = os.environ.get("DIAMOND", "diamond")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    labels = [f"{lo}-{min(hi, 100)}" for lo, hi in BINS]

    rows, pooled = [], {b: [0, 0] for b in labels}
    for q in sorted(Path(args.query_dir).glob("*.fa")):
        name = q.stem
        times = {}
        for sens in ("default", "very-sensitive"):
            path = out / f"{name}.{sens}.tsv"
            if not path.exists():
                times[sens] = run(diamond, q, args.shard, path, sens, args.threads, args.dbsize)
                (out / f"{name}.{sens}.seconds").write_text(f"{times[sens]:.1f}")
            else:
                times[sens] = float((out / f"{name}.{sens}.seconds").read_text())
        d, v = load(out / f"{name}.default.tsv"), load(out / f"{name}.very-sensitive.tsv")
        row = {"query": name, "default_hits": len(d), "very_sensitive_hits": len(v),
               "only_default": len(set(d) - set(v)),
               "recall_all": round(len(set(v) & set(d)) / len(v), 4) if v else None,
               "default_s": round(times["default"], 1), "very_sensitive_s": round(times["very-sensitive"], 1)}
        for b in labels:
            ref = [t for t, pid in v.items() if bin_of(pid) == b]
            found = sum(t in d for t in ref)
            pooled[b][0] += found
            pooled[b][1] += len(ref)
            row[f"recall_{b}"] = round(found / len(ref), 4) if ref else None
            row[f"n_{b}"] = len(ref)
        rows.append(row)
        print(row, flush=True)

    with open(out / "recall_by_query.tsv", "w") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    with open(out / "recall_pooled.tsv", "w") as f:
        f.write("pident_bin\treference_hits\tfound_by_default\trecall\n")
        for b in labels:
            found, n = pooled[b]
            f.write(f"{b}\t{n}\t{found}\t{found / n:.4f}\n" if n else f"{b}\t0\t0\tNA\n")
    print(open(out / "recall_pooled.tsv").read())
    print("RECALL DONE")


if __name__ == "__main__":
    main()
