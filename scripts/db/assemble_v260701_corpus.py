#!/usr/bin/env python3
"""
Assemble the v260701 search corpora from Thomas's pipeline outputs.

Writes two FASTAs with `>petadex_id`-only headers (e.g. `>LG1`, `>NR223`, `>PL1`):

  PETadex.seq.v260701.fa                  full-length proteins
  PETadex.seq_catalytic_cores.v260701.fa  catalytic cores

The core set (298,687,346 = PAZy 486 + nr 6,764,617 + Logan 291,922,243, the
clustering input) DEFINES membership: the full-length file holds exactly the
proteins whose petadex_id has a core. Core IDs are unique per protein, so the
join is 1:1. The `proteins` files are a superset (records whose HMM hit failed
the catalytic-site check, e.g. LG3, are not in the release).

Headers are cut to the ID because DIAMOND reports the ID as `target_id`, which
is the join key for the aggregator's tier-A enrichment. The other header fields
(run, HMM coords, component) go to a sidecar TSV instead of into every shard.

Logan full-length is ~500 GB, so it is filtered in parallel byte ranges against
a membership bitmap built from the core headers; parts are concatenated in
order, so record order matches the source.

Usage:
  python3 assemble_v260701_corpus.py --out-dir DIR [--workers 16]
"""

import argparse
import os
import re
import subprocess
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import numpy as np

SRC = Path("/watson/rnalab/tquigley/petadex.v260701/s3")
CORES = {
    "PL": SRC / "7_petadex_clustering/petadex-plasticome.v260701.catalytic_domain.fastaa",
    "NR": SRC / "7_petadex_clustering/petadex-nr.v260701.catalytic_domain.fastaa",
    "LG": SRC / "7_petadex_clustering/petadex-logan.v260701.catalytic_domain.fastaa",
}
PROTEINS = {
    "PL": SRC / "2_nr_search/petadex-plasticome.v260701.proteins.fastaa",
    "NR": SRC / "5_creating_logan_query/petadex-nr.v260701.proteins.fastaa",
    "LG": SRC / "6_logan_search_qc/petadex-logan.v260701.proteins.fastaa",
}
EXPECTED_CORES = {"PL": 486, "NR": 6_764_617, "LG": 291_922_243}
ORDER = ("PL", "NR", "LG")
ID_RE = re.compile(rb"^>([A-Z]+)(\d+)")
CHUNK = 64 * 1024 * 1024


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def byte_ranges(path, n):
    """Split `path` into n ranges, each starting on a '>' at line start."""
    size = path.stat().st_size
    starts = [0]
    with open(path, "rb") as f:
        for i in range(1, n):
            f.seek(size * i // n)
            f.readline()
            while True:
                pos = f.tell()
                line = f.readline()
                if not line or line.startswith(b">"):
                    break
            starts.append(pos)
    starts.append(size)
    return [(starts[i], starts[i + 1]) for i in range(n) if starts[i] < starts[i + 1]]


def iter_records(path, start, end):
    """Yield (header_line, seq_bytes) for records starting in [start, end)."""
    with open(path, "rb", buffering=CHUNK) as f:
        f.seek(start)
        header, seq = None, []
        while True:
            pos = f.tell()
            line = f.readline()
            if not line or (line.startswith(b">") and pos >= end):
                break
            if line.startswith(b">"):
                if header is not None:
                    yield header, b"".join(seq)
                header, seq = line.rstrip(b"\n"), []
            else:
                seq.append(line.strip())
        if header is not None:
            yield header, b"".join(seq)


# ---- pass 1: cores -> ID-only FASTA + bitmap + sidecar ---------------------

def cores_part(args):
    prefix, path, start, end, out_fa, out_tsv = args
    ids, n, letters = [], 0, 0
    with open(out_fa, "wb", buffering=CHUNK) as fa, \
            open(out_tsv, "wb", buffering=CHUNK) as tsv:
        for header, seq in iter_records(path, start, end):
            m = ID_RE.match(header)
            if not m or m.group(1).decode() != prefix:
                raise ValueError(f"unexpected core header {header[:80]!r} in {path}")
            pid = header[1:].split(b"|", 1)[0]
            # >LG1|C001|3.6e-26|58|299|C001 : component, hmm_evalue, start, end
            f = header[1:].split(b"|")
            fa.write(b">" + pid + b"\n" + seq + b"\n")
            tsv.write(b"\t".join([pid, f[1], f[2], f[3], f[4], str(len(seq)).encode()]) + b"\n")
            ids.append(int(m.group(2)))
            n += 1
            letters += len(seq)
    return prefix, np.array(ids, dtype=np.int64), n, letters


# ---- pass 2: proteins filtered by bitmap -----------------------------------

def proteins_part(args):
    prefix, path, start, end, bitmap_path, out_fa, out_tsv = args
    bitmap = np.load(bitmap_path, mmap_mode="r")
    kept = seen = letters = flag_mismatch = 0
    with open(out_fa, "wb", buffering=CHUNK) as fa, \
            open(out_tsv, "wb", buffering=CHUNK) as tsv:
        for header, seq in iter_records(path, start, end):
            seen += 1
            m = ID_RE.match(header)
            if not m or m.group(1).decode() != prefix:
                raise ValueError(f"unexpected protein header {header[:80]!r} in {path}")
            idx = int(m.group(2))
            inset = idx < len(bitmap) and bitmap[idx]
            # nr/Logan headers end in the catalytic-site flag (TRUE/FALSE); count
            # disagreements with the core set as a cross-check, don't filter on it.
            if prefix != "PL" and inset != header.endswith(b"|TRUE"):
                flag_mismatch += 1
            if not inset:
                continue
            pid = header[1:].split(b"|", 1)[0]
            fa.write(b">" + pid + b"\n" + seq + b"\n")
            # second field: Logan contig / nr accession / PAZy accession
            tsv.write(pid + b"\t" + header[1:].split(b"|")[1] + b"\t"
                      + str(len(seq)).encode() + b"\n")
            kept += 1
            letters += len(seq)
    return prefix, seen, kept, letters, flag_mismatch


def run_parts(pool, fn, jobs):
    return pool.map(fn, jobs, chunksize=1)


def concat(parts, dest):
    with open(dest, "wb") as out:
        for p in parts:
            with open(p, "rb") as f:
                while True:
                    b = f.read(CHUNK)
                    if not b:
                        break
                    out.write(b)
            os.unlink(p)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()
    out = args.out_dir
    tmp = out / "parts"
    tmp.mkdir(parents=True, exist_ok=True)

    stats = {}
    with Pool(args.workers) as pool:
        # pass 1: cores
        core_parts_fa, core_parts_tsv = [], []
        for prefix in ORDER:
            path = CORES[prefix]
            n = args.workers if path.stat().st_size > 1e9 else 1
            jobs = []
            for i, (s, e) in enumerate(byte_ranges(path, n)):
                fa, tsv = tmp / f"core_{prefix}_{i:03d}.fa", tmp / f"core_{prefix}_{i:03d}.tsv"
                jobs.append((prefix, path, s, e, fa, tsv))
                core_parts_fa.append(fa)
                core_parts_tsv.append(tsv)
            log(f"cores {prefix}: {len(jobs)} parts")
            res = run_parts(pool, cores_part, jobs)
            ids = np.concatenate([r[1] for r in res])
            n_rec = sum(r[2] for r in res)
            if len(np.unique(ids)) != n_rec:
                sys.exit(f"duplicate core IDs in {prefix}")
            if n_rec != EXPECTED_CORES[prefix]:
                sys.exit(f"{prefix} cores: {n_rec:,} != expected {EXPECTED_CORES[prefix]:,}")
            bitmap = np.zeros(ids.max() + 1, dtype=bool)
            bitmap[ids] = True
            np.save(tmp / f"bitmap_{prefix}.npy", bitmap)
            stats[prefix] = {"cores": n_rec, "core_letters": sum(r[3] for r in res)}
            log(f"cores {prefix}: {n_rec:,} records, {stats[prefix]['core_letters']:,} letters")
        concat(core_parts_fa, out / "PETadex.seq_catalytic_cores.v260701.fa")
        concat(core_parts_tsv, out / "PETadex.seq_catalytic_cores.v260701.ids.tsv")
        log("wrote cores FASTA")

        # pass 2: full-length proteins
        prot_parts_fa, prot_parts_tsv = [], []
        for prefix in ORDER:
            path = PROTEINS[prefix]
            n = args.workers * 4 if path.stat().st_size > 1e11 else (
                args.workers if path.stat().st_size > 1e9 else 1)
            jobs = []
            for i, (s, e) in enumerate(byte_ranges(path, n)):
                fa, tsv = tmp / f"prot_{prefix}_{i:03d}.fa", tmp / f"prot_{prefix}_{i:03d}.tsv"
                jobs.append((prefix, path, s, e, tmp / f"bitmap_{prefix}.npy", fa, tsv))
                prot_parts_fa.append(fa)
                prot_parts_tsv.append(tsv)
            log(f"proteins {prefix}: {len(jobs)} parts")
            res = run_parts(pool, proteins_part, jobs)
            st = stats[prefix]
            st["proteins_seen"] = sum(r[1] for r in res)
            st["full"] = sum(r[2] for r in res)
            st["full_letters"] = sum(r[3] for r in res)
            st["flag_mismatch"] = sum(r[4] for r in res)
            log(f"proteins {prefix}: seen {st['proteins_seen']:,}, kept {st['full']:,} "
                f"({st['full_letters']:,} letters), TRUE-flag mismatches {st['flag_mismatch']:,}")
            if st["full"] != st["cores"]:
                sys.exit(f"{prefix}: {st['full']:,} full-length != {st['cores']:,} cores "
                         "(core IDs missing from the proteins file)")
        concat(prot_parts_fa, out / "PETadex.seq.v260701.fa")
        concat(prot_parts_tsv, out / "PETadex.seq.v260701.ids.tsv")
        log("wrote full-length FASTA")

    with open(out / "assembly_stats.tsv", "w") as f:
        cols = ["cores", "core_letters", "proteins_seen", "full", "full_letters", "flag_mismatch"]
        f.write("source\t" + "\t".join(cols) + "\n")
        for p in ORDER:
            f.write(p + "\t" + "\t".join(str(stats[p][c]) for c in cols) + "\n")
        f.write("TOTAL\t" + "\t".join(str(sum(stats[p][c] for p in ORDER)) for c in cols) + "\n")
    subprocess.run(["cat", str(out / "assembly_stats.tsv")])
    log("DONE")


if __name__ == "__main__":
    main()
