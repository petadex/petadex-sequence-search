#!/usr/bin/env python3
"""
PETadex Sequence Search - local CLI

Searches one protein sequence against a LOCAL protein database with DIAMOND and
prints the result as JSON, in the same shape the production pipeline writes to
`results/{sessionId}/{jobId}.json` (minus the PETadex-specific metadata
enrichment, which needs the PETadex RDS database).

It runs the production code paths, not a re-implementation:
    common.parse_fasta / validate_sequence   query parsing + validation
    worker.run_shard_search                  the exact `diamond blastp` call
    aggregator.parse_part                    TSV parsing + the pident guard
so a local result ranks hits exactly as one shard of the live search does.

The database can be a native DIAMOND database (`diamond makedb --in x.fa -d x`
-> x.dmnd) or a plain protein FASTA (searched directly as a FASTA-as-DB).

Usage:
    python src/cli.py --db mydb.dmnd QUERY [--max-results 50] [-o out.json]

QUERY is a protein sequence, a FASTA string, or a path to a single-sequence
FASTA file. No AWS account is needed; DIAMOND must be on PATH.
"""

import argparse
import contextlib
import json
import os
import shutil
import sys

import common


def load_query(arg):
    """QUERY may be a file path, a FASTA string or a bare sequence."""
    if os.path.isfile(arg):
        with open(arg) as f:
            return f.read()
    return arg


def search(raw_query, db_path, max_results, sensitivity=None):
    """Run one search; return the result dict (production result shape)."""
    if sensitivity is not None:
        # worker reads its tuning from the environment at import time
        os.environ["DIAMOND_SENSITIVITY"] = sensitivity
    import worker      # noqa: E402  (after the env is set)
    import aggregator  # noqa: E402

    header, sequence = common.parse_fasta(raw_query)
    sequence = common.validate_sequence(sequence)
    # the worker logs its diamond command + timings to stdout (CloudWatch);
    # keep stdout clean for the JSON
    with contextlib.redirect_stdout(sys.stderr):
        tsv = worker.run_shard_search(f">query\n{sequence}\n", os.path.abspath(db_path),
                                      max_results)
    hits = aggregator.parse_part(tsv)
    hits.sort(key=lambda r: (-r["bitscore"], r["evalue"]))
    hits = hits[:max_results]
    for h in hits:
        h["metadata"] = None  # PETadex enrichment needs the RDS database
    return {
        "query_header": header,
        "query_sequence": sequence,
        "query_length": len(sequence),
        "num_results": len(hits),
        "results": hits,
        "result_depth": max_results,
        "engine": "diamond",
        "database": os.path.abspath(db_path),
        "database_release": None,
        "database_version": None,
        "search_version": common.SEARCH_VERSION,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Search a protein sequence against a local database with DIAMOND.")
    ap.add_argument("query", help="protein sequence, FASTA string, or single-sequence FASTA file")
    ap.add_argument("--db", required=True,
                    help="DIAMOND database (.dmnd) or protein FASTA to search against")
    ap.add_argument("--max-results", type=int, default=50, help="hits to return (default 50)")
    ap.add_argument("--sensitivity", default=None,
                    help="DIAMOND sensitivity flag, e.g. --very-sensitive "
                         "(default: DIAMOND's default mode, as in production)")
    ap.add_argument("-o", "--output", help="write JSON here instead of stdout")
    args = ap.parse_args(argv)

    if shutil.which("diamond") is None:
        sys.exit("error: `diamond` not found on PATH (see README > Installation)")
    if not os.path.exists(args.db):
        sys.exit(f"error: database not found: {args.db}")
    if not args.db.endswith((".dmnd", ".fa", ".fasta", ".fa.gz", ".fasta.gz",
                             ".fa.zst", ".fasta.zst")):
        sys.exit("error: --db must be a DIAMOND database (.dmnd) or a protein FASTA "
                 "ending in .fa / .fasta (optionally .gz or .zst)")
    if args.sensitivity is None:
        os.environ.setdefault("DIAMOND_SENSITIVITY", "default")

    try:
        result = search(load_query(args.query), args.db, args.max_results, args.sensitivity)
    except ValueError as e:  # invalid / multi-FASTA / too short or long
        sys.exit(f"error: {e}")

    text = json.dumps(result, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(text + "\n")
        print(f"{result['num_results']} hits -> {args.output}", file=sys.stderr)
    else:
        print(text)


if __name__ == "__main__":
    main()
