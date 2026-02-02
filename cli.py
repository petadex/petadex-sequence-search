#!/usr/bin/env python3
"""
PETadex Sequence Search - CLI Mode

Standalone command-line interface for MMseqs2 protein sequence search
against 217M+ plastic-degrading enzyme sequences.

Usage:
    python cli.py <sequence> [max_results]
"""

import sys
import json
import os
from lambda_function import validate_sequence, download_database

def run_search_cli(query_sequence, db_path, max_results=50):
    """
    Run MMseqs2 search and return results directly (no S3 upload)
    """
    import subprocess

    # Write query to temp file
    query_file = "/tmp/query.fasta"
    with open(query_file, 'w') as f:
        f.write(f">query\n{query_sequence}\n")

    # Output file
    result_file = "/tmp/results.tsv"

    # Run MMseqs2 easy-search
    print("Running MMseqs2 search...", file=sys.stderr)
    result = subprocess.run([
        "mmseqs", "easy-search",
        query_file,
        db_path,
        result_file,
        "/tmp",
        "--format-output", "target,qstart,qend,tstart,tend,alnlen,fident,evalue,bits",
        "--max-seqs", str(max_results)
    ], capture_output=True, text=True)

    if result.returncode != 0:
        print(f"MMseqs2 error: {result.stderr}", file=sys.stderr)
        raise Exception(f"MMseqs2 search failed: {result.stderr}")

    print("Search complete", file=sys.stderr)

    # Parse results
    results = []
    if os.path.exists(result_file):
        with open(result_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 9:
                    results.append({
                        'target_id': parts[0],
                        'query_start': int(parts[1]),
                        'query_end': int(parts[2]),
                        'target_start': int(parts[3]),
                        'target_end': int(parts[4]),
                        'alignment_length': int(parts[5]),
                        'percent_identity': float(parts[6]) * 100,
                        'evalue': float(parts[7]),
                        'bitscore': float(parts[8])
                    })

    return {
        'query_length': len(query_sequence),
        'num_results': len(results),
        'results': results
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python cli.py <sequence> [max_results]", file=sys.stderr)
        print("\nExample:", file=sys.stderr)
        print('  python cli.py "MKLLIVLLAACLAVFAAAEPQIAVV" 10', file=sys.stderr)
        sys.exit(1)

    try:
        # Parse arguments
        sequence = validate_sequence(sys.argv[1])
        max_results = int(sys.argv[2]) if len(sys.argv) > 2 else 50

        # Download database (cached after first run)
        db_path = download_database()

        # Run search locally (no S3 upload in CLI mode)
        results = run_search_cli(sequence, db_path, max_results)

        # Output results as JSON to stdout
        print(json.dumps(results, indent=2))

    except ValueError as e:
        print(f"Validation error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
