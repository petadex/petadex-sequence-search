#!/usr/bin/env python3
"""
PETadex DIAMOND Aggregator Lambda — Phase 4 of the scale-out plan.

The final state of the Step Functions Map execution. It runs ONLY after every
shard branch has succeeded (fail-fast: any worker failure makes the Map Catch
abort the execution before this state), so all `parts/shard_*.tsv` are present.

It merges the per-shard partial TSVs, ranks globally, truncates to maxResults,
enriches from RDS once, and writes the result in the UNCHANGED shape the web app
already consumes — `results/{sessionId}/{jobId}.json` + `results/{sessionId}.index`.

Self-contained (no `lambda_function` import — eager MMseqs2 download). It is the
only DIAMOND-path component that touches RDS, so `fetch_metadata` lives here.

Input (the execution state after the Map; see orchestrator.py):
    { "sessionId", "jobId", "queryHeader", "querySequence", "maxResults", ... }

pident GUARD (Phase 0 Check 8): DIAMOND's `pident` column is already 0–100. The
legacy MMseqs2 path does `fident*100`; that must NOT be re-applied here. We
assert `0 ≤ pident ≤ 100` per row — the designated place to catch the
silent-corruption bug.

METADATA CAVEAT: `fetch_metadata` joins `blast_nr_metadata.genbank_accession_id`,
but full-Logan-corpus target IDs are ORF IDs (e.g. `114593962||ERR1748848|...`),
not GenBank accessions — so enrichment will usually return `null` for the Logan
corpus. The contract already permits `metadata: null`; the round-trip is kept
per the plan. A real Logan-corpus metadata source is a separate, future concern.
"""

import json
import os
import time

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "petadex")
DB_HOST = os.environ.get("DB_HOST", "petadex.ccz9y6yshbls.us-east-1.rds.amazonaws.com")
DB_NAME = os.environ.get("DB_NAME", "petadex")

s3 = boto3.client("s3", region_name="us-east-1")
_db_secret_cache = None


def get_db_credentials():
    global _db_secret_cache
    if _db_secret_cache:
        return _db_secret_cache
    sm = boto3.client("secretsmanager", region_name="us-east-1")
    secret = json.loads(sm.get_secret_value(
        SecretId=os.environ["DB_SECRET_ARN"])["SecretString"])
    _db_secret_cache = secret
    return secret


def fetch_metadata(accession_ids):
    """Fetch blast_nr_metadata rows for a list of genbank_accession_ids."""
    if not accession_ids:
        return {}
    import psycopg2
    import psycopg2.extras
    creds = get_db_credentials()
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=creds["username"], password=creds["password"],
        connect_timeout=5,
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT genbank_accession_id, organism, protein_id, definition,
                          taxonomy, journal, collection_date, country
                   FROM blast_nr_metadata
                   WHERE genbank_accession_id = ANY(%s)""",
                (list(accession_ids),),
            )
            return {row["genbank_accession_id"]: dict(row) for row in cur.fetchall()}
    finally:
        conn.close()


def read_parts(session_id, job_id):
    """Read + parse every shard part TSV into result dicts.

    DIAMOND outfmt-6 column order (set by the worker):
        sseqid qstart qend sstart send length pident evalue bitscore
    """
    prefix = f"results/{session_id}/{job_id}/parts/"
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    n_parts = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".tsv"):
                continue
            n_parts += 1
            body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            for line in body.decode().splitlines():
                if not line.strip():
                    continue
                p = line.split("\t")
                if len(p) < 9:
                    continue
                pident = float(p[6])
                # Silent-corruption guard: DIAMOND emits 0–100, never 0–1.
                # Do NOT multiply by 100 (that is the MMseqs2 `fident` path).
                assert 0.0 <= pident <= 100.0, f"pident {pident} out of [0,100]: {line!r}"
                results.append({
                    "target_id": p[0],
                    "query_start": int(p[1]),
                    "query_end": int(p[2]),
                    "target_start": int(p[3]),
                    "target_end": int(p[4]),
                    "alignment_length": int(p[5]),
                    "percent_identity": pident,
                    "evalue": float(p[7]),
                    "bitscore": float(p[8]),
                })
    return results, n_parts


def handler(event, context):
    print(f"Aggregator event: sessionId={event.get('sessionId')} "
          f"jobId={event.get('jobId')}")
    session_id = event["sessionId"]
    job_id = event["jobId"]
    query_header = event.get("queryHeader")
    query_sequence = event["querySequence"]
    max_results = int(event.get("maxResults", 50))
    # Optional cross-check: number of shards we expected to report a part.
    expected_parts = len(event["shards"]) if "shards" in event else None

    t0 = time.time()
    results, n_parts = read_parts(session_id, job_id)
    print(f"read {len(results)} hits from {n_parts} parts in {time.time()-t0:.2f}s")
    if expected_parts is not None and n_parts != expected_parts:
        # Fail-fast should make this impossible; loudly flag if it ever happens.
        raise RuntimeError(f"expected {expected_parts} parts, found {n_parts}")

    # Global rank: bitscore desc, tiebreak evalue asc; then top-K. Targets are
    # unique across shards (corpus is partitioned), so no dedup is needed.
    results.sort(key=lambda r: (-r["bitscore"], r["evalue"]))
    results = results[:max_results]

    # Enrich once from RDS (see METADATA CAVEAT).
    accession_ids = {r["target_id"] for r in results}
    metadata = fetch_metadata(accession_ids)
    for r in results:
        r["metadata"] = metadata.get(r["target_id"])

    result_doc = {
        "query_header": query_header,
        "query_sequence": query_sequence,
        "query_length": len(query_sequence),
        "num_results": len(results),
        "results": results,
    }
    result_key = f"results/{session_id}/{job_id}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=result_key,
                  Body=json.dumps(result_doc), ContentType="application/json")
    # Index pointer the web app polls (sessionId -> latest jobId), unchanged.
    s3.put_object(Bucket=S3_BUCKET, Key=f"results/{session_id}.index",
                  Body=job_id, ContentType="text/plain")
    print(f"wrote {len(results)} results -> s3://{S3_BUCKET}/{result_key}")

    return {"sessionId": session_id, "jobId": job_id,
            "s3_key": result_key, "num_results": len(results)}
