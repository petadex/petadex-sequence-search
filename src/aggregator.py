#!/usr/bin/env python3
"""
PETadex DIAMOND Aggregator Lambda — Phase 4 of the scale-out plan.

The final state of the Step Functions Map execution. It runs ONLY after every
shard branch has succeeded (fail-fast: any worker failure makes the Map Catch
abort the execution before this state), so all `parts/shard_*.tsv` are present.

It merges the per-shard partial TSVs, ranks globally, truncates to the job's
search depth (`maxResults` in the execution input = common.RESULT_DEPTH),
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

import gzip
import heapq
import io
import json
import os
import shutil
import time
from datetime import datetime, timezone

import boto3

from common import (EXPORT_COLUMNS, EXPORT_DEPTH, EXPORT_EVALUE_BANDS,
                    EXPORT_FORMAT_VERSION, export_rank_key, hit_origin)

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


def parse_part(text):
    """Parse one shard part (DIAMOND outfmt-6 TSV text) into result dicts.

    DIAMOND outfmt-6 column order (set by the worker):
        sseqid qstart qend sstart send length pident evalue bitscore full_sseq

    `full_sseq` (column 10) is the FULL target protein sequence, appended last by
    the worker so this parser stays backward-compatible: a part written by an
    older worker has only 9 columns and yields `target_sequence: None` rather
    than failing the merge (which, under fail-fast, would fail the whole job).

    Pure (no S3), so the local CLI parses its output with exactly this code.
    """
    results = []
    for line in text.splitlines():
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
            # Full target amino-acid sequence (DIAMOND `full_sseq`).
            # None for parts written by a pre-full_sseq worker.
            "target_sequence": p[9] if len(p) > 9 else None,
        })
    return results


def read_parts(session_id, job_id):
    """Read + parse every shard part TSV under the job's parts/ prefix."""
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
            results.extend(parse_part(body.decode()))
    return results, n_parts


def read_export_parts(session_id, job_id):
    """Download every export part (`parts/shard_*.tsv.gz`) into memory.

    Parts are the worker's gzipped, rank-sorted rows (common.EXPORT_OUTFMT);
    the largest measured family is ~4 MB per part compressed. Returns the list
    of compressed bodies (one per shard) for `merge_export`.
    """
    prefix = f"results/{session_id}/{job_id}/parts/"
    paginator = s3.get_paginator("list_objects_v2")
    bodies = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".tsv.gz"):
                bodies.append(s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read())
    return bodies


def _export_rows(body):
    """Yield the split rows of one gzipped export part."""
    with gzip.open(io.BytesIO(body), "rt") as f:
        for line in f:
            if line.strip():
                yield line.rstrip("\n").split("\t")


def merge_export(bodies, out_path):
    """Stream-merge rank-sorted export parts into one gzipped TSV of ranked rows.

    Each part is already sorted by common.export_rank_key, so a k-way heap merge
    yields the global order without holding the hit set in memory. Writes the
    data rows (EXPORT_COLUMNS) to `out_path`, header excluded — it needs the
    total, which is only known at the end. Returns (total, counts) where counts
    has `by_evalue` (hits at or below each band) and `by_origin`.
    """
    by_evalue = {band: 0 for band in EXPORT_EVALUE_BANDS}
    by_origin = {}
    total = 0
    merged = heapq.merge(*(_export_rows(b) for b in bodies), key=export_rank_key)
    with gzip.open(out_path, "wt", compresslevel=6) as out:
        for f in merged:
            sseqid, qstart, qend, sstart, send, length, pident, evalue, bitscore, slen = f
            total += 1
            origin = hit_origin(sseqid)
            by_origin[origin] = by_origin.get(origin, 0) + 1
            ev = float(evalue)
            for band in EXPORT_EVALUE_BANDS:
                if ev <= band:
                    by_evalue[band] += 1
            out.write("\t".join((str(total), sseqid, origin, pident, length, qstart,
                                 qend, sstart, send, slen, evalue, bitscore)) + "\n")
    return total, {"by_evalue": {f"{b:g}": n for b, n in by_evalue.items()},
                   "by_origin": by_origin}


def export_header(event, total):
    """Commented header for the export TSV: provenance, then the column row."""
    lines = [
        "PETadex sequence search export (all significant hits)",
        f"export_format: {EXPORT_FORMAT_VERSION}",
        f"query: {event.get('queryHeader') or 'query'} ({len(event['querySequence'])} aa)",
        f"database_release: {event.get('databaseRelease')}  "
        f"database_version: {event.get('version')}",
        f"search_version: {event.get('searchVersion')}  engine: diamond  "
        f"evalue_cutoff: 1e-3  dbsize: {event.get('dbSize')}",
        f"total_hits: {total}  generated: {datetime.now(timezone.utc).isoformat()}",
        "ranked like the search: bitscore desc, then evalue asc",
    ]
    return "".join(f"# {l}\n" for l in lines) + "\t".join(EXPORT_COLUMNS) + "\n"


def handle_export(event):
    """Export mode (maxResults == EXPORT_DEPTH): merge the 32 uncapped parts.

    Writes, under the export's own sessionId (common.export_session_id):
      results/{sessionId}/{jobId}/hits.tsv.gz   every hit, ranked, header first
      results/{sessionId}/{jobId}.json          summary: counts + the file key
      results/{sessionId}.index                 → jobId, like a search
    The header is its own gzip member in front of the data member (a valid
    multi-member gzip, read transparently by gzip/zcat/pandas/R), so the rows
    stream to disk once without knowing the total in advance. All three objects
    are tagged reapable: an export is a 14-day cache, rebuilt on demand after.
    """
    session_id = event["sessionId"]
    job_id = event["jobId"]
    expected_parts = len(event["shards"]) if "shards" in event else None
    phase_ms = {}

    t0 = time.monotonic()
    bodies = read_export_parts(session_id, job_id)
    phase_ms["read_parts_ms"] = round((time.monotonic() - t0) * 1000, 1)
    if expected_parts is not None and len(bodies) != expected_parts:
        raise RuntimeError(f"expected {expected_parts} export parts, found {len(bodies)}")

    t0 = time.monotonic()
    data_path, out_path = "/tmp/export_rows.tsv.gz", "/tmp/export_hits.tsv.gz"
    total, counts = merge_export(bodies, data_path)
    del bodies
    with open(out_path, "wb") as out:
        out.write(gzip.compress(export_header(event, total).encode()))
        with open(data_path, "rb") as data:
            shutil.copyfileobj(data, out, 16 * 1024 * 1024)
    os.remove(data_path)
    phase_ms["merge_ms"] = round((time.monotonic() - t0) * 1000, 1)

    t0 = time.monotonic()
    hits_key = f"results/{session_id}/{job_id}/hits.tsv.gz"
    hits_bytes = os.path.getsize(out_path)
    with open(out_path, "rb") as f:
        s3.put_object(Bucket=S3_BUCKET, Key=hits_key, Body=f,
                      ContentType="application/gzip",
                      ContentDisposition='attachment; filename="petadex_hits.tsv.gz"',
                      Tagging="reapable=true")
    os.remove(out_path)
    summary = {
        "kind": "export",
        "status": "complete",
        "export_format": EXPORT_FORMAT_VERSION,
        "query_header": event.get("queryHeader"),
        "query_length": len(event["querySequence"]),
        "database": event.get("corpus"),
        "database_release": event.get("databaseRelease"),
        "database_version": event.get("version"),
        "search_version": event.get("searchVersion"),
        "evalue_cutoff": 1e-3,
        "total_hits": total,
        **counts,
        "files": {"hits_tsv_gz": {"key": hits_key, "bytes": hits_bytes,
                                  "columns": EXPORT_COLUMNS}},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    result_key = f"results/{session_id}/{job_id}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=result_key, Body=json.dumps(summary),
                  ContentType="application/json", Tagging="reapable=true")
    s3.put_object(Bucket=S3_BUCKET, Key=f"results/{session_id}.index",
                  Body=job_id, ContentType="text/plain", Tagging="reapable=true")
    phase_ms["write_result_ms"] = round((time.monotonic() - t0) * 1000, 1)
    phase_ms["total_ms"] = round(sum(phase_ms.values()), 1)
    print(f"export: {total} hits, {hits_bytes} bytes -> s3://{S3_BUCKET}/{hits_key} "
          f"({phase_ms})")

    write_job_timing(session_id, job_id, event.get("version"),
                     expected_parts or 0, event.get("submittedAt"),
                     status="success", aggregator_timing=phase_ms,
                     orchestrator_timing=event.get("orchestratorTiming"))
    return {"sessionId": session_id, "jobId": job_id, "s3_key": result_key,
            "num_results": total}


def read_shard_metas(session_id, job_id):
    """Read every per-shard timing sidecar (shard_*.meta.json) under parts/.

    Tolerant by design: a sidecar that is missing or unparseable is skipped
    here, not raised on — the rollup in `write_job_timing` decides per expected
    shard whether to record it as `missing`. Returns {shard_index: meta_dict}.
    """
    prefix = f"results/{session_id}/{job_id}/parts/"
    paginator = s3.get_paginator("list_objects_v2")
    metas = {}
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".meta.json"):
                continue
            try:
                body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
                meta = json.loads(body)
                metas[int(meta["shard_index"])] = meta
            except Exception as e:
                print(f"WARN unreadable timing sidecar {obj['Key']}: {e}")
    return metas


def write_job_timing(session_id, job_id, version, shard_count,
                     job_submitted_at, status, aggregator_timing=None,
                     orchestrator_timing=None):
    """Roll the per-shard timing sidecars up into one job-level timing.json.

    Written one level ABOVE parts/ (`results/{sessionId}/{jobId}/timing.json`)
    and, on the success path, AFTER the result JSON so it stays off the
    user-facing critical path. Like the worker sidecar, this logs and swallows
    any error — timing telemetry must never fail (or, on the failure path,
    re-fail) a job.

    `aggregator_timing` (success path only): a dict of the aggregator's own
    post-Map phase durations in ms — `read_parts_ms`, `sort_ms`, `metadata_ms`,
    `write_result_ms`, `total_ms`. This is what isolates how much of a job's
    tail is metadata enrichment vs. part-merge vs. ranking. Absent (None) on the
    timing-only failure path, where no aggregation ran.

    `orchestrator_timing` (both paths): the orchestrator's own pre-start phase
    durations in ms — `parse_ms`, `resolve_version_ms`, `load_shards_ms`,
    `total_pre_start_ms` — threaded through the execution input. This isolates
    the orchestrator's contribution to job latency, which `total_wall_ms`
    otherwise hides (the wall starts at `submittedAt`, stamped after this work).

    Two invocation sites:
      - success: the normal Aggregate state, after the result is written.
      - failure: the Step Functions failure state (timing-only mode), where the
        normal Aggregate never ran. It rolls up whatever sidecars exist and
        marks any absent shard `missing` rather than failing the rollup.
    """
    try:
        metas = read_shard_metas(session_id, job_id)

        shards = []
        completed_total_ms = []
        for i in range(shard_count):
            m = metas.get(i)
            if m is None:
                # Worker died before writing a sidecar (or never ran) — record a
                # breadcrumb-shaped placeholder instead of dropping the shard.
                shards.append({"shard_index": i, "status": "missing"})
                continue
            shards.append(m)
            if m.get("status") == "success" and m.get("total_ms") is not None:
                completed_total_ms.append(m["total_ms"])

        completed_at = datetime.now(timezone.utc)
        total_wall_ms = None
        if job_submitted_at:
            try:
                submitted = datetime.fromisoformat(job_submitted_at)
                total_wall_ms = round(
                    (completed_at - submitted).total_seconds() * 1000, 1)
            except ValueError as e:
                print(f"WARN bad submittedAt {job_submitted_at!r}: {e}")

        slowest = max(completed_total_ms) if completed_total_ms else None
        spread = (max(completed_total_ms) - min(completed_total_ms)
                  if completed_total_ms else None)

        doc = {
            "job_id": job_id,
            "session_id": session_id,
            "version": version,
            "status": status,
            "submitted_at": job_submitted_at,
            "completed_at": completed_at.isoformat(),
            "total_wall_ms": total_wall_ms,
            "shard_count": shard_count,
            "shards_expected": shard_count,
            "shards_completed": len(completed_total_ms),
            "slowest_shard_ms": slowest,
            "fastest_slowest_spread_ms": spread,
            "orchestrator": orchestrator_timing,
            "aggregator": aggregator_timing,
            "shards": sorted(shards, key=lambda s: s["shard_index"]),
        }
        key = f"results/{session_id}/{job_id}/timing.json"
        # reapable=true: diagnostic sidecar, never re-read by the web app; expired
        # by the tag-scoped lifecycle rule. The result {jobId}.json + .index below
        # stay UNtagged so the rule can never reap a live result.
        # Requires s3:PutObjectTagging in the aggregator IAM.
        s3.put_object(Bucket=S3_BUCKET, Key=key,
                      Body=json.dumps(doc).encode(),
                      ContentType="application/json",
                      Tagging="reapable=true")
        print(f"wrote job timing ({status}, {len(completed_total_ms)}/{shard_count} "
              f"shards) -> s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"WARN write_job_timing failed: {e}")


def handler(event, context):
    print(f"Aggregator event: sessionId={event.get('sessionId')} "
          f"jobId={event.get('jobId')}")
    session_id = event["sessionId"]
    job_id = event["jobId"]

    # Failure path (invoked from the Step Functions failure state): the normal
    # Aggregate never ran, so there is no result to write — just roll up whatever
    # per-shard sidecars exist and mark the job failed. No RDS round-trip.
    if event.get("mode") == "timing-only":
        shard_count = len(event["shards"]) if "shards" in event else 0
        write_job_timing(session_id, job_id, event.get("version"), shard_count,
                         event.get("submittedAt"), status="failed",
                         orchestrator_timing=event.get("orchestratorTiming"))
        return {"sessionId": session_id, "jobId": job_id, "status": "failed"}

    if int(event.get("maxResults", 50)) == EXPORT_DEPTH:
        return handle_export(event)

    query_header = event.get("queryHeader")
    query_sequence = event["querySequence"]
    max_results = int(event.get("maxResults", 50))
    # Optional cross-check: number of shards we expected to report a part.
    expected_parts = len(event["shards"]) if "shards" in event else None

    # Phase timing: isolates how much of the post-Map tail is part-merge vs.
    # ranking vs. metadata enrichment vs. the result write. Surfaced in
    # timing.json under `aggregator` (never-raise — see write_job_timing).
    phase_ms = {}

    t0 = time.monotonic()
    results, n_parts = read_parts(session_id, job_id)
    phase_ms["read_parts_ms"] = round((time.monotonic() - t0) * 1000, 1)
    print(f"read {len(results)} hits from {n_parts} parts in "
          f"{phase_ms['read_parts_ms']:.0f}ms")
    if expected_parts is not None and n_parts != expected_parts:
        # Fail-fast should make this impossible; loudly flag if it ever happens.
        raise RuntimeError(f"expected {expected_parts} parts, found {n_parts}")

    # Global rank: bitscore desc, tiebreak evalue asc; then top-K. Targets are
    # unique across shards (corpus is partitioned), so no dedup is needed.
    t0 = time.monotonic()
    results.sort(key=lambda r: (-r["bitscore"], r["evalue"]))
    results = results[:max_results]
    phase_ms["sort_ms"] = round((time.monotonic() - t0) * 1000, 1)

    # Enrich once from RDS (see METADATA CAVEAT). Timed so we can see exactly how
    # much of the tail this round-trip costs — including a slow/failed RDS
    # connect, which would otherwise be invisible.
    t0 = time.monotonic()
    accession_ids = {r["target_id"] for r in results}
    metadata = fetch_metadata(accession_ids)
    for r in results:
        r["metadata"] = metadata.get(r["target_id"])
    phase_ms["metadata_ms"] = round((time.monotonic() - t0) * 1000, 1)

    result_doc = {
        "query_header": query_header,
        "query_sequence": query_sequence,
        "query_length": len(query_sequence),
        "num_results": len(results),
        # Depth this job SEARCHED to (not the caller's display count, which no
        # longer reaches the search). Stamped from the value actually used, so
        # a result stays honest about itself if the constant changes later.
        # The web app serves any display count <= this from cache and re-runs
        # only for a deeper request; `num_results < result_depth` means the
        # e-value floor was reached first, i.e. the search was exhaustive and
        # every display count is servable. See common.RESULT_DEPTH.
        "result_depth": max_results,
        # Corpus identity (additive fields; the web app ignores unknown keys).
        # These make a result self-identifying as the DIAMOND/Logan 300M path —
        # `engine: diamond`, the corpus FASTA, the version, and the full DB seq
        # count — vs the legacy MMseqs2/nr 1M path. See lambda_function.py for
        # the matching fields on the legacy side.
        "engine": "diamond",
        "database": event.get("corpus"),
        # Two version axes label every search (see README "Versioning & cache
        # invalidation"): `database_release` is the human-facing corpus version
        # (v1.1); `database_version` is the precise timestamped build tag that
        # pins the exact build. `search_version` is the search-pipeline semver
        # (bumps on engine/sensitivity/scoring changes). The web app's cache key
        # should fold in `database_version` + `search_version`.
        "database_release": event.get("databaseRelease"),
        "database_version": event.get("version"),
        "search_version": event.get("searchVersion"),
        "db_sequence_count": event.get("dbSequenceCount"),
        "results": results,
    }
    t0 = time.monotonic()
    result_key = f"results/{session_id}/{job_id}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=result_key,
                  Body=json.dumps(result_doc), ContentType="application/json")
    # Index pointer the web app polls (sessionId -> latest jobId), unchanged.
    s3.put_object(Bucket=S3_BUCKET, Key=f"results/{session_id}.index",
                  Body=job_id, ContentType="text/plain")
    phase_ms["write_result_ms"] = round((time.monotonic() - t0) * 1000, 1)
    print(f"wrote {len(results)} results -> s3://{S3_BUCKET}/{result_key}")

    phase_ms["total_ms"] = round(sum(phase_ms.values()), 1)

    # Roll the per-shard timing sidecars into job-level timing.json. AFTER the
    # result write (off the critical path) and never raises — see write_job_timing.
    shard_count = expected_parts if expected_parts is not None else n_parts
    write_job_timing(session_id, job_id, event.get("version"), shard_count,
                     event.get("submittedAt"), status="success",
                     aggregator_timing=phase_ms,
                     orchestrator_timing=event.get("orchestratorTiming"))

    return {"sessionId": session_id, "jobId": job_id,
            "s3_key": result_key, "num_results": len(results)}
