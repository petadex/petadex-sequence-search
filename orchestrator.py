#!/usr/bin/env python3
"""
PETadex DIAMOND Orchestrator Lambda — Phase 3 of the scale-out plan.

The single entry point for the sharded DIAMOND search. It validates the query
ONCE, resolves the database version, reads the shard manifest, mints a jobId,
and kicks off a Step Functions execution that fans out one worker per shard
(Phase 4 Map) and aggregates. It returns `{ job_id, s3_key }` immediately and
NEVER block-waits — the web-app contract is unchanged.

Self-contained on purpose: it does NOT import `lambda_function.py` (that module
eager-downloads the 3.2 GB MMseqs2 DB at import). Query-contract helpers come
from `common.py`.

Event (web-app contract, unchanged):
    { "action": "search", "sessionId": "...", "sequence": "...", "max_results": 50 }
    { "action": "history", "sessionId": "..." }

Search response (unchanged):
    { "job_id": "<uuid>", "s3_key": "results/{sessionId}/{jobId}.json" }

Coordination — Step Functions Map (locked, Section 5). The orchestrator starts
an execution with this input; the Map's ItemSelector merges each `shards[*]`
entry with the top-level fields to build each worker's event
(`{ sessionId, jobId, shardIndex, shardKey, queryFasta, maxResults }`), and the
final aggregator state writes `results/{sessionId}/{jobId}.json` + `.index`:

    {
      "sessionId": "...", "jobId": "...", "version": "...",
      "queryFasta": ">query\\n<SEQ>", "maxResults": 50,
      "shards": [ { "shardIndex": 0, "shardKey": "diamond/{version}/shard_00.dmnd" }, ... ]
    }

Version pinning: `LATEST` is read ONCE here and the resolved version flows into
the execution input, so every worker pins to it even if `LATEST` flips mid-job.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3

from common import SEARCH_VERSION, parse_database_release, parse_fasta, validate_sequence

S3_BUCKET = os.environ.get("S3_BUCKET", "petadex")
DIAMOND_PREFIX = "diamond"
# Set by Phase 6 infra. Required for the search action.
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

s3 = boto3.client("s3", region_name="us-east-1")
sfn = boto3.client("stepfunctions", region_name="us-east-1")


def resolve_version(requested=None):
    """Return the DIAMOND DB version to search.

    `requested` (optional, for pinning/testing) wins; otherwise read
    `diamond/LATEST` exactly once per job.
    """
    if requested:
        return requested.strip().rstrip("/")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{DIAMOND_PREFIX}/LATEST")
    version = obj["Body"].read().decode("utf-8").strip().rstrip("/")
    if not version:
        raise RuntimeError(f"{DIAMOND_PREFIX}/LATEST is empty")
    return version


def load_shards(version):
    """Read the version's manifest.json; return (shards, db_meta).

    db_meta carries the corpus identity (source FASTA + sequence count) so the
    aggregator can stamp it into the result — that is what lets a reader tell a
    300M-Logan search from the 1M-nr legacy path without inspecting hit IDs.
    """
    key = f"{DIAMOND_PREFIX}/{version}/manifest.json"
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    manifest = json.loads(obj["Body"].read())
    shards = [
        # shardSeqs (per-shard sequence count) rides along for the timing
        # telemetry's shard-count benchmark; the worker records it. shardLetters
        # (per-shard residue count) lets a FASTA-as-DB worker size its single
        # reference block to the shard so finer shards fit Lambda memory; it is
        # None for manifests built before the builder recorded per-shard letters
        # (the worker then falls back to its env default). Unused on the .dmnd path.
        {"shardIndex": s["index"], "shardKey": s["key"],
         "shardSeqs": s.get("sequences"), "shardLetters": s.get("letters")}
        for s in manifest["shards"]
    ]
    if not shards:
        raise RuntimeError(f"manifest {key} lists no shards")
    db_meta = {
        "corpus": manifest.get("corpus"),
        "dbSequenceCount": manifest.get("total_sequences"),
        # Total corpus residues for --dbsize e-value calibration. None until the
        # manifest is (re)built or backfilled with total_letters; workers then
        # omit --dbsize (legacy per-shard behavior). See docs/evalue-calibration.md.
        "dbLetters": manifest.get("total_letters"),
        # Semantic corpus release (e.g. "v1.1"), used to LABEL the search. Prefer
        # the manifest's explicit field; fall back to parsing the corpus path so
        # a manifest built before that field existed still resolves a clean
        # release without a rebuild. NOTE this is the human-facing release, not
        # the precise build tag (`version`) — the cache key should use `version`.
        "databaseRelease": (manifest.get("database_release")
                            or parse_database_release(manifest.get("corpus"))),
    }
    return shards, db_meta


def start_search(session_id, job_id, version, query_header, query_sequence,
                 max_results, shards, db_meta, orch_timing=None):
    """Start the Step Functions Map execution. Returns immediately (async).

    The execution input carries `querySequence`/`queryHeader` (not a prebuilt
    FASTA): the Map's ItemSelector formats each worker's `queryFasta` from
    `querySequence`, while the aggregator uses `queryHeader`/`querySequence` to
    write the unchanged result-JSON shape.

    `orch_timing` (optional): the orchestrator's own pre-start phase durations
    (parse/resolve/load), passed through untouched (the ASL preserves top-level
    fields) so the aggregator can fold them into timing.json.
    """
    if not STATE_MACHINE_ARN:
        raise RuntimeError("STATE_MACHINE_ARN is not configured")
    payload = {
        "sessionId": session_id,
        "jobId": job_id,
        "version": version,
        # Job start stamp, threaded through so the aggregator can compute
        # total_wall_ms (it has no other way to know when the job began).
        "submittedAt": datetime.now(timezone.utc).isoformat(),
        # Orchestrator self-timing, recorded by the aggregator in timing.json.
        "orchestratorTiming": orch_timing or {},
        "queryHeader": query_header,
        "querySequence": query_sequence,
        "maxResults": max_results,
        "shards": shards,
        # Corpus identity, stamped into the result by the aggregator so a search
        # is self-identifying as Logan (300M) vs the legacy nr (1M) path.
        "corpus": db_meta.get("corpus"),
        "dbSequenceCount": db_meta.get("dbSequenceCount"),
        # Version labels stamped into the result + used by the web app's cache
        # key: `databaseRelease` (v1.1, human-facing) and `searchVersion` (the
        # search-pipeline semver). `version` (the precise build tag) is already
        # in the payload above. See README "Versioning & cache invalidation".
        "databaseRelease": db_meta.get("databaseRelease"),
        "searchVersion": SEARCH_VERSION,
        # Effective DB size (residues) so each worker calibrates e-values against
        # the full corpus, not its single shard. 0 ⇒ workers omit --dbsize
        # (unchanged legacy behavior until the manifest carries total_letters).
        "dbSize": db_meta.get("dbLetters") or 0,
    }
    # Name the execution after the job for traceability + idempotency (a retried
    # invoke with the same jobId collides instead of double-fanning-out).
    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=f"{session_id}-{job_id}"[:80],
        input=json.dumps(payload),
    )


def get_versions(requested=None):
    """Resolve the version identifiers a FRESH search would run with.

    Single source of truth for the web app's cache key: it needs
    `database_version` + `search_version` to compute its sessionId, but
    `search_version` is a code constant (not in S3) and `database_version` is
    `diamond/LATEST` — so the app can't self-serve both. This stateless action
    returns them (read it before reusing a cached sessionId; see the README
    "Versioning & cache invalidation" section).

    Cheap: one `LATEST` GET + one `manifest.json` GET (the app should cache the
    response briefly rather than calling per request). `requested` pins a version
    for testing, mirroring `resolve_version`.
    """
    version = resolve_version(requested)
    _, db_meta = load_shards(version)
    return {
        "database_version": version,                       # precise build tag
        "database_release": db_meta.get("databaseRelease"),  # human label (v1.1)
        "search_version": SEARCH_VERSION,                  # search-pipeline semver
    }


def get_history(session_id):
    """List completed searches for a session.

    NOTE: the sharded layout adds intermediate keys under
    `results/{sessionId}/{jobId}/parts/shard_*.tsv`. History must report only
    the final per-job result objects `results/{sessionId}/{jobId}.json` and
    ignore those `parts/` children (the legacy lister would have mis-reported
    each part as a job).
    """
    prefix = f"results/{session_id}/"
    paginator = s3.get_paginator("list_objects_v2")
    history = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rest = key[len(prefix):]
            # Keep only direct children that are "<jobId>.json" — no nested path.
            if "/" in rest or not rest.endswith(".json"):
                continue
            history.append({
                "job_id": rest[: -len(".json")],
                "s3_key": key,
                "last_modified": obj["LastModified"].isoformat(),
                "size": obj["Size"],
            })
    history.sort(key=lambda x: x["last_modified"], reverse=True)
    return history


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def handler(event, context):
    try:
        print(f"Event: {json.dumps(event)}")

        body = json.loads(event["body"]) if isinstance(event.get("body"), str) else event
        action = body.get("action", "search")

        # Stateless version lookup — deliberately BEFORE the sessionId check: the
        # web app calls this to learn database_version + search_version so it can
        # BUILD a version-aware cache-key sessionId, so requiring one here would
        # be circular. See get_versions / README "Versioning & cache invalidation".
        if action == "version":
            return _resp(200, get_versions(body.get("version")))

        session_id = body.get("sessionId")
        if not session_id:
            raise ValueError("sessionId is required")

        if action == "history":
            return _resp(200, {"history": get_history(session_id)})

        # --- search ---
        raw_input = body.get("sequence", "").strip()
        max_results = int(body.get("max_results", 50))

        # Measure the orchestrator's own phases and thread them into the execution
        # input so the aggregator records them in timing.json (symmetric to its
        # own `aggregator` block). All work below is pre-start_execution, so the
        # numbers are known before the payload is sent; the start_execution API
        # call's own latency can't be self-included (it stays in CloudWatch).
        orch_t0 = time.monotonic()

        # Validate ONCE here; workers receive the cleaned query and never re-check.
        query_header, query_sequence = parse_fasta(raw_input)
        query_sequence = validate_sequence(query_sequence)
        parse_ms = round((time.monotonic() - orch_t0) * 1000, 1)

        # Pin the version once, read the shard list, fan out via Step Functions.
        t1 = time.monotonic()
        version = resolve_version(body.get("version"))
        resolve_version_ms = round((time.monotonic() - t1) * 1000, 1)

        t1 = time.monotonic()
        shards, db_meta = load_shards(version)
        load_shards_ms = round((time.monotonic() - t1) * 1000, 1)

        job_id = str(uuid.uuid4())
        orch_timing = {
            "parse_ms": parse_ms,
            "resolve_version_ms": resolve_version_ms,
            "load_shards_ms": load_shards_ms,
            "total_pre_start_ms": round((time.monotonic() - orch_t0) * 1000, 1),
        }

        t0 = time.time()
        start_search(session_id, job_id, version, query_header, query_sequence,
                     max_results, shards, db_meta, orch_timing)
        print(f"TIMING start_execution: {time.time() - t0:.2f}s "
              f"(version={version}, shards={len(shards)}, jobId={job_id})")

        # Return immediately — the aggregator writes the result JSON.
        return _resp(200, {
            "job_id": job_id,
            "s3_key": f"results/{session_id}/{job_id}.json",
        })

    except ValueError as e:
        print(f"Validation error: {e}")
        return _resp(400, {"error": str(e)})
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return _resp(500, {"error": "Internal server error"})
