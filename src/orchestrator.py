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
    { "action": "version" }

The petadex.io backend also sends `clientIp` on search. Its presence marks a
public search, which must pass `check_admission` (rate and concurrency limits)
first; a refusal returns 429 (or 503 if the limits can't be checked) with
`{error, limit, retry_after}`. The backend invokes search synchronously so it
can relay that status.

NOTE `max_results` is still accepted but is now PRESENTATIONAL only — every job
searches to the fixed `common.RESULT_DEPTH` and the web app subsets at view
time, so changing a display count no longer re-runs the fan-out. See
common.RESULT_DEPTH.

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

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3

from common import (RESULT_DEPTH, SEARCH_VERSION, parse_database_release,
                    parse_fasta, validate_sequence)

S3_BUCKET = os.environ.get("S3_BUCKET", "petadex")
DIAMOND_PREFIX = "diamond"
# Set by Phase 6 infra. Required for the search action.
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

s3 = boto3.client("s3", region_name="us-east-1")
sfn = boto3.client("stepfunctions", region_name="us-east-1")

# Admission limits for PUBLIC searches (events carrying `clientIp`, i.e. relayed
# by the petadex.io backend). Every admitted search fans out 32 workers, so an
# unthrottled script could both run up Lambda spend and overrun the worker
# reserved concurrency, which under fail-fast fails every in-flight job. Direct
# IAM invokes (deploy example regen, benchmarks) carry no clientIp and skip this.
# Real traffic is a few searches a day (37 in the 90 days to 2026-10-26, peak 9
# in one hour), so these sit far above normal use.
MAX_RUNNING_JOBS = int(os.environ.get("MAX_RUNNING_JOBS", "4"))  # 4×32=128 of 200 reserved
MAX_JOBS_PER_IP_PER_HOUR = int(os.environ.get("MAX_JOBS_PER_IP_PER_HOUR", "10"))
MAX_JOBS_PER_HOUR = int(os.environ.get("MAX_JOBS_PER_HOUR", "30"))
MAX_JOBS_PER_DAY = int(os.environ.get("MAX_JOBS_PER_DAY", "100"))


class AdmissionDenied(Exception):
    """A public search refused by `check_admission`; maps to an HTTP status."""

    def __init__(self, status, limit, message, retry_after):
        super().__init__(message)
        self.status = status
        self.limit = limit
        self.retry_after = max(1, int(retry_after))


def client_tag(client_ip):
    """Pseudonymous 8-hex tag for a client IP, prefixed onto execution names.

    The execution history is the only per-client record, so no extra store
    (the backend's VPC reaches only S3, Lambda and RDS, and the DB user is
    read-only). A hash, not the raw IP, so names don't carry addresses.
    """
    return hashlib.sha256(f"petadex-search:{client_ip}".encode()).hexdigest()[:8]


def check_admission(tag):
    """Raise AdmissionDenied if a new public search would exceed a limit.

    One ListExecutions sweep over the last 24 h (newest first, so it stops at
    the cutoff) yields every count: running jobs, global hour/day starts, and
    this client's starts in the hour (execution names begin `{tag}_`). Failed
    and aborted executions count too: their workers still ran. Concurrent
    orchestrators can race past MAX_RUNNING_JOBS by a job or two, which the
    gap to the 200-worker reservation absorbs.
    """
    now = datetime.now(timezone.utc)
    hour_ago, day_ago = now - timedelta(hours=1), now - timedelta(days=1)
    running, hour_starts, day_starts, ip_starts = 0, [], [], []
    pages = sfn.get_paginator("list_executions").paginate(
        stateMachineArn=STATE_MACHINE_ARN, PaginationConfig={"PageSize": 1000})
    for page in pages:
        for ex in page["executions"]:
            started = ex["startDate"]
            if started < day_ago:
                break
            day_starts.append(started)
            running += ex["status"] == "RUNNING"
            if started >= hour_ago:
                hour_starts.append(started)
                if ex["name"].startswith(f"{tag}_"):
                    ip_starts.append(started)
        else:
            continue
        break

    def until_aged_out(starts, window, limit):
        # Seconds until the start that put the window at its limit leaves it.
        return (starts[limit - 1] + window - now).total_seconds()

    if running >= MAX_RUNNING_JOBS:
        raise AdmissionDenied(429, "running",
                              "PETadex search is busy right now. Please try again in a minute.", 30)
    if len(ip_starts) >= MAX_JOBS_PER_IP_PER_HOUR:
        raise AdmissionDenied(429, "client_hour",
                              f"Search limit reached ({MAX_JOBS_PER_IP_PER_HOUR} new searches "
                              "per hour). Previously run searches still load instantly.",
                              until_aged_out(ip_starts, timedelta(hours=1), MAX_JOBS_PER_IP_PER_HOUR))
    if len(hour_starts) >= MAX_JOBS_PER_HOUR:
        raise AdmissionDenied(429, "global_hour",
                              "PETadex search has reached its hourly capacity. Please try again later.",
                              until_aged_out(hour_starts, timedelta(hours=1), MAX_JOBS_PER_HOUR))
    if len(day_starts) >= MAX_JOBS_PER_DAY:
        raise AdmissionDenied(429, "global_day",
                              "PETadex search has reached its daily capacity. Please try again later.",
                              until_aged_out(day_starts, timedelta(days=1), MAX_JOBS_PER_DAY))


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
                 max_results, shards, db_meta, orch_timing=None, tag=None):
    """Start the Step Functions Map execution. Returns immediately (async).

    The execution input carries `querySequence`/`queryHeader` (not a prebuilt
    FASTA): the Map's ItemSelector formats each worker's `queryFasta` from
    `querySequence`, while the aggregator uses `queryHeader`/`querySequence` to
    write the unchanged result-JSON shape.

    `orch_timing` (optional): the orchestrator's own pre-start phase durations
    (parse/resolve/load), passed through untouched (the ASL preserves top-level
    fields) so the aggregator can fold them into timing.json.

    `tag` (optional): the client tag of a public search, prefixed onto the
    execution name so `check_admission` can count that client's jobs.
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
    # Public searches: `{tag}_{md5}-{uuid}` is 78 chars, inside the 80 cap.
    name = f"{tag}_{session_id}-{job_id}" if tag else f"{session_id}-{job_id}"
    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=name[:80],
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

    Also returns `result_depth` — the depth a fresh search would run to — which
    the app needs for the same decision: a cached result serves any display
    count up to its own stamped `result_depth` and is re-run only for a deeper
    one. `max_results` is NOT part of the cache key.

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
        # Depth a FRESH search would run to. The web app needs this BEFORE the
        # cache-hit decision: a cached result can serve any display count up to
        # its own stamped `result_depth`, and must be re-run only for a deeper
        # request. `max_results` is no longer part of the cache key.
        "result_depth": RESULT_DEPTH,
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
        # The caller's `max_results` is accepted (the request contract is
        # unchanged) but deliberately does NOT reach the search: every job runs
        # to the fixed RESULT_DEPTH and the web app subsets at view time. This
        # is what lets a display-count change be a free re-render instead of a
        # fresh 32-shard fan-out — see common.RESULT_DEPTH and the vault note
        # "12 Variable Result Count".
        requested_display = body.get("max_results")
        max_results = RESULT_DEPTH
        if requested_display is not None:
            print(f"max_results={requested_display} is presentational; "
                  f"searching to RESULT_DEPTH={RESULT_DEPTH}")

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

        # Public searches only (see MAX_RUNNING_JOBS). Fails closed: if the
        # limits can't be checked, the search doesn't start.
        tag = None
        admission_ms = None
        if body.get("clientIp") is not None:
            tag = client_tag(str(body["clientIp"])[:64])
            t1 = time.monotonic()
            try:
                check_admission(tag)
            except AdmissionDenied:
                raise
            except Exception as e:
                print(f"Admission check failed, refusing search: {e}")
                raise AdmissionDenied(503, "unavailable",
                                      "PETadex search is temporarily unavailable. Please try again shortly.",
                                      30)
            admission_ms = round((time.monotonic() - t1) * 1000, 1)

        job_id = str(uuid.uuid4())
        orch_timing = {
            "parse_ms": parse_ms,
            "resolve_version_ms": resolve_version_ms,
            "load_shards_ms": load_shards_ms,
            "admission_ms": admission_ms,
            "total_pre_start_ms": round((time.monotonic() - orch_t0) * 1000, 1),
        }

        t0 = time.time()
        start_search(session_id, job_id, version, query_header, query_sequence,
                     max_results, shards, db_meta, orch_timing, tag)
        print(f"TIMING start_execution: {time.time() - t0:.2f}s "
              f"(version={version}, shards={len(shards)}, jobId={job_id})")

        # Return immediately — the aggregator writes the result JSON.
        return _resp(200, {
            "job_id": job_id,
            "s3_key": f"results/{session_id}/{job_id}.json",
        })

    except AdmissionDenied as e:
        print(f"Admission denied ({e.limit}): {e}")
        return _resp(e.status, {"error": str(e), "limit": e.limit,
                                "retry_after": e.retry_after})
    except ValueError as e:
        print(f"Validation error: {e}")
        return _resp(400, {"error": str(e)})
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return _resp(500, {"error": "Internal server error"})
