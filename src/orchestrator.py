#!/usr/bin/env python3
"""
PETadex DIAMOND Orchestrator Lambda — Phase 3 of the scale-out plan.

The single entry point for the sharded DIAMOND search. It validates the query
ONCE, resolves the database version, reads the shard manifest, mints a jobId,
and kicks off a Step Functions execution that fans out one worker per shard
(Phase 4 Map) and aggregates. It returns `{ job_id, s3_key }` immediately and
NEVER block-waits — the web-app contract is unchanged.

Query-contract helpers come from `common.py`.

Event (web-app contract, unchanged):
    { "action": "search", "sessionId": "...", "sequence": "...", "max_results": 50 }
    { "action": "history", "sessionId": "..." }
    { "action": "version" }
    { "action": "samples", "id": "LG113721682" }   -> "Found in Samples" for one
        Logan sequence (metadata.samples; SAMPLES_STORE). Stateless, like version.

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
A public search whose sessionId is already running attaches to that job
instead of starting another (in-flight dedup, see `check_admission`); the
response then names the running job and adds `"deduped": true`.

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

import align
import metadata
from common import (EXPORT_DEPTH, RESULT_DEPTH, SEARCH_VERSION,
                    export_session_id, parse_database_release, parse_fasta,
                    validate_sequence)

S3_BUCKET = os.environ.get("S3_BUCKET", "petadex")
DIAMOND_PREFIX = "diamond"
# Set by Phase 6 infra. Required for the search action.
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

s3 = boto3.client("s3", region_name="us-east-1")
sfn = boto3.client("stepfunctions", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
# The alignment + tree Lambda (src/align.py). Unset = the `align` action answers 503.
ALIGN_FUNCTION = os.environ.get("ALIGN_FUNCTION")

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


def execution_job(name, session_id):
    """Return the jobId if execution `name` is a search of `session_id`, else None.

    Names are `{session}-{jobId}` or, for public searches, `{tag}_{session}-{jobId}`
    (see start_search); jobId is a 36-char uuid4.
    """
    job_id, prefix = name[-36:], name[:-37]
    if name[-37:-36] != "-":
        return None
    if prefix == session_id or (prefix[8:9] == "_" and prefix[9:] == session_id):
        return job_id
    return None


def check_admission(tag, session_id):
    """Admit a public search: return a running jobId to attach to, or None.

    In-flight dedup: if a RUNNING execution already searches this sessionId
    (the web app's cache key, so same sequence + database + search version),
    its jobId is returned and nothing new should start. This is checked before
    the limits because attaching costs nothing, so it is never refused. The
    caller polls by session, so the existing job's result (or failure) serves
    both requests. Two requests within the ListExecutions lag (~1 s) can still
    both start.

    Otherwise raise AdmissionDenied if a new search would exceed a limit.
    One ListExecutions sweep over the last 24 h (newest first, so it stops at
    the cutoff) yields every count: running jobs, global hour/day starts, and
    this client's starts in the hour (execution names begin `{tag}_`). Failed
    and aborted executions count too: their workers still ran. The check and
    the start aren't atomic; the orchestrator's reserved concurrency of 2
    bounds the race to one extra job, which the gap to the 200-worker
    reservation absorbs.
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
            if ex["status"] == "RUNNING":
                running += 1
                inflight = execution_job(ex["name"], session_id)
                if inflight:
                    return inflight
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


def find_export(export_sid):
    """Return the response for a finished export under `export_sid`, or None.

    A finished export is a `results/{export_sid}/{jobId}.json` summary whose
    `hits.tsv.gz` AND `hits.fasta.gz` both exist. Exports are kept like search
    results (see aggregator handle_export); the check still matters because a
    format-1.0 export has no FASTA and is rebuilt, and exports made before
    2026-10-27 were tagged to expire. Listing only: the orchestrator role can list the bucket but not
    read results, so the caller (the petadex.io backend) reads the summary and
    presigns the downloads.
    """
    prefix = f"results/{export_sid}/"
    page = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")
    done = sorted((o for o in page.get("Contents", []) if o["Key"].endswith(".json")),
                  key=lambda o: o["LastModified"], reverse=True)
    for obj in done:
        job_id = obj["Key"][len(prefix):-len(".json")]
        hits_key = f"{prefix}{job_id}/hits.tsv.gz"
        fasta_key = f"{prefix}{job_id}/hits.fasta.gz"
        found = {o["Key"] for o in s3.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=f"{prefix}{job_id}/hits.").get("Contents", [])}
        if {hits_key, fasta_key} <= found:
            return {"status": "ready", "job_id": job_id, "s3_key": obj["Key"],
                    "hits_key": hits_key, "fasta_key": fasta_key,
                    "export_session_id": export_sid}
    return None


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


BATCH_RESPONSE_MAX = 5_000_000


def align_action(body, session_id):
    """Alignment + tree for a finished search: {sessionId, jobId?, ids?, poll?}.

    The default alignment is started by the aggregator after every search; this
    returns it (status none | running | ready | failed; `afa` + `newick` when
    ready). With `ids` (<= align.MAX_HITS hits of that result) it is a custom
    alignment, started here on first request. A missing or failed one is
    (re)started unless `poll` is true. jobId defaults to the session's latest job.
    200 any status · 400 bad ids · 404 no such result · 503 not configured.
    """
    try:
        ids = align.validate_ids(body.get("ids"))
    except ValueError as e:
        return _resp(400, {"error": str(e)})
    job_id = body.get("jobId")
    if not job_id:
        try:
            job_id = s3.get_object(Bucket=S3_BUCKET, Key=f"results/{session_id}.index")["Body"].read().decode().strip()
        except s3.exceptions.NoSuchKey:
            return _resp(404, {"status": "none", "error": "no search result for this sessionId"})
    result_key = f"results/{session_id}/{job_id}.json"
    key = align.key_for(ids)
    doc = align.read_status(session_id, job_id, key)
    if doc and doc["status"] == "ready":
        afa, nwk = align.read_outputs(session_id, job_id, key)
        return _resp(200, {**doc, "afa": afa, "newick": nwk})
    if doc and doc["status"] == "running":
        return _resp(200, doc)
    if body.get("poll"):
        return _resp(200, doc or {"status": "none", "session_id": session_id, "job_id": job_id, "key": key})
    if not ALIGN_FUNCTION:
        return _resp(503, {"status": "unavailable", "error": "alignment is not configured"})
    try:
        result = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=result_key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return _resp(404, {"status": "none", "error": "no such search result"})
    try:
        n = len(align.select(result, ids))
    except ValueError as e:
        return _resp(400, {"error": str(e)})
    # Mark it running before the async start, so a second poll doesn't start it twice.
    running = {"status": "running", "session_id": session_id, "job_id": job_id, "key": key,
               "started_at": align._now(), "n_sequences": n}
    align._put_status(session_id, job_id, key, running)
    lam.invoke(FunctionName=ALIGN_FUNCTION, InvocationType="Event",
               Payload=json.dumps({"sessionId": session_id, "jobId": job_id, "ids": ids}))
    return _resp(200, running)


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

        # Stateless per-sequence lookup for the sequence page ("Found in Samples");
        # no search, no sessionId. The lookup's status maps to the HTTP status the
        # petadex.io route relays: ok 200, not_found 404 (a Logan id with no linked
        # BioSample), not_applicable / malformed id 400, unavailable 503 (no store).
        # With `ids` (a list, up to 500) it is the batch form: ok 200, malformed 400, unavailable 503.
        if action == "samples" and "ids" in body:
            try:
                res = metadata.samples_batch(body.get("ids"), limit=body.get("limit"))
            except ValueError as e:
                return _resp(400, {"error": str(e)})
            if len(json.dumps(res)) > BATCH_RESPONSE_MAX:   # Lambda's sync response cap is 6 MB
                return _resp(400, {"error": "response too large; lower `limit` or send fewer ids"})
            return _resp(200 if res["status"] == "ok" else 503, res)
        if action == "samples":
            try:
                res = metadata.samples(body.get("id"))
            except ValueError as e:
                return _resp(400, {"error": str(e)})
            code = {"ok": 200, "not_found": 404, "not_applicable": 400}.get(res["status"], 503)
            return _resp(code, res)

        session_id = body.get("sessionId")
        if not session_id:
            raise ValueError("sessionId is required")

        if action == "history":
            return _resp(200, {"history": get_history(session_id)})

        if action == "align":
            return align_action(body, session_id)

        # --- export: every significant hit, on demand (common.EXPORT_DEPTH) ---
        # Same request as a search (sessionId = the search's cache key). A
        # finished export is returned as-is; otherwise the search path below runs
        # uncapped under the export's own sessionId, with the same validation,
        # admission limits and in-flight dedup.
        is_export = action == "export"
        if is_export:
            session_id = export_session_id(session_id)
            ready = find_export(session_id)
            if ready:
                print(f"Export cache hit: {ready['job_id']} (sessionId={session_id})")
                return _resp(200, ready)

        # --- search ---
        raw_input = body.get("sequence", "").strip()
        # The caller's `max_results` is accepted (the request contract is
        # unchanged) but deliberately does NOT reach the search: every job runs
        # to the fixed RESULT_DEPTH and the web app subsets at view time. This
        # is what lets a display-count change be a free re-render instead of a
        # fresh 32-shard fan-out — see common.RESULT_DEPTH and the vault note
        # "12 Variable Result Count".
        requested_display = body.get("max_results")
        max_results = EXPORT_DEPTH if is_export else RESULT_DEPTH
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
                inflight = check_admission(tag, session_id)
            except AdmissionDenied:
                raise
            except Exception as e:
                print(f"Admission check failed, refusing search: {e}")
                raise AdmissionDenied(503, "unavailable",
                                      "PETadex search is temporarily unavailable. Please try again shortly.",
                                      30)
            admission_ms = round((time.monotonic() - t1) * 1000, 1)
            if inflight:
                print(f"Deduped onto running job {inflight} (sessionId={session_id})")
                return _resp(200, {
                    "job_id": inflight,
                    "s3_key": f"results/{session_id}/{inflight}.json",
                    "deduped": True,
                    **({"status": "running", "export_session_id": session_id}
                       if is_export else {}),
                })

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
            **({"status": "running", "export_session_id": session_id}
               if is_export else {}),
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
