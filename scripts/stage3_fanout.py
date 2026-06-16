#!/usr/bin/env python3
"""
Stage 3 fan-out driver — doc "08 Compressed-FASTA Merge Dev Build Validation".

Replicates the production orchestrator + Step Functions Map + aggregate path
using ONLY `lambda:invoke`, because the dev/test box has no Step Functions or
IAM-write access (doc 08 Phase B: "Runbook-driven fan-out"). It is a BENCHMARK
HARNESS, never a production entry point.

What it does, for one A/B arm:
  1. Resolve the DB version + per-shard list from the manifest (same as the
     orchestrator's resolve_version/load_shards).
  2. Fan out one worker invoke per shard, in parallel (the Map), each writing
     results/{session}/{job}/parts/shard_{i}.tsv (+ .meta.json). Fail-fast: any
     worker FunctionError aborts the run (mirrors the ASL Map Catch).
  3. Invoke the REAL aggregator (production merge code) to read the parts, do the
     across-shard global top-k merge, and write results/{session}/{job}.json.
  4. Cross-check: independently re-merge the parts here and assert the set+order
     matches the aggregator's output — the across-shard-merge correctness signal
     the single-shard Gate-1/Gate-2 tests structurally cannot see.
  5. Print a human summary + a final one-line JSON (STAGE3_RESULT={...}) for the
     runbook / Phase D replicate loop to parse.

Two arms (override any field via flags):
  --arm zstd : 32-shard zstd-on-dev  -> petadex-diamond-worker-devtest (-b1 override)
  --arm dmnd : prod .dmnd LATEST      -> petadex-diamond-worker (what prod serves)
Both aggregate through the SAME devtest aggregator (format-agnostic merge) so the
two arms run on a byte-identical harness — only the worker + shard set differ.
"""

import argparse
import base64
import concurrent.futures as cf
import hashlib
import json
import re
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config as BotoConfig

REGION = "us-east-1"
S3_BUCKET = "petadex"
DIAMOND_PREFIX = "diamond"

# Per-arm defaults. version=None for dmnd ⇒ resolve diamond/LATEST at runtime.
# BOTH arms run through the devtest worker: it auto-detects .dmnd vs .fa.zst by the
# shard-key extension, and the -b1 FASTA override is a no-op on the .dmnd path. This
# lets us force-cold both arms via a config bump WITHOUT touching the prod worker
# (which the box can't reconfigure anyway). Caveat: arm A's .dmnd then runs on the
# dev-build binary rather than the prod 2.2.1 release image — functionally identical
# for .dmnd (the cross-block fix only affects the FASTA path), same flags/sensitivity.
ARMS = {
    "zstd": {"version": "catalytic_orfs_v1.1_20260610_173654",
             "worker": "petadex-diamond-worker-devtest"},
    "dmnd": {"version": None,
             "worker": "petadex-diamond-worker-devtest"},
}

# CloudWatch REPORT-line fields (from --log-type Tail). "Duration" needs lookbehinds
# so it doesn't match "Billed Duration"/"Init Duration".
_REPORT_RE = {
    "duration_ms": re.compile(r"(?<!Billed )(?<!Init )Duration: ([\d.]+) ms"),
    "billed_ms": re.compile(r"Billed Duration: (\d+) ms"),
    "mem_size_mb": re.compile(r"Memory Size: (\d+) MB"),
    "max_mem_mb": re.compile(r"Max Memory Used: (\d+) MB"),
    "init_ms": re.compile(r"Init Duration: ([\d.]+) ms"),
}


def parse_report(log_b64):
    """Decode an invoke's base64 LogResult and pull the REPORT metrics."""
    out = {}
    if not log_b64:
        return out
    try:
        txt = base64.b64decode(log_b64).decode("utf-8", "replace")
    except Exception:
        return out
    for k, rx in _REPORT_RE.items():
        m = rx.search(txt)
        if m:
            out[k] = float(m.group(1))
    out["cold"] = "init_ms" in out  # Init Duration present ⇒ cold start
    if out.get("mem_size_mb") and out.get("billed_ms"):
        out["gbs"] = round(out["mem_size_mb"] / 1024 * out["billed_ms"] / 1000, 2)
    return out


def force_cold(lam, worker_fn):
    """Recycle the worker's execution environments so the next invokes are COLD,
    by bumping a benign env var (COLD_NONCE). Updating config replaces existing
    containers. Devtest worker only — never the prod worker."""
    cfg = lam.get_function_configuration(FunctionName=worker_fn)
    env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
    env["COLD_NONCE"] = str(int(time.time() * 1000))
    lam.update_function_configuration(FunctionName=worker_fn,
                                      Environment={"Variables": env})
    lam.get_waiter("function_updated").wait(FunctionName=worker_fn)

# Lambda sync invokes can run the worker's full timeout (600s); give boto room.
_BOTO = BotoConfig(read_timeout=660, connect_timeout=10,
                   retries={"max_attempts": 0}, max_pool_connections=64)


def s3c():
    return boto3.client("s3", region_name=REGION, config=_BOTO)


def lc():
    return boto3.client("lambda", region_name=REGION, config=_BOTO)


def resolve_version(s3, arm_version):
    if arm_version:
        return arm_version.strip().rstrip("/")
    body = s3.get_object(Bucket=S3_BUCKET, Key=f"{DIAMOND_PREFIX}/LATEST")["Body"].read()
    v = body.decode().strip().rstrip("/")
    if not v:
        raise RuntimeError(f"{DIAMOND_PREFIX}/LATEST is empty")
    return v


def load_manifest(s3, version):
    key = f"{DIAMOND_PREFIX}/{version}/manifest.json"
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def build_shards(manifest):
    shards = [
        {"shardIndex": s["index"], "shardKey": s["key"],
         "shardSeqs": s.get("sequences"), "shardLetters": s.get("letters")}
        for s in manifest["shards"]
    ]
    if not shards:
        raise RuntimeError("manifest lists no shards")
    return shards


def extract_query(path, name):
    """Return (header, sequence) for the FASTA record named `name`."""
    header, seq, capture = None, [], False
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if capture:
                    break
                if line[1:].strip().split()[0] == name or line[1:].strip() == name \
                        or line.startswith(">" + name):
                    header, capture = line[1:].strip(), True
                continue
            if capture:
                seq.append(line.strip())
    if not header or not seq:
        raise RuntimeError(f"could not extract '{name}' from {path}")
    return header, "".join(seq)


def invoke_worker(lam, worker_fn, session_id, job_id, shard, query_fasta,
                  max_results, db_size):
    """One worker invoke (RequestResponse). Returns a result dict; never raises
    (errors are captured so the caller can fail-fast after the pool drains)."""
    event = {
        "sessionId": session_id, "jobId": job_id,
        "shardIndex": shard["shardIndex"], "shardKey": shard["shardKey"],
        "shardSeqs": shard["shardSeqs"], "shardLetters": shard["shardLetters"],
        "dbSize": db_size, "maxResults": max_results,
        "queryFasta": query_fasta,
    }
    t0 = time.monotonic()
    try:
        resp = lam.invoke(FunctionName=worker_fn, InvocationType="RequestResponse",
                          LogType="Tail", Payload=json.dumps(event).encode())
    except Exception as e:
        return {"shardIndex": shard["shardIndex"], "ok": False,
                "error": f"invoke exception: {e}", "wall_s": round(time.monotonic() - t0, 1)}
    wall = round(time.monotonic() - t0, 1)
    payload = resp["Payload"].read().decode()
    report = parse_report(resp.get("LogResult"))
    if resp.get("FunctionError"):
        return {"shardIndex": shard["shardIndex"], "ok": False,
                "error": f"FunctionError={resp['FunctionError']}: {payload[:600]}",
                "wall_s": wall, "report": report}
    try:
        body = json.loads(payload)
    except Exception:
        body = {"raw": payload[:600]}
    return {"shardIndex": shard["shardIndex"], "ok": True,
            "numHits": body.get("numHits"), "wall_s": wall, "report": report}


def read_parts(s3, session_id, job_id):
    """Read+parse every shard part TSV into rows — an INDEPENDENT re-merge, mirroring
    aggregator.read_parts so we can cross-check the aggregator's across-shard merge."""
    prefix = f"results/{session_id}/{job_id}/parts/"
    rows, n_parts = [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".tsv"):
                continue
            n_parts += 1
            body = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read().decode()
            for line in body.splitlines():
                if not line.strip():
                    continue
                p = line.split("\t")
                if len(p) < 9:
                    continue
                rows.append({"target_id": p[0], "bitscore": float(p[8]),
                             "evalue": float(p[7]), "pident": float(p[6])})
    return rows, n_parts


def independent_merge(rows, max_results):
    """Reference merge: same key as the aggregator (bitscore desc, evalue asc)."""
    rows = sorted(rows, key=lambda r: (-r["bitscore"], r["evalue"]))[:max_results]
    return rows


def hitset_sig(targets):
    """Order-independent signature of a hit set (for set-equality across runs)."""
    return hashlib.sha256("\n".join(sorted(targets)).encode()).hexdigest()[:16]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", choices=list(ARMS), required=True)
    ap.add_argument("--aggregator-fn", default="petadex-diamond-aggregator-devtest")
    ap.add_argument("--query-name", default="IsPETase")
    ap.add_argument("--seqs-file", default=None,
                    help="example-sequences.txt path (default: ../example-sequences.txt)")
    ap.add_argument("--max-results", type=int, default=50)
    ap.add_argument("--max-parallel", type=int, default=32)
    ap.add_argument("--worker", default=None, help="override the arm's worker function")
    ap.add_argument("--cold", action="store_true",
                    help="force cold containers (bump worker COLD_NONCE) before fan-out")
    args = ap.parse_args()

    import os
    seqs_file = args.seqs_file or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example-sequences.txt")

    arm = ARMS[args.arm]
    worker_fn = args.worker or arm["worker"]
    s3, lam = s3c(), lc()

    version = resolve_version(s3, arm["version"])
    manifest = load_manifest(s3, version)
    shards = build_shards(manifest)
    db_size = manifest.get("total_letters") or 0
    fmt = manifest.get("format") or "dmnd"
    header, sequence = extract_query(seqs_file, args.query_name)
    query_fasta = f">query\n{sequence}"

    session_id = f"stage3-{args.arm}-{int(time.time())}"
    job_id = str(uuid.uuid4())
    submitted_at = datetime.now(timezone.utc).isoformat()

    print(f"== Stage 3 fan-out :: arm={args.arm} ({fmt}){'  [COLD]' if args.cold else ''} ==")
    print(f"   version={version}  shards={len(shards)}  dbSize={db_size}")
    print(f"   worker={worker_fn}  aggregator={args.aggregator_fn}")
    print(f"   query={args.query_name} ({len(sequence)} aa)  session={session_id}  job={job_id}")

    if args.cold:
        print(f"   forcing cold containers on {worker_fn} ...")
        force_cold(lam, worker_fn)

    # ---- Map: parallel worker invokes (fail-fast) ----
    t_fan0 = time.monotonic()
    results = []
    with cf.ThreadPoolExecutor(max_workers=min(args.max_parallel, len(shards))) as ex:
        futs = [ex.submit(invoke_worker, lam, worker_fn, session_id, job_id,
                          sh, query_fasta, args.max_results, db_size) for sh in shards]
        for fu in cf.as_completed(futs):
            results.append(fu.result())
    fanout_s = round(time.monotonic() - t_fan0, 1)
    results.sort(key=lambda r: r["shardIndex"])

    failed = [r for r in results if not r["ok"]]
    worker_walls = [r["wall_s"] for r in results if r["ok"]]
    # Per-worker REPORT metrics (MaxMem, GB-s, cold).
    reports = [r.get("report", {}) for r in results if r["ok"]]
    mems = [rp["max_mem_mb"] for rp in reports if rp.get("max_mem_mb")]
    gbs = [rp["gbs"] for rp in reports if rp.get("gbs")]
    colds = [rp.get("cold") for rp in reports]
    n_cold = sum(1 for c in colds if c)
    worker_gbs_total = round(sum(gbs), 1) if gbs else None
    print(f"-- fan-out: {len(results) - len(failed)}/{len(shards)} workers ok "
          f"in {fanout_s}s wall (slowest worker {max(worker_walls) if worker_walls else 0}s); "
          f"cold {n_cold}/{len(reports)}; worker MaxMem max={max(mems) if mems else '?'}MB; "
          f"worker GB-s total={worker_gbs_total}")
    if failed:
        for r in failed[:5]:
            print(f"   !! shard {r['shardIndex']} FAILED: {r['error']}")
        print(f"FAIL: {len(failed)} worker(s) errored — fail-fast, not aggregating.")
        print("STAGE3_RESULT=" + json.dumps({"arm": args.arm, "status": "worker_failed",
              "failed_shards": [r["shardIndex"] for r in failed]}))
        sys.exit(3)

    # ---- Aggregate: real production merge code ----
    agg_event = {
        "sessionId": session_id, "jobId": job_id,
        "queryHeader": header, "querySequence": sequence,
        "maxResults": args.max_results, "shards": shards,
        "version": version, "submittedAt": submitted_at,
        "orchestratorTiming": {}, "dbSize": db_size,
        "corpus": manifest.get("corpus"),
        "dbSequenceCount": manifest.get("total_sequences"),
        "databaseRelease": manifest.get("database_release"),
        "searchVersion": "stage3-bench",
    }
    t_agg0 = time.monotonic()
    resp = lam.invoke(FunctionName=args.aggregator_fn, InvocationType="RequestResponse",
                      LogType="Tail", Payload=json.dumps(agg_event).encode())
    agg_s = round(time.monotonic() - t_agg0, 1)
    agg_payload = resp["Payload"].read().decode()
    agg_report = parse_report(resp.get("LogResult"))
    if resp.get("FunctionError"):
        print(f"FAIL: aggregator FunctionError={resp['FunctionError']}: {agg_payload[:800]}")
        print("STAGE3_RESULT=" + json.dumps({"arm": args.arm, "status": "aggregator_failed"}))
        sys.exit(4)
    print(f"-- aggregate: {agg_s}s (MaxMem={agg_report.get('max_mem_mb')}MB "
          f"GB-s={agg_report.get('gbs')})  -> {json.loads(agg_payload)}")

    # ---- Read merged result + independent cross-check ----
    result_key = f"results/{session_id}/{job_id}.json"
    result_doc = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=result_key)["Body"].read())
    agg_targets = [r["target_id"] for r in result_doc["results"]]

    rows, n_parts = read_parts(s3, session_id, job_id)
    ref = independent_merge(rows, args.max_results)
    ref_targets = [r["target_id"] for r in ref]

    set_match = sorted(agg_targets) == sorted(ref_targets)
    order_match = agg_targets == ref_targets

    # timing.json rollup (download/search split, slowest shard)
    timing = {}
    try:
        tk = f"results/{session_id}/{job_id}/timing.json"
        timing = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=tk)["Body"].read())
    except Exception as e:
        print(f"   (timing.json unreadable: {e})")

    dls = [s.get("download_ms") for s in timing.get("shards", []) if s.get("download_ms")]
    srch = [s.get("search_ms") for s in timing.get("shards", []) if s.get("search_ms")]

    print(f"-- merged result: {result_doc['num_results']} hits  (parts read: {n_parts})")
    print(f"   across-shard merge cross-check: set_match={set_match} order_match={order_match}")
    if not set_match:
        only_agg = sorted(set(agg_targets) - set(ref_targets))[:5]
        only_ref = sorted(set(ref_targets) - set(agg_targets))[:5]
        print(f"   !! MERGE MISMATCH  only_in_aggregator={only_agg}  only_in_refmerge={only_ref}")
    if dls:
        print(f"   T_download per-shard: max={max(dls):.0f}ms median={sorted(dls)[len(dls)//2]:.0f}ms")
    if srch:
        print(f"   T_search  per-shard: max={max(srch):.0f}ms median={sorted(srch)[len(srch)//2]:.0f}ms")
    print(f"   slowest_shard_ms={timing.get('slowest_shard_ms')}  total_wall_ms={timing.get('total_wall_ms')}")

    agg_gbs = agg_report.get("gbs")
    total_gbs = round((worker_gbs_total or 0) + (agg_gbs or 0), 1)
    summary = {
        "arm": args.arm, "status": "ok", "format": fmt, "version": version,
        "shards": len(shards), "num_results": result_doc["num_results"],
        "hitset_sig": hitset_sig(agg_targets),
        "merge_set_match": set_match, "merge_order_match": order_match,
        "cold_workers": n_cold, "n_workers": len(reports),
        "fanout_wall_s": fanout_s, "slowest_worker_s": max(worker_walls) if worker_walls else None,
        "aggregate_s": agg_s,
        "slowest_shard_ms": timing.get("slowest_shard_ms"),
        "total_wall_ms": timing.get("total_wall_ms"),
        "max_download_ms": max(dls) if dls else None,
        "max_search_ms": max(srch) if srch else None,
        "max_worker_mem_mb": max(mems) if mems else None,
        "agg_mem_mb": agg_report.get("max_mem_mb"),
        "worker_gbs": worker_gbs_total, "agg_gbs": agg_gbs, "total_gbs": total_gbs,
        "session_id": session_id, "job_id": job_id, "result_key": result_key,
    }
    print("STAGE3_RESULT=" + json.dumps(summary))
    # Correctness is the gate: a merge mismatch is a hard failure even if it ran.
    sys.exit(0 if set_match else 5)


if __name__ == "__main__":
    main()
