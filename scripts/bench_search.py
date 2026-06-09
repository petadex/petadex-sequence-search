#!/usr/bin/env python3
"""Benchmark harness for the DIAMOND fan-out search (Search Optimization 07).

Invokes `petadex-diamond-orchestrator` N times, polls each job's `timing.json`,
and reports per-shard download/search ms + job wall time, plus the top-K
`target_id` hit set for the correctness gate. Saves a raw JSON artifact per
label so optimization deltas can be diffed offline.

Host python has no boto3, so everything shells out to the `aws` CLI. Run from
anywhere; sequence is read from ../example-sequences.txt.

Usage:
  python3 scripts/bench_search.py --label baseline --query FAST-PETase --runs 3
  python3 scripts/bench_search.py --label baseline --query FAST-PETase --runs 3 --sequential
  python3 scripts/bench_search.py --compare baseline s1_c1   # diff two saved labels
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

BUCKET = "petadex"
ORCH = "petadex-diamond-orchestrator"
HERE = os.path.dirname(os.path.abspath(__file__))
SEQ_FILE = os.path.join(HERE, "..", "example-sequences.txt")
OUT_DIR = os.path.join(HERE, "..", "docs", "bench")  # docs/ is gitignored


def aws(args, parse_json=True, check=True, timeout=60):
    """Run an aws CLI command. stdin is closed (harness backgrounding gotcha)."""
    proc = subprocess.run(
        ["aws", *args, "--no-cli-pager"],
        capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])}… failed:\n{proc.stderr}")
    out = proc.stdout.strip()
    if parse_json and out:
        return json.loads(out)
    return out


def load_sequence(name):
    seqs, cur, hdr = {}, [], None
    with open(SEQ_FILE) as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith(">"):
                if hdr:
                    seqs[hdr] = "".join(cur)
                hdr, cur = line[1:], []
            else:
                cur.append(line)
    if hdr:
        seqs[hdr] = "".join(cur)
    if name not in seqs:
        sys.exit(f"query {name!r} not in {SEQ_FILE}; have {list(seqs)}")
    return seqs[name]


def invoke(session_id, sequence, max_results):
    payload = json.dumps({
        "action": "search", "sessionId": session_id,
        "sequence": sequence, "max_results": max_results,
    })
    outfile = f"/tmp/bench_invoke_{session_id}.json"
    aws(["lambda", "invoke", "--function-name", ORCH,
         "--cli-binary-format", "raw-in-base64-out", "--payload", payload,
         "--cli-read-timeout", "40", "--cli-connect-timeout", "10", outfile],
        parse_json=False, timeout=60)
    with open(outfile) as f:
        resp = json.load(f)
    body = json.loads(resp["body"])
    if resp.get("statusCode") != 200:
        raise RuntimeError(f"orchestrator {resp.get('statusCode')}: {body}")
    return body["job_id"]


def s3_get_json(key):
    tmp = "/tmp/bench_" + key.replace("/", "_")
    proc = subprocess.run(
        ["aws", "s3api", "get-object", "--bucket", BUCKET, "--key", key, tmp,
         "--no-cli-pager"],
        capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60,
    )
    if proc.returncode != 0:
        return None
    with open(tmp) as f:
        return json.load(f)


def poll(session_id, job_id, timeout=720, interval=15):
    """Wait for timing.json (written by the aggregator after a successful job)."""
    tkey = f"results/{session_id}/{job_id}/timing.json"
    deadline = time.time() + timeout
    while time.time() < deadline:
        doc = s3_get_json(tkey)
        if doc is not None:
            return doc
        time.sleep(interval)
    return None


def target_ids(session_id, job_id):
    res = s3_get_json(f"results/{session_id}/{job_id}.json")
    if not res:
        return None, res
    return [r.get("target_id") for r in res.get("results", [])], res


def summarize_run(timing):
    """Pull per-shard download/search arrays + job-level fields from timing.json."""
    dl, se, tot = [], [], []
    for s in timing.get("shards", []):
        if s.get("status") != "success":
            continue
        if s.get("download_ms") is not None:
            dl.append(s["download_ms"])
        if s.get("search_ms") is not None:
            se.append(s["search_ms"])
        if s.get("total_ms") is not None:
            tot.append(s["total_ms"])
    return {
        "total_wall_ms": timing.get("total_wall_ms"),
        "slowest_shard_ms": timing.get("slowest_shard_ms"),
        "spread_ms": timing.get("fastest_slowest_spread_ms"),
        "shards_completed": timing.get("shards_completed"),
        "shards_expected": timing.get("shards_expected"),
        "download_ms": dl,
        "search_ms": se,
        "shard_total_ms": tot,
    }


def stat(xs):
    if not xs:
        return None
    return {
        "n": len(xs), "mean": round(statistics.mean(xs), 1),
        "median": round(statistics.median(xs), 1),
        "min": round(min(xs), 1), "max": round(max(xs), 1),
        "stdev": round(statistics.pstdev(xs), 1) if len(xs) > 1 else 0.0,
    }


def run_benchmark(args):
    seq = load_sequence(args.query)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    runs = []
    print(f"== bench '{args.label}' | query={args.query} ({len(seq)} aa) | "
          f"runs={args.runs} | {'sequential' if args.sequential else 'concurrent'} ==")

    if args.sequential:
        plan = [None] * args.runs  # invoke just-in-time
    else:
        # Fire all invokes first, then poll — concurrent cold burst.
        plan = []
        for i in range(args.runs):
            sid = f"bench_{args.label}_{stamp}_{i}"
            jid = invoke(sid, seq, args.max_results)
            print(f"  [{i}] invoked  session={sid} job={jid}")
            plan.append((sid, jid))

    for i in range(args.runs):
        if args.sequential:
            sid = f"bench_{args.label}_{stamp}_{i}"
            jid = invoke(sid, seq, args.max_results)
            print(f"  [{i}] invoked  session={sid} job={jid}")
        else:
            sid, jid = plan[i]
        t0 = time.time()
        timing = poll(sid, jid)
        if timing is None:
            print(f"  [{i}] TIMEOUT — no timing.json after poll window")
            runs.append({"session": sid, "job": jid, "status": "timeout"})
            continue
        s = summarize_run(timing)
        tids, _ = target_ids(sid, jid)
        s.update({"session": sid, "job": jid, "status": timing.get("status"),
                  "poll_wait_s": round(time.time() - t0, 1), "target_ids": tids})
        runs.append(s)
        print(f"  [{i}] done {s['status']} | wall={s['total_wall_ms']}ms "
              f"slowest={s['slowest_shard_ms']}ms "
              f"dl(mean)={stat(s['download_ms'])['mean'] if s['download_ms'] else '-'}ms "
              f"search(mean)={stat(s['search_ms'])['mean'] if s['search_ms'] else '-'}ms "
              f"hits={len(tids) if tids else 0}")

    # Aggregate across runs.
    ok = [r for r in runs if r.get("status") == "success"]
    all_dl = [x for r in ok for x in r["download_ms"]]
    all_se = [x for r in ok for x in r["search_ms"]]
    walls = [r["total_wall_ms"] for r in ok if r["total_wall_ms"] is not None]
    slowest = [r["slowest_shard_ms"] for r in ok if r["slowest_shard_ms"] is not None]
    # Correctness: identical top-K across runs?
    hitsets = [tuple(r["target_ids"]) for r in ok if r["target_ids"] is not None]
    identical = len(set(hitsets)) <= 1 if hitsets else None

    summary = {
        "label": args.label, "query": args.query, "seq_len": len(seq),
        "max_results": args.max_results, "mode": "sequential" if args.sequential else "concurrent",
        "timestamp": stamp, "runs_ok": len(ok), "runs_total": len(runs),
        "download_ms": stat(all_dl), "search_ms": stat(all_se),
        "total_wall_ms": stat(walls), "slowest_shard_ms": stat(slowest),
        "topk_identical_across_runs": identical,
        "reference_hitset": list(hitsets[0]) if hitsets else None,
        "raw_runs": runs,
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{args.label}.json")
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n--- SUMMARY ---")
    for k in ("download_ms", "search_ms", "slowest_shard_ms", "total_wall_ms"):
        print(f"  {k:18s} {summary[k]}")
    print(f"  top-{args.max_results} identical across runs: {identical}")
    print(f"  saved -> {os.path.relpath(path)}")
    return summary


def compare(a, b):
    pa = os.path.join(OUT_DIR, f"{a}.json")
    pb = os.path.join(OUT_DIR, f"{b}.json")
    A, B = json.load(open(pa)), json.load(open(pb))

    def m(s, k):
        return (s.get(k) or {}).get("mean")
    print(f"== {a}  ->  {b} ==")
    for k in ("download_ms", "search_ms", "slowest_shard_ms", "total_wall_ms"):
        va, vb = m(A, k), m(B, k)
        if va is None or vb is None:
            print(f"  {k:18s} {va} -> {vb}")
            continue
        d = vb - va
        pct = (d / va * 100) if va else 0
        print(f"  {k:18s} {va:>9.1f} -> {vb:>9.1f}   Δ {d:+9.1f}ms ({pct:+.1f}%)")
    ha = set(A.get("reference_hitset") or [])
    hb = set(B.get("reference_hitset") or [])
    if ha and hb:
        inter = len(ha & hb)
        print(f"  hitset overlap: {inter}/{len(ha)} (baseline) | "
              f"identical={ha == hb} added={len(hb - ha)} dropped={len(ha - hb)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="baseline")
    ap.add_argument("--query", default="FAST-PETase")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--max-results", type=int, default=50)
    ap.add_argument("--sequential", action="store_true",
                    help="invoke+wait one at a time (default: fire all, then poll)")
    ap.add_argument("--compare", nargs=2, metavar=("A", "B"))
    args = ap.parse_args()
    if args.compare:
        compare(*args.compare)
    else:
        run_benchmark(args)


if __name__ == "__main__":
    main()
