#!/usr/bin/env python3
"""
Stage 3 e2e A/B driver — doc "08 Compressed-FASTA Merge Dev Build Validation",
Phase D. Runs N COLD fan-outs per arm in randomized order via stage3_fanout.py,
then reports median + IQR per metric (latency is heavy-tailed → replicates, not
one-shot). Each run is a full 32/20-shard fan-out + real aggregate.

  python3 stage3_ab.py --runs-per-arm 5

Arms (both through the devtest worker; see stage3_fanout.ARMS):
  zstd : 32-shard zstd-on-dev (-b1)      dmnd : 20-shard .dmnd LATEST baseline
"""

import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FANOUT = os.path.join(HERE, "stage3_fanout.py")

# (key, label, unit, scale) — scale divides the raw value for display.
METRICS = [
    ("total_wall_ms", "T_e2e (job wall)", "s", 1000),
    ("max_download_ms", "T_download (slowest shard)", "s", 1000),
    ("max_search_ms", "T_search (slowest shard)", "s", 1000),
    ("slowest_shard_ms", "slowest shard total", "s", 1000),
    ("aggregate_s", "aggregate", "s", 1),
    ("max_worker_mem_mb", "worker MaxMem (peak)", "MB", 1),
    ("total_gbs", "cost (GB-s/job)", "GB-s", 1),
]


def run_once(arm, aggregator_fn, timeout):
    """One cold fan-out; returns the parsed STAGE3_RESULT dict (or an error dict)."""
    cmd = [sys.executable, FANOUT, "--arm", arm, "--cold", "--aggregator-fn", aggregator_fn]
    t0 = time.monotonic()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"arm": arm, "status": "timeout"}
    wall = round(time.monotonic() - t0, 1)
    summary = None
    for line in proc.stdout.splitlines():
        if line.startswith("STAGE3_RESULT="):
            summary = json.loads(line[len("STAGE3_RESULT="):])
    if summary is None:
        tail = (proc.stdout + proc.stderr).splitlines()[-3:]
        return {"arm": arm, "status": "no_result", "rc": proc.returncode, "tail": tail}
    summary["driver_wall_s"] = wall
    return summary


def med_iqr(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    med = statistics.median(vals)
    if len(vals) >= 4:
        q1, _, q3 = statistics.quantiles(vals, n=4, method="inclusive")
    else:
        q1, q3 = min(vals), max(vals)
    return med, q1, q3


def fmt_metric(summaries, key, scale):
    mi = med_iqr([s.get(key) for s in summaries])
    if mi is None:
        return "—"
    med, q1, q3 = mi
    return f"{med/scale:.1f} (IQR {q1/scale:.1f}–{q3/scale:.1f})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs-per-arm", type=int, default=5)
    ap.add_argument("--aggregator-fn", default="petadex-diamond-aggregator-devtest")
    ap.add_argument("--timeout", type=int, default=400, help="per-run timeout (s)")
    ap.add_argument("--out", default=os.path.join(HERE, "stage3_ab_results.json"))
    args = ap.parse_args()

    plan = (["zstd"] * args.runs_per_arm) + (["dmnd"] * args.runs_per_arm)
    random.shuffle(plan)
    print(f"== Stage 3 A/B :: {args.runs_per_arm} cold runs/arm, randomized ==")
    print(f"   order: {' '.join(plan)}\n")

    runs = []
    for i, arm in enumerate(plan, 1):
        print(f"[{i}/{len(plan)}] arm={arm} ...", flush=True)
        r = run_once(arm, args.aggregator_fn, args.timeout)
        runs.append(r)
        if r.get("status") != "ok":
            print(f"   !! run status={r.get('status')} {r}")
            continue
        print(f"   ok: e2e={ (r.get('total_wall_ms') or 0)/1000:.1f}s "
              f"dl={(r.get('max_download_ms') or 0)/1000:.1f}s "
              f"search={(r.get('max_search_ms') or 0)/1000:.1f}s "
              f"mem={r.get('max_worker_mem_mb')}MB GB-s={r.get('total_gbs')} "
              f"cold={r.get('cold_workers')}/{r.get('n_workers')} "
              f"hits={r.get('num_results')} merge_ok={r.get('merge_set_match')} "
              f"sig={r.get('hitset_sig')}")

    with open(args.out, "w") as f:
        json.dump(runs, f, indent=2)
    print(f"\nraw runs -> {args.out}")

    # ---- summary ----
    print("\n## Stage 3 e2e A/B — median (IQR)\n")
    print("| metric | zstd-on-dev (32) | .dmnd LATEST (20) |")
    print("|---|---|---|")
    by_arm = {a: [r for r in runs if r.get("arm") == a and r.get("status") == "ok"]
              for a in ("zstd", "dmnd")}
    for key, label, unit, scale in METRICS:
        z = fmt_metric(by_arm["zstd"], key, scale)
        d = fmt_metric(by_arm["dmnd"], key, scale)
        print(f"| {label} ({unit}) | {z} | {d} |")

    # ---- correctness / integrity panel ----
    print("\n## Integrity\n")
    for a in ("zstd", "dmnd"):
        ok = by_arm[a]
        n_ok = len(ok)
        n_planned = args.runs_per_arm
        merges = all(r.get("merge_set_match") for r in ok) if ok else False
        hits = sorted({r.get("num_results") for r in ok})
        sigs = sorted({r.get("hitset_sig") for r in ok})
        cold_all = all(r.get("cold_workers") == r.get("n_workers") for r in ok) if ok else False
        print(f"- **{a}**: {n_ok}/{n_planned} ok · merge_set_match all={merges} · "
              f"num_results={hits} · hitset_sigs={sigs} "
              f"({'stable' if len(sigs) == 1 else 'VARIES'}) · all-cold={cold_all}")
    # cross-arm hit-set overlap (confounded by the corpus delta — informational)
    zsig = {r.get("hitset_sig") for r in by_arm["zstd"]}
    dsig = {r.get("hitset_sig") for r in by_arm["dmnd"]}
    print(f"- cross-arm hitset_sig: zstd={sorted(zsig)} dmnd={sorted(dsig)} "
          f"(equal corpus would match; A/B builds differ 104.9B vs 102.9B letters)")


if __name__ == "__main__":
    main()
