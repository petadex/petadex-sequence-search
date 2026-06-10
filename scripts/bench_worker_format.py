#!/usr/bin/env python3
"""Direct-worker DB-format benchmark (Search Optimization 06 — Lambda zstd arm).

Invokes `petadex-diamond-worker` directly (synchronous) against shard_00 in each
DB format and reports cold download_ms / search_ms / total_ms + the hit set, so
`.dmnd` vs `.fa.zst` can be compared on real Lambda. shard_00 is the decision
metric: job wall ≈ slowest of ~20 uniform shards ≈ one shard's download+search.

Cold strategy: bump an (unused) worker env var to recycle all containers, then
fire N concurrent invokes — with no warm containers, every invoke is a cold
download. Each uses a distinct jobId so the timing sidecars don't collide.

Host has no boto3 → everything via the aws CLI.

Usage:
  python3 scripts/bench_worker_format.py --runs 5
  python3 scripts/bench_worker_format.py --runs 5 --query IsPETase
"""
import argparse
import concurrent.futures as cf
import json
import os
import statistics
import subprocess
import time
import uuid

BUCKET = "petadex"
FN = "petadex-diamond-worker"
# Production .dmnd build (20 shards). The zstd candidate is a SEPARATE finer-shard
# build (--format zstd --shard-count 32) — pass its version via --zst-version, so
# each arm runs in its viable config (zstd can't fit memory at 20 shards). Job
# wall ≈ the slowest single shard's download+search, so a shard_00-vs-shard_00 A/B
# across the two builds is the right job-wall comparison even at different counts.
DMND_VERSION = "catalytic_orfs_v1.1_20260602_222538"
DBSIZE = 104940484545  # corpus-wide residues (skips the FASTA letter-count pass)
HERE = os.path.dirname(os.path.abspath(__file__))
SEQ_FILE = os.path.join(HERE, "..", "example-sequences.txt")
OUT_DIR = os.path.join(HERE, "..", "docs", "bench")
BASE_ENV = {"S3_BUCKET": "petadex", "DIAMOND_BLOCK_SIZE": "1",
            "DIAMOND_SENSITIVITY": "default"}


def aws(args, parse=True, timeout=650):
    p = subprocess.run(["aws", *args, "--no-cli-pager"], capture_output=True,
                       text=True, stdin=subprocess.DEVNULL, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args[:3])} failed:\n{p.stderr}")
    return json.loads(p.stdout) if parse and p.stdout.strip() else p.stdout


def load_sequence(name):
    seqs, cur, hdr = {}, [], None
    for line in open(SEQ_FILE):
        line = line.rstrip("\n")
        if line.startswith(">"):
            if hdr:
                seqs[hdr] = "".join(cur)
            hdr, cur = line[1:], []
        else:
            cur.append(line)
    if hdr:
        seqs[hdr] = "".join(cur)
    return seqs[name]


def force_cold(nonce):
    env = dict(BASE_ENV, BENCH_NONCE=nonce)
    aws(["lambda", "update-function-configuration", "--function-name", FN,
         "--environment", json.dumps({"Variables": env})])
    for _ in range(30):
        cfg = aws(["lambda", "get-function-configuration", "--function-name", FN])
        if cfg.get("LastUpdateStatus") == "Successful":
            return
        time.sleep(2)
    raise RuntimeError("worker config did not settle")


def s3_get(key):
    tmp = "/tmp/bwf_" + key.replace("/", "_")
    p = subprocess.run(["aws", "s3api", "get-object", "--bucket", BUCKET,
                        "--key", key, tmp, "--no-cli-pager"],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if p.returncode != 0:
        return None
    return open(tmp).read()


def invoke_one(arm, shard_key, query_fasta, max_results, shard_letters=None):
    session = f"zstbench_{arm}"
    job = str(uuid.uuid4())
    event = {"sessionId": session, "jobId": job, "shardIndex": 0,
             "shardKey": shard_key, "queryFasta": query_fasta,
             "maxResults": max_results, "dbSize": DBSIZE}
    # Thread per-shard residues so the worker sizes the FASTA single block to the
    # shard (-b4 at 32 shards). Without it, the worker falls back to its env -b6
    # default → OOM. Harmless for the .dmnd arm (it uses DIAMOND_BLOCK_SIZE).
    if shard_letters:
        event["shardLetters"] = shard_letters
    outfile = f"/tmp/bwf_resp_{arm}_{job}.json"
    t0 = time.time()
    aws(["lambda", "invoke", "--function-name", FN,
         "--cli-binary-format", "raw-in-base64-out", "--payload", json.dumps(event),
         "--cli-read-timeout", "650", "--cli-connect-timeout", "15", outfile],
        parse=False)
    wall = time.time() - t0
    resp = json.load(open(outfile))
    if "FunctionError" in resp or resp.get("errorMessage"):
        return {"arm": arm, "job": job, "status": "error",
                "error": resp.get("errorMessage") or str(resp)}
    # timing sidecar + hit set
    meta = s3_get(f"results/{session}/{job}/parts/shard_0.meta.json")
    tsv = s3_get(f"results/{session}/{job}/parts/shard_0.tsv")
    m = json.loads(meta) if meta else {}
    tids = sorted(r.split("\t")[0] for r in tsv.splitlines()) if tsv else []
    return {"arm": arm, "job": job, "status": "ok", "client_wall_s": round(wall, 1),
            "download_ms": m.get("download_ms"), "search_ms": m.get("search_ms"),
            "total_ms": m.get("total_ms"), "shard_bytes": m.get("shard_size_bytes"),
            "num_hits": m.get("num_hits"), "target_ids": tids}


def run_arm(arm, shard_key, shard_letters, runs, query_fasta, max_results):
    print(f"\n== arm '{arm}' ({shard_key}) — {runs} cold concurrent ==")
    force_cold(f"{arm}-{int(time.time())}")
    with cf.ThreadPoolExecutor(max_workers=runs) as ex:
        results = list(ex.map(
            lambda _: invoke_one(arm, shard_key, query_fasta, max_results,
                                 shard_letters),
            range(runs)))
    for r in results:
        if r["status"] != "ok":
            print(f"  ERROR: {r.get('error')}")
        else:
            print(f"  dl={r['download_ms']}ms search={r['search_ms']}ms "
                  f"total={r['total_ms']}ms hits={r['num_hits']} "
                  f"bytes={r['shard_bytes']}")
    return results


def stat(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {"n": len(xs), "median": round(statistics.median(xs), 1),
            "mean": round(statistics.mean(xs), 1),
            "min": round(min(xs), 1), "max": round(max(xs), 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--query", default="FAST-PETase")
    ap.add_argument("--max-results", type=int, default=50)
    ap.add_argument("--dmnd-version", default=DMND_VERSION,
                    help="production .dmnd build version (20 shards)")
    ap.add_argument("--zst-version",
                    help="candidate zstd build version (e.g. a 32-shard "
                         "--format zstd build). Omit to run the .dmnd arm only.")
    ap.add_argument("--zst-shard-letters", type=int, default=DBSIZE // 32,
                    help="residues in a zst shard, threaded so the worker sizes "
                         "its single block (-b4 at 32 shards). Default = "
                         "corpus/32; set to the manifest's per-shard letters.")
    a = ap.parse_args()
    seq = load_sequence(a.query)
    qf = f">query\n{seq}"
    print(f"query={a.query} ({len(seq)} aa) runs={a.runs} maxResults={a.max_results}")

    # arm -> (shard_key, shard_letters). The zst arm only runs if its build version
    # is supplied (it's a separate finer-shard build, not yet in production).
    arms = {"dmnd": (f"diamond/{a.dmnd_version}/shard_00.dmnd", None)}
    if a.zst_version:
        arms["zst"] = (f"diamond/{a.zst_version}/shard_00.fa.zst",
                       a.zst_shard_letters)
    else:
        print("NOTE: --zst-version not given — running the .dmnd arm only.")

    all_res = {}
    for arm, (shard_key, shard_letters) in arms.items():
        all_res[arm] = run_arm(arm, shard_key, shard_letters, a.runs, qf,
                               a.max_results)

    # restore base env (drop BENCH_NONCE)
    aws(["lambda", "update-function-configuration", "--function-name", FN,
         "--environment", json.dumps({"Variables": BASE_ENV})])

    print("\n====== SUMMARY (median ms) ======")
    summ = {}
    for arm in all_res:
        ok = [r for r in all_res[arm] if r["status"] == "ok"]
        summ[arm] = {
            "download": stat([r["download_ms"] for r in ok]),
            "search": stat([r["search_ms"] for r in ok]),
            "total": stat([r["total_ms"] for r in ok]),
            "hits": ok[0]["num_hits"] if ok else None,
            "hitset": ok[0]["target_ids"] if ok else None,
        }
        s = summ[arm]
        print(f"{arm:5s} download={s['download']['median'] if s['download'] else '-':>9} "
              f"search={s['search']['median'] if s['search'] else '-':>9} "
              f"total={s['total']['median'] if s['total'] else '-':>9} "
              f"hits={s['hits']}")

    d, z = summ.get("dmnd"), summ.get("zst")
    if d and z and d["total"] and z["total"]:
        dt = z["total"]["median"] - d["total"]["median"]
        print(f"\nzst vs dmnd total: {d['total']['median']} -> {z['total']['median']} "
              f"Δ {dt:+.0f}ms ({dt/d['total']['median']*100:+.0f}%)")
    if d and z and d["hitset"] is not None and z["hitset"] is not None:
        same = d["hitset"] == z["hitset"]
        ds, zs = set(d["hitset"]), set(z["hitset"])
        print(f"hit-set identical: {same} (overlap {len(ds & zs)}/{len(ds)}, "
              f"zst_added={len(zs - ds)}, zst_dropped={len(ds - zs)})")

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "format_zst_vs_dmnd.json")
    json.dump({"query": a.query, "runs": a.runs, "arms": all_res, "summary": summ},
              open(path, "w"), indent=2)
    print(f"saved -> {os.path.relpath(path)}")


if __name__ == "__main__":
    main()
