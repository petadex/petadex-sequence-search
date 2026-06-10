#!/usr/bin/env python3
"""Flip benchmark env vars on petadex-diamond-worker, preserving the base set.

`update-function-configuration` replaces the WHOLE Environment.Variables map, so
this reads the base vars and overlays only the benchmark flags. Waits for
LastUpdateStatus=Successful so the next invocations use the new config (and are
cold, which keeps benchmark runs comparable).

Usage:
  set_worker_env.py --chunks 1                 # S1: add DIAMOND_CHUNKS=1
  set_worker_env.py --masking 0                # S2: add DIAMOND_MASKING=0
  set_worker_env.py --chunks 1 --masking 0     # combined
  set_worker_env.py --reset                    # back to base (no bench flags)
"""
import argparse
import json
import subprocess
import sys
import time

FN = "petadex-diamond-worker"
# The durable production env (mirrors scripts/provision_diamond_infra.sh).
BASE = {"S3_BUCKET": "petadex", "DIAMOND_BLOCK_SIZE": "1",
        "DIAMOND_SENSITIVITY": "default"}


def aws(args, parse=True):
    p = subprocess.run(["aws", *args, "--no-cli-pager"],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL,
                       timeout=60)
    if p.returncode != 0:
        sys.exit(f"aws {' '.join(args[:3])} failed:\n{p.stderr}")
    return json.loads(p.stdout) if parse and p.stdout.strip() else p.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks")
    ap.add_argument("--masking")
    ap.add_argument("--reset", action="store_true")
    a = ap.parse_args()

    env = dict(BASE)
    if not a.reset:
        if a.chunks is not None:
            env["DIAMOND_CHUNKS"] = a.chunks
        if a.masking is not None:
            env["DIAMOND_MASKING"] = a.masking

    aws(["lambda", "update-function-configuration", "--function-name", FN,
         "--environment", json.dumps({"Variables": env})])
    print("requested env:", env)

    for _ in range(30):
        cfg = aws(["lambda", "get-function-configuration", "--function-name", FN])
        if cfg.get("LastUpdateStatus") == "Successful":
            print("settled. live env:", cfg.get("Environment", {}).get("Variables"))
            return
        time.sleep(2)
    sys.exit("timed out waiting for LastUpdateStatus=Successful")


if __name__ == "__main__":
    main()
