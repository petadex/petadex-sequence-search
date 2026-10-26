#!/usr/bin/env python3
"""
Validate a not-yet-LATEST DIAMOND build through the live orchestrator.

Runs each site example (data/example-sequences.txt) twice: pinned to VERSION
(`"version"` in the request body, which `resolve_version` honours) and unpinned
(whatever diamond/LATEST is). Polls S3 for each result and writes one TSV row per
search: hits, top hit, top identity, wall time, database version/release.
`LATEST` is only read, never written.

Fails (exit 1) if any pinned search fails, returns no hits, comes back
labelled with a different database_version, or holds a hit that is not a
v260701 petadex_id, or if a v1.1 LATEST search holds one (cross-version leak).

Usage: validate_pinned_version.py VERSION OUT.tsv
"""

import json
import re
import sys
import time
import uuid
from pathlib import Path

import boto3

BUCKET = "petadex"
ORCH = "petadex-diamond-orchestrator"
EXAMPLES = Path(__file__).resolve().parents[2] / "data" / "example-sequences.txt"
TIMEOUT_S = 15 * 60
# v260701 hit IDs are bare petadex_ids; v1.1's are `orf||run|…` / `n|acc|||||`.
# A hit of the other format means a worker searched the wrong version's shard.
PETADEX_ID = re.compile(r"^(PL|NR|LG)\d+$")

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def read_examples():
    recs, name = {}, None
    for line in EXAMPLES.read_text().splitlines():
        if line.startswith(">"):
            name = line[1:].strip()
            recs[name] = ""
        elif name:
            recs[name] += line.strip()
    return recs


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def search(name, seq, version):
    session = f"validate-{'pin' if version else 'latest'}-{name}-{uuid.uuid4().hex[:8]}"
    body = {"action": "search", "sessionId": session, "sequence": f">{name}\n{seq}"}
    if version:
        body["version"] = version
    resp = lam.invoke(FunctionName=ORCH, Payload=json.dumps(body).encode())
    out = json.loads(resp["Payload"].read())
    payload = json.loads(out["body"]) if isinstance(out.get("body"), str) else out
    job = payload.get("job_id")
    if not job:
        return {"status": f"rejected: {payload}"}
    t0 = time.time()
    while time.time() - t0 < TIMEOUT_S:
        res = s3_json(f"results/{session}/{job}.json")
        if res is not None:
            break
        timing = s3_json(f"results/{session}/{job}/timing.json")
        if timing and timing.get("status") == "failed":
            return {"status": "failed", "session": session, "job": job}
        time.sleep(10)
    else:
        return {"status": "timeout", "session": session, "job": job}
    hits = res.get("results", [])
    top = hits[0] if hits else {}
    timing = s3_json(f"results/{session}/{job}/timing.json") or {}
    return {
        "status": "ok", "session": session, "job": job,
        "database_version": res.get("database_version"),
        "database_release": res.get("database_release"),
        "search_version": res.get("search_version"),
        "num_results": res.get("num_results", len(hits)),
        "top_target": top.get("target_id"),
        "top_pident": top.get("percent_identity"),
        "top_bitscore": top.get("bitscore"),
        "top_evalue": top.get("evalue"),
        "total_wall_ms": timing.get("total_wall_ms"),
        "n_petadex_ids": sum(bool(PETADEX_ID.match(h["target_id"])) for h in hits),
    }


def main():
    version, out = sys.argv[1], Path(sys.argv[2])
    cols = ["example", "arm", "status", "database_version", "database_release",
            "search_version", "num_results", "top_target", "top_pident",
            "top_bitscore", "top_evalue", "total_wall_ms", "n_petadex_ids",
            "session", "job"]
    rows, ok = [], True
    for name, seq in read_examples().items():
        for arm, ver in (("pinned", version), ("latest", None)):
            r = search(name, seq, ver)
            r.update(example=name, arm=arm)
            rows.append(r)
            print("\t".join(str(r.get(c, "")) for c in cols), flush=True)
            if arm == "pinned" and (r["status"] != "ok" or not r["num_results"]
                                    or r.get("database_version") != version
                                    or r["n_petadex_ids"] != r["num_results"]):
                ok = False
            # LATEST is v1.1 until cutover: any petadex_id there is a leak.
            if arm == "latest" and r.get("database_release") == "v1.1" \
                    and r.get("n_petadex_ids"):
                ok = False
    with open(out, "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(str(r.get(c, "")) for c in cols) + "\n")
    print("VALIDATION " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
