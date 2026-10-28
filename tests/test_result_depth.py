"""Validation for the fixed-depth search change (SEARCH_VERSION 1.6.0).

Standalone (no pytest): `python3 tests/test_result_depth.py`. S3/SFN/RDS are
stubbed, so this exercises the real orchestrator/aggregator code paths without
touching AWS.
"""
import json
import os
import sys
import types
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import common  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


MANIFEST = {
    "corpus": "s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa",
    "total_sequences": 307155746,
    "total_letters": 104940484545,
    "database_release": "v1.1",
    "shard_count": 2,
    "shards": [
        {"index": 0, "key": "diamond/V1/shard_00.fa.zst", "sequences": 5, "letters": 3_200_000_000},
        {"index": 1, "key": "diamond/V1/shard_01.fa.zst", "sequences": 5, "letters": 3_200_000_000},
    ],
}


def load_orchestrator():
    """Import orchestrator with boto3 clients stubbed; return (module, captured)."""
    captured = {}

    def get_object(Bucket, Key):
        if Key.endswith("LATEST"):
            body = b"V1"
        elif Key.endswith("manifest.json"):
            body = json.dumps(MANIFEST).encode()
        else:
            raise AssertionError("unexpected GET " + Key)
        return {"Body": types.SimpleNamespace(read=lambda: body)}

    s3 = mock.MagicMock()
    s3.get_object.side_effect = get_object
    sfn = mock.MagicMock()
    sfn.start_execution.side_effect = lambda **kw: captured.update(
        json.loads(kw["input"])) or {"executionArn": "arn:x"}

    with mock.patch("boto3.client", side_effect=lambda svc, **kw: s3 if svc == "s3" else sfn):
        for m in ("orchestrator",):
            sys.modules.pop(m, None)
        import orchestrator
    orchestrator.STATE_MACHINE_ARN = "arn:aws:states:us-east-1:1:stateMachine:test"
    return orchestrator, captured


print("\n=== common.py ===")
check("SEARCH_VERSION bumped to 1.6.0", common.SEARCH_VERSION == "1.6.0", common.SEARCH_VERSION)
check("RESULT_DEPTH defined and positive", isinstance(common.RESULT_DEPTH, int) and common.RESULT_DEPTH > 0)
check("RESULT_DEPTH == 500 (matches web-app Joi cap)", common.RESULT_DEPTH == 500, str(common.RESULT_DEPTH))

print("\n=== orchestrator: caller's max_results must NOT reach the search ===")
orch, cap = load_orchestrator()
SEQ = "MKLLIVLLAACLAVFAAAEPQIAVVMNKGQ"

for requested in (10, 50, 400, None):
    cap.clear()
    body = {"action": "search", "sessionId": "s1", "sequence": SEQ}
    if requested is not None:
        body["max_results"] = requested
    resp = orch.handler(body, None)
    check(f"max_results={requested} -> execution searches to RESULT_DEPTH",
          cap.get("maxResults") == common.RESULT_DEPTH, f"got {cap.get('maxResults')}")
    check(f"max_results={requested} -> HTTP 200", resp["statusCode"] == 200)

check("execution input still carries searchVersion 1.6.0", cap.get("searchVersion") == "1.6.0")
check("execution input unchanged otherwise (shards/dbSize/version)",
      len(cap.get("shards", [])) == 2 and cap.get("dbSize") == 104940484545 and cap.get("version") == "V1")

print("\n=== orchestrator: version action reports result_depth ===")
v = json.loads(orch.handler({"action": "version"}, None)["body"])
check("version action returns result_depth", v.get("result_depth") == common.RESULT_DEPTH, str(v))
check("version action returns search_version 1.6.0", v.get("search_version") == "1.6.0")
check("version action still returns database_version/release",
      v.get("database_version") == "V1" and v.get("database_release") == "v1.1")
check("version action needs no sessionId", "error" not in v)

print("\n=== aggregator: result_depth stamped, truncation honours depth ===")


def run_aggregator(depth, n_hits_per_shard, n_shards=2):
    """Drive the real aggregator over synthetic parts; return the written doc."""
    written = {}
    rows = []
    for sh in range(n_shards):
        lines = []
        for i in range(n_hits_per_shard):
            bits = 1000 - (sh * n_hits_per_shard + i)
            lines.append("\t".join([f"orf_{sh}_{i}", "1", "100", "1", "100", "100",
                                    "77.7", "1e-50", str(float(bits)), "MKLLV"]))
        rows.append(("\n".join(lines) + "\n").encode())

    def list_pages(Bucket, Prefix):
        return [{"Contents": [{"Key": f"{Prefix}shard_{i}.tsv"} for i in range(n_shards)]}]

    s3 = mock.MagicMock()
    paginator = mock.MagicMock()
    paginator.paginate.side_effect = lambda **kw: list_pages(kw["Bucket"], kw["Prefix"])
    s3.get_paginator.return_value = paginator
    s3.get_object.side_effect = lambda Bucket, Key: {
        "Body": types.SimpleNamespace(read=lambda k=Key: rows[int(k.split("shard_")[1].split(".")[0])])}
    s3.put_object.side_effect = lambda **kw: written.update({kw["Key"]: kw["Body"]})

    with mock.patch("boto3.client", return_value=s3):
        sys.modules.pop("aggregator", None)
        import aggregator
    aggregator.s3 = s3

    aggregator.handler({
        "sessionId": "s1", "jobId": "j1", "querySequence": SEQ, "queryHeader": "query",
        "maxResults": depth, "shards": [{"shardIndex": i} for i in range(n_shards)],
        "version": "V1", "databaseRelease": "v1.1", "searchVersion": common.SEARCH_VERSION,
        "corpus": MANIFEST["corpus"], "dbSequenceCount": 307155746,
    }, None)
    return json.loads(written["results/s1/j1.json"])


# depth binding: 2 shards x 400 hits = 800 candidates, depth 500
doc = run_aggregator(depth=500, n_hits_per_shard=400)
check("result_depth stamped in result JSON", doc.get("result_depth") == 500, str(doc.get("result_depth")))
check("truncated to depth", doc["num_results"] == 500, str(doc["num_results"]))
check("num_results == len(results)", doc["num_results"] == len(doc["results"]))
check("ranked by bitscore desc", all(
    doc["results"][i]["bitscore"] >= doc["results"][i + 1]["bitscore"] for i in range(len(doc["results"]) - 1)))
check("search_version 1.6.0 stamped", doc.get("search_version") == "1.6.0")
check("target_sequence still stamped (1.3.0 field intact)", doc["results"][0].get("target_sequence") == "MKLLV")

# exhaustive case: fewer hits than depth
doc2 = run_aggregator(depth=500, n_hits_per_shard=5)
check("exhaustive search: num_results < result_depth", doc2["num_results"] == 10 and doc2["result_depth"] == 500,
      f"{doc2['num_results']}/{doc2['result_depth']}")

# the exactness invariant: top-N of a deep result == top-N of a shallow search
deep = run_aggregator(depth=500, n_hits_per_shard=400)
shallow = run_aggregator(depth=50, n_hits_per_shard=400)
ids_deep = [r["target_id"] for r in deep["results"][:50]]
ids_shallow = [r["target_id"] for r in shallow["results"]]
check("top-50 slice of depth-500 == a depth-50 search (exactness invariant)", ids_deep == ids_shallow)

print("\n" + ("ALL CHECKS PASSED" if not FAILURES else f"{len(FAILURES)} FAILURE(S): {FAILURES}"))
sys.exit(1 if FAILURES else 0)
