"""Validation for public-search admission limits (orchestrator.check_admission).

Standalone (no pytest): `python3 tests/test_admission.py`. S3/SFN are stubbed,
so this exercises the real orchestrator handler without touching AWS.
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


MANIFEST = {
    "corpus": "s3://petadex/logan/petadex.catalytic_orfs.v1.1.fa",
    "total_sequences": 10,
    "total_letters": 1000,
    "database_release": "v1.1",
    "shards": [{"index": 0, "key": "diamond/V1/shard_00.fa.zst", "sequences": 10, "letters": 1000}],
}
SEQ = "MNFPRASRLMQAAVLGGLMAVSAAATAQTNPYARGPNPTAASLEASAGPFTVRSFTVSRPSGYGAGTVYYPTNAGGTVGAIAIVPGYTARQSSIKWWGPRLASHGFVVITIDTNSTLDQPSSRSSQQMAALRQVASLNGTSSSPIYGKVDTARMGVMGWSMGGGGSLISAANNPSLKAAAPQAPWDSSTNFSSVTVPTLIFACENDSIAPVNSSALPIYDSMSRNAKQFLEINGGSHSCANSGNSNQALIGKKGVAWMKRFMDNDTRYSTFACENPNSTRVSDFRTANCS"


def load_orchestrator(executions):
    """Import orchestrator with boto3 stubbed; `executions` is the fake history."""
    started = []

    def get_object(Bucket, Key):
        body = b"V1" if Key.endswith("LATEST") else json.dumps(MANIFEST).encode()
        return {"Body": types.SimpleNamespace(read=lambda: body)}

    s3 = mock.MagicMock()
    s3.get_object.side_effect = get_object
    sfn = mock.MagicMock()
    sfn.start_execution.side_effect = lambda **kw: started.append(kw["name"]) or {"executionArn": "arn:x"}
    paginator = mock.MagicMock()
    # Newest first, split across pages to exercise the early stop.
    ordered = sorted(executions, key=lambda e: e["startDate"], reverse=True)
    paginator.paginate.return_value = [{"executions": ordered[:3]}, {"executions": ordered[3:]}]
    sfn.get_paginator.return_value = paginator

    with mock.patch("boto3.client", side_effect=lambda svc, **kw: s3 if svc == "s3" else sfn):
        sys.modules.pop("orchestrator", None)
        import orchestrator
    orchestrator.STATE_MACHINE_ARN = "arn:aws:states:us-east-1:1:stateMachine:test"
    return orchestrator, started


NOW = datetime.now(timezone.utc)


def ex(minutes_ago, status="SUCCEEDED", name="0" * 32 + "-job"):
    return {"startDate": NOW - timedelta(minutes=minutes_ago), "status": status, "name": name}


def search(orch, client_ip="203.0.113.7"):
    event = {"action": "search", "sessionId": "a" * 32, "sequence": SEQ}
    if client_ip is not None:
        event["clientIp"] = client_ip
    resp = orch.handler(event, None)
    return resp["statusCode"], json.loads(resp["body"])


print("\n=== admitted: quiet history, execution name carries the client tag ===")
orch, started = load_orchestrator([ex(120), ex(300)])
status, body = search(orch)
tag = orch.client_tag("203.0.113.7")
check("200 when under every limit", status == 200, f"{status} {body}")
check("name is {tag}_{session}-{job}", started and started[0].startswith(f"{tag}_{'a' * 32}-"), started)
check("name fits the 80-char cap", started and len(started[0]) <= 80, started)

print("\n=== running-job cap ===")
orch, started = load_orchestrator([ex(1, "RUNNING") for _ in range(4)])
status, body = search(orch)
check("429 at MAX_RUNNING_JOBS", status == 429 and body["limit"] == "running", f"{status} {body}")
check("nothing started", not started)

print("\n=== per-client hourly cap: other clients unaffected ===")
mine = [ex(5 + i, name=f"{tag}_{'b' * 32}-job{i}") for i in range(10)]
orch, started = load_orchestrator(mine)
status, body = search(orch)
check("429 at MAX_JOBS_PER_IP_PER_HOUR", status == 429 and body["limit"] == "client_hour", f"{status} {body}")
# Oldest of the 10 started 14 min ago, so it ages out in ~46 min.
check("retry_after = time until the oldest ages out", 45 * 60 <= body.get("retry_after", 0) <= 47 * 60, body)
orch, started = load_orchestrator(mine)
status, _ = search(orch, client_ip="198.51.100.9")
check("a different client is still admitted", status == 200)

print("\n=== starts older than the hour don't count toward hourly caps ===")
orch, started = load_orchestrator([ex(61 + i, name=f"{tag}_{'b' * 32}-j{i}") for i in range(40)])
status, body = search(orch)
check("admitted", status == 200, f"{status} {body}")

print("\n=== global hourly and daily caps ===")
orch, _ = load_orchestrator([ex(1 + i) for i in range(30)])
status, body = search(orch)
check("429 global_hour", status == 429 and body["limit"] == "global_hour", f"{status} {body}")
orch, _ = load_orchestrator([ex(70 + i * 10) for i in range(100)])
status, body = search(orch)
check("429 global_day", status == 429 and body["limit"] == "global_day", f"{status} {body}")

print("\n=== fails closed if the limits can't be read ===")
orch, started = load_orchestrator([])
orch.sfn.get_paginator.side_effect = RuntimeError("AccessDenied")
status, body = search(orch)
check("503 unavailable", status == 503 and body["limit"] == "unavailable", f"{status} {body}")
check("nothing started", not started)

print("\n=== direct invokes (no clientIp) skip admission ===")
orch, started = load_orchestrator([ex(1, "RUNNING") for _ in range(10)])
status, body = search(orch, client_ip=None)
check("200 with no clientIp", status == 200, f"{status} {body}")
check("untagged execution name", started and started[0].startswith("a" * 32 + "-"), started)
check("no ListExecutions call", not orch.sfn.get_paginator.called)

print("\n=== invalid sequence is rejected before admission ===")
orch, _ = load_orchestrator([])
resp = orch.handler({"action": "search", "sessionId": "a" * 32, "sequence": "12345", "clientIp": "1.2.3.4"}, None)
check("400", resp["statusCode"] == 400, resp)
check("no ListExecutions call", not orch.sfn.get_paginator.called)

print(f"\n{'ALL PASSED' if not FAILURES else f'{len(FAILURES)} FAILED: {FAILURES}'}")
sys.exit(1 if FAILURES else 0)
