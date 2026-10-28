"""Alignment + tree (src/align.py), the aggregator's background start and the orchestrator's `align` action.

Uses real FAMSA / FastTree when FAMSA_BIN / FASTTREE_BIN point at them (or they are on PATH);
otherwise the build step is stubbed and the rest still runs. No AWS: S3 is an in-memory fake."""
import io
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
FAILURES = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


class NoSuchKey(Exception):
    pass


class FakeS3:
    def __init__(self):
        self.store = {}
        self.exceptions = mock.Mock(NoSuchKey=NoSuchKey)

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.store[Key] = Body if isinstance(Body, bytes) else Body.encode()

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise NoSuchKey(Key)
        return {"Body": io.BytesIO(self.store[Key])}


s3 = FakeS3()
with mock.patch("boto3.client", return_value=s3):
    import align  # noqa: E402
align.s3 = s3
REAL = all(shutil.which(b) for b in (align.FAMSA_BIN, align.FASTTREE_BIN))
print(f"real FAMSA/FastTree: {REAL}")

SEQ = "MNFPRASRLMQAAVLGGLMAVSAAATAQTNPYARGPNPTAASLEASAGPFTVRSFTVSRPSGYGAGTVYYPTNAGGTVGAIAIVPGYTARQSSIKWWGPRLASHGFVVITIDTNSTLDQPSSRSSQQMAALRQVASLNGTSSSPIYGKVDTARMGVMGWSMGGGGSLISAANNPSLKAAAPQAPWDSSTNFSSVTVPTLIFACENDSIAPVNSSALPIYDSMSRNAKQFLEINGGSHSCANSGNSNQALIGKKGVAWMKRFMDNDTRYSTFACENPNSTRVSDFRTANCS"
def mut(i):   # a deterministic variant of SEQ
    s = list(SEQ); s[(7 * i) % len(s)] = "W"; s[(13 * i) % len(s)] = "K"; return "".join(s)
HITS = [
    {"target_id": "PL1", "target_sequence": SEQ, "metadata": {"c90": "A"}},
    {"target_id": "NR1", "target_sequence": mut(1), "metadata": {"c90": "A"}},     # same c90 as PL1 -> skipped
    {"target_id": "PL2", "target_sequence": mut(2), "metadata": {"c90": "A"}},     # Plasticome -> always kept
    {"target_id": "LG1", "target_sequence": mut(3), "metadata": {"c90": "B"}},
    {"target_id": "LG2", "target_sequence": mut(4), "metadata": {"c90": "B"}},     # skipped
    {"target_id": "LG3", "target_sequence": mut(5), "metadata": None},             # no card -> kept
    {"target_id": "LG4", "target_sequence": None, "metadata": {"c90": "C"}},       # no sequence -> never aligned
    {"target_id": "LG5", "target_sequence": mut(6), "metadata": {"c90": "C"}},
]
RESULT = {"query_sequence": SEQ, "database_version": "V", "search_version": "1.6.0", "results": HITS}

print("\n=== select / keys ===")
names = [n for n, _ in align.select(RESULT)]
check("default: query, Plasticome, best hit per c90, no-sequence hits dropped",
      names == ["query", "PL1", "PL2", "LG1", "LG3", "LG5"], names)
check("ids: those hits in rank order", [n for n, _ in align.select(RESULT, ["LG5", "PL1"])] == ["query", "PL1", "LG5"])
try:
    align.select(RESULT, ["PL1", "NR999"]); check("ids outside the result rejected", False)
except ValueError:
    check("ids outside the result rejected", True)
with mock.patch.object(align, "MAX_HITS", 2):
    check("default capped at MAX_HITS hits", len(align.select(RESULT)) == 3)
check("default key", align.key_for(None) == "default")
check("ids key is order-free", align.key_for(["A", "B"]) == align.key_for(["B", "A", "A"]) and align.key_for(["A"]).startswith("ids-"))
for bad in ([], "PL1", [1], [""]):
    try:
        align.validate_ids(bad); check(f"validate_ids rejects {bad!r}", False)
    except ValueError:
        check(f"validate_ids rejects {bad!r}", True)
try:
    align.validate_ids([f"LG{i}" for i in range(201)]); check("validate_ids rejects > 200", False)
except ValueError:
    check("validate_ids rejects > 200", True)

print("\n=== run ===")
s3.put_object(Bucket="b", Key="results/s/j.json", Body=json.dumps(RESULT))
fake_build = lambda seqs, threads=None: ("".join(f">{n}\n{q}\n" for n, q in seqs), "(" + ",".join(n for n, _ in seqs) + ");\n", {"famsa_ms": 1, "fasttree_ms": 1})
with mock.patch.object(align, "build", align.build if REAL else fake_build):
    doc = align.run("s", "j")
check("run -> ready", doc["status"] == "ready", doc.get("error"))
check("status stored", json.loads(s3.store["alignments/s/j/default.json"])["status"] == "ready")
afa, nwk = align.read_outputs("s", "j", "default")
heads = [l[1:] for l in afa.splitlines() if l.startswith(">")]
check("afa headers = query + petadex_ids", heads == ["query", "PL1", "PL2", "LG1", "LG3", "LG5"], heads)
check("afa rows equal length", len({len(r) for r in afa.split(">")[1:] for r in ["".join(r.split("\n")[1:])]}) == 1)
leaves = sorted(x.strip("()").split(":")[0] for x in nwk.strip().rstrip(";").replace("(", ",").replace(")", ",").split(",") if x.strip("() ").split(":")[0])
check("newick leaves = afa headers", set(heads) <= set(leaves), leaves)
check("ready doc records ids, columns, versions", doc["ids"] == heads[1:] and doc["n_columns"] > 0 and doc["search_version"] == "1.6.0")
bad = align.run("s", "missing-job")
check("run on a missing result -> failed, not raised", bad["status"] == "failed" and "NoSuchKey" in bad["error"], bad.get("error"))

print("\n=== read_status ===")
old = (datetime.now(timezone.utc) - timedelta(seconds=align.STALE_RUNNING_S + 5)).isoformat(timespec="seconds")
align._put_status("s", "j", "stale", {"status": "running", "started_at": old})
check("stale running reads as failed", align.read_status("s", "j", "stale")["status"] == "failed")
check("missing status -> None", align.read_status("s", "j", "nope") is None)

print("\n=== aggregator starts it after the write ===")
lam = mock.MagicMock()
with mock.patch("boto3.client", return_value=s3):
    sys.modules.pop("aggregator", None)
    import aggregator  # noqa: E402
with mock.patch.object(aggregator, "ALIGN_FUNCTION", "petadex-diamond-align"), mock.patch("boto3.client", return_value=lam):
    started = aggregator.start_alignment("s", "j")
check("aggregator invokes the align Lambda async", started and lam.invoke.call_args.kwargs["InvocationType"] == "Event"
      and json.loads(lam.invoke.call_args.kwargs["Payload"]) == {"sessionId": "s", "jobId": "j"})
with mock.patch.object(aggregator, "ALIGN_FUNCTION", None):
    check("no ALIGN_FUNCTION -> no start", aggregator.start_alignment("s", "j") is False)
boom = mock.MagicMock(); boom.invoke.side_effect = RuntimeError("throttled")
with mock.patch.object(aggregator, "ALIGN_FUNCTION", "f"), mock.patch("boto3.client", return_value=boom):
    check("a failed start never raises", aggregator.start_alignment("s", "j") is False)

print("\n=== orchestrator align action ===")
with mock.patch("boto3.client", return_value=s3):
    sys.modules.pop("orchestrator", None)
    import orchestrator  # noqa: E402
orchestrator.s3 = s3
orchestrator.lam = mock.MagicMock()
s3.put_object(Bucket="b", Key="results/s.index", Body="j")
call = lambda **b: orchestrator.handler({"action": "align", "sessionId": "s", **b}, None)
body = lambda r: json.loads(r["body"])
r = call()
check("ready default via the latest job -> afa + newick", r["statusCode"] == 200 and body(r)["status"] == "ready"
      and body(r)["afa"].startswith(">query") and body(r)["newick"].strip().endswith(";"), r.get("statusCode"))
with mock.patch.object(orchestrator, "ALIGN_FUNCTION", "petadex-diamond-align"):
    r = call(jobId="j", ids=["LG5", "PL1"])
    check("custom ids -> running, async start", r["statusCode"] == 200 and body(r)["status"] == "running"
          and orchestrator.lam.invoke.call_args.kwargs["InvocationType"] == "Event"
          and json.loads(orchestrator.lam.invoke.call_args.kwargs["Payload"])["ids"] == ["LG5", "PL1"])
    n_before = orchestrator.lam.invoke.call_count
    r = call(jobId="j", ids=["PL1", "LG5"])
    check("second request while running does not start again", body(r)["status"] == "running" and orchestrator.lam.invoke.call_count == n_before)
    r = call(jobId="j", ids=["PL1", "NR999"])
    check("ids outside the result -> 400", r["statusCode"] == 400, r.get("statusCode"))
    r = call(jobId="nojob")
    check("no such result -> 404", r["statusCode"] == 404, r.get("statusCode"))
    r = call(jobId="j", ids=["LG3", "LG1"], poll=True)
    check("poll does not start -> none", body(r)["status"] == "none" and orchestrator.lam.invoke.call_count == n_before)
r = call(jobId="j", ids=["LG1", "LG3"])
check("not configured -> 503", r["statusCode"] == 503, r.get("statusCode"))
r = call(ids="PL1")
check("malformed ids -> 400", r["statusCode"] == 400, r.get("statusCode"))
r = orchestrator.handler({"action": "align", "sessionId": "unknown"}, None)
check("session with no search -> 404", r["statusCode"] == 404, r.get("statusCode"))

print(f"\n{len(FAILURES)} FAILURE(S): {FAILURES}" if FAILURES else "\nALL PASSED")
sys.exit(1 if FAILURES else 0)
