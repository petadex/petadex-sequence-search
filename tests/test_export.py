"""Export of all significant hits (common.EXPORT_DEPTH, export format 1.0).

Standalone (no pytest): `python3 tests/test_export.py`. S3/SFN/DIAMOND are
stubbed, so this drives the real orchestrator/worker/aggregator export paths
without touching AWS.
"""
import gzip
import io
import json
import os
import random
import sys
import types
from datetime import datetime, timezone
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import common  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


SEQ = "MKLLIVLLAACLAVFAAAEPQIAVVMNKGQ"
SID = "0123456789abcdef0123456789abcdef"  # the web app's md5 cache key
MANIFEST = {
    "corpus": "s3://petadex/diamond/corpus/PETadex.seq.v260701.fa",
    "total_sequences": 10, "total_letters": 88181580142, "database_release": "v260701",
    "shards": [{"index": i, "key": f"diamond/V2/shard_0{i}.fa.zst", "sequences": 5,
                "letters": 2_756_000_000} for i in range(2)],
}

print("\n=== common ===")
check("export sessionId = sessionId + suffix", common.export_session_id(SID) == SID + "_x")
check("public export execution name fits 80 chars",
      len(f"{'a' * 8}_{common.export_session_id(SID)}-{'u' * 36}") <= 80)
check("origin from petadex_id prefix",
      [common.hit_origin(t) for t in ("PL1", "NR223", "LG113721682")]
      == ["plasticome", "ncbi_nr", "logan"])
check("v1.1 IDs have unknown origin",
      common.hit_origin("2271374|MFC4015899.1|||||") == "unknown"
      and common.hit_origin("LGx|y") == "unknown")


def load_orchestrator(listings):
    """Import orchestrator with S3/SFN stubbed. `listings` maps a list prefix to
    a list_objects_v2 response. Returns (module, captured execution, sfn)."""
    captured = {}

    def get_object(Bucket, Key):
        body = b"V2" if Key.endswith("LATEST") else json.dumps(MANIFEST).encode()
        return {"Body": types.SimpleNamespace(read=lambda: body)}

    s3 = mock.MagicMock()
    s3.get_object.side_effect = get_object
    s3.list_objects_v2.side_effect = lambda **kw: listings.get(kw["Prefix"], {"KeyCount": 0})
    sfn = mock.MagicMock()
    sfn.start_execution.side_effect = lambda **kw: captured.update(
        json.loads(kw["input"]), _name=kw["name"]) or {"executionArn": "arn:x"}
    sfn.get_paginator.return_value.paginate.return_value = [{"executions": []}]
    with mock.patch("boto3.client", side_effect=lambda svc, **kw: s3 if svc == "s3" else sfn):
        sys.modules.pop("orchestrator", None)
        import orchestrator
    orchestrator.STATE_MACHINE_ARN = "arn:aws:states:us-east-1:1:stateMachine:test"
    return orchestrator, captured, sfn


print("\n=== orchestrator: export starts an uncapped fan-out under its own sessionId ===")
orch, cap, sfn = load_orchestrator({})
resp = orch.handler({"action": "export", "sessionId": SID, "sequence": SEQ}, None)
body = json.loads(resp["body"])
check("HTTP 200", resp["statusCode"] == 200, str(body))
check("status running", body.get("status") == "running", str(body))
check("execution searches with maxResults == EXPORT_DEPTH (0)", cap.get("maxResults") == 0, str(cap.get("maxResults")))
check("execution runs under the export sessionId", cap.get("sessionId") == SID + "_x")
check("summary key under the export sessionId",
      body.get("s3_key") == f"results/{SID}_x/{body.get('job_id')}.json")
check("export_session_id returned", body.get("export_session_id") == SID + "_x")
check("same version/dbSize/shards as a search",
      cap.get("version") == "V2" and cap.get("dbSize") == 88181580142 and len(cap.get("shards", [])) == 2)

print("\n=== orchestrator: public export goes through admission, name fits ===")
orch, cap, sfn = load_orchestrator({})
resp = orch.handler({"action": "export", "sessionId": SID, "sequence": SEQ, "clientIp": "1.2.3.4"}, None)
check("public export admitted", resp["statusCode"] == 200, resp["body"])
check("execution name <= 80 and ends in the full jobId",
      len(cap["_name"]) <= 80 and cap["_name"].endswith(json.loads(resp["body"])["job_id"]), cap["_name"])

print("\n=== orchestrator: finished export is returned, no fan-out ===")
prefix = f"results/{SID}_x/"
done = {prefix: {"Contents": [
            {"Key": f"{prefix}old.json", "LastModified": datetime(2026, 10, 1, tzinfo=timezone.utc)},
            {"Key": f"{prefix}new.json", "LastModified": datetime(2026, 10, 27, tzinfo=timezone.utc)}]},
        f"{prefix}new/hits.tsv.gz": {"KeyCount": 1},
        f"{prefix}old/hits.tsv.gz": {"KeyCount": 1}}
orch, cap, sfn = load_orchestrator(done)
body = json.loads(orch.handler({"action": "export", "sessionId": SID, "sequence": SEQ}, None)["body"])
check("status ready", body.get("status") == "ready", str(body))
check("newest export chosen", body.get("job_id") == "new", str(body))
check("hits key returned", body.get("hits_key") == f"{prefix}new/hits.tsv.gz")
check("no execution started", not sfn.start_execution.called)

print("\n=== orchestrator: summary whose file expired -> re-export ===")
expired = {prefix: done[prefix]}
orch, cap, sfn = load_orchestrator(expired)
body = json.loads(orch.handler({"action": "export", "sessionId": SID, "sequence": SEQ}, None)["body"])
check("re-runs when hits.tsv.gz is gone", body.get("status") == "running" and sfn.start_execution.called, str(body))

print("\n=== orchestrator: search unchanged ===")
orch, cap, sfn = load_orchestrator({})
body = json.loads(orch.handler({"action": "search", "sessionId": SID, "sequence": SEQ}, None)["body"])
check("search still capped at RESULT_DEPTH", cap.get("maxResults") == common.RESULT_DEPTH)
check("search still under its own sessionId", cap.get("sessionId") == SID)
check("search response has no export fields", "status" not in body and "export_session_id" not in body, str(body))

print("\n=== worker: export runs -k 0 without sequences, writes a sorted gzip part ===")
import worker  # noqa: E402

seen = {}


def fake_run(cmd, capture_output, text):
    seen["cmd"] = cmd
    out = cmd[cmd.index("-o") + 1]
    rows = ["LG5\t1\t90\t1\t90\t90\t50.0\t1e-20\t100.0\t300",
            "NR2\t1\t90\t1\t90\t90\t60.0\t1e-40\t180.0\t310",
            "PL1\t1\t90\t1\t90\t90\t99.0\t1e-60\t250.0\t290",
            "LG3\t1\t90\t1\t90\t90\t60.0\t1e-40\t180.0\t305"]
    open(out, "w").write("\n".join(rows) + "\n")
    return types.SimpleNamespace(returncode=0, stderr="")


with mock.patch.object(worker.subprocess, "run", side_effect=fake_run):
    tsv = worker.run_shard_search(">q\nMKL\n", "/tmp/shard_x.fa.zst", 0, dbsize=88181580142)
cmd = seen["cmd"]
check("-k 0", cmd[cmd.index("-k") + 1] == "0")
check("export columns, no full_sseq",
      cmd[cmd.index("--outfmt") + 1:] == ["6", *common.EXPORT_OUTFMT], str(cmd[cmd.index("--outfmt"):]))
with mock.patch.object(worker.subprocess, "run", side_effect=fake_run):
    worker.run_shard_search(">q\nMKL\n", "/tmp/shard_x.fa.zst", 500, dbsize=88181580142)
check("search keeps full_sseq", seen["cmd"][-1] == "full_sseq")

puts = {}
with mock.patch.object(worker, "download_shard", return_value="/tmp/shard_x.fa.zst"), \
     mock.patch.object(worker, "run_shard_search", return_value=tsv), \
     mock.patch.object(worker.s3, "put_object", side_effect=lambda **kw: puts.update({kw["Key"]: kw})):
    worker.handler({"sessionId": SID + "_x", "jobId": "J", "shardIndex": 3, "queryFasta": ">q\nMKL",
                    "maxResults": 0, "shardKey": "diamond/V2/shard_03.fa.zst"}, None)
part = puts.get(f"results/{SID}_x/J/parts/shard_3.tsv.gz")
check("export part is .tsv.gz, tagged reapable", part is not None and part["Tagging"] == "reapable=true", str(list(puts)))
ids = [l.split("\t")[0] for l in gzip.decompress(part["Body"]).decode().splitlines()]
check("part sorted bitscore desc, then evalue, then id", ids == ["PL1", "LG3", "NR2", "LG5"], str(ids))

print("\n=== aggregator: merge 32 sorted parts into one ranked file ===")
random.seed(7)
n_shards, rows_all, part_bodies = 32, [], []
for sh in range(n_shards):
    rows = []
    for i in range(random.randint(0, 400)):
        tid = random.choice(["LG", "NR", "PL"]) + str(sh * 1000 + i)
        bits = round(random.uniform(30, 600), 1)
        ev = f"{10 ** -random.uniform(3.1, 150):.2e}"
        rows.append([tid, "1", "200", "5", "204", "200", "55.5", ev, str(bits), "321"])
    rows_all.extend(rows)
    part_bodies.append(gzip.compress(common.sort_export_rows(
        "".join("\t".join(r) + "\n" for r in rows)).encode()))

written = {}


def put_object(**kw):
    body = kw["Body"]
    written[kw["Key"]] = dict(kw, Body=body.read() if hasattr(body, "read") else body)


s3 = mock.MagicMock()
s3.get_paginator.return_value.paginate.side_effect = lambda **kw: [
    {"Contents": [{"Key": f"{kw['Prefix']}shard_{i}.tsv.gz"} for i in range(n_shards)]
     + [{"Key": f"{kw['Prefix']}shard_{i}.meta.json"} for i in range(n_shards)]}]
s3.get_object.side_effect = lambda Bucket, Key: {"Body": io.BytesIO(
    part_bodies[int(Key.split("shard_")[1].split(".")[0])] if Key.endswith(".tsv.gz")
    else json.dumps({"shard_index": int(Key.split("shard_")[1].split(".")[0])}).encode())}
s3.put_object.side_effect = put_object
with mock.patch("boto3.client", return_value=s3):
    sys.modules.pop("aggregator", None)
    import aggregator
aggregator.s3 = s3
aggregator.fetch_metadata = lambda ids: (_ for _ in ()).throw(AssertionError("export must not hit RDS"))

ret = aggregator.handler({
    "sessionId": SID + "_x", "jobId": "J", "querySequence": SEQ, "queryHeader": "q1",
    "maxResults": 0, "shards": [{"shardIndex": i} for i in range(n_shards)],
    "version": "V2", "databaseRelease": "v260701", "searchVersion": common.SEARCH_VERSION,
    "corpus": MANIFEST["corpus"], "dbSize": 88181580142,
}, None)

hits = written.get(f"results/{SID}_x/J/hits.tsv.gz")
summary = json.loads(written[f"results/{SID}_x/J.json"]["Body"])
text = gzip.decompress(hits["Body"]).decode()  # multi-member: header + rows
lines = text.splitlines()
header = [l for l in lines if l.startswith("#")]
cols = lines[len(header)].split("\t")
data = [l.split("\t") for l in lines[len(header) + 1:]]
expected = sorted(rows_all, key=common.export_rank_key)

check("header first, carries total + versions",
      any(f"total_hits: {len(rows_all)}" in l for l in header)
      and any("database_version: V2" in l for l in header), "\n".join(header))
check("column row matches EXPORT_COLUMNS", cols == common.EXPORT_COLUMNS, str(cols))
check("every hit present once", len(data) == len(rows_all) == summary["total_hits"],
      f"{len(data)} vs {len(rows_all)}")
check("global order == full sort", [r[1] for r in data] == [r[0] for r in expected])
check("ranks 1..N", [int(r[0]) for r in data] == list(range(1, len(data) + 1)))
check("origin column", all(r[2] == common.hit_origin(r[1]) for r in data))
check("by_origin sums to total", sum(summary["by_origin"].values()) == summary["total_hits"])
check("by_evalue bands correct",
      all(summary["by_evalue"][f"{b:g}"] == sum(float(r[7]) <= b for r in rows_all)
          for b in common.EXPORT_EVALUE_BANDS), str(summary["by_evalue"]))
check("summary points at the file", summary["files"]["hits_tsv_gz"]["key"] == f"results/{SID}_x/J/hits.tsv.gz")
check("summary is an export, format 1.0", summary["kind"] == "export" and summary["export_format"] == "1.0")
check("hits/summary/index all tagged reapable",
      all(written[k].get("Tagging") == "reapable=true"
          for k in (f"results/{SID}_x/J/hits.tsv.gz", f"results/{SID}_x/J.json", f"results/{SID}_x.index")))
check("download named as an attachment", "attachment" in hits.get("ContentDisposition", ""))
check("handler returns total", ret.get("num_results") == len(rows_all))
check("timing.json written", f"results/{SID}_x/J/timing.json" in written)

print("\n=== export top N == search top N (same rows, same ranking) ===")
search_top = sorted(({"target_id": r[0], "bitscore": float(r[8]), "evalue": float(r[7])} for r in rows_all),
                    key=lambda r: (-r["bitscore"], r["evalue"]))[:common.RESULT_DEPTH]
check("export's first 500 IDs are the search's top 500 (as a set)",
      {r[1] for r in data[:common.RESULT_DEPTH]} == {r["target_id"] for r in search_top})

print(f"\n{'ALL PASS' if not FAILURES else f'{len(FAILURES)} FAILED'}")
sys.exit(1 if FAILURES else 0)
