"""End-to-end export check on real data (no AWS).

Usage (DIAMOND 2.2.2 built with zstd on PATH; shards are any local .fa.zst
FASTA DBs, e.g. one production shard split in two):
  DIAMOND_SENSITIVITY=default DIAMOND_FASTA_CROSSBLOCK_MERGE=1 DIAMOND_THREADS=6 \
  python3 scripts/bench/export_e2e.py src/ SHARD_DIR QUERY_DIR QUERY [QUERY ...]
SHARD_DIR holds half_0.fa.zst and half_1.fa.zst; QUERY_DIR holds QUERY.fa.

Drives the REAL orchestrator payload → worker.handler (real DIAMOND 2.2.2,
production flags) per shard → aggregator.handler, for both a normal search and
an export of the same query over two real shards, with S3 as an in-memory stub.
Checks: export total == sum of per-shard -k 0 hits; export rows globally ranked;
every search hit is in the export at the same score, and the export's top 500
is at least as strong rank by rank (DIAMOND's -k is a heuristic, so the capped
search can miss a top-500 hit that the export finds);
summary counts consistent; header readable as one gzip stream.
"""
import gzip
import io
import json
import os
import sys
import types
import unittest.mock as mock

SRC, E2E, QDIR = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path.insert(0, SRC)
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
import common  # noqa: E402

store = {}


class S3Stub:
    def put_object(self, Bucket, Key, Body, **kw):
        store[Key] = Body.read() if hasattr(Body, "read") else (Body if isinstance(Body, bytes) else Body.encode())

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(store[Key])}

    def download_file(self, Bucket, Key, path):
        open(path, "wb").write(store[Key])

    def upload_file(self, path, Bucket, Key, Config=None, ExtraArgs=None):
        store[Key] = open(path, "rb").read()

    def get_paginator(self, _):
        return types.SimpleNamespace(paginate=lambda Bucket, Prefix: [
            {"Contents": [{"Key": k} for k in sorted(store) if k.startswith(Prefix)]}])


with mock.patch("boto3.client", return_value=S3Stub()):
    import worker  # noqa: E402
    import aggregator  # noqa: E402
worker.s3 = aggregator.s3 = S3Stub()
aggregator.fetch_metadata = lambda ids: {}

SHARDS = [f"{E2E}/half_0.fa.zst", f"{E2E}/half_1.fa.zst"]
DBSIZE = 88181580142
FAILS = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)
    if not cond:
        FAILS.append(name)


def run(session, depth, query_header, seq):
    """One job: every shard through worker.handler, then aggregator.handler."""
    job = "job"
    shards = [{"shardIndex": i, "shardKey": f"diamond/T/shard_0{i}.fa.zst"} for i in range(len(SHARDS))]
    for i, path in enumerate(SHARDS):
        with mock.patch.object(worker, "download_shard", return_value=path):
            worker.handler({"sessionId": session, "jobId": job, "shardIndex": i,
                            "queryFasta": f">{query_header}\n{seq}\n", "maxResults": depth,
                            "shardKey": shards[i]["shardKey"], "dbSize": DBSIZE}, None)
    aggregator.handler({"sessionId": session, "jobId": job, "queryHeader": query_header,
                        "querySequence": seq, "maxResults": depth, "shards": shards,
                        "version": "T", "databaseRelease": "v260701",
                        "searchVersion": common.SEARCH_VERSION, "corpus": "test",
                        "dbSize": DBSIZE}, None)
    return json.loads(store[f"results/{session}/{job}.json"])


for q in sys.argv[4:]:
    seq = "".join(l.strip() for l in open(f"{QDIR}/{q}.fa") if not l.startswith(">"))
    print(f"\n=== {q} ({len(seq)} aa) ===", flush=True)
    search = run(q, common.RESULT_DEPTH, q, seq)
    xsid = common.export_session_id(q)
    summary = run(xsid, common.EXPORT_DEPTH, q, seq)
    raw = store[summary["files"]["hits_tsv_gz"]["key"]]
    lines = gzip.decompress(raw).decode().splitlines()
    header = [l for l in lines if l.startswith("#")]
    rows = [l.split("\t") for l in lines[len(header) + 1:]]
    part_hits = sum(gzip.decompress(store[f"results/{xsid}/job/parts/shard_{i}.tsv.gz"]).count(b"\n")
                    for i in range(len(SHARDS)))
    s_hits = search["results"]
    n = len(s_hits)
    print(f"  search: {n} hits; export: {summary['total_hits']} hits, {len(raw) / 1e6:.2f} MB gz; "
          f"by_evalue={summary['by_evalue']} by_origin={summary['by_origin']}", flush=True)
    check("export total == sum of shard parts == rows", summary["total_hits"] == part_hits == len(rows),
          f"{summary['total_hits']} {part_hits} {len(rows)}")
    keys = [(-float(r[11]), float(r[10]), r[1]) for r in rows]  # bitscore, evalue, id columns
    check("export ranked (bitscore desc, evalue asc, id)", keys == sorted(keys))
    # DIAMOND's -k is a heuristic: the capped search can miss a hit that belongs
    # in its top N (seen 2026-10-27: IsPETase, 1/500). So the export must
    # CONTAIN every search hit at the same score, and its top N must be at least
    # as strong rank by rank; the IDs need not be identical.
    exp = {r[1]: (float(r[11]), float(r[10])) for r in rows}
    check("every search hit is in the export, same bitscore",
          all(h["target_id"] in exp and abs(exp[h["target_id"]][0] - h["bitscore"]) < 1e-6 for h in s_hits))
    check("export top N >= search top N, rank by rank",
          all(float(r[11]) >= h["bitscore"] for r, h in zip(rows, s_hits)))
    recovered = [r[1] for r in rows[:n] if r[1] not in {h["target_id"] for h in s_hits}]
    print(f"  export top {n} hits the capped search missed: {len(recovered)} {recovered[:5]}", flush=True)
    check("search size consistent with export total",
          n == min(common.RESULT_DEPTH, summary["total_hits"]), f"{n} vs {summary['total_hits']}")
    fa = gzip.decompress(store[summary["files"]["hits_fasta_gz"]["key"]]).decode().splitlines()
    check("FASTA: one record per row, same order, lengths == slen",
          [h[1:].split(" ")[0] for h in fa[0::2]] == [r[1] for r in rows]
          and all(len(q) == int(r[9]) for q, r in zip(fa[1::2], rows)))
    seqs = {h["target_id"]: h["target_sequence"] for h in s_hits}
    check("FASTA: search hits' sequences identical to the search's target_sequence",
          all(seqs.get(h[1:].split(" ")[0], q) == q for h, q in zip(fa[0::2], fa[1::2])))
    check("header has total + release", any(f"total_hits: {len(rows)}" in l for l in header)
          and any("database_release: v260701" in l for l in header))

print(f"\n{'E2E ALL PASS' if not FAILS else f'E2E {len(FAILS)} FAILED'}", flush=True)
