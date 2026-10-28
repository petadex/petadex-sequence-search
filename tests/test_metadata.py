"""Tier-A metadata enrichment (src/metadata.py, search_version 1.5.0).

Standalone (no pytest): `python3 tests/test_metadata.py`. Builds a tiny store on
disk in the layout scripts/db/build_metadata_store.py writes, so the real reader
runs end to end without AWS.
"""
import gzip
import json
import os
import sys
import tempfile
import types
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import metadata  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def make_store(root, release="v260701", block_rows=2):
    """Cards for PL1, PL10, NR5, LG7, LG8, LG9 and three clusters, BLOCK_ROWS rows per block."""
    c90a, c90b, c60, c30 = "PL1|C001|1e-40|1|200|C001", "LG8|C001|1e-30|1|190|C001", "PL1|C001|1e-40|1|200|C001", "PL1|C001|1e-40|1|200|C001"
    cards = {
        "PL": [{"id": "PL1", "enzyme_names": "ISPETase", "c90": c90a, "c60": c60, "c30": c30},
               {"id": "PL10", "enzyme_names": "decoy", "c90": c90a, "c60": c60, "c30": c30}],
        "NR": [{"id": "NR5", "organism": "Ideonella", "c90": c90a, "c60": c60, "c30": c30}],
        "LG": [{"id": "LG7", "prof_hist": {"soil": 3, "marine": 1}, "c90": c90b, "c60": c60, "c30": c30},
               {"id": "LG8", "prof_hist": {"soil": 1}, "c90": c90b, "c60": c60, "c30": c30},
               {"id": "LG9", "prof_hist": {}, "c90": c90b, "c60": c60, "c30": c30}],
        "c90": [{"id": c90a, "t1_n_members": 2}, {"id": c90b, "t1_n_members": 0}],
        "c60": [{"id": c60, "n_domains": 7}],
        "c30": [{"id": c30, "n_domains": 7}],
    }
    index = {}
    for ns, rows in cards.items():
        key = (lambda r: r["id"]) if ns.startswith("c") else (lambda r: int(r["id"][2:]))
        rows = sorted(rows, key=key)
        index[ns] = {"keys": [], "blocks": []}
        for b in range(0, len(rows), block_rows):
            chunk = rows[b:b + block_rows]
            blk = b // block_rows
            index[ns]["keys"].append(key(chunk[0])); index[ns]["blocks"].append(blk)
            d = os.path.join(root, "blocks", ns, f"block={blk}")
            os.makedirs(d)
            with gzip.open(os.path.join(d, "data_0.json.gz"), "wt") as f:
                f.write("\n".join(json.dumps(r, separators=(",", ":")) for r in chunk) + "\n")
    json.dump(index, open(os.path.join(root, "index.json"), "w"))
    json.dump({"n_sequences": 100, "weight": {"soil": 10.0, "marine": 40.0}}, open(os.path.join(root, "background.json"), "w"))
    json.dump({"tag": "test_store", "database_release": release}, open(os.path.join(root, "manifest.json"), "w"))
    json.dump({"c90": {c90a: [{"id": "PL1", "enzyme_names": "ISPETase"}]}, "c60": {c60: [{"id": "PL1", "enzyme_names": "ISPETase"}]}},
              open(os.path.join(root, "pl_members.json"), "w"))
    return root


def hit(tid, pident=90.0):
    return {"target_id": tid, "percent_identity": pident}


root = make_store(tempfile.mkdtemp())

print("\n=== parse_target / rep ===")
check("LG id parses", metadata.parse_target("LG113721682") == ("LG", 113721682))
check("v1.1 header does not parse", metadata.parse_target("1|WP_054022242.1|||||") is None)
check("rep of a domain id", metadata.rep("PL228|C001|3.4e-40|48|261|C001") == "PL228")

print("\n=== lookup ===")
store = metadata.Store(root)
cards, nblk = store.lookup([("PL", 1, "PL1"), ("LG", 8, "LG8"), ("LG", 9, "LG9")])
check("cards found", set(cards) == {"PL1", "LG8", "LG9"}, sorted(cards))
check("PL1 is not confused with PL10", cards["PL1"]["enzyme_names"] == "ISPETase")
check("only the needed blocks are read (PL 0, LG 0 and 1)", nblk == 3, nblk)
check("a key below the first block has no block", store.block_of("LG", 3) is None)

print("\n=== enrich ===")
results = [hit("PL1", 100.0), hit("NR5", 95.0), hit("LG7", 80.0), hit("LG8", 70.0), hit("LG404", 60.0)]
out = metadata.enrich(results, database_release="v260701", store_uri=root)
check("status partial when an id is missing", out["status"] == "partial", out["status"])
check("store tag reported", out["store"] == "test_store")
check("missing id has no card", "LG404" not in out["cards"])
check("cluster cards attached", out["clusters"]["c90"]["PL1|C001|1e-40|1|200|C001"]["t1_n_members"] == 2)
check("Plasticome members on the 60 % card", out["clusters"]["c60"]["PL1|C001|1e-40|1|200|C001"]["pl_members"][0]["id"] == "PL1")
check("no Plasticome member -> empty list", out["clusters"]["c90"]["LG8|C001|1e-30|1|190|C001"]["pl_members"] == [])
s = out["summary"]
check("by_origin", s["by_origin"] == {"plasticome": 1, "ncbi_nr": 1, "logan": 3}, s["by_origin"])
env = {e["profile"]: e for e in s["environments"]["profiles"]}
# LG7 splits 0.75 soil / 0.25 marine, LG8 is 1.0 soil -> soil 1.75 of 2 sequences = 0.875; background 0.10
check("per-sequence share", abs(env["soil"]["share"] - 0.875) < 1e-9, env["soil"])
check("x_expected vs a random sequence", abs(env["soil"]["x_expected"] - 8.75) < 1e-9, env["soil"])
check("sequences without a profile are left out", s["environments"]["n_sequences"] == 2)
check("clusters rolled up", s["clusters"]["c30"]["n"] == 1 and s["clusters"]["c30"]["top"][0]["hits"] == 4, s["clusters"]["c30"])
check("cluster best identity", s["clusters"]["c90"]["top"][0]["best_pident"] == 100.0)

all_ok = metadata.enrich([hit("PL1"), hit("LG9")], store_uri=root)
check("status ok when every id is found", all_ok["status"] == "ok", all_ok["status"])

none = metadata.enrich(results, store_uri="")
check("no store configured -> unavailable", none["status"] == "unavailable" and none["cards"] == {})
other = metadata.enrich(results, database_release="v1.1", store_uri=root)
check("store for another release -> unavailable", other["status"] == "unavailable" and other["cards"] == {})
broken = metadata.enrich(results, store_uri=tempfile.mkdtemp())
check("unreadable store -> failed, never raises", broken["status"] == "failed" and broken["cards"] == {})

print("\n=== aggregator integration ===")
written = {}
part = "".join(f"{t}\t1\t100\t1\t100\t100\t{p}\t1e-20\t{b}\tMKV\n"
               for t, p, b in [("PL1", 100.0, 300), ("LG7", 80.0, 200), ("LG404", 60.0, 100)])
s3 = mock.MagicMock()
s3.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": "results/s/j/parts/shard_0.tsv"}]}]
s3.get_object.return_value = {"Body": types.SimpleNamespace(read=lambda: part.encode())}
s3.put_object.side_effect = lambda **kw: written.update({kw["Key"]: kw["Body"]})
with mock.patch("boto3.client", return_value=s3):
    sys.modules.pop("aggregator", None)
    import aggregator  # noqa: E402
aggregator.s3 = s3
with mock.patch.dict(os.environ, {"METADATA_STORE": root}):
    aggregator.handler({"sessionId": "s", "jobId": "j", "querySequence": "MKV", "maxResults": 500,
                        "shards": [{"shardIndex": 0}], "version": "V", "databaseRelease": "v260701",
                        "searchVersion": "1.5.0"}, None)
doc = json.loads(written["results/s/j.json"])
check("hits carry their card", doc["results"][0]["metadata"]["enzyme_names"] == "ISPETase")
check("missing hit has metadata null", doc["results"][2]["metadata"] is None)
check("result carries enrichment fields", doc["enrichment_status"] == "partial" and doc["metadata_version"] == "test_store"
      and doc["summary"]["n_hits"] == 3 and "c90" in doc["clusters"], {k: doc.get(k) for k in ("enrichment_status", "metadata_version")})
check("hit order unchanged", [r["target_id"] for r in doc["results"]] == ["PL1", "LG7", "LG404"])

print("\n=== samples (Found in Samples) ===")


def make_samples_store(root):
    cards = {"LGS": [{"id": "LG7", "n_biosamples": 3, "n_bioprojects": 2,
                      "samples": [["SAMN2", "PRJNA1", "4", "12.5"], ["SAMEA9", "PRJEB3", "1", "3.0"]]}],
             "BS": [{"id": "SAMEA9", "bioproject": "PRJEB3", "profile": "marine", "isolation_source": "seawater", "top_species": None},
                    {"id": "SAMN2", "bioproject": "PRJNA1", "profile": "soil", "country": "china", "temperature_c": 25.0,
                     "top_species": [["Escherichia coli", "0.75"], ["Bacillus subtilis", "0.25"]]}],
             "BP": [{"id": "PRJEB3", "title": "Tara Oceans"}, {"id": "PRJNA1", "title": "Soil survey"}]}
    index = {}
    for ns, rows in cards.items():
        key = (lambda r: int(r["id"][2:])) if ns == "LGS" else (lambda r: r["id"])
        rows = sorted(rows, key=key)
        index[ns] = {"keys": [key(rows[0])], "blocks": [0]}
        d = os.path.join(root, "blocks", ns, "block=0"); os.makedirs(d)
        with gzip.open(os.path.join(d, "data_0.json.gz"), "wt") as f:
            f.write("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n")
    json.dump(index, open(os.path.join(root, "index.json"), "w"))
    json.dump({"n_sequences": 0, "weight": {}}, open(os.path.join(root, "background.json"), "w"))
    json.dump({"tag": "test_samples", "database_release": "v260701", "sample_cap": 200}, open(os.path.join(root, "manifest.json"), "w"))
    return root


sroot = make_samples_store(tempfile.mkdtemp())
sm = metadata.samples("LG7", store_uri=sroot)
check("samples ok", sm["status"] == "ok" and sm["n_biosamples"] == 3 and len(sm["samples"]) == 2, sm["status"])
first = sm["samples"][0]
check("sample keeps the store's order (by runs)", first["biosample"] == "SAMN2")
check("study title joined", first["bioproject_title"] == "Soil survey")
check("numbers typed", first["n_runs"] == 4 and first["ka_max"] == 12.5)
check("species shares typed", first["top_species"] == [["Escherichia coli", 0.75], ["Bacillus subtilis", 0.25]])
check("no species -> empty list", sm["samples"][1]["top_species"] == [])
check("unknown Logan id -> not_found", metadata.samples("LG8", store_uri=sroot)["status"] == "not_found")
check("non-Logan id -> not_applicable", metadata.samples("NR5", store_uri=sroot)["status"] == "not_applicable")
check("no store -> unavailable", metadata.samples("LG7", store_uri="")["status"] == "unavailable")
try:
    metadata.samples("WP_1.1", store_uri=sroot); check("malformed id raises", False)
except ValueError:
    check("malformed id raises", True)

print("\n=== samples batch ===")
sb = metadata.samples_batch(["LG7", "LG8", "NR5", "LG7"], store_uri=sroot)
check("batch ok", sb["status"] == "ok" and sb["store"] == "test_samples" and sb["limit"] == 50, sb["status"])
check("batch per-id statuses", {k: v["status"] for k, v in sb["results"].items()} == {"LG7": "ok", "LG8": "not_found", "NR5": "not_applicable"})
check("batch rows are refs with typed numbers", sb["results"]["LG7"]["samples"] == [["SAMN2", 4, 12.5], ["SAMEA9", 1, 3.0]])
check("batch keeps true totals", sb["results"]["LG7"]["n_biosamples"] == 3 and sb["results"]["LG7"]["n_bioprojects"] == 2)
check("batch biosample records once each", sorted(sb["biosamples"]) == ["SAMEA9", "SAMN2"] and sb["biosamples"]["SAMN2"]["profile"] == "soil")
check("batch study titles", sb["bioprojects"] == {"PRJEB3": "Tara Oceans", "PRJNA1": "Soil survey"})
check("batch limit trims rows", metadata.samples_batch(["LG7"], limit=1, store_uri=sroot)["results"]["LG7"]["samples"] == [["SAMN2", 4, 12.5]])
check("batch no store -> unavailable", metadata.samples_batch(["LG7"], store_uri="")["status"] == "unavailable")
for bad_args, what in (([], "empty"), ("LG7", "not a list"), (["LG7", "WP_1.1"], "malformed id"),
                       (["LG7"] * 501, "too many ids"), (["LG7", 7], "non-string id")):
    try:
        metadata.samples_batch(bad_args, store_uri=sroot); check(f"batch rejects {what}", False)
    except ValueError:
        check(f"batch rejects {what}", True)
for lim in (0, 201, "5", True):
    try:
        metadata.samples_batch(["LG7"], limit=lim, store_uri=sroot); check(f"batch rejects limit {lim!r}", False)
    except ValueError:
        check(f"batch rejects limit {lim!r}", True)
check("keywords match recorded text", metadata.keywords({"isolation_source": "Activated sludge, WWTP"}) == ["waste"])
check("keywords use the study title", metadata.keywords({"isolation_source": "soil"}, "PET bottle plastisphere") == ["plastic"])
check("keywords: none for plain text", metadata.keywords({"isolation_source": "soil", "host": None}) == [])
check("keywords: 'dumped' is not 'dump'", metadata.keywords({"isolation_source": "dumped soil"}) == [])

with mock.patch("boto3.client", return_value=mock.MagicMock()):
    sys.modules.pop("orchestrator", None)
    import orchestrator  # noqa: E402
with mock.patch.dict(os.environ, {"SAMPLES_STORE": sroot}):
    ok = orchestrator.handler({"action": "samples", "id": "LG7"}, None)
    bad = orchestrator.handler({"action": "samples", "id": "nonsense"}, None)
    missing = orchestrator.handler({"action": "samples", "id": "LG8"}, None)
    notlg = orchestrator.handler({"action": "samples", "id": "NR5"}, None)
    bok = orchestrator.handler({"action": "samples", "ids": ["LG7", "NR5"], "limit": 1}, None)
    bbad = orchestrator.handler({"action": "samples", "ids": ["LG7", "nonsense"]}, None)
    bbody = orchestrator.handler({"body": json.dumps({"action": "samples", "ids": ["LG7"]})}, None)
with mock.patch.dict(os.environ, {"SAMPLES_STORE": ""}):
    nostore = orchestrator.handler({"action": "samples", "id": "LG7"}, None)
    bnostore = orchestrator.handler({"action": "samples", "ids": ["LG7"]}, None)
check("orchestrator batch -> 200", bok["statusCode"] == 200 and json.loads(bok["body"])["results"]["LG7"]["samples"] == [["SAMN2", 4, 12.5]], bok.get("statusCode"))
check("orchestrator batch malformed -> 400", bbad["statusCode"] == 400, bbad.get("statusCode"))
check("orchestrator batch via API body -> 200", bbody["statusCode"] == 200, bbody.get("statusCode"))
check("orchestrator batch no store -> 503", bnostore["statusCode"] == 503, bnostore.get("statusCode"))
with mock.patch.dict(os.environ, {"SAMPLES_STORE": sroot}), mock.patch.object(orchestrator, "BATCH_RESPONSE_MAX", 10):
    big = orchestrator.handler({"action": "samples", "ids": ["LG7"]}, None)
check("orchestrator batch over the size cap -> 400", big["statusCode"] == 400 and "too large" in json.loads(big["body"])["error"], big.get("statusCode"))
check("not_found -> 404", missing["statusCode"] == 404, missing.get("statusCode"))
check("non-Logan id -> 400", notlg["statusCode"] == 400, notlg.get("statusCode"))
check("no store configured -> 503", nostore["statusCode"] == 503, nostore.get("statusCode"))
check("orchestrator samples action -> 200", ok["statusCode"] == 200 and json.loads(ok["body"])["samples"][0]["biosample"] == "SAMN2", ok.get("statusCode"))
check("orchestrator rejects a malformed id with 400", bad["statusCode"] == 400, bad.get("statusCode"))

print(f"\n{len(FAILURES)} FAILURE(S): {FAILURES}" if FAILURES else "\nALL PASSED")
sys.exit(1 if FAILURES else 0)
