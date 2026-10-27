#!/usr/bin/env python3
"""
Build the v260701 "Found in Samples" store: for every Logan sequence, the
BioSamples it was found in, with each sample's record and its study's title.

Same block layout as build_metadata_store.py, so src/metadata.py reads it:

  LGS  key = LG number    {"id":"LG7", "n_biosamples":N, "n_bioprojects":M,
                           "samples":[[biosample, bioproject, n_runs, ka_max], ...]}
                          (the first SAMPLE_CAP samples by n_runs, then ka_max)
  BS   key = accession    one BioSample: profile + evidence, isolation source,
                          environment fields, host, country, lat/lon, stated
                          temperature / pH, top species by NCBI STAT reads
  BP   key = accession    BioProject title (NCBI E-utilities)

"Found in" = a contig assembled from one of the sample's runs (Logan) carries
exactly this ORF; the species are the sample's most abundant by read
classification, not an assignment of the sequence.

Runs on EC2 (DuckDB). Needs iGEM credentials and internet (E-utilities).
Usage:
  python3 build_samples_store.py --out <dir>/<tag> --hierarchy hierarchy.tsv [--mem 200GB --threads 30]
"""

import argparse
import glob
import gzip
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--hierarchy", required=True, help="clustering hierarchy.tsv; its LG rows define the corpus Logan set")
ap.add_argument("--meta-s3", default="s3://automated-metadata/metadata-v260701/repo")
ap.add_argument("--runmap", default="s3://automated-metadata/metadata-v260701/inputs/runmap_full.parquet")
ap.add_argument("--sample-cap", type=int, default=200)
ap.add_argument("--block-rows", type=int, default=2000)
ap.add_argument("--mem", default="200GB")
ap.add_argument("--threads", type=int, default=30)
ap.add_argument("--tmp", default="/tmp/petadex_samples_store_tmp")
ap.add_argument("--skip-bp", action="store_true", help="no E-utilities (tests)")
a = ap.parse_args()

OUT, S3, R = a.out.rstrip("/"), a.meta_s3, a.block_rows
os.makedirs(f"{OUT}/work", exist_ok=True)
t0 = time.time()
log = lambda s: print(f"[{time.time() - t0:6.0f}s] {s}", flush=True)
con = duckdb.connect()
con.execute(f"set memory_limit='{a.mem}'; set threads={a.threads}; set temp_directory='{a.tmp}'; "
            "set preserve_insertion_order=false; set max_temp_directory_size='600GB'")
con.execute("install httpfs; load httpfs; set s3_region='us-east-1'")
try:   # S3 inputs need credentials; a local-only run (tests) has none
    con.execute("create secret (type s3, provider credential_chain, chain 'env;config;instance')")
except duckdb.Error as e:
    print(f"no S3 credentials ({str(e).splitlines()[0]}); local inputs only")
q1 = lambda sql: con.execute(sql).fetchone()
POOL = ThreadPoolExecutor(max_workers=a.threads)
state_f = f"{OUT}/work/state.json"
state = json.load(open(state_f)) if os.path.exists(state_f) else {"index": {}, "rows": {}, "lgs_buckets": []}
save = lambda: json.dump(state, open(state_f, "w"))


def _put(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", compresslevel=6) as f:
        f.write(text + "\n")


def write_blocks(ns, table, offset, cols, tag):
    """Blocks offset.. from `table` (has columns id, n, and `cols`), ordered by n."""
    con.execute(f"create or replace temp table cur as select *, {offset} + (row_number() over (order by n) - 1) // {R} block from {table}")
    n = q1("select count(*) from cur")[0]
    obj = "struct_pack(" + ", ".join(f'"{c}" := "{c}"' for c in ["id"] + cols) + ")"
    res = con.execute(f"select block, min(n), string_agg(to_json({obj}), chr(10) order by n) from cur group by block order by block")
    first = []
    while rows := res.fetchmany(64):
        futs = []
        for blk, k, text in rows:
            first.append([k, blk])
            futs.append(POOL.submit(_put, f"{OUT}/blocks/{ns}/block={blk}/data_0.json.gz", text))
        for f in futs:
            f.result()
    log(f"{ns} {tag}: {n:,} rows -> blocks {offset}..{offset + len(first) - 1}")
    return offset + len(first), first, n


# ---- 0. corpus Logan ids (the hierarchy's LG proteins = Thomas's 291,922,243 Logan cores), by lg_biosample bucket
CORPUS = f"{OUT}/work/corpus_lg"
if not os.path.exists(f"{CORPUS}/_done"):
    con.execute(f"""copy (select distinct cast(substr(petadex_id, 3) as bigint) n, cast(substr(petadex_id, 3) as bigint) // 30000000 bucket
        from read_csv('{a.hierarchy}', delim='\t', header=true, quote='', all_varchar=true) where petadex_id like 'LG%')
        to '{CORPUS}' (format parquet, partition_by (bucket))""")
    open(f"{CORPUS}/_done", "w").close()
a.corpus_lg = CORPUS
n_corpus = q1(f"select count(*) from read_parquet('{CORPUS}/*/*.parquet')")[0]
log(f"corpus Logan ids: {n_corpus:,}")

# ---- 1. LGS: per corpus Logan sequence, its BioSamples (capped), per lg_biosample bucket
buckets = sorted(int(p.rsplit("=", 1)[1]) for p in glob.glob(f"{a.corpus_lg}/bucket=*"))
state["index"].setdefault("LGS", []); state["rows"].setdefault("LGS", 0)
off = (state["index"]["LGS"][-1][1] + 1) if state["index"]["LGS"] else 0
for b in buckets:
    if b in state["lgs_buckets"]:
        continue
    con.execute(f"""create or replace temp table lgs as
        with l as (select lb.lg, lb.biosample, lb.bioproject, lb.n_runs, lb.ka_max,
                          row_number() over (partition by lb.lg order by lb.n_runs desc, lb.ka_max desc, lb.biosample) r
                   from read_parquet('{S3}/data/t3/lg_biosample/bucket={b}/data.parquet') lb
                   semi join read_parquet('{a.corpus_lg}/bucket={b}/*.parquet') c on c.n = lb.lg)
        select 'LG' || lg id, lg n, count(*)::int n_biosamples, count(distinct bioproject)::int n_bioprojects,
               list([biosample, bioproject, n_runs::varchar, round(ka_max, 2)::varchar] order by r) filter (where r <= {a.sample_cap}) samples
        from l group by lg""")
    off, idx, n = write_blocks("LGS", "lgs", off, ["n_biosamples", "n_bioprojects", "samples"], f"bucket {b}")
    state["index"]["LGS"] += idx; state["rows"]["LGS"] += n; state["lgs_buckets"].append(b); save()

# ---- 2. BS: every BioSample linked to a corpus Logan sequence
if "BS" not in state["index"]:
    con.execute(f"""create or replace temp table bs_ids as select distinct biosample from
        read_parquet('{S3}/data/t3/lg_biosample/*/data.parquet') lb
        semi join read_parquet('{a.corpus_lg}/*/*.parquet') c on c.n = lb.lg""")
    log(f"BioSamples linked to corpus sequences: {q1('select count(*) from bs_ids')[0]:,}")
    # top species per BioSample: STAT self_reads summed over the sample's runs
    con.execute(f"""create or replace temp table top as
        with rb as (select run, biosample from '{a.runmap}' where biosample in (select biosample from bs_ids)),
             s as (select rb.biosample, st.species_taxid, sum(st.self_reads) reads
                   from read_parquet('{S3}/data/t3/stat/*.parquet') st join rb using (run)
                   where st.self_reads > 0 group by 1, 2),
             r as (select *, row_number() over (partition by biosample order by reads desc) k,
                          sum(reads) over (partition by biosample) tot from s)
        select r.biosample, list([t.name, round(r.reads / r.tot, 4)::varchar] order by r.k) top_species
        from r left join '{S3}/data/ref/taxonomy.parquet' t on t.taxid = r.species_taxid where r.k <= 5 group by 1""")
    log("top species done")
    con.execute(f"""create or replace temp table bsc as
        select p.biosample id, p.biosample n, bp.bioproject, p.profile, p.profile_evidence, p.isolation_source,
               p.env_broad_scale, p.env_medium, p.host, p.country_calc country, p.latitude, p.longitude,
               p.temperature_c, p.ph, t.top_species
        from bs_ids i join '{S3}/tier3/profiles/results/biosample_profiles.parquet' p on p.biosample = i.biosample
        left join (select biosample, min(bioproject) bioproject from '{S3}/data/t3/biosample_bp.parquet' group by 1) bp on bp.biosample = p.biosample
        left join top t on t.biosample = p.biosample""")
    cols = ["bioproject", "profile", "profile_evidence", "isolation_source", "env_broad_scale", "env_medium", "host",
            "country", "latitude", "longitude", "temperature_c", "ph", "top_species"]
    _, idx, n = write_blocks("BS", "bsc", 0, cols, "all")
    state["index"]["BS"], state["rows"]["BS"] = idx, n; save()

# ---- 3. BP: BioProject titles from NCBI E-utilities (3 requests/s without a key)
if "BP" not in state["index"]:
    projs = [r[0] for r in con.execute(f"""select distinct bioproject from read_parquet('{S3}/data/t3/lg_biosample/*/data.parquet') lb
        semi join read_parquet('{a.corpus_lg}/*/*.parquet') c on c.n = lb.lg where bioproject is not null""").fetchall()]
    log(f"BioProjects: {len(projs):,}")
    titles = {}
    def get(url, data=None):
        for k in range(5):
            try:
                return json.load(urllib.request.urlopen(url, data=data, timeout=60))
            except Exception:
                time.sleep(2 + 3 * k)
        return None
    if not a.skip_bp:
        E = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        for i in range(0, len(projs), 200):
            chunk = projs[i:i + 200]
            r = get(E + "esearch.fcgi", urllib.parse.urlencode(dict(db="bioproject", term=" OR ".join(chunk), retmax=2000, retmode="json")).encode())
            ids = (r or {}).get("esearchresult", {}).get("idlist", []); time.sleep(0.34)
            for j in range(0, len(ids), 300):
                s = get(E + "esummary.fcgi", urllib.parse.urlencode(dict(db="bioproject", id=",".join(ids[j:j + 300]), retmode="json")).encode())
                for u in (s or {}).get("result", {}).get("uids", []):
                    e = s["result"][u]
                    titles[e.get("project_acc")] = e.get("project_title")
                time.sleep(0.34)
            if i % 20000 == 0:
                log(f"  titles {len(titles):,} after {i + len(chunk):,} projects")
    con.execute("create or replace temp table bp (id varchar, n varchar, title varchar)")
    con.executemany("insert into bp values (?, ?, ?)", [(p, p, titles.get(p)) for p in projs])
    _, idx, n = write_blocks("BP", "bp", 0, ["title"], "all")
    state["index"]["BP"], state["rows"]["BP"] = idx, n
    state["bp_titles_found"] = sum(1 for p in projs if titles.get(p)); save()

json.dump({ns: {"keys": [k for k, _ in v], "blocks": [b for _, b in v]} for ns, v in state["index"].items()},
          open(f"{OUT}/index.json", "w"), separators=(",", ":"))
json.dump({"n_sequences": 0, "weight": {}}, open(f"{OUT}/background.json", "w"))   # the reader expects one
json.dump({"store_format": "1.0", "kind": "samples", "tag": os.path.basename(OUT), "database_release": "v260701",
           "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "block_rows": R, "sample_cap": a.sample_cap,
           "rows": state["rows"], "bp_titles_found": state.get("bp_titles_found"),
           "blocks": {ns: len(v) for ns, v in state["index"].items()},
           "key": {"LGS": "numeric part of the LG id", "BS": "BioSample accession", "BP": "BioProject accession"},
           "sample_columns": ["biosample", "bioproject", "n_runs", "ka_max"]},
          open(f"{OUT}/manifest.json", "w"), indent=1)
log("done")
