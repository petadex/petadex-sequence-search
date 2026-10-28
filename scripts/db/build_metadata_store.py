#!/usr/bin/env python3
"""
Build the v260701 metadata store the aggregator enriches search hits from.

One "card" per searchable sequence (298,687,346 = PAZy 486 + nr 6,764,617 +
Logan 291,922,243, the same set as PETadex.seq.v260701) plus one per cluster at
90 / 60 / 30 %, written as small gzipped NDJSON blocks with a sorted index:

  <out>/index.json                     first key of every block, per namespace
  <out>/blocks/<ns>/block=<k>/data_0.json.gz
  <out>/background.json                per-sequence environment background
  <out>/manifest.json                  counts, inputs, schema, build tag

Namespaces: PL / NR / LG (key = the numeric part of the petadex_id) and
c90 / c60 / c30 (key = the cluster's representative domain_id, a string).
Each block holds BLOCK_ROWS rows; every line starts with `{"id":"<key>",` so a
reader can skip the lines it does not want without parsing them. The search
fetches only the blocks its hits fall in (src/metadata.py), so a lookup is a
handful of small ranged GETs instead of a database round-trip.

Sources (all v260701, see the vault "PETadex Metadata v260701"):
  - clusters      petadex-clustering Run 1 hierarchy.tsv (domain -> c90/c60/c30)
  - PL            metadata data/t1/plasticome_metadata.parquet (with the curation sheet)
  - NR            data/t2/id_organism + id_biosample + nr_header (in_clustering)
  - LG            s3 .../data/t3/lg_metadata/bucket=*/data.parquet
  - c90 / c30     s3 .../data/joint/cluster_c{90,30}.parquet ; c60 cluster_stats_60.tsv

Runs on watson (DuckDB, memory-capped). Stages resume from their outputs.
LG reads need iGEM credentials in the environment, e.g.
  eval "$(aws configure export-credentials --profile igem-dennis --format env)"

Usage:
  python3 build_metadata_store.py --out /franklin/.../metadata_store/<tag> [--mem 24GB --threads 16]
Upload (rnalab account) afterwards:
  aws s3 sync <out> s3://petadex/metadata/v260701/<tag>/ --exclude 'work/*'
"""

import argparse
import gzip
import glob
import json
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb

BUCKET = 30_000_000   # lg_metadata / lg_biosample partition width (petadex-metadata-v260701 lib/common.py)
NS_CODE = {"PL": 1, "NR": 2, "LG": 3}

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--meta", default="/watson/rnalab/dzhu/petadex-metadata-v260701/data")
ap.add_argument("--meta-s3", default="s3://automated-metadata/metadata-v260701/repo")
ap.add_argument("--hierarchy", default="/watson/rnalab/dzhu/petadex-clustering/results/hierarchy.tsv")
ap.add_argument("--c60-stats", default="/watson/rnalab/dzhu/petadex-clustering/results/cluster_stats_60.tsv")
ap.add_argument("--c90", default=None, help="cluster_c90.parquet (default: <meta-s3>/data/joint/)")
ap.add_argument("--c30", default=None, help="cluster_c30.parquet (default: <meta-s3>/data/joint/)")
ap.add_argument("--block-rows", type=int, default=500)   # 500: ~40 % faster lookups than 2000 (in-region, 2026-10-28)
ap.add_argument("--mem", default="24GB")
ap.add_argument("--threads", type=int, default=16)
ap.add_argument("--tmp", default="/tmp/petadex_metadata_store_tmp")
ap.add_argument("--no-count-check", action="store_true", help="skip the corpus-size check (tests on a slice)")
a = ap.parse_args()

OUT, M, S3, R = a.out.rstrip("/"), a.meta, a.meta_s3, a.block_rows
WORK = f"{OUT}/work"
os.makedirs(WORK, exist_ok=True)
t0 = time.time()
log = lambda s: print(f"[{time.time() - t0:6.0f}s] {s}", flush=True)
con = duckdb.connect()
con.execute(f"set memory_limit='{a.mem}'; set threads={a.threads}; set temp_directory='{a.tmp}'; "
            "set preserve_insertion_order=false; set max_temp_directory_size='300GB'")
con.execute("install httpfs; load httpfs; set s3_region='us-east-1'")
try:   # S3 inputs need credentials; a local-only run (tests) has none
    con.execute("create secret (type s3, provider credential_chain, chain 'env;config;instance')")
except duckdb.Error as e:
    print(f"no S3 credentials ({str(e).splitlines()[0]}); local inputs only")
q1 = lambda sql: con.execute(sql).fetchone()

# ---- 1. clusters: one row per protein (its first core by domain_id), partitioned like lg_metadata
CL = f"{WORK}/clusters"
if not os.path.exists(f"{CL}/_done"):
    shutil.rmtree(CL, ignore_errors=True)
    con.execute(f"""copy (
        select petadex_id, substr(petadex_id, 1, 2) ns, cast(substr(petadex_id, 3) as bigint) n,
               case when petadex_id like 'LG%' then cast(substr(petadex_id, 3) as bigint) // {BUCKET} else 0 end bucket,
               count(*)::int n_cores, arg_min(component, domain_id) component,
               arg_min(c90, domain_id) c90, arg_min(c60, domain_id) c60, arg_min(c30, domain_id) c30
        from read_csv('{a.hierarchy}', delim='\t', header=true, quote='', all_varchar=true)
        group by petadex_id) to '{CL}' (format parquet, partition_by (ns, bucket))""")
    open(f"{CL}/_done", "w").close()
cl = lambda ns, b="*": f"read_parquet('{CL}/ns={ns}/bucket={b}/*.parquet')"
counts = {ns: q1(f"select count(*) from {cl(ns)}")[0] if glob.glob(f"{CL}/ns={ns}/bucket=*/*.parquet") else 0
          for ns in NS_CODE}
log(f"clusters: {counts}")
EXPECTED = {"PL": 486, "NR": 6_764_617, "LG": 291_922_243}   # PETadex.seq.v260701
# The corpus, not the hierarchy, defines the cards: Run 1 still holds PL559, which the corpus dropped.
CORPUS = {"PL": f"select petadex_id from '{M}/t2/pl_in_clustering.parquet'",
          "NR": f"select petadex_id from '{M}/t2/nr_header.parquet' where upper(in_clustering::varchar) = 'TRUE'"}
if a.no_count_check:   # test slice: only the corpus ids present in the slice's hierarchy
    CORPUS = {ns: f"select petadex_id from ({sql}) semi join {cl(ns)} c using (petadex_id)" for ns, sql in CORPUS.items()}
counts.update({ns: q1(f"select count(*) from ({sql})")[0] for ns, sql in CORPUS.items()})
if not a.no_count_check:
    assert counts == EXPECTED, f"corpus sets {counts}, the search corpus is {EXPECTED}"
    for ns in ("PL", "NR"):
        miss = q1(f"select count(*) from ({CORPUS[ns]}) k anti join {cl(ns)} c using (petadex_id)")[0]
        log(f"{ns}: {miss} corpus sequences without a cluster row")

# ---- 2. cards, as one view per namespace (LG per bucket) with `id` first
PL_SQL = f"""select k.petadex_id id, cast(substr(k.petadex_id, 3) as bigint) n, c.component, c.c90, c.c60, c.c30, c.n_cores,
    coalesce(p.sheet_all_enzyme_names, p.enzyme_name) AS enzyme_names, coalesce(p.sheet_all_substrates, p.substrate) substrates,
    p.sheet_all_ec ec, coalesce(p.sheet_organism, p.curated_organism) organism, p.fasta_accession accession,
    coalesce(p.sheet_catalytic_cath, p.curated_catalytic_cath) cath, p.sheet_has_structure has_structure,
    p.sheet_pdb_accession pdb, coalesce(p.sheet_all_doi, p.doi) doi, p.sheet_all_activity_modes activity_mode,
    p.taxid_mode taxid, p.t_opt_c, p.ph_opt
    from ({CORPUS['PL']}) k left join {cl('PL')} c using (petadex_id)
    left join '{M}/t1/plasticome_metadata.parquet' p on p.petadex_id = k.petadex_id"""
NR_SQL = f"""select k.petadex_id id, cast(substr(k.petadex_id, 3) as bigint) n, c.component, c.c90, c.c60, c.c30, c.n_cores,
    coalesce(o.species_mode_name, o.lca_name) organism, o.lca_rank, o.taxid_mode taxid, o.superkingdom, o.phylum,
    o.genus, o.n_accessions, o.taxon_conflict, upper(h.complete_catalytic_hit::varchar) = 'TRUE' core_complete,
    h.nearest_pl, try_cast(h.pident_to_pl as double) pident_to_pl, b.n_assemblies, b.n_biosamples, b.frac_mag,
    b.any_isolate, b.prof_dominant, b.meas_isolation_source isolation_source, b.meas_host host, b.meas_country country,
    o.t_opt_c, o.ph_opt, o.oxygen
    from ({CORPUS['NR']}) k left join {cl('NR')} c using (petadex_id)
    left join '{M}/t2/id_organism.parquet' o on o.petadex_id = k.petadex_id
    left join '{M}/t2/id_biosample.parquet' b on b.petadex_id = k.petadex_id
    left join '{M}/t2/nr_header.parquet' h on h.petadex_id = k.petadex_id"""
def lg_sql(b):
    return f"""select c.petadex_id id, c.n, c.component, c.c90, c.c60, c.c30, c.n_cores,
    m.sup_n_biosamples n_biosamples, m.sup_n_bioprojects n_bioprojects, m.sup_n_runs n_runs, m.support_tier,
    m.prof_dominant, m.prof_dominant_share, m.prof_hist, m.meas_temp_n, m.meas_temp_median, m.meas_ph_median,
    m.inf_tenv_median tenv, m.inf_tenv_q25 tenv_q25, m.inf_tenv_q75 tenv_q75, m.inf_phenv_median phenv
    from {cl('LG', b)} c left join read_parquet('{S3}/data/t3/lg_metadata/bucket={b}/data.parquet') m on m.lg = c.n"""

# ---- 3. blocks: ordered by n inside each source, BLOCK_ROWS per block, NDJSON gz
INDEX = {}
POOL = ThreadPoolExecutor(max_workers=a.threads)


def _put(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", compresslevel=6) as f:
        f.write(text + "\n")


def write_blocks(ns, sql, offset, tag):
    """Write one ordered source as blocks offset..; returns (next offset, [(first key, block)], rows).

    Each line is the row as a JSON object whose first key is `id`, so readers can match
    `{"id":"<key>",` without parsing. Lines are assembled by DuckDB; Python only gzips."""
    dst = f"{OUT}/blocks/{ns}"
    con.execute(f"create or replace temp table cur as select *, {offset} + (row_number() over (order by n) - 1) // {R} block from ({sql})")
    n = q1("select count(*) from cur")[0]
    cols = [r[0] for r in con.execute("describe cur").fetchall() if r[0] not in ("n", "block")]
    obj = "struct_pack(" + ", ".join(f'"{c}" := "{c}"' for c in cols) + ")"
    res = con.execute(f"""select block, min(n), string_agg(to_json({obj}), chr(10) order by n)
        from cur group by block order by block""")
    first, futs = [], []
    while True:
        rows = res.fetchmany(64)
        if not rows:
            break
        for blk, k, text in rows:
            first.append([k, blk])
            futs.append(POOL.submit(_put, f"{dst}/block={blk}/data_0.json.gz", text))
        for f in futs:
            f.result()
        futs = []
    log(f"{ns} {tag}: {n:,} rows -> blocks {offset}..{offset + len(first) - 1}")
    return offset + len(first), first, n


done = f"{WORK}/cards_done.json"
state = json.load(open(done)) if os.path.exists(done) else {"index": {}, "rows": {}, "lg_buckets": [], "bg": {"n": 0, "w": {}, "k": {}}}
for ns, sql in (("PL", PL_SQL), ("NR", NR_SQL)):
    if ns not in state["index"] and counts[ns]:
        shutil.rmtree(f"{OUT}/blocks/{ns}", ignore_errors=True)
        _, idx, n = write_blocks(ns, sql, 0, "all")
        state["index"][ns], state["rows"][ns] = idx, n
        json.dump(state, open(done, "w"))
lg_buckets = sorted(int(p.rsplit("=", 1)[1]) for p in glob.glob(f"{CL}/ns=LG/bucket=*"))
state["index"].setdefault("LG", []); state["rows"].setdefault("LG", 0)
off = (state["index"]["LG"][-1][1] + 1) if state["index"]["LG"] else 0
if not state["lg_buckets"]:
    shutil.rmtree(f"{OUT}/blocks/LG", ignore_errors=True)
for b in lg_buckets:
    if b in state["lg_buckets"]:
        continue
    off, idx, n = write_blocks("LG", lg_sql(b), off, f"bucket {b}")
    # background from the same rows: each sequence weighs 1, split over its profiled BioSamples
    for p, w, k in con.execute("""with h as (select id, unnest(map_keys(prof_hist)) p, unnest(map_values(prof_hist)) k
            from cur where cardinality(prof_hist) > 0)
        select p, sum(k / s), count(distinct id) from (select *, sum(k) over (partition by id) s from h) group by 1""").fetchall():
        state["bg"]["w"][p] = state["bg"]["w"].get(p, 0) + w; state["bg"]["k"][p] = state["bg"]["k"].get(p, 0) + k
    state["bg"]["n"] += q1("select count(*) from cur where cardinality(prof_hist) > 0")[0]
    state["index"]["LG"] += idx; state["rows"]["LG"] += n; state["lg_buckets"].append(b)
    json.dump(state, open(done, "w"))
for ns in NS_CODE:
    assert state["rows"][ns] == counts.get(ns, 0), f"{ns}: {state['rows'][ns]} cards vs {counts[ns]} sequences"

# ---- 4. cluster cards (string keys = representative domain_id)
C_SQL = {
    "c90": f"select cl id, cl n, * exclude (cl, t3_prof_hist) from '{a.c90 or S3 + '/data/joint/cluster_c90.parquet'}'",
    "c30": f"select cl id, cl n, * exclude (cl, t3_prof_hist) from '{a.c30 or S3 + '/data/joint/cluster_c30.parquet'}'",
    "c60": f"""select centroid id, centroid n, component, centroid_source, try_cast(n_children as int) n_children,
               try_cast(n_domains as int) n_domains, try_cast(n_pazy as int) n_pazy, try_cast(n_nr as int) n_nr,
               try_cast(n_logan as int) n_logan
               from read_csv('{a.c60_stats}', delim='\t', header=true, quote='', all_varchar=true)""",
}
for ns, sql in C_SQL.items():
    if ns not in state["index"]:
        shutil.rmtree(f"{OUT}/blocks/{ns}", ignore_errors=True)
        _, idx, n = write_blocks(ns, sql, 0, "all")
        state["index"][ns], state["rows"][ns] = idx, n
        json.dump(state, open(done, "w"))

# ---- 5. background (accumulated per LG bucket above)
json.dump({"unit": "corpus Logan sequence; weight 1 split over the profiles of its profiled BioSamples",
           "n_sequences": state["bg"]["n"], "weight": state["bg"]["w"], "n_sequences_with_profile": state["bg"]["k"]},
          open(f"{OUT}/background.json", "w"), indent=1)
log(f"background over {state['bg']['n']:,} profiled sequences")

# ---- 5b. Plasticome members of each 90 % / 60 % cluster ("characterized relatives"), one small file
PLM = ["id", "enzyme_names", "substrates", "ec", "organism", "has_structure", "pdb", "doi"]
rows = con.execute(f"select c90, c60, {', '.join(PLM)} from ({PL_SQL}) order by n").fetchall()
plm = {"c90": {}, "c60": {}}
for c90, c60, *v in rows:
    m = dict(zip(PLM, v))
    for lvl, cid in (("c90", c90), ("c60", c60)):
        if cid:
            plm[lvl].setdefault(cid, []).append(m)
json.dump(plm, open(f"{OUT}/pl_members.json", "w"), separators=(",", ":"))
log(f"pl_members: {len(plm['c90'])} 90 % and {len(plm['c60'])} 60 % clusters with a Plasticome member")

# ---- 6. index + manifest
json.dump({ns: {"keys": [k for k, _ in v], "blocks": [b for _, b in v]} for ns, v in state["index"].items()},
          open(f"{OUT}/index.json", "w"), separators=(",", ":"))
json.dump({"store_format": "1.0", "tag": os.path.basename(OUT), "database_release": "v260701",
           "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "block_rows": R,
           "rows": state["rows"], "blocks": {ns: len(v) for ns, v in state["index"].items()},
           "key": {"PL/NR/LG": "numeric part of petadex_id", "c90/c60/c30": "representative domain_id"},
           "path": "blocks/<ns>/block=<k>/data_0.json.gz",
           "sources": {"metadata": M, "metadata_s3": S3, "hierarchy": a.hierarchy, "c60_stats": a.c60_stats,
                       "clustering": "petadex-clustering Run 1 (2026-10-15)"}},
          open(f"{OUT}/manifest.json", "w"), indent=1)
log("done")
