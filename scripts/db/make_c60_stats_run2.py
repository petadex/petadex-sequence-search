#!/usr/bin/env python3
"""
Write the 60 % cluster statistics TSV that build_metadata_store.py reads for its c60 cards
(--c60-stats), computed from the Run 2 hierarchy. Run 1 had this file from the clustering
pipeline (cluster_stats_60.tsv); Run 2 does not, so it is derived here.

One row per 60 % cluster, keyed like the hierarchy (`centroid` = the centroid's domain_id):
  centroid, component (the centroid's), centroid_source (pazy|nr|logan),
  n_children (distinct 90 % clusters), n_domains, n_pazy, n_nr, n_logan

Usage:
  python3 make_c60_stats_run2.py --hierarchy .../hierarchy_run2.parquet --out cluster_stats_60.run2.tsv
"""
import argparse
import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--hierarchy", default="/watson/rnalab/dzhu/petadex-clustering/run2/products/hierarchy_run2.parquet")
ap.add_argument("--out", required=True)
ap.add_argument("--expected", type=int, default=3_230_662, help="Run 2 c60 cluster count (0 = skip check)")
a = ap.parse_args()

con = duckdb.connect()
con.execute(f"""copy (
    select g.c60 centroid, ce.component, ce.source centroid_source, g.n_children, g.n_domains, g.n_pazy, g.n_nr, g.n_logan
    from (select c60, count(distinct c90) n_children, count(*) n_domains, count_if(source = 'pazy') n_pazy,
                 count_if(source = 'nr') n_nr, count_if(source = 'logan') n_logan
          from read_parquet('{a.hierarchy}') group by c60) g
    join read_parquet('{a.hierarchy}') ce on ce.domain_id = g.c60
    order by g.c60
) to '{a.out}' (format csv, delimiter '\t', header true)""")
n, dom, cen = con.execute(f"""select count(*), sum(n_domains), count(distinct centroid)
                              from read_csv('{a.out}', delim='\t', header=true)""").fetchone()
total = con.execute(f"select count(*) from read_parquet('{a.hierarchy}')").fetchone()[0]
assert n == cen and dom == total, (n, cen, dom, total)
assert not a.expected or n == a.expected, (n, a.expected)
print(f"c60 clusters {n:,}, domains {dom:,} -> {a.out}")
