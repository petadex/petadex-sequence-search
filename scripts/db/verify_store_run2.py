#!/usr/bin/env python3
"""
Check a built metadata store against the Run 2 hierarchy through the production reader (src/metadata.py).

Samples sequences from every source (PL all 486; NR and LG reservoir samples), enriches them as fake hits,
and requires: status ok; every id has a card; card c90/c60/c30 == hierarchy; every cluster those ids fall
in has a cluster card at each level.

Usage: python3 verify_store_run2.py --store s3://petadex/metadata/v260701/<tag> [--n 2000]
"""
import argparse
import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
import metadata as m  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--store", required=True)
ap.add_argument("--hierarchy", default="/watson/rnalab/dzhu/petadex-clustering/run2/products/hierarchy_run2.parquet")
ap.add_argument("--n", type=int, default=2000, help="NR and LG ids each")
a = ap.parse_args()

con = duckdb.connect()
H = f"read_parquet('{a.hierarchy}')"
rows = con.execute(f"""
    select petadex_id, c90, c60, c30 from {H} where source = 'pazy'
    union all select * from (select petadex_id, c90, c60, c30 from (select * from {H} where source = 'nr') using sample reservoir({a.n} rows) repeatable (5))
    union all select * from (select petadex_id, c90, c60, c30 from (select * from {H} where source = 'logan') using sample reservoir({a.n} rows) repeatable (5))
""").fetchall()
truth = {r[0]: r[1:] for r in rows}
fails = []
ids = list(truth)
for i in range(0, len(ids), 500):            # the aggregator enriches at most 500 hits per result
    chunk = ids[i:i + 500]
    out = m.enrich([{"target_id": t, "percent_identity": 90.0, "bitscore": 100, "evalue": 1e-30} for t in chunk],
                   store_uri=a.store)
    if out["status"] != "ok":
        fails.append(f"status {out['status']} at chunk {i}")
        continue
    for t in chunk:
        c = out["cards"].get(t)
        if not c:
            fails.append(f"no card {t}")
        elif (c.get("c90"), c.get("c60"), c.get("c30")) != truth[t]:
            fails.append(f"clusters differ {t}")
    for j, lvl in enumerate(("c90", "c60", "c30")):
        have = (out["clusters"] or {}).get(lvl) or {}
        for cid in {truth[t][j] for t in chunk}:
            if cid not in have:
                fails.append(f"no {lvl} card {cid}")
print(f"ids checked {len(ids):,} (PL {sum(t.startswith('PL') for t in ids)}, NR {sum(t.startswith('NR') for t in ids)}, "
      f"LG {sum(t.startswith('LG') for t in ids)}); fails {len(fails)}")
for f in fails[:20]:
    print("  ", f)
sys.exit(1 if fails else 0)
