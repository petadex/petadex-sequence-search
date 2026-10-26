#!/usr/bin/env bash
# Unattended v260701 search rebuild, one step after another (run under ONE watchjob):
#   1. wait for the full-vs-cores benchmark, summarise it
#   2. verify both corpora against Thomas's files (abort on any mismatch)
#   3. build the 32 zstd shards from the FULL-LENGTH corpus and upload them
#      under a new version with --no-bump-latest (LATEST is never written)
#   4. check the manifest and that LATEST did not move
#   5. validate through the live orchestrator, pinned to the new version
#
# Usage: run_v260701_rebuild.sh BASE_DIR   (BASE_DIR holds inputs/ work/ logs/)
set -euo pipefail
BASE=$1
REPO=$(cd "$(dirname "$0")/../.." && pwd)
IN=$BASE/inputs; LOGS=$BASE/logs; BENCH=$BASE/work/bench; SCRATCH=$BASE/work/shards
export AWS_PROFILE=default                       # rnalab-dennis (owns s3://petadex)
export PATH=$HOME/tools/diamond-2.2.2-zstd/build:$PATH   # the worker image's DIAMOND
EXPECTED_SEQS=298687346
EXPECTED_LETTERS=$(awk -F'\t' '$1=="TOTAL"{print $6}' "$IN/assembly_stats.tsv")
log() { echo "[$(date '+%F %T')] $*"; }

latest_before=$(aws s3 cp s3://petadex/diamond/LATEST -)
log "LATEST at start: $latest_before"

log "1/5 waiting for the benchmark"
while pgrep -f bench_full_vs_cores.sh > /dev/null; do sleep 60; done
if grep -q "BENCH DONE" "$LOGS/bench.log"; then
  python3 - "$BENCH" <<'EOF' | tee "$BENCH/bench_summary.txt"
import csv, statistics, sys
from collections import defaultdict
d = sys.argv[1]
t = defaultdict(list); rss = defaultdict(list); hits = {}
for r in csv.DictReader(open(f"{d}/timings.tsv"), delimiter="\t"):
    t[(r["query"], r["arm"])].append(float(r["wall_s"]))
    rss[r["arm"]].append(int(r["max_rss_kb"]))
    hits[(r["query"], r["arm"])] = int(r["hits"])
qs = sorted({q for q, _ in t})
print("query\tfull_med_s\tcores_med_s\tcores/full\tfull_hits\tcores_hits")
tot = {"full": 0.0, "cores": 0.0}
for q in qs:
    f, c = statistics.median(t[(q, "full")]), statistics.median(t[(q, "cores")])
    tot["full"] += f; tot["cores"] += c
    print(f"{q}\t{f:.1f}\t{c:.1f}\t{c/f:.2f}\t{hits[(q,'full')]}\t{hits[(q,'cores')]}")
print(f"TOTAL\t{tot['full']:.1f}\t{tot['cores']:.1f}\t{tot['cores']/tot['full']:.2f}")
for a in ("full", "cores"):
    print(f"max_rss_{a}_GB\t{max(rss[a])/1e6:.2f}")
print(open(f"{d}/shards.tsv").read())
EOF
else
  log "benchmark did not finish cleanly; continuing with full-length (the default)"
fi

log "2/5 verifying corpora against Thomas's files"
"$REPO/scripts/db/verify_v260701_corpus.sh" "$IN"

log "3/5 building + uploading 32 zstd shards (no LATEST bump)"
mkdir -p "$SCRATCH"
python3 "$REPO/scripts/db/build_diamond_shards.py" --format zstd --shard-count 32 \
  --local-corpus "$IN/PETadex.seq.v260701.fa" \
  --key diamond/corpus/PETadex.seq.v260701.fa \
  --expected-count "$EXPECTED_SEQS" --expected-letters "$EXPECTED_LETTERS" \
  --release v260701 --version-prefix petadex_seq_v260701 \
  --scratch-dir "$SCRATCH" --threads 64 --no-bump-latest
manifest=$(ls -t "$SCRATCH"/manifest_petadex_seq_v260701_*.json | head -1)
version=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['version'])" "$manifest")
echo "$version" > "$BASE/VERSION"
log "built version $version"

log "4/5 checking manifest and LATEST"
python3 - "$manifest" "$EXPECTED_SEQS" "$EXPECTED_LETTERS" <<'EOF'
import json, sys
m = json.load(open(sys.argv[1]))
assert m["total_sequences"] == int(sys.argv[2]), m["total_sequences"]
assert m["total_letters"] == int(sys.argv[3]), m["total_letters"]
assert m["database_release"] == "v260701" and m["shard_count"] == 32
print("manifest OK:", m["version"], m["total_sequences"], m["total_letters"],
      m["total_dmnd_bytes"])
EOF
aws s3 ls "s3://petadex/diamond/$version/manifest.json" > /dev/null
latest_after=$(aws s3 cp s3://petadex/diamond/LATEST -)
[ "$latest_after" = "$latest_before" ] || { log "LATEST MOVED to $latest_after"; exit 1; }
log "LATEST unchanged: $latest_after"

log "5/5 pinned validation searches"
python3 "$REPO/scripts/deploy/validate_pinned_version.py" "$version" "$BASE/validation.tsv"

log "PIPELINE DONE ($version)"
