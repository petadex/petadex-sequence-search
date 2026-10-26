#!/usr/bin/env bash
# Full-length vs catalytic-core search DB, one production-sized shard each.
#
# Takes the SAME 1/N of petadex_ids from both v260701 corpora (numeric id % N == 0),
# zstd-19 compresses each like a production shard, then runs every panel query
# against both with the worker's exact flags (default sensitivity, -b1 with the
# cross-block merge, -k500, --dbsize = that arm's full-corpus letters, 6 threads
# = the 10 GB Lambda's vCPUs). Arms alternate per query so box load hits both.
#
# Usage: bench_full_vs_cores.sh INPUT_DIR WORK_DIR PANEL.fa [N=32] [REPS=3]
set -euo pipefail
IN=$1; W=$2; PANEL=$3; N=${4:-32}; REPS=${5:-3}
DIAMOND=${DIAMOND:-$HOME/tools/diamond-2.2.2-zstd/build/diamond}
THREADS=${THREADS:-6}
mkdir -p "$W/q" "$W/out"

declare -A SRC=([full]="$IN/PETadex.seq.v260701.fa"
                [cores]="$IN/PETadex.seq_catalytic_cores.v260701.fa")
declare -A DBSIZE
for arm in full cores; do
  DBSIZE[$arm]=$(awk -F'\t' -v c=$([ $arm = full ] && echo 6 || echo 3) \
                 '$1=="TOTAL"{print $c}' "$IN/assembly_stats.tsv")
  if [ ! -s "$W/$arm.fa.zst" ]; then
    awk -v n="$N" '/^>/{id=substr($0,4)+0; keep=(id % n == 0)} keep' "${SRC[$arm]}" > "$W/$arm.fa"
    /usr/bin/time -f "%e" -o "$W/$arm.zstd_s" zstd -q -f -19 -T"$THREADS" "$W/$arm.fa" -o "$W/$arm.fa.zst"
  fi
done

{
  printf 'arm\tsequences\tletters\tfa_bytes\tzst_bytes\tzstd_s\tdbsize\n'
  for arm in full cores; do
    awk -v a=$arm -v z="$(stat -c %s "$W/$arm.fa.zst")" -v f="$(stat -c %s "$W/$arm.fa")" \
        -v t="$(cat "$W/$arm.zstd_s")" -v d="${DBSIZE[$arm]}" \
      '/^>/{n++; next} {l+=length($0)} END{printf "%s\t%d\t%d\t%d\t%d\t%s\t%s\n",a,n,l,f,z,t,d}' "$W/$arm.fa"
  done
} > "$W/shards.tsv"
cat "$W/shards.tsv"

awk '/^>/{f=sprintf("%s/q/%s.fa", w, substr($1,2))} {print > f}' w="$W" "$PANEL"

printf 'query\tarm\trep\twall_s\tmax_rss_kb\thits\n' > "$W/timings.tsv"
for rep in $(seq 1 "$REPS"); do
  for q in "$W"/q/*.fa; do
    name=$(basename "$q" .fa)
    # alternate which arm goes first so drift in box load doesn't favour one
    if (( (rep + $(stat -c %s "$q")) % 2 )); then arms="full cores"; else arms="cores full"; fi
    for arm in $arms; do
      out="$W/out/$name.$arm.$rep.tsv"
      /usr/bin/time -f "%e\t%M" -o "$W/out/time.tmp" \
        "$DIAMOND" blastp -q "$q" -d "$W/$arm.fa.zst" -o "$out" -b1 -k500 \
          --threads "$THREADS" --dbsize "${DBSIZE[$arm]}" --quiet \
          --outfmt 6 sseqid qstart qend sstart send length pident evalue bitscore full_sseq
      printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$arm" "$rep" "$(cat "$W/out/time.tmp")" \
        "$(wc -l < "$out")" >> "$W/timings.tsv"
    done
  done
done
echo BENCH DONE
