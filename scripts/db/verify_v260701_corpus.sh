#!/usr/bin/env bash
# Independent check that the assembled v260701 search corpora carry exactly
# Thomas's records, compared as normalised "id<TAB>sequence" streams (immune to
# line wrapping and header truncation). Shares no code with the assembler:
#   cores: Thomas's s3/7 catalytic_domain files
#   full : Thomas's proteins files — PAZy kept by core-ID membership, nr/Logan
#          kept by the trailing |TRUE catalytic-site flag
# Exits non-zero and prints the first differing record on any mismatch.
#
# Usage: verify_v260701_corpus.sh INPUT_DIR
set -euo pipefail
IN=$1
T=/watson/rnalab/tquigley/petadex.v260701/s3
export LC_ALL=C

# FASTA -> "id\tseq" (id = header up to the first '|'; wrapped lines joined)
norm='/^>/{ if (id != "") print id "\t" s; id = substr($0, 2); sub(/\|.*/, "", id); s = ""; next }
      { s = s $0 } END { if (id != "") print id "\t" s }'

check() {  # name, thomas-stream cmd, mine file
  local name=$1 theirs=$2 mine=$3
  if cmp <(eval "$theirs") <(mawk "$norm" "$mine") > "$IN/verify_$name.cmp" 2>&1; then
    echo "verify $name: IDENTICAL"
  else
    echo "verify $name: MISMATCH — $(cat "$IN/verify_$name.cmp")"
    return 1
  fi
}

cores_theirs="mawk '$norm' $T/7_petadex_clustering/petadex-plasticome.v260701.catalytic_domain.fastaa \
  $T/7_petadex_clustering/petadex-nr.v260701.catalytic_domain.fastaa \
  $T/7_petadex_clustering/petadex-logan.v260701.catalytic_domain.fastaa"

full_theirs="{ mawk -F'|' 'NR==FNR { if (/^>/) keep[substr(\$1,2)] = 1; next }
                           /^>/ { k = (substr(\$1,2) in keep) } k' \
                 $T/7_petadex_clustering/petadex-plasticome.v260701.catalytic_domain.fastaa \
                 $T/2_nr_search/petadex-plasticome.v260701.proteins.fastaa
               mawk '/^>/ { k = /\|TRUE\$/ } k' $T/5_creating_logan_query/petadex-nr.v260701.proteins.fastaa \
                 $T/6_logan_search_qc/petadex-logan.v260701.proteins.fastaa; } | mawk '$norm'"

check cores "$cores_theirs" "$IN/PETadex.seq_catalytic_cores.v260701.fa" & c=$!
check full  "$full_theirs"  "$IN/PETadex.seq.v260701.fa" & f=$!
rc=0; wait $c || rc=1; wait $f || rc=1
exit $rc
