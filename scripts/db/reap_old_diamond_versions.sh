#!/bin/bash
# Reap old DIAMOND shard-DB versions from S3, safely.
#
# Lifecycle rules CANNOT express "every diamond/{version}/ except the one
# diamond/LATEST points to" — they only see prefix/age/tags, so a blind
# age-based rule on diamond/ would eventually delete the *live* database
# (the active version is often the oldest object, since rebuilds are rare).
# This script is the safe alternative: it resolves LATEST, keeps it plus the
# N most-recent other versions (for rollback), and deletes the rest.
#
# DRY-RUN by default. Pass --apply to actually delete. Each real version is
# ~120 GB, so deletions are irreversible and large — review the dry-run first.
#
# Usage:
#   ./reap_old_diamond_versions.sh              # dry-run, keep LATEST + 1 previous
#   ./reap_old_diamond_versions.sh --keep 2     # keep LATEST + 2 previous
#   ./reap_old_diamond_versions.sh --apply       # actually delete
set -euo pipefail

BUCKET="${BUCKET:-petadex}"
REGION="${AWS_REGION:-us-east-1}"
PREFIX="diamond/"
VERSION_GLOB="catalytic_orfs_"   # only ever touch real build prefixes
KEEP_PREVIOUS=1
APPLY=0
export AWS_PAGER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY=1; shift;;
    --keep)  KEEP_PREVIOUS="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

LIVE=$(aws s3 cp "s3://${BUCKET}/${PREFIX}LATEST" - --region "$REGION")
[[ -n "$LIVE" ]] || { echo "ERROR: could not read ${PREFIX}LATEST — refusing to delete anything." >&2; exit 1; }
echo "LIVE version (always kept): $LIVE"

# All real version prefixes (names are timestamped, so reverse-sort = newest first).
# Plain while-read (not mapfile) so this runs on bash 3.2 (macOS) too.
VERSIONS=()
while IFS= read -r _v; do
  [[ -n "$_v" ]] && VERSIONS+=("$_v")
done < <(
  aws s3 ls "s3://${BUCKET}/${PREFIX}" --region "$REGION" \
    | awk '/PRE /{print $2}' | sed 's:/$::' \
    | grep "^${VERSION_GLOB}" | sort -r
)

keep_count=0
to_delete=()
for v in "${VERSIONS[@]}"; do
  if [[ "$v" == "$LIVE" ]]; then
    echo "  keep (live):     $v"; continue
  fi
  if (( keep_count < KEEP_PREVIOUS )); then
    echo "  keep (rollback): $v"; keep_count=$((keep_count+1)); continue
  fi
  echo "  REAP:            $v"; to_delete+=("$v")
done

if [[ ${#to_delete[@]} -eq 0 ]]; then
  echo "Nothing to reap."; exit 0
fi

if [[ $APPLY -eq 0 ]]; then
  echo
  echo "DRY-RUN — would delete ${#to_delete[@]} version prefix(es). Re-run with --apply to execute."
  exit 0
fi

for v in "${to_delete[@]}"; do
  # Belt-and-suspenders: never delete the live version, whatever happened above.
  [[ "$v" == "$LIVE" ]] && { echo "REFUSING to delete live version $v"; continue; }
  echo "deleting s3://${BUCKET}/${PREFIX}${v}/ ..."
  aws s3 rm "s3://${BUCKET}/${PREFIX}${v}/" --recursive --region "$REGION"
done
echo "Done."
