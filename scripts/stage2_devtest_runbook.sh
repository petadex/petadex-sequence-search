#!/usr/bin/env bash
# =============================================================================
# stage2_devtest_runbook.sh — doc "08 Compressed-FASTA Merge Dev Build
# Validation", Stages 2–3, against a THROWAWAY worker function.
#
# Builds the dev-pinned worker image (Dockerfile.dev → diamond_dev dev@<SHA>),
# deploys it to petadex-diamond-worker-devtest (NOT prod), invokes IsPETase vs
# the 32-shard shard_00.fa.zst at -b1, and reads Lambda Max Memory Used.
#
# Two machines:
#   • `build`  → run on an arm64/Graviton box with Docker+BuildKit (r6g). DIAMOND
#                has no arm64 binary, so the image MUST be built on arm64.
#   • `deploy` / `invoke` / `cleanup` → run anywhere with the rnalab-dennis AWS
#                identity (this cluster works; it has lambda:invoke in us-east-1).
#
# Usage:
#   GH_PAT=ghp_xxx ./stage2_devtest_runbook.sh build      # on r6g
#   ./stage2_devtest_runbook.sh deploy                    # cluster
#   ./stage2_devtest_runbook.sh invoke                    # cluster (x2: Stage 2 + dup)
#   ./stage2_devtest_runbook.sh cleanup                   # cluster (delete devtest fn)
#
# ⚠️ This pins an UNPINNED PRIVATE dev branch. It is a benchmark harness, never a
#    cutover. Prod petadex-diamond-worker / :latest are never touched (guarded).
# =============================================================================
set -euo pipefail

# ---- Config -----------------------------------------------------------------
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="${AWS_ACCOUNT:-797308887321}"
ECR_REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
ECR_REPO="petadex-mmseq2-search"                 # existing worker image repo
DEV_SHA="4b2ae056851d28585ec7d3067ebaaaebdc7fbaac"
IMAGE_TAG="devtest-${DEV_SHA:0:7}"               # NEVER :latest
IMAGE_URI="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

PROD_WORKER="petadex-diamond-worker"             # clone its config; never modify it
DEVTEST_FN="petadex-diamond-worker-devtest"      # the throwaway target

# 32-shard zstd build (memory: built 2026-06-10, LATEST NOT bumped)
SHARD_VERSION="catalytic_orfs_v1.1_20260610_173654"
SHARD_KEY="diamond/${SHARD_VERSION}/shard_00.fa.zst"
SHARD_LETTERS="3216257430"                       # shard_00 exact (manifest)
DBSIZE="102929980061"                            # full-corpus letters (manifest)
BLOCK_OVERRIDE="1"                               # the whole point: -b1 on the dev build

# Hard guard: never let the throwaway name collapse onto a prod function.
case "$DEVTEST_FN" in
  "$PROD_WORKER"|petadex-diamond-orchestrator|petadex-diamond-aggregator)
    echo "FATAL: DEVTEST_FN '$DEVTEST_FN' is a production function. Aborting." >&2; exit 2;;
esac

phase="${1:-}"; [ -n "$phase" ] || { sed -n '2,30p' "$0"; exit 1; }
HERE="$(cd "$(dirname "$0")/.." && pwd)"         # repo root (Dockerfile.dev lives here)

# ---- build (r6g arm64 box) --------------------------------------------------
build() {
  command -v docker >/dev/null || { echo "FATAL: docker not found (run on the r6g box)"; exit 2; }
  [ "$(uname -m)" = "aarch64" ] || echo "WARN: $(uname -m), not aarch64 — image must be arm64 for Lambda."
  : "${GH_PAT:?set GH_PAT to a token with read access to bbuchfink/diamond_dev}"
  local tok; tok="$(mktemp)"; printf '%s' "$GH_PAT" > "$tok"
  trap 'shred -u "$tok" 2>/dev/null || rm -f "$tok"' RETURN

  aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$ECR_REGISTRY"

  echo ">> building $IMAGE_URI from Dockerfile.dev (dev@$DEV_SHA, arm64)"
  DOCKER_BUILDKIT=1 docker build -f "$HERE/Dockerfile.dev" \
    --secret id=gh_token,src="$tok" \
    --build-arg DIAMOND_DEV_SHA="$DEV_SHA" \
    --provenance=false \
    -t "$IMAGE_URI" "$HERE"
  docker push "$IMAGE_URI"
  echo ">> pushed $IMAGE_URI"
}

# ---- deploy (clone prod worker config → devtest fn on the dev image) ---------
deploy() {
  local cfg role mem timeout eph env_json
  cfg="$(aws lambda get-function-configuration --function-name "$PROD_WORKER" --region "$REGION")"
  role=$(echo "$cfg"     | python3 -c 'import json,sys;print(json.load(sys.stdin)["Role"])')
  mem=$(echo "$cfg"      | python3 -c 'import json,sys;print(json.load(sys.stdin)["MemorySize"])')
  timeout=$(echo "$cfg"  | python3 -c 'import json,sys;print(json.load(sys.stdin)["Timeout"])')
  eph=$(echo "$cfg"      | python3 -c 'import json,sys;print(json.load(sys.stdin).get("EphemeralStorage",{}).get("Size",10240))')
  # Inherit prod env (S3_BUCKET etc.), then ADD the -b1 override.
  env_json=$(echo "$cfg" | python3 -c '
import json,sys
e=json.load(sys.stdin).get("Environment",{}).get("Variables",{})
e["DIAMOND_FASTA_BLOCK_OVERRIDE"]="'"$BLOCK_OVERRIDE"'"
print(json.dumps({"Variables":e}))')
  echo ">> prod worker: mem=${mem}MB timeout=${timeout}s eph=${eph}MB role=$role"

  if aws lambda get-function --function-name "$DEVTEST_FN" --region "$REGION" >/dev/null 2>&1; then
    echo ">> updating existing $DEVTEST_FN code+config"
    aws lambda update-function-code --function-name "$DEVTEST_FN" \
      --image-uri "$IMAGE_URI" --region "$REGION" >/dev/null
    aws lambda wait function-updated --function-name "$DEVTEST_FN" --region "$REGION"
    aws lambda update-function-configuration --function-name "$DEVTEST_FN" \
      --memory-size "$mem" --timeout "$timeout" \
      --ephemeral-storage "Size=$eph" --environment "$env_json" \
      --image-config 'Command=["worker.handler"]' --region "$REGION" >/dev/null
  else
    echo ">> creating $DEVTEST_FN"
    aws lambda create-function --function-name "$DEVTEST_FN" \
      --package-type Image --code "ImageUri=$IMAGE_URI" \
      --role "$role" --architectures arm64 \
      --memory-size "$mem" --timeout "$timeout" \
      --ephemeral-storage "Size=$eph" --environment "$env_json" \
      --image-config 'Command=["worker.handler"]' \
      --region "$REGION" >/dev/null
  fi
  aws lambda wait function-updated --function-name "$DEVTEST_FN" --region "$REGION"
  echo ">> $DEVTEST_FN ready on $IMAGE_URI"
}

# ---- invoke (Stage 2: cold -b1 RSS read; run twice for the variance check) --
invoke() {
  local seq stamp ev out
  seq=$(awk '/^>IsPETase/{f=1;next} /^>/{f=0} f{printf "%s",$0}' "$HERE/example-sequences.txt")
  [ -n "$seq" ] || { echo "FATAL: could not extract IsPETase from example-sequences.txt"; exit 2; }
  stamp="devtest-$(date +%s)"
  ev=$(python3 -c '
import json,sys
print(json.dumps({
  "sessionId":"'"$stamp"'","jobId":"'"$stamp"'","shardIndex":0,
  "shardKey":"'"$SHARD_KEY"'","shardLetters":'"$SHARD_LETTERS"',
  "dbSize":'"$DBSIZE"',"maxResults":50,
  "queryFasta":">IsPETase\n"+sys.argv[1]
}))' "$seq")
  out=$(mktemp)
  echo ">> invoking $DEVTEST_FN (IsPETase vs shard_00.fa.zst @ -b${BLOCK_OVERRIDE})"
  resp=$(aws lambda invoke --function-name "$DEVTEST_FN" --region "$REGION" \
    --cli-binary-format raw-in-base64-out --payload "$ev" \
    --log-type Tail --query 'LogResult' --output text "$out")
  echo "-- response payload --"; cat "$out"; echo
  echo "-- REPORT (memory/duration) --"
  echo "$resp" | base64 -d | grep -E "REPORT|Max Memory|Memory Size|Init Duration" || \
    echo "$resp" | base64 -d | tail -3
  echo ">> Stage-2 gate: Max Memory Used should be ~2 GB (Benjamin anchor 2.2 GB),"
  echo "   well under the 10,240 MB cap. Run 'invoke' again for the duplicate variance check."
  rm -f "$out"
}

# ---- cleanup ----------------------------------------------------------------
cleanup() {
  aws lambda delete-function --function-name "$DEVTEST_FN" --region "$REGION" && \
    echo ">> deleted $DEVTEST_FN (image $IMAGE_URI left in ECR; remove manually if desired)"
}

case "$phase" in
  build) build;; deploy) deploy;; invoke) invoke;; cleanup) cleanup;;
  *) echo "unknown phase '$phase' (build|deploy|invoke|cleanup)"; exit 1;;
esac
