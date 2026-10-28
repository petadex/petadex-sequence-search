#!/usr/bin/env bash
# One-time setup for the alignment + tree Lambda (src/align.py), on top of the live DIAMOND stack.
# Idempotent. Creates petadex-diamond-align (same image, handler align.handler) with its own role,
# lets the aggregator + orchestrator invoke it and read/write alignments/*, and sets ALIGN_FUNCTION
# on both, merging into their existing environment (update-function-configuration replaces it whole).
# Run after the image that contains align.py + FAMSA + FastTree is deployed:
#   ACCOUNT_ID=797308887321 ./scripts/deploy/provision_align.sh
set -euo pipefail
ACCOUNT_ID="${ACCOUNT_ID:?set ACCOUNT_ID}"
REGION="${AWS_REGION:-us-east-1}"; export AWS_DEFAULT_REGION="$REGION"
FN=petadex-diamond-align
IMAGE_URI="${IMAGE_URI:-$(aws lambda get-function --function-name petadex-diamond-aggregator --query Code.ImageUri --output text)}"
IAM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../infra/iam" && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
for f in align-policy invoke-align-policy; do sed "s|REPLACE_ACCOUNT_ID|$ACCOUNT_ID|g" "$IAM_DIR/$f.json" > "$TMP/$f.json"; done

echo "[1/4] role ${FN}-role"
aws iam get-role --role-name "${FN}-role" >/dev/null 2>&1 ||
  { aws iam create-role --role-name "${FN}-role" --assume-role-policy-document "file://$IAM_DIR/lambda-trust.json" >/dev/null; sleep 10; }
aws iam put-role-policy --role-name "${FN}-role" --policy-name align-policy --policy-document "file://$TMP/align-policy.json"

echo "[2/4] function $FN ($IMAGE_URI)"
if ! aws lambda get-function --function-name "$FN" >/dev/null 2>&1; then
  aws lambda create-function --function-name "$FN" --package-type Image --code "ImageUri=$IMAGE_URI" \
    --role "arn:aws:iam::${ACCOUNT_ID}:role/${FN}-role" --architectures arm64 \
    --memory-size 4096 --timeout 300 --image-config 'Command=["align.handler"]' \
    --environment 'Variables={S3_BUCKET=petadex}' >/dev/null
  aws lambda wait function-active --function-name "$FN"
fi
# Background starts: no retries (a failure is recorded in the status file; the orchestrator restarts on request).
aws lambda put-function-event-invoke-config --function-name "$FN" --maximum-retry-attempts 0 >/dev/null

echo "[3/4] aggregator + orchestrator may invoke it"
for r in petadex-diamond-aggregator-role petadex-diamond-orchestrator-role; do
  aws iam put-role-policy --role-name "$r" --policy-name invoke-align --policy-document "file://$TMP/invoke-align-policy.json"
done

echo "[4/4] ALIGN_FUNCTION on aggregator + orchestrator (other variables kept)"
for f in petadex-diamond-aggregator petadex-diamond-orchestrator; do
  aws lambda get-function-configuration --function-name "$f" --query Environment.Variables --output json |
    python3 -c 'import json,sys; v=json.load(sys.stdin) or {}; v["ALIGN_FUNCTION"]=sys.argv[1]; print(json.dumps({"Variables":v}))' "$FN" > "$TMP/env-$f.json"
  aws lambda update-function-configuration --function-name "$f" --environment "file://$TMP/env-$f.json" >/dev/null
  aws lambda wait function-updated --function-name "$f"
  echo "  $f ALIGN_FUNCTION=$(aws lambda get-function-configuration --function-name "$f" --query Environment.Variables.ALIGN_FUNCTION --output text)"
done
echo "done. Also add $FN to the CI role's UpdateFunctionCode resources (infra/iam/github-actions-deploy-update.json)."
