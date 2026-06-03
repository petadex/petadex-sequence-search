#!/usr/bin/env bash
#
# One-time bootstrap of the DIAMOND scale-out infrastructure (Phase 6).
#
# Creates (idempotently): three IAM execution roles + inline policies, the three
# Lambda functions from the SAME container image (orchestrator / worker /
# aggregator, distinguished by their ImageConfig command), worker reserved
# concurrency, and the Step Functions state machine. Per-push CODE updates are
# handled separately by .github/workflows/deploy.yml — this script provisions
# the resources that workflow assumes already exist.
#
# RUN ONCE, by an admin principal (NOT the S3-scoped petadex-ec2-role, and NOT
# from the build/dev box). Re-running is safe: existing resources are updated.
#
# PREREQUISITES:
#   * The container image is already in ECR (push it via deploy.yml or manually).
#   * The full DIAMOND build has published diamond/LATEST (otherwise the stack
#     provisions fine but cannot serve a query until the database exists).
#   * Does NOT touch the legacy petadex-mmseqs2-search function or mmseqs2/ path.
#
# REQUIRED env:
#   DB_SECRET_ARN   Secrets Manager ARN of the RDS credentials (aggregator).
# OPTIONAL env:
#   IMAGE_URI       Full ECR image URI (default: <acct>.dkr.ecr.<region>.amazonaws.com/petadex-mmseq2-search:latest)
#   VPC_SUBNET_IDS  Comma-separated subnet IDs for the aggregator (to reach RDS).
#   VPC_SG_IDS      Comma-separated security group IDs for the aggregator.
#   WORKER_RESERVED_CONCURRENCY  Default 60 (= SHARD_COUNT × concurrent jobs).
#       Each job's Map fans out SHARD_COUNT (20) workers at once, so this caps
#       the number of *simultaneous* searches at RESERVED / 20. At 20 a single
#       extra concurrent job (e.g. the deploy's 3-example regen) throttles every
#       worker, and the Map's short throttle-retry then fail-fasts the whole job.
#       60 = 3 concurrent jobs; raise by 20 per additional concurrent search.
#
# The aggregator reaches RDS exactly as the legacy function does. If that
# function runs inside the VPC, set VPC_SUBNET_IDS/VPC_SG_IDS to the same values
# (and the role gets the VPC-access managed policy); otherwise leave them unset.

set -euo pipefail

ACCOUNT_ID="${ACCOUNT_ID:-797308887321}"
REGION="${AWS_REGION:-us-east-1}"
ECR_REPOSITORY="${ECR_REPOSITORY:-petadex-mmseq2-search}"
IMAGE_URI="${IMAGE_URI:-${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPOSITORY}:latest}"
WORKER_RESERVED_CONCURRENCY="${WORKER_RESERVED_CONCURRENCY:-60}"

ORCH_FN="petadex-diamond-orchestrator"
WORKER_FN="petadex-diamond-worker"
AGG_FN="petadex-diamond-aggregator"
SM_NAME="petadex-diamond-search"
GHA_ROLE="${GHA_ROLE:-petadex-github-actions-role}"  # pre-existing CI role
SM_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SM_NAME}"

: "${DB_SECRET_ARN:?set DB_SECRET_ARN to the RDS credentials secret ARN}"

IAM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../infra/iam" && pwd)"
ASL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../infra" && pwd)/search_state_machine.asl.json"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Account=$ACCOUNT_ID Region=$REGION Image=$IMAGE_URI"

# --- IAM role helper: create-if-absent, then (re)attach the inline policy ----
ensure_role() {  # name  trust-file  policy-name  policy-file
  local name="$1" trust="$2" pname="$3" pfile="$4"
  if ! aws iam get-role --role-name "$name" >/dev/null 2>&1; then
    echo "  creating role $name"
    aws iam create-role --role-name "$name" \
      --assume-role-policy-document "file://$trust" >/dev/null
  else
    echo "  role $name exists"
  fi
  aws iam put-role-policy --role-name "$name" \
    --policy-name "$pname" --policy-document "file://$pfile" >/dev/null
}

echo "[1/4] IAM roles"
# Aggregator policy embeds the secret ARN — substitute into a temp copy.
sed "s|REPLACE_DB_SECRET_ARN|${DB_SECRET_ARN}|g" \
  "$IAM_DIR/aggregator-policy.json" > "$TMP/aggregator-policy.json"

ensure_role "${ORCH_FN}-role"   "$IAM_DIR/lambda-trust.json"       orchestrator-policy "$IAM_DIR/orchestrator-policy.json"
ensure_role "${WORKER_FN}-role" "$IAM_DIR/lambda-trust.json"       worker-policy       "$IAM_DIR/worker-policy.json"
ensure_role "${AGG_FN}-role"    "$IAM_DIR/lambda-trust.json"       aggregator-policy   "$TMP/aggregator-policy.json"
ensure_role "${SM_NAME}-role"   "$IAM_DIR/statemachine-trust.json" statemachine-policy "$IAM_DIR/statemachine-policy.json"

# Aggregator needs VPC ENI management only if it runs in a VPC to reach RDS.
if [[ -n "${VPC_SUBNET_IDS:-}" ]]; then
  aws iam attach-role-policy --role-name "${AGG_FN}-role" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole >/dev/null
fi

# Grant the pre-existing CI role permission to invoke the orchestrator (and
# describe the worker/aggregator) so deploy.yml's example-regen + wait steps
# work. We only update the inline policy; the role itself is owned elsewhere and
# is never created here.
if aws iam get-role --role-name "$GHA_ROLE" >/dev/null 2>&1; then
  echo "  updating $GHA_ROLE inline policy petadex-lambda-invoke"
  aws iam put-role-policy --role-name "$GHA_ROLE" \
    --policy-name petadex-lambda-invoke \
    --policy-document "file://$IAM_DIR/github-actions-lambda-invoke.json" >/dev/null
else
  echo "::warning:: CI role $GHA_ROLE not found — skipping lambda-invoke grant"
fi

ORCH_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ORCH_FN}-role"
WORKER_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${WORKER_FN}-role"
AGG_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${AGG_FN}-role"
SM_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${SM_NAME}-role"

echo "  waiting 10s for new IAM roles to propagate..."
sleep 10

# --- Lambda helper: create-if-absent, else update config -----------------
# create-function takes --image-config/--memory-size/etc.; an existing function
# is reconfigured with update-function-configuration. CODE (image tag) is the
# CD pipeline's job, so we only ensure existence + config here.
ensure_function() {  # name role-arn handler memory timeout 'ephemeral' 'env' [vpc]
  local name="$1" role="$2" handler="$3" mem="$4" timeout="$5" ephemeral="$6" env="$7" vpc="${8:-}"
  local common=(--memory-size "$mem" --timeout "$timeout"
                --image-config "Command=[\"$handler\"]"
                --ephemeral-storage "Size=$ephemeral"
                --environment "$env")
  [[ -n "$vpc" ]] && common+=(--vpc-config "$vpc")
  if ! aws lambda get-function --function-name "$name" >/dev/null 2>&1; then
    echo "  creating function $name ($handler)"
    aws lambda create-function --function-name "$name" \
      --package-type Image --code "ImageUri=$IMAGE_URI" --role "$role" \
      --architectures arm64 "${common[@]}" >/dev/null
    aws lambda wait function-active --function-name "$name"
  else
    echo "  updating config for $name ($handler)"
    aws lambda update-function-configuration --function-name "$name" \
      --role "$role" "${common[@]}" >/dev/null
    aws lambda wait function-updated --function-name "$name"
  fi
}

echo "[2/4] Lambda functions (one image, three handlers)"
ensure_function "$ORCH_FN" "$ORCH_ROLE_ARN" orchestrator.handler 512 30 512 \
  "Variables={S3_BUCKET=petadex,STATE_MACHINE_ARN=$SM_ARN}"

ensure_function "$WORKER_FN" "$WORKER_ROLE_ARN" worker.handler 2048 300 10240 \
  "Variables={S3_BUCKET=petadex,DIAMOND_SENSITIVITY=--very-sensitive,DIAMOND_BLOCK_SIZE=1}"
echo "  reserved concurrency = $WORKER_RESERVED_CONCURRENCY"
aws lambda put-function-concurrency --function-name "$WORKER_FN" \
  --reserved-concurrent-executions "$WORKER_RESERVED_CONCURRENCY" >/dev/null

AGG_VPC=""
if [[ -n "${VPC_SUBNET_IDS:-}" ]]; then
  AGG_VPC="SubnetIds=${VPC_SUBNET_IDS},SecurityGroupIds=${VPC_SG_IDS:?set VPC_SG_IDS too}"
fi
ensure_function "$AGG_FN" "$AGG_ROLE_ARN" aggregator.handler 1024 120 512 \
  "Variables={S3_BUCKET=petadex,DB_NAME=petadex,DB_HOST=petadex.ccz9y6yshbls.us-east-1.rds.amazonaws.com,DB_SECRET_ARN=$DB_SECRET_ARN}" \
  "$AGG_VPC"

echo "[3/4] Step Functions state machine"
# Substitute the worker/aggregator ARNs into the ASL definition.
sed -e "s|\${WorkerFunctionArn}|arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${WORKER_FN}|g" \
    -e "s|\${AggregatorFunctionArn}|arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${AGG_FN}|g" \
    "$ASL_FILE" > "$TMP/definition.json"

if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" >/dev/null 2>&1; then
  echo "  updating state machine $SM_NAME"
  aws stepfunctions update-state-machine --state-machine-arn "$SM_ARN" \
    --definition "file://$TMP/definition.json" --role-arn "$SM_ROLE_ARN" >/dev/null
else
  echo "  creating state machine $SM_NAME"
  aws stepfunctions create-state-machine --name "$SM_NAME" \
    --definition "file://$TMP/definition.json" --role-arn "$SM_ROLE_ARN" \
    --type STANDARD >/dev/null
fi

echo "[4/4] Done."
echo "  Orchestrator : $ORCH_FN  (-> $SM_ARN)"
echo "  Worker       : $WORKER_FN  (reserved=$WORKER_RESERVED_CONCURRENCY)"
echo "  Aggregator   : $AGG_FN"
echo "  State machine: $SM_ARN"
echo
echo "Next: ensure diamond/LATEST exists (run the full build), then smoke-test by"
echo "invoking $ORCH_FN with a search event. Cutover (point the web app at the"
echo "orchestrator) is Phase 7."
