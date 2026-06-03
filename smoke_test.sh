#!/usr/bin/env bash
# Smoke test for the PETadex DIAMOND scale-out search.
# Invokes the orchestrator, then polls S3 for the result (or fail-fast timing.json).
set -euo pipefail

REGION=us-east-1
ORCH=petadex-diamond-orchestrator
WORKER=petadex-diamond-worker
SESSION=smoke
SEQ="MNFPRASRLMQAAVLGGLMAVSAAATAQTNPYARGPNPTAASLEASAGPFTVRSFTVSRPSGYGAGTVYYPTNAGGTVGAIAIVPGYTARQSSIKWWGPRLASHGFVVITIDTNSTLDQPSSRSSQQMAALRQVASLNGTSSSPIYGKVDTARMGVMGWSMGGGGSLISAANNPSLKAAAPQAPWDSSTNFSSVTVPTLIFACENDSIAPVNSSALPIYDSMSRNAKQFLEINGGSHSCANSGNSNQALIGKKGVAWMKRFMDNDTRYSTFACENPNSTRVSDFRTANCS"

echo "=== 1. Wait for redeploy to settle (both functions Active/Successful) ==="
for FN in "$WORKER" "$ORCH"; do
  while :; do
    read -r STATE UPD < <(aws lambda get-function-configuration --region "$REGION" \
      --function-name "$FN" --query '[State,LastUpdateStatus]' --output text)
    echo "  $FN: State=$STATE LastUpdateStatus=$UPD"
    [[ "$STATE" == "Active" && "$UPD" == "Successful" ]] && break
    [[ "$UPD" == "Failed" ]] && { echo "  !! $FN update FAILED"; exit 1; }
    sleep 5
  done
done

echo "=== 2. Invoke orchestrator ==="
aws lambda invoke --region "$REGION" --function-name "$ORCH" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"action\":\"search\",\"sessionId\":\"$SESSION\",\"sequence\":\"$SEQ\"}" \
  out.json >/dev/null
echo "  orchestrator response:"; cat out.json | python3 -m json.tool

JOB=$(python3 -c 'import json;print(json.loads(json.load(open("out.json"))["body"])["job_id"])')
echo "  job_id=$JOB"
START=$(date -u +%s)

echo "=== 3. Poll S3 for result (timeout 12 min) ==="
RESULT="results/$SESSION/$JOB.json"
TIMING="results/$SESSION/$JOB/timing.json"
while :; do
  EL=$(( $(date -u +%s) - START ))
  if aws s3 ls "s3://petadex/$RESULT" >/dev/null 2>&1; then
    echo "  RESULT READY after ${EL}s"
    echo "=== result (head) ==="
    aws s3 cp "s3://petadex/$RESULT" - | python3 -m json.tool | head -40
    echo "=== timing.json ==="
    aws s3 cp "s3://petadex/$TIMING" - | python3 -m json.tool
    exit 0
  fi
  # fail-fast writes timing.json with status:failed and no result.json
  if aws s3 ls "s3://petadex/$TIMING" >/dev/null 2>&1; then
    STATUS=$(aws s3 cp "s3://petadex/$TIMING" - 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",""))' || echo "")
    if [[ "$STATUS" == "failed" ]]; then
      echo "  JOB FAILED after ${EL}s — fail-fast timing.json:"
      aws s3 cp "s3://petadex/$TIMING" - | python3 -m json.tool
      exit 2
    fi
  fi
  # NB: `aws s3 ls` exits 1 on an empty/absent prefix; with set -euo pipefail
  # that would kill the loop on the first (always-empty) iteration. Swallow it.
  NPARTS=$( { aws s3 ls "s3://petadex/results/$SESSION/$JOB/parts/" 2>/dev/null || true; } | wc -l | tr -d ' ')
  echo "  [${EL}s] waiting... parts=$NPARTS"
  (( EL > 720 )) && { echo "  TIMED OUT after ${EL}s"; exit 3; }
  sleep 15
done
