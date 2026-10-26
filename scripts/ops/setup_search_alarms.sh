#!/bin/bash
# Search alarms → ntfy push. Idempotent: re-run after editing an alarm or the notifier.
#
#   CloudWatch alarm ─► SNS petadex-search-alarms ─► Lambda petadex-search-alarm-notifier ─► ntfy.sh
#
# Alarms (see infra/alarms/ntfy_notifier.py for the push text and priority):
#   petadex-search-worker-throttles  worker Throttles ≥ 1/min: searches failing under fail-fast
#   petadex-search-unavailable       orchestrator refused with global_hour/global_day/unavailable
#   petadex-search-refused           refused with client_hour/running, or the orchestrator throttled
#
# The ntfy topic is read from ~/.config/run-notify/ntfy_topic (see the vault note
# "ntfy Notifications") and stored only in the notifier's env. It is never printed.
#
# Usage:
#   ./setup_search_alarms.sh          # create/update everything
#   ./setup_search_alarms.sh --test   # also force one alarm to ALARM: sends one real push
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text --region "$REGION" </dev/null)"
TOPIC_FILE="${NTFY_TOPIC_FILE:-$HOME/.config/run-notify/ntfy_topic}"
FN=petadex-search-alarm-notifier
ROLE=petadex-search-alarm-notifier-role
SNS_NAME=petadex-search-alarms
ORCH_LOGS=/aws/lambda/petadex-diamond-orchestrator
NS=PETadex/Search
HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="$HERE/../../infra/alarms/ntfy_notifier.py"
AWS="aws --region $REGION --no-cli-pager --cli-read-timeout 20 --cli-connect-timeout 10"
TEST=0
[[ "${1:-}" == "--test" ]] && TEST=1

[[ -s "$TOPIC_FILE" ]] || { echo "ntfy topic file $TOPIC_FILE missing or empty" >&2; exit 1; }
WORK="$(mktemp -d)"; chmod 700 "$WORK"; trap 'rm -rf "$WORK"' EXIT

# ── Notifier role ─────────────────────────────────────────────────────────────
if ! $AWS iam get-role --role-name "$ROLE" >/dev/null 2>&1 </dev/null; then
  echo "Creating role $ROLE"
  $AWS iam create-role --role-name "$ROLE" >/dev/null </dev/null --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  $AWS iam attach-role-policy --role-name "$ROLE" </dev/null \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
fi
ROLE_ARN="arn:aws:iam::$ACCOUNT:role/$ROLE"

# ── Notifier function (topic goes in via a 600 file, never on the command line) ─
cp "$SRC" "$WORK/ntfy_notifier.py"
(cd "$WORK" && zip -q notifier.zip ntfy_notifier.py)
(umask 077; python3 -c 'import json,sys; print(json.dumps({"Variables": {"NTFY_TOPIC": open(sys.argv[1]).read().strip()}}))' \
  "$TOPIC_FILE" > "$WORK/env.json")

if $AWS lambda get-function --function-name "$FN" >/dev/null 2>&1 </dev/null; then
  echo "Updating $FN"
  $AWS lambda update-function-code --function-name "$FN" --zip-file "fileb://$WORK/notifier.zip" >/dev/null </dev/null
  $AWS lambda wait function-updated-v2 --function-name "$FN" </dev/null
  $AWS lambda update-function-configuration --function-name "$FN" --environment "file://$WORK/env.json" >/dev/null </dev/null
else
  echo "Creating $FN"
  # A new role takes a few seconds to become assumable by Lambda.
  for i in 1 2 3 4 5 6; do
    if $AWS lambda create-function --function-name "$FN" --runtime python3.12 --architectures arm64 \
         --role "$ROLE_ARN" --handler ntfy_notifier.handler --timeout 15 --memory-size 128 \
         --zip-file "fileb://$WORK/notifier.zip" --environment "file://$WORK/env.json" >/dev/null 2>"$WORK/err" </dev/null; then
      break
    fi
    grep -q "cannot be assumed" "$WORK/err" || { cat "$WORK/err" >&2; exit 1; }
    sleep 5
  done
fi
$AWS lambda wait function-updated-v2 --function-name "$FN" </dev/null
FN_ARN="arn:aws:lambda:$REGION:$ACCOUNT:function:$FN"

# ── SNS topic → notifier ──────────────────────────────────────────────────────
SNS_ARN="$($AWS sns create-topic --name "$SNS_NAME" --query TopicArn --output text </dev/null)"
$AWS lambda add-permission --function-name "$FN" --statement-id sns-invoke --action lambda:InvokeFunction \
  --principal sns.amazonaws.com --source-arn "$SNS_ARN" >/dev/null 2>&1 </dev/null || true  # exists on re-run
$AWS sns subscribe --topic-arn "$SNS_ARN" --protocol lambda --notification-endpoint "$FN_ARN" >/dev/null </dev/null

# ── Metric filters on the orchestrator's "Admission denied (<limit>)" log line ─
put_filter() {  # name pattern metric
  $AWS logs put-metric-filter --log-group-name "$ORCH_LOGS" --filter-name "$1" --filter-pattern "$2" </dev/null \
    --metric-transformations "metricName=$3,metricNamespace=$NS,metricValue=1"
}
put_filter search-unavailable '?"Admission denied (global" ?"Admission denied (unavailable"' SearchUnavailable
put_filter search-refused '?"Admission denied (client_hour" ?"Admission denied (running"' SearchRefused

# ── Alarms (OK actions too, so a cleared alarm sends a low-priority "resolved") ─
COMMON=(--alarm-actions "$SNS_ARN" --ok-actions "$SNS_ARN" --treat-missing-data notBreaching
        --comparison-operator GreaterThanOrEqualToThreshold --threshold 1 --evaluation-periods 1)

$AWS cloudwatch put-metric-alarm --alarm-name petadex-search-worker-throttles "${COMMON[@]}" </dev/null \
  --alarm-description "Search workers throttled: jobs fail under fail-fast." \
  --namespace AWS/Lambda --metric-name Throttles --dimensions Name=FunctionName,Value=petadex-diamond-worker \
  --statistic Sum --period 60

$AWS cloudwatch put-metric-alarm --alarm-name petadex-search-unavailable "${COMMON[@]}" </dev/null \
  --alarm-description "Search refusing everyone: global budget used up or admission check failing." \
  --namespace "$NS" --metric-name SearchUnavailable --statistic Sum --period 300

# Per-user refusals: admission denials plus orchestrator throttles (its reserved
# concurrency of 2 turning a burst away). Metric math sums the two.
$AWS cloudwatch put-metric-alarm --alarm-name petadex-search-refused "${COMMON[@]}" </dev/null \
  --alarm-description "A search was refused (per-client limit, too many running, or orchestrator busy)." \
  --metrics '[
    {"Id":"denied","MetricStat":{"Metric":{"Namespace":"'"$NS"'","MetricName":"SearchRefused"},"Period":300,"Stat":"Sum"},"ReturnData":false},
    {"Id":"throttled","MetricStat":{"Metric":{"Namespace":"AWS/Lambda","MetricName":"Throttles","Dimensions":[{"Name":"FunctionName","Value":"petadex-diamond-orchestrator"}]},"Period":300,"Stat":"Sum"},"ReturnData":false},
    {"Id":"refused","Expression":"FILL(denied,0)+FILL(throttled,0)","Label":"Refused searches","ReturnData":true}
  ]'

echo "Alarms:"
$AWS cloudwatch describe-alarms --alarm-name-prefix petadex-search- \
  --query 'MetricAlarms[].[AlarmName,StateValue]' --output text </dev/null

if [[ $TEST == 1 ]]; then
  echo "Test: forcing petadex-search-refused to ALARM (one real push; it returns to OK on its next evaluation)"
  $AWS cloudwatch set-alarm-state --alarm-name petadex-search-refused --state-value ALARM \
    --state-reason "Manual test from setup_search_alarms.sh" </dev/null
fi
