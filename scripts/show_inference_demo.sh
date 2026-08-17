#!/bin/sh
set -eu

API_URL=${API_URL:-http://localhost:18000}
BUDGET=${BUDGET:-3}
RESPONSE_FILE=$(mktemp "${TMPDIR:-/tmp}/lzd-campaign-response.XXXXXX")
trap 'rm -f "$RESPONSE_FILE"' EXIT INT TERM

USER_JSON='["U0000000","U0000001","U0000002","U0000003","U0000004","U0000005","U0000006","U0000007","U0000008","U0000009"]'

printf '\n=== 1. MODEL + FEATURE STORE ===\n'
curl --fail --silent --show-error "$API_URL/store/info" \
  | jq '{active_version,batch_keys,realtime_keys,model}'

printf '\n=== 2. CAMPAIGN TOP-%s + SYNTHETIC REALTIME POLICY ===\n' "$BUDGET"
curl --fail --silent --show-error \
  --header 'Content-Type: application/json' \
  --data "{\"user_ids\":$USER_JSON,\"budget\":$BUDGET}" \
  "$API_URL/campaign/decide" > "$RESPONSE_FILE"

jq '{model_version,feature_version,model_contract,policy,action_counts,disclaimer}' \
  "$RESPONSE_FILE"

printf '\nRANK | USER       | UPLIFT_PP | ACTION                         | CART | ORDER | REASON\n'
printf '%s\n' '-----|------------|-----------|--------------------------------|------|-------|------------------------------'
jq -r '.results[] | [
  (.rank|tostring),
  .user_id,
  (.uplift_percentage_point|tostring),
  .campaign_action,
  (.realtime.rt_add_to_cart_1h|tostring),
  (.realtime.rt_order_1h|tostring),
  .reason
] | join(" | ")' "$RESPONSE_FILE"

printf '\nGhi chu: uplift_pp la diem phan tram; rt_* chi gate campaign_action, khong sua model score.\n'
