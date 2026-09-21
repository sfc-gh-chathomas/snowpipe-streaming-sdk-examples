#!/usr/bin/env bash
# First REST ingest: discover host, exchange PAT, append two rows. No automatic replay.
set +x
set -euo pipefail
: "${SNOWFLAKE_PAT:?Inject a PAT through your credential manager}"
: "${SNOWFLAKE_URL:?Set the HTTPS Snowflake account URL}"
: "${SNOWFLAKE_DATABASE:?Set the target database}"
: "${SNOWFLAKE_SCHEMA:?Set the target schema}"
: "${SNOWFLAKE_TABLE:?Set the target table}"
command -v curl >/dev/null
command -v jq >/dev/null
command -v uuidgen >/dev/null
CONTROL_URL="${SNOWFLAKE_URL%/}"
[[ "$CONTROL_URL" =~ ^https://[A-Za-z0-9.-]+\.snowflakecomputing\.com$ ]] || { printf '%s\n' 'Invalid Snowflake account URL' >&2; exit 1; }
[[ "$SNOWFLAKE_PAT" != *$'\n'* && "$SNOWFLAKE_PAT" != *$'\r'* && "$SNOWFLAKE_PAT" != *'"'* && "$SNOWFLAKE_PAT" != *'\'* ]] || exit 1

# Credentials go through curl stdin configuration, not command-line arguments or files.
account_request() {
  printf 'header = "Authorization: Bearer %s"\nheader = "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN"\n' "$SNOWFLAKE_PAT" |
    curl --config - --silent --show-error --fail --connect-timeout 10 --max-time 30 "$@"
}
HOST_RESPONSE=$(account_request "$CONTROL_URL/v2/streaming/hostname")
INGEST_HOST=$(printf '%s' "$HOST_RESPONSE" | jq -Rrs '. as $raw | try (fromjson | if type == "object" then .hostname else . end) catch ($raw | rtrimstr("\n"))')
INGEST_HOST="${INGEST_HOST//_/-}"
[[ "$INGEST_HOST" =~ ^[A-Za-z0-9.-]+\.snowflakecomputing\.com$ ]] || { printf '%s\n' 'Invalid discovered ingest host' >&2; exit 1; }
SCOPE="$INGEST_HOST"
if [[ -n "${SNOWFLAKE_ROLE:-}" ]]; then SCOPE="$SCOPE session:role:$SNOWFLAKE_ROLE"; fi

# Exchange the PAT for a scoped ingestion token. Never print either token.
FORM=$(jq -nr --arg scope "$SCOPE" '"grant_type=urn:ietf:params:oauth:grant-type:token-exchange&subject_token_type=programmatic_access_token&subject_token=" + (env.SNOWFLAKE_PAT | @uri) + "&scope=" + ($scope | @uri)')
TOKEN_RESPONSE=$({ printf 'header = "Authorization: Bearer %s"\ndata = "%s"\n' "$SNOWFLAKE_PAT" "$FORM"; } |
  curl --config - --silent --show-error --fail --connect-timeout 10 --max-time 30 \
    -H 'Content-Type: application/x-www-form-urlencoded' "$CONTROL_URL/oauth/token")
SCOPED_TOKEN=$(printf '%s' "$TOKEN_RESPONSE" | jq -Rrs '. as $raw | try (fromjson | if type == "object" then (.token // .access_token) else . end) catch ($raw | rtrimstr("\n"))')
[[ -n "$SCOPED_TOKEN" && "$SCOPED_TOKEN" != null && "$SCOPED_TOKEN" != *$'\n'* && "$SCOPED_TOKEN" != *$'\r'* && "$SCOPED_TOKEN" != *'"'* && "$SCOPED_TOKEN" != *'\'* ]] || exit 1
REQUEST_ID=$(uuidgen)
RUN_ID="${SNOWFLAKE_RUN_ID:-$REQUEST_ID}"
DB=$(printf '%s' "$SNOWFLAKE_DATABASE" | jq -sRr @uri)
SCHEMA=$(printf '%s' "$SNOWFLAKE_SCHEMA" | jq -sRr @uri)
TABLE=$(printf '%s' "$SNOWFLAKE_TABLE" | jq -sRr @uri)
ROWS=$(jq -cn --arg run "$RUN_ID" 'range(0;2) | {EVENT_ID:., C1:., C2:$run}')
# Encode newlines and quotes for curl configuration; the HTTP body remains NDJSON.
BODY_CONFIG=$(printf '%s\n' "$ROWS" | jq -sR .)
{ printf 'header = "Authorization: Bearer %s"\ndata-binary = %s\n' "$SCOPED_TOKEN" "$BODY_CONFIG"; } |
  curl --config - --silent --show-error --fail --connect-timeout 10 --max-time 30 \
    -H 'X-Snowflake-Authorization-Token-Type: OAUTH' -H 'Content-Type: application/x-ndjson' \
    "https://$INGEST_HOST/v2/streaming/data/databases/$DB/schemas/$SCHEMA/tables/$TABLE/rows?requestId=$REQUEST_ID&retryCount=0"
printf '\nDurably acknowledged 2 rows; run=%s. Verify materialization separately.\n' "$RUN_ID"
