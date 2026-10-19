#!/usr/bin/env bash
# One recording take: new run ID, open the deployed dashboard on that run, start the fleet.
#
# Settings (environment):
#   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER    required by the producer (see README)
#   ELASTIC_DEMO_DASHBOARD_URL           optional; your app's https://... URL from `snow app open`
#   ELASTIC_DEMO_PAT                     optional; otherwise the producer prompts (hidden input)
# This script never reads, stores, or passes any credential.
set -euo pipefail

cd "$(dirname "$0")/.."

RUN="$(uuidgen | tr '[:upper:]' '[:lower:]')"
echo "run_id: $RUN"

if [ -n "${ELASTIC_DEMO_DASHBOARD_URL:-}" ]; then
  URL="${ELASTIC_DEMO_DASHBOARD_URL%/}/?run_id=$RUN"
  if command -v open >/dev/null 2>&1; then open "$URL"
  elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$URL" >/dev/null 2>&1 &
  else echo "open $URL"; fi
else
  echo "ELASTIC_DEMO_DASHBOARD_URL not set; open your dashboard with ?run_id=$RUN"
fi

exec .venv/bin/python -m producer.fleet --run-id "$RUN" \
  --ramp 1,25,100,250 --ramp-interval 30 --heat-at 110 --recover-at 150 --duration 200 "$@"
