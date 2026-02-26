#!/bin/bash
# Execute ClickHouse SQL via curl (no Node.js fetch timeout issues).
# Usage: ./ch-curl.sh "SQL statement" [max_time_seconds]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

# Parse .env for CLICKHOUSE_URL and CLICKHOUSE_PASSWORD
CLICKHOUSE_URL=$(grep '^CLICKHOUSE_URL=' "$ENV_FILE" | cut -d'=' -f2- | tr -d '"' | tr -d "'")
CLICKHOUSE_PASSWORD=$(grep '^CLICKHOUSE_PASSWORD=' "$ENV_FILE" | cut -d'=' -f2- | tr -d '"' | tr -d "'")

if [ -z "$CLICKHOUSE_URL" ] || [ -z "$CLICKHOUSE_PASSWORD" ]; then
  echo "Error: CLICKHOUSE_URL or CLICKHOUSE_PASSWORD not found in $ENV_FILE" >&2
  exit 1
fi

SQL="$1"
MAX_TIME="${2:-3600}"

curl -sS --max-time "$MAX_TIME" --connect-timeout 30 \
  -X POST "${CLICKHOUSE_URL}?default_format=TabSeparated&max_execution_time=${MAX_TIME}" \
  -H 'Content-Type: text/plain' \
  -H 'X-ClickHouse-User: default' \
  -H "X-ClickHouse-Key: ${CLICKHOUSE_PASSWORD}" \
  --data-binary "$SQL"
