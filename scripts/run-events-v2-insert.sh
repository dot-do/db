#!/bin/bash
# Run the events_v2 INSERT SELECT via curl (bypasses Node.js header limits)
set -euo pipefail

# Load credentials from .env
eval "$(npx tsx -e "require('dotenv/config'); console.log('CH_URL=' + process.env.CLICKHOUSE_URL); console.log('CH_PASS=' + process.env.CLICKHOUSE_PASSWORD); console.log('CH_USER=' + (process.env.CLICKHOUSE_USER || 'default'))")"

SQL="INSERT INTO platform.events_v2 SELECT id, ray, ns, '' AS domain, type, event, url, source, actor, data, meta, file, ingested FROM platform.events SETTINGS max_execution_time=600"

echo "Starting INSERT INTO events_v2 (50M rows)..."
time curl -sS --max-time 600 \
  "${CH_URL}/?database=platform&default_format=TabSeparated" \
  -H "X-ClickHouse-User: ${CH_USER}" \
  -H "X-ClickHouse-Key: ${CH_PASS}" \
  -H "Content-Type: text/plain" \
  --data-binary "${SQL}"

echo "Done."
