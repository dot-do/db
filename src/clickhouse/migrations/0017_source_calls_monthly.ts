import type { Migration } from '../migrate'

/**
 * Migration 17: Pre-aggregated monthly source call rollup.
 *
 * SummingMergeTree auto-sums total_calls, total_cost, total_price, cached_calls
 * on merge. MV sources from events (same pattern as source_calls_mv) to avoid
 * chaining issues. Used by loadTenantUsage for fast callsUsed lookups and by
 * the usage analytics dashboard.
 */
export const migration: Migration = {
  version: 17,
  name: 'source_calls_monthly',
  up: `
CREATE TABLE IF NOT EXISTS {database}.source_calls_monthly (
  ns String,
  month Date,
  source String,
  resource String,
  total_calls UInt64,
  total_cost UInt64,
  total_price UInt64,
  cached_calls UInt64
) ENGINE = SummingMergeTree((total_calls, total_cost, total_price, cached_calls))
PARTITION BY toYYYYMM(month)
ORDER BY (ns, month, source, resource);

CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.source_calls_monthly_mv TO {database}.source_calls_monthly AS
SELECT
  ev.ns AS ns,
  toStartOfMonth(ev.ts) AS month,
  ev.data.source.:String AS source,
  ev.data.resource.:String AS resource,
  toUInt64(1) AS total_calls,
  ev.data.cost.:UInt64 AS total_cost,
  ev.data.price.:UInt64 AS total_price,
  toUInt64(ev.data.cached.:Bool) AS cached_calls
FROM {database}.events AS ev
WHERE ev.type = 'source_call'
`,
  down: `
DROP TABLE IF EXISTS {database}.source_calls_monthly_mv;
DROP TABLE IF EXISTS {database}.source_calls_monthly
`,
}
