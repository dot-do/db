import type { Migration } from '../migrate'

/**
 * Migration 18: Drop premature calls/usage materialized tables.
 *
 * Enforcement queries platform.events directly (with tiered caching).
 * Analytics use views in the stripe/billing compat databases.
 * No need for separate materialized tables — ClickHouse handles
 * aggregation over monthly partitions efficiently.
 */
export const migration: Migration = {
  version: 18,
  name: 'cleanup',
  up: `
DROP TABLE IF EXISTS {database}.calls;
DROP TABLE IF EXISTS {database}.usage
`,
  down: `
CREATE TABLE IF NOT EXISTS {database}.calls (
  id String,
  ns String,
  ts DateTime64(3),
  source String,
  resource String,
  endpoint String,
  capability String,
  latencyMs UInt32,
  statusCode UInt16,
  cost UInt64,
  price UInt64,
  cached Bool,
  cacheStatus String,
  cacheTtl UInt32,
  staleTtl UInt32,
  itemCount UInt32,
  inputHash String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ns, source, resource, ts);

CREATE TABLE IF NOT EXISTS {database}.usage (
  ns String,
  month Date,
  source String,
  resource String,
  totalCalls UInt64,
  totalCost UInt64,
  totalPrice UInt64,
  cachedCalls UInt64
) ENGINE = SummingMergeTree((totalCalls, totalCost, totalPrice, cachedCalls))
PARTITION BY toYYYYMM(month)
ORDER BY (ns, month, source, resource)
`,
}
