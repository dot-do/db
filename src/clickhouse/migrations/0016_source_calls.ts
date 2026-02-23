import type { Migration } from '../migrate'

/**
 * Migration 16: Source call analytics table + materialized view.
 *
 * Events with type='source_call' flow through the existing headlessly_events
 * pipeline. This MV extracts them into a typed source_calls table using
 * columnar JSON path access (ev.data.source.:String, etc.).
 */
export const migration: Migration = {
  version: 16,
  name: 'source_calls',
  up: `
CREATE TABLE IF NOT EXISTS {database}.source_calls (
  id String,
  ns String,
  ts DateTime64(3),
  source String,
  resource String,
  endpoint String,
  capability String,
  latency_ms UInt32,
  status_code UInt16,
  cost Decimal(10,4),
  cached Bool,
  cache_status String,
  cache_ttl UInt32,
  stale_ttl UInt32,
  item_count UInt32,
  input_hash String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ns, source, resource, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.source_calls_mv TO {database}.source_calls AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ev.ts AS ts,
  ev.data.source.:String AS source,
  ev.data.resource.:String AS resource,
  ev.data.endpoint.:String AS endpoint,
  ev.data.capability.:String AS capability,
  ev.data.latency_ms.:UInt32 AS latency_ms,
  ev.data.status.:UInt16 AS status_code,
  ev.data.cost.:Float64 AS cost,
  ev.data.cached.:Bool AS cached,
  ev.data.cache_status.:String AS cache_status,
  ev.data.cache_ttl.:UInt32 AS cache_ttl,
  ev.data.stale_ttl.:UInt32 AS stale_ttl,
  ev.data.item_count.:UInt32 AS item_count,
  ev.data.input_hash.:String AS input_hash
FROM {database}.events AS ev
WHERE ev.type = 'source_call'
`,
  down: `
DROP TABLE IF EXISTS {database}.source_calls_mv;
DROP TABLE IF EXISTS {database}.source_calls
`,
}
