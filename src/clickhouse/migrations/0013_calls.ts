import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 13,
  name: 'calls',
  up: `
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
ORDER BY (ns, source, resource, ts)
`,
  down: 'DROP TABLE IF EXISTS {database}.calls',
}
