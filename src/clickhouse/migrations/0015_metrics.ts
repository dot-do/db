import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 15,
  name: 'metrics',
  up: `
CREATE TABLE IF NOT EXISTS {database}.metrics_daily (
  ns String,
  day Date,
  type String,
  event String,
  event_count UInt64,
  entity_count AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (ns, day, type, event)
`,
  down: 'DROP TABLE IF EXISTS {database}.metrics_daily',
}
