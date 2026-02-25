import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 19,
  name: 'rename_metrics',
  up: `
CREATE TABLE IF NOT EXISTS {database}.metrics (
  ns          String,
  day         Date,
  type        String,
  event       String,
  eventCount  UInt64,
  entityCount AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (ns, day, type, event)
;
INSERT INTO {database}.metrics
SELECT ns, day, type, event, event_count AS eventCount, entity_count AS entityCount
FROM {database}.metrics_daily
;
RENAME TABLE {database}.metrics_daily TO {database}.metrics_daily_legacy
`,
  down: `
RENAME TABLE {database}.metrics_daily_legacy TO {database}.metrics_daily
;
DROP TABLE IF EXISTS {database}.metrics
`.trim(),
}
