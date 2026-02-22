import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 13,
  name: 'events_recreate',
  up: `
DROP TABLE IF EXISTS {database}.events;

CREATE TABLE {database}.events (
  id        String,
  ray       String,
  ns        String,
  ts        DateTime64(3),
  type      String,
  event     String,
  actor     JSON,
  url       String,
  source    String,
  data      JSON,
  meta      JSON,
  file      String DEFAULT '',
  ingested  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY id
`,
  down: 'DROP TABLE IF EXISTS {database}.events',
}
