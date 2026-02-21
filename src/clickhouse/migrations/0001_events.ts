import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 1,
  name: 'events',
  up: `
CREATE TABLE IF NOT EXISTS {database}.events (
  id        String,
  ns        String,
  ts        DateTime64(3),
  type      String,
  event     String,
  url       String,
  source    String,
  actor     String,
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
