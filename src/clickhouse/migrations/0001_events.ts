import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 1,
  name: 'events',
  up: `
CREATE TABLE IF NOT EXISTS {database}.events (
  id        String,
  ray       String,
  ns        String,
  ts        DateTime64(3),
  type      String,
  event     String,
  actor     JSON(max_dynamic_paths=32),
  url       String,
  source    String,
  data      JSON(max_dynamic_paths=64),
  meta      JSON(max_dynamic_paths=16),
  file      String DEFAULT '',
  ingested  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY id
`,
  down: 'DROP TABLE IF EXISTS {database}.events',
}
