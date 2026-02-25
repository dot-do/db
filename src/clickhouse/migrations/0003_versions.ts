import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 3,
  name: 'versions',
  up: `
CREATE TABLE IF NOT EXISTS {database}.versions (
  url         String,
  ns          String,
  type        String,
  id          String,
  name        Nullable(String),
  data        JSON,
  content     Nullable(String) CODEC(ZSTD(3)),
  code        Nullable(String) CODEC(ZSTD(3)),
  meta        JSON,
  event       Nullable(String),
  visibility  Nullable(String),
  actor       Nullable(String),
  v           UInt64,
  e           String
) ENGINE = ReplacingMergeTree()
ORDER BY (ns, type, id, e)
`,
  down: 'DROP TABLE IF EXISTS {database}.versions',
}
