import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 4,
  name: 'data',
  up: `
CREATE TABLE IF NOT EXISTS {database}.data (
  url         String,
  ns          String,
  type        String,
  id          String,
  name        Nullable(String),
  data        JSON,
  content     Nullable(String) CODEC(ZSTD(3)),
  code        Nullable(String) CODEC(ZSTD(3)),
  meta        JSON,
  visibility  Nullable(String),
  updatedAt   DateTime64(3),
  updatedBy   Nullable(String),
  updatedIn   String,
  v           UInt64
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id)
`,
  down: 'DROP TABLE IF EXISTS {database}.data',
}
