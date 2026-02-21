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
  data        String DEFAULT '{}',
  content     Nullable(String) CODEC(ZSTD(3)),
  code        Nullable(String) CODEC(ZSTD(3)),
  meta        String DEFAULT '{}',
  visibility  Nullable(String),
  updatedAt   DateTime64(3),
  updatedBy   Nullable(String),
  updatedIn   String,
  v           UInt64
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, type, id)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.data_mv TO {database}.data AS
SELECT
  url, ns, type, id, name,
  data, content, code,
  '{}' AS meta,
  visibility,
  toDateTime64(v / 1000, 3) AS updatedAt,
  actor AS updatedBy,
  e AS updatedIn,
  v
FROM {database}.versions
`,
  down: 'DROP VIEW IF EXISTS {database}.data_mv; DROP TABLE IF EXISTS {database}.data',
}
