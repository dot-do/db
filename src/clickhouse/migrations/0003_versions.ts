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
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.versions_mv TO {database}.versions AS
SELECT
  ev.url AS url,
  ev.ns AS ns,
  ev.data.type AS type,
  ev.data.id AS id,
  ev.data.name AS name,
  ev.data AS data,
  ev.data.content AS content,
  ev.data.code AS code,
  ev.meta AS meta,
  ev.event AS event,
  ev.data.visibility AS visibility,
  ev.actor AS actor,
  toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v,
  ev.id AS e
FROM {database}.events AS ev
WHERE ev.type = 'cdc'
`,
  down: 'DROP VIEW IF EXISTS {database}.versions_mv; DROP TABLE IF EXISTS {database}.versions',
}
