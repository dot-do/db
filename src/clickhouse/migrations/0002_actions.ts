import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 2,
  name: 'actions',
  up: `
CREATE TABLE IF NOT EXISTS {database}.actions (
  id          String,
  url         Nullable(String),
  ns          Nullable(String),
  action      Nullable(String),
  actor       Nullable(String),
  status      Nullable(String),
  input       Nullable(String),
  output      Nullable(String),
  options     Nullable(String),
  error       Nullable(String),
  parent      Nullable(String),
  ts          Nullable(DateTime64(3)),
  duration    Nullable(UInt64)
) ENGINE = CoalescingMergeTree()
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.actions_mv TO {database}.actions AS
SELECT
  JSONExtractString(ev.data, 'actionId') AS id,
  ev.url AS url,
  ev.ns AS ns,
  JSONExtractString(ev.data, 'action') AS action,
  ev.actor AS actor,
  JSONExtractString(ev.data, 'status') AS status,
  JSONExtractRaw(ev.data, 'input') AS input,
  JSONExtractRaw(ev.data, 'output') AS output,
  JSONExtractRaw(ev.data, 'options') AS options,
  JSONExtractRaw(ev.data, 'error') AS error,
  JSONExtractString(ev.data, 'parent') AS parent,
  ev.ts AS ts,
  JSONExtractUInt(ev.data, 'duration') AS duration
FROM {database}.events AS ev
WHERE ev.type = 'action'
`,
  down: 'DROP VIEW IF EXISTS {database}.actions_mv; DROP TABLE IF EXISTS {database}.actions',
}
