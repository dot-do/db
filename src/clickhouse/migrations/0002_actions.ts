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
  input       Nullable(JSON),
  output      Nullable(JSON),
  options     Nullable(JSON),
  error       Nullable(JSON),
  parent      Nullable(String),
  ts          Nullable(DateTime64(3)),
  duration    Nullable(UInt64)
) ENGINE = CoalescingMergeTree()
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.actions_mv TO {database}.actions AS
SELECT
  ev.data.actionId AS id,
  ev.url AS url,
  ev.ns AS ns,
  ev.data.action AS action,
  ev.actor AS actor,
  ev.data.status AS status,
  ev.data.input AS input,
  ev.data.output AS output,
  ev.data.options AS options,
  ev.data.error AS error,
  ev.data.parent AS parent,
  ev.ts AS ts,
  ev.data.duration AS duration
FROM {database}.events AS ev
WHERE ev.type = 'action'
`,
  down: 'DROP VIEW IF EXISTS {database}.actions_mv; DROP TABLE IF EXISTS {database}.actions',
}
