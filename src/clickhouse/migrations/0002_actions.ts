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
`,
  down: 'DROP TABLE IF EXISTS {database}.actions',
}
