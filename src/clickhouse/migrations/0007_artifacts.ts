import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 7,
  name: 'artifacts',
  up: `
CREATE TABLE IF NOT EXISTS {database}.artifacts (
  url         String,
  format      String,
  ns          String,
  name        Nullable(String),
  data        JSON,
  v           UInt64,
  e           String
) ENGINE = ReplacingMergeTree(v)
ORDER BY (ns, url, format)
`,
  down: 'DROP TABLE IF EXISTS {database}.artifacts',
}
