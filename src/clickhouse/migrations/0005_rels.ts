import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 5,
  name: 'rels',
  up: `
CREATE TABLE IF NOT EXISTS {database}.rels (
  \`from\`      String,
  predicate   String,
  \`to\`        String,
  reverse     String,
  type        Nullable(String),
  meta        Nullable(String),
  ns          String,
  v           UInt64,
  e           String
) ENGINE = ReplacingMergeTree(v)
ORDER BY (\`to\`, predicate, \`from\`)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.rels_mv TO {database}.rels AS
SELECT
  JSONExtractString(ev.data, 'from') AS \`from\`,
  JSONExtractString(ev.data, 'predicate') AS predicate,
  JSONExtractString(ev.data, 'to') AS \`to\`,
  JSONExtractString(ev.data, 'reverse') AS reverse,
  JSONExtractString(ev.data, 'type') AS type,
  JSONExtractRaw(ev.data, 'meta') AS meta,
  ev.ns AS ns,
  toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v,
  ev.id AS e
FROM {database}.events AS ev
WHERE ev.type = 'rel'
`,
  down: 'DROP VIEW IF EXISTS {database}.rels_mv; DROP TABLE IF EXISTS {database}.rels',
}
