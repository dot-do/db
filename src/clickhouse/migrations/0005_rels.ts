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
  meta        Nullable(JSON),
  ns          String,
  v           UInt64,
  e           String
) ENGINE = ReplacingMergeTree(v)
ORDER BY (\`to\`, predicate, \`from\`)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.rels_mv TO {database}.rels AS
SELECT
  ev.data.from AS \`from\`,
  ev.data.predicate AS predicate,
  ev.data.to AS \`to\`,
  ev.data.reverse AS reverse,
  ev.data.type AS type,
  ev.data.meta AS meta,
  ev.ns AS ns,
  toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v,
  ev.id AS e
FROM {database}.events AS ev
WHERE ev.type = 'rel'
`,
  down: 'DROP VIEW IF EXISTS {database}.rels_mv; DROP TABLE IF EXISTS {database}.rels',
}
