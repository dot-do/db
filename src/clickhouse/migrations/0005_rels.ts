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
`,
  down: 'DROP TABLE IF EXISTS {database}.rels',
}
