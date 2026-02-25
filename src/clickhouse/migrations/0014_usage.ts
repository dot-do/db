import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 14,
  name: 'usage',
  up: `
CREATE TABLE IF NOT EXISTS {database}.usage (
  ns String,
  month Date,
  source String,
  resource String,
  totalCalls UInt64,
  totalCost UInt64,
  totalPrice UInt64,
  cachedCalls UInt64
) ENGINE = SummingMergeTree((totalCalls, totalCost, totalPrice, cachedCalls))
PARTITION BY toYYYYMM(month)
ORDER BY (ns, month, source, resource)
`,
  down: 'DROP TABLE IF EXISTS {database}.usage',
}
