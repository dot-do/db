import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 124,
  name: 'events_ts_default',
  up: `
ALTER TABLE {database}.events MODIFY COLUMN ts DateTime64(3) DEFAULT ULIDStringToDateTime(id)
`,
  down: `
ALTER TABLE {database}.events MODIFY COLUMN ts DateTime64(3) MATERIALIZED ULIDStringToDateTime(id)
`.trim(),
}
