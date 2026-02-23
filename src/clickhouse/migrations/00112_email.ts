import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 112,
  name: 'email',
  up: `
CREATE DATABASE IF NOT EXISTS email
;
CREATE VIEW IF NOT EXISTS email.received AS
SELECT
  ev.id AS id,
  ev.data.from AS sender,
  ev.data.to AS recipient,
  ev.data.subject AS subject,
  ev.data.text AS text_body,
  ev.data.html AS html_body,
  ev.data.date AS email_date,
  replaceOne(ev.ns, 'email.', '') AS tenant,
  ev.ns AS ns,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'email.received'
;
CREATE VIEW IF NOT EXISTS email.sent AS
SELECT
  ev.id AS id,
  ev.data.to AS recipient,
  ev.data.from AS sender,
  ev.data.subject AS subject,
  ev.data.providerId AS provider_id,
  ev.data.provider AS provider,
  ev.data.error AS error,
  ev.ns AS ns,
  ev.event AS event,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type IN ('email.sent', 'email.send.failed')
`,
  down: `
DROP VIEW IF EXISTS email.sent;
DROP VIEW IF EXISTS email.received
`.trim(),
}
