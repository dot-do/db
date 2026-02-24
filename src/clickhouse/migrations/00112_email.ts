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
  ev.data.sender AS sender,
  ev.data.from.name AS sender_name,
  ev.data.recipient AS recipient,
  ev.data.to AS to_addresses,
  ev.data.cc AS cc_addresses,
  ev.data.bcc AS bcc_addresses,
  ev.data.replyTo AS reply_to_addresses,
  ev.data.messageId AS message_id,
  ev.data.inReplyTo AS in_reply_to,
  ev.data.references AS \`references\`,
  ev.data.subject AS subject,
  ev.data.text AS text_body,
  ev.data.html AS html_body,
  ev.data.date AS email_date,
  ev.data.attachments AS attachments,
  ev.data.headers AS headers,
  ev.ns AS ns,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'email.received'
;
CREATE VIEW IF NOT EXISTS email.sent AS
SELECT
  ev.id AS id,
  ev.data.actionId AS action_id,
  ev.data.input.to AS recipient,
  ev.data.input.from AS sender,
  ev.data.input.subject AS subject,
  ev.data.input.text AS text_body,
  ev.data.input.html AS html_body,
  ev.data.input.replyTo AS reply_to,
  ev.data.output.id AS provider_id,
  ev.data.output.provider AS provider,
  ev.data.status AS status,
  ev.data.error.message AS error,
  ev.data.duration AS duration,
  ev.ns AS ns,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'action' AND ev.event = 'email.send'
;
CREATE VIEW IF NOT EXISTS email.all AS
-- Received emails (from events)
SELECT
  ev.id AS id,
  'received' AS email_type,
  ev.data.sender AS sender,
  ev.data.recipient AS recipient,
  ev.data.subject AS subject,
  ev.data.text AS text_body,
  ev.data.html AS html_body,
  'completed' AS status,
  ev.ns AS ns,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'email.received'

UNION ALL

-- Sent emails (from events directly — actions table loses nested JSON)
SELECT
  ev.id AS id,
  'sent' AS email_type,
  ev.data.input.from AS sender,
  ev.data.input.to AS recipient,
  ev.data.input.subject AS subject,
  ev.data.input.text AS text_body,
  ev.data.input.html AS html_body,
  ev.data.status AS status,
  ev.ns AS ns,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'action' AND ev.event = 'email.send'

UNION ALL

-- Drafts (from data via CDC — data column is String, use JSONExtract)
SELECT
  ev.id AS id,
  'draft' AS email_type,
  JSONExtractString(ev.data, 'from') AS sender,
  JSONExtractString(JSONExtractArrayRaw(ev.data, 'to')[1], 'email') AS recipient,
  JSONExtractString(ev.data, 'subject') AS subject,
  JSONExtractString(ev.data, 'text') AS text_body,
  JSONExtractString(ev.data, 'html') AS html_body,
  JSONExtractString(ev.data, 'status') AS status,
  ev.ns AS ns,
  ev.updatedAt AS time
FROM {database}.data AS ev
WHERE ev.type = 'email-drafts'
`,
  down: `
DROP VIEW IF EXISTS email.all;
DROP VIEW IF EXISTS email.sent;
DROP VIEW IF EXISTS email.received
`.trim(),
}
