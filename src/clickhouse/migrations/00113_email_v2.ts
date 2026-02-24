import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 113,
  name: 'email_v2',
  up: `
DROP VIEW IF EXISTS email.received
;
DROP VIEW IF EXISTS email.sent
;
CREATE DATABASE IF NOT EXISTS email
;
CREATE VIEW IF NOT EXISTS email.received AS
SELECT
  ev.id AS id,
  -- Flat envelope fields (written by receive handler for easy view access)
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
  replaceOne(ev.ns, 'email.', '') AS tenant,
  ev.ns AS ns,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'email.received'
;
CREATE VIEW IF NOT EXISTS email.sent AS
SELECT
  ev.id AS id,
  ev.action AS action_id,
  JSONExtractString(ev.input, 'to') AS recipient,
  JSONExtractString(ev.input, 'from') AS sender,
  JSONExtractString(ev.input, 'subject') AS subject,
  JSONExtractString(ev.input, 'text') AS text_body,
  JSONExtractString(ev.input, 'html') AS html_body,
  JSONExtractString(ev.input, 'replyTo') AS reply_to,
  JSONExtractString(ev.output, 'id') AS provider_id,
  COALESCE(JSONExtractString(ev.output, 'provider'), 'resend') AS provider,
  ev.status AS status,
  JSONExtractString(ev.error, 'message') AS error,
  ev.duration AS duration,
  ev.ns AS ns,
  ev.ts AS time
FROM {database}.actions AS ev
WHERE ev.action = 'email.send'
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

-- Sent emails (from actions)
SELECT
  ev.id AS id,
  'sent' AS email_type,
  JSONExtractString(ev.input, 'from') AS sender,
  JSONExtractString(ev.input, 'to') AS recipient,
  JSONExtractString(ev.input, 'subject') AS subject,
  JSONExtractString(ev.input, 'text') AS text_body,
  JSONExtractString(ev.input, 'html') AS html_body,
  ev.status AS status,
  ev.ns AS ns,
  ev.ts AS time
FROM {database}.actions AS ev
WHERE ev.action = 'email.send'

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
