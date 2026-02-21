import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 106,
  name: 'svix',
  up: `
CREATE DATABASE IF NOT EXISTS svix
;
CREATE VIEW IF NOT EXISTS svix.endpoints AS
SELECT
  ev.data.id AS id,
  ev.ns AS app_id,
  ev.data.data.targetUrl AS url,
  ev.data.data.event AS filter_types,
  ev.data.data.description AS description,
  ev.data.data.status AS status,
  ev.data.data.secret IS NOT NULL AS has_secret,
  toDateTime(ev.ts) AS created_at,
  toDateTime(ev.data.data.updatedAt) AS updated_at
FROM {database}.events AS ev
WHERE ev.type = 'cdc' AND ev.data.entityType = 'Webhook'
;
CREATE VIEW IF NOT EXISTS svix.messages AS
SELECT
  ev.id AS id,
  ev.ns AS app_id,
  ev.event AS event_type,
  ev.data.entityType AS entity_type,
  ev.data.entityId AS entity_id,
  toString(ev.data.doc) AS payload,
  toDateTime(ev.ts) AS created_at
FROM {database}.events AS ev
WHERE ev.type = 'cdc' AND ev.event LIKE '%.%'
;
CREATE VIEW IF NOT EXISTS svix.message_attempts AS
SELECT
  ev.id AS id,
  ev.ns AS app_id,
  ev.data.input.endpointId AS endpoint_id,
  ev.data.input.messageId AS msg_id,
  ev.data.input.url AS url,
  multiIf(
    ev.data.status = 'completed' AND toUInt16OrZero(toString(ev.data.output.statusCode)) >= 200 AND toUInt16OrZero(toString(ev.data.output.statusCode)) < 300, 'success',
    ev.data.status = 'completed', 'fail',
    ev.data.status = 'error', 'fail',
    'pending'
  ) AS status,
  toUInt16OrZero(toString(ev.data.output.statusCode)) AS response_status_code,
  toString(ev.data.output.body) AS response,
  ev.data.duration AS response_duration_ms,
  toUInt8OrZero(toString(ev.data.input.attemptNumber)) AS attempt_number,
  if(ev.data.input.manual = true, 'manual', 'scheduled') AS trigger_type,
  toDateTime(ev.ts) AS created_at
FROM {database}.events AS ev
WHERE ev.type = 'action' AND ev.data.action = 'webhook.deliver'
;
CREATE VIEW IF NOT EXISTS svix.event_types AS
SELECT
  ev.event AS name,
  count() AS event_count,
  min(ev.ts) AS first_seen,
  max(ev.ts) AS last_seen
FROM {database}.events AS ev
WHERE ev.type = 'cdc' AND ev.event LIKE '%.%'
GROUP BY ev.event
`,
  down: `
DROP VIEW IF EXISTS svix.event_types;
DROP VIEW IF EXISTS svix.message_attempts;
DROP VIEW IF EXISTS svix.messages;
DROP VIEW IF EXISTS svix.endpoints
`.trim(),
}
