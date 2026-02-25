import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 107,
  name: 'unkey',
  up: `
CREATE DATABASE IF NOT EXISTS unkey
;
CREATE VIEW IF NOT EXISTS unkey.key_verifications AS
SELECT
  ev.id AS request_id,
  ev.data.input.keyId AS key_id,
  ev.ns AS workspace_id,
  ev.data.input.apiId AS api_id,
  multiIf(
    ev.data.status = 'error', 'INSUFFICIENT_PERMISSIONS',
    ev.data.output.valid = true, 'VALID',
    ev.data.output.rateLimited = true, 'RATE_LIMITED',
    ev.data.output.expired = true, 'EXPIRED',
    ev.data.output.disabled = true, 'DISABLED',
    'INVALID'
  ) AS outcome,
  ev.data.duration AS latency,
  ev.data.output.remaining AS remaining,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'action' AND ev.data.action LIKE 'apikey.verify%'
;
CREATE VIEW IF NOT EXISTS unkey.rate_limits AS
SELECT
  ev.id AS request_id,
  ev.data.input.identifier AS identifier,
  ev.ns AS workspace_id,
  ev.data.output.passed AS passed,
  ev.data.output.limit AS \`limit\`,
  ev.data.output.remaining AS remaining,
  ev.data.output.reset AS reset_at,
  ev.data.duration AS latency,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'action' AND ev.data.action LIKE 'ratelimit.%'
`,
  down: `
DROP VIEW IF EXISTS unkey.rate_limits;
DROP VIEW IF EXISTS unkey.key_verifications
`.trim(),
}
