import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 113,
  name: 'events',
  up: `
CREATE DATABASE IF NOT EXISTS events
;
CREATE OR REPLACE VIEW events.all AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ev.domain AS domain,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.type AS type,
  ev.event AS event,
  ev.url AS url,
  ev.source AS source,
  ev.actor AS actor,
  ev.data AS data,
  ev.meta AS meta
FROM {database}.events AS ev
;
CREATE OR REPLACE VIEW events.requests AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ev.domain AS domain,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.data.scriptName AS worker,
  ev.data.event.request.method AS method,
  ev.data.event.request.url AS url,
  ev.data.event.response.status AS status,
  ev.data.event.request.cf.country AS country,
  ev.data.event.request.cf.city AS city,
  ev.data.event.request.cf.colo AS colo,
  ev.data.event.request.cf.asOrganization AS asOrg,
  ev.data.event.request.cf.tlsVersion AS tlsVersion,
  ev.data.event.request.cf.httpProtocol AS httpProtocol,
  ev.data.cpuTime AS cpuTimeMs,
  ev.actor AS actor,
  ev.data AS data
FROM {database}.events AS ev
WHERE ev.type = 'request'
;
CREATE OR REPLACE VIEW events.webhooks AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.data.provider AS provider,
  ev.data.eventType AS eventType,
  ev.data.deliveryId AS deliveryId,
  ev.data.verified AS verified,
  ev.data.payload AS payload,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type = 'webhook'
;
CREATE OR REPLACE VIEW events.traces AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.data.scriptName AS worker,
  ev.data.outcome AS outcome,
  ev.data.cpuTime AS cpuTimeMs,
  ev.data.wallTime AS wallTimeMs,
  ev.data.event.request.url AS url,
  ev.data.event.request.method AS method,
  ev.data.event.response.status AS status,
  ev.data.exceptions AS exceptions,
  ev.data AS data
FROM {database}.events AS ev
WHERE ev.type = 'trace'
;
CREATE OR REPLACE VIEW events.rpc AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.data.scriptName AS worker,
  ev.data.outcome AS outcome,
  ev.data.cpuTime AS cpuTimeMs,
  ev.data AS data
FROM {database}.events AS ev
WHERE ev.type = 'rpc'
;
CREATE OR REPLACE VIEW events.cdc AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.source AS source,
  ev.actor AS actor,
  ev.data AS data
FROM {database}.events AS ev
WHERE ev.type = 'cdc'
;
CREATE OR REPLACE VIEW events.cron AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.data.scriptName AS worker,
  ev.data.outcome AS outcome,
  ev.data.cpuTime AS cpuTimeMs,
  ev.data AS data
FROM {database}.events AS ev
WHERE ev.type = 'cron'
;
CREATE OR REPLACE VIEW events.email AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.type AS type,
  ev.event AS event,
  ev.source AS source,
  ev.data AS data,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type LIKE 'email%'
;
CREATE OR REPLACE VIEW events.websocket AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.url AS url,
  ev.data AS data,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type = 'websocket'
;
CREATE OR REPLACE VIEW events.ai AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.source AS source,
  ev.data.provider AS provider,
  ev.data.model AS model,
  ev.data.tenant AS tenant,
  ev.data.inputTokens AS inputTokens,
  ev.data.outputTokens AS outputTokens,
  ev.data.totalTokens AS totalTokens,
  ev.data.cost AS cost,
  ev.data.latencyMs AS latencyMs,
  ev.data.status AS status,
  ev.data.cached AS cached,
  ev.data.agentSlug AS agent,
  ev.data.stopReason AS stopReason,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type IN ('ai.call', 'source.call', 'source_call')
;
CREATE OR REPLACE VIEW events.source_calls AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ev.domain AS domain,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.source AS source,
  ev.data.provider AS provider,
  ev.data.model AS model,
  ev.data.tokens AS tokens,
  ev.data.latencyMs AS latencyMs,
  ev.actor AS actor,
  ev.data AS data
FROM {database}.events AS ev
WHERE ev.type = 'source_call' OR ev.type = 'source.call'
`,
  down: `
DROP VIEW IF EXISTS events.source_calls;
DROP VIEW IF EXISTS events.ai;
DROP VIEW IF EXISTS events.websocket;
DROP VIEW IF EXISTS events.email;
DROP VIEW IF EXISTS events.cron;
DROP VIEW IF EXISTS events.cdc;
DROP VIEW IF EXISTS events.rpc;
DROP VIEW IF EXISTS events.traces;
DROP VIEW IF EXISTS events.webhooks;
DROP VIEW IF EXISTS events.requests;
DROP VIEW IF EXISTS events.all
`.trim(),
}
