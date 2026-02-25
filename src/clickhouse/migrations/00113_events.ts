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
  ULIDStringToDateTime(ev.id) AS ts,
  ev.type AS type,
  ev.event AS event,
  ev.actor AS actor,
  ev.url AS url,
  ev.source AS source,
  ev.data AS data,
  ev.meta AS meta
FROM {database}.events AS ev
;
CREATE OR REPLACE VIEW events.requests AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.url AS url,
  ev.source AS source,
  ev.data AS data,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type = 'request'
;
CREATE OR REPLACE VIEW events.webhooks AS
SELECT
  ev.id AS id,
  ev.ray AS ray,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.url AS url,
  ev.source AS source,
  ev.data AS data,
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
  ev.url AS url,
  ev.source AS source,
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
  ev.url AS url,
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
  ev.url AS url,
  ev.data AS data,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type = 'cdc'
;
CREATE OR REPLACE VIEW events.cron AS
SELECT
  ev.id AS id,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.event AS event,
  ev.url AS url,
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
`,
  down: `
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
