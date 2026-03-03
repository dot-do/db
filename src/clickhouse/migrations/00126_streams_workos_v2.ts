import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 126,
  name: 'streams_workos_v2',
  up: `
DROP VIEW IF EXISTS streams.workos_mv
;
DROP TABLE IF EXISTS streams.workos
;
CREATE TABLE IF NOT EXISTS streams.workos_authentications (
  id           String,
  ts           DateTime64(3),
  ns           String,
  event_id     String DEFAULT '',
  event_type   LowCardinality(String) DEFAULT '',
  status       LowCardinality(String) DEFAULT '',
  auth_type    LowCardinality(String) DEFAULT '',
  user_id      String DEFAULT '',
  email        String DEFAULT '',
  ip_address   String DEFAULT '',
  user_agent   String DEFAULT ''
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_authentications_mv
TO streams.workos_authentications AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS event_id,
  CAST(ev.data.\`eventType\`, 'String') AS event_type,
  CAST(ev.data.\`payload.data.status\`, 'String') AS status,
  CAST(ev.data.\`payload.data.type\`, 'String') AS auth_type,
  CAST(ev.data.\`payload.data.user_id\`, 'String') AS user_id,
  CAST(ev.data.\`payload.data.email\`, 'String') AS email,
  CAST(ev.data.\`payload.data.ip_address\`, 'String') AS ip_address,
  CAST(ev.data.\`payload.data.user_agent\`, 'String') AS user_agent
FROM {database}.events AS ev
WHERE ev.source = 'workos'
  AND CAST(ev.data.\`eventType\`, 'String') LIKE 'authentication.%'
;
CREATE TABLE IF NOT EXISTS streams.workos_sessions (
  id              String,
  ts              DateTime64(3),
  ns              String,
  event_id        String DEFAULT '',
  event_type      LowCardinality(String) DEFAULT '',
  action          LowCardinality(String) DEFAULT '',
  session_id      String DEFAULT '',
  user_id         String DEFAULT '',
  organization_id String DEFAULT '',
  ip_address      String DEFAULT '',
  user_agent      String DEFAULT '',
  status          LowCardinality(String) DEFAULT '',
  auth_method     String DEFAULT '',
  expires_at      String DEFAULT ''
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_sessions_mv
TO streams.workos_sessions AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS event_id,
  CAST(ev.data.\`eventType\`, 'String') AS event_type,
  replaceOne(CAST(ev.data.\`eventType\`, 'String'), 'session.', '') AS action,
  CAST(ev.data.\`payload.data.id\`, 'String') AS session_id,
  CAST(ev.data.\`payload.data.user_id\`, 'String') AS user_id,
  CAST(ev.data.\`payload.data.organization_id\`, 'String') AS organization_id,
  CAST(ev.data.\`payload.data.ip_address\`, 'String') AS ip_address,
  CAST(ev.data.\`payload.data.user_agent\`, 'String') AS user_agent,
  CAST(ev.data.\`payload.data.status\`, 'String') AS status,
  CAST(ev.data.\`payload.data.auth_method\`, 'String') AS auth_method,
  CAST(ev.data.\`payload.data.expires_at\`, 'String') AS expires_at
FROM {database}.events AS ev
WHERE ev.source = 'workos'
  AND CAST(ev.data.\`eventType\`, 'String') LIKE 'session.%'
;
CREATE TABLE IF NOT EXISTS streams.workos_users (
  id             String,
  ts             DateTime64(3),
  ns             String,
  event_id       String DEFAULT '',
  event_type     LowCardinality(String) DEFAULT '',
  action         LowCardinality(String) DEFAULT '',
  user_id        String DEFAULT '',
  email          String DEFAULT '',
  email_verified Bool DEFAULT false,
  user           JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_users_mv
TO streams.workos_users AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS event_id,
  CAST(ev.data.\`eventType\`, 'String') AS event_type,
  replaceOne(CAST(ev.data.\`eventType\`, 'String'), 'user.', '') AS action,
  CAST(ev.data.\`payload.data.id\`, 'String') AS user_id,
  CAST(ev.data.\`payload.data.email\`, 'String') AS email,
  CAST(ev.data.\`payload.data.email_verified\`, 'Bool') AS email_verified,
  JSONExtractRaw(toString(ev.data), 'payload', 'data') AS user
FROM {database}.events AS ev
WHERE ev.source = 'workos'
  AND CAST(ev.data.\`eventType\`, 'String') LIKE 'user.%'
;
CREATE TABLE IF NOT EXISTS streams.workos_organizations (
  id           String,
  ts           DateTime64(3),
  ns           String,
  event_id     String DEFAULT '',
  event_type   LowCardinality(String) DEFAULT '',
  action       LowCardinality(String) DEFAULT '',
  org_id       String DEFAULT '',
  name         String DEFAULT '',
  organization JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_organizations_mv
TO streams.workos_organizations AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS event_id,
  CAST(ev.data.\`eventType\`, 'String') AS event_type,
  replaceOne(CAST(ev.data.\`eventType\`, 'String'), 'organization.', '') AS action,
  CAST(ev.data.\`payload.data.id\`, 'String') AS org_id,
  CAST(ev.data.\`payload.data.name\`, 'String') AS name,
  JSONExtractRaw(toString(ev.data), 'payload', 'data') AS organization
FROM {database}.events AS ev
WHERE ev.source = 'workos'
  AND CAST(ev.data.\`eventType\`, 'String') LIKE 'organization.%'
  AND CAST(ev.data.\`eventType\`, 'String') NOT LIKE 'organization_membership.%'
;
CREATE TABLE IF NOT EXISTS streams.workos_organization_memberships (
  id              String,
  ts              DateTime64(3),
  ns              String,
  event_id        String DEFAULT '',
  event_type      LowCardinality(String) DEFAULT '',
  action          LowCardinality(String) DEFAULT '',
  membership_id   String DEFAULT '',
  user_id         String DEFAULT '',
  organization_id String DEFAULT '',
  role            String DEFAULT '',
  status          LowCardinality(String) DEFAULT '',
  membership      JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_organization_memberships_mv
TO streams.workos_organization_memberships AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS event_id,
  CAST(ev.data.\`eventType\`, 'String') AS event_type,
  replaceOne(CAST(ev.data.\`eventType\`, 'String'), 'organization_membership.', '') AS action,
  CAST(ev.data.\`payload.data.id\`, 'String') AS membership_id,
  CAST(ev.data.\`payload.data.user_id\`, 'String') AS user_id,
  CAST(ev.data.\`payload.data.organization_id\`, 'String') AS organization_id,
  CAST(ev.data.\`payload.data.role\`, 'String') AS role,
  CAST(ev.data.\`payload.data.status\`, 'String') AS status,
  JSONExtractRaw(toString(ev.data), 'payload', 'data') AS membership
FROM {database}.events AS ev
WHERE ev.source = 'workos'
  AND CAST(ev.data.\`eventType\`, 'String') LIKE 'organization_membership.%'
;
CREATE TABLE IF NOT EXISTS streams.workos_email_verifications (
  id           String,
  ts           DateTime64(3),
  ns           String,
  event_id     String DEFAULT '',
  event_type   LowCardinality(String) DEFAULT '',
  action       LowCardinality(String) DEFAULT '',
  user_id      String DEFAULT '',
  email        String DEFAULT '',
  verification JSON(max_dynamic_paths = 4)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_email_verifications_mv
TO streams.workos_email_verifications AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS event_id,
  CAST(ev.data.\`eventType\`, 'String') AS event_type,
  replaceOne(CAST(ev.data.\`eventType\`, 'String'), 'email_verification.', '') AS action,
  CAST(ev.data.\`payload.data.user_id\`, 'String') AS user_id,
  CAST(ev.data.\`payload.data.email\`, 'String') AS email,
  JSONExtractRaw(toString(ev.data), 'payload', 'data') AS verification
FROM {database}.events AS ev
WHERE ev.source = 'workos'
  AND CAST(ev.data.\`eventType\`, 'String') LIKE 'email_verification.%'
`.trim(),
  down: `
DROP VIEW IF EXISTS streams.workos_email_verifications_mv
;
DROP TABLE IF EXISTS streams.workos_email_verifications
;
DROP VIEW IF EXISTS streams.workos_organization_memberships_mv
;
DROP TABLE IF EXISTS streams.workos_organization_memberships
;
DROP VIEW IF EXISTS streams.workos_organizations_mv
;
DROP TABLE IF EXISTS streams.workos_organizations
;
DROP VIEW IF EXISTS streams.workos_users_mv
;
DROP TABLE IF EXISTS streams.workos_users
;
DROP VIEW IF EXISTS streams.workos_sessions_mv
;
DROP TABLE IF EXISTS streams.workos_sessions
;
DROP VIEW IF EXISTS streams.workos_authentications_mv
;
DROP TABLE IF EXISTS streams.workos_authentications
;
CREATE TABLE IF NOT EXISTS streams.workos (
  entity_type  LowCardinality(String),
  entity_id    String,
  action       LowCardinality(String),
  ns           String,
  ts           DateTime64(3),
  event_id     String,
  event_type   LowCardinality(String),
  sender       String DEFAULT '',
  payload      String,
  version      UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(ts)
ORDER BY (entity_type, entity_id)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_mv
TO streams.workos AS
SELECT
  ev.data.entity::String AS entity_type,
  ev.data.id::String AS entity_id,
  ev.data.action::String AS action,
  ev.ns AS ns,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.id AS event_id,
  ev.data.eventType::String AS event_type,
  ev.actor.id::String AS sender,
  toString(ev.data.payload) AS payload,
  toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id)) AS version
FROM {database}.events AS ev
WHERE ev.source = 'workos'
`.trim(),
}
