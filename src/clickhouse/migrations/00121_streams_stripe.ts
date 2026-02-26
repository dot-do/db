import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 121,
  name: 'streams_stripe',
  up: `
CREATE TABLE IF NOT EXISTS streams.stripe (
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
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_mv
TO streams.stripe AS
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
WHERE ev.source = 'stripe'
`.trim(),
  down: `
DROP VIEW IF EXISTS streams.stripe_mv;
DROP TABLE IF EXISTS streams.stripe
`.trim(),
}
