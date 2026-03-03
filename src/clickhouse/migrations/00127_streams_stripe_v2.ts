import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 127,
  name: 'streams_stripe_v2',
  up: `
DROP VIEW IF EXISTS streams.stripe_mv
;
DROP TABLE IF EXISTS streams.stripe
;
CREATE TABLE IF NOT EXISTS streams.stripe_customers (
  id                String,
  ts                DateTime64(3),
  ns                String,
  stripe_event_id   String DEFAULT '',
  account           String DEFAULT '',
  api_version       String DEFAULT '',
  livemode          Bool DEFAULT false,
  action            LowCardinality(String) DEFAULT '',
  customer_id       String DEFAULT '',
  email             String DEFAULT '',
  name              String DEFAULT '',
  customer          JSON(max_dynamic_paths = 16)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_customers_mv
TO streams.stripe_customers AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS stripe_event_id,
  CAST(ev.data.\`payload.account\`, 'String') AS account,
  CAST(ev.data.\`payload.api_version\`, 'String') AS api_version,
  CAST(ev.data.\`payload.livemode\`, 'Bool') AS livemode,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), 2) AS action,
  CAST(ev.data.\`payload.data.object.id\`, 'String') AS customer_id,
  CAST(ev.data.\`payload.data.object.email\`, 'String') AS email,
  CAST(ev.data.\`payload.data.object.name\`, 'String') AS name,
  JSONExtractRaw(toString(ev.data), 'payload', 'data', 'object') AS customer
FROM {database}.events AS ev
WHERE ev.source = 'stripe'
  AND CAST(ev.data.eventType, 'String') IN ('customer.created', 'customer.updated', 'customer.deleted')
;
CREATE TABLE IF NOT EXISTS streams.stripe_subscriptions (
  id                    String,
  ts                    DateTime64(3),
  ns                    String,
  stripe_event_id       String DEFAULT '',
  account               String DEFAULT '',
  api_version           String DEFAULT '',
  livemode              Bool DEFAULT false,
  action                LowCardinality(String) DEFAULT '',
  subscription_id       String DEFAULT '',
  customer_id           String DEFAULT '',
  status                LowCardinality(String) DEFAULT '',
  current_period_start  String DEFAULT '',
  current_period_end    String DEFAULT '',
  subscription          JSON(max_dynamic_paths = 16)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_subscriptions_mv
TO streams.stripe_subscriptions AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS stripe_event_id,
  CAST(ev.data.\`payload.account\`, 'String') AS account,
  CAST(ev.data.\`payload.api_version\`, 'String') AS api_version,
  CAST(ev.data.\`payload.livemode\`, 'Bool') AS livemode,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), -1) AS action,
  CAST(ev.data.\`payload.data.object.id\`, 'String') AS subscription_id,
  CAST(ev.data.\`payload.data.object.customer\`, 'String') AS customer_id,
  CAST(ev.data.\`payload.data.object.status\`, 'String') AS status,
  CAST(ev.data.\`payload.data.object.current_period_start\`, 'String') AS current_period_start,
  CAST(ev.data.\`payload.data.object.current_period_end\`, 'String') AS current_period_end,
  JSONExtractRaw(toString(ev.data), 'payload', 'data', 'object') AS subscription
FROM {database}.events AS ev
WHERE ev.source = 'stripe'
  AND startsWith(CAST(ev.data.eventType, 'String'), 'customer.subscription.')
;
CREATE TABLE IF NOT EXISTS streams.stripe_invoices (
  id              String,
  ts              DateTime64(3),
  ns              String,
  stripe_event_id String DEFAULT '',
  account         String DEFAULT '',
  api_version     String DEFAULT '',
  livemode        Bool DEFAULT false,
  action          LowCardinality(String) DEFAULT '',
  invoice_id      String DEFAULT '',
  customer_id     String DEFAULT '',
  subscription_id String DEFAULT '',
  status          LowCardinality(String) DEFAULT '',
  amount_due      String DEFAULT '',
  amount_paid     String DEFAULT '',
  currency        LowCardinality(String) DEFAULT '',
  invoice         JSON(max_dynamic_paths = 16)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_invoices_mv
TO streams.stripe_invoices AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS stripe_event_id,
  CAST(ev.data.\`payload.account\`, 'String') AS account,
  CAST(ev.data.\`payload.api_version\`, 'String') AS api_version,
  CAST(ev.data.\`payload.livemode\`, 'Bool') AS livemode,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), 2) AS action,
  CAST(ev.data.\`payload.data.object.id\`, 'String') AS invoice_id,
  CAST(ev.data.\`payload.data.object.customer\`, 'String') AS customer_id,
  CAST(ev.data.\`payload.data.object.subscription\`, 'String') AS subscription_id,
  CAST(ev.data.\`payload.data.object.status\`, 'String') AS status,
  CAST(ev.data.\`payload.data.object.amount_due\`, 'String') AS amount_due,
  CAST(ev.data.\`payload.data.object.amount_paid\`, 'String') AS amount_paid,
  CAST(ev.data.\`payload.data.object.currency\`, 'String') AS currency,
  JSONExtractRaw(toString(ev.data), 'payload', 'data', 'object') AS invoice
FROM {database}.events AS ev
WHERE ev.source = 'stripe'
  AND startsWith(CAST(ev.data.eventType, 'String'), 'invoice.')
;
CREATE TABLE IF NOT EXISTS streams.stripe_payments (
  id              String,
  ts              DateTime64(3),
  ns              String,
  stripe_event_id String DEFAULT '',
  account         String DEFAULT '',
  api_version     String DEFAULT '',
  livemode        Bool DEFAULT false,
  action          LowCardinality(String) DEFAULT '',
  payment_id      String DEFAULT '',
  customer_id     String DEFAULT '',
  amount          UInt64 DEFAULT 0,
  currency        LowCardinality(String) DEFAULT '',
  status          LowCardinality(String) DEFAULT '',
  payment         JSON(max_dynamic_paths = 16)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_payments_mv
TO streams.stripe_payments AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS stripe_event_id,
  CAST(ev.data.\`payload.account\`, 'String') AS account,
  CAST(ev.data.\`payload.api_version\`, 'String') AS api_version,
  CAST(ev.data.\`payload.livemode\`, 'Bool') AS livemode,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), 2) AS action,
  CAST(ev.data.\`payload.data.object.id\`, 'String') AS payment_id,
  CAST(ev.data.\`payload.data.object.customer\`, 'String') AS customer_id,
  CAST(ev.data.\`payload.data.object.amount\`, 'UInt64') AS amount,
  CAST(ev.data.\`payload.data.object.currency\`, 'String') AS currency,
  CAST(ev.data.\`payload.data.object.status\`, 'String') AS status,
  JSONExtractRaw(toString(ev.data), 'payload', 'data', 'object') AS payment
FROM {database}.events AS ev
WHERE ev.source = 'stripe'
  AND (
    startsWith(CAST(ev.data.eventType, 'String'), 'payment_intent.')
    OR startsWith(CAST(ev.data.eventType, 'String'), 'charge.')
  )
;
CREATE TABLE IF NOT EXISTS streams.stripe_checkout_sessions (
  id                String,
  ts                DateTime64(3),
  ns                String,
  stripe_event_id   String DEFAULT '',
  account           String DEFAULT '',
  api_version       String DEFAULT '',
  livemode          Bool DEFAULT false,
  action            LowCardinality(String) DEFAULT '',
  session_id        String DEFAULT '',
  customer_id       String DEFAULT '',
  payment_intent_id String DEFAULT '',
  subscription_id   String DEFAULT '',
  status            LowCardinality(String) DEFAULT '',
  mode              LowCardinality(String) DEFAULT '',
  session           JSON(max_dynamic_paths = 16)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_checkout_sessions_mv
TO streams.stripe_checkout_sessions AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS stripe_event_id,
  CAST(ev.data.\`payload.account\`, 'String') AS account,
  CAST(ev.data.\`payload.api_version\`, 'String') AS api_version,
  CAST(ev.data.\`payload.livemode\`, 'Bool') AS livemode,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), 3) AS action,
  CAST(ev.data.\`payload.data.object.id\`, 'String') AS session_id,
  CAST(ev.data.\`payload.data.object.customer\`, 'String') AS customer_id,
  CAST(ev.data.\`payload.data.object.payment_intent\`, 'String') AS payment_intent_id,
  CAST(ev.data.\`payload.data.object.subscription\`, 'String') AS subscription_id,
  CAST(ev.data.\`payload.data.object.status\`, 'String') AS status,
  CAST(ev.data.\`payload.data.object.mode\`, 'String') AS mode,
  JSONExtractRaw(toString(ev.data), 'payload', 'data', 'object') AS session
FROM {database}.events AS ev
WHERE ev.source = 'stripe'
  AND startsWith(CAST(ev.data.eventType, 'String'), 'checkout.session.')
;
CREATE TABLE IF NOT EXISTS streams.stripe_products (
  id              String,
  ts              DateTime64(3),
  ns              String,
  stripe_event_id String DEFAULT '',
  account         String DEFAULT '',
  api_version     String DEFAULT '',
  livemode        Bool DEFAULT false,
  action          LowCardinality(String) DEFAULT '',
  object_type     LowCardinality(String) DEFAULT '',
  object_id       String DEFAULT '',
  name            String DEFAULT '',
  active          Bool DEFAULT false,
  object          JSON(max_dynamic_paths = 8)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY id
;
CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_products_mv
TO streams.stripe_products AS
SELECT
  ev.id AS id,
  ULIDStringToDateTime(ev.id) AS ts,
  ev.ns AS ns,
  CAST(ev.data.\`payload.id\`, 'String') AS stripe_event_id,
  CAST(ev.data.\`payload.account\`, 'String') AS account,
  CAST(ev.data.\`payload.api_version\`, 'String') AS api_version,
  CAST(ev.data.\`payload.livemode\`, 'Bool') AS livemode,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), 2) AS action,
  arrayElement(splitByChar('.', CAST(ev.data.eventType, 'String')), 1) AS object_type,
  CAST(ev.data.\`payload.data.object.id\`, 'String') AS object_id,
  CAST(ev.data.\`payload.data.object.name\`, 'String') AS name,
  CAST(ev.data.\`payload.data.object.active\`, 'Bool') AS active,
  JSONExtractRaw(toString(ev.data), 'payload', 'data', 'object') AS object
FROM {database}.events AS ev
WHERE ev.source = 'stripe'
  AND (
    startsWith(CAST(ev.data.eventType, 'String'), 'product.')
    OR startsWith(CAST(ev.data.eventType, 'String'), 'price.')
  )
`.trim(),
  down: `
DROP VIEW IF EXISTS streams.stripe_products_mv
;
DROP TABLE IF EXISTS streams.stripe_products
;
DROP VIEW IF EXISTS streams.stripe_checkout_sessions_mv
;
DROP TABLE IF EXISTS streams.stripe_checkout_sessions
;
DROP VIEW IF EXISTS streams.stripe_payments_mv
;
DROP TABLE IF EXISTS streams.stripe_payments
;
DROP VIEW IF EXISTS streams.stripe_invoices_mv
;
DROP TABLE IF EXISTS streams.stripe_invoices
;
DROP VIEW IF EXISTS streams.stripe_subscriptions_mv
;
DROP TABLE IF EXISTS streams.stripe_subscriptions
;
DROP VIEW IF EXISTS streams.stripe_customers_mv
;
DROP TABLE IF EXISTS streams.stripe_customers
;
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
}
