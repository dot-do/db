import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 102,
  name: 'billing',
  up: `
CREATE DATABASE IF NOT EXISTS lago
;
CREATE VIEW IF NOT EXISTS lago.events_enriched AS
SELECT
  ev.ns AS organization_id,
  ev.data.externalSubscriptionId AS external_subscription_id,
  ev.data.code AS code,
  ev.ts AS timestamp,
  ev.data.transactionId AS transaction_id,
  CAST(ev.data.properties, 'Map(String, String)') AS properties,
  mapSort(CAST(ev.data.properties, 'Map(String, String)')) AS sorted_properties,
  toString(ev.data.value) AS value,
  toDecimal128OrZero(toString(ev.data.value), 26) AS decimal_value,
  ev.ts AS enriched_at,
  toDecimal128OrNull(ev.data.preciseTotalAmountCents, 15) AS precise_total_amount_cents
FROM {database}.events AS ev
WHERE ev.type = 'meter'
;
CREATE DATABASE IF NOT EXISTS openmeter
;
CREATE VIEW IF NOT EXISTS openmeter.events AS
SELECT
  ev.ns AS namespace,
  ev.id AS id,
  coalesce(ev.data.ceType, ev.data.code) AS type,
  ev.data.subject AS subject,
  ev.data.ceSource AS source,
  ev.ts AS time,
  toString(ev.data.properties) AS data,
  ev.ts AS ingested_at,
  ev.ts AS stored_at,
  ev.id AS store_row_id
FROM {database}.events AS ev
WHERE ev.type = 'meter'
;
CREATE DATABASE IF NOT EXISTS stripe
;
CREATE VIEW IF NOT EXISTS stripe.events AS
SELECT
  ev.data.stripeEventId AS stripe_event_id,
  ev.data.eventType AS event_type,
  ev.data.apiVersion AS api_version,
  ev.ts AS created,
  ev.data.livemode AS livemode,
  ev.data.objectId AS object_id,
  ev.data.objectType AS object_type,
  ev.data.customerId AS customer_id,
  ev.data.amount AS amount,
  ev.data.currency AS currency,
  ev.data.status AS status,
  ev.data.subscriptionId AS subscription_id,
  ev.data.invoiceId AS invoice_id,
  toString(ev.data.payload) AS payload,
  ev.ns AS ns,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type = 'stripe'
;
CREATE VIEW IF NOT EXISTS stripe.invoices AS
SELECT * FROM stripe.events WHERE event_type LIKE 'invoice.%'
;
CREATE VIEW IF NOT EXISTS stripe.payments AS
SELECT * FROM stripe.events WHERE event_type LIKE 'payment_intent.%' OR event_type LIKE 'charge.%'
;
CREATE VIEW IF NOT EXISTS stripe.subscriptions AS
SELECT * FROM stripe.events WHERE event_type LIKE 'customer.subscription.%'
;
CREATE VIEW IF NOT EXISTS stripe.customers AS
SELECT * FROM stripe.events WHERE event_type LIKE 'customer.%' AND event_type NOT LIKE 'customer.subscription.%'
`,
  down: `
DROP VIEW IF EXISTS stripe.customers;
DROP VIEW IF EXISTS stripe.subscriptions;
DROP VIEW IF EXISTS stripe.payments;
DROP VIEW IF EXISTS stripe.invoices;
DROP VIEW IF EXISTS stripe.events;
DROP VIEW IF EXISTS openmeter.events;
DROP VIEW IF EXISTS lago.events_enriched
`.trim(),
}
