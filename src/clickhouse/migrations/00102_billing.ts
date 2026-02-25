import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 102,
  name: 'stripe',
  up: `
CREATE DATABASE IF NOT EXISTS stripe
;
CREATE OR REPLACE VIEW stripe.events AS
SELECT
  ev.data.stripeEventId AS stripe_event_id,
  ev.data.eventType AS event_type,
  ev.data.apiVersion AS api_version,
  ULIDStringToDateTime(ev.id) AS created,
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
  toString(ev.actor) AS actor
FROM {database}.events AS ev
WHERE ev.type = 'stripe'
;
CREATE OR REPLACE VIEW stripe.invoices AS
SELECT * FROM stripe.events WHERE event_type LIKE 'invoice.%'
;
CREATE OR REPLACE VIEW stripe.payments AS
SELECT * FROM stripe.events WHERE event_type LIKE 'payment_intent.%' OR event_type LIKE 'charge.%'
;
CREATE OR REPLACE VIEW stripe.subscriptions AS
SELECT * FROM stripe.events WHERE event_type LIKE 'customer.subscription.%'
;
CREATE OR REPLACE VIEW stripe.customers AS
SELECT * FROM stripe.events WHERE event_type LIKE 'customer.%' AND event_type NOT LIKE 'customer.subscription.%'
`,
  down: `
DROP VIEW IF EXISTS stripe.customers;
DROP VIEW IF EXISTS stripe.subscriptions;
DROP VIEW IF EXISTS stripe.payments;
DROP VIEW IF EXISTS stripe.invoices;
DROP VIEW IF EXISTS stripe.events
`.trim(),
}
