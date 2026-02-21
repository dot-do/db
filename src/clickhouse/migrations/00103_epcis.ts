import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 103,
  name: 'epcis',
  up: `
CREATE DATABASE IF NOT EXISTS epcis
;
CREATE VIEW IF NOT EXISTS epcis.events AS
SELECT
  coalesce(ev.data.eventId, ev.id) AS event_id,
  replaceOne(ev.type, 'epcis.', '') AS event_type,
  toString(coalesce(ev.data.action, '')) AS action,

  ev.ts AS event_time,
  toString(coalesce(ev.data.eventTimeZoneOffset, '')) AS event_tz_offset,

  toString(coalesce(ev.data.bizStep, '')) AS biz_step,
  toString(coalesce(ev.data.disposition, '')) AS disposition,
  toString(coalesce(ev.data.readPoint, ev.url, '')) AS read_point,
  toString(coalesce(ev.data.bizLocation, '')) AS biz_location,

  CAST(coalesce(ev.data.epcList, []), 'Array(String)') AS epc_list,
  toString(coalesce(ev.data.quantityList, '[]')) AS quantity_list,
  toString(coalesce(ev.data.parentID, ev.actor, '')) AS parent_id,
  CAST(coalesce(ev.data.childEPCs, []), 'Array(String)') AS child_epcs,
  toString(coalesce(ev.data.childQuantityList, '[]')) AS child_quantities,
  CAST(coalesce(ev.data.inputEPCList, []), 'Array(String)') AS input_epc_list,
  toString(coalesce(ev.data.inputQuantityList, '[]')) AS input_quantities,
  CAST(coalesce(ev.data.outputEPCList, []), 'Array(String)') AS output_epc_list,
  toString(coalesce(ev.data.outputQuantityList, '[]')) AS output_quantities,
  toString(coalesce(ev.data.transformationID, '')) AS transformation_id,

  toString(coalesce(ev.data.bizTransactionList, '[]')) AS biz_transactions,
  toString(coalesce(ev.data.sourceList, '[]')) AS source_list,
  toString(coalesce(ev.data.destinationList, '[]')) AS destination_list,

  toString(coalesce(ev.data.sensorElementList, '[]')) AS sensor_elements,
  CAST(coalesce(ev.data.persistentDisposition.set, []), 'Array(String)') AS persistent_disp_set,
  CAST(coalesce(ev.data.persistentDisposition.unset, []), 'Array(String)') AS persistent_disp_unset,
  toString(coalesce(ev.data.ilmd, '{}')) AS ilmd,
  toString(coalesce(ev.data.errorDeclaration, '')) AS error_declaration,
  toString(coalesce(ev.data.certificationInfo, '')) AS certification_info,

  ev.ns AS ns,
  ev.actor AS actor
FROM {database}.events AS ev
WHERE ev.type LIKE 'epcis.%'
;
CREATE VIEW IF NOT EXISTS epcis.object_events AS
SELECT * FROM epcis.events WHERE event_type = 'object'
;
CREATE VIEW IF NOT EXISTS epcis.aggregation_events AS
SELECT * FROM epcis.events WHERE event_type = 'aggregation'
;
CREATE VIEW IF NOT EXISTS epcis.transaction_events AS
SELECT * FROM epcis.events WHERE event_type = 'transaction'
;
CREATE VIEW IF NOT EXISTS epcis.transformation_events AS
SELECT * FROM epcis.events WHERE event_type = 'transformation'
;
CREATE VIEW IF NOT EXISTS epcis.association_events AS
SELECT * FROM epcis.events WHERE event_type = 'association'
`,
  down: `
DROP VIEW IF EXISTS epcis.association_events;
DROP VIEW IF EXISTS epcis.transformation_events;
DROP VIEW IF EXISTS epcis.transaction_events;
DROP VIEW IF EXISTS epcis.aggregation_events;
DROP VIEW IF EXISTS epcis.object_events;
DROP VIEW IF EXISTS epcis.events
`.trim(),
}
