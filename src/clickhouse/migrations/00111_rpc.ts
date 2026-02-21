import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 111,
  name: 'rpc',
  up: `
CREATE DATABASE IF NOT EXISTS rpc
;
CREATE VIEW IF NOT EXISTS rpc.batches AS
SELECT
  ev.id AS id,
  ev.data.transport AS transport,
  ev.data.callCount AS call_count,
  ev.data.duration AS duration,
  ev.data.pipelineDepth AS pipeline_depth,
  ev.data.caller AS caller,
  ev.data.target.class AS target_class,
  ev.data.target.id AS target_id,
  ev.data.colo AS colo,
  ev.data.methods AS methods,
  ev.data.errors AS error_count,
  ev.ns AS ns,
  ev.actor AS actor,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'rpc.batch'
;
CREATE VIEW IF NOT EXISTS rpc.calls AS
SELECT
  ev.id AS id,
  ev.data.batchId AS batch_id,
  ev.data.method AS method,
  ev.data.namespace AS namespace,
  ev.data.duration AS duration,
  ev.data.index AS call_index,
  ev.data.pipelined AS pipelined,
  ev.data.error.name AS error_name,
  ev.data.error.message AS error_message,
  ev.data.target.class AS target_class,
  ev.data.target.id AS target_id,
  ev.data.error IS NOT NULL AS is_error,
  ev.ns AS ns,
  ev.actor AS actor,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'rpc.call'
`,
  down: `
DROP VIEW IF EXISTS rpc.calls;
DROP VIEW IF EXISTS rpc.batches
`.trim(),
}
