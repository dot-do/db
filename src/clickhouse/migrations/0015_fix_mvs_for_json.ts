import type { Migration } from '../migrate'

/**
 * Migration 15: Recreate all materialized views that read from events.
 *
 * The events table columns `data`, `meta`, and `actor` changed from String
 * to native JSON type (migration 13). Existing MVs used JSONExtractString()
 * on these columns, which requires String input. This migration drops and
 * recreates them using direct JSON path access syntax.
 */
export const migration: Migration = {
  version: 15,
  name: 'fix_mvs_for_json',
  up: `
DROP TABLE IF EXISTS {database}.data_mv;
DROP TABLE IF EXISTS {database}.versions_mv;
DROP TABLE IF EXISTS {database}.scores_mv;
DROP TABLE IF EXISTS {database}.observations_mv;
DROP TABLE IF EXISTS {database}.sessions_mv;
DROP TABLE IF EXISTS {database}.traces_mv;
DROP TABLE IF EXISTS {database}.logs_mv;
DROP TABLE IF EXISTS {database}.rels_mv;
DROP TABLE IF EXISTS {database}.actions_mv;

CREATE MATERIALIZED VIEW {database}.actions_mv TO {database}.actions AS
SELECT
  ev.data.actionId.:String AS id,
  ev.url AS url,
  ev.ns AS ns,
  ev.data.action.:String AS action,
  ev.actor.id.:String AS actor,
  ev.data.status.:String AS status,
  toString(ev.data.input) AS input,
  toString(ev.data.output) AS output,
  toString(ev.data.options) AS options,
  toString(ev.data.error) AS error,
  ev.data.parent.:String AS parent,
  ev.ts AS ts,
  ev.data.duration.:UInt64 AS duration
FROM {database}.events AS ev
WHERE ev.type = 'action';

CREATE MATERIALIZED VIEW {database}.rels_mv TO {database}.rels AS
SELECT
  ev.data.\`from\`.:String AS \`from\`,
  ev.data.predicate.:String AS predicate,
  ev.data.\`to\`.:String AS \`to\`,
  ev.data.reverse.:String AS reverse,
  ev.data.type.:String AS type,
  toString(ev.data.meta) AS meta,
  ev.ns AS ns,
  toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v,
  ev.id AS e
FROM {database}.events AS ev
WHERE ev.type = 'rel';

CREATE MATERIALIZED VIEW {database}.logs_mv TO {database}.logs AS
SELECT
  ev.ts AS Timestamp,
  ev.data.traceId.:String AS TraceId,
  ev.data.spanId.:String AS SpanId,
  ev.data.severityText.:String AS SeverityText,
  toUInt8(ev.data.severityNumber.:UInt64) AS SeverityNumber,
  ev.data.serviceName.:String AS ServiceName,
  ev.data.body.:String AS Body,
  CAST(map(), 'Map(String, String)') AS ResourceAttributes,
  CAST(map(), 'Map(String, String)') AS LogAttributes,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'otel.log';

CREATE MATERIALIZED VIEW {database}.traces_mv TO {database}.traces AS
SELECT
  ev.ts AS Timestamp,
  ev.data.traceId.:String AS TraceId,
  ev.data.spanId.:String AS SpanId,
  ev.data.parentSpanId.:String AS ParentSpanId,
  ev.data.spanName.:String AS SpanName,
  ev.data.spanKind.:String AS SpanKind,
  ev.data.serviceName.:String AS ServiceName,
  CAST(map(), 'Map(String, String)') AS ResourceAttributes,
  CAST(map(), 'Map(String, String)') AS SpanAttributes,
  ev.data.duration.:UInt64 AS Duration,
  ev.data.statusCode.:String AS StatusCode,
  ev.data.statusMessage.:String AS StatusMessage,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'otel.span';

CREATE MATERIALIZED VIEW {database}.sessions_mv TO {database}.sessions AS
SELECT
  ev.ts AS Timestamp,
  ev.data.sessionId.:String AS SessionId,
  ev.data.serviceName.:String AS ServiceName,
  ev.data.body.:String AS Body,
  CAST(map(), 'Map(String, String)') AS ResourceAttributes,
  CAST(map(), 'Map(String, String)') AS LogAttributes,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'session';

CREATE MATERIALIZED VIEW {database}.observations_mv TO {database}.observations AS
SELECT
  ev.data.id.:String AS id,
  ev.data.traceId.:String AS trace_id,
  ev.ns AS ns,
  ev.data.type.:String AS type,
  ev.data.name.:String AS name,
  ev.ts AS start_time,
  CAST(NULL, 'Nullable(DateTime64(3))') AS end_time,
  ev.data.model.:String AS model,
  toString(ev.data.input) AS input,
  toString(ev.data.output) AS output,
  CAST(map(), 'Map(String, UInt64)') AS usage,
  CAST(map(), 'Map(String, Float64)') AS cost,
  CAST(NULL, 'Nullable(Float64)') AS total_cost,
  CAST(NULL, 'Nullable(DateTime64(3))') AS completion_start_time,
  CAST(map(), 'Map(String, String)') AS metadata,
  ev.data.level.:String AS level,
  ev.data.status.:String AS status,
  ev.data.parentId.:String AS parent_id,
  ev.actor.id.:String AS actor,
  ev.ts AS event_ts
FROM {database}.events AS ev
WHERE ev.type LIKE 'llm.%' AND ev.type != 'llm.score';

CREATE MATERIALIZED VIEW {database}.scores_mv TO {database}.scores AS
SELECT
  ev.data.id.:String AS id,
  ev.data.traceId.:String AS trace_id,
  ev.data.observationId.:String AS observation_id,
  ev.ns AS ns,
  ev.data.name.:String AS name,
  ev.data.value.:Float64 AS value,
  ev.data.source.:String AS source,
  ev.data.dataType.:String AS data_type,
  ev.data.comment.:String AS comment,
  CAST(map(), 'Map(String, String)') AS metadata,
  ev.actor.id.:String AS actor,
  ev.ts AS event_ts
FROM {database}.events AS ev
WHERE ev.type = 'llm.score';

CREATE MATERIALIZED VIEW {database}.versions_mv TO {database}.versions AS
SELECT
  ev.url AS url,
  ev.ns AS ns,
  ev.data.type.:String AS type,
  ev.data.id.:String AS id,
  ev.data.name.:String AS name,
  toString(ev.data) AS data,
  ev.data.content.:String AS content,
  ev.data.code.:String AS code,
  ev.event AS event,
  ev.data.visibility.:String AS visibility,
  ev.actor.id.:String AS actor,
  toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v,
  ev.id AS e
FROM {database}.events AS ev
WHERE ev.type = 'cdc';

CREATE MATERIALIZED VIEW {database}.data_mv TO {database}.data AS
SELECT
  url, ns, type, id, name, data, content, code,
  '{}' AS meta, visibility,
  toDateTime64(v / 1000, 3) AS updatedAt,
  actor AS updatedBy,
  e AS updatedIn, v
FROM {database}.versions
`,
  down: `
DROP TABLE IF EXISTS {database}.data_mv;
DROP TABLE IF EXISTS {database}.versions_mv;
DROP TABLE IF EXISTS {database}.scores_mv;
DROP TABLE IF EXISTS {database}.observations_mv;
DROP TABLE IF EXISTS {database}.sessions_mv;
DROP TABLE IF EXISTS {database}.traces_mv;
DROP TABLE IF EXISTS {database}.logs_mv;
DROP TABLE IF EXISTS {database}.rels_mv;
DROP TABLE IF EXISTS {database}.actions_mv
`,
}
