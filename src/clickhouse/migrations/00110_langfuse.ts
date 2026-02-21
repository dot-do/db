import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 110,
  name: 'langfuse',
  up: `
CREATE DATABASE IF NOT EXISTS langfuse
;
CREATE VIEW IF NOT EXISTS langfuse.traces AS
SELECT
  o.trace_id AS id,
  o.ns AS project_id,
  o.name AS name,
  o.actor AS user_id,
  o.metadata AS metadata,
  min(o.start_time) AS start_time,
  max(o.end_time) AS end_time,
  sumMap(o.cost) AS total_cost,
  count() AS observation_count
FROM {database}.observations AS o
GROUP BY o.trace_id, o.ns, o.name, o.actor, o.metadata
;
CREATE VIEW IF NOT EXISTS langfuse.observations AS
SELECT
  o.id AS id,
  o.trace_id AS trace_id,
  o.ns AS project_id,
  o.type AS type,
  o.name AS name,
  o.model AS model,
  o.start_time AS start_time,
  o.end_time AS end_time,
  o.completion_start_time AS completion_start_time,
  o.input AS input,
  o.output AS output,
  o.usage AS usage,
  o.total_cost AS total_cost,
  o.metadata AS metadata,
  o.level AS level,
  o.status AS status_message,
  o.parent_id AS parent_observation_id,
  o.actor AS user_id
FROM {database}.observations AS o
;
CREATE VIEW IF NOT EXISTS langfuse.scores AS
SELECT
  s.id AS id,
  s.trace_id AS trace_id,
  s.observation_id AS observation_id,
  s.ns AS project_id,
  s.name AS name,
  s.value AS value,
  s.source AS source,
  s.data_type AS data_type,
  s.comment AS comment,
  s.metadata AS metadata,
  s.event_ts AS created_at
FROM {database}.scores AS s
`,
  down: `
DROP VIEW IF EXISTS langfuse.scores;
DROP VIEW IF EXISTS langfuse.observations;
DROP VIEW IF EXISTS langfuse.traces
`.trim(),
}
