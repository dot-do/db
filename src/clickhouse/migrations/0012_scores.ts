import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 12,
  name: 'scores',
  up: `
CREATE TABLE IF NOT EXISTS {database}.scores (
  id              String,
  trace_id        Nullable(String),
  observation_id  Nullable(String),
  ns              String,
  name            String,
  value           Float64,
  source          String,
  data_type       String,
  comment         Nullable(String) CODEC(ZSTD(1)),
  metadata        Map(LowCardinality(String), String),
  actor           Nullable(String),
  event_ts        DateTime64(3),
  INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (ns, toDate(event_ts), name, id)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.scores_mv TO {database}.scores AS
SELECT
  ev.data.id AS id,
  ev.data.traceId AS trace_id,
  ev.data.observationId AS observation_id,
  ev.ns AS ns,
  ev.data.name AS name,
  ev.data.value AS value,
  ev.data.source AS source,
  ev.data.dataType AS data_type,
  ev.data.comment AS comment,
  CAST(ev.data.metadata, 'Map(String, String)') AS metadata,
  ev.actor AS actor,
  ev.ts AS event_ts
FROM {database}.events AS ev
WHERE ev.type = 'llm.score'
`,
  down: 'DROP VIEW IF EXISTS {database}.scores_mv; DROP TABLE IF EXISTS {database}.scores',
}
