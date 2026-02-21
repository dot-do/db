import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 11,
  name: 'observations',
  up: `
CREATE TABLE IF NOT EXISTS {database}.observations (
  id              String,
  trace_id        String,
  ns              String,
  type            LowCardinality(String),
  name            String,
  start_time      DateTime64(3),
  end_time        Nullable(DateTime64(3)),
  model           Nullable(String),
  input           Nullable(String) CODEC(ZSTD(3)),
  output          Nullable(String) CODEC(ZSTD(3)),
  usage           Map(LowCardinality(String), UInt64),
  cost            Map(LowCardinality(String), Decimal64(12)),
  total_cost      Nullable(Decimal64(12)),
  completion_start_time Nullable(DateTime64(3)),
  metadata        Map(LowCardinality(String), String),
  level           LowCardinality(String),
  status          Nullable(String),
  parent_id       Nullable(String),
  actor           Nullable(String),
  event_ts        DateTime64(3),
  INDEX idx_trace_id trace_id TYPE bloom_filter() GRANULARITY 1,
  INDEX idx_id id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree(event_ts)
PARTITION BY toYYYYMM(start_time)
ORDER BY (ns, type, toDate(start_time), id)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.observations_mv TO {database}.observations AS
SELECT
  ev.data.id AS id,
  ev.data.traceId AS trace_id,
  ev.ns AS ns,
  ev.data.type AS type,
  ev.data.name AS name,
  coalesce(parseDateTime64BestEffortOrNull(ev.data.startTime, 3), ev.ts) AS start_time,
  parseDateTime64BestEffortOrNull(ev.data.endTime, 3) AS end_time,
  ev.data.model AS model,
  toString(ev.data.input) AS input,
  toString(ev.data.output) AS output,
  CAST(ev.data.usage, 'Map(String, UInt64)') AS usage,
  CAST(ev.data.cost, 'Map(String, Decimal64(12))') AS cost,
  ev.data.totalCost AS total_cost,
  parseDateTime64BestEffortOrNull(ev.data.completionStartTime, 3) AS completion_start_time,
  CAST(ev.data.metadata, 'Map(String, String)') AS metadata,
  ev.data.level AS level,
  ev.data.status AS status,
  ev.data.parentId AS parent_id,
  ev.actor AS actor,
  ev.ts AS event_ts
FROM {database}.events AS ev
WHERE ev.type LIKE 'llm.%' AND ev.type != 'llm.score'
`,
  down: 'DROP VIEW IF EXISTS {database}.observations_mv; DROP TABLE IF EXISTS {database}.observations',
}
