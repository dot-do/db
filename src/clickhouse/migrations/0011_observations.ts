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

`,
  down: 'DROP TABLE IF EXISTS {database}.observations',
}
