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

`,
  down: 'DROP TABLE IF EXISTS {database}.scores',
}
