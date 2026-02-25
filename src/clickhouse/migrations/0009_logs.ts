import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 9,
  name: 'logs',
  up: `
CREATE TABLE IF NOT EXISTS {database}.logs (
  Timestamp       DateTime64(9) CODEC(Delta(8), ZSTD(1)),
  TraceId         String CODEC(ZSTD(1)),
  SpanId          String CODEC(ZSTD(1)),
  SeverityText    LowCardinality(String) CODEC(ZSTD(1)),
  SeverityNumber  UInt8,
  ServiceName     LowCardinality(String) CODEC(ZSTD(1)),
  Body            String CODEC(ZSTD(1)),
  ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  LogAttributes   Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  ns              LowCardinality(String),
  INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_body lower(Body) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toDateTime(Timestamp), Timestamp)
TTL toDate(Timestamp) + toIntervalDay(30)

`,
  down: 'DROP TABLE IF EXISTS {database}.logs',
}
