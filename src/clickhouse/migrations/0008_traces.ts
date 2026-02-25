import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 8,
  name: 'traces',
  up: `
CREATE TABLE IF NOT EXISTS {database}.traces (
  Timestamp       DateTime64(9) CODEC(Delta(8), ZSTD(1)),
  TraceId         String CODEC(ZSTD(1)),
  SpanId          String CODEC(ZSTD(1)),
  ParentSpanId    String CODEC(ZSTD(1)),
  SpanName        LowCardinality(String) CODEC(ZSTD(1)),
  SpanKind        LowCardinality(String) CODEC(ZSTD(1)),
  ServiceName     LowCardinality(String) CODEC(ZSTD(1)),
  ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  SpanAttributes  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  Duration        UInt64 CODEC(ZSTD(1)),
  StatusCode      LowCardinality(String) CODEC(ZSTD(1)),
  StatusMessage   String CODEC(ZSTD(1)),
  ns              LowCardinality(String),
  INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
  INDEX idx_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
  INDEX idx_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDate(Timestamp) + toIntervalDay(30)

`,
  down: 'DROP TABLE IF EXISTS {database}.traces',
}
