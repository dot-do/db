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
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.logs_mv TO {database}.logs AS
SELECT
  ev.ts AS Timestamp,
  JSONExtractString(ev.data, 'traceId') AS TraceId,
  JSONExtractString(ev.data, 'spanId') AS SpanId,
  JSONExtractString(ev.data, 'severityText') AS SeverityText,
  toUInt8(JSONExtractUInt(ev.data, 'severityNumber')) AS SeverityNumber,
  JSONExtractString(ev.data, 'serviceName') AS ServiceName,
  JSONExtractString(ev.data, 'body') AS Body,
  JSONExtract(ev.data, 'resourceAttributes', 'Map(String, String)') AS ResourceAttributes,
  JSONExtract(ev.data, 'logAttributes', 'Map(String, String)') AS LogAttributes,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'otel.log'
`,
  down: 'DROP VIEW IF EXISTS {database}.logs_mv; DROP TABLE IF EXISTS {database}.logs',
}
