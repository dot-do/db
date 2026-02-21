import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 10,
  name: 'sessions',
  up: `
CREATE TABLE IF NOT EXISTS {database}.sessions (
  Timestamp       DateTime64(9) CODEC(Delta(8), ZSTD(1)),
  SessionId       String CODEC(ZSTD(1)),
  ServiceName     LowCardinality(String) CODEC(ZSTD(1)),
  Body            String CODEC(ZSTD(1)),
  ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  LogAttributes   Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  ns              LowCardinality(String),
  INDEX idx_session_id SessionId TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toDateTime(Timestamp), Timestamp)
TTL toDate(Timestamp) + toIntervalDay(30)
;
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.sessions_mv TO {database}.sessions AS
SELECT
  ev.ts AS Timestamp,
  JSONExtractString(ev.data, 'sessionId') AS SessionId,
  JSONExtractString(ev.data, 'serviceName') AS ServiceName,
  JSONExtractString(ev.data, 'body') AS Body,
  JSONExtract(ev.data, 'resourceAttributes', 'Map(String, String)') AS ResourceAttributes,
  JSONExtract(ev.data, 'logAttributes', 'Map(String, String)') AS LogAttributes,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'session'
`,
  down: 'DROP VIEW IF EXISTS {database}.sessions_mv; DROP TABLE IF EXISTS {database}.sessions',
}
