import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 14,
  name: 'events_queue',
  up: `
DROP TABLE IF EXISTS {database}.events_ingest_mv;

DROP TABLE IF EXISTS {database}.events_queue;

CREATE TABLE {database}.events_queue (
  value String
) ENGINE = S3Queue(
  '{env.R2_URL}/events/incoming/**',
  '{env.R2_ACCESS_KEY_ID}',
  '{env.R2_SECRET_ACCESS_KEY}',
  'JSONEachRow'
) SETTINGS
  mode = 'unordered',
  keeper_path = '/clickhouse/s3queue/events_cloud_v6',
  s3queue_loading_retries = 3,
  s3queue_processing_threads_num = 2;

CREATE MATERIALIZED VIEW {database}.events_ingest_mv TO {database}.events AS
SELECT
  coalesce(nullIf(JSONExtractString(value, 'id'), ''), toString(generateUUIDv4())) AS id,
  JSONExtractString(value, 'ray')    AS ray,
  JSONExtractString(value, 'ns')     AS ns,
  coalesce(
    parseDateTime64BestEffortOrNull(JSONExtractString(value, 'ts'), 3),
    now64(3)
  ) AS ts,
  JSONExtractString(value, 'type')   AS type,
  JSONExtractString(value, 'event')  AS event,
  if(JSONExtractRaw(value, 'actor') IN ('', 'null'), '{}', JSONExtractRaw(value, 'actor')) AS actor,
  JSONExtractString(value, 'url')    AS url,
  JSONExtractString(value, 'source') AS source,
  if(JSONExtractRaw(value, 'data') IN ('', 'null'), '{}', JSONExtractRaw(value, 'data')) AS data,
  if(JSONExtractRaw(value, 'meta') IN ('', 'null'), '{}', JSONExtractRaw(value, 'meta')) AS meta,
  JSONExtractString(value, 'file')   AS file,
  now64(3)                           AS ingested
FROM {database}.events_queue
`,
  down: `
DROP TABLE IF EXISTS {database}.events_ingest_mv;
DROP TABLE IF EXISTS {database}.events_queue
`,
}
