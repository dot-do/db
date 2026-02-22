import type { Migration } from '../migrate'

export const migration: Migration = {
  version: 108,
  name: 'api',
  up: `
CREATE DATABASE IF NOT EXISTS api
;
CREATE VIEW IF NOT EXISTS api.requests AS
SELECT
  ev.id AS id,
  ev.data.method AS method,
  ev.data.url AS url,
  ev.data.path AS path,
  ev.data.status AS status,
  ev.data.latency AS latency,
  ev.data.upstreamLatency AS upstream_latency,
  ev.data.gatewayLatency AS gateway_latency,
  ev.data.direction AS direction,
  ev.data.contentType AS content_type,
  ev.data.requestSize AS request_size,
  ev.data.responseSize AS response_size,
  ev.data.requestTruncated AS request_truncated,
  ev.data.responseTruncated AS response_truncated,
  ev.data.clientIp AS client_ip,
  ev.data.userAgent AS user_agent,
  ev.data.apiKeyId AS api_key_id,
  ev.data.consumerId AS consumer_id,
  ev.data.routeId AS route_id,
  ev.data.serviceId AS service_id,
  ev.data.upstream AS upstream,
  ev.data.traceId AS trace_id,
  ev.data.spanId AS span_id,
  ev.data.verbosity AS verbosity,
  ev.ns AS ns,
  toString(ev.actor) AS actor,
  toDateTime(ev.ts) AS time
FROM {database}.events AS ev
WHERE ev.type = 'api.request'
;
CREATE DATABASE IF NOT EXISTS moesif
;
CREATE VIEW IF NOT EXISTS moesif.events AS
SELECT
  ev.id AS _id,
  ev.data.method AS request__verb,
  ev.data.url AS request__uri,
  toString(ev.data.requestHeaders) AS request__headers,
  ev.data.clientIp AS request__ip_address,
  ev.data.requestBody AS request__body,
  ev.data.requestSize AS request__content_length,
  ev.data.status AS response__status,
  toString(ev.data.responseHeaders) AS response__headers,
  ev.data.responseBody AS response__body,
  ev.data.responseSize AS response__content_length,
  ev.data.latency AS duration_ms,
  ev.data.direction AS direction,
  ev.data.consumerId AS user_id,
  ev.ns AS company_id,
  ev.data.traceId AS metadata__trace_id,
  toDateTime(ev.ts) AS request__time
FROM {database}.events AS ev
WHERE ev.type = 'api.request'
;
CREATE DATABASE IF NOT EXISTS apisix
;
CREATE VIEW IF NOT EXISTS apisix.requests AS
SELECT
  ev.id AS id,
  ev.data.method AS request__method,
  ev.data.url AS request__uri,
  ev.data.path AS request__url,
  toString(ev.data.requestHeaders) AS request__headers,
  ev.data.requestSize AS request__size,
  ev.data.status AS response__status,
  toString(ev.data.responseHeaders) AS response__headers,
  ev.data.responseSize AS response__size,
  ev.data.clientIp AS client_ip,
  ev.data.latency AS latency,
  ev.data.upstreamLatency AS upstream_latency,
  greatest(toInt64(ev.data.latency) - toInt64(ev.data.upstreamLatency), 0) AS apisix_latency,
  ev.data.upstream AS upstream,
  ev.data.routeId AS route_id,
  ev.data.serviceId AS service_id,
  ev.data.consumerId AS consumer__username,
  toUnixTimestamp64Milli(ev.ts) AS start_time,
  ev.ns AS ns
FROM {database}.events AS ev
WHERE ev.type = 'api.request'
`,
  down: `
DROP VIEW IF EXISTS apisix.requests;
DROP VIEW IF EXISTS moesif.events;
DROP VIEW IF EXISTS api.requests
`.trim(),
}
