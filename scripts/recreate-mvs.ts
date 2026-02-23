#!/usr/bin/env npx tsx
/**
 * Recreate all materialized views in the streams database.
 * These MVs transform platform.events → platform.{domain} tables.
 *
 * Usage:
 *   npx tsx .do/db/scripts/recreate-mvs.ts          # create (IF NOT EXISTS)
 *   npx tsx .do/db/scripts/recreate-mvs.ts --force   # drop + recreate
 */
import { config } from 'dotenv'
import { resolve } from 'path'
import { ClickHouseClient } from '../src/clickhouse/client'

config({ path: resolve(import.meta.dirname, '../.env') })

const client = new ClickHouseClient({
  endpoint: process.env.CLICKHOUSE_URL || '',
  user: 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

const force = process.argv.includes('--force')

const mvs: Record<string, string> = {
  logs: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.logs TO platform.logs AS
    SELECT ev.ts AS Timestamp, ev.data.traceId.:String AS TraceId, ev.data.spanId.:String AS SpanId,
      ev.data.severityText.:String AS SeverityText, toUInt8(ev.data.severityNumber.:UInt64) AS SeverityNumber,
      ev.data.serviceName.:String AS ServiceName, ev.data.body.:String AS Body,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS LogAttributes, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'otel.log'`,

  traces: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.traces TO platform.traces AS
    SELECT ev.ts AS Timestamp, ev.data.traceId.:String AS TraceId, ev.data.spanId.:String AS SpanId,
      ev.data.parentSpanId.:String AS ParentSpanId, ev.data.spanName.:String AS SpanName,
      ev.data.spanKind.:String AS SpanKind, ev.data.serviceName.:String AS ServiceName,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS SpanAttributes,
      ev.data.duration.:UInt64 AS Duration, ev.data.statusCode.:String AS StatusCode,
      ev.data.statusMessage.:String AS StatusMessage, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'otel.span'`,

  observations: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.observations TO platform.observations AS
    SELECT ev.data.id.:String AS id, ev.data.traceId.:String AS trace_id, ev.ns AS ns,
      ev.data.type.:String AS type, ev.data.name.:String AS name, ev.ts AS start_time,
      CAST(NULL, 'Nullable(DateTime64(3))') AS end_time, ev.data.model.:String AS model,
      toString(ev.data.input) AS input, toString(ev.data.output) AS output,
      CAST(map(), 'Map(String, UInt64)') AS usage, CAST(map(), 'Map(String, Float64)') AS cost,
      CAST(NULL, 'Nullable(Float64)') AS total_cost,
      CAST(NULL, 'Nullable(DateTime64(3))') AS completion_start_time,
      CAST(map(), 'Map(String, String)') AS metadata,
      ev.data.level.:String AS level, ev.data.status.:String AS status,
      ev.data.parentId.:String AS parent_id, ev.actor.id.:String AS actor, ev.ts AS event_ts
    FROM platform.events AS ev WHERE (ev.type LIKE 'llm.%') AND (ev.type != 'llm.score')`,

  scores: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.scores TO platform.scores AS
    SELECT ev.data.id.:String AS id, ev.data.traceId.:String AS trace_id,
      ev.data.observationId.:String AS observation_id, ev.ns AS ns,
      ev.data.name.:String AS name, ev.data.value.:Float64 AS value,
      ev.data.source.:String AS source, ev.data.dataType.:String AS data_type,
      ev.data.comment.:String AS comment, CAST(map(), 'Map(String, String)') AS metadata,
      ev.actor.id.:String AS actor, ev.ts AS event_ts
    FROM platform.events AS ev WHERE ev.type = 'llm.score'`,

  sessions: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.sessions TO platform.sessions AS
    SELECT ev.ts AS Timestamp, ev.data.sessionId.:String AS SessionId,
      ev.data.serviceName.:String AS ServiceName, ev.data.body.:String AS Body,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS LogAttributes, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'session'`,

  versions: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.versions TO platform.versions AS
    SELECT ev.url AS url, ev.ns AS ns, ifNull(ev.data.type.:String, '') AS type,
      ifNull(ev.data.id.:String, '') AS id, ev.data.name.:String AS name, toString(ev.data) AS data,
      ev.data.content.:String AS content, ev.data.code.:String AS code, ev.event AS event,
      ev.data.visibility.:String AS visibility, ev.actor.id.:String AS actor,
      toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v, ev.id AS e
    FROM platform.events AS ev WHERE ev.type = 'cdc'`,

  rels: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.rels TO platform.rels AS
    SELECT ev.data.\`from\`.:String AS \`from\`, ev.data.predicate.:String AS predicate,
      ev.data.\`to\`.:String AS \`to\`, ev.data.reverse.:String AS reverse,
      ev.data.type.:String AS type, toString(ev.data.meta) AS meta, ev.ns AS ns,
      toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v, ev.id AS e
    FROM platform.events AS ev WHERE ev.type = 'rel'`,

  actions: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.actions TO platform.actions AS
    SELECT ev.data.actionId.:String AS id, ev.url AS url, ev.ns AS ns,
      ev.data.action.:String AS action, ev.actor.id.:String AS actor,
      ev.data.status.:String AS status,
      toString(ev.data.input) AS input, toString(ev.data.output) AS output,
      toString(ev.data.options) AS options, toString(ev.data.error) AS error,
      ev.data.parent.:String AS parent, ev.ts AS ts,
      ev.data.duration.:UInt64 AS duration
    FROM platform.events AS ev WHERE ev.type = 'action'`,

  data: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.data TO platform.data AS
    SELECT url, ns, type, id, name, data, content, code, '{}' AS meta, visibility,
      toDateTime64(v / 1000, 3) AS updatedAt, actor AS updatedBy, e AS updatedIn, v
    FROM platform.versions`,

  metrics_daily: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.metrics_daily TO platform.metrics_daily AS
    SELECT ns, toDate(ts) AS day, type, event,
      count() AS event_count, uniqState(id) AS entity_count
    FROM platform.events
    GROUP BY ns, day, type, event`,
}

console.log(`${force ? 'Recreating' : 'Creating'} ${Object.keys(mvs).length} materialized views in streams database...`)

for (const [name, sql] of Object.entries(mvs)) {
  try {
    if (force) {
      await client.exec(`DROP VIEW IF EXISTS streams.${name}`)
    }
    await client.exec(sql)
    console.log(`OK: streams.${name}`)
  } catch (e: unknown) {
    const msg = (e as Error).message
    if (msg.includes('TIMEOUT_EXCEEDED')) {
      console.log(`TIMEOUT (executing in bg): streams.${name}`)
    } else {
      console.error(`FAILED: streams.${name} — ${msg.slice(0, 200)}`)
    }
  }
}

console.log('Done')
