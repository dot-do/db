#!/usr/bin/env npx tsx
/**
 * Rename default → platform database, move MVs to streams database.
 *
 * This is a one-time structural migration:
 *   1. CREATE DATABASE platform, streams
 *   2. DROP all MVs in default (they reference default.X)
 *   3. RENAME TABLE default.X → platform.X for all base tables
 *   4. Recreate S3Queue as platform.queue (same keeper_path for continuity)
 *   5. CREATE all MVs in streams database (clean names, no _mv suffix)
 *   6. Verify everything
 *
 * DDL on ClickHouse Cloud: expect TIMEOUT_EXCEEDED on 1 of 3 replicas.
 * The script treats timeouts as success (DDL executes in background).
 *
 * Usage:
 *   npx tsx .do/db/scripts/rename-to-platform.ts           # dry-run (print SQL)
 *   npx tsx .do/db/scripts/rename-to-platform.ts --execute  # execute DDL
 */
import { config } from 'dotenv'
import { resolve } from 'path'
import { ClickHouseClient } from '../src/clickhouse/client'

config({ path: resolve(import.meta.dirname, '../.env') })

const endpoint = process.env.CLICKHOUSE_URL
const user = process.env.CLICKHOUSE_USER ?? 'default'
const password = process.env.CLICKHOUSE_PASSWORD
const r2Ak = process.env.R2_ACCESS_KEY_ID
const r2Sk = process.env.R2_SECRET_ACCESS_KEY

if (!endpoint || !password) {
  console.error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD in .do/db/.env')
  process.exit(1)
}
if (!r2Ak || !r2Sk) {
  console.error('Missing R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY in .do/db/.env')
  process.exit(1)
}

const execute = process.argv.includes('--execute')
const client = new ClickHouseClient({ endpoint, user, password })

async function run(label: string, sql: string): Promise<boolean> {
  if (!execute) {
    console.log(`[DRY-RUN] ${label}\n  ${sql.trim().split('\n')[0]}...`)
    return true
  }
  try {
    await client.exec(sql)
    console.log(`OK: ${label}`)
    return true
  } catch (e: unknown) {
    const msg = (e as Error).message
    if (msg.includes('TIMEOUT_EXCEEDED')) {
      console.log(`TIMEOUT (executing in bg): ${label}`)
      return true
    }
    if (msg.includes('already exists')) {
      console.log(`EXISTS: ${label}`)
      return true
    }
    console.error(`FAILED: ${label} — ${msg.slice(0, 300)}`)
    return false
  }
}

// ── Step 1: Create databases ──────────────────────────────────────────────────

console.log('\n=== Step 1: Create databases ===')
await run('create platform db', 'CREATE DATABASE IF NOT EXISTS platform')
await run('create streams db', 'CREATE DATABASE IF NOT EXISTS streams')

// ── Step 2: Drop all MVs in default ───────────────────────────────────────────

console.log('\n=== Step 2: Drop MVs in default ===')
const mvsToDrop = [
  'actions_mv',
  'data_mv',
  'events_ingest_mv',
  'events_ingest_headlessly_mv',
  'logs_mv',
  'observations_mv',
  'rels_mv',
  'scores_mv',
  'versions_mv',
]
for (const mv of mvsToDrop) {
  await run(`drop ${mv}`, `DROP VIEW IF EXISTS default.${mv}`)
}

// ── Step 3: Rename base tables ────────────────────────────────────────────────

console.log('\n=== Step 3: Rename tables default → platform ===')
const tablesToRename: Array<[string, string]> = [
  ['events', 'events'],
  ['actions', 'actions'],
  ['artifacts', 'artifacts'],
  ['data', 'data'],
  ['logs', 'logs'],
  ['observations', 'observations'],
  ['rels', 'rels'],
  ['scores', 'scores'],
  ['search', 'search'],
  ['sessions', 'sessions'],
  ['traces', 'traces'],
  ['versions', 'versions'],
  ['schema_migrations', 'migrations'],
]
for (const [from, to] of tablesToRename) {
  await run(`rename ${from} → platform.${to}`, `RENAME TABLE default.${from} TO platform.${to}`)
}

// ── Step 4: Recreate S3Queue as platform.queue ────────────────────────────────

console.log('\n=== Step 4: Recreate S3Queue ===')
// Drop old queue (keeper data persists in ZK, won't reprocess old files)
await run('drop default.events_queue', 'DROP TABLE IF EXISTS default.events_queue')

const r2Base = 'https://b6641681fe423910342b9ffa1364c76d.r2.cloudflarestorage.com'
await run(
  'create platform.queue',
  `CREATE TABLE IF NOT EXISTS platform.queue (value String)
   ENGINE = S3Queue('${r2Base}/events/incoming/**', '${r2Ak}', '${r2Sk}', 'JSONEachRow')
   SETTINGS
     mode = 'unordered',
     keeper_path = '/clickhouse/s3queue/platform_queue_v4',
     s3queue_loading_retries = 3,
     s3queue_processing_threads_num = 2`,
)

// ── Step 5: Create MVs in streams database ────────────────────────────────────

console.log('\n=== Step 5: Create MVs in streams ===')

const streamMvs: Record<string, string> = {
  // S3Queue → platform.events (full schema with actor/url/meta)
  ingest: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.ingest TO platform.events AS
    SELECT
      coalesce(nullIf(JSONExtractString(value, 'id'), ''), toString(generateUUIDv4())) AS id,
      JSONExtractString(value, 'ray') AS ray,
      JSONExtractString(value, 'ns') AS ns,
      coalesce(parseDateTime64BestEffortOrNull(JSONExtractString(value, 'ts'), 3), now64(3)) AS ts,
      JSONExtractString(value, 'type') AS type,
      JSONExtractString(value, 'event') AS event,
      multiIf(
        JSONExtractRaw(value, 'actor') IN ('', 'null'), '{}',
        startsWith(JSONExtractRaw(value, 'actor'), '{'), JSONExtractRaw(value, 'actor'),
        concat('{"id":', JSONExtractRaw(value, 'actor'), '}')
      ) AS actor,
      JSONExtractString(value, 'url') AS url,
      JSONExtractString(value, 'source') AS source,
      if(JSONExtractRaw(value, 'data') IN ('', 'null'), '{}', JSONExtractRaw(value, 'data')) AS data,
      if(JSONExtractRaw(value, 'meta') IN ('', 'null'), '{}', JSONExtractRaw(value, 'meta')) AS meta,
      JSONExtractString(value, 'file') AS file,
      now64(3) AS ingested
    FROM platform.queue`,

  // Daily metrics rollup from platform.events
  metrics_daily: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.metrics_daily TO platform.metrics_daily AS
    SELECT
      ns, toDate(ts) AS day, type, event,
      count() AS event_count, uniqState(id) AS entity_count
    FROM platform.events
    GROUP BY ns, day, type, event`,

  // events → domain tables
  actions: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.actions TO platform.actions AS
    SELECT
      ev.data.actionId.:String AS id, ev.url AS url, ev.ns AS ns,
      ev.data.action.:String AS action, ev.actor.id.:String AS actor,
      ev.data.status.:String AS status,
      toString(ev.data.input) AS input, toString(ev.data.output) AS output,
      toString(ev.data.options) AS options, toString(ev.data.error) AS error,
      ev.data.parent.:String AS parent, ev.ts AS ts,
      ev.data.duration.:UInt64 AS duration
    FROM platform.events AS ev WHERE ev.type = 'action'`,

  logs: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.logs TO platform.logs AS
    SELECT
      ev.ts AS Timestamp, ev.data.traceId.:String AS TraceId,
      ev.data.spanId.:String AS SpanId, ev.data.severityText.:String AS SeverityText,
      toUInt8(ev.data.severityNumber.:UInt64) AS SeverityNumber,
      ev.data.serviceName.:String AS ServiceName, ev.data.body.:String AS Body,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS LogAttributes, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'otel.log'`,

  traces: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.traces TO platform.traces AS
    SELECT
      ev.ts AS Timestamp, ev.data.traceId.:String AS TraceId,
      ev.data.spanId.:String AS SpanId, ev.data.parentSpanId.:String AS ParentSpanId,
      ev.data.spanName.:String AS SpanName, ev.data.spanKind.:String AS SpanKind,
      ev.data.serviceName.:String AS ServiceName,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS SpanAttributes,
      ev.data.duration.:UInt64 AS Duration, ev.data.statusCode.:String AS StatusCode,
      ev.data.statusMessage.:String AS StatusMessage, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'otel.span'`,

  observations: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.observations TO platform.observations AS
    SELECT
      ev.data.id.:String AS id, ev.data.traceId.:String AS trace_id, ev.ns AS ns,
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
    SELECT
      ev.data.id.:String AS id, ev.data.traceId.:String AS trace_id,
      ev.data.observationId.:String AS observation_id, ev.ns AS ns,
      ev.data.name.:String AS name, ev.data.value.:Float64 AS value,
      ev.data.source.:String AS source, ev.data.dataType.:String AS data_type,
      ev.data.comment.:String AS comment, CAST(map(), 'Map(String, String)') AS metadata,
      ev.actor.id.:String AS actor, ev.ts AS event_ts
    FROM platform.events AS ev WHERE ev.type = 'llm.score'`,

  sessions: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.sessions TO platform.sessions AS
    SELECT
      ev.ts AS Timestamp, ev.data.sessionId.:String AS SessionId,
      ev.data.serviceName.:String AS ServiceName, ev.data.body.:String AS Body,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS LogAttributes, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'session'`,

  versions: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.versions TO platform.versions AS
    SELECT
      ev.url AS url, ev.ns AS ns, ifNull(ev.data.type.:String, '') AS type,
      ifNull(ev.data.id.:String, '') AS id, ev.data.name.:String AS name, toString(ev.data) AS data,
      ev.data.content.:String AS content, ev.data.code.:String AS code, ev.event AS event,
      ev.data.visibility.:String AS visibility, ev.actor.id.:String AS actor,
      toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v, ev.id AS e
    FROM platform.events AS ev WHERE ev.type = 'cdc'`,

  rels: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.rels TO platform.rels AS
    SELECT
      ev.data.\`from\`.:String AS \`from\`, ev.data.predicate.:String AS predicate,
      ev.data.\`to\`.:String AS \`to\`, ev.data.reverse.:String AS reverse,
      ev.data.type.:String AS type, toString(ev.data.meta) AS meta, ev.ns AS ns,
      toUInt64(toUnixTimestamp64Milli(ev.ts)) AS v, ev.id AS e
    FROM platform.events AS ev WHERE ev.type = 'rel'`,

  // versions → data (chained MV, not directly from events)
  data: `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.data TO platform.data AS
    SELECT
      url, ns, type, id, name, data, content, code, '{}' AS meta, visibility,
      toDateTime64(v / 1000, 3) AS updatedAt, actor AS updatedBy, e AS updatedIn, v
    FROM platform.versions`,
}

for (const [name, sql] of Object.entries(streamMvs)) {
  await run(`create streams.${name}`, sql)
}

// ── Step 6: Verify ────────────────────────────────────────────────────────────

if (execute) {
  console.log('\n=== Step 6: Verify ===')

  // Wait a moment for DDL propagation
  await new Promise((r) => setTimeout(r, 3000))

  const platformTables = await client.query<{ name: string; engine: string }>(
    `SELECT name, engine FROM system.tables WHERE database = 'platform' ORDER BY name`,
  )
  console.log(`\nplatform database: ${platformTables.data.length} tables`)
  for (const t of platformTables.data) {
    console.log(`  ${t.name} (${t.engine})`)
  }

  const streamsTables = await client.query<{ name: string; engine: string }>(
    `SELECT name, engine FROM system.tables WHERE database = 'streams' ORDER BY name`,
  )
  console.log(`\nstreams database: ${streamsTables.data.length} tables`)
  for (const t of streamsTables.data) {
    console.log(`  ${t.name} (${t.engine})`)
  }

  const defaultTables = await client.query<{ name: string; engine: string }>(
    `SELECT name, engine FROM system.tables WHERE database = 'default' ORDER BY name`,
  )
  console.log(`\ndefault database (should be empty): ${defaultTables.data.length} tables`)
  for (const t of defaultTables.data) {
    console.log(`  ${t.name} (${t.engine})`)
  }

  // Check row counts
  try {
    const eventsCount = await client.query<{ c: string }>('SELECT count() AS c FROM platform.events')
    console.log(`\nplatform.events: ${Number(eventsCount.data[0].c).toLocaleString()} rows`)
  } catch {
    console.log('\nplatform.events: query failed (table may still be propagating)')
  }
} else {
  console.log('\n=== Dry run complete. Run with --execute to apply. ===')
}
