#!/usr/bin/env npx tsx
/**
 * Migrate platform.events to events_v3 with optimized JSON schema.
 *
 * Problem: events.data column has max_dynamic_paths=64, causing 277 MB reads for 31 rows.
 * Solution: Create events_v3 with data JSON(max_dynamic_paths=0) + advanced serialization.
 *
 * Steps:
 *   1. Create events_v3 with correct schema
 *   2. Copy data in time-based chunks (6-hour windows)
 *   3. Drop all MVs referencing platform.events
 *   4. RENAME events → events_v2_old, events_v3 → events
 *   5. Gap-fill: copy any events that arrived during the copy window
 *   6. Recreate all MVs + ingest
 *
 * Usage:
 *   npx tsx .do/db/scripts/migrate-events-v3.ts              # Full migration
 *   npx tsx .do/db/scripts/migrate-events-v3.ts --dry-run     # Show plan without executing
 *   npx tsx .do/db/scripts/migrate-events-v3.ts --copy-only   # Only create table + copy data
 *   npx tsx .do/db/scripts/migrate-events-v3.ts --swap-only   # Only drop MVs + rename + recreate
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

/** Execute DDL/DML via curl (no Node.js fetch timeout issues). */
async function execLong(sql: string): Promise<void> {
  const { execSync } = await import('child_process')
  const url = new URL(process.env.CLICKHOUSE_URL || '')
  url.searchParams.set('default_format', 'TabSeparated')
  url.searchParams.set('max_execution_time', '3600')

  // Use curl with long connect/max timeouts — no undici HeadersTimeoutError
  const result = execSync(
    `curl -sS --max-time 3600 --connect-timeout 30 -X POST '${url.toString()}' ` +
    `-H 'Content-Type: text/plain' ` +
    `-H 'X-ClickHouse-User: default' ` +
    `-H 'X-ClickHouse-Key: ${process.env.CLICKHOUSE_PASSWORD || ''}' ` +
    `--data-binary @-`,
    { input: sql, encoding: 'utf-8', maxBuffer: 10 * 1024 * 1024, timeout: 3600_000 }
  )
  if (result && result.includes('DB::Exception')) {
    throw new Error(`ClickHouse error: ${result.slice(0, 500)}`)
  }
}

const dryRun = process.argv.includes('--dry-run')
const copyOnly = process.argv.includes('--copy-only')
const swapOnly = process.argv.includes('--swap-only')

// ─── Step 1: Create events_v3 ───────────────────────────────────────────────

const CREATE_EVENTS_V3 = `
CREATE TABLE IF NOT EXISTS platform.events_v3 (
  id          String,
  ts          DateTime64(3) MATERIALIZED ULIDStringToDateTime(id),
  ray         String DEFAULT '',
  ns          String,
  domain      String DEFAULT '',
  type        LowCardinality(String),
  event       LowCardinality(String),
  url         String,
  source      LowCardinality(String),
  actor       JSON(max_dynamic_paths = 32),
  data        JSON(max_dynamic_paths = 0),
  meta        JSON(max_dynamic_paths = 0),
  file        String DEFAULT '',
  ingested    DateTime64(3) DEFAULT now64(3),

  INDEX idx_ns ns TYPE bloom_filter GRANULARITY 4,
  INDEX idx_ray ray TYPE minmax GRANULARITY 4,
  INDEX idx_actor_id CAST(actor.id, 'String') TYPE bloom_filter GRANULARITY 4,
  INDEX idx_actor_org CAST(actor.org, 'String') TYPE bloom_filter GRANULARITY 4
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ULIDStringToDateTime(id))
ORDER BY id
SETTINGS
  object_serialization_version = 'v3',
  object_shared_data_serialization_version = 'advanced',
  object_shared_data_serialization_version_for_zero_level_parts = 'map'
`

// ─── Step 2: Time-chunked INSERT SELECT ──────────────────────────────────────

async function copyData() {
  // Get time range
  const range = await client.query<{ min_ts: string; max_ts: string }>(
    `SELECT min(ULIDStringToDateTime(id)) as min_ts, max(ULIDStringToDateTime(id)) as max_ts FROM platform.events`
  )
  const minTs = new Date(range.data[0].min_ts + 'Z')
  const maxTs = new Date(range.data[0].max_ts + 'Z')
  // Extend maxTs by 1 hour to catch anything arriving during copy
  maxTs.setHours(maxTs.getHours() + 1)

  console.log(`  Time range: ${minTs.toISOString()} → ${maxTs.toISOString()}`)

  // Generate 6-hour chunks
  const CHUNK_MS = 6 * 60 * 60 * 1000
  const chunks: { start: string; end: string }[] = []
  let cursor = minTs.getTime()
  while (cursor < maxTs.getTime()) {
    const chunkEnd = Math.min(cursor + CHUNK_MS, maxTs.getTime())
    chunks.push({
      start: new Date(cursor).toISOString().replace('T', ' ').replace('Z', ''),
      end: new Date(chunkEnd).toISOString().replace('T', ' ').replace('Z', ''),
    })
    cursor = chunkEnd
  }

  console.log(`  ${chunks.length} chunks (6-hour windows)`)

  let totalCopied = 0
  for (let i = 0; i < chunks.length; i++) {
    const { start, end } = chunks[i]

    if (dryRun) {
      console.log(`  [DRY] Chunk ${i + 1}/${chunks.length}: ${start} → ${end}`)
      continue
    }

    // Check if chunk already has data (resume support)
    const existing = await client.query<{ cnt: string }>(
      `SELECT count() as cnt FROM platform.events_v3 WHERE ULIDStringToDateTime(id) >= '${start}' AND ULIDStringToDateTime(id) < '${end}'`
    )
    const existingRows = Number(existing.data[0].cnt)
    if (existingRows > 0) {
      totalCopied += existingRows
      console.log(`  Chunk ${i + 1}/${chunks.length}: SKIP (${existingRows.toLocaleString()} rows already present)`)
      continue
    }

    const sql = `INSERT INTO platform.events_v3
      SELECT id, ray, ns, domain, type, event, url, source, actor, data, meta, file, ingested
      FROM platform.events
      WHERE ULIDStringToDateTime(id) >= '${start}' AND ULIDStringToDateTime(id) < '${end}'`

    // Retry up to 3 times with 10-min timeout
    let success = false
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const t0 = Date.now()
        await execLong(sql)
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1)

        const count = await client.query<{ cnt: string }>(
          `SELECT count() as cnt FROM platform.events_v3 WHERE ULIDStringToDateTime(id) >= '${start}' AND ULIDStringToDateTime(id) < '${end}'`
        )
        const chunkRows = Number(count.data[0].cnt)
        totalCopied += chunkRows
        console.log(`  Chunk ${i + 1}/${chunks.length}: ${chunkRows.toLocaleString()} rows (${elapsed}s)`)
        success = true
        break
      } catch (err) {
        console.error(`  Chunk ${i + 1}/${chunks.length}: attempt ${attempt}/3 failed — ${(err as Error).message.slice(0, 100)}`)
        if (attempt < 3) await new Promise((r) => setTimeout(r, 5000))
      }
    }
    if (!success) throw new Error(`Chunk ${i + 1} failed after 3 attempts`)
  }

  if (!dryRun) {
    // Verify total
    const v3Count = await client.query<{ cnt: string }>(`SELECT count() as cnt FROM platform.events_v3`)
    const origCount = await client.query<{ cnt: string }>(`SELECT count() as cnt FROM platform.events`)
    console.log(`  events_v3: ${Number(v3Count.data[0].cnt).toLocaleString()} rows`)
    console.log(`  events:    ${Number(origCount.data[0].cnt).toLocaleString()} rows`)
  }
}

// ─── Step 3: Drop all MVs ────────────────────────────────────────────────────

const MV_DROP_ORDER = [
  'streams.ingest',       // stops new data flowing to events
  'streams.actions',
  'streams.logs',
  'streams.traces',
  'streams.observations',
  'streams.scores',
  'streams.sessions',
  'streams.versions',
  'streams.rels',
  'streams.metrics_daily',
  'streams.github_mv',
  'streams.stripe_mv',
  'streams.workos_mv',
]

async function dropMVs() {
  for (const mv of MV_DROP_ORDER) {
    const sql = `DROP VIEW IF EXISTS ${mv}`
    if (dryRun) {
      console.log(`  [DRY] ${sql}`)
    } else {
      await client.exec(sql)
      console.log(`  Dropped ${mv}`)
    }
  }
}

// ─── Step 4: RENAME tables ───────────────────────────────────────────────────

async function renameTables() {
  const sql1 = `RENAME TABLE platform.events TO platform.events_v2_old`
  const sql2 = `RENAME TABLE platform.events_v3 TO platform.events`

  if (dryRun) {
    console.log(`  [DRY] ${sql1}`)
    console.log(`  [DRY] ${sql2}`)
  } else {
    await client.exec(sql1)
    console.log(`  Renamed events → events_v2_old`)
    await client.exec(sql2)
    console.log(`  Renamed events_v3 → events`)
  }
}

// ─── Step 5: Gap-fill ────────────────────────────────────────────────────────

async function gapFill() {
  if (dryRun) {
    console.log(`  [DRY] Gap-fill: INSERT INTO events SELECT FROM events_v2_old WHERE id NOT IN events`)
    return
  }

  const result = await client.query<{ cnt: string }>(`
    SELECT count() as cnt FROM platform.events_v2_old AS old
    WHERE NOT EXISTS (SELECT 1 FROM platform.events AS new WHERE new.id = old.id)
  `)
  const gapCount = Number(result.data[0].cnt)
  if (gapCount === 0) {
    console.log(`  No gap — all events copied`)
    return
  }

  console.log(`  ${gapCount.toLocaleString()} events to gap-fill`)
  await client.exec(`
    INSERT INTO platform.events
    SELECT id, ray, ns, domain, type, event, url, source, actor, data, meta, file, ingested
    FROM platform.events_v2_old AS old
    WHERE NOT EXISTS (SELECT 1 FROM platform.events AS new WHERE new.id = old.id)
  `)
  console.log(`  Gap-filled ${gapCount.toLocaleString()} events`)
}

// ─── Step 6: Recreate all MVs ────────────────────────────────────────────────

const MV_RECREATE: Record<string, string> = {
  'streams.ingest': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.ingest TO platform.events AS
    SELECT
      if(match(JSONExtractString(value, 'id'), '^[0-9A-HJKMNP-TV-Z]{26}$'), JSONExtractString(value, 'id'), generateULID()) AS id,
      coalesce(nullIf(JSONExtractString(value, 'ray'), ''), nullIf(JSONExtractString(value, 'request', 'headers', 'cf-ray'), ''), nullIf(JSONExtractString(value, 'data', 'event', 'request', 'headers', 'cf-ray'), ''), '') AS ray,
      replaceRegexpOne(coalesce(nullIf(JSONExtractString(value, 'ns'), ''), if(position(domain(coalesce(nullIf(JSONExtractString(value, 'data', 'event', 'request', 'url'), ''), '')), '.') > 0, domain(coalesce(nullIf(JSONExtractString(value, 'data', 'event', 'request', 'url'), ''), '')), '')), '^timing\\\\.', '') AS ns,
      coalesce(nullIf(JSONExtractString(value, 'domain'), ''), nullIf(domain(coalesce(nullIf(JSONExtractString(value, 'url'), ''), nullIf(JSONExtractString(value, 'data', 'event', 'request', 'url'), ''), '')), ''), '') AS domain,
      coalesce(nullIf(JSONExtractString(value, 'type'), ''), multiIf(JSONHas(value, 'data', 'event', 'request'), 'request', JSONHas(value, 'data', 'event', 'rpcMethod'), 'rpc', JSONHas(value, 'data', 'event', 'scheduledTime'), 'cron', JSONHas(value, 'data', 'event', 'queue'), 'queue', JSONExtractString(value, 'data', 'event', 'type') = 'alarm', 'alarm', JSONHas(value, 'data', 'event', 'rcptTo'), 'email', JSONHas(value, 'data', 'event', 'getWebSocketEvent'), 'websocket', 'trace')) AS type,
      coalesce(nullIf(JSONExtractString(value, 'event'), ''), multiIf(JSONHas(value, 'data', 'event', 'request'), concat('fetch.', if(toUInt16OrZero(JSONExtractRaw(value, 'data', 'event', 'response', 'status')) BETWEEN 200 AND 399, 'ok', 'error')), JSONHas(value, 'data', 'event', 'rpcMethod'), concat('rpc.', JSONExtractString(value, 'data', 'event', 'rpcMethod')), JSONHas(value, 'data', 'event', 'scheduledTime'), concat('cron.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')), JSONHas(value, 'data', 'event', 'queue'), concat('queue.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')), JSONExtractString(value, 'data', 'event', 'type') = 'alarm', concat('alarm.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')), JSONHas(value, 'data', 'event', 'rcptTo'), concat('email.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')), JSONHas(value, 'data', 'event', 'getWebSocketEvent'), concat('websocket.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')), coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown'))) AS event,
      coalesce(nullIf(JSONExtractString(value, 'url'), ''), nullIf(cutQueryString(coalesce(nullIf(JSONExtractString(value, 'request', 'url'), ''), nullIf(JSONExtractString(value, 'data', 'event', 'request', 'url'), ''), '')), ''), '') AS url,
      if(JSONExtractString(value, 'source') = 'tail' AND JSONHas(value, 'data', 'scriptName'), JSONExtractString(value, 'data', 'scriptName'), coalesce(nullIf(JSONExtractString(value, 'source'), ''), 'unknown')) AS source,
      if(JSONHas(value, 'actor') AND JSONExtractRaw(value, 'actor') NOT IN ('', 'null', '{}'), JSONExtractRaw(value, 'actor'), concat('{', '"ip":"', replaceAll(coalesce(nullIf(JSONExtractString(value, 'data', 'event', 'request', 'headers', 'cf-connecting-ip'), ''), ''), '"', '\\"'), '",', '"country":"', JSONExtractString(value, 'data', 'event', 'request', 'cf', 'country'), '",', '"city":"', replaceAll(JSONExtractString(value, 'data', 'event', 'request', 'cf', 'city'), '"', '\\"'), '",', '"region":"', replaceAll(JSONExtractString(value, 'data', 'event', 'request', 'cf', 'region'), '"', '\\"'), '",', '"continent":"', JSONExtractString(value, 'data', 'event', 'request', 'cf', 'continent'), '",', '"colo":"', JSONExtractString(value, 'data', 'event', 'request', 'cf', 'colo'), '",', '"timezone":"', JSONExtractString(value, 'data', 'event', 'request', 'cf', 'timezone'), '",', '"asn":', toString(JSONExtractUInt(value, 'data', 'event', 'request', 'cf', 'asn')), ',', '"asOrg":"', replaceAll(JSONExtractString(value, 'data', 'event', 'request', 'cf', 'asOrganization'), '"', '\\"'), '"', '}')) AS actor,
      if(JSONHas(value, 'data') AND JSONExtractRaw(value, 'data') NOT IN ('', 'null'), JSONExtractRaw(value, 'data'), '{}') AS data,
      if(JSONHas(value, 'meta') AND JSONExtractRaw(value, 'meta') NOT IN ('', 'null'), JSONExtractRaw(value, 'meta'), '{}') AS meta,
      _file AS file
    FROM streams.queue`,

  'streams.actions': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.actions TO platform.actions AS
    SELECT ev.data.actionId.:String AS id, ev.url AS url, ev.ns AS ns,
      ev.data.action.:String AS action, ev.actor.id.:String AS actor,
      ev.data.status.:String AS status,
      toString(ev.data.input) AS input, toString(ev.data.output) AS output,
      toString(ev.data.options) AS options, toString(ev.data.error) AS error,
      ev.data.parent.:String AS parent, ULIDStringToDateTime(ev.id) AS ts,
      ev.data.duration.:UInt64 AS duration
    FROM platform.events AS ev WHERE ev.type = 'action'`,

  'streams.logs': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.logs TO platform.logs AS
    SELECT ULIDStringToDateTime(ev.id) AS Timestamp, ev.data.traceId.:String AS TraceId, ev.data.spanId.:String AS SpanId,
      ev.data.severityText.:String AS SeverityText, toUInt8(ev.data.severityNumber.:UInt64) AS SeverityNumber,
      ev.data.serviceName.:String AS ServiceName, ev.data.body.:String AS Body,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS LogAttributes, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'otel.log'`,

  'streams.traces': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.traces TO platform.traces AS
    SELECT ULIDStringToDateTime(ev.id) AS Timestamp, ev.data.traceId.:String AS TraceId, ev.data.spanId.:String AS SpanId,
      ev.data.parentSpanId.:String AS ParentSpanId, ev.data.spanName.:String AS SpanName,
      ev.data.spanKind.:String AS SpanKind, ev.data.serviceName.:String AS ServiceName,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS SpanAttributes,
      ev.data.duration.:UInt64 AS Duration, ev.data.statusCode.:String AS StatusCode,
      ev.data.statusMessage.:String AS StatusMessage, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'otel.span'`,

  'streams.observations': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.observations TO platform.observations AS
    SELECT ev.data.id.:String AS id, ev.data.traceId.:String AS trace_id, ev.ns AS ns,
      ev.data.type.:String AS type, ev.data.name.:String AS name, ULIDStringToDateTime(ev.id) AS start_time,
      CAST(NULL, 'Nullable(DateTime64(3))') AS end_time, ev.data.model.:String AS model,
      toString(ev.data.input) AS input, toString(ev.data.output) AS output,
      CAST(map(), 'Map(String, UInt64)') AS usage, CAST(map(), 'Map(String, Float64)') AS cost,
      CAST(NULL, 'Nullable(Float64)') AS total_cost,
      CAST(NULL, 'Nullable(DateTime64(3))') AS completion_start_time,
      CAST(map(), 'Map(String, String)') AS metadata,
      ev.data.level.:String AS level, ev.data.status.:String AS status,
      ev.data.parentId.:String AS parent_id, ev.actor.id.:String AS actor, ULIDStringToDateTime(ev.id) AS event_ts
    FROM platform.events AS ev WHERE (ev.type LIKE 'llm.%') AND (ev.type != 'llm.score')`,

  'streams.scores': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.scores TO platform.scores AS
    SELECT ev.data.id.:String AS id, ev.data.traceId.:String AS trace_id,
      ev.data.observationId.:String AS observation_id, ev.ns AS ns,
      ev.data.name.:String AS name, ev.data.value.:Float64 AS value,
      ev.data.source.:String AS source, ev.data.dataType.:String AS data_type,
      ev.data.comment.:String AS comment, CAST(map(), 'Map(String, String)') AS metadata,
      ev.actor.id.:String AS actor, ULIDStringToDateTime(ev.id) AS event_ts
    FROM platform.events AS ev WHERE ev.type = 'llm.score'`,

  'streams.sessions': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.sessions TO platform.sessions AS
    SELECT ULIDStringToDateTime(ev.id) AS Timestamp, ev.data.sessionId.:String AS SessionId,
      ev.data.serviceName.:String AS ServiceName, ev.data.body.:String AS Body,
      CAST(map(), 'Map(String, String)') AS ResourceAttributes,
      CAST(map(), 'Map(String, String)') AS LogAttributes, ev.ns AS ns
    FROM platform.events AS ev WHERE ev.type = 'session'`,

  'streams.versions': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.versions TO platform.versions AS
    SELECT ev.url AS url, ev.ns AS ns, ifNull(ev.data.type.:String, '') AS type,
      ifNull(ev.data.id.:String, '') AS id, ev.data.name.:String AS name, toString(ev.data) AS data,
      ev.data.content.:String AS content, ev.data.code.:String AS code, ev.event AS event,
      ev.data.visibility.:String AS visibility, ev.actor.id.:String AS actor,
      toUInt64(toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id))) AS v, ev.id AS e
    FROM platform.events AS ev WHERE ev.type = 'cdc'`,

  'streams.rels': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.rels TO platform.rels AS
    SELECT ev.data.\`from\`.:String AS \`from\`, ev.data.predicate.:String AS predicate,
      ev.data.\`to\`.:String AS \`to\`, ev.data.reverse.:String AS reverse,
      ev.data.type.:String AS type, toString(ev.data.meta) AS meta, ev.ns AS ns,
      toUInt64(toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id))) AS v, ev.id AS e
    FROM platform.events AS ev WHERE ev.type = 'rel'`,

  'streams.metrics_daily': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.metrics_daily TO platform.metrics AS
    SELECT ns, toDate(ULIDStringToDateTime(ev.id)) AS day, type, event,
      count() AS eventCount, uniqState(ev.id) AS entityCount
    FROM platform.events AS ev
    GROUP BY ns, day, type, event`,

  'streams.github_mv': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.github_mv TO streams.github AS
    SELECT CAST(ev.data.entity, 'String') AS entity_type, CAST(ev.data.id, 'String') AS entity_id,
      CAST(ev.data.action, 'String') AS action, ev.ns AS ns, ULIDStringToDateTime(ev.id) AS ts,
      ev.id AS event_id, CAST(ev.data.eventType, 'String') AS event_type,
      CAST(ev.actor.id, 'String') AS sender, toString(ev.data.payload) AS payload,
      toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id)) AS version
    FROM platform.events AS ev WHERE ev.source = 'github'`,

  'streams.stripe_mv': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.stripe_mv TO streams.stripe AS
    SELECT CAST(ev.data.entity, 'String') AS entity_type, CAST(ev.data.id, 'String') AS entity_id,
      CAST(ev.data.action, 'String') AS action, ev.ns AS ns, ULIDStringToDateTime(ev.id) AS ts,
      ev.id AS event_id, CAST(ev.data.eventType, 'String') AS event_type,
      CAST(ev.actor.id, 'String') AS sender, toString(ev.data.payload) AS payload,
      toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id)) AS version
    FROM platform.events AS ev WHERE ev.source = 'stripe'`,

  'streams.workos_mv': `CREATE MATERIALIZED VIEW IF NOT EXISTS streams.workos_mv TO streams.workos AS
    SELECT CAST(ev.data.entity, 'String') AS entity_type, CAST(ev.data.id, 'String') AS entity_id,
      CAST(ev.data.action, 'String') AS action, ev.ns AS ns, ULIDStringToDateTime(ev.id) AS ts,
      ev.id AS event_id, CAST(ev.data.eventType, 'String') AS event_type,
      CAST(ev.actor.id, 'String') AS sender, toString(ev.data.payload) AS payload,
      toUnixTimestamp64Milli(ULIDStringToDateTime(ev.id)) AS version
    FROM platform.events AS ev WHERE ev.source = 'workos'`,
}

async function recreateMVs() {
  for (const [name, sql] of Object.entries(MV_RECREATE)) {
    if (dryRun) {
      console.log(`  [DRY] CREATE ${name}`)
      continue
    }
    try {
      await client.exec(sql)
      console.log(`  Created ${name}`)
    } catch (e: unknown) {
      const msg = (e as Error).message
      if (msg.includes('TIMEOUT_EXCEEDED')) {
        console.log(`  TIMEOUT (executing in bg): ${name}`)
      } else {
        console.error(`  FAILED: ${name} — ${msg.slice(0, 200)}`)
      }
    }
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n=== Events V3 Migration${dryRun ? ' [DRY RUN]' : ''} ===\n`)

  if (!swapOnly) {
    // Step 1: Create events_v3
    console.log('Step 1: Creating platform.events_v3...')
    if (dryRun) {
      console.log('  [DRY] CREATE TABLE platform.events_v3 ...')
    } else {
      await client.exec(CREATE_EVENTS_V3)
      console.log('  Created platform.events_v3')
    }

    // Step 2: Copy data
    console.log('\nStep 2: Copying data (6-hour chunks)...')
    await copyData()

    if (copyOnly) {
      console.log('\n--copy-only: stopping after data copy. Run --swap-only to finish.')
      return
    }
  }

  // Step 3: Drop MVs
  console.log('\nStep 3: Dropping materialized views...')
  await dropMVs()

  // Step 4: RENAME
  console.log('\nStep 4: Renaming tables...')
  await renameTables()

  // Step 5: Gap-fill
  console.log('\nStep 5: Gap-filling missed events...')
  await gapFill()

  // Step 6: Recreate MVs
  console.log('\nStep 6: Recreating materialized views...')
  await recreateMVs()

  // Verify
  console.log('\n=== Verification ===')
  if (!dryRun) {
    const newCount = await client.query<{ cnt: string }>(`SELECT count() as cnt FROM platform.events`)
    const oldCount = await client.query<{ cnt: string }>(`SELECT count() as cnt FROM platform.events_v2_old`)
    console.log(`  platform.events (new):      ${Number(newCount.data[0].cnt).toLocaleString()} rows`)
    console.log(`  platform.events_v2_old:     ${Number(oldCount.data[0].cnt).toLocaleString()} rows`)

    const cols = await client.query<{ name: string; type: string }>(
      `SELECT name, type FROM system.columns WHERE database = 'platform' AND table = 'events' AND name IN ('data', 'meta', 'actor') ORDER BY name`
    )
    for (const c of cols.data) {
      console.log(`  ${c.name}: ${c.type}`)
    }

    const settings = await client.query<{ name: string; value: string }>(
      `SELECT name, value FROM system.merge_tree_settings WHERE database = 'platform' AND table = 'events' AND name LIKE 'object_%'`
    )
    for (const s of settings.data) {
      console.log(`  ${s.name} = ${s.value}`)
    }

    // Quick benchmark
    console.log('\n=== Benchmark ===')
    const t0 = Date.now()
    await client.query(`SELECT id, ns, ts, type, event, source, data FROM platform.events ORDER BY id DESC LIMIT 31`)
    console.log(`  SELECT with data (31 rows): ${Date.now() - t0}ms`)
  }

  console.log('\n=== Migration complete ===')
  console.log('  events_v2_old can be dropped later: DROP TABLE platform.events_v2_old')
}

main().catch((err) => {
  console.error('Migration failed:', err)
  process.exit(1)
})
