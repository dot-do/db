#!/usr/bin/env npx tsx
/**
 * Set up S3Queue on ClickHouse Cloud to ingest events from R2.
 *
 * Pipeline flow: Worker → headlessly_events stream → events pipeline → events_json sink → R2 events/incoming/
 * S3Queue flow: R2 events/incoming/* → events_queue (S3Queue) → events_ingest_mv (MV) → events table
 *
 * Usage:
 *   setup-s3queue.ts create   — Create S3Queue table + MV
 *   setup-s3queue.ts drop     — Drop S3Queue table + MV
 *   setup-s3queue.ts status   — Check S3Queue processing status
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

const client = new ClickHouseClient({ endpoint, user, password, database: 'default' })
const r2Base = 'https://b6641681fe423910342b9ffa1364c76d.r2.cloudflarestorage.com'
const queueUrl = `${r2Base}/events/incoming/*`

const step = process.argv[2] || 'help'

async function create() {
  console.log('Creating S3Queue table...')

  // S3Queue reads raw JSON lines from R2 (pipeline sink writes JSON format)
  // Column is named 'raw' — ClickHouse Cloud default for single-column JSONEachRow
  // Low polling intervals for <5s e2e latency once files land in R2
  await client.exec(`
    CREATE TABLE IF NOT EXISTS default.events_queue (
      raw String
    ) ENGINE = S3Queue('${queueUrl}', '${r2Ak}', '${r2Sk}', 'JSONEachRow')
    SETTINGS
      mode = 'unordered',
      keeper_path = '/clickhouse/s3queue/events_cloud_v2',
      s3queue_polling_min_timeout_ms = 1000,
      s3queue_polling_max_timeout_ms = 5000,
      s3queue_processing_threads_num = 2,
      s3queue_loading_retries = 3
  `)
  console.log('  events_queue created')

  // MV: parse Pipeline JSON envelope → events table
  // Pipeline wraps events in {"value": {...}} envelope
  console.log('Creating materialized view...')
  await client.exec(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS default.events_ingest_mv TO default.events AS
    SELECT
      coalesce(nullIf(JSONExtractString(raw, 'value', 'id'), ''), toString(generateUUIDv4())) AS id,
      JSONExtractString(raw, 'value', 'ns') AS ns,
      parseDateTime64BestEffortOrZero(JSONExtractString(raw, 'value', 'ts'), 3) AS ts,
      JSONExtractString(raw, 'value', 'type') AS type,
      JSONExtractString(raw, 'value', 'event') AS event,
      JSONExtractString(raw, 'value', 'url') AS url,
      JSONExtractString(raw, 'value', 'source') AS source,
      JSONExtractString(raw, 'value', 'actor') AS actor,
      if(JSONHas(raw, 'value', 'data') AND JSONExtractRaw(raw, 'value', 'data') NOT IN ('', 'null'), JSONExtractRaw(raw, 'value', 'data'), '{}') AS data,
      if(JSONHas(raw, 'value', 'meta') AND JSONExtractRaw(raw, 'value', 'meta') NOT IN ('', 'null'), JSONExtractRaw(raw, 'value', 'meta'), '{}') AS meta
    FROM default.events_queue
  `)
  console.log('  events_ingest_mv created')

  console.log('\nS3Queue setup complete. Events from R2 will be auto-ingested.')
  console.log('Run: setup-s3queue.ts status  to check processing')
}

async function drop() {
  console.log('Dropping S3Queue + MV...')
  await client.exec('DROP VIEW IF EXISTS default.events_ingest_mv')
  console.log('  events_ingest_mv dropped')
  await client.exec('DROP TABLE IF EXISTS default.events_queue')
  console.log('  events_queue dropped')
}

async function status() {
  console.log('S3Queue status:')

  try {
    const tables = await client.query<{ name: string; engine: string }>(`
      SELECT name, engine FROM system.tables
      WHERE database = 'default' AND name IN ('events_queue', 'events_ingest_mv')
    `)
    if (tables.data.length === 0) {
      console.log('  No S3Queue tables found. Run: setup-s3queue.ts create')
      return
    }
    for (const t of tables.data) {
      console.log(`  ${t.name}: ${t.engine}`)
    }
  } catch {
    console.log('  Could not query tables')
  }

  try {
    const count = await client.query<{ cnt: string }>(`SELECT count() as cnt FROM default.events`)
    console.log(`  events table: ${Number(count.data[0].cnt).toLocaleString()} rows`)
  } catch {
    console.log('  Could not count events')
  }

  try {
    const recent = await client.query<{ ts: string; type: string; event: string }>(`
      SELECT ts, type, event FROM default.events ORDER BY ingested DESC LIMIT 5
    `)
    if (recent.data.length > 0) {
      console.log('  Recent events:')
      for (const r of recent.data) {
        console.log(`    ${r.ts} ${r.type} ${r.event}`)
      }
    }
  } catch {
    console.log('  Could not query recent events')
  }
}

if (step === 'create') {
  await create()
} else if (step === 'drop') {
  await drop()
} else if (step === 'status') {
  await status()
} else {
  console.log('Usage: setup-s3queue.ts [create|drop|status]')
  console.log('  create  — Create S3Queue table + MV for auto-ingestion from R2')
  console.log('  drop    — Drop S3Queue table + MV')
  console.log('  status  — Check S3Queue processing status')
}
