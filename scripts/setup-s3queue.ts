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

const client = new ClickHouseClient({ endpoint, user, password, database: 'platform' })
const r2Base = 'https://b6641681fe423910342b9ffa1364c76d.r2.cloudflarestorage.com'
const queueUrl = `${r2Base}/events/incoming/**`

const step = process.argv[2] || 'help'

async function create() {
  console.log('Creating S3Queue table...')

  // S3Queue reads NDJSON from R2 (pipeline sink writes JSON format)
  // Column named 'value' — matches the Pipeline envelope key {"value": {...}}
  // JSONEachRow maps JSON keys to column names, so 'value' gets the inner object
  // Low polling intervals for <5s e2e latency once files land in R2
  await client.exec(`
    CREATE TABLE IF NOT EXISTS platform.queue (
      value String
    ) ENGINE = S3Queue('${queueUrl}', '${r2Ak}', '${r2Sk}', 'JSONEachRow')
    SETTINGS
      mode = 'unordered',
      keeper_path = '/clickhouse/s3queue/platform_queue_v1',
      s3queue_polling_min_timeout_ms = 1000,
      s3queue_polling_max_timeout_ms = 5000,
      s3queue_processing_threads_num = 2,
      s3queue_loading_retries = 3
  `)
  console.log('  platform.queue created')

  // MV: parse Pipeline JSON → events table
  // With column named 'value', JSONEachRow already unwraps the envelope
  // so we extract directly: JSONExtractString(value, 'id') — no nested path
  console.log('Creating materialized view...')
  await client.exec(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS streams.ingest TO platform.events AS
    SELECT
      coalesce(nullIf(JSONExtractString(value, 'id'), ''), toString(generateUUIDv4())) AS id,
      JSONExtractString(value, 'ns') AS ns,
      parseDateTime64BestEffortOrZero(JSONExtractString(value, 'ts'), 3) AS ts,
      JSONExtractString(value, 'type') AS type,
      JSONExtractString(value, 'event') AS event,
      JSONExtractString(value, 'url') AS url,
      JSONExtractString(value, 'source') AS source,
      JSONExtractString(value, 'actor') AS actor,
      if(JSONHas(value, 'data') AND JSONExtractRaw(value, 'data') NOT IN ('', 'null'), JSONExtractRaw(value, 'data'), '{}') AS data,
      if(JSONHas(value, 'meta') AND JSONExtractRaw(value, 'meta') NOT IN ('', 'null'), JSONExtractRaw(value, 'meta'), '{}') AS meta
    FROM platform.queue
  `)
  console.log('  streams.ingest created')

  console.log('\nS3Queue setup complete. Events from R2 will be auto-ingested.')
  console.log('Run: setup-s3queue.ts status  to check processing')
}

async function drop() {
  console.log('Dropping S3Queue + MV...')
  await client.exec('DROP VIEW IF EXISTS streams.ingest')
  console.log('  streams.ingest dropped')
  await client.exec('DROP TABLE IF EXISTS platform.queue')
  console.log('  platform.queue dropped')
}

async function status() {
  console.log('S3Queue status:')

  try {
    const tables = await client.query<{ name: string; engine: string }>(`
      SELECT name, engine FROM system.tables
      WHERE (database = 'platform' AND name = 'queue') OR (database = 'streams' AND name = 'ingest')
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
    const count = await client.query<{ cnt: string }>(`SELECT count() as cnt FROM platform.events`)
    console.log(`  events table: ${Number(count.data[0].cnt).toLocaleString()} rows`)
  } catch {
    console.log('  Could not count events')
  }

  try {
    const recent = await client.query<{ ts: string; type: string; event: string }>(`
      SELECT ts, type, event FROM platform.events ORDER BY ingested DESC LIMIT 5
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
