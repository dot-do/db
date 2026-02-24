#!/usr/bin/env npx tsx
/**
 * Set up S3Queue on ClickHouse Cloud to ingest events from R2.
 *
 * Pipeline flow: Worker → headlessly_events stream → events pipeline → events_json sink → R2 events/incoming/
 * S3Queue flow: R2 events/incoming/* → streams.queue (S3Queue) → streams.ingest (MV) → platform.events
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
    CREATE TABLE IF NOT EXISTS streams.queue (
      value String
    ) ENGINE = S3Queue('${queueUrl}', '${r2Ak}', '${r2Sk}', 'JSONEachRow')
    SETTINGS
      mode = 'unordered',
      keeper_path = '/clickhouse/s3queue/platform_queue_v7',
      s3queue_polling_min_timeout_ms = 500,
      s3queue_polling_max_timeout_ms = 2000,
      s3queue_processing_threads_num = 4,
      s3queue_loading_retries = 3
  `)
  console.log('  streams.queue created')

  // Wait for S3Queue to fully initialize (ClickHouse Cloud replication delay)
  console.log('  waiting 5s for S3Queue initialization...')
  await new Promise((r) => setTimeout(r, 5000))

  // MV: parse Pipeline JSON → events table
  // Handles TWO record shapes:
  //   1. Full records (ingest, otel, webhooks, CDC): source provides ns/type/event/url/ray/actor
  //   2. Minimal records (tail): only id/ts/source/data — everything else derived here
  //
  // For tail events, the full TraceItem is in `data`, including:
  //   data.event.request.url      → ns (hostname), url (path)
  //   data.event.request.headers  → ray (cf-ray)
  //   data.event.request.cf       → actor (geo, network, bot detection)
  //   data.event.request/rpcMethod/scheduledTime/queue/etc → type, event
  //   data.scriptName, data.outcome → event name components
  //
  // For sources with HTTP context, request.cf can also be passed at top level.
  console.log('Creating materialized view...')
  await client.exec(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS streams.ingest TO platform.events AS
    SELECT
      -- id: preserve valid ULIDs, generate if missing (dedup with direct CH inserts)
      if(
        match(JSONExtractString(value, 'id'), '^[0-9A-HJKMNP-TV-Z]{26}$'),
        JSONExtractString(value, 'id'),
        generateULID()
      ) AS id,

      -- ray: source-provided > request.headers > tail trace headers
      coalesce(
        nullIf(JSONExtractString(value, 'ray'), ''),
        nullIf(JSONExtractString(value, 'request', 'headers', 'cf-ray'), ''),
        nullIf(JSONExtractString(value, 'data', 'event', 'request', 'headers', 'cf-ray'), ''),
        ''
      ) AS ray,

      -- ns: source-provided > hostname from request url > hostname from tail trace > scriptName
      coalesce(
        nullIf(JSONExtractString(value, 'ns'), ''),
        nullIf(domain(coalesce(
          nullIf(JSONExtractString(value, 'request', 'url'), ''),
          nullIf(JSONExtractString(value, 'data', 'event', 'request', 'url'), ''),
          ''
        )), ''),
        nullIf(JSONExtractString(value, 'data', 'scriptName'), ''),
        'unknown'
      ) AS ns,

      parseDateTime64BestEffortOrZero(JSONExtractString(value, 'ts'), 3) AS ts,

      -- type: source-provided > classify from tail trace event shape
      coalesce(
        nullIf(JSONExtractString(value, 'type'), ''),
        multiIf(
          JSONHas(value, 'data', 'event', 'request'),       'request',
          JSONHas(value, 'data', 'event', 'rpcMethod'),     'rpc',
          JSONHas(value, 'data', 'event', 'scheduledTime'), 'cron',
          JSONHas(value, 'data', 'event', 'queue'),         'queue',
          JSONExtractString(value, 'data', 'event', 'type') = 'alarm', 'alarm',
          JSONHas(value, 'data', 'event', 'rcptTo'),        'email',
          JSONHas(value, 'data', 'event', 'getWebSocketEvent'), 'websocket',
          'trace'
        )
      ) AS type,

      -- event: source-provided > derive from tail trace (scriptName.category.outcome)
      coalesce(
        nullIf(JSONExtractString(value, 'event'), ''),
        concat(
          coalesce(nullIf(JSONExtractString(value, 'data', 'scriptName'), ''), 'unknown'),
          '.',
          multiIf(
            JSONHas(value, 'data', 'event', 'request'),
              concat('fetch.', if(
                toUInt16OrZero(JSONExtractRaw(value, 'data', 'event', 'response', 'status')) BETWEEN 200 AND 399,
                'ok', 'error'
              )),
            JSONHas(value, 'data', 'event', 'rpcMethod'),
              concat('rpc.', JSONExtractString(value, 'data', 'event', 'rpcMethod')),
            JSONHas(value, 'data', 'event', 'scheduledTime'),
              concat('cron.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')),
            JSONHas(value, 'data', 'event', 'queue'),
              concat('queue.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')),
            JSONExtractString(value, 'data', 'event', 'type') = 'alarm',
              concat('alarm.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')),
            JSONHas(value, 'data', 'event', 'rcptTo'),
              concat('email.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')),
            JSONHas(value, 'data', 'event', 'getWebSocketEvent'),
              concat('websocket.', coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')),
            coalesce(nullIf(JSONExtractString(value, 'data', 'outcome'), ''), 'unknown')
          )
        )
      ) AS event,

      -- url: source-provided > request url (strip query) > tail trace url (strip query)
      coalesce(
        nullIf(JSONExtractString(value, 'url'), ''),
        nullIf(cutQueryString(coalesce(
          nullIf(JSONExtractString(value, 'request', 'url'), ''),
          nullIf(JSONExtractString(value, 'data', 'event', 'request', 'url'), ''),
          ''
        )), ''),
        ''
      ) AS url,

      coalesce(nullIf(JSONExtractString(value, 'source'), ''), 'unknown') AS source,

      -- actor: source-provided > request.cf > tail trace cf
      multiIf(
        JSONHas(value, 'actor') AND JSONExtractRaw(value, 'actor') NOT IN ('', 'null', '{}'),
          JSONExtractRaw(value, 'actor'),
        JSONHas(value, 'request', 'cf'),
          JSONExtractRaw(value, 'request', 'cf'),
        JSONHas(value, 'data', 'event', 'request', 'cf'),
          JSONExtractRaw(value, 'data', 'event', 'request', 'cf'),
        '{}'
      ) AS actor,

      if(JSONHas(value, 'data') AND JSONExtractRaw(value, 'data') NOT IN ('', 'null'),
        JSONExtractRaw(value, 'data'), '{}') AS data,
      if(JSONHas(value, 'meta') AND JSONExtractRaw(value, 'meta') NOT IN ('', 'null'),
        JSONExtractRaw(value, 'meta'), '{}') AS meta,
      _file AS file
    FROM streams.queue
  `)
  console.log('  streams.ingest created')

  console.log('\nS3Queue setup complete. Events from R2 will be auto-ingested.')
  console.log('Run: setup-s3queue.ts status  to check processing')
}

async function drop() {
  console.log('Dropping S3Queue + MV...')
  await client.exec('DROP VIEW IF EXISTS streams.ingest')
  console.log('  streams.ingest dropped')
  await client.exec('DROP TABLE IF EXISTS streams.queue')
  console.log('  streams.queue dropped')
}

async function status() {
  console.log('S3Queue status:')

  try {
    const tables = await client.query<{ name: string; engine: string }>(`
      SELECT name, engine FROM system.tables
      WHERE database = 'streams' AND name IN ('queue', 'ingest')
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
