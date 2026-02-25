#!/usr/bin/env npx tsx
/**
 * Fix historical webhook + CDC events and optionally export to R2.
 *
 * Usage:
 *   export-events.ts export     — Export corrected webhook + CDC to R2 (archive)
 *   export-events.ts mutate     — Fix webhook + CDC in-place via ALTER TABLE UPDATE
 *   export-events.ts reset      — Truncate events + reset S3Queue for re-processing
 *   export-events.ts all        — Export → mutate → done
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

async function exportWebhooks() {
  const count = await client.query<{ cnt: string }>(`
    SELECT count() as cnt FROM platform.events
    WHERE source IN ('github', 'stripe', 'workos', 'slack', 'linear', 'svix')
  `)
  console.log(`Exporting ${Number(count.data[0].cnt).toLocaleString()} webhook events to R2...`)

  await client.exec(`
    INSERT INTO FUNCTION s3(
      '${r2Base}/events/incoming/backfill/webhooks.jsonl.gz',
      '${r2Ak}', '${r2Sk}',
      'JSONEachRow',
      'gz'
    )
    SELECT
      id, ray,
      multiIf(
        source = 'github',
          concat('github.com/',
            coalesce(
              nullIf(JSONExtractString(toString(data), 'payload', 'organization', 'login'), ''),
              nullIf(JSONExtractString(toString(data), 'payload', 'repository', 'owner', 'login'), ''),
              'unknown'
            )
          ),
        source = 'stripe',
          concat('stripe.com/',
            coalesce(nullIf(JSONExtractString(toString(data), 'payload', 'account'), ''), 'unknown')
          ),
        source = 'workos', 'workos',
        source
      ) AS ns,
      domain,
      replaceOne(event, 'webhook.', '') AS type,
      replaceOne(event, 'webhook.', '') AS event,
      url, source,
      toString(actor) AS actor,
      toString(data) AS data,
      toString(meta) AS meta
    FROM platform.events
    WHERE source IN ('github', 'stripe', 'workos', 'slack', 'linear', 'svix')
  `)
  console.log('  → events/incoming/backfill/webhooks.jsonl.gz')
}

async function exportCDC() {
  const count = await client.query<{ cnt: string }>(`
    SELECT count() as cnt FROM platform.events WHERE type = 'cdc'
  `)
  console.log(`Exporting ${Number(count.data[0].cnt).toLocaleString()} CDC events to R2...`)

  await client.exec(`
    INSERT INTO FUNCTION s3(
      '${r2Base}/events/incoming/backfill/cdc.jsonl.gz',
      '${r2Ak}', '${r2Sk}',
      'JSONEachRow',
      'gz'
    )
    SELECT
      id, ray,
      replaceOne(ns, 'https://', '') AS ns,
      domain, type, event, url, source,
      toString(actor) AS actor,
      toString(data) AS data,
      toString(meta) AS meta
    FROM platform.events
    WHERE type = 'cdc'
  `)
  console.log('  → events/incoming/backfill/cdc.jsonl.gz')
}

async function mutate() {
  // Fix webhook events: ns, type, event
  console.log('Mutating webhook events...')

  // 1. Fix event + type: strip 'webhook.' prefix
  //    webhook.github.push → github.push
  console.log('  fixing type + event (strip webhook. prefix)...')
  await client.exec(`
    ALTER TABLE platform.events UPDATE
      type = replaceOne(event, 'webhook.', ''),
      event = replaceOne(event, 'webhook.', '')
    WHERE source IN ('github', 'stripe', 'workos', 'slack', 'linear', 'svix')
      AND event LIKE 'webhook.%'
  `)

  // 2. Fix github ns: webhook.github → github.com/{owner}
  console.log('  fixing github ns...')
  await client.exec(`
    ALTER TABLE platform.events UPDATE
      ns = concat('github.com/',
        coalesce(
          nullIf(JSONExtractString(toString(data), 'payload', 'organization', 'login'), ''),
          nullIf(JSONExtractString(toString(data), 'payload', 'repository', 'owner', 'login'), ''),
          'unknown'
        )
      )
    WHERE source = 'github'
      AND (ns = 'webhook.github' OR ns = 'github')
  `)

  // 3. Fix workos ns: webhook.workos → workos
  console.log('  fixing workos ns...')
  await client.exec(`
    ALTER TABLE platform.events UPDATE ns = 'workos'
    WHERE source = 'workos' AND ns = 'webhook.workos'
  `)

  // 4. Fix CDC ns: strip https:// prefix
  console.log('Mutating CDC events (strip https:// from ns)...')
  await client.exec(`
    ALTER TABLE platform.events UPDATE
      ns = replaceOne(ns, 'https://', '')
    WHERE type = 'cdc' AND ns LIKE 'https://%'
  `)

  console.log('\nMutations submitted. Check progress:')
  console.log('  ch "SELECT * FROM system.mutations WHERE is_done = 0"')
}

async function reset() {
  console.log('Truncating platform.events...')
  await client.exec('TRUNCATE TABLE platform.events')
  console.log('  Truncated')

  console.log('Dropping S3Queue + MV...')
  await client.exec('DROP VIEW IF EXISTS streams.ingest')
  console.log('  streams.ingest dropped')
  await client.exec('DROP TABLE IF EXISTS streams.queue')
  console.log('  streams.queue dropped')

  console.log('Recreating S3Queue with new keeper_path (v8)...')
  await client.exec(`
    CREATE TABLE IF NOT EXISTS streams.queue (
      value String
    ) ENGINE = S3Queue('${queueUrl}', '${r2Ak}', '${r2Sk}', 'JSONEachRow')
    SETTINGS
      mode = 'unordered',
      keeper_path = '/clickhouse/s3queue/platform_queue_v8',
      s3queue_polling_min_timeout_ms = 500,
      s3queue_polling_max_timeout_ms = 2000,
      s3queue_processing_threads_num = 4,
      s3queue_loading_retries = 3
  `)
  console.log('  streams.queue created (v8)')

  console.log('  waiting 5s for S3Queue initialization...')
  await new Promise((r) => setTimeout(r, 5000))

  console.log('\nReset complete. Now recreate the ingest MV:')
  console.log('  npx tsx scripts/setup-s3queue.ts create')
}

if (step === 'export') {
  await exportWebhooks()
  await exportCDC()
  console.log('\nExport complete.')
} else if (step === 'mutate') {
  await mutate()
} else if (step === 'reset') {
  await reset()
} else if (step === 'all') {
  await exportWebhooks()
  await exportCDC()
  await mutate()
  console.log('\nDone. Mutations are async — check system.mutations for progress.')
} else {
  console.log('Usage: export-events.ts [export|mutate|reset|all]')
  console.log('  export  — Export corrected webhook + CDC to R2 (archive)')
  console.log('  mutate  — Fix webhook + CDC in-place (ALTER TABLE UPDATE)')
  console.log('  reset   — Truncate events + reset S3Queue')
  console.log('  all     — Export + mutate')
}
