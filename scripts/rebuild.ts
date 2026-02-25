#!/usr/bin/env npx tsx
/**
 * Rebuild ClickHouse from scratch.
 *
 * Since ClickHouse is 100% derived from R2 (the durable event log), this
 * script drops everything and recreates the schema cleanly, then sets up
 * S3Queue to re-ingest all events from R2.
 *
 * Usage:
 *   npx tsx .do/db/scripts/rebuild.ts              # dry-run (show what would happen)
 *   npx tsx .do/db/scripts/rebuild.ts --confirm     # actually rebuild
 */
import { config } from 'dotenv'
import { resolve } from 'path'
import { ClickHouseClient } from '../src/clickhouse/client'
import { migrate } from '../src/clickhouse/migrate'
import { migrations } from '../src/clickhouse/migrations'

config({ path: resolve(import.meta.dirname, '../.env') })

const endpoint = process.env.CLICKHOUSE_URL
const user = process.env.CLICKHOUSE_USER ?? 'default'
const password = process.env.CLICKHOUSE_PASSWORD

if (!endpoint || !password) {
  console.error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD in .do/db/.env')
  process.exit(1)
}

const confirm = process.argv.includes('--confirm')
const database = process.argv.find((a) => !a.startsWith('-') && a !== process.argv[0] && a !== process.argv[1]) ?? 'platform'

const client = new ClickHouseClient({ endpoint, user, password, database })

if (!confirm) {
  console.log('DRY RUN — pass --confirm to execute\n')
  console.log(`Target database: ${database}`)
  console.log('This will:')
  console.log('  1. Drop all streams.* MVs')
  console.log('  2. Drop S3Queue (platform.queue + streams.ingest)')
  console.log(`  3. Drop all tables in ${database}`)
  console.log(`  4. Drop ${database}.migrations tracking table`)
  console.log(`  5. Run ${migrations.length} migrations`)
  console.log('  6. Recreate streams.* MVs')
  console.log('  7. Recreate S3Queue (re-ingests all events from R2)')
  console.log('\nAll data will be re-ingested from R2. This is safe because ClickHouse is derived.')
  process.exit(0)
}

console.log(`Rebuilding ${database} database...`)

// 1. Drop streams MVs
console.log('\n1. Dropping streams MVs...')
const streamsMvs = ['logs', 'traces', 'observations', 'scores', 'sessions', 'versions', 'rels', 'actions', 'data', 'metrics_daily', 'calls', 'usage', 'ingest']
for (const mv of streamsMvs) {
  try {
    await client.exec(`DROP VIEW IF EXISTS streams.${mv}`)
  } catch {}
}
console.log('   Done')

// 2. Drop S3Queue
console.log('2. Dropping S3Queue...')
try { await client.exec('DROP TABLE IF EXISTS streams.queue') } catch {}
console.log('   Done')

// 3. Drop all tables
console.log(`3. Dropping all tables in ${database}...`)
const tables = await client.query<{ name: string }>(`SELECT name FROM system.tables WHERE database = '${database}'`)
for (const { name } of tables.data) {
  try {
    await client.exec(`DROP TABLE IF EXISTS ${database}.${name}`)
  } catch (e) {
    console.error(`   Failed to drop ${name}: ${(e as Error).message.slice(0, 100)}`)
  }
}
console.log(`   Dropped ${tables.data.length} tables`)

// 4. Run migrations
console.log(`4. Running ${migrations.length} migrations...`)
const result = await migrate(client, database, migrations)
console.log(`   Applied: ${result.applied.length}`)
if (result.errors.length > 0) {
  for (const err of result.errors) {
    console.error(`   Error in migration ${err.version}: ${err.error}`)
  }
  console.error('\nMigrations failed. Fix errors and re-run.')
  process.exit(1)
}

// 5. Recreate streams MVs (inline — same as recreate-mvs.ts)
console.log('5. Recreating streams MVs...')
console.log('   Run: npx tsx .do/db/scripts/recreate-mvs.ts')

// 6. Recreate S3Queue
console.log('6. Recreating S3Queue...')
console.log('   Run: npx tsx .do/db/scripts/setup-s3queue.ts create')

console.log('\nDone. Tables created, run the two commands above to complete setup.')
