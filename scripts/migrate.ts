#!/usr/bin/env npx tsx
import { config } from 'dotenv'
import { resolve } from 'path'
import { ClickHouseClient } from '../src/clickhouse/client'
import { migrate } from '../src/clickhouse/migrate'
import { migrations } from '../src/clickhouse/migrations'

// Load credentials from .env (never printed, never logged)
config({ path: resolve(import.meta.dirname, '../.env') })

const endpoint = process.env.CLICKHOUSE_URL
const user = process.env.CLICKHOUSE_USER ?? 'default'
const password = process.env.CLICKHOUSE_PASSWORD

if (!endpoint || !password) {
  console.error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD in .do/db/.env')
  process.exit(1)
}

const database = process.argv[2] ?? 'default'
console.log(`Running ${migrations.length} migrations against database: ${database}`)

const client = new ClickHouseClient({ endpoint, user, password, database })

const result = await migrate(client, database, migrations)

console.log(`Applied: ${result.applied.length} (${result.applied.join(', ') || 'none'})`)
console.log(`Skipped: ${result.skipped.length} (${result.skipped.join(', ') || 'none'})`)

if (result.errors.length > 0) {
  console.error('Errors:')
  for (const err of result.errors) {
    console.error(`  Migration ${err.version}: ${err.error}`)
  }
  process.exit(1)
}

console.log('All migrations complete.')
