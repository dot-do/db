#!/usr/bin/env npx tsx
import { config } from 'dotenv'
import { resolve } from 'path'
import { ClickHouseClient } from '../src/clickhouse/client'

// Load credentials from .env (never printed, never logged)
config({ path: resolve(import.meta.dirname, '../.env'), quiet: true })

const endpoint = process.env.CLICKHOUSE_URL
const user = process.env.CLICKHOUSE_USER ?? 'default'
const password = process.env.CLICKHOUSE_PASSWORD

if (!endpoint || !password) {
  console.error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD in .do/db/.env')
  process.exit(1)
}

const sql = process.argv[2]
if (!sql) {
  console.error('Usage: ch "SELECT 1"')
  process.exit(1)
}

const database = process.argv[3]
const client = new ClickHouseClient({ endpoint, user, password, database })

const isDDL = /^\s*(CREATE|DROP|ALTER|INSERT|TRUNCATE|RENAME|OPTIMIZE)/i.test(sql)

if (isDDL) {
  const result = await client.exec(sql)
  if (result.text.trim()) console.log(result.text.trim())
} else {
  const result = await client.query(sql)
  console.log(JSON.stringify(result.data, null, 2))
  if (result.statistics) {
    console.error(`${result.rows} rows, ${result.statistics.elapsed.toFixed(3)}s, ${(result.statistics.bytes_read / 1024).toFixed(1)}KB read`)
  }
}
