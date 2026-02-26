#!/usr/bin/env npx tsx
/**
 * Chunked INSERT INTO events_v2 — splits the ID range into chunks
 * and inserts missing rows per chunk. Avoids OOM and timeout issues.
 */
import 'dotenv/config'
import { execFileSync } from 'child_process'

const url = process.env.CLICKHOUSE_URL
const pass = process.env.CLICKHOUSE_PASSWORD
const user = process.env.CLICKHOUSE_USER || 'default'

if (!url || !pass) {
  console.error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD')
  process.exit(1)
}

const reqUrl = new URL(url)
reqUrl.searchParams.set('database', 'platform')
reqUrl.searchParams.set('default_format', 'JSON')

function query<T = any>(sql: string): T[] {
  const result = execFileSync('curl', [
    '-sS', '--max-time', '120',
    reqUrl.toString(),
    '-H', `X-ClickHouse-User: ${user}`,
    '-H', `X-ClickHouse-Key: ${pass}`,
    '-H', 'Content-Type: text/plain',
    '--data-binary', '@-',
  ], { input: sql, maxBuffer: 50 * 1024 * 1024 })
  const parsed = JSON.parse(result.toString())
  return parsed.data
}

function exec(sql: string, label: string): boolean {
  const tsUrl = new URL(reqUrl.toString())
  tsUrl.searchParams.set('default_format', 'TabSeparated')
  const start = Date.now()
  try {
    const result = execFileSync('curl', [
      '-sS', '--max-time', '3600',
      tsUrl.toString(),
      '-H', `X-ClickHouse-User: ${user}`,
      '-H', `X-ClickHouse-Key: ${pass}`,
      '-H', 'Content-Type: text/plain',
      '--data-binary', '@-',
    ], { input: sql, timeout: 3_600_000, maxBuffer: 10 * 1024 * 1024 })
    const elapsed = ((Date.now() - start) / 1000).toFixed(1)
    const output = result.toString().trim()
    if (output && output.startsWith('Code:')) {
      console.error(`  [${label}] ERROR in ${elapsed}s: ${output.slice(0, 200)}`)
      return false
    }
    console.log(`  [${label}] SUCCESS in ${elapsed}s`)
    return true
  } catch (err: any) {
    const elapsed = ((Date.now() - start) / 1000).toFixed(1)
    const stderr = err.stderr?.toString?.() || ''
    const stdout = err.stdout?.toString?.() || ''
    console.error(`  [${label}] ERROR in ${elapsed}s:`, (stderr || stdout || err.message).slice(0, 300))
    return false
  }
}

// Get ID range boundaries by splitting the events table into ~20 chunks
console.log('Fetching chunk boundaries...')
const boundaries = query<{ boundary: string }>(`
  SELECT id as boundary FROM (
    SELECT id, rowNumberInAllBlocks() as rn, count() OVER () as total
    FROM platform.events
    ORDER BY id
  )
  WHERE rn % intDiv(total, 20) = 0
  ORDER BY id
  SETTINGS max_execution_time=120
`)

const ids = boundaries.map(b => b.boundary)
console.log(`Got ${ids.length} boundary points, creating ${ids.length + 1} chunks`)

// Insert chunk by chunk
let totalInserted = 0
let errors = 0

for (let i = 0; i <= ids.length; i++) {
  const lower = i === 0 ? '' : ids[i - 1]
  const upper = i === ids.length ? '' : ids[i]

  let whereClause: string
  let label: string
  if (!lower) {
    whereClause = `id < '${upper}'`
    label = `chunk ${i + 1}/${ids.length + 1} (start..${upper?.slice(0, 10)})`
  } else if (!upper) {
    whereClause = `id >= '${lower}'`
    label = `chunk ${i + 1}/${ids.length + 1} (${lower?.slice(0, 10)}..end)`
  } else {
    whereClause = `id >= '${lower}' AND id < '${upper}'`
    label = `chunk ${i + 1}/${ids.length + 1} (${lower?.slice(0, 10)}..${upper?.slice(0, 10)})`
  }

  const sql = `INSERT INTO platform.events_v2
SELECT id, ray, ns, '' AS domain, type, event, url, source, actor, data, meta, file, ingested
FROM platform.events
WHERE ${whereClause}
SETTINGS max_execution_time=0, max_memory_usage=15000000000`

  console.log(`[${label}]`)
  const ok = exec(sql, label)
  if (!ok) errors++
}

// Final count check
console.log('\n--- Final check ---')
const v2Count = query<{ cnt: string }>('SELECT count() as cnt FROM platform.events_v2')
const evCount = query<{ cnt: string }>('SELECT count() as cnt FROM platform.events')
console.log(`events_v2: ${v2Count[0]?.cnt} rows, events: ${evCount[0]?.cnt} rows`)
console.log(`Errors: ${errors}`)
