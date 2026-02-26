#!/usr/bin/env npx tsx
/**
 * Backfill missing events from events_legacy into events.
 * Targets the specific ID ranges that failed during the v2 migration.
 * ReplacingMergeTree deduplicates any overlapping rows.
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

// Failed chunk ranges from the original migration run
// These ranges failed due to OOM or events_v2 table rename
const failedRanges: [string, string][] = [
  // Chunk 10: OOM'd on both original and retry
  ['01KJ7ZAZVV', '01KJ8E1JT1'],
  // Chunks 15, 17, 19, 20: failed after RENAME (events_v2 didn't exist)
  ['01KJ91SM4F', '01KJ9468WJ'],
  ['01KJ9NBEGJ', '01KJ9XYMSP'],
  ['01KJA0RX2W', '01KJAA0Y02'],
  ['01KJAA0Y02', '01KJAVWCFZ'],
]

console.log(`Backfilling ${failedRanges.length} failed ranges from events_legacy → events`)

// First, sub-chunk the large OOM range
const SUB_CHUNK_SIZE = 4
let errors = 0

for (const [lower, upper] of failedRanges) {
  // Get sub-chunk boundaries within this range
  console.log(`\nRange ${lower}..${upper}:`)
  const countResult = query<{ cnt: string }>(`
    SELECT count() as cnt FROM platform.events_legacy
    WHERE id >= '${lower}' AND id < '${upper}'
  `)
  const cnt = parseInt(countResult[0]?.cnt || '0')
  console.log(`  ${cnt} rows in events_legacy for this range`)

  if (cnt === 0) {
    console.log('  Skipping (no rows)')
    continue
  }

  if (cnt < 500000) {
    // Small enough to do in one shot
    const sql = `INSERT INTO platform.events
SELECT id, ray, ns, '' AS domain, type, event, url, source, actor, data, meta, file, ingested
FROM platform.events_legacy
WHERE id >= '${lower}' AND id < '${upper}'
SETTINGS max_execution_time=0, max_memory_usage=15000000000`
    const ok = exec(sql, `${lower.slice(0, 10)}..${upper.slice(0, 10)}`)
    if (!ok) errors++
  } else {
    // Sub-chunk for larger ranges
    const boundaries = query<{ boundary: string }>(`
      SELECT id as boundary FROM (
        SELECT id, rowNumberInAllBlocks() as rn
        FROM platform.events_legacy
        WHERE id >= '${lower}' AND id < '${upper}'
        ORDER BY id
      )
      WHERE rn % intDiv(${cnt}, ${SUB_CHUNK_SIZE}) = 0
      AND rn > 0
      ORDER BY id
      SETTINGS max_execution_time=120
    `)
    const ids = boundaries.map(b => b.boundary)
    console.log(`  Split into ${ids.length + 1} sub-chunks`)

    const subRanges: [string, string][] = []
    subRanges.push([lower, ids[0] || upper])
    for (let i = 0; i < ids.length; i++) {
      subRanges.push([ids[i], ids[i + 1] || upper])
    }

    for (const [sl, su] of subRanges) {
      const sql = `INSERT INTO platform.events
SELECT id, ray, ns, '' AS domain, type, event, url, source, actor, data, meta, file, ingested
FROM platform.events_legacy
WHERE id >= '${sl}' AND id < '${su}'
SETTINGS max_execution_time=0, max_memory_usage=15000000000`
      const ok = exec(sql, `sub ${sl.slice(0, 10)}..${su.slice(0, 10)}`)
      if (!ok) errors++
    }
  }
}

// Final check
console.log('\n--- Final check ---')
const evCount = query<{ cnt: string }>('SELECT count() as cnt FROM platform.events')
const legCount = query<{ cnt: string }>('SELECT count() as cnt FROM platform.events_legacy')
console.log(`events: ${evCount[0]?.cnt} rows, events_legacy: ${legCount[0]?.cnt} rows`)
console.log(`Errors: ${errors}`)
