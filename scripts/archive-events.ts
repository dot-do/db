#!/usr/bin/env npx tsx
/**
 * Archive legacy access logs from R2 events bucket to parquet in R2.
 *
 * Uses ClickHouse s3() table function to read NDJSON from R2 and write
 * directly to parquet in R2 — no intermediate table needed.
 *
 * Batched by month to avoid HTTP timeout on 24 GB scan.
 */
import { config } from 'dotenv'
import { resolve } from 'path'

config({ path: resolve(import.meta.dirname, '../.env') })

const chEndpoint = process.env.CLICKHOUSE_URL
const chUser = process.env.CLICKHOUSE_USER ?? 'default'
const chPassword = process.env.CLICKHOUSE_PASSWORD
const r2Url = process.env.R2_URL
const r2Ak = process.env.R2_ACCESS_KEY_ID
const r2Sk = process.env.R2_SECRET_ACCESS_KEY

if (!chEndpoint || !chPassword) {
  console.error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD')
  process.exit(1)
}
if (!r2Url || !r2Ak || !r2Sk) {
  console.error('Missing R2_URL, R2_ACCESS_KEY_ID, or R2_SECRET_ACCESS_KEY')
  process.exit(1)
}

const r2Base = r2Url.replace(/\/$/, '')
const schema = 'method String, path String, hostname String, status UInt16, latency UInt32, userId String, ip String, userAgent String, referer String, timestamp UInt64'

// Long-running exec with 10 min timeout
async function chExec(sql: string): Promise<string> {
  const url = new URL(chEndpoint!)
  url.searchParams.set('default_format', 'TabSeparated')
  url.searchParams.set('database', 'default')
  url.searchParams.set('wait_end_of_query', '1')
  url.searchParams.set('send_progress_in_http_headers', '1')
  url.searchParams.set('http_headers_progress_interval_ms', '5000')

  const headers: Record<string, string> = { 'Content-Type': 'text/plain' }
  if (chUser) headers['X-ClickHouse-User'] = chUser
  if (chPassword) headers['X-ClickHouse-Key'] = chPassword

  const resp = await fetch(url.toString(), {
    method: 'POST',
    body: sql,
    headers,
    signal: AbortSignal.timeout(600_000), // 10 min
  })

  if (!resp.ok) {
    const text = await resp.text()
    throw new Error(`ClickHouse error (${resp.status}): ${text.slice(0, 500)}`)
  }

  return resp.text()
}

async function chQuery<T>(sql: string): Promise<T[]> {
  const url = new URL(chEndpoint!)
  url.searchParams.set('default_format', 'JSON')
  url.searchParams.set('database', 'default')
  url.searchParams.set('wait_end_of_query', '1')
  url.searchParams.set('send_progress_in_http_headers', '1')
  url.searchParams.set('http_headers_progress_interval_ms', '5000')

  const headers: Record<string, string> = { 'Content-Type': 'text/plain' }
  if (chUser) headers['X-ClickHouse-User'] = chUser
  if (chPassword) headers['X-ClickHouse-Key'] = chPassword

  const resp = await fetch(url.toString(), {
    method: 'POST',
    body: sql,
    headers,
    signal: AbortSignal.timeout(600_000),
  })

  if (!resp.ok) {
    const text = await resp.text()
    throw new Error(`ClickHouse error (${resp.status}): ${text.slice(0, 500)}`)
  }

  const json = await resp.json() as { data: T[] }
  return json.data
}

const step = process.argv[2] || 'help'

// Generate daily date strings from 2025-04-01 to 2026-02-21
function getDays(): string[] {
  const days: string[] = []
  const start = new Date(2025, 3, 1) // April 1, 2025
  const end = new Date(2026, 1, 21) // Feb 21, 2026
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    days.push(d.toISOString().slice(0, 10))
  }
  return days
}

// Group days by month for final parquet output
function groupByMonth(days: string[]): Map<string, string[]> {
  const map = new Map<string, string[]>()
  for (const day of days) {
    const month = day.slice(0, 7) // "2025-04"
    if (!map.has(month)) map.set(month, [])
    map.get(month)!.push(day)
  }
  return map
}

async function run() {
  if (step === 'count') {
    console.log('Counting rows by sampling a single file...')
    const fileCount = await chQuery<{ cnt: string }>(
      `SELECT count() as cnt FROM s3('${r2Base}/events/event_date=2026-02-20/hr=12/*.json.gz', '${r2Ak}', '${r2Sk}', 'JSONEachRow', '${schema}')`
    )
    console.log(`Sample hour (2026-02-20 hr=12): ${fileCount[0].cnt} rows`)
    console.log('Estimated total: ~50-100M rows across 327 days')
    return
  }

  if (step === 'archive') {
    // Export daily, writing per-month parquet files
    // Each day is ~75 MB, manageable within timeout
    const days = getDays()
    const byMonth = groupByMonth(days)
    const skipMonth = process.argv[3] // optional: skip months already done, e.g. "2025-04"

    console.log(`Archiving ${days.length} days across ${byMonth.size} months...`)

    for (const [month, monthDays] of byMonth) {
      if (skipMonth && month <= skipMonth) {
        console.log(`  ${month}: skipping (already archived)`)
        continue
      }

      const dest = `${r2Base}/events-archive/${month}.parquet`
      console.log(`  ${month}: archiving ${monthDays.length} days...`)
      const monthStart = Date.now()
      let monthRows = 0

      // First, create a temp table to accumulate daily data
      const tempTable = `_archive_${month.replace('-', '_')}`
      try {
        await chExec(`DROP TABLE IF EXISTS ${tempTable}`)
        await chExec(`CREATE TABLE ${tempTable} (${schema}) ENGINE = MergeTree() ORDER BY timestamp`)
      } catch (err) {
        console.error(`  ${month}: failed to create temp table: ${err instanceof Error ? err.message : err}`)
        continue
      }

      for (const day of monthDays) {
        const glob = `${r2Base}/events/event_date=${day}/hr=*/*.json.gz`
        const start = Date.now()
        try {
          const result = await chExec(
            `INSERT INTO ${tempTable}
             SELECT * FROM s3('${glob}', '${r2Ak}', '${r2Sk}', 'JSONEachRow', '${schema}')
             SETTINGS s3_max_connections=10`
          )
          const elapsed = ((Date.now() - start) / 1000).toFixed(1)
          process.stdout.write(`.`)
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err)
          if (msg.includes('The specified key does not exist') || msg.includes('no files') || msg.includes('The table structure cannot be extracted')) {
            // no data for this day
          } else {
            console.error(`\n    ${day}: ERROR: ${msg.slice(0, 200)}`)
          }
        }
      }

      // Get row count
      const countResult = await chQuery<{ cnt: string }>(`SELECT count() as cnt FROM ${tempTable}`)
      monthRows = Number(countResult[0].cnt)

      if (monthRows === 0) {
        console.log(`\n  ${month}: no data, skipping`)
        await chExec(`DROP TABLE IF EXISTS ${tempTable}`)
        continue
      }

      // Export temp table to parquet
      // Use large min_part_size to avoid R2 multipart upload issues
      try {
        await chExec(
          `INSERT INTO FUNCTION s3('${dest}', '${r2Ak}', '${r2Sk}', 'Parquet')
           SELECT * FROM ${tempTable}
           SETTINGS s3_min_upload_part_size = 536870912, s3_upload_part_size_multiply_factor = 1`
        )
        const elapsed = ((Date.now() - monthStart) / 1000).toFixed(1)
        console.log(`\n  ${month}: done — ${monthRows.toLocaleString()} rows (${elapsed}s)`)
      } catch (err) {
        console.error(`\n  ${month}: parquet export ERROR: ${err instanceof Error ? err.message : err}`)
      }

      // Cleanup temp table
      await chExec(`DROP TABLE IF EXISTS ${tempTable}`)
    }

    console.log('\nArchive complete. Files in events-archive/ bucket.')
    return
  }

  if (step === 'retry') {
    // Retry exporting months that failed — reads from R2 NDJSON directly to parquet
    // Uses single-part upload to avoid R2 multipart issues
    const months = process.argv.slice(3) // e.g. 2025-05 2025-06 ...
    if (months.length === 0) {
      console.error('Usage: archive-events.ts retry 2025-05 2025-06 ...')
      process.exit(1)
    }

    for (const month of months) {
      const [y, mm] = month.split('-')
      const dest = `${r2Base}/events-archive/${month}.parquet`
      console.log(`Retrying ${month}...`)
      const start = Date.now()

      // Get all days in the month
      const year = Number(y)
      const m = Number(mm)
      const daysInMonth = new Date(year, m, 0).getDate()
      const days: string[] = []
      for (let d = 1; d <= daysInMonth; d++) {
        days.push(`${y}-${mm}-${String(d).padStart(2, '0')}`)
      }

      // Load day by day into temp table
      const tempTable = `_archive_retry_${y}_${mm}`
      await chExec(`DROP TABLE IF EXISTS ${tempTable}`)
      await chExec(`CREATE TABLE ${tempTable} (${schema}) ENGINE = MergeTree() ORDER BY timestamp`)

      for (const day of days) {
        const glob = `${r2Base}/events/event_date=${day}/hr=*/*.json.gz`
        try {
          await chExec(
            `INSERT INTO ${tempTable}
             SELECT * FROM s3('${glob}', '${r2Ak}', '${r2Sk}', 'JSONEachRow', '${schema}')
             SETTINGS s3_max_connections=10`
          )
          process.stdout.write('.')
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err)
          if (!msg.includes('The specified key does not exist') && !msg.includes('no files') && !msg.includes('table structure')) {
            console.error(`\n    ${day}: ERROR: ${msg.slice(0, 200)}`)
          }
        }
      }

      const countResult = await chQuery<{ cnt: string }>(`SELECT count() as cnt FROM ${tempTable}`)
      const rows = Number(countResult[0].cnt)

      if (rows === 0) {
        console.log(`\n  ${month}: no data`)
        await chExec(`DROP TABLE IF EXISTS ${tempTable}`)
        continue
      }

      // Export with large part size to avoid R2 multipart issues
      try {
        await chExec(
          `INSERT INTO FUNCTION s3('${dest}', '${r2Ak}', '${r2Sk}', 'Parquet')
           SELECT * FROM ${tempTable}
           SETTINGS s3_min_upload_part_size = 536870912, s3_upload_part_size_multiply_factor = 1`
        )
        const elapsed = ((Date.now() - start) / 1000).toFixed(1)
        console.log(`\n  ${month}: done — ${rows.toLocaleString()} rows (${elapsed}s)`)
      } catch (err) {
        console.error(`\n  ${month}: parquet export ERROR: ${err instanceof Error ? err.message : err}`)
      }

      await chExec(`DROP TABLE IF EXISTS ${tempTable}`)
    }
    return
  }

  if (step === 'verify') {
    // Count rows in the parquet archive
    console.log('Verifying archive...')
    const result = await chQuery<{ cnt: string; mn: string; mx: string }>(
      `SELECT count() as cnt, min(timestamp) as mn, max(timestamp) as mx
       FROM s3('${r2Base}/events-archive/*.parquet', '${r2Ak}', '${r2Sk}', 'Parquet')`
    )
    console.log(`Archive: ${result[0].cnt} rows`)
    console.log(`  Earliest: ${new Date(Number(result[0].mn)).toISOString()}`)
    console.log(`  Latest: ${new Date(Number(result[0].mx)).toISOString()}`)
    return
  }

  console.log('Usage: archive-events.ts [count|archive|verify]')
  console.log('  count   - Sample row count from one hour')
  console.log('  archive - Export each month from R2 NDJSON → R2 Parquet')
  console.log('  verify  - Count rows in the parquet archive')
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})
