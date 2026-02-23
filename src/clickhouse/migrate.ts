import type { ClickHouseClient } from './client'

export interface Migration {
  version: number
  name: string
  up: string
  down: string
}

export interface MigrateResult {
  applied: number[]
  skipped: number[]
  errors: { version: number; error: string }[]
}

export interface MigrationStatus {
  applied: number[]
  latestVersion: number
}

async function ensureMigrationsTable(client: ClickHouseClient, database: string): Promise<void> {
  await client.exec(
    `CREATE TABLE IF NOT EXISTS ${database}.migrations (
      version UInt64,
      name String,
      applied_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree()
    ORDER BY version`,
  )
}

async function getAppliedVersions(client: ClickHouseClient, database: string): Promise<number[]> {
  const result = await client.query<{ version: string }>(`SELECT version FROM ${database}.migrations FINAL ORDER BY version`)
  return result.data.map((row) => Number(row.version))
}

function splitStatements(sql: string): string[] {
  return sql
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
}

function replacePlaceholders(sql: string, database: string): string {
  let result = sql.replaceAll('{database}', database)
  result = result.replace(/\{env\.([A-Za-z_][A-Za-z0-9_]*)\}/g, (_, name) => {
    const val = process.env[name]
    if (!val) throw new Error(`Missing env var: ${name}`)
    return val
  })
  return result
}

export async function migrate(client: ClickHouseClient, database: string, migrations: Migration[]): Promise<MigrateResult> {
  await ensureMigrationsTable(client, database)

  const applied = await getAppliedVersions(client, database)
  const sorted = [...migrations].sort((a, b) => a.version - b.version)

  const result: MigrateResult = {
    applied: [],
    skipped: [],
    errors: [],
  }

  for (const migration of sorted) {
    if (applied.includes(migration.version)) {
      result.skipped.push(migration.version)
      continue
    }

    try {
      const sql = replacePlaceholders(migration.up, database)
      const statements = splitStatements(sql)

      for (const stmt of statements) {
        await client.exec(stmt)
      }

      await client.exec(`INSERT INTO ${database}.migrations (version, name) VALUES (${migration.version}, '${migration.name}')`)

      result.applied.push(migration.version)
    } catch (err) {
      result.errors.push({
        version: migration.version,
        error: err instanceof Error ? err.message : String(err),
      })
      break
    }
  }

  return result
}

export async function getMigrationStatus(client: ClickHouseClient, database: string): Promise<MigrationStatus> {
  await ensureMigrationsTable(client, database)

  const applied = await getAppliedVersions(client, database)

  return {
    applied,
    latestVersion: applied.length > 0 ? Math.max(...applied) : 0,
  }
}
