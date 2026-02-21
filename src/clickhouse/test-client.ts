import { ClickHouseClient } from './client'

export function createTestClient(database?: string): ClickHouseClient {
  const endpoint = process.env.CLICKHOUSE_URL
  const user = process.env.CLICKHOUSE_USER
  const password = process.env.CLICKHOUSE_PASSWORD

  if (!endpoint || !password) {
    throw new Error('Missing CLICKHOUSE_URL or CLICKHOUSE_PASSWORD env vars. Copy .env.example to .env and fill in credentials.')
  }

  return new ClickHouseClient({ endpoint, user, password, database })
}
