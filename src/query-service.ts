import { WorkerEntrypoint } from 'cloudflare:workers'
import { ClickHouseClient } from './clickhouse/client'
import type { ExecResult } from './clickhouse/types'

export interface QueryServiceEnv {
  CLICKHOUSE_URL: string
  CLICKHOUSE_PASSWORD: string
  DATABASE: DurableObjectNamespace
}

export class QueryService extends WorkerEntrypoint<QueryServiceEnv> {
  private getClient(database?: string): ClickHouseClient {
    return new ClickHouseClient({
      endpoint: this.env.CLICKHOUSE_URL,
      user: 'default',
      password: this.env.CLICKHOUSE_PASSWORD,
      database,
    })
  }

  async query<T = Record<string, unknown>>(sql: string, params?: Record<string, string | number>, database?: string): Promise<T[]> {
    const client = this.getClient(database)
    const result = await client.query<T>(sql, params)
    return result.data
  }

  async insert(database: string, table: string, rows: Array<Record<string, unknown>>): Promise<void> {
    if (rows.length === 0) return

    const ndjson = rows.map((r) => JSON.stringify(r)).join('\n')

    const url = new URL(this.env.CLICKHOUSE_URL)
    url.searchParams.set('database', database)
    url.searchParams.set('query', `INSERT INTO ${table} FORMAT JSONEachRow`)

    const resp = await fetch(url.toString(), {
      method: 'POST',
      body: ndjson,
      headers: {
        'Content-Type': 'text/plain',
        'X-ClickHouse-User': 'default',
        'X-ClickHouse-Key': this.env.CLICKHOUSE_PASSWORD,
      },
      signal: AbortSignal.timeout(10000),
    })

    if (!resp.ok) {
      const text = await resp.text()
      throw new Error(`ClickHouse insert error (${resp.status}): ${text.slice(0, 500)}`)
    }
  }

  async exec(sql: string, database?: string): Promise<ExecResult> {
    const client = this.getClient(database)
    return client.exec(sql, undefined, database)
  }

  async ping(): Promise<boolean> {
    const client = this.getClient()
    return client.ping()
  }
}
