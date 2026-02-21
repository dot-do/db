import type { ClickHouseConfig, ExecResult, QueryResult } from './types'

export class ClickHouseClient {
  private config: ClickHouseConfig

  constructor(config: ClickHouseConfig) {
    this.config = config
  }

  /** Execute a SQL query and return parsed JSON results. */
  async query<T = Record<string, unknown>>(sql: string, params?: Record<string, string | number>): Promise<QueryResult<T>> {
    const url = new URL(this.config.endpoint)
    url.searchParams.set('default_format', 'JSON')

    if (this.config.database) {
      url.searchParams.set('database', this.config.database)
    }

    if (params) {
      for (const [k, v] of Object.entries(params)) {
        url.searchParams.set(`param_${k}`, String(v))
      }
    }

    const headers: Record<string, string> = { 'Content-Type': 'text/plain' }
    if (this.config.user) headers['X-ClickHouse-User'] = this.config.user
    if (this.config.password) headers['X-ClickHouse-Key'] = this.config.password

    const resp = await fetch(url.toString(), { method: 'POST', body: sql, headers })

    if (!resp.ok) {
      const text = await resp.text()
      throw new Error(`ClickHouse error (${resp.status}): ${text.slice(0, 500)}`)
    }

    return resp.json() as Promise<QueryResult<T>>
  }

  /** Execute DDL/DML statement. No JSON response expected. */
  async exec(sql: string, params?: Record<string, string | number>, database?: string): Promise<ExecResult> {
    const url = new URL(this.config.endpoint)
    url.searchParams.set('default_format', 'TabSeparated')

    const db = database ?? this.config.database
    if (db) {
      url.searchParams.set('database', db)
    }

    if (params) {
      for (const [k, v] of Object.entries(params)) {
        url.searchParams.set(`param_${k}`, String(v))
      }
    }

    const headers: Record<string, string> = { 'Content-Type': 'text/plain' }
    if (this.config.user) headers['X-ClickHouse-User'] = this.config.user
    if (this.config.password) headers['X-ClickHouse-Key'] = this.config.password

    const resp = await fetch(url.toString(), { method: 'POST', body: sql, headers })

    if (!resp.ok) {
      const text = await resp.text()
      throw new Error(`ClickHouse error (${resp.status}): ${text.slice(0, 500)}`)
    }

    const text = await resp.text()
    return { ok: true, text }
  }

  /** Health check — returns true if the server responds. */
  async ping(): Promise<boolean> {
    try {
      await this.query('SELECT 1')
      return true
    } catch {
      return false
    }
  }
}
