export interface ClickHouseConfig {
  /** ClickHouse HTTP endpoint (e.g., 'https://host:8443') */
  endpoint: string
  /** ClickHouse user (sent as X-ClickHouse-User header) */
  user?: string
  /** ClickHouse password (sent as X-ClickHouse-Key header) */
  password?: string
  /** Default database for all queries */
  database?: string
}

export interface QueryResult<T = Record<string, unknown>> {
  data: T[]
  rows: number
  statistics?: {
    elapsed: number
    rows_read: number
    bytes_read: number
  }
}

export interface ExecResult {
  ok: boolean
  text: string
}
