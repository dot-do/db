/**
 * DatabaseDO — Durable Object for headless.ly database coordination
 *
 * Extends ParqueDBDO (the core event-sourced DB engine) and adds:
 * - CDC event logging to a separate events table (for events.do forwarding)
 * - Write mutex (serializes mutations)
 * - Multi-tenancy (namespace/systemSlug from request headers)
 * - fetch() routing for REST, capnweb RPC, and batch RPC protocols
 * - Event compaction via alarm()
 *
 * Architecture:
 * - ParqueDBDO: event-sourced entity storage (entities table + events_wal)
 * - DatabaseDO: CDC event log + multi-tenancy + protocol routing
 *
 * One DO per system per org (e.g. "org_123:crm", "org_123:finance").
 */

import { ParqueDBDO, type CacheInvalidationSignal } from '../parquedb/dist/worker/ParqueDBDO'
import { DB, DOSqliteBackend } from '../parquedb/dist/index'
import { validateEntityType } from './lib/entity-types'
import { DEFAULT_RETENTION_DAYS, MIN_EVENTS_PER_ENTITY, COMPACTION_ALARM_INTERVAL_MS, buildCompactionQuery, computeCompactionCutoff, shouldCompact } from './lib/compaction'
import type { DOContext, ParqueDBCollection } from './handlers/handler-context'
import { EventLogger, queryEventsFromWal } from './handlers/event-logger'
import type { EventsBinding } from './handlers/event-forwarding'

// Re-export types used by consumers
export type { DOContext, ParqueDBCollection } from './handlers/handler-context'
export type { EventQueryOptions } from './handlers/handler-context'
export type { SyncEvent, Entity, SyncRequest, SyncConflict } from './handlers/types'
export type { EventsBinding, DurableEvent } from './handlers/event-forwarding'
export type { EventLoggerDeps } from './handlers/event-logger'
export { EventLogger, queryEventsFromWal } from './handlers/event-logger'
export { validateEntityType, isValidEntityType, toEntityType, DANGEROUS_TYPE_NAMES } from './lib/entity-types'
export { generateEntityId } from './lib/id'
export { DEFAULT_RETENTION_DAYS, MIN_EVENTS_PER_ENTITY, COMPACTION_ALARM_INTERVAL_MS, buildCompactionQuery, computeCompactionCutoff, shouldCompact, COMPACTION_WRITE_INTERVAL, COMPACTION_COOLDOWN_MS } from './lib/compaction'
export { reconstructFromEvents, type EventRow, type ReconstructionResult } from './lib/time-travel'
export { purgeExpiredEntities, previewPurge, DEFAULT_PURGE_RETENTION_DAYS, type PurgeContext, type PurgeOptions, type PurgeResult, type PreviewResult } from './lib/purge-policy'
export { buildDurableEvent, forwardEvents, mapOperationToEventType, OPERATION_TYPE_MAP } from './handlers/event-forwarding'
export type { CacheInvalidationSignal }

// =========================================================================
// SqlStorage Adapter
// =========================================================================

/**
 * Adapts CF Workers SqlStorage (exec-based) to DOSqliteBackend's expected
 * interface (prepare/bind/first/all/run).
 */
function adaptSqlStorage(sql: SqlStorage) {
  return {
    exec(query: string): void {
      sql.exec(query)
    },
    prepare(query: string) {
      return {
        bind(...params: unknown[]) {
          return {
            first<T = Record<string, unknown>>(): T | null {
              const rows = sql.exec(query, ...params).toArray()
              return rows.length > 0 ? (rows[0] as T) : null
            },
            all<T = Record<string, unknown>>(): { results: T[] } {
              return { results: sql.exec(query, ...params).toArray() as T[] }
            },
            run(): { changes: number } {
              const cursor = sql.exec(query, ...params)
              return { changes: cursor.rowsWritten }
            },
          }
        },
      }
    },
  }
}

// =========================================================================
// Entity Normalization Helpers
// =========================================================================

/** Strip R2 path prefix from $id (e.g. "namespace/Contact_abc" -> "Contact_abc") */
function normalizeEntity<T extends Record<string, unknown>>(entity: T): T {
  if (entity.$id && typeof entity.$id === 'string' && entity.$id.includes('/')) {
    return { ...entity, $id: entity.$id.split('/').pop() }
  }
  return entity
}

function normalizeEntities<T extends Record<string, unknown>>(entities: T[]): T[] {
  return entities.map((e) => normalizeEntity(e))
}

// =========================================================================
// Response Helpers
// =========================================================================

function jsonResponse(data: unknown, status = 200, extraHeaders?: Record<string, string>): Response {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...extraHeaders,
  }
  return new Response(JSON.stringify(data), { status, headers })
}

async function parseJsonBody<T>(request: Request): Promise<T | Response> {
  try {
    return (await request.json()) as T
  } catch {
    return jsonResponse({ success: false, error: 'Invalid JSON in request body' }, 400)
  }
}

// =========================================================================
// DatabaseDO Env Extension
// =========================================================================

/**
 * Env bindings that DatabaseDO expects beyond ParqueDBDO's base Env.
 * Workers that deploy DatabaseDO should include these in their wrangler.jsonc.
 */
export interface DatabaseDOEnv {
  /** Events service binding for CDC event forwarding */
  EVENTS?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }
  /** R2 bucket for event archival during compaction */
  DB_BUCKET?: { put(key: string, body: string): Promise<unknown> }
}

// =========================================================================
// DatabaseDO
// =========================================================================

export class DatabaseDO extends ParqueDBDO {
  /** ParqueDB instance via DB() factory with DOSqliteBackend */
  protected db: ReturnType<typeof DB> | null = null

  /** Tenant namespace */
  protected _namespace: string | null = null

  /** System slug (e.g. 'crm', 'finance') */
  protected _systemSlug: string | null = null

  /** CDC event logger (writes to events table, forwards to events.do) */
  protected eventLogger!: EventLogger

  /** Write mutex — serializes mutation operations */
  protected _writeTail: Promise<void> = Promise.resolve()

  constructor(state: DurableObjectState, env: Record<string, unknown>) {
    super(state, env as never)
    this.initCDCTables()

    this.eventLogger = new EventLogger({
      sql: this.sql,
      eventsBinding: (env as DatabaseDOEnv).EVENTS as EventsBinding | undefined,
      waitUntil: (p) => this.ctx.waitUntil(p),
      doIdentity: { id: this.ctx.id.toString(), class: 'DatabaseDO' },
      onCompactionThreshold: () => this.maybeCompact(),
    })

    // Restore namespace + system from SQLite metadata
    const stored = this.sql.exec('SELECT key, value FROM metadata WHERE key IN (?, ?)', 'namespace', 'system').toArray()
    for (const row of stored) {
      if (row.key === 'namespace') {
        this._namespace = row.value as string
      } else if (row.key === 'system') {
        this._systemSlug = row.value as string
      }
    }
    if (this._namespace) {
      this.initDB(this._namespace)
    }

    // Schedule the first compaction alarm if none is set
    this.ctx.storage.getAlarm().then((alarm) => {
      if (!alarm) {
        this.ctx.storage.setAlarm(Date.now() + COMPACTION_ALARM_INTERVAL_MS)
      }
    })
  }

  // =========================================================================
  // Write Lock
  // =========================================================================

  protected withWriteLock<T>(fn: () => Promise<T>): Promise<T> {
    const result = this._writeTail.then(fn, fn)
    this._writeTail = result.then(
      () => {},
      () => {},
    )
    return result
  }

  // =========================================================================
  // DB Initialization
  // =========================================================================

  protected initDB(namespace: string): void {
    const adapted = adaptSqlStorage(this.sql)
    const storage = new DOSqliteBackend(adapted as unknown as ConstructorParameters<typeof DOSqliteBackend>[0], { prefix: `${namespace}/` })
    this.db = DB({ schema: 'flexible' }, { storage })
  }

  protected ensureDB(request: Request): ReturnType<typeof DB> {
    const ns = request.headers.get('X-Namespace')
    const system = request.headers.get('X-System')

    if (ns && !this._namespace) {
      this._namespace = ns
      this.sql.exec('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 'namespace', ns)
      this.initDB(ns)
    }
    if (system && !this._systemSlug) {
      this._systemSlug = system
      this.sql.exec('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 'system', system)
    }
    if (!this.db) {
      const fallback = this._namespace ?? this.ctx.id.toString()
      this.initDB(fallback)
    }
    return this.db!
  }

  protected initCDCTables(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        operation TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        data TEXT,
        checksum TEXT
      )
    `)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)`)
  }

  // =========================================================================
  // DOContext Implementation
  // =========================================================================

  protected getCollection(type: string): ParqueDBCollection {
    validateEntityType(type)
    return (this.db as unknown as Record<string, ParqueDBCollection>)[type]
  }

  /**
   * Build the DOContext that extracted handler modules use.
   */
  protected getContext(): DOContext {
    return {
      sql: this.sql,
      getCollection: (type: string) => this.getCollection(type),
      normalizeEntity,
      normalizeEntities,
      safeLogEvent: (op, type, entityId, data?, prev?, timestamp?) => this.eventLogger.safeLogEvent(op, type, entityId, data, prev, timestamp),
      beginEventBatch: () => this.eventLogger.beginEventBatch(),
      flushEventBatch: () => this.eventLogger.flushEventBatch(),
      queryEventsFromWal: (options) => queryEventsFromWal(this.sql, options),
      jsonResponse,
      parseJsonBody,
      validateEntityType: (type: string) => validateEntityType(type),
      withWriteLock: <T>(fn: () => Promise<T>) => this.withWriteLock(fn),
      namespace: this._namespace,
      systemSlug: this._systemSlug,
    }
  }

  // =========================================================================
  // Request Routing
  // =========================================================================

  /**
   * Main fetch handler. Override in subclasses to add additional routes
   * (e.g., capnweb RPC, batch RPC, custom endpoints).
   *
   * By default, handles basic REST entity operations.
   * Override onRestRequest() to customize REST routing.
   */
  async fetch(request: Request): Promise<Response> {
    this.ensureDB(request)

    const url = new URL(request.url)
    const path = url.pathname
    const method = request.method

    // Allow subclasses to intercept before default routing
    const intercepted = await this.onRequest(request, path, method, url)
    if (intercepted) return intercepted

    return jsonResponse({ error: 'Not found' }, 404)
  }

  /**
   * Override point for request routing.
   * Return a Response to handle the request, or null to fall through to 404.
   */
  protected async onRequest(_request: Request, _path: string, _method: string, _url: URL): Promise<Response | null> {
    return null
  }

  // =========================================================================
  // Event Compaction
  // =========================================================================

  async alarm(): Promise<void> {
    // Call parent alarm for ParqueDBDO flush operations
    try {
      await super.alarm()
    } catch {
      // Parent alarm may not be implemented
    }

    // CDC event compaction
    try {
      const result = await this.compactEvents()
      if (result.deleted > 0) {
        console.log(`[DatabaseDO] Alarm compaction: deleted ${result.deleted} events`)
      }
    } catch (err) {
      console.warn('[DatabaseDO] Alarm compaction failed:', err)
    }

    this.ctx.storage.setAlarm(Date.now() + COMPACTION_ALARM_INTERVAL_MS)
  }

  async compactEvents(retentionDays: number = DEFAULT_RETENTION_DAYS): Promise<{ deleted: number; compactedAt: string }> {
    const compactedAt = new Date().toISOString()

    const lastRow = this.sql.exec("SELECT value FROM metadata WHERE key = 'last_compaction'").toArray()
    const lastTimestamp = lastRow.length > 0 ? (lastRow[0].value as string) : null
    if (!shouldCompact(lastTimestamp)) {
      return { deleted: 0, compactedAt }
    }

    const cutoff = computeCompactionCutoff(retentionDays)

    // Archive events to R2 before deleting them
    try {
      const eventsToDelete = this.sql
        .exec(
          `SELECT * FROM events WHERE timestamp < ? AND id NOT IN (
            SELECT id FROM events WHERE entity_type || ':' || entity_id IN (
              SELECT entity_type || ':' || entity_id FROM events GROUP BY entity_type, entity_id
            ) ORDER BY timestamp DESC LIMIT ?
          )`,
          cutoff,
          MIN_EVENTS_PER_ENTITY,
        )
        .toArray()

      const dbBucket = (this.env as unknown as DatabaseDOEnv).DB_BUCKET
      if (eventsToDelete.length > 0 && dbBucket) {
        const archiveKey = `${this._namespace ?? 'default'}/archive/events-${compactedAt.replace(/[:.]/g, '-')}.ndjson`
        const ndjson = eventsToDelete.map((row) => JSON.stringify(row)).join('\n')
        await dbBucket.put(archiveKey, ndjson)
      }
    } catch (err) {
      console.warn('[DatabaseDO] Event archival failed (proceeding with compaction):', err)
    }

    const deleteQuery = buildCompactionQuery()
    this.sql.exec(deleteQuery, MIN_EVENTS_PER_ENTITY, cutoff)

    const changesRow = this.sql.exec('SELECT changes() as cnt').toArray()
    const deleted = (changesRow[0]?.cnt as number) ?? 0

    this.sql.exec("INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_compaction', ?)", compactedAt)

    return { deleted, compactedAt }
  }

  protected maybeCompact(): void {
    this.compactEvents().catch((err) => {
      console.warn('[DatabaseDO] Write-triggered compaction failed:', err)
    })
  }
}
