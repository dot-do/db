/**
 * DB — Durable Object for headless.ly database coordination
 *
 * Standalone Durable Object (no ParqueDB dependency) that provides:
 * - Entity CRUD via raw SqlStorage on an `entities` table
 * - CDC event logging to a separate events table (for events.do forwarding)
 * - Write mutex (serializes mutations)
 * - Multi-tenancy (namespace/systemSlug from request headers)
 * - fetch() routing for REST, capnweb RPC, and batch RPC protocols
 * - Event compaction via alarm()
 *
 * Architecture:
 * - `entities` table: id (PK), type, data (JSON), created_at, updated_at, deleted_at (soft deletes)
 * - `events` table: CDC event log for sync, time-travel, and forwarding
 * - `metadata` table: namespace, system slug, compaction state
 *
 * One DO per system per org (e.g. "org_123:crm", "org_123:finance").
 */

import { DurableObject } from 'cloudflare:workers'
import { validateEntityType } from './lib/entity-types'
import { generateEntityId } from './lib/id'
import {
  DEFAULT_RETENTION_DAYS,
  MIN_EVENTS_PER_ENTITY,
  COMPACTION_ALARM_INTERVAL_MS,
  buildCompactionQuery,
  computeCompactionCutoff,
  shouldCompact,
} from './lib/compaction'
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
export {
  DEFAULT_RETENTION_DAYS,
  MIN_EVENTS_PER_ENTITY,
  COMPACTION_ALARM_INTERVAL_MS,
  buildCompactionQuery,
  computeCompactionCutoff,
  shouldCompact,
  COMPACTION_WRITE_INTERVAL,
  COMPACTION_COOLDOWN_MS,
} from './lib/compaction'
export { reconstructFromEvents, type EventRow, type ReconstructionResult } from './lib/time-travel'
export {
  purgeExpiredEntities,
  previewPurge,
  DEFAULT_PURGE_RETENTION_DAYS,
  type PurgeContext,
  type PurgeOptions,
  type PurgeResult,
  type PreviewResult,
} from './lib/purge-policy'
export { buildDurableEvent, forwardEvents, mapOperationToEventType, OPERATION_TYPE_MAP } from './handlers/event-forwarding'

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
// Filter Matching
// =========================================================================

/**
 * Evaluate a MongoDB-style filter against a data object.
 * Supports: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $regex, direct equality.
 */
function matchesFilter(data: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    const value = data[key]

    if (condition === null || condition === undefined || typeof condition !== 'object' || Array.isArray(condition)) {
      // Direct equality
      if (value !== condition) return false
      continue
    }

    const ops = condition as Record<string, unknown>
    for (const [op, expected] of Object.entries(ops)) {
      switch (op) {
        case '$eq':
          if (value !== expected) return false
          break
        case '$ne':
          if (value === expected) return false
          break
        case '$gt':
          if (typeof value !== 'number' || typeof expected !== 'number' || value <= expected) return false
          break
        case '$gte':
          if (typeof value !== 'number' || typeof expected !== 'number' || value < expected) return false
          break
        case '$lt':
          if (typeof value !== 'number' || typeof expected !== 'number' || value >= expected) return false
          break
        case '$lte':
          if (typeof value !== 'number' || typeof expected !== 'number' || value > expected) return false
          break
        case '$in':
          if (!Array.isArray(expected) || !expected.includes(value)) return false
          break
        case '$nin':
          if (!Array.isArray(expected) || expected.includes(value)) return false
          break
        case '$exists':
          if (expected && value === undefined) return false
          if (!expected && value !== undefined) return false
          break
        case '$regex': {
          if (typeof value !== 'string' || typeof expected !== 'string') return false
          const re = new RegExp(expected)
          if (!re.test(value)) return false
          break
        }
        default:
          // Unknown operator — treat as direct equality on nested field
          if (value !== expected) return false
          break
      }
    }
  }
  return true
}

// =========================================================================
// DBEnv Extension
// =========================================================================

/**
 * Env bindings that DB expects.
 * Workers that deploy DB should include these in their wrangler.jsonc.
 */
export interface DBEnv {
  /** Events service binding for CDC event forwarding */
  EVENTS?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }
  /** R2 bucket for event archival during compaction */
  DB_BUCKET?: { put(key: string, body: string): Promise<unknown> }
}

// Keep backward-compatible alias
export type DatabaseDOEnv = DBEnv

// =========================================================================
// DB Durable Object
// =========================================================================

export class DB extends DurableObject {
  /** Raw SqlStorage handle */
  protected sql: SqlStorage

  /** Tenant namespace */
  protected _namespace: string | null = null

  /** System slug (e.g. 'crm', 'finance') */
  protected _systemSlug: string | null = null

  /** CDC event logger (writes to events table, forwards to events.do) */
  protected eventLogger!: EventLogger

  /** Write mutex — serializes mutation operations */
  protected _writeTail: Promise<void> = Promise.resolve()

  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.initTables()

    this.eventLogger = new EventLogger({
      sql: this.sql,
      eventsBinding: (env as DBEnv).EVENTS as EventsBinding | undefined,
      waitUntil: (p) => this.ctx.waitUntil(p),
      doIdentity: { id: this.ctx.id.toString(), class: 'DB' },
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

    // Schedule the first compaction alarm if none is set
    this.ctx.storage.getAlarm().then((alarm) => {
      if (!alarm) {
        this.ctx.storage.setAlarm(Date.now() + COMPACTION_ALARM_INTERVAL_MS)
      }
    })
  }

  // =========================================================================
  // Table Initialization
  // =========================================================================

  protected initTables(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `)
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS entities (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        data TEXT NOT NULL DEFAULT '{}',
        created_at TEXT,
        updated_at TEXT,
        deleted_at TEXT
      )
    `)
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_entities_type_deleted ON entities(type, deleted_at)')
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
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id)')
    this.sql.exec('CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)')
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
  // Namespace Initialization
  // =========================================================================

  protected ensureNamespace(request: Request): void {
    const ns = request.headers.get('X-Namespace')
    const system = request.headers.get('X-System')

    if (ns && !this._namespace) {
      this._namespace = ns
      this.sql.exec('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 'namespace', ns)
    }
    if (system && !this._systemSlug) {
      this._systemSlug = system
      this.sql.exec('INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)', 'system', system)
    }
  }

  // =========================================================================
  // Collection Interface (ParqueDBCollection-compatible)
  // =========================================================================

  protected getCollection(type: string): ParqueDBCollection {
    validateEntityType(type)
    const sql = this.sql

    return {
      async find(
        filter?: Record<string, unknown>,
        options?: Record<string, unknown>,
      ): Promise<{ items: unknown[]; total?: number; hasMore?: boolean }> {
        const limit = (options?.limit as number) ?? 100
        const offset = (options?.skip as number) ?? (options?.offset as number) ?? 0
        const sort = options?.sort as Record<string, 1 | -1> | undefined

        // Query all non-deleted entities of this type
        const rows = sql.exec('SELECT id, data, created_at, updated_at FROM entities WHERE type = ? AND deleted_at IS NULL', type).toArray()

        // Parse JSON data and attach meta fields
        let items: Record<string, unknown>[] = rows.map((row) => {
          const parsed = row.data ? JSON.parse(row.data as string) : {}
          return {
            ...parsed,
            $id: row.id as string,
            $type: type,
            $createdAt: row.created_at as string,
            $updatedAt: row.updated_at as string,
          }
        })

        // Apply filter in-memory (MongoDB-style)
        if (filter && Object.keys(filter).length > 0) {
          items = items.filter((item) => matchesFilter(item, filter))
        }

        const total = items.length

        // Apply sort
        if (sort) {
          const sortEntries = Object.entries(sort)
          items.sort((a, b) => {
            for (const [field, dir] of sortEntries) {
              const aVal = a[field]
              const bVal = b[field]
              if (aVal === bVal) continue
              if (aVal === undefined || aVal === null) return dir === 1 ? 1 : -1
              if (bVal === undefined || bVal === null) return dir === 1 ? -1 : 1
              if (aVal < bVal) return dir === 1 ? -1 : 1
              if (aVal > bVal) return dir === 1 ? 1 : -1
            }
            return 0
          })
        }

        // Apply pagination
        const paged = items.slice(offset, offset + limit)
        const hasMore = offset + limit < total

        return { items: paged, total, hasMore }
      },

      async findOne(filter?: Record<string, unknown>): Promise<unknown | null> {
        const result = await this.find(filter, { limit: 1 })
        return result.items[0] ?? null
      },

      async get(id: string): Promise<Record<string, unknown> | null> {
        const rows = sql.exec('SELECT id, data, created_at, updated_at FROM entities WHERE id = ? AND deleted_at IS NULL', id).toArray()
        if (rows.length === 0) return null
        const row = rows[0]
        const parsed = row.data ? JSON.parse(row.data as string) : {}
        return {
          ...parsed,
          $id: row.id as string,
          $type: type,
          $createdAt: row.created_at as string,
          $updatedAt: row.updated_at as string,
        }
      },

      async create(data: Record<string, unknown>): Promise<unknown> {
        const id = (data.$id as string) ?? generateEntityId(type)
        const now = new Date().toISOString()

        // Strip meta fields from data payload
        const { $id: _id, $type: _type, $createdAt: _ca, $updatedAt: _ua, $version: _v, $createdBy: _cb, $updatedBy: _ub, ...rest } = data
        const jsonData = JSON.stringify(rest)

        sql.exec('INSERT INTO entities (id, type, data, created_at, updated_at) VALUES (?, ?, ?, ?, ?)', id, type, jsonData, now, now)

        return {
          ...rest,
          $id: id,
          $type: type,
          $createdAt: now,
          $updatedAt: now,
        }
      },

      async update(id: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null> {
        // Fetch existing
        const rows = sql.exec('SELECT id, data, created_at, updated_at FROM entities WHERE id = ? AND deleted_at IS NULL', id).toArray()
        if (rows.length === 0) return null
        const row = rows[0]
        const existing = row.data ? JSON.parse(row.data as string) : {}
        const now = new Date().toISOString()

        // Strip meta fields from incoming data
        const { $id: _id, $type: _type, $createdAt: _ca, $updatedAt: _ua, $version: _v, $createdBy: _cb, $updatedBy: _ub, ...rest } = data
        const merged = { ...existing, ...rest }
        const jsonData = JSON.stringify(merged)

        sql.exec('UPDATE entities SET data = ?, updated_at = ? WHERE id = ?', jsonData, now, id)

        return {
          ...merged,
          $id: id,
          $type: type,
          $createdAt: row.created_at as string,
          $updatedAt: now,
        }
      },

      async delete(id: string): Promise<{ deletedCount: number }> {
        // Soft delete
        const now = new Date().toISOString()
        sql.exec('UPDATE entities SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL', now, id)
        const changesRow = sql.exec('SELECT changes() as cnt').toArray()
        const deletedCount = (changesRow[0]?.cnt as number) ?? 0
        return { deletedCount }
      },
    }
  }

  // =========================================================================
  // DOContext Implementation
  // =========================================================================

  /**
   * Build the DOContext that extracted handler modules use.
   */
  protected getContext(): DOContext {
    return {
      sql: this.sql,
      getCollection: (type: string) => this.getCollection(type),
      normalizeEntity: <T extends Record<string, unknown>>(entity: T): T => entity,
      normalizeEntities: <T extends Record<string, unknown>>(entities: T[]): T[] => entities,
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
   * Override onRequest() to customize REST routing.
   */
  async fetch(request: Request): Promise<Response> {
    this.ensureNamespace(request)

    const url = new URL(request.url)
    const path = url.pathname
    const method = request.method

    // Allow subclasses to intercept before default routing
    const intercepted = await this.onRequest(request, path, method, url)
    if (intercepted) return intercepted

    return jsonResponse({ error: 'Not found' }, 404)
  }

  // =========================================================================
  // RPC-friendly async wrappers
  //
  // These async methods make entity operations accessible via cross-worker
  // DO RPC (Cloudflare requires RPC methods to return Promises).
  // =========================================================================

  async find(
    type: string,
    filter?: Record<string, unknown>,
    options?: { limit?: number; offset?: number; sort?: Record<string, 1 | -1> },
  ): Promise<{ items: unknown[]; total: number; hasMore: boolean }> {
    const collection = this.getCollection(type)
    const result = await collection.find(filter, { ...options, skip: options?.offset })
    return {
      items: result.items,
      total: (result.total as number) ?? 0,
      hasMore: (result.hasMore as boolean) ?? false,
    }
  }

  async countEntities(type: string): Promise<number> {
    const rows = this.sql.exec('SELECT COUNT(*) as cnt FROM entities WHERE type = ? AND deleted_at IS NULL', type).toArray()
    return (rows[0]?.cnt as number) ?? 0
  }

  async getEntity(type: string, id: string): Promise<unknown> {
    const collection = this.getCollection(type)
    return collection.get(id)
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
    // CDC event compaction
    try {
      const result = await this.compactEvents()
      if (result.deleted > 0) {
        console.log(`[DB] Alarm compaction: deleted ${result.deleted} events`)
      }
    } catch (err) {
      console.warn('[DB] Alarm compaction failed:', err)
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

      const dbBucket = (this.env as unknown as DBEnv).DB_BUCKET
      if (eventsToDelete.length > 0 && dbBucket) {
        const archiveKey = `${this._namespace ?? 'default'}/archive/events-${compactedAt.replace(/[:.]/g, '-')}.ndjson`
        const ndjson = eventsToDelete.map((row) => JSON.stringify(row)).join('\n')
        await dbBucket.put(archiveKey, ndjson)
      }
    } catch (err) {
      console.warn('[DB] Event archival failed (proceeding with compaction):', err)
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
      console.warn('[DB] Write-triggered compaction failed:', err)
    })
  }
}

// Backward-compatible alias
export { DB as DatabaseDO }
