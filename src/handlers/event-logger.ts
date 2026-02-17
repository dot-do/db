/**
 * EventLogger — CDC event logging, batching, and forwarding.
 */

import { type DurableEvent, buildDurableEvent, forwardEvents, type EventsBinding } from './event-forwarding'
import { COMPACTION_WRITE_INTERVAL } from '../lib/compaction'
import type { SyncEvent } from './types'
import type { EventQueryOptions } from './handler-context'

export interface EventLoggerDeps {
  sql: SqlStorage
  eventsBinding: EventsBinding | undefined
  waitUntil: (promise: Promise<unknown>) => void
  doIdentity: { id: string; class: string }
  onCompactionThreshold: () => void
}

export class EventLogger {
  private eventBuffer: DurableEvent[] = []
  private isBatching = false
  private writeCounter = 0

  constructor(private deps: EventLoggerDeps) {}

  logEvent(
    operation: 'create' | 'update' | 'delete' | (string & {}),
    type: string,
    entityId: string,
    data?: Record<string, unknown>,
    timestamp?: string,
  ): string {
    const id = crypto.randomUUID()
    const ts = timestamp ?? new Date().toISOString()

    this.deps.sql.exec(
      'INSERT INTO events (id, timestamp, operation, entity_type, entity_id, data) VALUES (?, ?, ?, ?, ?, ?)',
      id,
      ts,
      operation,
      type,
      entityId,
      data ? JSON.stringify(data) : null,
    )

    return id
  }

  safeLogEvent(
    operation: 'create' | 'update' | 'delete' | (string & {}),
    type: string,
    entityId: string,
    data?: Record<string, unknown>,
    prev?: Record<string, unknown>,
    timestamp?: string,
  ): string | undefined {
    try {
      const eventId = this.logEvent(operation, type, entityId, data, timestamp)

      const durableEvent = buildDurableEvent(operation, type, entityId, this.deps.doIdentity, data, prev)

      if (this.isBatching) {
        this.eventBuffer.push(durableEvent)
      } else {
        this.forwardToEvents([durableEvent])
      }

      this.writeCounter++
      if (this.writeCounter >= COMPACTION_WRITE_INTERVAL) {
        this.writeCounter = 0
        this.deps.onCompactionThreshold()
      }

      return eventId
    } catch (err) {
      console.warn(`[DatabaseDO] Failed to log CDC event (${operation} ${type}/${entityId}):`, err)
      return undefined
    }
  }

  forwardToEvents(events: DurableEvent[]): void {
    if (!this.deps.eventsBinding || events.length === 0) return

    try {
      const promise = forwardEvents(this.deps.eventsBinding, events)
      this.deps.waitUntil(promise)
    } catch (err) {
      console.warn('[DatabaseDO] EVENTS forwarding failed:', err)
    }
  }

  beginEventBatch(): void {
    this.isBatching = true
    this.eventBuffer = []
  }

  flushEventBatch(): void {
    this.isBatching = false
    if (this.eventBuffer.length > 0) {
      this.forwardToEvents(this.eventBuffer)
      this.eventBuffer = []
    }
  }
}

/**
 * Query CDC events from the SQLite WAL with optional filters.
 */
export function queryEventsFromWal(sql: SqlStorage, options: EventQueryOptions): SyncEvent[] {
  let query = 'SELECT * FROM events'
  const conditions: string[] = []
  const values: (string | number)[] = []

  if (options.since) {
    conditions.push('timestamp > ?')
    values.push(options.since)
  }
  if (options.entityType) {
    conditions.push('entity_type = ?')
    values.push(options.entityType)
  }
  if (options.entityId) {
    conditions.push('entity_id = ?')
    values.push(options.entityId)
  }
  if (options.operation) {
    conditions.push('operation = ?')
    values.push(options.operation)
  }

  if (conditions.length > 0) {
    query += ' WHERE ' + conditions.join(' AND ')
  }

  query += ' ORDER BY timestamp DESC LIMIT ?'
  values.push(options.limit)

  const rows = sql.exec(query, ...values).toArray()

  return rows.map((row) => ({
    id: row.id as string,
    timestamp: row.timestamp as string,
    operation: row.operation as 'create' | 'update' | 'delete',
    type: row.entity_type as string,
    entityId: row.entity_id as string,
    data: row.data ? JSON.parse(row.data as string) : undefined,
    checksum: row.checksum as string | undefined,
  }))
}
