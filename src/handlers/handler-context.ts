/**
 * DOContext — interface that captures the DatabaseDO capabilities needed
 * by extracted handler modules.
 */

import type { SyncEvent } from './types'
import type { DBEntity, FindResult, DeleteResult } from '../types'

export interface ParqueDBCollection {
  find(filter?: Record<string, unknown>, options?: Record<string, unknown>): Promise<FindResult>
  findOne(filter?: Record<string, unknown>): Promise<DBEntity | null>
  get(id: string): Promise<DBEntity | null>
  create(data: Record<string, unknown>): Promise<DBEntity>
  update(id: string, data: Record<string, unknown>): Promise<DBEntity | null>
  delete(id: string): Promise<DeleteResult>
}

export interface EventQueryOptions {
  since?: string
  entityType?: string
  entityId?: string
  operation?: string
  limit: number
}

export interface DOContext {
  sql: SqlStorage
  getCollection(type: string): ParqueDBCollection
  normalizeEntity<T extends Record<string, unknown>>(entity: T): T
  normalizeEntities<T extends Record<string, unknown>>(entities: T[]): T[]
  safeLogEvent(op: string, type: string, entityId: string, data?: Record<string, unknown>, prev?: Record<string, unknown>, timestamp?: string): string | undefined
  beginEventBatch(): void
  flushEventBatch(): void
  queryEventsFromWal(options: EventQueryOptions): SyncEvent[]
  jsonResponse(data: unknown, status?: number, extraHeaders?: Record<string, string>): Response
  parseJsonBody<T>(request: Request): Promise<T | Response>
  validateEntityType(type: string): void
  withWriteLock<T>(fn: () => Promise<T>): Promise<T>
  namespace: string | null
  systemSlug: string | null
}
