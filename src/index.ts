/**
 * @dotdo/db — Database infrastructure for the .do ecosystem
 *
 * DB Durable Object (WAL + CDC) + ClickHouse HTTP client.
 *
 * @example
 * ```typescript
 * // ClickHouse queries
 * import { ClickHouseClient } from '@dotdo/db'
 * const ch = new ClickHouseClient({ endpoint, user, password, database })
 * const result = await ch.query('SELECT data.thesis FROM events WHERE type = {type:String}', { type: 'hypothesis' })
 *
 * // DB Durable Object (in Worker)
 * import { DB } from '@dotdo/db/do'
 * export { DB }
 * ```
 */

// ClickHouse client
export { ClickHouseClient } from './clickhouse/client'
export type { ClickHouseConfig, QueryResult, ExecResult } from './clickhouse/types'
export { migrate, getMigrationStatus } from './clickhouse/migrate'
export type { Migration, MigrateResult, MigrationStatus } from './clickhouse/migrate'

// DB Entity Types
export { toExpanded, toFlat } from './types'
export type { DBEntity, DBEntityExpanded, FindResult, DeleteResult } from './types'

// DB Durable Object
export { DB, DatabaseDO } from './do'
export type { DBEnv, DatabaseDOEnv, DOContext, ParqueDBCollection, EventQueryOptions } from './do'
export type { Entity as DatabaseEntity, SyncEvent, SyncRequest, SyncConflict } from './do'
export type { EventsBinding, DurableEvent, EventLoggerDeps } from './do'
export { EventLogger, queryEventsFromWal } from './do'
export { validateEntityType, isValidEntityType, toEntityType, DANGEROUS_TYPE_NAMES } from './do'
export { generateEntityId } from './do'
export {
  DEFAULT_RETENTION_DAYS,
  MIN_EVENTS_PER_ENTITY,
  COMPACTION_ALARM_INTERVAL_MS,
  buildCompactionQuery,
  computeCompactionCutoff,
  shouldCompact,
  COMPACTION_WRITE_INTERVAL,
  COMPACTION_COOLDOWN_MS,
} from './do'
export { reconstructFromEvents } from './do'
export type { EventRow, ReconstructionResult } from './do'
export { purgeExpiredEntities, previewPurge, DEFAULT_PURGE_RETENTION_DAYS } from './do'
export type { PurgeContext, PurgeOptions, PurgeResult, PreviewResult } from './do'
export { buildDurableEvent, forwardEvents, mapOperationToEventType, OPERATION_TYPE_MAP } from './do'

export const VERSION = '0.2.0'
