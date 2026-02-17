/**
 * @dotdo/db - ParqueDB wrapper for dotdo ecosystem
 *
 * Re-exports core ParqueDB APIs with sensible defaults.
 *
 * @example
 * ```typescript
 * import { DB, FsBackend, MemoryBackend } from '@dotdo/db'
 *
 * // Create a typed database
 * const db = DB({
 *   User: { email: 'string!#', name: 'string' },
 *   Post: { title: 'string!', author: '-> User' }
 * }, {
 *   storage: new FsBackend({ basePath: '.db' })
 * })
 *
 * await db.User.create({ email: 'alice@example.com', name: 'Alice' })
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// DB Factory (Main Entry Point)
// =============================================================================

export {
  DB,
  type CollectionSchema,
  type CollectionOptions,
  type DBSchema,
  type DBConfig,
  type DBInput,
  type DBInstance,
  type DBSchemaInput,
  type TypedDBInstance,
  DEFAULT_COLLECTION_OPTIONS,
  extractCollectionOptions,
  getFieldsWithoutOptions,
  extractAllCollectionOptions,
} from 'parquedb'

// =============================================================================
// Storage Backends
// =============================================================================

export {
  MemoryBackend,
  FsBackend,
  R2Backend,
  R2OperationError,
  R2ETagMismatchError,
  R2NotFoundError,
  DOSqliteBackend,
  getStorageCapabilities,
  hasStorageCapability,
  isStreamable,
  isMultipart,
  isTransactional,
  type R2BackendOptions,
  type DOSqliteBackendOptions,
  type StorageCapabilities,
} from 'parquedb'

// =============================================================================
// Entity Types
// =============================================================================

export type {
  Entity,
  EntityRef,
  Filter,
  FindOptions,
  UpdateInput,
  PaginatedResult,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
} from 'parquedb'

// =============================================================================
// Schema Utilities
// =============================================================================

export {
  parseSchema,
  parseFieldType,
  parseRelation,
  isRelationString,
  isValidFieldType,
  isValidRelationString,
  validateSchema,
  validateTypeDefinition,
  validateRelationshipTargets,
  validateEntityCoreFields,
  validateEntityFull,
  SchemaValidationError,
  SchemaValidator,
  createValidator,
  validate,
  inferSchema,
  inferSchemaFromCollections,
  inferredToTypeDefinition,
  type ValidationMode,
  type SchemaValidatorOptions,
} from 'parquedb'

// =============================================================================
// Query Utilities
// =============================================================================

export {
  matchesFilter,
  createPredicate,
  applyUpdate,
  QueryBuilder,
  type ComparisonOp,
  type StringOp,
  type ExistenceOp,
  type QueryOp,
} from 'parquedb'

// =============================================================================
// Configuration
// =============================================================================

export {
  defineConfig,
  defineSchema,
  detectRuntime,
  isServer,
  isWorkers,
  isBrowser,
  loadWorkersEnv,
  type Runtime,
} from 'parquedb'

// =============================================================================
// SQL Integration
// =============================================================================

export {
  createSQL,
  buildQuery,
  escapeIdentifier,
  escapeString,
  parseSQL,
  translateStatement,
  translateSelect,
  translateInsert,
  translateUpdate,
  translateDelete,
  translateWhere,
  whereToFilter,
  type SQLExecutor,
  type CreateSQLOptions,
  type SQLStatement,
  type SQLSelect,
  type SQLInsert,
  type SQLUpdate,
  type SQLDelete,
  type SQLQueryResult,
  type TranslatedQuery,
  type TranslatedMutation,
} from 'parquedb'

// =============================================================================
// Migration Utilities
// =============================================================================

export {
  importFromJson,
  importFromJsonl,
  importFromCsv,
  importFromMongodb,
  importFromBson,
  inferType,
  parseCsvLine,
  convertBsonValue,
  type MigrationOptions,
  type JsonImportOptions,
  type CsvImportOptions,
  type BsonImportOptions,
  type MigrationResult,
  type MigrationError,
} from 'parquedb'

// =============================================================================
// Errors
// =============================================================================

export {
  ParqueDBError,
  ErrorCode,
  ParqueDBValidationError,
  ParqueDBNotFoundError,
  ParqueDBEntityNotFoundError,
  ParqueDBStorageError,
  ParqueDBRelationshipError,
  isParqueDBError,
  isValidationError,
  isNotFoundError,
  isEntityNotFoundError,
  isStorageError,
  isRelationshipError,
  wrapError,
  errorFromStatus,
  assertValid,
  assertFound,
  type SerializedError,
} from 'parquedb'

// =============================================================================
// Constants
// =============================================================================

export {
  DEFAULT_MAX_INBOUND,
  DEFAULT_PAGE_SIZE,
  MAX_BATCH_SIZE,
  DEFAULT_CONCURRENCY,
  DEFAULT_CACHE_TTL,
  MAX_CACHE_SIZE,
} from 'parquedb'

// =============================================================================
// Entity Search (Cross-Type Full-Text Scoring)
// =============================================================================

export {
  scoreEntity,
  searchEntities,
  type ScoreResult,
  type EntitySearchParams,
  type EntitySearchResult,
} from 'parquedb'

// =============================================================================
// Event Sync (Lightweight CDC Bidirectional Sync)
// =============================================================================

export {
  resolveEventSync,
  computeEventSyncCursor,
  type EventSyncEvent,
  type EventSyncConflictStrategy,
  type EventSyncConflictInfo,
  type EventSyncResult,
  type ResolveEventSyncParams,
  type ResolveEventSyncResult,
} from 'parquedb'

// =============================================================================
// DatabaseDO (Durable Object)
// =============================================================================

export { DatabaseDO } from './do'
export type { DOContext, ParqueDBCollection, EventQueryOptions, DatabaseDOEnv, SyncRequest, SyncConflict, EventsBinding, DurableEvent, EventLoggerDeps } from './do'
export type { SyncEvent, Entity as DatabaseEntity } from './do'
export { EventLogger, queryEventsFromWal, validateEntityType, isValidEntityType, toEntityType, DANGEROUS_TYPE_NAMES, generateEntityId } from './do'
export { DEFAULT_RETENTION_DAYS, MIN_EVENTS_PER_ENTITY, COMPACTION_ALARM_INTERVAL_MS, buildCompactionQuery, computeCompactionCutoff, shouldCompact, COMPACTION_WRITE_INTERVAL, COMPACTION_COOLDOWN_MS } from './do'
export { reconstructFromEvents } from './do'
export type { EventRow, ReconstructionResult } from './do'
export { purgeExpiredEntities, previewPurge, DEFAULT_PURGE_RETENTION_DAYS } from './do'
export type { PurgeContext, PurgeOptions, PurgeResult, PreviewResult } from './do'
export { buildDurableEvent, forwardEvents, mapOperationToEventType, OPERATION_TYPE_MAP } from './do'

// =============================================================================
// Version
// =============================================================================

export const VERSION = '0.1.0'
