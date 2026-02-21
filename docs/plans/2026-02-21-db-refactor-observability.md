# @dotdo/db Refactor + Observability Schema Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace ParqueDB with standalone DB Durable Object + ClickHouse client, add observability columns and experiment MVs to ClickHouse schema, update telemetry snippet for variant attribution.

**Architecture:** @dotdo/db becomes two things: (1) a DB Durable Object with raw SqlStorage for WAL + CDC event logging, and (2) a ClickHouse HTTP client for analytics queries. The existing handlers/, lib/, and clickhouse/ modules are already standalone — only do.ts, index.ts, and backends.ts need rewriting. ClickHouse schema evolves via ALTER TABLE + new MVs in apps/clickhouse.

**Tech Stack:** TypeScript, Cloudflare Workers (Durable Objects, SqlStorage), ClickHouse (native JSON, AggregatingMergeTree), Cloudflare Snippets (HTMLRewriter)

---

### Task 1: Remove ParqueDB submodule

**Files:**
- Delete: `.do/db/parquedb/` (git submodule)
- Modify: `.do/db/.gitmodules` (remove parquedb entry)
- Modify: `.do/db/package.json` (remove parquedb dependency)

**Step 1: Deinit and remove the submodule**

```bash
cd /Users/nathanclevenger/projects/headless.ly/.do/db
git submodule deinit -f parquedb
git rm -f parquedb
rm -rf .git/modules/parquedb
```

**Step 2: Remove parquedb from package.json dependencies**

In `.do/db/package.json`, remove:
```json
"parquedb": "file:./parquedb",
```

Keep `sqids` — it's used by `src/lib/id.ts`.

**Step 3: Verify .gitmodules is clean**

If `.gitmodules` is now empty, delete it. Otherwise verify the parquedb entry is gone.

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove parquedb submodule from @dotdo/db"
```

---

### Task 2: Rewrite DB Durable Object (rename DatabaseDO → DB)

The current `DatabaseDO` extends `ParqueDBDO` and uses `DB()` factory + `DOSqliteBackend` from parquedb. The rewrite uses raw `SqlStorage` directly. All the standalone modules (handlers/, lib/) remain unchanged.

**Files:**
- Rewrite: `.do/db/src/do.ts`

**Step 1: Rewrite do.ts**

Replace the entire file. The new `DB` class uses `DurableObject` base class directly (from `cloudflare:workers`) with raw `SqlStorage`. It keeps:
- CDC event logging (EventLogger)
- Write mutex
- Multi-tenancy (namespace/systemSlug)
- Compaction via alarm()
- Entity storage via raw SQL (entities table)
- RPC-friendly async wrappers

It drops:
- `ParqueDBDO` base class
- `DB()` factory and `DOSqliteBackend`
- `normalizeEntity` (was stripping R2 path prefixes from ParqueDB)
- `CacheInvalidationSignal` type

```typescript
/**
 * DB — Durable Object for .do database coordination
 *
 * Standalone entity storage with raw SqlStorage (no ParqueDB). Adds:
 * - Entity CRUD via raw SQL (entities table)
 * - CDC event logging to events table (for Pipeline forwarding)
 * - Write mutex (serializes mutations)
 * - Multi-tenancy (namespace/systemSlug from request headers)
 * - Event compaction via alarm()
 *
 * One DO per system per org (e.g. "org_123:crm", "org_123:finance").
 */

import { DurableObject } from 'cloudflare:workers'
import { validateEntityType } from './lib/entity-types'
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
import { generateEntityId } from './lib/id'

// Re-export types used by consumers
export type { DOContext, ParqueDBCollection } from './handlers/handler-context'
export type { EventQueryOptions } from './handlers/handler-context'
export type { SyncEvent, Entity as DatabaseEntity } from './handlers/types'
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
export { reconstructFromEvents } from './lib/time-travel'
export type { EventRow, ReconstructionResult } from './lib/time-travel'
export { purgeExpiredEntities, previewPurge, DEFAULT_PURGE_RETENTION_DAYS } from './lib/purge-policy'
export type { PurgeContext, PurgeOptions, PurgeResult, PreviewResult } from './lib/purge-policy'
export { buildDurableEvent, forwardEvents, mapOperationToEventType, OPERATION_TYPE_MAP } from './handlers/event-forwarding'
export type { SyncRequest, SyncConflict } from './handlers/types'

// =========================================================================
// DB Env Extension
// =========================================================================

export interface DBEnv {
  EVENTS?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> }
  DB_BUCKET?: { put(key: string, body: string): Promise<unknown> }
}

// =========================================================================
// Response Helpers
// =========================================================================

function jsonResponse(data: unknown, status = 200, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
  })
}

async function parseJsonBody<T>(request: Request): Promise<T | Response> {
  try {
    return (await request.json()) as T
  } catch {
    return jsonResponse({ success: false, error: 'Invalid JSON in request body' }, 400)
  }
}

// =========================================================================
// DB Durable Object
// =========================================================================

export class DB extends DurableObject {
  protected sql: SqlStorage
  protected _namespace: string | null = null
  protected _systemSlug: string | null = null
  protected eventLogger!: EventLogger
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

    // Restore namespace + system from metadata
    const stored = this.sql.exec('SELECT key, value FROM metadata WHERE key IN (?, ?)', 'namespace', 'system').toArray()
    for (const row of stored) {
      if (row.key === 'namespace') this._namespace = row.value as string
      else if (row.key === 'system') this._systemSlug = row.value as string
    }

    // Schedule compaction alarm
    this.ctx.storage.getAlarm().then((alarm) => {
      if (!alarm) this.ctx.storage.setAlarm(Date.now() + COMPACTION_ALARM_INTERVAL_MS)
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
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        deleted_at TEXT
      )
    `)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type)`)
    this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_entities_type_deleted ON entities(type, deleted_at)`)
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
  // Tenant Resolution
  // =========================================================================

  protected ensureTenant(request: Request): void {
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
  // Write Lock
  // =========================================================================

  protected withWriteLock<T>(fn: () => Promise<T>): Promise<T> {
    const result = this._writeTail.then(fn, fn)
    this._writeTail = result.then(() => {}, () => {})
    return result
  }

  // =========================================================================
  // Entity CRUD (raw SQL)
  // =========================================================================

  protected getCollection(type: string): ParqueDBCollection {
    validateEntityType(type)
    const sql = this.sql
    const logger = this.eventLogger

    return {
      async find(filter?: Record<string, unknown>, options?: Record<string, unknown>): Promise<{ items: unknown[]; total?: number; hasMore?: boolean }> {
        const limit = (options?.limit as number) ?? 100
        const offset = (options?.offset as number) ?? 0
        const rows = sql.exec(
          `SELECT id, type, data, created_at, updated_at FROM entities WHERE type = ? AND deleted_at IS NULL ORDER BY created_at DESC LIMIT ? OFFSET ?`,
          type, limit + 1, offset,
        ).toArray()
        const hasMore = rows.length > limit
        const items = (hasMore ? rows.slice(0, limit) : rows).map((r) => ({ ...JSON.parse(r.data as string), $id: r.id, $type: r.type }))
        const countRow = sql.exec(`SELECT count(*) as c FROM entities WHERE type = ? AND deleted_at IS NULL`, type).toArray()
        return { items, total: Number(countRow[0]?.c ?? 0), hasMore }
      },
      async findOne(filter?: Record<string, unknown>): Promise<unknown | null> {
        const result = await this.find(filter, { limit: 1 })
        return result.items[0] ?? null
      },
      async get(id: string): Promise<Record<string, unknown> | null> {
        const row = sql.exec(`SELECT id, type, data, created_at, updated_at FROM entities WHERE id = ? AND deleted_at IS NULL`, id).toArray()
        if (row.length === 0) return null
        return { ...JSON.parse(row[0].data as string), $id: row[0].id, $type: row[0].type }
      },
      async create(data: Record<string, unknown>): Promise<unknown> {
        const id = (data.$id as string) ?? generateEntityId(type)
        const json = JSON.stringify(data)
        sql.exec(`INSERT INTO entities (id, type, data) VALUES (?, ?, ?)`, id, type, json)
        logger.safeLogEvent('create', type, id, data)
        return { ...data, $id: id, $type: type }
      },
      async update(id: string, data: Record<string, unknown>): Promise<Record<string, unknown> | null> {
        const existing = sql.exec(`SELECT data FROM entities WHERE id = ? AND deleted_at IS NULL`, id).toArray()
        if (existing.length === 0) return null
        const prev = JSON.parse(existing[0].data as string)
        const merged = { ...prev, ...data }
        const json = JSON.stringify(merged)
        sql.exec(`UPDATE entities SET data = ?, updated_at = datetime('now') WHERE id = ?`, json, id)
        logger.safeLogEvent('update', type, id, merged, prev)
        return { ...merged, $id: id, $type: type }
      },
      async delete(id: string): Promise<{ deletedCount: number }> {
        const existing = sql.exec(`SELECT data FROM entities WHERE id = ? AND deleted_at IS NULL`, id).toArray()
        if (existing.length === 0) return { deletedCount: 0 }
        sql.exec(`UPDATE entities SET deleted_at = datetime('now') WHERE id = ?`, id)
        logger.safeLogEvent('delete', type, id)
        return { deletedCount: 1 }
      },
    }
  }

  // =========================================================================
  // DOContext
  // =========================================================================

  protected getContext(): DOContext {
    return {
      sql: this.sql,
      getCollection: (type: string) => this.getCollection(type),
      normalizeEntity: <T extends Record<string, unknown>>(e: T) => e,
      normalizeEntities: <T extends Record<string, unknown>>(es: T[]) => es,
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
  // RPC Methods
  // =========================================================================

  async find(
    type: string,
    filter?: Record<string, unknown>,
    options?: { limit?: number; offset?: number },
  ): Promise<{ items: unknown[]; total?: number; hasMore?: boolean }> {
    return this.getCollection(type).find(filter, options)
  }

  async countEntities(type: string): Promise<number> {
    const row = this.sql.exec(`SELECT count(*) as c FROM entities WHERE type = ? AND deleted_at IS NULL`, type).toArray()
    return Number(row[0]?.c ?? 0)
  }

  async getEntity(type: string, id: string): Promise<unknown> {
    return this.getCollection(type).get(id)
  }

  // =========================================================================
  // Request Routing
  // =========================================================================

  async fetch(request: Request): Promise<Response> {
    this.ensureTenant(request)
    const intercepted = await this.onRequest(request, new URL(request.url).pathname, request.method, new URL(request.url))
    if (intercepted) return intercepted
    return jsonResponse({ error: 'Not found' }, 404)
  }

  protected async onRequest(_request: Request, _path: string, _method: string, _url: URL): Promise<Response | null> {
    return null
  }

  // =========================================================================
  // Compaction
  // =========================================================================

  async alarm(): Promise<void> {
    try {
      const result = await this.compactEvents()
      if (result.deleted > 0) console.log(`[DB] Compaction: deleted ${result.deleted} events`)
    } catch (err) {
      console.warn('[DB] Compaction failed:', err)
    }
    this.ctx.storage.setAlarm(Date.now() + COMPACTION_ALARM_INTERVAL_MS)
  }

  async compactEvents(retentionDays: number = DEFAULT_RETENTION_DAYS): Promise<{ deleted: number; compactedAt: string }> {
    const compactedAt = new Date().toISOString()
    const lastRow = this.sql.exec("SELECT value FROM metadata WHERE key = 'last_compaction'").toArray()
    const lastTimestamp = lastRow.length > 0 ? (lastRow[0].value as string) : null
    if (!shouldCompact(lastTimestamp)) return { deleted: 0, compactedAt }

    const cutoff = computeCompactionCutoff(retentionDays)

    // Archive before deleting
    try {
      const eventsToDelete = this.sql.exec(
        `SELECT * FROM events WHERE timestamp < ? AND id NOT IN (
          SELECT id FROM events WHERE entity_type || ':' || entity_id IN (
            SELECT entity_type || ':' || entity_id FROM events GROUP BY entity_type, entity_id
          ) ORDER BY timestamp DESC LIMIT ?
        )`,
        cutoff, MIN_EVENTS_PER_ENTITY,
      ).toArray()

      const dbBucket = (this.env as unknown as DBEnv).DB_BUCKET
      if (eventsToDelete.length > 0 && dbBucket) {
        const archiveKey = `${this._namespace ?? 'default'}/archive/events-${compactedAt.replace(/[:.]/g, '-')}.ndjson`
        const ndjson = eventsToDelete.map((row) => JSON.stringify(row)).join('\n')
        await dbBucket.put(archiveKey, ndjson)
      }
    } catch (err) {
      console.warn('[DB] Event archival failed:', err)
    }

    this.sql.exec(buildCompactionQuery(), MIN_EVENTS_PER_ENTITY, cutoff)
    const changesRow = this.sql.exec('SELECT changes() as cnt').toArray()
    const deleted = (changesRow[0]?.cnt as number) ?? 0
    this.sql.exec("INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_compaction', ?)", compactedAt)
    return { deleted, compactedAt }
  }

  protected maybeCompact(): void {
    this.compactEvents().catch((err) => console.warn('[DB] Write-triggered compaction failed:', err))
  }
}
```

**Step 2: Verify the file compiles in isolation**

Run: `cd /Users/nathanclevenger/projects/headless.ly/.do/db && npx tsc --noEmit src/do.ts 2>&1 | head -20`

Note: This will fail until Task 3 updates index.ts and removes backends.ts. That's expected. The important thing is that do.ts itself has no parquedb imports.

**Step 3: Commit**

```bash
git add src/do.ts
git commit -m "refactor: rewrite DatabaseDO → DB with raw SqlStorage (no ParqueDB)"
```

---

### Task 3: Rewrite @dotdo/db exports + delete backends.ts

**Files:**
- Rewrite: `.do/db/src/index.ts`
- Delete: `.do/db/src/backends.ts`
- Modify: `.do/db/package.json` (update description, exports)

**Step 1: Delete backends.ts**

```bash
rm /Users/nathanclevenger/projects/headless.ly/.do/db/src/backends.ts
```

**Step 2: Rewrite index.ts**

Replace the entire file:

```typescript
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

// DB Durable Object
export { DB } from './do'
export type { DBEnv, DOContext, ParqueDBCollection, EventQueryOptions } from './do'
export type { SyncEvent, DatabaseEntity, SyncRequest, SyncConflict } from './do'
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
```

**Step 3: Update package.json**

Change description and update exports:

```json
{
  "name": "@dotdo/db",
  "version": "0.2.0",
  "description": "DB Durable Object + ClickHouse client for the .do ecosystem",
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "import": "./dist/index.js",
      "types": "./dist/index.d.ts"
    },
    "./do": {
      "workerd": "./dist/do.js",
      "worker": "./dist/do.js",
      "import": "./dist/do.js",
      "types": "./dist/do.d.ts"
    },
    "./clickhouse": {
      "import": "./dist/clickhouse/index.js",
      "types": "./dist/clickhouse/index.d.ts"
    }
  },
  "files": ["dist"],
  "scripts": {
    "build": "tsc",
    "clean": "rm -rf dist",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "sqids": "^0.3.0"
  },
  "devDependencies": {
    "@cloudflare/workers-types": "^4.20260205.0",
    "typescript": "^5.7.2"
  },
  "keywords": ["database", "clickhouse", "durable-objects", "dotdo"],
  "license": "MIT"
}
```

**Step 4: Run typecheck**

Run: `cd /Users/nathanclevenger/projects/headless.ly/.do/db && npx tsc --noEmit`
Expected: 0 errors (or only cloudflare:workers import errors which resolve at runtime)

**Step 5: Commit**

```bash
git add src/index.ts src/backends.ts package.json
git commit -m "refactor: rewrite @dotdo/db exports — ClickHouse client + DB DO, remove ParqueDB"
```

---

### Task 4: Add observability columns to ClickHouse events table

The existing events table in `apps/clickhouse/src/index.ts` already has `id`, `ns`, `ts`, `type`, `event`, `source`, `data JSON`, `embedding`. We need to add: `category`, `experiment`, `variant`, `hypothesis`, `actor`, `anon`, `session`, `trace`.

**Files:**
- Modify: `apps/clickhouse/src/index.ts` — `getSetupStatements()` function

**Step 1: Update the events CREATE TABLE statement**

In `apps/clickhouse/src/index.ts`, replace the events table DDL (lines ~196-208) with the expanded schema. Keep the fixed UUID and s3 settings.

Replace:
```sql
CREATE TABLE IF NOT EXISTS events ${u.events} (
  id String DEFAULT generateUUIDv4(),
  ns LowCardinality(String) DEFAULT '',
  ts DateTime64(3) DEFAULT now64(3),
  type LowCardinality(String) DEFAULT '',
  event LowCardinality(String) DEFAULT '',
  source LowCardinality(String) DEFAULT '',
  data JSON,
  embedding Array(Float32) DEFAULT []
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ns, type, id, ts)${s3}
```

With:
```sql
CREATE TABLE IF NOT EXISTS events ${u.events} (
  id String DEFAULT generateUUIDv4(),
  ns LowCardinality(String) DEFAULT '',
  ts DateTime64(3) DEFAULT now64(3),
  type LowCardinality(String) DEFAULT '',
  category LowCardinality(String) DEFAULT multiIf(
    type IN ('pageview','click','scroll','vitals','error','console','network','impression'), 'browser',
    startsWith(type, 'otel.'), 'otel',
    type = 'recording', 'recording',
    'business'
  ),
  event LowCardinality(String) DEFAULT '',
  source LowCardinality(String) DEFAULT '',
  data JSON,
  experiment Nullable(String) DEFAULT NULL,
  variant Nullable(String) DEFAULT NULL,
  hypothesis Nullable(String) DEFAULT NULL,
  actor Nullable(String) DEFAULT NULL,
  anon Nullable(String) DEFAULT NULL,
  session Nullable(String) DEFAULT NULL,
  trace Nullable(String) DEFAULT NULL,
  embedding Array(Float32) DEFAULT []
) ENGINE = ReplacingMergeTree()
PARTITION BY (category, toYYYYMM(ts))
ORDER BY (ns, type, toDate(ts), id)
TTL toDate(ts) + INTERVAL 7 DAY DELETE WHERE category = 'recording',
    toDate(ts) + INTERVAL 30 DAY DELETE WHERE category = 'otel',
    toDate(ts) + INTERVAL 90 DAY DELETE WHERE category = 'browser'
${s3}
```

**Step 2: Update the S3Queue MV to pass new columns**

In `getQueueStatements()`, update the `events_ingest_mv` to extract the new fields from the value JSON:

Replace the MV with:
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS events_ingest_mv TO events AS
SELECT
  coalesce(nullIf(JSONExtractString(value, 'id'), ''), toString(generateUUIDv4())) AS id,
  JSONExtractString(value, 'ns') AS ns,
  parseDateTime64BestEffortOrZero(JSONExtractString(value, 'ts'), 3) AS ts,
  JSONExtractString(value, 'type') AS type,
  JSONExtractString(value, 'event') AS event,
  JSONExtractString(value, 'source') AS source,
  if(JSONHas(value, 'data') AND JSONExtractRaw(value, 'data') NOT IN ('', 'null'), JSONExtractRaw(value, 'data'), '{}') AS data,
  nullIf(JSONExtractString(value, 'experiment'), '') AS experiment,
  nullIf(JSONExtractString(value, 'variant'), '') AS variant,
  nullIf(JSONExtractString(value, 'hypothesis'), '') AS hypothesis,
  nullIf(JSONExtractString(value, 'actor'), '') AS actor,
  nullIf(JSONExtractString(value, 'anon'), '') AS anon,
  nullIf(JSONExtractString(value, 'session'), '') AS session,
  nullIf(JSONExtractString(value, 'trace'), '') AS trace,
  CAST([] AS Array(Float32)) AS embedding
FROM events_queue
```

**Step 3: Update ensureSetup table count**

The `ensureSetup` function checks for a minimum table count. After adding new tables (rewards, sessions, recordings in Tasks 5-7), increase the count check to match.

**Step 4: Verify typecheck**

Run: `cd /Users/nathanclevenger/projects/headless.ly/apps/clickhouse && pnpm typecheck`
Expected: PASS

**Step 5: Commit**

```bash
git add apps/clickhouse/src/index.ts
git commit -m "feat(clickhouse): add observability columns — category, experiment, variant, session, trace"
```

---

### Task 5: Add rewards materialized view

**Files:**
- Modify: `apps/clickhouse/src/index.ts` — add to `getSetupStatements()`

**Step 1: Add rewards table + MV**

Add a new fixed UUID to the `u` object:
```typescript
rewards: "UUID 'deadbeef-0001-4000-a000-200000000016'",
```

Add these two DDL statements after the existing metrics_daily MV:

```sql
CREATE TABLE IF NOT EXISTS rewards ${u.rewards} (
  experiment String,
  variant String,
  ns LowCardinality(String),
  day Date,
  impressions SimpleAggregateFunction(sum, UInt64),
  clicks SimpleAggregateFunction(sum, UInt64),
  conversions SimpleAggregateFunction(sum, UInt64),
  revenue SimpleAggregateFunction(sum, Float64),
  users AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (ns, experiment, variant, day)${s3}
```

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS rewards_mv TO rewards AS
SELECT
  experiment, variant, ns,
  toDate(ts) AS day,
  countIf(type = 'impression') AS impressions,
  countIf(type = 'click') AS clicks,
  countIf(type = 'conversion') AS conversions,
  sumIf(toFloat64OrZero(toString(data.revenue)), type = 'conversion') AS revenue,
  uniqState(coalesce(anon, '')) AS users
FROM events
WHERE variant IS NOT NULL
  AND type IN ('impression', 'click', 'conversion')
GROUP BY experiment, variant, ns, day
```

**Step 2: Add to DROP_STATEMENTS**

Add at the top of DROP_STATEMENTS (before AI analytics):
```typescript
'DROP VIEW IF EXISTS rewards_mv',
'DROP TABLE IF EXISTS rewards',
```

**Step 3: Update ensureSetup table count check**

Increment the minimum table count by 1 (rewards).

**Step 4: Commit**

```bash
git add apps/clickhouse/src/index.ts
git commit -m "feat(clickhouse): add rewards MV — per-variant experiment metrics"
```

---

### Task 6: Add sessions materialized view

**Files:**
- Modify: `apps/clickhouse/src/index.ts` — add to `getSetupStatements()`

**Step 1: Add sessions table + MV**

Add UUID:
```typescript
sessions_agg: "UUID 'deadbeef-0001-4000-a000-200000000017'",
```

Add DDL:

```sql
CREATE TABLE IF NOT EXISTS sessions_agg ${u.sessions_agg} (
  session String,
  ns LowCardinality(String),
  anon SimpleAggregateFunction(any, String),
  actor SimpleAggregateFunction(any, Nullable(String)),
  started SimpleAggregateFunction(min, DateTime64(3)),
  ended SimpleAggregateFunction(max, DateTime64(3)),
  clicks SimpleAggregateFunction(sum, UInt64),
  errors SimpleAggregateFunction(sum, UInt64),
  pages AggregateFunction(groupUniqArray, String),
  bytes SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (ns, session)${s3}
```

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS sessions_agg_mv TO sessions_agg AS
SELECT
  session, ns,
  anyOrNull(anon) AS anon,
  anyOrNull(actor) AS actor,
  min(ts) AS started,
  max(ts) AS ended,
  countIf(type = 'click') AS clicks,
  countIf(type = 'error') AS errors,
  groupUniqArrayState(coalesce(toString(data.href), toString(data.path), '')) AS pages,
  sum(length(toString(data))) AS bytes
FROM events
WHERE session IS NOT NULL AND category IN ('browser', 'recording')
GROUP BY session, ns
```

Note: Named `sessions_agg` to avoid conflict with the existing `sessions` table (from migration 0010).

**Step 2: Add to DROP_STATEMENTS**

```typescript
'DROP VIEW IF EXISTS sessions_agg_mv',
'DROP TABLE IF EXISTS sessions_agg',
```

**Step 3: Commit**

```bash
git add apps/clickhouse/src/index.ts
git commit -m "feat(clickhouse): add sessions_agg MV — session metadata aggregation"
```

---

### Task 7: Add recordings materialized view

**Files:**
- Modify: `apps/clickhouse/src/index.ts`

**Step 1: Add recordings table + MV**

Add UUID:
```typescript
recordings: "UUID 'deadbeef-0001-4000-a000-200000000018'",
```

Add DDL:

```sql
CREATE TABLE IF NOT EXISTS recordings ${u.recordings} (
  session String,
  ns LowCardinality(String),
  ts DateTime64(3),
  anon String DEFAULT '',
  actor Nullable(String) DEFAULT NULL,
  url String DEFAULT '',
  payload String CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ns, session, ts)
TTL toDate(ts) + INTERVAL 7 DAY
${s3}
```

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS recordings_mv TO recordings AS
SELECT
  session, ns, ts,
  coalesce(anon, '') AS anon,
  actor,
  coalesce(toString(data.href), '') AS url,
  toString(data) AS payload
FROM events
WHERE type = 'recording' AND session IS NOT NULL
```

**Step 2: Add to DROP_STATEMENTS**

```typescript
'DROP VIEW IF EXISTS recordings_mv',
'DROP TABLE IF EXISTS recordings',
```

**Step 3: Commit**

```bash
git add apps/clickhouse/src/index.ts
git commit -m "feat(clickhouse): add recordings MV — rrweb session payloads (ZSTD, 7d TTL)"
```

---

### Task 8: Update telemetry snippet — rename, cookies, batching

The existing snippet at `apps/api/snippets/analytics.js` already captures pageviews + vitals. We need to:
- Rename cookie names (`__hly_aid` → `_id`, `__hly_sid` → `_session`)
- Add `session` and `anon` fields to all events
- Add event batching (5s flush + visibilitychange)
- Add error/console capture
- Rename script tag from `data-hly` to `data-e`

**Files:**
- Modify: `apps/api/snippets/analytics.js`

**Step 1: Update the snippet**

Rewrite `buildScript()` in `apps/api/snippets/analytics.js`. The function signature stays the same (takes `actorJson`), but the inline script changes:

Key changes:
- `__hly_aid` → `_id`, `__hly_sid` → `_session`
- `<script data-hly>` → `<script data-e>`
- Add `Q` (event queue) and 5s flush interval
- Add `session` and `anon` on every event
- Add `window.onerror` and `unhandledrejection` capture
- Add `console.error` interception

The full snippet code will be provided in implementation.

**Step 2: Update ScriptInjector selector if needed**

The selector stays `head` (prepend into `<head>`).

**Step 3: Verify snippet deploys**

Run: `cd /Users/nathanclevenger/projects/headless.ly && npx tsx scripts/deploy-analytics-snippet.ts` (or verify in Cloudflare dashboard)

**Step 4: Commit**

```bash
git add apps/api/snippets/analytics.js
git commit -m "feat(snippet): rename cookies to _id/_session, add batching + error capture"
```

---

### Task 9: Add variant attribution to snippet

**Files:**
- Modify: `apps/api/snippets/analytics.js`

**Step 1: Add IntersectionObserver for `[data-v]` impression tracking**

In the inline script, after the core pageview/vitals code, add:

- IntersectionObserver watching `[data-v]` elements (50% threshold)
- On intersection: emit `impression` event with `experiment` (from nearest `data-e` ancestor) and `variant` (from `data-v`)
- MutationObserver to catch dynamically added `[data-v]` elements
- Track already-observed elements to avoid duplicate impressions

**Step 2: Add click attribution**

On `document.addEventListener('click', ...)`:
- Walk up from `event.target` to find nearest `[data-v]` ancestor
- If found, emit `click` event with `experiment` and `variant` fields
- Include `data.text` (element text), `data.href` (if link)

**Step 3: Commit**

```bash
git add apps/api/snippets/analytics.js
git commit -m "feat(snippet): add variant impression tracking + click attribution"
```

---

### Task 10: Add rrweb lazy-loading to snippet

**Files:**
- Modify: `apps/api/snippets/analytics.js`

**Step 1: Add rrweb loader after window.load**

In the inline script, after `window.addEventListener('load', ...)`:

```javascript
// Lazy-load rrweb after page load
setTimeout(function() {
  var s = document.createElement('script')
  s.src = 'https://cdn.jsdelivr.net/npm/rrweb@2/dist/rrweb-all.umd.min.js'
  s.onload = function() {
    rrweb.record({
      emit: function(event) {
        ev('recording', 'rrweb.mutation', { payload: JSON.stringify(event) })
      },
      sampling: { mousemove: false, scroll: 150 }
    })
  }
  document.head.appendChild(s)
}, 3000)
```

Note: `mousemove: false` reduces recording volume. `scroll: 150` samples every 150ms.

**Step 2: Commit**

```bash
git add apps/api/snippets/analytics.js
git commit -m "feat(snippet): add lazy-loaded rrweb session recording"
```

---

### Task 11: Update docs and commit everything

**Files:**
- Modify: `.do/db/README.md`
- Modify: `.do/db/CLAUDE.md` (if exists)

**Step 1: Update README**

Update `.do/db/README.md` to reflect:
- `DB` class (not `DatabaseDO`)
- No ParqueDB dependency
- ClickHouse client in `src/clickhouse/`
- New package exports (`.`, `./do`, `./clickhouse`)

**Step 2: Final verification**

Run: `cd /Users/nathanclevenger/projects/headless.ly/.do/db && npx tsc --noEmit`
Run: `cd /Users/nathanclevenger/projects/headless.ly/apps/clickhouse && pnpm typecheck`

**Step 3: Commit**

```bash
cd /Users/nathanclevenger/projects/headless.ly/.do/db
git add -A
git commit -m "docs: update README for DB DO + ClickHouse client architecture"
```
