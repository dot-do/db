# Sharding Strategy and Capacity Planning

Operational documentation for sharding, capacity planning, and migration patterns in the `.do/db` stack (ParqueDB + Cloudflare Durable Objects + R2).

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Cloudflare Durable Object Limits](#cloudflare-durable-object-limits)
- [Current Sharding Model](#current-sharding-model)
- [Sharding Strategies](#sharding-strategies)
- [When to Shard](#when-to-shard)
- [Capacity Planning](#capacity-planning)
- [Migration Patterns](#migration-patterns)
- [ParqueDB-Specific Concerns](#parquedb-specific-concerns)
- [R2 Storage Costs and Access Patterns](#r2-storage-costs-and-access-patterns)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Runbooks](#runbooks)

---

## Architecture Overview

The `.do/db` stack uses a three-layer storage architecture:

```
Worker (Hono)
  |
  +-- getDO() --> DatabaseDO (Durable Object)
  |                  |
  |                  +-- SQLite WAL  (CDC events, per-DO isolation)
  |                  +-- @dotdo/db   (ParqueDB with R2Backend)
  |                          |
  +-- QueryExecutor --------+---> R2 Bucket ("headlessly-db")
                                    |
                                    +-- {orgId}/data/{ns}/data.parquet
                                    +-- {orgId}/rels/forward/{ns}.parquet
                                    +-- {orgId}/rels/reverse/{ns}.parquet
                                    +-- {orgId}/events/current.parquet
```

**Key design decisions:**

- **One DO per system per org**: `DO('{orgId}:{systemSlug}')` -- e.g. `org_123:crm`, `org_123:billing`
- **Shared R2 namespace**: All systems in an org share the same R2 prefix (`{orgId}/`)
- **Isolated CDC streams**: Each DO has its own SQLite WAL for change events
- **Reads bypass the DO**: The `QueryExecutor` reads directly from R2 Parquet files (no DO bottleneck for reads)

---

## Cloudflare Durable Object Limits

These are hard limits enforced by the Cloudflare runtime. Exceeding them causes runtime errors, isolate termination, or silent data loss.

Source: [`@dotdo/do/storage-limits.ts`](../../dotdo/do/storage-limits.ts)

| Resource | Limit | Constant | Notes |
|----------|-------|----------|-------|
| **Memory per DO** | 128 MB | `MAX_MEMORY_MB` | Exceeding terminates the isolate. All in-flight requests fail. DO restarts on next request. |
| **Persistent storage** | 10 GB | `MAX_STORAGE_GB` | Total KV storage via `state.storage.put()`. Exceeding causes `put()` to throw. |
| **Value size** | 128 KB | `MAX_VALUE_SIZE` | Per-key value limit for `state.storage.put()`. Structured clone serialized. |
| **Key size** | 2,048 bytes | `MAX_KEY_SIZE` | UTF-8 encoded key length. |
| **Keys per `list()`** | 1,000 | `MAX_LIST_BATCH` | Must paginate with `startAfter` for more. Use `StorageGuard.paginatedList()`. |
| **Keys per `get()`** | 128 | `MAX_KEYS_PER_GET` | Batch get limit. |
| **Keys per `put()`** | 128 | `MAX_KEYS_PER_PUT` | Batch put limit. |
| **Durable writes/sec** | ~30 | (empirical) | Per-DO write throughput before backpressure causes latency spikes. |
| **Read throughput** | ~100/sec | (empirical) | In-memory cache hits are much higher. Cold reads hit SQLite. |
| **Subrequests per invocation** | 1,000 | `WORKERS_PAID_SUBREQUEST_LIMIT` | Each R2 get/put counts. Bulk operations must batch carefully. |

### Warning Thresholds

The `StorageGuard` emits warnings at 80% of limits (`DEFAULT_WARN_THRESHOLD = 0.8`):

- Value size warning at **102,400 bytes** (80% of 128 KB)
- Storage warning at **70%** of capacity (configurable via `ShardHealthMonitor`)
- Storage unhealthy at **90%** of capacity

---

## Current Sharding Model

### Tenant + System Sharding (Production)

headless.ly currently uses a two-level sharding scheme:

```
DO ID = "{orgId}:{systemSlug}"

Examples:
  org_abc:crm       -- CRM system for org abc
  org_abc:billing   -- Billing system for org abc
  org_xyz:crm       -- CRM system for org xyz
  public            -- Public/anonymous access (no system suffix)
```

This is implemented in [`apps/db/src/lib/do.ts`](../../../apps/db/src/lib/do.ts):

```typescript
const doName = systemSlug ? `${ns}:${systemSlug}` : ns
const doId = c.env.DATABASE.idFromName(doName)
const stub = c.env.DATABASE.get(doId)
```

**Isolation guarantees:**
- Each org's data is in a separate DO (strong tenant isolation)
- Each system within an org has its own DO (isolated CDC streams)
- All systems in an org share the same R2 prefix (cross-system queries possible via R2)

### Limitations of Current Model

- **No entity-type sharding within a system**: A single CRM DO handles Contacts, Deals, Companies, etc.
- **No time-based sharding**: Event streams grow unbounded within a single DO's SQLite WAL
- **Hot tenant problem**: A single high-traffic org can saturate its DO's 30 writes/sec limit

---

## Sharding Strategies

The `@dotdo/do` package provides a full sharding toolkit. See [`.do/dotdo/docs/SHARDING.md`](../../dotdo/docs/SHARDING.md) for API reference.

### 1. Type-Based Sharding

Separate DOs per entity type within a system. Best for workloads where one entity type dominates write volume.

```typescript
import { ShardRouter } from '@dotdo/do'

const router = new ShardRouter({
  defaultShardCount: 1,  // Most types in a single DO
  entityShards: {
    events: 16,     // High-write-volume event streams
    contacts: 4,    // Moderate-volume CRM entities
    invoices: 8,    // Financial data with write bursts
  },
})

// Route: org_abc:events:shard-7
const { doName } = router.route({
  namespace: 'org_abc',
  entityType: 'events',
  entityId: 'evt_12345',
  path: '/events/evt_12345',
})
```

**When to use:**
- One entity type (e.g. `Event`) generates 10x+ more writes than others
- Entity types have different access patterns (hot events vs cold settings)
- You need to isolate blast radius -- a crash in the events DO should not affect contacts

**Trade-offs:**
- (+) Fine-grained isolation per entity type
- (+) Independent scaling per type
- (-) Cross-type queries require scatter-gather
- (-) More DOs to monitor and manage

### 2. Tenant-Based Sharding (Current)

Already implemented via `idFromName(tenantId)`. Each tenant (org) gets its own DO.

```typescript
const doId = env.DATABASE.idFromName('org_abc')  // or 'org_abc:crm'
const stub = env.DATABASE.get(doId)
```

**When to use:**
- Multi-tenant SaaS (headless.ly's primary model)
- Regulatory isolation requirements (tenant data must not co-locate)
- Tenants have predictable, bounded data volumes

**Trade-offs:**
- (+) Perfect tenant isolation -- no noisy-neighbor risk
- (+) Simple mental model -- one org = one DO
- (-) Cannot scale within a single tenant beyond DO limits
- (-) Small tenants waste DO resources (each DO has fixed overhead)

### 3. Time-Based Sharding

Partition high-write event streams into hourly or daily buckets. Essential for event-sourced systems where the event log grows unbounded.

```typescript
function getTimeShardedDO(orgId: string, timestamp: Date): string {
  const bucket = timestamp.toISOString().slice(0, 13) // YYYY-MM-DDTHH
  return `${orgId}:events:${bucket}`
  // e.g. org_abc:events:2024-01-15T14
}

// For queries spanning multiple time ranges, scatter-gather across shards:
async function queryTimeRange(orgId: string, start: Date, end: Date) {
  const shards = getTimeShardsBetween(orgId, start, end)
  const results = await Promise.all(
    shards.map(shard => env.DATABASE.get(env.DATABASE.idFromName(shard)).fetch(query))
  )
  return mergeResults(results)
}
```

**When to use:**
- Event streams exceeding 100K events per DO
- Analytics data with time-series access patterns
- CDC event logs that need periodic compaction/archival
- Compliance requirements for time-bounded data retention

**Trade-offs:**
- (+) Bounded growth per shard -- old shards can be archived
- (+) Time-range queries are efficient (only hit relevant shards)
- (+) Old shards become read-only (no write contention)
- (-) Cross-time queries require scatter-gather
- (-) Clock skew can cause routing errors in distributed systems
- (-) More complex query planning

### 4. Hash-Based Sharding (Consistent Hashing)

Distribute entities evenly across N shards using FNV-1a hashing. Provided by `ShardRouter`.

```typescript
import { ShardRouter, getShardIndex, fnv1aHash } from '@dotdo/do'

const router = new ShardRouter({
  defaultShardCount: 16,
  entityShards: {
    contacts: 32,  // 32 shards for contacts
    deals: 64,     // 64 shards for high-volume deals
  },
})

// Always routes 'contact_abc' to the same shard
const result = router.route({
  namespace: 'org_123',
  entityType: 'contacts',
  entityId: 'contact_abc',
  path: '/contacts/contact_abc',
})
// result.doName = 'org_123:contacts:shard-14'
// result.shardIndex = 14

// Low-level: get shard index directly
const idx = getShardIndex('contact_abc', 32)  // 0-31, deterministic
```

**Properties of FNV-1a consistent hashing:**
- **Deterministic**: Same key always maps to same shard
- **Uniform distribution**: Keys spread evenly (verified by tests with 1000+ keys)
- **Fast**: O(n) where n = key length, no crypto overhead

**When to use:**
- High-cardinality entity types with uniform access patterns
- Stateless routing (no registry needed -- hash determines shard)
- Write parallelism needed across many entities

**Trade-offs:**
- (+) No coordination needed -- any worker can compute the shard
- (+) Even distribution prevents hot spots
- (-) Adding/removing shards requires rehashing (affects ~(1/N) of keys)
- (-) Range queries impossible (must scatter to all shards)
- (-) Related entities may land on different shards

---

## When to Shard

### Decision Triggers

Sharding adds complexity. Only shard when you hit concrete limits. Monitor these metrics:

| Metric | Threshold | Action |
|--------|-----------|--------|
| **p99 read latency** | > 500 ms | Investigate cache hit rate. If cache misses dominate, shard by type to reduce working set per DO. |
| **p99 write latency** | > 200 ms | Approaching DO write throughput limit. Shard by type or hash. |
| **Sustained write throughput** | > 30 writes/sec | Hard DO limit. Must shard writes across multiple DOs. |
| **DO memory usage** | > 100 MB (80% of 128 MB) | LRU cache pressure. Reduce `maxCacheSize` or shard by type. |
| **DO storage** | > 8 GB (80% of 10 GB) | Approaching storage limit. Shard by type or archive old data. |
| **Entity count per DO** | > 100K | LRU cache pressure (`DEFAULT_MAX_ENTITIES = 10000`). Frequent evictions cause R2 reads. |
| **Event log size** | > 50K events | Compaction latency increases. Consider time-based sharding for events. |
| **SQLite WAL size** | > 100 MB | CDC event backlog growing. Flush to R2 or shard event streams. |
| **Error rate** | > 5% | Health degradation. Use `ShardHealthMonitor` to route around unhealthy DOs. |

### Decision Matrix

```
                    Low Entity Count (<10K)    High Entity Count (>100K)
                   +-------------------------+-------------------------+
  Low Writes       | No sharding needed.     | Type-based sharding.    |
  (<5 writes/sec)  | Single DO per tenant.   | Separate hot types.     |
                   +-------------------------+-------------------------+
  High Writes      | Hash-based sharding.    | Type + Hash sharding.   |
  (>30 writes/sec) | Distribute writes       | Both axes needed.       |
                   | across N DOs.           | Consider time sharding  |
                   |                         | for event streams.      |
                   +-------------------------+-------------------------+
```

### When NOT to Shard

- **Startup phase** (< 1K entities per tenant): Overhead of sharding outweighs benefit
- **Read-heavy workloads**: Reads go through QueryExecutor directly to R2, bypassing the DO
- **Uniform small tenants**: Each tenant already has its own DO
- **Low write volume** (< 5 writes/sec sustained): Single DO handles this easily

---

## Capacity Planning

### Per-DO Capacity Estimates

| Metric | Conservative | Typical | Maximum |
|--------|-------------|---------|---------|
| **Entities** | 10K | 50K | 200K |
| **Entity avg size** | 500 B | 2 KB | 10 KB |
| **Total entity storage** | 5 MB | 100 MB | 2 GB |
| **Relationships** | 20K | 100K | 500K |
| **Events (before compaction)** | 10K | 50K | 200K |
| **In-memory cache** | 10K entities | 50K entities | 100K entities |
| **SQLite WAL** | 1 MB | 10 MB | 100 MB |
| **R2 Parquet files** | 10 MB | 500 MB | 5 GB |

### Sizing Formula

To estimate how many DOs you need:

```
required_DOs = max(
  ceil(total_entities / 100K),           // Entity count limit
  ceil(total_storage_gb / 8),            // Storage limit (80% of 10GB)
  ceil(peak_writes_per_sec / 25),        // Write throughput (80% of 30/sec)
  ceil(total_memory_mb / 100),           // Memory limit (80% of 128MB)
)
```

**Example: A CRM system with 500K contacts, 100K deals, 10 writes/sec:**

```
By entity count:  ceil(600K / 100K) = 6 DOs
By storage:       ceil(1.2GB / 8) = 1 DO
By write rate:    ceil(10 / 25) = 1 DO
By memory:        ceil(60MB / 100) = 1 DO

Result: 6 DOs needed (entity count is the bottleneck)
Recommendation: Hash-shard contacts across 8 DOs (power of 2), keep deals in 1 DO
```

### Growth Projections

Plan for 10x growth over 12 months. If you need 6 DOs today, provision for 64 shard capacity:

```typescript
const router = new ShardRouter({
  defaultShardCount: 1,
  entityShards: {
    contacts: 8,    // Current need: 6, provisioned: 8 (power of 2)
    deals: 1,       // Current: well under limits
    events: 16,     // Anticipating high event volume
  },
})
```

### Cloudflare Cost Model

| Resource | Cost | Notes |
|----------|------|-------|
| **DO requests** | $0.15 / million | Each fetch to a DO counts |
| **DO duration** | $12.50 / million GB-s | Wall clock time the DO is active |
| **DO storage (SQLite)** | $0.20 / GB-month | Per-DO SQLite storage |
| **R2 storage** | $0.015 / GB-month | Parquet files |
| **R2 Class A ops** | $4.50 / million | PUT, POST, LIST |
| **R2 Class B ops** | $0.36 / million | GET, HEAD |
| **R2 egress** | Free | No egress charges |

**Sharding increases DO request count** (more DOs = more fetch calls) but reduces per-DO resource pressure. For most workloads, the cost increase is negligible compared to the reliability improvement.

---

## Migration Patterns

### Pattern 1: Split an Overloaded DO

When a single DO exceeds capacity, split it into N shards.

**Prerequisites:**
- Identify the bottleneck metric (entity count, write rate, storage)
- Choose shard count (power of 2 recommended)
- Plan for a maintenance window or gradual migration

**Steps:**

```
Phase 1: Deploy dual-write
  1. Deploy new ShardRouter configuration
  2. New writes go to new shard based on hash
  3. Reads check new shard first, fall back to old DO

Phase 2: Backfill
  1. List all entities in old DO
  2. For each entity:
     a. Compute target shard via ShardRouter
     b. Copy entity to target shard
     c. Track progress via MigrationTask
  3. Batch size: 100 entities per iteration (stay under subrequest limit)

Phase 3: Cutover
  1. Verify all entities migrated (compare counts)
  2. Switch reads to new shards only
  3. Set old DO to 'draining' status
  4. After confirmation period (24h), decommission old DO

Phase 4: Cleanup
  1. Delete old DO data
  2. Remove dual-write code path
```

**Implementation using ShardRebalancer:**

```typescript
import { ShardRebalancer, LoadMetricsStore, ShardHealthMonitor } from '@dotdo/do'

const rebalancer = new ShardRebalancer({
  minShardCount: 1,
  maxShardCount: 64,
  splitThreshold: 2.0,  // Split if load > 200% of average
  mergeThreshold: 0.1,  // Merge if load < 10% of average
  cooldownMs: 60000,    // 1 minute between operations
})

// Register current shards
rebalancer.registerShard({
  shardId: 'org_abc:contacts:shard-0',
  namespace: 'org_abc',
  entityType: 'contacts',
  shardIndex: 0,
  status: 'active',
})

// Analyze and get recommendations
const decisions = rebalancer.analyze(metricsStore, healthMonitor)

for (const decision of decisions) {
  if (decision.action.type === 'split') {
    // Execute split migration
    const task = rebalancer.startMigration(
      decision.action.sourceShardId,
      `${decision.action.sourceShardId}-new`,
      keysToMigrate
    )
    // ... batch migration logic ...
    rebalancer.completeMigration(task.id)
  }
}
```

### Pattern 2: Rebalance Hot Shards

When hash distribution creates uneven load (hot keys, temporal patterns):

```
1. Identify hot shard via ShardHealthMonitor
   - p99 latency > 500ms
   - Storage > 70% capacity
   - Error rate > 5%

2. Determine cause:
   - Hot key? -> Use LoadBalancedRouter for writes, consistent hash for reads
   - Temporal pattern? -> Switch to time-based sharding for that entity type
   - Data skew? -> Increase shard count (double it)

3. Execute migration:
   - Set hot shard to 'draining'
   - Redirect new writes to alternative shards
   - Backfill existing data using batch migration
   - Complete cutover when source is drained
```

### Pattern 3: Data Consistency During Migration

Migrations must handle in-flight requests without data loss.

**Dual-write approach:**

```typescript
async function writeWithMigration(
  entity: Entity,
  oldShard: DurableObjectStub,
  newShard: DurableObjectStub,
  migrationPhase: 'dual-write' | 'new-only'
): Promise<void> {
  if (migrationPhase === 'dual-write') {
    // Write to both, new shard is source of truth
    await newShard.fetch('/entity', { method: 'POST', body: JSON.stringify(entity) })
    try {
      await oldShard.fetch('/entity', { method: 'POST', body: JSON.stringify(entity) })
    } catch {
      // Old shard write failure is acceptable during migration
    }
  } else {
    await newShard.fetch('/entity', { method: 'POST', body: JSON.stringify(entity) })
  }
}
```

**Read fallback approach:**

```typescript
async function readWithFallback(
  entityId: string,
  primaryShard: DurableObjectStub,
  fallbackShard: DurableObjectStub,
): Promise<Entity | null> {
  const primary = await primaryShard.fetch(`/entity/${entityId}`)
  if (primary.ok) return primary.json()

  // Entity not yet migrated -- read from old shard
  const fallback = await fallbackShard.fetch(`/entity/${entityId}`)
  if (fallback.ok) return fallback.json()

  return null
}
```

### Pattern 4: Zero-Downtime Shard Expansion

Expand from N to 2N shards without downtime:

```
Current: 4 shards (shard-0 through shard-3)
Target:  8 shards (shard-0 through shard-7)

1. Deploy new router config with 8 shards
2. New entities route to their correct shard (0-7)
3. Existing entities still hash to 0-3 (superset of new mapping)

Key insight: getShardIndex(key, 4) is NOT guaranteed to equal
getShardIndex(key, 8) % 4. FNV-1a hash % N changes when N changes.

Therefore you MUST migrate:
4. For each entity in shards 0-3:
   a. Compute new shard = getShardIndex(entityId, 8)
   b. If new shard != old shard, migrate
5. Expected migration: ~50% of entities move

To reduce migration churn, consider virtual shards:
- Always hash to 256 virtual shards
- Map virtual shards to physical DOs
- Expansion = reassign virtual shards to new physical DOs
- Only ~(1/N) of entities need to move
```

---

## ParqueDB-Specific Concerns

### Event Log Compaction

ParqueDB stores all mutations as events. The event log grows unbounded without compaction.

**Compaction thresholds** (from `parquedb/src/constants.ts`):

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_COMPACTION_MIN_EVENTS` | 10,000 | Minimum events before compaction triggers |
| `DEFAULT_AUTO_COMPACTION_EVENT_THRESHOLD` | 10,000 | Auto-compaction event count trigger |
| `DEFAULT_AUTO_COMPACTION_BATCH_FILE_THRESHOLD` | 100 | Auto-compaction batch file count trigger |
| `DEFAULT_AUTO_COMPACTION_INTERVAL_MS` | 60,000 | Minimum interval between auto-compaction runs |
| `DEFAULT_AUTO_COMPACTION_ON_STARTUP` | false | Auto-compact on startup (can be slow) |
| `DEFAULT_COMPACTION_MAX_SEGMENT_AGE` | '24h' | Maximum segment age before compaction |
| `DEFAULT_COMPACTION_RETENTION` | '30d' | Retention period for compacted data |

**WAL compaction thresholds:**

| Constant | Default | Description |
|----------|---------|-------------|
| `WAL_COMPACTION_MIN_BATCHES` | 10 | Minimum batches before WAL compaction |
| `WAL_COMPACTION_TARGET_EVENTS` | 1,000 | Target events per compacted batch |
| `WAL_MAX_BLOB_SIZE` | 256 KB | Maximum blob size before splitting |
| `WAL_FLUSHED_RETENTION_MS` | 7 days | Retention for flushed WAL batches |

**Compaction best practices:**
- Enable `DEFAULT_AUTO_COMPACTION_ENABLED` (default: true)
- Set `DEFAULT_AUTO_COMPACTION_ON_STARTUP` to true for small datasets (< 10K events)
- For large datasets, run compaction during off-peak hours via DO alarm
- Monitor compaction duration -- if it exceeds 30 seconds, shard the event stream

### Parquet File Size vs Query Latency

ParqueDB writes data to Parquet files in R2. File size directly affects query latency.

**File structure and constants:**

| Constant | Default | Impact |
|----------|---------|--------|
| `DEFAULT_ROW_GROUP_SIZE` | 10,000 rows | Larger = fewer row groups = fewer R2 reads, but more memory per read |
| `DEFAULT_PARQUET_PAGE_SIZE` | 1 MB | Page size within row groups |
| `DEFAULT_COMPRESSION` | 'none' | Uncompressed -- prioritizes CPU over storage on Workers |
| `DEFAULT_ENABLE_COLUMN_INDEX` | true | Min/max stats per page for predicate pushdown |
| `DEFAULT_ENABLE_OFFSET_INDEX` | true | Page location indexes for efficient skipping |

**Sizing guidelines:**

| Entity Count | Avg Entity Size | data.parquet Size | Read Latency (p50) | Read Latency (p99) |
|-------------|----------------|-------------------|--------------------|--------------------|
| 1K | 1 KB | ~1 MB | 5 ms | 20 ms |
| 10K | 1 KB | ~10 MB | 20 ms | 100 ms |
| 50K | 1 KB | ~50 MB | 50 ms | 200 ms |
| 100K | 1 KB | ~100 MB | 100 ms | 500 ms |
| 100K | 5 KB | ~500 MB | 200 ms | 1,000 ms |

**Optimization strategies:**
- Use column indexes (`DEFAULT_ENABLE_COLUMN_INDEX = true`) for predicate pushdown
- Keep Parquet files under 100 MB for sub-second queries
- If a single entity type exceeds 100 MB, shard by type
- Use `DEFAULT_COMPRESSION = 'snappy'` if storage cost matters more than latency

### In-Memory Cache Pressure

ParqueDB uses an LRU cache to avoid repeated R2 reads.

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_ENTITY_CACHE_SIZE` | 1,000 | Entity cache entries per DO |
| `DEFAULT_GLOBAL_STORAGE_MAX_ENTITIES_PER_NS` | 10,000 | Max entities per namespace in global storage |
| `DEFAULT_GLOBAL_STORAGE_MAX_RELS_PER_NS` | 50,000 | Max relationships per namespace |
| `DEFAULT_ROW_GROUP_CACHE_MAX_BYTES` | 100 MB | Max row group cache size |
| `DEFAULT_ROW_GROUP_CACHE_MAX_ENTRIES` | 50 | Max row group cache entries |

**Cache pressure indicators:**
- High R2 read rate despite stable data (cache misses)
- DO memory approaching 128 MB
- p99 latency spikes on reads

**Mitigation:**
1. Increase `maxCacheSize` up to memory limits
2. Shard by entity type to reduce working set per DO
3. Use secondary indexes to avoid full scans

### Bulk Operation Gotchas

From project memory:
- **OOM with `insertMany`**: `Promise.all(inputs.map(create))` overwhelms heap. Use sequential `for (const r of records) await col.create(r)`.
- **Use bulk operations**: `db.beginBulkOperation()` before bulk writes, `db.endBulkOperation(true)` after. Skips per-write Parquet rewrites. 10-50x faster.
- **Event buffering**: Set `maxBufferedEvents: 5000` (default 1 = immediate flush). Reduces flush overhead.
- **Migration batch size**: `DEFAULT_MIGRATION_BATCH_SIZE = 400`. Stays under the 1,000 subrequest limit (read + write = 2 per entity).

---

## R2 Storage Costs and Access Patterns

### Cost Model

| Operation | Cost | Budget for 1M entities |
|-----------|------|----------------------|
| **Storage** | $0.015/GB-month | 1M x 2KB = 2GB = $0.03/month |
| **PUT (write)** | $4.50/million | 1M writes = $4.50 |
| **GET (read)** | $0.36/million | 10M reads = $3.60 |
| **LIST** | $4.50/million | Rare in normal operations |
| **Egress** | Free | No egress charges |

### Access Patterns

**Write path:**
1. Entity mutation arrives at DO
2. DO writes to in-memory state + SQLite WAL
3. Periodically flushes to R2 as Parquet files

**Read path (hot):**
1. Query arrives at Worker
2. QueryExecutor checks row group cache
3. Cache hit -> return immediately (no R2 access)

**Read path (cold):**
1. Query arrives at Worker
2. QueryExecutor reads Parquet metadata from R2 (1 GET)
3. Reads relevant row groups from R2 (1-N GETs depending on predicate pushdown)
4. Caches decoded row groups

**Compaction path:**
1. Read all event batch files (N GETs)
2. Merge and deduplicate
3. Write compacted files (M PUTs, M << N)
4. Delete old batch files (N DELETEs)

### R2 Optimization Strategies

1. **Minimize file count**: Fewer, larger Parquet files = fewer R2 operations
2. **Use event buffering**: Buffer events before flushing to reduce PUT frequency
3. **Enable column indexes**: Reduces row groups read per query
4. **Cache aggressively**: Row group cache (`DEFAULT_ROW_GROUP_CACHE_MAX_BYTES = 100MB`) amortizes R2 reads
5. **Batch R2 operations**: Stay under `WORKERS_PAID_SUBREQUEST_LIMIT = 1000` per invocation

---

## Monitoring and Alerting

### Metrics to Track

Use `ShardHealthMonitor` and `LoadMetricsStore` from `@dotdo/do`:

```typescript
import {
  ShardHealthMonitor,
  LoadMetricsStore,
  HealthAwareRouter,
} from '@dotdo/do'

const healthMonitor = new ShardHealthMonitor({
  latencyDegradedThreshold: 500,    // 500ms -> degraded
  latencyUnhealthyThreshold: 2000,  // 2s -> unhealthy
  errorRateDegradedThreshold: 0.05, // 5% -> degraded
  errorRateUnhealthyThreshold: 0.2, // 20% -> unhealthy
  storageDegradedThreshold: 0.7,    // 70% -> degraded
  storageUnhealthyThreshold: 0.9,   // 90% -> unhealthy
})
```

### Alert Thresholds

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| **Shard Unhealthy** | `healthScore < 30` or `status === 'unhealthy'` | Critical | Page on-call. Route traffic away. Investigate root cause. |
| **Shard Degraded** | `healthScore < 60` or `status === 'degraded'` | Warning | Investigate within 1 hour. May need rebalancing. |
| **Storage > 80%** | `storagePercent > 0.8` | Warning | Plan shard expansion or data archival. |
| **Storage > 90%** | `storagePercent > 0.9` | Critical | Immediate action needed. Archive or shard. |
| **Write Latency Spike** | `p99 > 500ms` sustained 5 min | Warning | Check DO write throughput. Consider sharding. |
| **Event Log Backlog** | `> 50K uncompacted events` | Warning | Trigger manual compaction or increase auto-compaction frequency. |
| **Migration Stalled** | `MigrationTask.status === 'in_progress'` for > 1 hour | Warning | Check migration task for errors. May need restart. |

### Health Dashboard Endpoint

```typescript
app.get('/admin/shards/health', async (c) => {
  const allMetrics = healthMonitor.getAllHealthMetrics()
  const summary = {
    total: allMetrics.size,
    healthy: 0,
    degraded: 0,
    unhealthy: 0,
    shards: [] as ShardHealthMetrics[],
  }

  for (const metrics of allMetrics.values()) {
    summary.shards.push(metrics)
    summary[metrics.status]++
  }

  return c.json(summary)
})
```

---

## Runbooks

### Runbook 1: DO Memory Exceeded (OOM)

**Symptom:** DO restarts unexpectedly. Logs show "Exceeded memory limit".

**Root cause:** In-memory entity cache or event buffer exceeded 128 MB.

**Resolution:**
1. Reduce `maxCacheSize` to lower entity count in memory
2. Reduce `maxBufferedEvents` to flush events more frequently
3. If neither helps, shard by entity type to reduce per-DO working set
4. Check for memory leaks (e.g., unbounded arrays in event handlers)

### Runbook 2: Write Throughput Saturated

**Symptom:** Write latency > 500ms sustained. Write queue backlog growing.

**Root cause:** Single DO limited to ~30 durable writes/sec.

**Resolution:**
1. Identify the hot entity type (`LoadMetricsStore.getShardLoads()`)
2. Deploy hash-based sharding for that entity type
3. Start with 4-8 shards (power of 2)
4. Monitor until latency normalizes

### Runbook 3: Shard Imbalance

**Symptom:** One shard shows 3x+ load compared to average.

**Root cause:** Hot key, temporal pattern, or data skew.

**Resolution:**
1. Run `ShardRebalancer.analyze()` to get recommendations
2. If `split` recommended: increase shard count for the affected entity type
3. If `drain` recommended: migrate data from hot shard to others
4. Execute migration using Pattern 1 (Split an Overloaded DO)
5. Record rebalance: `rebalancer.recordRebalance()` (respects cooldown)

### Runbook 4: Storage Approaching Limit

**Symptom:** `storagePercent > 0.8` alert fires.

**Root cause:** Entity data + event log + SQLite WAL approaching 10 GB.

**Resolution:**
1. Run event log compaction: reduces event storage by removing superseded events
2. Archive old events to R2 (time-based sharding for events)
3. If entity data itself is large, shard by entity type
4. Check for orphaned data (deleted entities still in storage)

### Runbook 5: Migration Failure Recovery

**Symptom:** `MigrationTask.status === 'failed'` with error.

**Resolution:**
1. Check error message: `rebalancer.getMigration(taskId)?.error`
2. Common errors:
   - "Network timeout" -> Retry with smaller batch size
   - "Storage limit" -> Target shard is full, choose a different target
   - "Conflict" -> Entity was modified during migration, re-read and retry
3. Resume migration from last successful key:
   ```typescript
   const task = rebalancer.getMigration(taskId)
   const remainingKeys = task.keys.slice(task.keysMigrated)
   const newTask = rebalancer.startMigration(
     task.sourceShardId,
     task.targetShardId,
     remainingKeys
   )
   ```
4. If unrecoverable, restore source shard to `active` status

---

## References

- [`@dotdo/do/shard.ts`](../../dotdo/do/shard.ts) -- ShardRouter, LoadBalancedRouter, HealthAwareRouter, ShardRebalancer
- [`@dotdo/do/storage-limits.ts`](../../dotdo/do/storage-limits.ts) -- CF DO limit constants and StorageGuard
- [`.do/dotdo/docs/SHARDING.md`](../../dotdo/docs/SHARDING.md) -- API reference and usage patterns
- [`apps/db/src/lib/do.ts`](../../../apps/db/src/lib/do.ts) -- Current tenant+system DO routing
- [`apps/db/src/do/database.ts`](../../../apps/db/src/do/database.ts) -- DatabaseDO implementation
- [`parquedb/src/constants.ts`](../parquedb/src/constants.ts) -- ParqueDB operational constants
- [Cloudflare DO Limits](https://developers.cloudflare.com/durable-objects/platform/limits/) -- Official documentation
- [Cloudflare R2 Pricing](https://developers.cloudflare.com/r2/pricing/) -- Storage cost reference
