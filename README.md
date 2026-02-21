# @dotdo/db v0.2.0

Database infrastructure for the .do ecosystem. ClickHouse HTTP client for analytics queries and migrations, DB Durable Object for WAL + CDC event logging, Cloudflare Pipeline for zero-cost ingestion.

## Architecture

```
@dotdo/db (single package, three export paths)
  ├─ @dotdo/db              — ClickHouseClient + DB DO + utilities
  ├─ @dotdo/db/clickhouse   — ClickHouseClient, migrate(), types
  └─ @dotdo/db/do           — DB Durable Object (workerd entry)

DB (Durable Object, raw SqlStorage)
  ├─ Entity storage (event-sourced)
  ├─ CDC event log → Pipeline → ClickHouse
  ├─ Write mutex + multi-tenancy
  ├─ Compaction via alarm()
  ├─ Time-travel (event reconstruction)
  └─ Purge policy (TTL-based expiry)

ClickHouse HTTP Client (src/clickhouse/)
  ├─ HTTP client (query, exec, ping)
  ├─ Versioned migrations (0001-0012)
  └─ Database-per-tenant support

Pipeline (headlessly_events)
  → R2 (batched NDJSON)
  → S3Queue (min poll interval)
  → ClickHouse (compound partitions, native JSON)
```

## Exports

```typescript
import { ClickHouseClient, migrate, DB } from '@dotdo/db'            // Main entry
import { ClickHouseClient, migrate } from '@dotdo/db/clickhouse'     // ClickHouse only
import { DB } from '@dotdo/db/do'                                    // DB Durable Object (workerd)
```

## ClickHouse Schema

Single `events` table with compound partitions `(category, toYYYYMM(ts))`:

| Category | Types | TTL |
|----------|-------|-----|
| `browser` | pageview, click, vitals, error, console, impression | 90 days |
| `otel` | otel.span, otel.log | 30 days |
| `recording` | rrweb session recordings | 7 days |
| `business` | hypothesis, experiment, decision, conversion, CDC mutations | forever |

Native JSON column (`data JSON`) with dot-access queries. MV-backed aggregation tables: `rewards`, `sessions`, `recordings`.

## Commands

```bash
pnpm build               # tsc
pnpm typecheck           # tsc --noEmit
pnpm ch "SELECT 1"       # Query ClickHouse Cloud via CLI
```

## Key Files

| File | Purpose |
|------|---------|
| `src/index.ts` | Main entry point (re-exports everything) |
| `src/do.ts` | DB Durable Object (entity storage + CDC) |
| `src/clickhouse/client.ts` | ClickHouse HTTP client |
| `src/clickhouse/migrate.ts` | Migration runner |
| `src/clickhouse/migrations/` | Versioned schema migrations (0001-0012) |
| `src/clickhouse/types.ts` | ClickHouseConfig, QueryResult, ExecResult |
| `src/lib/compaction.ts` | Event log compaction logic |
| `src/lib/time-travel.ts` | Event reconstruction (time-travel queries) |
| `src/lib/purge-policy.ts` | TTL-based entity purge |
| `src/lib/entity-types.ts` | Entity type validation |
| `src/lib/id.ts` | Entity ID generation (sqids) |
| `src/handlers/` | Event logger, forwarding, handler context |
