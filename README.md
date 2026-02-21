# .do/db

Database infrastructure for the .do ecosystem. DB Durable Object for WAL + CDC event logging, ClickHouse client for analytics backend, Cloudflare Pipeline for zero-cost ingestion.

## Architecture

```
DB (Durable Object, SQLite)
  ├─ Entity storage (event-sourced)
  ├─ CDC event log → Pipeline → ClickHouse
  ├─ Write mutex + multi-tenancy
  └─ Compaction via alarm()

@dotdo/clickhouse (packages/clickhouse)
  ├─ HTTP client (query, exec, ping)
  ├─ Versioned migrations (0001-0012)
  └─ BatchInserter for bulk writes

Pipeline (headlessly_events)
  → R2 (batched NDJSON)
  → S3Queue (min poll interval)
  → ClickHouse (compound partitions, native JSON)
```

## Packages

| Package | Path | Purpose |
|---------|------|---------|
| `@dotdo/db` | `.` | DB Durable Object + entity storage |
| `@dotdo/clickhouse` | `packages/clickhouse/` | ClickHouse HTTP client + migrations |

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
cd packages/clickhouse && pnpm test   # ClickHouse client tests
```

## Key Files

| File | Purpose |
|------|---------|
| `src/do.ts` | DB Durable Object (entity storage + CDC) |
| `packages/clickhouse/src/client.ts` | ClickHouse HTTP client |
| `packages/clickhouse/src/migrate.ts` | Migration runner |
| `packages/clickhouse/src/migrations/` | Versioned schema migrations |
