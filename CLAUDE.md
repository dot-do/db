# @dotdo/db

DB Durable Object + ClickHouse client for the .do ecosystem.

## Commands

```bash
pnpm build               # tsc
pnpm ch "SELECT 1"       # Query ClickHouse Cloud
pnpm typecheck           # tsc --noEmit
```

## ClickHouse CLI

```bash
ch "SELECT 1"            # Query ClickHouse Cloud (global command)
ch "SHOW TABLES"         # DDL auto-detected, uses exec() instead of query()
ch "SELECT ..." dbname   # Query a specific database
```

- Script: `scripts/ch.ts` — loads credentials via dotenv, uses `ClickHouseClient`
- Credentials: `.env` (`CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`)
- **NEVER read, cat, or log `.env`** — the `ch` script loads it opaquely
- Host: `jr0jywmccu.us-east-1.aws.clickhouse.cloud:8443`

## ClickHouse Databases

| Database | Contents |
|----------|----------|
| `platform` | Core tables: events, actions, data, versions, traces, logs, sessions, observations, scores, rels, artifacts, search, metrics_daily, migrations. S3Queue: `queue` |
| `streams` | All materialized views: ingest, actions, data, logs, traces, sessions, observations, scores, rels, versions, metrics_daily |
| `startups` | ICP/segmentation data (startups, segments, relationships) |
| `src` | CommonCrawl/Source data (~34 GiB) |
| `otel` | OpenTelemetry traces |

## Schema Changes — CRITICAL

**NEVER run DDL (CREATE/ALTER/DROP TABLE) manually via `ch` CLI or inline scripts.**

All schema changes MUST go through the migration system:

```bash
# Run migrations against a database
npx tsx scripts/migrate.ts platform      # platform database (default)
npx tsx scripts/migrate.ts headlessly    # headlessly database

# S3Queue management
npx tsx scripts/setup-s3queue.ts create  # create S3Queue + ingest MV
npx tsx scripts/setup-s3queue.ts status  # check processing status

# Recreate materialized views
npx tsx scripts/recreate-mvs.ts          # create (IF NOT EXISTS)
npx tsx scripts/recreate-mvs.ts --force  # drop + recreate
```

### Migration files

- Location: `src/clickhouse/migrations/*.ts`
- Use `{database}` placeholder — replaced at runtime with target database
- Versioned: `0001_events.ts`, `0002_actions.ts`, etc.
- Tracked in `{database}.migrations` table (ReplacingMergeTree)
- Runner: `src/clickhouse/migrate.ts` — skips already-applied versions

### Adding new migrations

1. Create `src/clickhouse/migrations/NNNN_description.ts`
2. Export `migration` with `version`, `name`, `up`, `down` fields
3. Use `{database}` in SQL (e.g., `CREATE TABLE IF NOT EXISTS {database}.mytable`)
4. Import and add to `src/clickhouse/migrations/index.ts`
5. Run: `npx tsx scripts/migrate.ts platform`

## Package Exports

```typescript
import { ... } from '@dotdo/db'              // Main: ClickHouse client + DB DO + utilities
import { ... } from '@dotdo/db/clickhouse'   // ClickHouseClient, migrate, types
import { ... } from '@dotdo/db/do'           // DB Durable Object (workerd)
```

## ClickHouse Client (`src/clickhouse/`)

- `client.ts` — `ClickHouseClient` with `query<T>()`, `exec()`, `ping()`, database support
- `migrate.ts` — Migration runner with `migrations` table tracking
- `test-client.ts` — `createTestClient()` factory (reads from env vars)
- `types.ts` — `ClickHouseConfig`, `QueryResult<T>`, `ExecResult`
- `migrations/` — SQL migration files

## Code Style

- No semicolons, single quotes, print width 160
- TypeScript strict mode, ESM
