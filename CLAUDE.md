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
| `default` | Unified platform schema (clean, ready for new tables) |
| `startups` | ICP/segmentation data (startups, segments, relationships, company_type_swot) |
| `src` | CommonCrawl/Source data (~34 GiB) |
| `otel` | OpenTelemetry traces |
| `headlessly` | Legacy entity data |

## Package Exports

```typescript
import { ... } from '@dotdo/db'              // Main: ClickHouse client + DB DO + utilities
import { ... } from '@dotdo/db/clickhouse'   // ClickHouseClient, migrate, types
import { ... } from '@dotdo/db/do'           // DB Durable Object (workerd)
```

## ClickHouse Client (`src/clickhouse/`)

- `client.ts` — `ClickHouseClient` with `query<T>()`, `exec()`, `ping()`, database support
- `migrate.ts` — Migration runner with `schema_migrations` tracking
- `test-client.ts` — `createTestClient()` factory (reads from env vars)
- `types.ts` — `ClickHouseConfig`, `QueryResult<T>`, `ExecResult`
- `migrations/` — SQL migration files

## Code Style

- No semicolons, single quotes, print width 160
- TypeScript strict mode, ESM
