#!/usr/bin/env npx tsx
import { config } from 'dotenv'
import { resolve } from 'path'
import { execSync } from 'child_process'

config({ path: resolve(import.meta.dirname, '../.env') })

// Default user like ch CLI does
if (!process.env.CLICKHOUSE_USER) process.env.CLICKHOUSE_USER = 'default'

const vitest = resolve(import.meta.dirname, '../../../apps/clickhouse/node_modules/.bin/vitest')
const args = process.argv.slice(2).join(' ') || 'run src/clickhouse/migrations/migrations.test.ts'

try {
  execSync(`${vitest} ${args}`, {
    cwd: resolve(import.meta.dirname, '..'),
    stdio: 'inherit',
    env: process.env,
  })
} catch (e) {
  process.exit(1)
}
