import { describe, it, expect, afterAll } from 'vitest'
import { createTestClient } from './test-client'
import { migrate, getMigrationStatus } from './migrate'
import type { Migration } from './migrate'

const client = createTestClient()

const TEST_DB = 'platform_test_migrate'

afterAll(async () => {
  await client.exec(`DROP DATABASE IF EXISTS ${TEST_DB}`)
})

describe('migrate', () => {
  it('creates test database', async () => {
    const result = await client.exec(`CREATE DATABASE IF NOT EXISTS ${TEST_DB}`)
    expect(result.ok).toBe(true)
  })

  it('applies migrations in order', async () => {
    const migrations: Migration[] = [
      {
        version: 1,
        name: 'create_users',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_users (
          id UInt64,
          name String
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_users`,
      },
      {
        version: 2,
        name: 'create_orders',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_orders (
          id UInt64,
          user_id UInt64,
          amount Float64
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_orders`,
      },
    ]

    const result = await migrate(client, TEST_DB, migrations)

    expect(result.applied).toEqual([1, 2])
    expect(result.skipped).toEqual([])
    expect(result.errors).toEqual([])
  })

  it('skips already-applied migrations', async () => {
    const migrations: Migration[] = [
      {
        version: 1,
        name: 'create_users',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_users (
          id UInt64,
          name String
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_users`,
      },
      {
        version: 2,
        name: 'create_orders',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_orders (
          id UInt64,
          user_id UInt64,
          amount Float64
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_orders`,
      },
      {
        version: 3,
        name: 'create_products',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_products (
          id UInt64,
          title String
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_products`,
      },
    ]

    const result = await migrate(client, TEST_DB, migrations)

    expect(result.applied).toEqual([3])
    expect(result.skipped).toEqual([1, 2])
    expect(result.errors).toEqual([])
  })

  it('getMigrationStatus() reports correct state', async () => {
    const status = await getMigrationStatus(client, TEST_DB)

    expect(status.applied).toEqual([1, 2, 3])
    expect(status.latestVersion).toBe(3)
  })

  it('handles multi-statement migrations', async () => {
    const migrations: Migration[] = [
      { version: 1, name: 'skip', up: '', down: '' },
      { version: 2, name: 'skip', up: '', down: '' },
      { version: 3, name: 'skip', up: '', down: '' },
      {
        version: 4,
        name: 'create_multi_tables',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_categories (
          id UInt64,
          name String
        ) ENGINE = MergeTree()
        ORDER BY id;
        CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_tags (
          id UInt64,
          label String
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_categories; DROP TABLE IF EXISTS ${TEST_DB}.test_tags`,
      },
    ]

    const result = await migrate(client, TEST_DB, migrations)

    expect(result.applied).toEqual([4])
    expect(result.skipped).toEqual([1, 2, 3])
    expect(result.errors).toEqual([])

    // Verify both tables exist
    const catResult = await client.query<{ name: string }>(`SELECT name FROM system.tables WHERE database = '${TEST_DB}' AND name = 'test_categories'`)
    expect(catResult.rows).toBe(1)

    const tagResult = await client.query<{ name: string }>(`SELECT name FROM system.tables WHERE database = '${TEST_DB}' AND name = 'test_tags'`)
    expect(tagResult.rows).toBe(1)
  })

  it('{database} placeholder replacement works', async () => {
    const migrations: Migration[] = [
      { version: 1, name: 'skip', up: '', down: '' },
      { version: 2, name: 'skip', up: '', down: '' },
      { version: 3, name: 'skip', up: '', down: '' },
      { version: 4, name: 'skip', up: '', down: '' },
      {
        version: 5,
        name: 'create_via_placeholder',
        up: `CREATE TABLE IF NOT EXISTS {database}.test_placeholder (
          id UInt64,
          value String
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: 'DROP TABLE IF EXISTS {database}.test_placeholder',
      },
    ]

    const result = await migrate(client, TEST_DB, migrations)

    expect(result.applied).toEqual([5])
    expect(result.errors).toEqual([])

    // Verify the table was created in the correct database
    const check = await client.query<{ name: string }>(`SELECT name FROM system.tables WHERE database = '${TEST_DB}' AND name = 'test_placeholder'`)
    expect(check.rows).toBe(1)
  })

  it('error in migration stops execution', async () => {
    const migrations: Migration[] = [
      { version: 1, name: 'skip', up: '', down: '' },
      { version: 2, name: 'skip', up: '', down: '' },
      { version: 3, name: 'skip', up: '', down: '' },
      { version: 4, name: 'skip', up: '', down: '' },
      { version: 5, name: 'skip', up: '', down: '' },
      {
        version: 6,
        name: 'bad_migration',
        up: 'SELEKT THIS IS INVALID SQL',
        down: '',
      },
      {
        version: 7,
        name: 'should_not_run',
        up: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.test_should_not_exist (
          id UInt64
        ) ENGINE = MergeTree()
        ORDER BY id`,
        down: `DROP TABLE IF EXISTS ${TEST_DB}.test_should_not_exist`,
      },
    ]

    const result = await migrate(client, TEST_DB, migrations)

    expect(result.skipped).toEqual([1, 2, 3, 4, 5])
    expect(result.errors.length).toBe(1)
    expect(result.errors[0].version).toBe(6)
    expect(result.errors[0].error).toMatch(/ClickHouse error/)
    expect(result.applied).toEqual([])

    // Verify migration 7 did NOT run
    const check = await client.query<{ name: string }>(
      `SELECT name FROM system.tables WHERE database = '${TEST_DB}' AND name = 'test_should_not_exist'`,
    )
    expect(check.rows).toBe(0)
  })
})
