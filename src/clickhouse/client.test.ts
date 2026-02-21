import { describe, it, expect, afterAll } from 'vitest'
import { ClickHouseClient } from './client'
import { createTestClient } from './test-client'

const client = createTestClient()

const TEST_DB = 'platform_test_client'

afterAll(async () => {
  await client.exec(`DROP DATABASE IF EXISTS ${TEST_DB}`)
})

describe('ClickHouseClient', () => {
  it('ping() returns true', async () => {
    const ok = await client.ping()
    expect(ok).toBe(true)
  })

  it('exec() creates a test database', async () => {
    const result = await client.exec(`CREATE DATABASE IF NOT EXISTS ${TEST_DB}`)
    expect(result.ok).toBe(true)
  })

  it('exec() creates a table in the test database', async () => {
    const result = await client.exec(
      `CREATE TABLE IF NOT EXISTS test_events (
        id UInt64,
        name String,
        timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY id`,
      undefined,
      TEST_DB,
    )
    expect(result.ok).toBe(true)
  })

  it('query() selects from system tables', async () => {
    const result = await client.query<{ name: string }>('SELECT name FROM system.databases LIMIT 5')
    expect(result.rows).toBeGreaterThan(0)
    expect(result.data.length).toBeGreaterThan(0)
    expect(result.data[0]).toHaveProperty('name')
  })

  it('exec() with database override works', async () => {
    const result = await client.exec("INSERT INTO test_events (id, name) VALUES (1, 'hello')", undefined, TEST_DB)
    expect(result.ok).toBe(true)

    const clientWithDb = createTestClient(TEST_DB)

    // Brief wait for ClickHouse Cloud cross-replica consistency
    await new Promise((r) => setTimeout(r, 500))

    const queryResult = await clientWithDb.query<{ id: number; name: string }>('SELECT id, name FROM test_events WHERE id = 1')
    expect(queryResult.rows).toBe(1)
    expect(queryResult.data[0].name).toBe('hello')
  })

  it('throws on invalid SQL with ClickHouse error text', async () => {
    await expect(client.query('SELEKT invalid')).rejects.toThrow(/ClickHouse error/)
  })

  it('query() supports parameter substitution', async () => {
    const result = await client.query<{ result: number }>('SELECT {val:UInt64} AS result', { val: 42 })
    expect(result.rows).toBe(1)
    expect(result.data[0].result).toBe(42)
  })
})
