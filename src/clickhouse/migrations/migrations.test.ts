import { describe, it, expect, afterAll } from 'vitest'
import { createTestClient } from '../test-client'
import { migrate } from '../migrate'
import { migrations } from './index'

const client = createTestClient()

const TEST_DB = 'platform_test_schema'

afterAll(async () => {
  await client.exec(`DROP DATABASE IF EXISTS ${TEST_DB}`)
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

describe('core table migrations', () => {
  it('creates test database', async () => {
    // Clean slate — longer sleep to let ZooKeeper release old table metadata
    await client.exec(`DROP DATABASE IF EXISTS ${TEST_DB}`)
    await sleep(3000)
    const result = await client.exec(`CREATE DATABASE IF NOT EXISTS ${TEST_DB}`)
    expect(result.ok).toBe(true)
    await sleep(1000)
  })

  it('applies all migrations with no errors', async () => {
    const result = await migrate(client, TEST_DB, migrations)

    expect(result.errors).toEqual([])
    expect(result.applied).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    expect(result.skipped).toEqual([])
  })

  it('events table — INSERT and SELECT', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000001',
        'test.do/~e2e',
        now64(3),
        'cdc',
        'Contact.created',
        'https://test.do/~e2e/Contact/con_1',
        'platform',
        'test',
        '{"type":"Contact","id":"con_1","name":"Alice","visibility":"public"}'
      )`,
    )

    const result = await client.query<{ id: string; event: string; ns: string }>(`SELECT id, event, ns FROM ${TEST_DB}.events FINAL WHERE id = '01TEST000001'`)

    expect(result.rows).toBe(1)
    expect(result.data[0].id).toBe('01TEST000001')
    expect(result.data[0].event).toBe('Contact.created')
    expect(result.data[0].ns).toBe('test.do/~e2e')
  })

  it('MV chain: events -> versions -> data', async () => {
    // Wait for MV propagation
    await sleep(2000)

    const versions = await client.query<{ type: string; id: string; name: string }>(
      `SELECT type, id, name FROM ${TEST_DB}.versions FINAL WHERE id = 'con_1' AND ns = 'test.do/~e2e'`,
    )
    expect(versions.rows).toBe(1)
    expect(versions.data[0].type).toBe('Contact')
    expect(versions.data[0].id).toBe('con_1')
    expect(versions.data[0].name).toBe('Alice')

    const data = await client.query<{ ns: string; type: string; id: string; name: string }>(
      `SELECT ns, type, id, name FROM ${TEST_DB}.data FINAL WHERE id = 'con_1' AND ns = 'test.do/~e2e'`,
    )
    expect(data.rows).toBe(1)
    expect(data.data[0].ns).toBe('test.do/~e2e')
    expect(data.data[0].type).toBe('Contact')
    expect(data.data[0].id).toBe('con_1')
    expect(data.data[0].name).toBe('Alice')
  })

  it('data keeps latest version via ReplacingMergeTree', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000002',
        'test.do/~e2e',
        now64(3),
        'cdc',
        'Contact.updated',
        'https://test.do/~e2e/Contact/con_1',
        'platform',
        'test',
        '{"type":"Contact","id":"con_1","name":"Alice Smith","visibility":"public"}'
      )`,
    )

    await sleep(2000)

    // data FINAL should show the latest name
    const data = await client.query<{ name: string }>(
      `SELECT name FROM ${TEST_DB}.data FINAL WHERE id = 'con_1' AND ns = 'test.do/~e2e' AND type = 'Contact'`,
    )
    expect(data.rows).toBe(1)
    expect(data.data[0].name).toBe('Alice Smith')

    // versions FINAL should have 2 rows (different e values)
    const versions = await client.query<{ name: string; e: string }>(
      `SELECT name, e FROM ${TEST_DB}.versions FINAL WHERE id = 'con_1' AND ns = 'test.do/~e2e' AND type = 'Contact' ORDER BY v`,
    )
    expect(versions.rows).toBe(2)
    expect(versions.data[0].name).toBe('Alice')
    expect(versions.data[1].name).toBe('Alice Smith')
  })

  it('actions lifecycle — CoalescingMergeTree merges by id', async () => {
    // Insert a pending action event
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000010',
        'test.do/~e2e',
        now64(3),
        'action',
        'action.pending',
        'https://test.do/~e2e/actions/act_1',
        'platform',
        'test',
        '{"actionId":"act_1","action":"Contact.create","status":"pending","input":{"name":"Bob"}}'
      )`,
    )

    // Insert a completed action event for the same actionId
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000011',
        'test.do/~e2e',
        now64(3),
        'action',
        'action.completed',
        'https://test.do/~e2e/actions/act_1',
        'platform',
        'test',
        '{"actionId":"act_1","action":"Contact.create","status":"completed","output":{"id":"con_2"},"duration":150}'
      )`,
    )

    await sleep(2000)

    const actions = await client.query<{ id: string; action: string; status: string; duration: string }>(
      `SELECT id, action, status, duration FROM ${TEST_DB}.actions FINAL WHERE id = 'act_1'`,
    )
    expect(actions.rows).toBe(1)
    expect(actions.data[0].id).toBe('act_1')
    expect(actions.data[0].action).toBe('Contact.create')
    expect(actions.data[0].status).toBe('completed')
    expect(Number(actions.data[0].duration)).toBe(150)
  })

  it('rels table — relationship events', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000020',
        'test.do/~e2e',
        now64(3),
        'rel',
        'rel.created',
        'https://test.do/~e2e/rels',
        'platform',
        'test',
        '{"from":"org_1","predicate":"hasContact","to":"con_1","reverse":"belongsTo","type":"Organization-Contact","meta":{}}'
      )`,
    )

    await sleep(2000)

    const rels = await client.query<{ from: string; predicate: string; to: string; reverse: string; type: string }>(
      `SELECT \`from\`, predicate, \`to\`, reverse, type FROM ${TEST_DB}.rels FINAL WHERE \`to\` = 'con_1' AND predicate = 'hasContact'`,
    )
    expect(rels.rows).toBe(1)
    expect(rels.data[0].from).toBe('org_1')
    expect(rels.data[0].predicate).toBe('hasContact')
    expect(rels.data[0].to).toBe('con_1')
    expect(rels.data[0].reverse).toBe('belongsTo')
    expect(rels.data[0].type).toBe('Organization-Contact')
  })

  it('search table — direct INSERT and FTS query', async () => {
    // Direct INSERT (no MV, this is how embedding worker populates it)
    await client.exec(`
      INSERT INTO ${TEST_DB}.search (url, ns, type, id, name, content, embedding, v)
      VALUES (
        'https://test.do/~e2e/Contact/con_1', 'test.do/~e2e', 'Contact', 'con_1',
        'Alice Smith', 'Alice Smith is the CEO of Acme Corp with expertise in artificial intelligence',
        [${Array(8).fill('0.1').join(',')}], ${Date.now()}
      )
    `)
    await sleep(500)

    const result = await client.query<{ name: string }>(
      `SELECT name FROM ${TEST_DB}.search FINAL WHERE ns = 'test.do/~e2e' AND hasToken(content, 'CEO')`,
    )
    expect(result.data.length).toBe(1)
    expect(result.data[0].name).toBe('Alice Smith')
  })

  it('artifacts table — multiple formats per entity', async () => {
    const url = 'https://test.do/~e2e/Content/page_1'
    const ns = 'test.do/~e2e'
    const v = Date.now()

    await client.exec(`
      INSERT INTO ${TEST_DB}.artifacts (url, format, ns, name, data, v, e)
      VALUES
        ('${url}', 'html', '${ns}', 'Test Page', '{"html":"<h1>Test</h1>"}', ${v}, '01TEST000050'),
        ('${url}', 'json', '${ns}', 'Test Page', '{"title":"Test Page"}', ${v}, '01TEST000050'),
        ('${url}', 'ast', '${ns}', 'Test Page', '{"type":"root","children":[]}', ${v}, '01TEST000050')
    `)
    await sleep(500)

    const result = await client.query<{ format: string }>(
      `SELECT format FROM ${TEST_DB}.artifacts FINAL WHERE ns = '${ns}' AND url = '${url}' ORDER BY format`,
    )
    expect(result.data.length).toBe(3)
    expect(result.data.map((r) => r.format)).toEqual(['ast', 'html', 'json'])
  })

  it('OTEL span materializes to traces', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000100',
        'test.do/~e2e',
        now64(3),
        'otel.span',
        'otel.span',
        '',
        'otel',
        'test',
        '{"traceId":"abc123trace","spanId":"span001","parentSpanId":"","spanName":"HTTP GET /api","spanKind":"SERVER","serviceName":"api-gateway","resourceAttributes":"{}","spanAttributes":"{}","duration":42,"statusCode":"OK","statusMessage":""}'
      )`,
    )

    await sleep(2000)

    const traces = await client.query<{ TraceId: string; SpanId: string; SpanName: string; ServiceName: string; Duration: string; StatusCode: string }>(
      `SELECT TraceId, SpanId, SpanName, ServiceName, Duration, StatusCode FROM ${TEST_DB}.traces WHERE TraceId = 'abc123trace'`,
    )
    expect(traces.rows).toBe(1)
    expect(traces.data[0].TraceId).toBe('abc123trace')
    expect(traces.data[0].SpanId).toBe('span001')
    expect(traces.data[0].SpanName).toBe('HTTP GET /api')
    expect(traces.data[0].ServiceName).toBe('api-gateway')
    expect(Number(traces.data[0].Duration)).toBe(42)
    expect(traces.data[0].StatusCode).toBe('OK')
  })

  it('OTEL log materializes to logs', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000110',
        'test.do/~e2e',
        now64(3),
        'otel.log',
        'otel.log',
        '',
        'otel',
        'test',
        '{"traceId":"abc123trace","spanId":"span001","severityText":"ERROR","severityNumber":17,"serviceName":"api-gateway","body":"Connection refused to upstream","resourceAttributes":"{}","logAttributes":"{}"}'
      )`,
    )

    await sleep(2000)

    const logs = await client.query<{ TraceId: string; SeverityText: string; ServiceName: string; Body: string; SeverityNumber: string }>(
      `SELECT TraceId, SeverityText, ServiceName, Body, SeverityNumber FROM ${TEST_DB}.logs WHERE TraceId = 'abc123trace'`,
    )
    expect(logs.rows).toBe(1)
    expect(logs.data[0].TraceId).toBe('abc123trace')
    expect(logs.data[0].SeverityText).toBe('ERROR')
    expect(logs.data[0].ServiceName).toBe('api-gateway')
    expect(logs.data[0].Body).toBe('Connection refused to upstream')
    expect(Number(logs.data[0].SeverityNumber)).toBe(17)
  })

  it('LLM generation materializes to observations', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000120',
        'test.do/~e2e',
        now64(3),
        'llm.generation',
        'llm.generation',
        '',
        'langfuse',
        'test',
        '{"id":"obs_001","traceId":"trace_llm_1","type":"generation","name":"chat-completion","model":"claude-3-opus","level":"DEFAULT","status":"complete","totalCost":0.015}'
      )`,
    )

    await sleep(2000)

    const obs = await client.query<{ id: string; trace_id: string; type: string; name: string; model: string; level: string }>(
      `SELECT id, trace_id, type, name, model, level FROM ${TEST_DB}.observations FINAL WHERE id = 'obs_001'`,
    )
    expect(obs.rows).toBe(1)
    expect(obs.data[0].id).toBe('obs_001')
    expect(obs.data[0].trace_id).toBe('trace_llm_1')
    expect(obs.data[0].type).toBe('generation')
    expect(obs.data[0].name).toBe('chat-completion')
    expect(obs.data[0].model).toBe('claude-3-opus')
    expect(obs.data[0].level).toBe('DEFAULT')
  })

  it('LLM score materializes to scores', async () => {
    await client.exec(
      `INSERT INTO ${TEST_DB}.events (id, ns, ts, type, event, url, source, actor, data) VALUES (
        '01TEST000130',
        'test.do/~e2e',
        now64(3),
        'llm.score',
        'llm.score',
        '',
        'langfuse',
        'test',
        '{"id":"score_001","traceId":"trace_llm_1","observationId":"obs_001","name":"accuracy","value":0.95,"source":"api","dataType":"NUMERIC","comment":"High accuracy response"}'
      )`,
    )

    await sleep(2000)

    const scores = await client.query<{ id: string; trace_id: string; name: string; value: string; source: string; comment: string }>(
      `SELECT id, trace_id, name, value, source, comment FROM ${TEST_DB}.scores FINAL WHERE id = 'score_001'`,
    )
    expect(scores.rows).toBe(1)
    expect(scores.data[0].id).toBe('score_001')
    expect(scores.data[0].trace_id).toBe('trace_llm_1')
    expect(scores.data[0].name).toBe('accuracy')
    expect(Number(scores.data[0].value)).toBeCloseTo(0.95)
    expect(scores.data[0].source).toBe('api')
    expect(scores.data[0].comment).toBe('High accuracy response')
  })

  it('idempotent re-run — all migrations skipped', async () => {
    const result = await migrate(client, TEST_DB, migrations)

    expect(result.errors).toEqual([])
    expect(result.applied).toEqual([])
    expect(result.skipped).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
  })
})
