// The per-integration caches as server.js wires them: two rows that share a client-credentials
// client (same token url and client id, different secrets) and two Apify rows, against a fake
// PostgREST, a fake token endpoint and a fake MCP server, all on 127.0.0.1 (let through with
// ORCH_ALLOW_PRIVATE_UPSTREAMS). Nothing leaves this machine.
import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { startOrchestrator } from '../fixtures/orchestrator.mjs'

const KEY = 'test-inbound-key-0123456789abcdef'
const ROW_A = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
const ROW_B = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
const APIFY_A = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
const APIFY_B = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'

const listen = (handler) => new Promise((resolve) => {
  const server = http.createServer(handler)
  server.listen(0, '127.0.0.1', () => resolve(server))
})
const readBody = (req) => new Promise((resolve) => {
  let body = ''
  req.on('data', (d) => { body += d })
  req.on('end', () => resolve(body))
})

let db
let token
let mcp
let orch
let tokenRequests = 0
// Every tools/call the fake MCP server received: the row (by its path), the Authorization header and the arguments.
const calls = []

before(async () => {
  token = await listen(async (req, res) => {
    tokenRequests++
    const form = new URLSearchParams(await readBody(req))
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ access_token: `token-for-${form.get('client_secret')}`, expires_in: 3600 }))
  })
  mcp = await listen(async (req, res) => {
    const msg = JSON.parse((await readBody(req)) || '{}')
    let result = { protocolVersion: '2024-11-05', capabilities: {}, serverInfo: { name: 'fake', version: '1' } }
    if (msg.method === 'tools/call') {
      calls.push({ row: req.url.split('/')[1], authorization: req.headers.authorization, name: msg.params.name, args: msg.params.arguments })
      // A started Apify run answers with its run and dataset ids, named after the row it ran for.
      const row = req.url.split('/')[1]
      result = msg.params.name === 'call-actor'
        ? { content: [{ type: 'text', text: JSON.stringify({ id: `run-${row}`, defaultDatasetId: `dataset-${row}` }) }] }
        : { content: [{ type: 'text', text: 'ok' }] }
    }
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }))
  })
  const mcpUrl = (row) => `http://127.0.0.1:${mcp.address().port}/${row}/mcp`
  const tokenUrl = `http://127.0.0.1:${token.address().port}/oauth/token`
  const credentials = (secret) => ({ authType: 'oauth_client_credentials', oauthTokenUrl: tokenUrl, oauthClientId: 'shared-client', oauthClientSecret: secret })
  const rows = {
    [ROW_A]: { name: 'Shop A', config: { url: mcpUrl('a'), ...credentials('secret-a') } },
    [ROW_B]: { name: 'Shop B', config: { url: mcpUrl('b'), ...credentials('secret-b') } },
    [APIFY_A]: { name: 'Apify A', config: { url: mcpUrl('apify-a'), authType: 'none' } },
    [APIFY_B]: { name: 'Apify B', config: { url: mcpUrl('apify-b'), authType: 'none' } },
  }
  db = await listen((req, res) => {
    const id = Object.keys(rows).find((key) => req.url.startsWith('/rest/v1/agent_integrations') && req.url.includes(`id=eq.${key}`))
    if (id) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ id, integration_type: 'mcp', is_active: true, ...rows[id] }))
      return
    }
    res.writeHead(406, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ code: 'PGRST116', message: 'no rows' }))
  })
  orch = await startOrchestrator({
    ORCH_INBOUND_KEY: KEY,
    SUPABASE_URL: `http://127.0.0.1:${db.address().port}`,
    SUPABASE_SERVICE_ROLE_KEY: 'test-service-key',
    ORCH_ALLOW_PRIVATE_UPSTREAMS: '127.0.0.1',
  })
})
after(() => {
  orch?.child.kill()
  for (const s of [db, token, mcp]) s?.close()
})

const callTool = async (row, name, args = {}) => (await fetch(orch.url, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'x-orchestrator-key': KEY, 'x-mcp-integration-id': row, 'x-mcp-integration-name': name.includes('actor') ? 'Apify' : 'Shop' },
  body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }),
})).json()

test('two rows sharing a client id each get their own client-credentials token, fetched once per row', async () => {
  assert.equal((await callTool(ROW_A, 'get_orders')).error, undefined)
  assert.equal((await callTool(ROW_B, 'get_orders')).error, undefined)
  assert.equal((await callTool(ROW_A, 'get_orders')).error, undefined)
  const seen = calls.filter((c) => c.name === 'get_orders').map((c) => [c.row, c.authorization])
  assert.deepEqual(seen, [['a', 'Bearer token-for-secret-a'], ['b', 'Bearer token-for-secret-b'], ['a', 'Bearer token-for-secret-a']])
  assert.equal(tokenRequests, 2)
})

test('an Apify dataset id cached for one row is never injected into another row\'s call', async () => {
  assert.equal((await callTool(APIFY_A, 'call-actor', { actor: 'x' })).error, undefined)
  await callTool(APIFY_B, 'get-actor-output', { datasetId: 'your-dataset-id-here' })
  await callTool(APIFY_A, 'get-actor-output', { datasetId: 'your-dataset-id-here' })
  const outputs = calls.filter((c) => c.name === 'get-actor-output').map((c) => [c.row, c.args.datasetId])
  // Row B started no run: its placeholder stays. Row A gets the dataset of its own run.
  assert.deepEqual(outputs, [['apify-b', 'your-dataset-id-here'], ['apify-a', 'dataset-apify-a']])
})
