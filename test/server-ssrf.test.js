// The upstream SSRF guard as wired in server.js, run as a child process. Its database is a fake
// PostgREST on 127.0.0.1 started here; every upstream named points at a refused address, except
// the fake MCP server on 127.0.0.1 that ORCH_ALLOW_PRIVATE_UPSTREAMS lets through.
import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { fileURLToPath } from 'node:url'
import { startOrchestrator } from '../fixtures/orchestrator.mjs'

const KEY = 'test-inbound-key-0123456789abcdef'
const FAKE_DNS = fileURLToPath(new URL('../fixtures/fake-dns.mjs', import.meta.url))
const TOKEN_ROW = '33333333-3333-4333-8333-333333333333'
const TOKEN_DNS_ROW = '44444444-4444-4444-8444-444444444444'
const SCHEMELESS_ROW = '55555555-5555-4555-8555-555555555555'
const SELF_ROW = '66666666-6666-4666-8666-666666666666'
const STATIC_URL_ROW = '77777777-7777-4777-8777-777777777777'
const SECRET = 'client-secret-never-logged'

const listen = (handler) => new Promise((resolve) => {
  const server = http.createServer(handler)
  server.listen(0, '127.0.0.1', () => resolve(server))
})

let db
let mcp
let mcpHits = 0
const servers = []

async function start(env) {
  const s = await startOrchestrator(
    { ORCH_INBOUND_KEY: KEY, SUPABASE_URL: `http://127.0.0.1:${db.address().port}`, SUPABASE_SERVICE_ROLE_KEY: 'test-service-key', ...env },
    { nodeArgs: ['--import', FAKE_DNS] },
  )
  servers.push(s)
  return s
}

const call = async (url, headers = {}) => (await fetch(url, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'x-orchestrator-key': KEY, ...headers },
  body: JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'tools/call', params: { name: 'get_contacts', arguments: {} } }),
})).json()

before(async () => {
  // PostgREST for four agent_integrations rows: a client-credentials token url on the metadata
  // address, one on a name that resolves to 127.0.0.1, an API wrapper whose url has no scheme
  // (the shape of production's one active wrapper), and a self-loop: a row whose url is the
  // orchestrator's own public name, with custom headers naming another tenant's row.
  const rows = {
    [TOKEN_ROW]: { integration_type: 'mcp', config: { url: 'https://mcp.example.com/mcp', authType: 'oauth_client_credentials', oauthTokenUrl: 'http://169.254.169.254/latest/token', oauthClientId: 'client', oauthClientSecret: SECRET } },
    [TOKEN_DNS_ROW]: { integration_type: 'mcp', config: { url: 'https://mcp.example.com/mcp', authType: 'oauth_client_credentials', oauthTokenUrl: 'https://token.example.invalid/oauth/token', oauthClientId: 'client', oauthClientSecret: SECRET } },
    [SCHEMELESS_ROW]: { integration_type: 'api', config: { wrapperEnabled: true, wrapper: { url: 'mcp-wrapper.example.invalid/api/mcp/wrapper', authType: 'none' } } },
    [SELF_ROW]: { integration_type: 'mcp', config: { url: 'https://orch-self.example.com/', authType: 'none', headers: { 'x-mcp-integration-id': TOKEN_ROW, 'x-orchestrator-key': 'guess' } } },
    [STATIC_URL_ROW]: { integration_type: 'mcp', config: { url: 'https://orch-static.example.com/mcp', authType: 'none' } },
  }
  db = await listen((req, res) => {
    const id = Object.keys(rows).find((key) => req.url.startsWith('/rest/v1/agent_integrations') && req.url.includes(`id=eq.${key}`))
    if (id) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ id, name: 'Row', is_active: true, ...rows[id] }))
      return
    }
    res.writeHead(406, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ code: 'PGRST116', message: 'no rows' }))
  })
  mcp = await listen((req, res) => {
    mcpHits++
    let body = ''
    req.on('data', (d) => { body += d })
    req.on('end', () => {
      const msg = JSON.parse(body || '{}')
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify(msg.method === 'tools/call'
        ? { jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'reached' }] } }
        : { jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', capabilities: {}, serverInfo: { name: 'fake', version: '1' } } }))
    })
  })
})
after(() => {
  for (const s of servers) s.child.kill()
  db?.close()
  mcp?.close()
})

test('a static upstream on 127.0.0.1 answers upstream_refused, logs one line with the host, and is never reached', async () => {
  const s = await start({ MCP_UPSTREAMS: JSON.stringify([{ name: 'local', url: `http://127.0.0.1:${mcp.address().port}/mcp` }]) })
  mcpHits = 0
  const reply = await call(s.url)
  assert.equal(reply.id, 7)
  // -32010, not the SDK's -32001 RequestTimeout; one generic message, no host or reason.
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.equal(mcpHits, 0)
  const lines = (await s.waitFor(/upstream_refused 127/)).split('\n').filter((l) => l.includes('upstream_refused'))
  assert.equal(lines.length, 1)
  assert.match(lines[0], /upstream_refused 127\.0\.0\.1: private address/)
  assert.match(await s.waitFor(/Upstream SSRF guard on \(at most 5 redirects\); private hosts allowed: none/), /Upstream SSRF guard on \(at most 5 redirects\); private hosts allowed: none/)
})

test('the client-credentials token fetch is guarded too, and the secret is never logged or answered', async () => {
  const s = await start({})
  const reply = await call(s.url, { 'x-mcp-integration-id': TOKEN_ROW })
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused 169\.254\.169\.254: private address/), /upstream_refused 169\.254\.169\.254: private address/)
  assert.ok(!s.output().includes(SECRET))
  assert.ok(!JSON.stringify(reply).includes(SECRET))
})

test('a token url whose name resolves to 127.0.0.1 is refused by the token fetch\'s own pinned lookup', async () => {
  const s = await start({})
  const reply = await call(s.url, { 'x-mcp-integration-id': TOKEN_DNS_ROW })
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused token\.example\.invalid: resolves to a private address/), /upstream_refused token\.example\.invalid: resolves to a private address/)
  assert.ok(!s.output().includes(SECRET))
})

test('a wrapper url with no scheme is checked as the https url it is fetched as', async () => {
  const s = await start({})
  const reply = await call(s.url, { 'x-mcp-integration-id': SCHEMELESS_ROW })
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused mcp-wrapper\.example\.invalid: resolves to a private address/), /upstream_refused mcp-wrapper\.example\.invalid: resolves to a private address/)
  assert.ok(!/Invalid URL/i.test(s.output()))
})

test('ORCH_ALLOW_PRIVATE_UPSTREAMS lets an exact host through', async () => {
  const s = await start({
    MCP_UPSTREAMS: JSON.stringify([{ name: 'local', url: `http://127.0.0.1:${mcp.address().port}/mcp` }]),
    ORCH_ALLOW_PRIVATE_UPSTREAMS: '127.0.0.1',
  })
  mcpHits = 0
  const reply = await call(s.url)
  assert.equal(reply.error, undefined)
  assert.ok(mcpHits > 0)
  assert.match(await s.waitFor(/private hosts allowed: 127\.0\.0\.1 \(any port\)/), /private hosts allowed: 127\.0\.0\.1 \(any port\)/)
})

test('an error that is not a refusal keeps its old answer: an allow-listed host on a closed port fails as fetch failed', async () => {
  const s = await start({
    MCP_UPSTREAMS: JSON.stringify([{ name: 'closed', url: 'http://127.0.0.1:9/mcp' }]),
    ORCH_ALLOW_PRIVATE_UPSTREAMS: '127.0.0.1:9',
  })
  const res = await fetch(s.url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-orchestrator-key': KEY },
    body: JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'tools/call', params: { name: 'get_contacts', arguments: {} } }),
  })
  assert.equal(res.status, 500)
  assert.deepEqual(await res.json(), { error: 'fetch failed' })
  assert.ok(!s.output().includes('upstream_refused'))
})

test('a name the static checks pass but that resolves to 127.0.0.1 is refused at connect time (the pinned lookup)', async () => {
  const s = await start({ MCP_UPSTREAMS: JSON.stringify([{ name: 'rebind', url: `http://rebind.example.invalid:${mcp.address().port}/mcp` }]) })
  mcpHits = 0
  const reply = await call(s.url)
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused rebind\.example\.invalid: resolves to a private address/), /upstream_refused rebind\.example\.invalid: resolves to a private address/)
  assert.equal(mcpHits, 0)
})

test('the orchestrator\'s own Railway names are refused as upstreams, even when listed as allowed', async () => {
  const s = await start({
    RAILWAY_PUBLIC_DOMAIN: 'Orch-Self.example.com',
    RAILWAY_PRIVATE_DOMAIN: 'orch-self.railway.internal',
    RAILWAY_STATIC_URL: 'https://orch-static.example.com',
    MCP_UPSTREAMS: JSON.stringify([{ name: 'self', url: 'http://orch-self.railway.internal:9/' }]),
    ORCH_ALLOW_PRIVATE_UPSTREAMS: 'orch-self.railway.internal:9,orch-self.example.com',
  })
  assert.match(await s.waitFor(/own hosts refused: orch-self\.example\.com, orch-self\.railway\.internal, orch-static\.example\.com/), /own hosts refused: orch-self\.example\.com, orch-self\.railway\.internal, orch-static\.example\.com/)
  // The self-loop row: its url is the public domain, its custom headers name another row.
  const byRow = await call(s.url, { 'x-mcp-integration-id': SELF_ROW })
  assert.deepEqual(byRow.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused orch-self\.example\.com: the orchestrator itself/), /upstream_refused orch-self\.example\.com: the orchestrator itself/)
  // The private name, through MCP_UPSTREAMS, although ORCH_ALLOW_PRIVATE_UPSTREAMS lists it.
  const byStatic = await call(s.url)
  assert.deepEqual(byStatic.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused orch-self\.railway\.internal: the orchestrator itself/), /upstream_refused orch-self\.railway\.internal: the orchestrator itself/)
  // The legacy RAILWAY_STATIC_URL, given as a url, counts by its hostname.
  const byStaticUrl = await call(s.url, { 'x-mcp-integration-id': STATIC_URL_ROW })
  assert.deepEqual(byStaticUrl.error, { code: -32010, message: 'upstream_refused' })
  assert.match(await s.waitFor(/upstream_refused orch-static\.example\.com: the orchestrator itself/), /upstream_refused orch-static\.example\.com: the orchestrator itself/)
})
