// The upstream SSRF guard as wired in server.js, run as a child process. Its database is a fake
// PostgREST on 127.0.0.1 started here; every upstream named points at a refused address, except
// the fake MCP server on 127.0.0.1 that ORCH_ALLOW_PRIVATE_UPSTREAMS lets through.
import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { spawn } from 'node:child_process'
import { createServer } from 'node:net'
import { fileURLToPath } from 'node:url'

const KEY = 'test-inbound-key-0123456789abcdef'
const SERVER = fileURLToPath(new URL('../server.js', import.meta.url))
const FAKE_DNS = fileURLToPath(new URL('../fixtures/fake-dns.mjs', import.meta.url))
const TOKEN_ROW = '33333333-3333-4333-8333-333333333333'
const TOKEN_DNS_ROW = '44444444-4444-4444-8444-444444444444'
const SCHEMELESS_ROW = '55555555-5555-4555-8555-555555555555'
const SECRET = 'client-secret-never-logged'

async function freePort() {
  return new Promise((resolve) => {
    const s = createServer()
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address()
      s.close(() => resolve(port))
    })
  })
}

const listen = (handler) => new Promise((resolve) => {
  const server = http.createServer(handler)
  server.listen(0, '127.0.0.1', () => resolve(server))
})

let db
let mcp
let mcpHits = 0
const servers = []

async function start(env) {
  const port = await freePort()
  const child = spawn(process.execPath, ['--import', FAKE_DNS, SERVER], {
    env: { PATH: process.env.PATH, PORT: String(port), ORCH_INBOUND_KEY: KEY, SUPABASE_URL: `http://127.0.0.1:${db.address().port}`, SUPABASE_SERVICE_ROLE_KEY: 'test-service-key', ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let output = ''
  child.stdout.on('data', (d) => { output += d })
  child.stderr.on('data', (d) => { output += d })
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`server did not start: ${output}`)), 5000)
    child.stdout.on('data', () => {
      if (output.includes('Listening on port')) { clearTimeout(timer); setTimeout(resolve, 20) }
    })
  })
  const s = { url: `http://127.0.0.1:${port}`, child, output: () => output }
  servers.push(s)
  return s
}

const call = async (url, headers = {}) => (await fetch(url, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'x-orchestrator-key': KEY, ...headers },
  body: JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'tools/call', params: { name: 'get_contacts', arguments: {} } }),
})).json()

before(async () => {
  // PostgREST for three agent_integrations rows: a client-credentials token url on the metadata
  // address, one on a name that resolves to 127.0.0.1, and an API wrapper whose url has no scheme
  // (the shape of production's one active wrapper).
  const rows = {
    [TOKEN_ROW]: { integration_type: 'mcp', config: { url: 'https://mcp.example.com/mcp', authType: 'oauth_client_credentials', oauthTokenUrl: 'http://169.254.169.254/latest/token', oauthClientId: 'client', oauthClientSecret: SECRET } },
    [TOKEN_DNS_ROW]: { integration_type: 'mcp', config: { url: 'https://mcp.example.com/mcp', authType: 'oauth_client_credentials', oauthTokenUrl: 'https://token.example.invalid/oauth/token', oauthClientId: 'client', oauthClientSecret: SECRET } },
    [SCHEMELESS_ROW]: { integration_type: 'api', config: { wrapperEnabled: true, wrapper: { url: 'mcp-wrapper.example.invalid/api/mcp/wrapper', authType: 'none' } } },
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
  const lines = s.output().split('\n').filter((l) => l.includes('upstream_refused'))
  assert.equal(lines.length, 1)
  assert.match(lines[0], /upstream_refused 127\.0\.0\.1: private address/)
  assert.match(s.output(), /Upstream SSRF guard on \(at most 5 redirects\); private hosts allowed: none/)
})

test('the client-credentials token fetch is guarded too, and the secret is never logged or answered', async () => {
  const s = await start({})
  const reply = await call(s.url, { 'x-mcp-integration-id': TOKEN_ROW })
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(s.output(), /upstream_refused 169\.254\.169\.254: private address/)
  assert.ok(!s.output().includes(SECRET))
  assert.ok(!JSON.stringify(reply).includes(SECRET))
})

test('a token url whose name resolves to 127.0.0.1 is refused by the token fetch\'s own pinned lookup', async () => {
  const s = await start({})
  const reply = await call(s.url, { 'x-mcp-integration-id': TOKEN_DNS_ROW })
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(s.output(), /upstream_refused token\.example\.invalid: resolves to a private address/)
  assert.ok(!s.output().includes(SECRET))
})

test('a wrapper url with no scheme is checked as the https url it is fetched as', async () => {
  const s = await start({})
  const reply = await call(s.url, { 'x-mcp-integration-id': SCHEMELESS_ROW })
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(s.output(), /upstream_refused mcp-wrapper\.example\.invalid: resolves to a private address/)
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
  assert.match(s.output(), /private hosts allowed: 127\.0\.0\.1 \(any port\)/)
})

test('a name the static checks pass but that resolves to 127.0.0.1 is refused at connect time (the pinned lookup)', async () => {
  const s = await start({ MCP_UPSTREAMS: JSON.stringify([{ name: 'rebind', url: `http://rebind.example.invalid:${mcp.address().port}/mcp` }]) })
  mcpHits = 0
  const reply = await call(s.url)
  assert.deepEqual(reply.error, { code: -32010, message: 'upstream_refused' })
  assert.match(s.output(), /upstream_refused rebind\.example\.invalid: resolves to a private address/)
  assert.equal(mcpHits, 0)
})
