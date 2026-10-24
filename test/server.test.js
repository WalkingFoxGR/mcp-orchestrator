// The HTTP surface of server.js, run as a child process on a free local port
// with no upstream and, where a service key is set, a database url nothing
// listens on that no test reaches: nothing leaves this machine.
import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createServer } from 'node:net'
import { fileURLToPath } from 'node:url'

const KEY = 'test-inbound-key-0123456789abcdef'
const SERVER = fileURLToPath(new URL('../server.js', import.meta.url))

async function freePort() {
  return new Promise((resolve) => {
    const s = createServer()
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address()
      s.close(() => resolve(port))
    })
  })
}

async function start(env) {
  const port = await freePort()
  const child = spawn(process.execPath, [SERVER], {
    env: { PATH: process.env.PATH, PORT: String(port), SUPABASE_URL: '', SUPABASE_SERVICE_ROLE_KEY: '', ...env },
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
  return { url: `http://127.0.0.1:${port}`, child, output: () => output }
}

const rpc = (url, headers = {}, body = { jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) =>
  fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) })

// A service key with a database url on a closed local port: the client is built, never reached.
const DB = { SUPABASE_URL: 'http://127.0.0.1:9', SUPABASE_SERVICE_ROLE_KEY: 'test-service-key' }

let keyed
let closed
let allowed
let open
before(async () => {
  keyed = await start({ ORCH_INBOUND_KEY: ` ${KEY},next-key-for-rotation\n` })
  closed = await start({ ...DB, ORCH_INBOUND_KEY: ' ' })
  allowed = await start({ ...DB, ORCH_ALLOW_UNAUTHENTICATED: 'true' })
  open = await start({})
})
after(() => {
  for (const s of [keyed, closed, allowed, open]) s?.child.kill()
})

test('with a key set: /health is open, everything else needs the key', async () => {
  assert.equal((await fetch(`${keyed.url}/health`)).status, 200)
  for (const headers of [{}, { 'x-orchestrator-key': 'wrong' }]) {
    const res = await rpc(keyed.url, headers)
    assert.equal(res.status, 401)
    assert.deepEqual(await res.json(), { error: 'Unauthorized' })
  }
  assert.equal((await fetch(`${keyed.url}/other`)).status, 401)
  // The env value is trimmed and read as a list: both keys pass.
  assert.equal((await rpc(keyed.url, { 'x-orchestrator-key': KEY })).status, 200)
  assert.equal((await rpc(keyed.url, { 'x-orchestrator-key': 'next-key-for-rotation' })).status, 200)
  assert.match(keyed.output(), /Inbound key required on every request but \/health \(2 keys accepted\)/)
})

test('refusals are logged once per address per minute, without the key', async () => {
  await rpc(keyed.url, { 'x-orchestrator-key': `${KEY}-nearly` })
  const lines = keyed.output().split('\n').filter((l) => l.includes(' 401 '))
  assert.equal(lines.length, 1)
  assert.match(lines[0], /401 POST \/ from (::ffff:)?127\.0\.0\.1: inbound key missing/)
  assert.ok(!keyed.output().includes(KEY))
})

test('a name header alone, or an id with no database, resolves nothing', async () => {
  const byName = await (await rpc(keyed.url, { 'x-orchestrator-key': KEY, 'x-mcp-integration-name': 'Some name' })).json()
  assert.equal(byName.error.code, -32601)
  const byId = await (await rpc(keyed.url, { 'x-orchestrator-key': KEY, 'x-mcp-integration-id': '11111111-1111-4111-8111-111111111111' })).json()
  assert.deepEqual(byId.error, { code: -32601, message: 'No MCP integration with that x-mcp-integration-id' })
  const badId = await (await rpc(keyed.url, { 'x-orchestrator-key': KEY, 'x-mcp-integration-id': 'Some name' })).json()
  assert.equal(badId.error.code, -32602)
})

test('no key but a service key: fails closed, /health still answers', async () => {
  assert.match(closed.output(), /WARNING: ORCH_INBOUND_KEY is not set\. Every request but \/health is refused/)
  assert.equal((await fetch(`${closed.url}/health`)).status, 200)
  for (const headers of [{}, { 'x-orchestrator-key': 'anything' }]) {
    const res = await rpc(closed.url, headers)
    assert.equal(res.status, 401)
    assert.deepEqual(await res.json(), { error: 'Unauthorized' })
  }
  assert.match(closed.output(), /inbound key not configured/)
})

test('no key, a service key and ORCH_ALLOW_UNAUTHENTICATED=true: open, and says so', async () => {
  assert.match(allowed.output(), /accepted without a key \(ORCH_ALLOW_UNAUTHENTICATED=true\)/)
  assert.equal((await rpc(allowed.url)).status, 200)
})

test('no key and no service key: open, and says so', async () => {
  assert.match(open.output(), /accepted without a key \(no Supabase service key configured\)/)
  assert.equal((await rpc(open.url)).status, 200)
})
