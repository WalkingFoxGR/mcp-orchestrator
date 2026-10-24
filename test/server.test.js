// The HTTP surface of server.js, run as a child process on a port of its own
// with no upstream and, where a service key is set, a database url nothing
// listens on that no test reaches: nothing leaves this machine.
import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { startOrchestrator } from '../fixtures/orchestrator.mjs'

const KEY = 'test-inbound-key-0123456789abcdef'
const NEXT_KEY = 'next-key-for-rotation-0123456789abcdef'

const start = (env) => startOrchestrator({ SUPABASE_URL: '', SUPABASE_SERVICE_ROLE_KEY: '', ...env })

const rpc = (url, headers = {}, body = { jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) =>
  fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) })

// A service key with a database url on a closed local port: the client is built, never reached.
const DB = { SUPABASE_URL: 'http://127.0.0.1:9', SUPABASE_SERVICE_ROLE_KEY: 'test-service-key' }

let keyed
let closed
let allowed
let open
let allowFalse
let shortOnly
before(async () => {
  // A third entry too short to be a key: ignored, with a warning, while the two real keys work.
  keyed = await start({ ORCH_INBOUND_KEY: ` ${KEY},${NEXT_KEY}, typo\n` })
  closed = await start({ ...DB, ORCH_INBOUND_KEY: ' ' })
  allowed = await start({ ...DB, ORCH_ALLOW_UNAUTHENTICATED: 'true' })
  open = await start({})
  allowFalse = await start({ ...DB, ORCH_ALLOW_UNAUTHENTICATED: 'false' })
  // No service key: with nothing set this would be open, but a key was set, only too short.
  shortOnly = await start({ ORCH_INBOUND_KEY: 'short-key' })
})
after(() => {
  for (const s of [keyed, closed, allowed, open, allowFalse, shortOnly]) s?.child.kill()
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
  assert.equal((await rpc(keyed.url, { 'x-orchestrator-key': NEXT_KEY })).status, 200)
  assert.equal((await rpc(keyed.url, { 'x-orchestrator-key': 'typo' })).status, 401)
  assert.match(keyed.output(), /Inbound key required on every request but \/health \(2 keys accepted\)/)
  assert.match(keyed.output(), /WARNING: 1 ORCH_INBOUND_KEY entry is shorter than 32 characters and ignored/)
  assert.ok(!keyed.output().includes('typo'))
})

test('refusals are logged once per address per minute, without the key', async () => {
  await rpc(keyed.url, { 'x-orchestrator-key': `${KEY}-nearly` })
  const lines = (await keyed.waitFor(/ 401 /)).split('\n').filter((l) => l.includes(' 401 '))
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
  assert.match(await closed.waitFor(/inbound key not configured/), /inbound key not configured/)
})

test('no key, a service key and ORCH_ALLOW_UNAUTHENTICATED=true: open, and says so', async () => {
  assert.match(allowed.output(), /accepted without a key \(ORCH_ALLOW_UNAUTHENTICATED=true\)/)
  assert.equal((await rpc(allowed.url)).status, 200)
})

test('ORCH_ALLOW_UNAUTHENTICATED=false (or anything but true) keeps it closed', async () => {
  assert.match(allowFalse.output(), /WARNING: ORCH_INBOUND_KEY is not set\. Every request but \/health is refused/)
  assert.equal((await rpc(allowFalse.url)).status, 401)
})

test('a key set with no entry of 32 characters counts as unset and fails closed, even with no service key', async () => {
  assert.match(shortOnly.output(), /WARNING: 1 ORCH_INBOUND_KEY entry is shorter than 32 characters and ignored/)
  assert.match(shortOnly.output(), /WARNING: ORCH_INBOUND_KEY has no entry of at least 32 characters\. Every request but \/health is refused/)
  assert.ok(!shortOnly.output().includes('short-key'))
  assert.equal((await fetch(`${shortOnly.url}/health`)).status, 200)
  for (const headers of [{}, { 'x-orchestrator-key': 'short-key' }]) assert.equal((await rpc(shortOnly.url, headers)).status, 401)
})

test('no key and no service key: open, and says so', async () => {
  assert.match(open.output(), /accepted without a key \(no Supabase service key configured\)/)
  assert.equal((await rpc(open.url)).status, 200)
})
