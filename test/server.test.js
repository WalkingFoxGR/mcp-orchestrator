// The HTTP surface of server.js, run as a child process on a free local port
// with no database and no upstream: nothing leaves this machine.
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

let keyed
let open
before(async () => {
  keyed = await start({ ORCH_INBOUND_KEY: KEY })
  open = await start({})
})
after(() => {
  keyed?.child.kill()
  open?.child.kill()
})

test('with a key set: /health is open, everything else needs the key', async () => {
  assert.equal((await fetch(`${keyed.url}/health`)).status, 200)
  for (const headers of [{}, { 'x-orchestrator-key': 'wrong' }]) {
    const res = await rpc(keyed.url, headers)
    assert.equal(res.status, 401)
    assert.deepEqual(await res.json(), { error: 'Unauthorized' })
  }
  assert.equal((await fetch(`${keyed.url}/other`)).status, 401)
  const ok = await rpc(keyed.url, { 'x-orchestrator-key': KEY })
  assert.equal(ok.status, 200)
})

test('the refusal is logged in one line without the key', async () => {
  await rpc(keyed.url, { 'x-orchestrator-key': `${KEY}-nearly` })
  assert.match(keyed.output(), /401 POST \/: inbound key wrong/)
  assert.ok(!keyed.output().includes(KEY))
})

test('a name header alone, or an id with no database, resolves nothing', async () => {
  const byName = await (await rpc(keyed.url, { 'x-orchestrator-key': KEY, 'x-mcp-integration-name': 'Some name' })).json()
  assert.equal(byName.error.code, -32601)
  const byId = await (await rpc(keyed.url, { 'x-orchestrator-key': KEY, 'x-mcp-integration-id': '11111111-1111-4111-8111-111111111111' })).json()
  assert.deepEqual(byId.error, { code: -32601, message: 'No active MCP integration with that x-mcp-integration-id' })
  const badId = await (await rpc(keyed.url, { 'x-orchestrator-key': KEY, 'x-mcp-integration-id': 'Some name' })).json()
  assert.equal(badId.error.code, -32602)
})

test('without a key set: today\'s behaviour, with a loud startup warning', async () => {
  assert.match(open.output(), /WARNING: ORCH_INBOUND_KEY is not set/)
  assert.equal((await rpc(open.url)).status, 200)
})
