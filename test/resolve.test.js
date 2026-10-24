import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  checkInboundKey,
  createResolver,
  inboundKeyWarning,
  integrationIdOf,
  logLabel,
  sessionKeyFor,
} from '../lib/resolve.js'

const KEY = 'k'.repeat(48)
const A = '11111111-1111-4111-8111-111111111111'
const B = '22222222-2222-4222-8222-222222222222'

const row = (id, over = {}) => ({
  id,
  name: 'Shared name',
  integration_type: 'mcp',
  is_active: true,
  config: { url: `https://mcp-${id.slice(0, 4)}.example/mcp`, authType: 'bearer', bearerToken: `secret-${id.slice(0, 4)}` },
  ...over,
})

function world(rows, opts = {}) {
  let clock = 1_000
  const fetched = []
  const resolver = createResolver({
    fetchIntegrationById: async (id) => {
      fetched.push(id)
      return rows.find((r) => r.id === id) ?? null
    },
    buildAuthHeaders: async (config) => ({ Authorization: `Bearer ${config.bearerToken}` }),
    normalizeUrl: (u) => u,
    ttlMs: 300_000,
    defaultTimeoutMs: 300_000,
    now: () => clock,
    ...opts,
  })
  return { resolver, fetched, tick: (ms) => { clock += ms } }
}

test('inbound key: missing and wrong are refused, right passes', () => {
  assert.deepEqual(checkInboundKey({}, KEY), { ok: false, reason: 'missing' })
  assert.deepEqual(checkInboundKey({ 'x-orchestrator-key': 'nope' }, KEY), { ok: false, reason: 'wrong' })
  assert.deepEqual(checkInboundKey({ 'x-orchestrator-key': 'x'.repeat(48) }, KEY), { ok: false, reason: 'wrong' })
  assert.deepEqual(checkInboundKey({ 'x-orchestrator-key': `${KEY}x` }, KEY), { ok: false, reason: 'wrong' })
  assert.deepEqual(checkInboundKey({ 'x-orchestrator-key': KEY }, KEY), { ok: true, open: false })
  assert.deepEqual(checkInboundKey({ 'x-orchestrator-key': [KEY, 'other'] }, KEY), { ok: true, open: false })
})

test('inbound key unset: every request passes, and startup warns', () => {
  assert.deepEqual(checkInboundKey({}, ''), { ok: true, open: true })
  assert.deepEqual(checkInboundKey({ 'x-orchestrator-key': 'anything' }, undefined), { ok: true, open: true })
  assert.match(inboundKeyWarning(''), /ORCH_INBOUND_KEY is not set/)
  assert.equal(inboundKeyWarning(KEY), null)
})

test('the warning and the refusal never carry the key', () => {
  assert.ok(!JSON.stringify(checkInboundKey({ 'x-orchestrator-key': 'nope' }, KEY)).includes(KEY))
})

test('resolves by integration id only, with that row\'s url and credentials', async () => {
  const { resolver } = world([row(A), row(B)])
  const { upstream } = await resolver.resolve({ 'x-mcp-integration-id': A.toUpperCase() }, 'any_tool')
  assert.equal(upstream.integrationId, A)
  assert.equal(upstream.url, 'https://mcp-1111.example/mcp')
  assert.deepEqual(upstream.headers, { Authorization: 'Bearer secret-1111' })
})

test('a name header alone resolves nothing, even when a row has that name', async () => {
  const { resolver, fetched } = world([row(A)])
  const result = await resolver.resolve({ 'x-mcp-integration-name': 'Shared name' }, 'any_tool')
  assert.equal(result.upstream, null)
  assert.equal(result.reason, 'no_upstream')
  assert.deepEqual(fetched, [])
})

test('two rows with the same name in different tenants never cross, whatever name is sent', async () => {
  const { resolver } = world([row(A), row(B)])
  const first = await resolver.resolve({ 'x-mcp-integration-id': A, 'x-mcp-integration-name': 'Shared name' }, '')
  const second = await resolver.resolve({ 'x-mcp-integration-id': B, 'x-mcp-integration-name': 'Shared name' }, '')
  assert.equal(first.upstream.integrationId, A)
  assert.equal(second.upstream.integrationId, B)
  assert.notDeepEqual(first.upstream.headers, second.upstream.headers)
  // A's cached entry never answers for B's name or id.
  const again = await resolver.resolve({ 'x-mcp-integration-id': B, 'x-mcp-integration-name': 'Shared name' }, '')
  assert.equal(again.upstream.headers.Authorization, 'Bearer secret-2222')
  assert.deepEqual(Array.from(resolver.integrationCache.keys()).sort(), [A, B])
})

test('an id that does not resolve never falls back to a static upstream', async () => {
  const STATIC = { name: 'static', url: 'https://static.example/mcp', toolNameIncludes: ['tool'] }
  const { resolver } = world([row(B, { is_active: false }), row('33333333-3333-4333-8333-333333333333', { integration_type: 'webhook' })], { staticUpstreams: [STATIC] })
  for (const id of [A, B, '33333333-3333-4333-8333-333333333333']) {
    assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': id }, 'tool'), { upstream: null, reason: 'not_found' })
  }
  assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': 'Shared name' }, 'tool'), { upstream: null, reason: 'bad_id' })
  // No id at all: the static fallback stays as it was.
  assert.equal((await resolver.resolve({}, 'my_tool')).upstream, STATIC)
})

test('an id lookup is refused when auto-discovery is off or there is no database', async () => {
  for (const opts of [{ autoDiscover: false }, { fetchIntegrationById: null }]) {
    const { resolver } = world([row(A)], opts)
    assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': A }, ''), { upstream: null, reason: 'not_found' })
  }
})

test('an API row resolves only through its enabled wrapper', async () => {
  const wrapped = row(A, { integration_type: 'api', config: { wrapperEnabled: true, wrapper: { url: 'https://wrap.example/mcp' } } })
  const off = row(B, { integration_type: 'api', config: { wrapperEnabled: false, wrapper: { url: 'https://wrap.example/mcp' } } })
  const { resolver } = world([wrapped, off])
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.url, 'https://wrap.example/mcp')
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': B }, '')).upstream, null)
})

test('the cache answers within the TTL and reads the row again after it', async () => {
  const rows = [row(A)]
  const { resolver, fetched, tick } = world(rows)
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  rows[0] = row(A, { config: { url: 'https://moved.example/mcp' } })
  tick(299_999)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.url, 'https://mcp-1111.example/mcp')
  tick(2)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.url, 'https://moved.example/mcp')
  assert.deepEqual(fetched, [A, A])
})

test('a row switched off drops out once the cache entry expires', async () => {
  const rows = [row(A)]
  const { resolver, tick } = world(rows)
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  rows[0] = row(A, { is_active: false })
  tick(300_001)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream, null)
})

test('upstream sessions are keyed by integration id: two tenants on the same url and name never share one', async () => {
  const sameUrl = { url: 'https://shared.example/mcp', authType: 'none' }
  const { resolver } = world([row(A, { config: sameUrl }), row(B, { config: sameUrl })])
  const a = (await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream
  const b = (await resolver.resolve({ 'x-mcp-integration-id': B }, '')).upstream
  assert.equal(sessionKeyFor(a), `id:${A}`)
  assert.equal(sessionKeyFor(b), `id:${B}`)
  assert.notEqual(sessionKeyFor(a), sessionKeyFor(b))
  assert.equal(sessionKeyFor({ name: 'Shared name', url: 'https://shared.example/mcp' }), 'static:shared name')
})

test('id header parsing and log labels', () => {
  assert.equal(integrationIdOf({}), '')
  assert.equal(integrationIdOf({ 'x-mcp-integration-id': ` ${A.toUpperCase()} ` }), A)
  assert.equal(integrationIdOf({ 'x-mcp-integration-id': 'drop table' }), null)
  assert.equal(logLabel('Φθηνούλης\nenergy'), '??????????energy')
  assert.equal(logLabel('x'.repeat(200)).length, 80)
})
