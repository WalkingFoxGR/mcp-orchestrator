import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  apifyCacheKey,
  createInboundGuard,
  createRateLimitedLog,
  createResolver,
  integrationIdOf,
  logLabel,
  oauthTokenCacheKey,
  parseInboundKeys,
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
  const logs = []
  const db = { down: false }
  const resolver = createResolver({
    fetchIntegrationById: async (id) => {
      fetched.push(id)
      if (db.down) throw new Error('connection reset')
      return rows.find((r) => r.id === id) ?? null
    },
    buildAuthHeaders: async (config) => ({ Authorization: `Bearer ${config.bearerToken}` }),
    normalizeUrl: (u) => u,
    ttlMs: 300_000,
    defaultTimeoutMs: 300_000,
    log: (line) => logs.push(line),
    now: () => clock,
    ...opts,
  })
  return { resolver, fetched, logs, db, tick: (ms) => { clock += ms } }
}

const OLD_KEY = 'old-key-'.padEnd(40, 'o')
const NEW_KEY = 'new-key-'.padEnd(40, 'n')

const guard = (raw, opts = {}) => {
  const { keys, ignored } = parseInboundKeys(raw)
  return createInboundGuard({ keys, ignoredKeys: ignored, serviceKeyConfigured: true, ...opts })
}

test('inbound key: missing and wrong are refused, right passes', () => {
  const { mode, check } = guard(KEY)
  assert.equal(mode, 'key')
  assert.deepEqual(check({}), { ok: false, reason: 'missing' })
  assert.deepEqual(check({ 'x-orchestrator-key': 'nope' }), { ok: false, reason: 'wrong' })
  assert.deepEqual(check({ 'x-orchestrator-key': 'x'.repeat(48) }), { ok: false, reason: 'wrong' })
  assert.deepEqual(check({ 'x-orchestrator-key': `${KEY}x` }), { ok: false, reason: 'wrong' })
  assert.deepEqual(check({ 'x-orchestrator-key': KEY }), { ok: true })
  assert.deepEqual(check({ 'x-orchestrator-key': [KEY, 'other'] }), { ok: true })
})

test('inbound key: the env value is trimmed, and a comma list accepts each key for rotation', () => {
  assert.deepEqual(parseInboundKeys(` ${KEY}\n`), { keys: [KEY], ignored: 0 })
  assert.deepEqual(parseInboundKeys(` ${OLD_KEY} , ,${NEW_KEY},`), { keys: [OLD_KEY, NEW_KEY], ignored: 0 })
  assert.deepEqual(parseInboundKeys(' , \n'), { keys: [], ignored: 0 })
  assert.deepEqual(guard(` ${KEY}\n`).check({ 'x-orchestrator-key': KEY }), { ok: true })
  const rotating = guard(`${OLD_KEY},${NEW_KEY}`)
  assert.deepEqual(rotating.check({ 'x-orchestrator-key': OLD_KEY }), { ok: true })
  assert.deepEqual(rotating.check({ 'x-orchestrator-key': NEW_KEY }), { ok: true })
  assert.deepEqual(rotating.check({ 'x-orchestrator-key': `${OLD_KEY},${NEW_KEY}` }), { ok: false, reason: 'wrong' })
  assert.match(rotating.startupLine, /2 keys accepted/)
  assert.equal(rotating.ignoredLine, null)
})

test('inbound key: an entry under 32 characters is ignored with a warning; with none left it fails closed', () => {
  // A rotation typo: '<current>,x' must not accept 'x'.
  assert.deepEqual(parseInboundKeys(`${KEY},x, ${'y'.repeat(31)} `), { keys: [KEY], ignored: 2 })
  assert.deepEqual(parseInboundKeys('k'.repeat(32)), { keys: ['k'.repeat(32)], ignored: 0 })
  const typo = guard(`${KEY},x`)
  assert.equal(typo.mode, 'key')
  assert.deepEqual(typo.check({ 'x-orchestrator-key': 'x' }), { ok: false, reason: 'wrong' })
  assert.deepEqual(typo.check({ 'x-orchestrator-key': KEY }), { ok: true })
  assert.match(typo.startupLine, /1 key accepted/)
  assert.match(typo.ignoredLine, /WARNING: 1 ORCH_INBOUND_KEY entry is shorter than 32 characters and ignored/)
  // Only short entries: counted as unset, and closed even with no service key.
  for (const serviceKeyConfigured of [true, false]) {
    const { keys, ignored } = parseInboundKeys('short-one,short-two')
    const closed = createInboundGuard({ keys, ignoredKeys: ignored, serviceKeyConfigured })
    assert.equal(closed.mode, 'closed')
    assert.deepEqual(closed.check({ 'x-orchestrator-key': 'short-one' }), { ok: false, reason: 'not configured' })
    assert.match(closed.startupLine, /has no entry of at least 32 characters\. Every request but \/health is refused/)
    assert.match(closed.ignoredLine, /2 ORCH_INBOUND_KEY entries are shorter than 32 characters/)
    assert.ok(!closed.ignoredLine.includes('short-one') && !closed.startupLine.includes('short-one'))
  }
  // ORCH_ALLOW_UNAUTHENTICATED=true still opens it, as it would with no key at all.
  assert.equal(guard('short', { allowUnauthenticated: true }).mode, 'open')
})

test('inbound key unset with a service key: every request is refused, unless explicitly allowed', () => {
  for (const raw of ['', undefined, ' , ']) {
    const closed = guard(raw)
    assert.equal(closed.mode, 'closed')
    assert.deepEqual(closed.check({ 'x-orchestrator-key': 'anything' }), { ok: false, reason: 'not configured' })
    assert.match(closed.startupLine, /not set\. Every request but \/health is refused/)
  }
  const allowed = guard('', { allowUnauthenticated: true })
  assert.equal(allowed.mode, 'open')
  assert.deepEqual(allowed.check({}), { ok: true })
  assert.match(allowed.startupLine, /accepted without a key \(ORCH_ALLOW_UNAUTHENTICATED=true\)/)
  const noDb = createInboundGuard({ keys: [], serviceKeyConfigured: false })
  assert.equal(noDb.mode, 'open')
  // A key wins over the allow flag.
  assert.equal(guard(KEY, { allowUnauthenticated: true }).check({}).ok, false)
})

test('the startup line and the refusal never carry the key', () => {
  const g = guard(KEY)
  assert.ok(!JSON.stringify(g.check({ 'x-orchestrator-key': 'nope' })).includes(KEY))
  assert.ok(!g.startupLine.includes(KEY))
})

test('the refusal log: one line per address per minute, then a count of what was left out', () => {
  let clock = 0
  const lines = []
  const log = createRateLimitedLog({ windowMs: 60_000, log: (l) => lines.push(l), now: () => clock })
  log('10.0.0.1', 'refused')
  log('10.0.0.1', 'refused')
  log('10.0.0.1', 'refused')
  log('10.0.0.2', 'refused other')
  clock += 59_999
  log('10.0.0.1', 'refused')
  clock += 1
  log('10.0.0.1', 'refused again')
  clock += 60_000
  log('10.0.0.1', 'quiet')
  assert.deepEqual(lines, ['refused', 'refused other', 'refused again (3 more from this address suppressed)', 'quiet'])
})

test('the refusal log stays bounded: past maxKeys the oldest address is evicted and logged again', () => {
  const lines = []
  const log = createRateLimitedLog({ windowMs: 60_000, log: (l) => lines.push(l), now: () => 0, maxKeys: 3 })
  for (let i = 0; i < 4; i++) log(`a${i}`, `line ${i}`)
  // a3 evicted a0 (nothing had expired): a0 is new again inside its window, and is logged.
  log('a0', 'a0 again')
  // a0 in turn evicted a1; a2 is still held, so it stays quiet.
  log('a2', 'a2 again')
  assert.deepEqual(lines, ['line 0', 'line 1', 'line 2', 'line 3', 'a0 again'])
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
  const { resolver } = world([row(B, { config: { authType: 'none' } }), row('33333333-3333-4333-8333-333333333333', { integration_type: 'webhook' })], { staticUpstreams: [STATIC] })
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

test('is_active is not checked: an inactive row resolves (it is the chat toggle; Nexus picks the rows)', async () => {
  const { resolver } = world([row(A, { is_active: false })])
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.integrationId, A)
})

test('a deleted row drops out once the cache entry expires', async () => {
  const rows = [row(A)]
  const { resolver, tick } = world(rows)
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  rows.length = 0
  tick(300_001)
  assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': A }, ''), { upstream: null, reason: 'not_found' })
  assert.equal(resolver.integrationCache.get(A).upstream, null)
})

test('a read error on refresh keeps serving the expired entry, logs one line, and retries later', async () => {
  const rows = [row(A)]
  const { resolver, fetched, logs, db, tick } = world(rows)
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  db.down = true
  tick(300_001)
  const stale = await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  assert.equal(stale.upstream.integrationId, A)
  assert.deepEqual(stale.upstream.headers, { Authorization: 'Bearer secret-1111' })
  assert.equal(logs.length, 1)
  assert.match(logs[0], /could not be read again \(connection reset\); serving the cached entry/)
  // Within the retry window the database is not asked again.
  tick(29_999)
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  assert.equal(fetched.length, 2)
  db.down = false
  rows[0] = row(A, { config: { url: 'https://moved.example/mcp' } })
  tick(2)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.url, 'https://moved.example/mcp')
  assert.equal(fetched.length, 3)
})

test('a cached entry is served through a read outage for at most an hour after its last good read', async () => {
  const { resolver, logs, db, tick } = world([row(A)])
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  db.down = true
  tick(60 * 60_000)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.integrationId, A)
  tick(30_001)
  assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': A }, ''), { upstream: null, reason: 'lookup_failed' })
  assert.match(logs.at(-1), /older than 60 min and dropped/)
  // Dropped, not kept: the next failing read has nothing to serve either.
  assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': A }, ''), { upstream: null, reason: 'lookup_failed' })
  assert.match(logs.at(-1), /could not be read \(connection reset\)$/)
})

test('concurrent resolves of one id share one database read', async () => {
  let release
  const gate = new Promise((resolve) => { release = resolve })
  const fetched = []
  const { resolver, tick } = world([], {
    fetchIntegrationById: async (id) => {
      fetched.push(id)
      await gate
      return row(id)
    },
  })
  const pending = [A, A, A, B].map((id) => resolver.resolve({ 'x-mcp-integration-id': id }, ''))
  release()
  const results = await Promise.all(pending)
  assert.deepEqual(results.map((r) => r.upstream.integrationId), [A, A, A, B])
  assert.deepEqual(fetched, [A, B])
  // Once settled, the shared read is forgotten: after the TTL the next resolve reads again.
  tick(300_001)
  await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  assert.deepEqual(fetched, [A, B, A])
})

test('a read error with nothing cached answers lookup_failed, not not_found, and is not remembered', async () => {
  const { resolver, fetched, logs, db } = world([row(A)])
  db.down = true
  assert.deepEqual(await resolver.resolve({ 'x-mcp-integration-id': A }, ''), { upstream: null, reason: 'lookup_failed' })
  assert.equal(logs.length, 1)
  db.down = false
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.integrationId, A)
  assert.equal(fetched.length, 2)
})

test('an id that resolves nothing is remembered for 10 s', async () => {
  const rows = []
  const { resolver, fetched, tick } = world(rows)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).reason, 'not_found')
  rows.push(row(A))
  tick(9_999)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).reason, 'not_found')
  assert.equal(fetched.length, 1)
  tick(2)
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.integrationId, A)
})

test('the auth headers are built on every resolve, so a cached upstream never carries an expired token', async () => {
  let issued = 0
  const { resolver } = world([row(A, { config: { url: 'https://cc.example/mcp', authType: 'oauth_client_credentials' } })], {
    buildAuthHeaders: async (config, id) => ({ Authorization: `Bearer ${id.slice(0, 4)}-token-${++issued}` }),
  })
  const first = await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  const second = await resolver.resolve({ 'x-mcp-integration-id': A }, '')
  assert.equal(first.upstream.headers.Authorization, 'Bearer 1111-token-1')
  assert.equal(second.upstream.headers.Authorization, 'Bearer 1111-token-2')
  assert.equal(second.upstream.mcpConfig, undefined)
})

test('a name never reaches an upstream cached for another id', async () => {
  const { resolver } = world([row(A)])
  assert.equal((await resolver.resolve({ 'x-mcp-integration-id': A }, '')).upstream.integrationId, A)
  // (a) only the name A has.
  const byName = await resolver.resolve({ 'x-mcp-integration-name': 'Shared name' }, 'any_tool')
  assert.deepEqual(byName, { upstream: null, reason: 'no_upstream' })
  // (b) an unknown id with that name.
  const unknownId = await resolver.resolve({ 'x-mcp-integration-id': B, 'x-mcp-integration-name': 'Shared name' }, 'any_tool')
  assert.deepEqual(unknownId, { upstream: null, reason: 'not_found' })
})

test('the client-credentials token and Apify caches are keyed per integration id', () => {
  const client = { oauthTokenUrl: 'https://auth.example/token', oauthClientId: 'shared-client' }
  assert.notEqual(oauthTokenCacheKey(A, client), oauthTokenCacheKey(B, client))
  assert.equal(oauthTokenCacheKey(A, client), oauthTokenCacheKey(A, { ...client }))
  assert.notEqual(oauthTokenCacheKey(A, client), oauthTokenCacheKey(A, { ...client, oauthClientId: 'other' }))
  assert.notEqual(oauthTokenCacheKey(A, client), oauthTokenCacheKey(A, { ...client, oauthTokenUrl: 'https://other.example/token' }))
  assert.equal(apifyCacheKey(A), `id:${A}`)
  assert.notEqual(apifyCacheKey(A), apifyCacheKey(B))
  assert.equal(apifyCacheKey(''), 'default')
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
