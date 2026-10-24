// The upstream SSRF guard. Name resolution is faked; the only sockets opened are to a server on
// 127.0.0.1 that this test starts, reached through an allowed or refused fake name.
import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { Agent } from 'undici'
import {
  UpstreamRefusedError,
  createGuardedLookup,
  guardedFetch,
  hostRefusal,
  isAllowed,
  isPrivateIp,
  parseAllowedHosts,
  parseOwnHosts,
  refusalIn,
} from '../lib/upstream-guard.js'

const lookupResult = (guarded, host, options = {}) => new Promise((resolve) => {
  guarded(host, options, (err, address, family) => resolve({ err, address, family }))
})

/** A dns.lookup stand-in: each name answers the next entry of its list (the last one repeats). */
function fakeDns(answers) {
  const calls = {}
  const lookup = (host, options, callback) => {
    const list = answers[host]
    if (!list) return process.nextTick(callback, Object.assign(new Error(`getaddrinfo ENOTFOUND ${host}`), { code: 'ENOTFOUND' }))
    const n = calls[host] = (calls[host] || 0) + 1
    const address = list[Math.min(n, list.length) - 1]
    const entry = { address, family: address.includes(':') ? 6 : 4 }
    process.nextTick(callback, null, options?.all ? [entry] : entry.address, entry.family)
  }
  return { lookup, calls }
}

test('every refused address class, in any textual form', () => {
  const refused = [
    '127.0.0.1', '127.8.9.10', '0.0.0.0', '10.1.2.3', '172.16.0.1', '172.31.255.255', '192.168.1.1',
    '169.254.169.254', '169.254.1.1', '100.64.0.1', '100.127.255.254', '192.0.0.1', '192.0.0.170', '192.0.2.10',
    '198.18.0.1', '198.19.255.255', '224.0.0.1', '255.255.255.255',
    '::1', '::', '[::1]', '0:0:0:0:0:0:0:1', '0000:0000:0000:0000:0000:0000:0000:0001', '::7f00:1',
    'fe80::1', 'febf::1', 'fec0::1', 'fc00::1', 'fd12:3456::1', 'fd00:ec2::254', 'FD00:EC2:0:0:0:0:0:254', 'ff02::1',
    '::ffff:127.0.0.1', '::ffff:7f00:1', '0:0:0:0:0:ffff:7f00:1', '::ffff:a9fe:a9fe', '::ffff:10.0.0.1',
    '::ffff:0:7f00:1', '::ffff:0:a9fe:a9fe', '::ffff:0:10.0.0.1',
    '64:ff9b::a9fe:a9fe', '64:ff9b::127.0.0.1', '64:ff9b:1::a9fe:a9fe', '64:ff9b:1:abcd::1',
    '2002:a9fe:a9fe::1', '2002:7f00:1::', '2002:c0a8:101::1', '2001:0:4136:e378:8000:63bf:3fff:fdd2', '2001::1', '2001:db8::1',
    'fe80::1%eth0', 'not-an-ip',
  ]
  for (const ip of refused) assert.equal(isPrivateIp(ip), true, ip)
  const allowed = [
    '93.184.216.34', '8.8.8.8', '100.63.255.255', '100.128.0.1', '172.32.0.1', '198.17.255.255', '198.20.0.1',
    // WordPress.com, Jetpack, WooCommerce, IANA: public hosting inside 192.0.0.0/16.
    '192.0.78.9', '192.0.66.5', '192.0.43.8', '192.0.1.1', '192.0.3.1',
    '2606:4700::6810:84e5', '::ffff:93.184.216.34', '::ffff:0:5db8:d822', '64:ff9b::c000:4e09', '64:ff9b::192.0.78.9', '2002:5db8:d822::1', '2001:4860:4860::8888',
  ]
  for (const ip of allowed) assert.equal(isPrivateIp(ip), false, ip)
})

test('internal names, single labels and private literals are refused before any lookup, with any trailing dots', () => {
  const cases = {
    localhost: 'internal hostname',
    'localhost.': 'internal hostname',
    'localhost..': 'internal hostname',
    'api.localhost': 'internal hostname',
    'printer.local': 'internal hostname',
    'mcp-orchestrator.railway.internal': 'internal hostname',
    'foo.internal..': 'internal hostname',
    'metadata.google.internal': 'internal hostname',
    postgres: 'single-label hostname',
    'postgres...': 'single-label hostname',
    '127.0.0.1': 'private address',
    '[fd00:ec2::254]': 'private address',
    '169.254.169.254': 'private address',
  }
  for (const [host, reason] of Object.entries(cases)) assert.equal(hostRefusal(host), reason, host)
  assert.equal(hostRefusal('mcp.example.com'), null)
  assert.equal(hostRefusal('93.184.216.34'), null)
  assert.equal(hostRefusal('mcp.example.com.'), null)
})

test('a public host resolves and is handed to the connection as resolved', async () => {
  const { lookup } = fakeDns({ 'mcp.example.com': ['93.184.216.34'], 'public-api.wordpress.com': ['192.0.78.9'] })
  const guarded = createGuardedLookup({ lookup })
  assert.deepEqual(await lookupResult(guarded, 'mcp.example.com'), { err: null, address: '93.184.216.34', family: 4 })
  assert.deepEqual(await lookupResult(guarded, 'public-api.wordpress.com'), { err: null, address: '192.0.78.9', family: 4 })
  const all = await new Promise((resolve) => guarded('mcp.example.com', { all: true }, (err, list) => resolve({ err, list })))
  assert.deepEqual(all, { err: null, list: [{ address: '93.184.216.34', family: 4 }] })
})

test('a name that resolves to a private or metadata address is refused', async () => {
  const { lookup } = fakeDns({ 'evil.example.com': ['169.254.169.254'], 'six.example.com': ['fd00:ec2::254'], 'cgnat.example.com': ['100.100.1.1'] })
  const guarded = createGuardedLookup({ lookup })
  for (const host of ['evil.example.com', 'six.example.com', 'cgnat.example.com']) {
    const { err } = await lookupResult(guarded, host)
    assert.ok(err instanceof UpstreamRefusedError, host)
    assert.equal(err.reason, 'resolves to a private address')
  }
  const { err } = await lookupResult(guarded, 'mcp-orchestrator.railway.internal')
  assert.equal(err.reason, 'internal hostname')
})

test('one DNS answer mixing a public and a private address is refused, whichever comes first', async () => {
  for (const answers of [['93.184.216.34', '127.0.0.1'], ['127.0.0.1', '93.184.216.34']]) {
    const lookup = (host, options, callback) => process.nextTick(callback, null, answers.map((address) => ({ address, family: 4 })))
    const guarded = createGuardedLookup({ lookup })
    const all = await new Promise((resolve) => guarded('mixed.example.com', { all: true }, (err, list) => resolve({ err, list })))
    assert.ok(all.err instanceof UpstreamRefusedError, answers.join(','))
    assert.equal(all.list, undefined)
    assert.ok((await lookupResult(guarded, 'mixed.example.com')).err instanceof UpstreamRefusedError)
  }
})

test('rebinding: each resolution is checked on the answer it hands over, so the private second answer is refused', async () => {
  const { lookup, calls } = fakeDns({ 'rebind.example.com': ['93.184.216.34', '127.0.0.1'] })
  const guarded = createGuardedLookup({ lookup })
  assert.equal((await lookupResult(guarded, 'rebind.example.com')).address, '93.184.216.34')
  const second = await lookupResult(guarded, 'rebind.example.com')
  assert.ok(second.err instanceof UpstreamRefusedError)
  assert.equal(second.address, undefined)
  assert.equal(calls['rebind.example.com'], 2)
})

test('ORCH_ALLOW_PRIVATE_UPSTREAMS: exact hosts, optionally scoped to a port', async () => {
  const allowed = parseAllowedHosts(' Internal-MCP.railway.internal.. , ,10.0.0.5,[fd00::5],fd00::6,ported.railway.internal:8080,ported.railway.internal:9090')
  assert.deepEqual([...allowed.keys()].sort(), ['10.0.0.5', 'fd00::5', 'fd00::6', 'internal-mcp.railway.internal', 'ported.railway.internal'])
  assert.equal(hostRefusal('internal-mcp.railway.internal', allowed, '3000'), null)
  assert.equal(hostRefusal('INTERNAL-MCP.railway.internal.', allowed, '443'), null)
  assert.equal(hostRefusal('10.0.0.5', allowed, '80'), null)
  assert.equal(hostRefusal('[fd00::5]', allowed, '80'), null)
  assert.equal(hostRefusal('other.railway.internal', allowed, '80'), 'internal hostname')
  assert.equal(hostRefusal('10.0.0.6', allowed, '80'), 'private address')
  // A port-scoped entry lets only its ports through.
  assert.equal(isAllowed(allowed, 'ported.railway.internal', '8080'), true)
  assert.equal(isAllowed(allowed, 'ported.railway.internal', '9090'), true)
  assert.equal(hostRefusal('ported.railway.internal', allowed, '5432'), 'port not on the allow list')
  // Also for a public-looking name: the lookup lets a listed host through, so the port is checked here.
  assert.equal(hostRefusal('svc.example.com', parseAllowedHosts('svc.example.com:8080'), '9000'), 'port not on the allow list')
  assert.equal(hostRefusal('svc.example.com', parseAllowedHosts('svc.example.com:8080'), '8080'), null)
  assert.deepEqual([...parseAllowedHosts('[fd00::7]:9000').get('fd00::7')], ['9000'])
  const { lookup } = fakeDns({ 'internal-mcp.railway.internal': ['10.250.0.7'], 'evil.example.com': ['10.250.0.7'] })
  const guarded = createGuardedLookup({ lookup, allowed })
  assert.equal((await lookupResult(guarded, 'internal-mcp.railway.internal')).address, '10.250.0.7')
  assert.ok((await lookupResult(guarded, 'evil.example.com')).err instanceof UpstreamRefusedError)
})

const FAKE_DISPATCHER = { guarded: true }
const SIGNAL = new AbortController().signal

/** A fetch stand-in that answers each url from a table, recording what it was asked. */
function fakeFetch(routes) {
  const asked = []
  const impl = async (url, init) => {
    asked.push({ url: String(url), method: init.method, headers: new Headers(init.headers), body: init.body, redirect: init.redirect, dispatcher: init.dispatcher, signal: init.signal })
    const route = routes[String(url)]
    if (!route) throw new Error(`unexpected fetch ${url}`)
    return new Response(route.body ?? null, { status: route.status ?? 200, headers: route.headers ?? {} })
  }
  return { impl, asked }
}
const guarded = (url, init = {}, fetchImpl) => guardedFetch(url, { dispatcher: FAKE_DISPATCHER, ...init }, { fetchImpl })

test('guardedFetch refuses to run without a dispatcher', async () => {
  const { impl, asked } = fakeFetch({ 'https://mcp.example.com/mcp': { status: 200 } })
  await assert.rejects(guardedFetch('https://mcp.example.com/mcp', {}, { fetchImpl: impl }), /needs a dispatcher/)
  assert.equal(asked.length, 0)
})

test('a redirect to 127.0.0.1 or an internal name is refused before it is fetched', async () => {
  for (const target of ['http://127.0.0.1:8080/admin', 'http://[::ffff:7f00:1]/', 'http://mcp-orchestrator.railway.internal:3000/', 'http://169.254.169.254/latest/meta-data/', 'http://localhost../']) {
    const { impl, asked } = fakeFetch({ 'https://mcp.example.com/mcp': { status: 302, headers: { location: target } } })
    await assert.rejects(guarded('https://mcp.example.com/mcp', { method: 'GET' }, impl), (err) => err instanceof UpstreamRefusedError)
    assert.equal(asked.length, 1, target)
    assert.equal(asked[0].redirect, 'manual')
  }
})

test('a private url is refused without any fetch, and the scheme must be http(s)', async () => {
  const { impl, asked } = fakeFetch({})
  for (const url of ['http://127.0.0.1/', 'http://localhost:3000/', 'http://postgres:5432/', 'file:///etc/passwd']) {
    await assert.rejects(guarded(url, {}, impl), UpstreamRefusedError, url)
  }
  assert.equal(asked.length, 0)
})

test('redirects: followed hop by hop up to 5, each hop keeps the signal and dispatcher, and credentials never cross origins', async () => {
  // Five redirects: /1 → /2 → /3 → /4 → /5 → b.example.com.
  const routes = { 'https://a.example.com/1': { status: 307, headers: { location: '/2' } } }
  for (let i = 2; i <= 4; i++) routes[`https://a.example.com/${i}`] = { status: 302, headers: { location: `/${i + 1}` } }
  routes['https://a.example.com/5'] = { status: 301, headers: { location: 'https://b.example.com/end' } }
  routes['https://b.example.com/end'] = { status: 200, body: 'ok' }
  const { impl, asked } = fakeFetch(routes)
  const res = await guarded('https://a.example.com/1', { method: 'GET', signal: SIGNAL, headers: { authorization: 'Bearer row-secret', 'x-api-key': 'row-key', accept: 'application/json' } }, impl)
  assert.equal(await res.text(), 'ok')
  assert.equal(asked.length, 6)
  for (const hop of asked) {
    assert.equal(hop.signal, SIGNAL)
    assert.equal(hop.dispatcher, FAKE_DISPATCHER)
  }
  assert.equal(asked[4].headers.get('x-api-key'), 'row-key')
  const crossed = asked[5].headers
  assert.equal(crossed.get('authorization'), null)
  assert.equal(crossed.get('x-api-key'), null)
  assert.equal(crossed.get('accept'), 'application/json')

  // A sixth is refused.
  routes['https://a.example.com/5'] = { status: 302, headers: { location: '/6' } }
  routes['https://a.example.com/6'] = { status: 302, headers: { location: 'https://b.example.com/end' } }
  await assert.rejects(guarded('https://a.example.com/1', {}, fakeFetch(routes).impl), /more than 5 redirects/)
})

test('only 301, 302, 303, 307 and 308 are followed: a 300 with a Location is the response, its body never re-sent', async () => {
  for (const status of [300, 304, 305, 306]) {
    const { impl, asked } = fakeFetch({ 'https://a.example.com/rpc': { status, headers: { location: 'https://other.example.net/collect' } } })
    const res = await guarded('https://a.example.com/rpc', { method: 'POST', body: '{"secret":"args"}' }, impl)
    assert.equal(res.status, status)
    assert.equal(asked.length, 1, String(status))
  }
})

test('a body never follows a 307 or 308 to another origin; a same-host http to https upgrade keeps it', async () => {
  for (const status of [307, 308]) {
    const post = fakeFetch({ 'https://a.example.com/token': { status, headers: { location: 'https://c.example.com/token' } } })
    await assert.rejects(guarded('https://a.example.com/token', { method: 'POST', body: 'client_secret=s' }, post.impl), /another origin with a body/)
    assert.equal(post.asked.length, 1)
  }
  const upgrade = fakeFetch({
    'http://h.example.com/x': { status: 308, headers: { location: 'https://h.example.com/x' } },
    'https://h.example.com/x': { status: 200, body: 'ok' },
  })
  const res = await guarded('http://h.example.com/x', { method: 'POST', body: 'payload', headers: { 'x-api-key': 'row-key' } }, upgrade.impl)
  assert.equal(await res.text(), 'ok')
  assert.equal(upgrade.asked[1].body, 'payload')
  assert.equal(upgrade.asked[1].headers.get('x-api-key'), 'row-key')
  // Not an upgrade: http to https on another host, or onto another port. The body and the
  // credentials stay behind: a 308 is refused, and a 302 goes on as a bare GET.
  for (const to of ['https://b.example.com/x', 'https://h.example.com:8443/x']) {
    const kept = fakeFetch({ 'http://h.example.com/x': { status: 308, headers: { location: to } } })
    await assert.rejects(guarded('http://h.example.com/x', { method: 'POST', body: 'payload', headers: { 'x-api-key': 'row-key' } }, kept.impl), /another origin with a body/, to)
    assert.equal(kept.asked.length, 1)
    const moved = fakeFetch({ 'http://h.example.com/x': { status: 302, headers: { location: to } }, [to]: { status: 200 } })
    await guarded('http://h.example.com/x', { method: 'POST', body: 'payload', headers: { 'x-api-key': 'row-key' } }, moved.impl)
    assert.equal(moved.asked[1].method, 'GET', to)
    assert.equal(moved.asked[1].body, undefined, to)
    assert.equal(moved.asked[1].headers.get('x-api-key'), null, to)
  }
})

test('credentials are dropped when only the scheme (not an upgrade) or only the port changes', async () => {
  for (const [from, to] of [
    ['https://h.example.com/x', 'http://h.example.com/x'],
    ['https://h.example.com/x', 'https://h.example.com:8443/x'],
    ['http://h.example.com:8080/x', 'https://h.example.com:8080/x'],
  ]) {
    const { impl, asked } = fakeFetch({ [from]: { status: 302, headers: { location: to } }, [new URL(to).href]: { status: 200 } })
    await guarded(from, { headers: { authorization: 'Bearer row-secret', 'x-api-key': 'row-key', accept: 'application/json' } }, impl)
    assert.equal(asked[1].headers.get('authorization'), null, `${from} -> ${to}`)
    assert.equal(asked[1].headers.get('x-api-key'), null, `${from} -> ${to}`)
    assert.equal(asked[1].headers.get('accept'), 'application/json')
  }
})

test('a refusal inside a fetch failure is found through its causes', () => {
  const refused = new UpstreamRefusedError('evil.example.com', 'resolves to a private address')
  assert.equal(refusalIn(new TypeError('fetch failed', { cause: refused })), refused)
  assert.equal(refusalIn(new Error('other')), null)
  assert.ok(!refused.message.includes('secret'))
})

// The pin, end to end through undici: the address the guarded lookup hands over is the one dialled.
let local
let hits = 0
before(async () => {
  local = http.createServer((req, res) => { hits++; res.end('reached') })
  await new Promise((resolve) => local.listen(0, '127.0.0.1', resolve))
})
after(() => local?.close())

test('through a real undici Agent: a name resolving to 127.0.0.1 is refused with no connection, unless allowed', async () => {
  const port = local.address().port
  const { lookup } = fakeDns({ 'rebind.example.com': ['127.0.0.1'], 'internal-mcp.example.com': ['127.0.0.1'], 'svc.railway.internal': ['127.0.0.1'] })
  // One entry for any port, one scoped to the port (the form the README recommends).
  const allowed = parseAllowedHosts(`internal-mcp.example.com,svc.railway.internal:${port}`)
  const dispatcher = new Agent({ connect: { lookup: createGuardedLookup({ lookup, allowed }) } })
  try {
    hits = 0
    await assert.rejects(
      guardedFetch(`http://rebind.example.com:${port}/`, { dispatcher }, { allowed }),
      (err) => err instanceof UpstreamRefusedError && err.host === 'rebind.example.com',
    )
    assert.equal(hits, 0)
    const ok = await guardedFetch(`http://internal-mcp.example.com:${port}/`, { dispatcher }, { allowed })
    assert.equal(await ok.text(), 'reached')
    assert.equal(hits, 1)
    const scoped = await guardedFetch(`http://svc.railway.internal:${port}/`, { dispatcher }, { allowed })
    assert.equal(await scoped.text(), 'reached')
    assert.equal(hits, 2)
  } finally {
    await dispatcher.close()
  }
})

test('the orchestrator\'s own names are refused everywhere, before the allow list', async () => {
  const own = parseOwnHosts(' Orch.up.railway.app. ', 'https://orch.railway.internal:3000/x', '', undefined)
  assert.deepEqual([...own].sort(), ['orch.railway.internal', 'orch.up.railway.app'])
  const allowed = parseAllowedHosts('orch.railway.internal:3000,orch.up.railway.app')
  assert.equal(hostRefusal('orch.up.railway.app', allowed, '443', own), 'the orchestrator itself')
  assert.equal(hostRefusal('ORCH.railway.internal.', allowed, '3000', own), 'the orchestrator itself')
  assert.equal(hostRefusal('orch.up.railway.app', allowed, '443'), null)
  assert.equal(hostRefusal('other.up.railway.app', allowed, '443', own), null)
  // The lookup refuses it without resolving, allowed or not.
  const { lookup, calls } = fakeDns({ 'orch.up.railway.app': ['66.33.22.11'] })
  const lookedUp = await lookupResult(createGuardedLookup({ lookup, allowed, own }), 'orch.up.railway.app')
  assert.ok(lookedUp.err instanceof UpstreamRefusedError)
  assert.equal(calls['orch.up.railway.app'], undefined)
  // A redirect to it is refused before it is fetched.
  const { impl, asked } = fakeFetch({ 'https://mcp.example.com/mcp': { status: 302, headers: { location: 'https://orch.up.railway.app/' } } })
  await assert.rejects(
    guardedFetch('https://mcp.example.com/mcp', { dispatcher: FAKE_DISPATCHER }, { fetchImpl: impl, allowed, own }),
    (e) => e instanceof UpstreamRefusedError && /the orchestrator itself/.test(e.message),
  )
  assert.equal(asked.length, 1)
})
