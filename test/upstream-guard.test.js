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
  isPrivateIp,
  parseAllowedHosts,
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

test('every refused address class', () => {
  const refused = [
    '127.0.0.1', '127.8.9.10', '0.0.0.0', '10.1.2.3', '172.16.0.1', '172.31.255.255', '192.168.1.1',
    '169.254.169.254', '169.254.1.1', '100.64.0.1', '100.127.255.254', '224.0.0.1', '255.255.255.255',
    '::1', '::', '[::1]', 'fe80::1', 'febf::1', 'fc00::1', 'fd12:3456::1', 'fd00:ec2::254', 'ff02::1',
    '::ffff:127.0.0.1', '::ffff:7f00:1', '::ffff:a9fe:a9fe', '::ffff:10.0.0.1', '64:ff9b::a9fe:a9fe', 'not-an-ip',
  ]
  for (const ip of refused) assert.equal(isPrivateIp(ip), true, ip)
  for (const ip of ['93.184.216.34', '8.8.8.8', '100.63.255.255', '100.128.0.1', '172.32.0.1', '2606:4700::6810:84e5', '::ffff:93.184.216.34']) {
    assert.equal(isPrivateIp(ip), false, ip)
  }
})

test('internal names, single labels and private literals are refused before any lookup', () => {
  const cases = {
    localhost: 'internal hostname',
    'api.localhost': 'internal hostname',
    'printer.local': 'internal hostname',
    'mcp-orchestrator.railway.internal': 'internal hostname',
    'metadata.google.internal': 'internal hostname',
    postgres: 'single-label hostname',
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
  const { lookup } = fakeDns({ 'mcp.example.com': ['93.184.216.34'] })
  const guarded = createGuardedLookup({ lookup })
  assert.deepEqual(await lookupResult(guarded, 'mcp.example.com'), { err: null, address: '93.184.216.34', family: 4 })
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

test('rebinding: each resolution is checked on the answer it hands over, so the private second answer is refused', async () => {
  const { lookup, calls } = fakeDns({ 'rebind.example.com': ['93.184.216.34', '127.0.0.1'] })
  const guarded = createGuardedLookup({ lookup })
  assert.equal((await lookupResult(guarded, 'rebind.example.com')).address, '93.184.216.34')
  const second = await lookupResult(guarded, 'rebind.example.com')
  assert.ok(second.err instanceof UpstreamRefusedError)
  assert.equal(second.address, undefined)
  assert.equal(calls['rebind.example.com'], 2)
})

test('ORCH_ALLOW_PRIVATE_UPSTREAMS: exact hosts only', async () => {
  const allowed = parseAllowedHosts(' Internal-MCP.railway.internal , ,10.0.0.5,[fd00::5]')
  assert.deepEqual([...allowed].sort(), ['10.0.0.5', 'fd00::5', 'internal-mcp.railway.internal'])
  assert.equal(hostRefusal('internal-mcp.railway.internal', allowed), null)
  assert.equal(hostRefusal('INTERNAL-MCP.railway.internal', allowed), null)
  assert.equal(hostRefusal('10.0.0.5', allowed), null)
  assert.equal(hostRefusal('[fd00::5]', allowed), null)
  assert.equal(hostRefusal('other.railway.internal', allowed), 'internal hostname')
  assert.equal(hostRefusal('10.0.0.6', allowed), 'private address')
  const { lookup } = fakeDns({ 'internal-mcp.railway.internal': ['10.250.0.7'], 'evil.example.com': ['10.250.0.7'] })
  const guarded = createGuardedLookup({ lookup, allowed })
  assert.equal((await lookupResult(guarded, 'internal-mcp.railway.internal')).address, '10.250.0.7')
  assert.ok((await lookupResult(guarded, 'evil.example.com')).err instanceof UpstreamRefusedError)
})

/** A fetch stand-in that answers each url from a table, recording what it was asked. */
function fakeFetch(routes) {
  const asked = []
  const impl = async (url, init) => {
    asked.push({ url: String(url), method: init.method, headers: new Headers(init.headers), body: init.body, redirect: init.redirect })
    const route = routes[String(url)]
    if (!route) throw new Error(`unexpected fetch ${url}`)
    return new Response(route.body ?? null, { status: route.status ?? 200, headers: route.headers ?? {} })
  }
  return { impl, asked }
}

test('a redirect to 127.0.0.1 or an internal name is refused before it is fetched', async () => {
  for (const target of ['http://127.0.0.1:8080/admin', 'http://[::ffff:7f00:1]/', 'http://mcp-orchestrator.railway.internal:3000/', 'http://169.254.169.254/latest/meta-data/']) {
    const { impl, asked } = fakeFetch({ 'https://mcp.example.com/mcp': { status: 302, headers: { location: target } } })
    await assert.rejects(guardedFetch('https://mcp.example.com/mcp', { method: 'GET' }, { fetchImpl: impl }), (err) => err instanceof UpstreamRefusedError)
    assert.equal(asked.length, 1, target)
    assert.equal(asked[0].redirect, 'manual')
  }
})

test('a private url is refused without any fetch, and the scheme must be http(s)', async () => {
  const { impl, asked } = fakeFetch({})
  for (const url of ['http://127.0.0.1/', 'http://localhost:3000/', 'http://postgres:5432/', 'file:///etc/passwd']) {
    await assert.rejects(guardedFetch(url, {}, { fetchImpl: impl }), UpstreamRefusedError, url)
  }
  assert.equal(asked.length, 0)
})

test('redirects: followed hop by hop up to 5, and credentials or a body never cross to another origin', async () => {
  // Five redirects: /1 → /2 → /3 → /4 → /5 → b.example.com.
  const routes = { 'https://a.example.com/1': { status: 307, headers: { location: '/2' } } }
  for (let i = 2; i <= 4; i++) routes[`https://a.example.com/${i}`] = { status: 302, headers: { location: `/${i + 1}` } }
  routes['https://a.example.com/5'] = { status: 301, headers: { location: 'https://b.example.com/end' } }
  routes['https://b.example.com/end'] = { status: 200, body: 'ok' }
  const { impl, asked } = fakeFetch(routes)
  const res = await guardedFetch('https://a.example.com/1', { method: 'GET', headers: { authorization: 'Bearer row-secret', 'x-api-key': 'row-key', accept: 'application/json' } }, { fetchImpl: impl })
  assert.equal(await res.text(), 'ok')
  assert.equal(asked.length, 6)
  assert.equal(asked[4].headers.get('x-api-key'), 'row-key')
  const crossed = asked[5].headers
  assert.equal(crossed.get('authorization'), null)
  assert.equal(crossed.get('x-api-key'), null)
  assert.equal(crossed.get('accept'), 'application/json')

  // A sixth is refused.
  routes['https://a.example.com/5'] = { status: 302, headers: { location: '/6' } }
  routes['https://a.example.com/6'] = { status: 302, headers: { location: 'https://b.example.com/end' } }
  await assert.rejects(guardedFetch('https://a.example.com/1', {}, { fetchImpl: fakeFetch(routes).impl }), /more than 5 redirects/)

  const post = fakeFetch({ 'https://a.example.com/token': { status: 307, headers: { location: 'https://c.example.com/token' } } })
  await assert.rejects(guardedFetch('https://a.example.com/token', { method: 'POST', body: 'client_secret=s' }, { fetchImpl: post.impl }), /another origin with a body/)
  assert.equal(post.asked.length, 1)
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
  const { lookup } = fakeDns({ 'rebind.example.com': ['127.0.0.1'], 'internal-mcp.example.com': ['127.0.0.1'] })
  const allowed = parseAllowedHosts('internal-mcp.example.com')
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
  } finally {
    await dispatcher.close()
  }
})
