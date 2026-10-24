// SSRF guard for upstream fetches: an integration's url (and its OAuth token url) is whatever a
// tenant typed, so it must never reach loopback, private, link-local, CGNAT or cloud-metadata
// addresses, or Railway's private network, from this service. Ported from Nexus
// lib/net/public-host.ts, plus the pin: the guarded lookup is the one the connection itself uses
// (undici Agent connect.lookup), so a DNS answer that changes between a check and the connect
// (rebinding) is checked on the answer actually dialled.

import dns from 'node:dns'
import { isIP } from 'node:net'

export const MAX_REDIRECTS = 5
/** The only headers a redirect to another origin keeps: credentials (any header name a row
 *  configured) and the upstream's session id belong to the first origin. */
const CROSS_ORIGIN_HEADERS = new Set(['accept', 'content-type'])

/** A refused upstream: the host and why, never a credential. */
export class UpstreamRefusedError extends Error {
  constructor(host, reason) {
    super(`upstream_refused: ${host} (${reason})`)
    this.name = 'UpstreamRefusedError'
    this.code = 'upstream_refused'
    this.host = host
    this.reason = reason
  }
}

function isPrivateV4(ip) {
  const [a, b, c] = ip.split('.').map(Number)
  return (
    a === 0 || a === 10 || a === 127 ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    (a === 192 && b === 0 && c === 0) ||
    (a === 192 && b === 0 && c === 2) ||
    (a === 198 && (b === 18 || b === 19)) ||
    a >= 224
  )
}

/** An IPv6 address as its eight 16-bit groups, whatever its textual form; null when it is not one. */
function v6Groups(ip) {
  let canonical
  try {
    canonical = new URL(`http://[${ip}]`).hostname.slice(1, -1)
  } catch {
    return null
  }
  const [head, tail] = canonical.includes('::') ? canonical.split('::') : [canonical, null]
  const left = head ? head.split(':') : []
  const right = tail ? tail.split(':') : []
  const groups = tail === null ? left : [...left, ...Array(8 - left.length - right.length).fill('0'), ...right]
  return groups.length === 8 ? groups.map((g) => parseInt(g, 16)) : null
}

const v4Of = (hi, lo) => `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`

/**
 * Loopback, private, link-local, CGNAT, multicast, documentation, unique-local, site-local, Teredo,
 * local-use NAT64, or an IPv4-mapped / NAT64 / 6to4 address embedding any of those. Any textual
 * form: IPv6 is read into its groups first.
 */
export function isPrivateIp(ip) {
  const raw = String(ip || '').trim().replace(/^\[|\]$/g, '').toLowerCase()
  if (isIP(raw) === 4) return isPrivateV4(raw)
  if (isIP(raw) !== 6) return true
  const g = v6Groups(raw)
  if (!g) return true
  const zeroTo = (n) => g.slice(0, n).every((x) => x === 0)
  if (zeroTo(5) && g[5] === 0xffff) return isPrivateV4(v4Of(g[6], g[7])) // ::ffff:a.b.c.d
  if (zeroTo(6)) return true // ::, ::1 and IPv4-compatible
  if (g[0] === 0x64 && g[1] === 0xff9b && g.slice(2, 6).every((x) => x === 0)) return isPrivateV4(v4Of(g[6], g[7])) // NAT64
  if (g[0] === 0x64 && g[1] === 0xff9b && g[2] === 1) return true // local-use NAT64
  if (g[0] === 0x2002) return isPrivateV4(v4Of(g[1], g[2])) // 6to4
  if (g[0] === 0x2001 && g[1] === 0) return true // Teredo
  if (g[0] === 0x2001 && g[1] === 0x0db8) return true // documentation
  return (
    (g[0] & 0xfe00) === 0xfc00 || // unique-local, which includes fd00:ec2::254
    (g[0] & 0xffc0) === 0xfe80 || // link-local
    (g[0] & 0xffc0) === 0xfec0 || // site-local
    (g[0] & 0xff00) === 0xff00 // multicast
  )
}

const bareHost = (hostname) => String(hostname || '').trim().toLowerCase().replace(/^\[|\]$/g, '').replace(/\.+$/, '')

/** A URL's port, the scheme's default when it names none. */
export const portOf = (url) => url.port || (url.protocol === 'https:' ? '443' : '80')

const NO_HOSTS = new Set()

/**
 * The orchestrator's own hostnames (RAILWAY_PUBLIC_DOMAIN, RAILWAY_PRIVATE_DOMAIN, the legacy
 * RAILWAY_STATIC_URL), as a Set of
 * bare hosts. A value given as a url or with a port counts by its hostname; empty values are skipped.
 */
export function parseOwnHosts(...values) {
  const own = new Set()
  for (const value of values) {
    const raw = String(value || '').trim()
    if (!raw) continue
    let host
    try {
      host = new URL(/^[a-z][a-z0-9+.-]*:\/\//i.test(raw) ? raw : `http://${raw}`).hostname
    } catch {
      host = raw
    }
    if (bareHost(host)) own.add(bareHost(host))
  }
  return own
}

/**
 * ORCH_ALLOW_PRIVATE_UPSTREAMS: comma-separated `host` or `host:port` (`[v6]:port` for IPv6).
 * A Map of host to the ports allowed, or null for any port.
 */
export function parseAllowedHosts(raw) {
  const allowed = new Map()
  for (const entry of String(raw || '').split(',').map((e) => e.trim()).filter(Boolean)) {
    const m = /^\[([^\]]+)\](?::(\d+))?$/.exec(entry) || (entry.split(':').length === 2 ? /^([^:]+):(\d+)$/.exec(entry) : null)
    const host = bareHost(m ? m[1] : entry)
    const port = m ? m[2] : undefined
    if (!host) continue
    const ports = allowed.has(host) ? allowed.get(host) : new Set()
    if (!port || ports === null) allowed.set(host, null)
    else allowed.set(host, ports.add(port))
  }
  return allowed
}

/** Whether the allow list lets this host (and, when the entry names one, this port) through. */
export function isAllowed(allowed, hostname, port) {
  const host = bareHost(hostname)
  if (!allowed?.has(host)) return false
  const ports = allowed.get(host)
  return ports === null || (port !== undefined && ports.has(String(port)))
}

/**
 * Why this host may not be dialled before any lookup, or null. One of the orchestrator's own names
 * is always refused; any other allowed host never is.
 * Without a port (the lookup knows none) any entry for the host counts; guardedFetch passes the
 * port, and a listed host on a port its entry does not name is refused.
 */
export function hostRefusal(hostname, allowed = new Map(), port, own = NO_HOSTS) {
  const host = bareHost(hostname)
  // Before the allow list: listing its own name must not let the orchestrator call itself.
  if (own.has(host)) return 'the orchestrator itself'
  if (port === undefined ? allowed?.has(host) : isAllowed(allowed, host, port)) return null
  // The lookup lets a listed host through on any port, so another port is refused here.
  if (port !== undefined && allowed?.has(host)) return 'port not on the allow list'
  if (!host) return 'no host'
  if (isIP(host)) return isPrivateIp(host) ? 'private address' : null
  if (host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local') || host.endsWith('.internal')) {
    return 'internal hostname'
  }
  if (!host.includes('.')) return 'single-label hostname'
  return null
}

/**
 * A dns.lookup-compatible function for undici's connect: it resolves once, refuses the host when
 * any answer is private (unless allowed), and hands the connection exactly the answers it checked.
 */
export function createGuardedLookup({ allowed = new Map(), own = NO_HOSTS, lookup = dns.lookup } = {}) {
  return function guardedLookup(hostname, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    const host = bareHost(hostname)
    const early = hostRefusal(host, allowed, undefined, own)
    if (early) return process.nextTick(callback, new UpstreamRefusedError(host, early))
    lookup(hostname, { ...options, all: true }, (err, addresses) => {
      if (err) return callback(err)
      const list = Array.isArray(addresses) ? addresses : [{ address: addresses, family: isIP(addresses) }]
      if (!list.length) return callback(Object.assign(new Error(`no address for ${host}`), { code: 'ENOTFOUND' }))
      if (!allowed.has(host) && list.some((a) => isPrivateIp(a.address))) {
        return callback(new UpstreamRefusedError(host, 'resolves to a private address'))
      }
      if (options?.all) return callback(null, list)
      callback(null, list[0].address, list[0].family)
    })
  }
}

/** The UpstreamRefusedError somewhere in a fetch failure's cause chain, or null. */
export function refusalIn(error) {
  for (let e = error, depth = 0; e && depth < 6; e = e.cause, depth++) {
    if (e instanceof UpstreamRefusedError || e?.code === 'upstream_refused') return e
  }
  return null
}

const REDIRECTS = new Set([301, 302, 303, 307, 308])

/** Same origin, or the same host moving from http to https on the default ports (an upgrade). */
function sameOrigin(from, to) {
  if (from.origin === to.origin) return true
  return from.protocol === 'http:' && to.protocol === 'https:' && from.hostname === to.hostname && !from.port && !to.port
}

/**
 * fetch for an upstream url: refuses a private or internal host before dialling (an IP literal
 * never reaches the lookup), follows only 301/302/303/307/308 and at most MAX_REDIRECTS of them,
 * checking every hop the same way, keeps only Accept and Content-Type when a hop changes origin,
 * and never carries a body to another origin. `init.dispatcher` must carry createGuardedLookup,
 * which checks every name actually dialled; without one this refuses to fetch at all.
 */
export async function guardedFetch(input, init = {}, { allowed = new Map(), own = NO_HOSTS, fetchImpl = fetch } = {}) {
  if (!init.dispatcher) throw new Error('guardedFetch needs a dispatcher whose connect.lookup is createGuardedLookup')
  let url = new URL(input)
  let method = String(init.method || 'GET').toUpperCase()
  let body = init.body
  const headers = new Headers(init.headers)
  for (let hop = 0; ; hop++) {
    if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new UpstreamRefusedError(bareHost(url.hostname), `scheme ${url.protocol}`)
    const refused = hostRefusal(url.hostname, allowed, portOf(url), own)
    if (refused) throw new UpstreamRefusedError(bareHost(url.hostname), refused)
    let response
    try {
      response = await fetchImpl(url, { ...init, method, body, headers, redirect: 'manual' })
    } catch (err) {
      throw refusalIn(err) || err
    }
    const location = response.headers.get('location')
    if (!REDIRECTS.has(response.status) || !location) return response
    await response.body?.cancel().catch(() => {})
    if (hop === MAX_REDIRECTS) throw new UpstreamRefusedError(bareHost(url.hostname), `more than ${MAX_REDIRECTS} redirects`)
    const next = new URL(location, url)
    const same = sameOrigin(url, next)
    const keepsBody = body != null && (response.status === 307 || response.status === 308)
    // A body (tool arguments, a client secret) never follows a redirect to another origin.
    if (keepsBody && !same) throw new UpstreamRefusedError(bareHost(next.hostname), 'redirect to another origin with a body')
    if (!same) for (const name of Array.from(headers.keys())) if (!CROSS_ORIGIN_HEADERS.has(name)) headers.delete(name)
    if (response.status === 303 || ((response.status === 301 || response.status === 302) && method === 'POST')) {
      method = 'GET'
      body = undefined
      headers.delete('content-type')
    }
    url = next
  }
}
