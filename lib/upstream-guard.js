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
  const [a, b] = ip.split('.').map(Number)
  return (
    a === 0 || a === 10 || a === 127 ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    (a === 192 && b === 0) ||
    (a === 198 && (b === 18 || b === 19)) ||
    a >= 224
  )
}

/** The IPv4 address in the last 32 bits of an IPv6 tail, written dotted or as two hex groups. */
function embeddedV4(tail) {
  if (isIP(tail) === 4) return tail
  const m = /^([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(tail)
  if (!m) return null
  const hi = parseInt(m[1], 16)
  const lo = parseInt(m[2], 16)
  return `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`
}

/** Loopback, private, link-local, CGNAT, multicast, unique-local, IPv4-mapped or NAT64 to any of those. */
export function isPrivateIp(ip) {
  const raw = String(ip || '').replace(/^\[|\]$/g, '').toLowerCase()
  if (isIP(raw) === 4) return isPrivateV4(raw)
  if (isIP(raw) !== 6) return true
  for (const prefix of ['::ffff:', '64:ff9b::']) {
    if (raw.startsWith(prefix)) {
      const v4 = embeddedV4(raw.slice(prefix.length))
      return v4 === null || isPrivateV4(v4)
    }
  }
  return (
    raw.startsWith('::') ||
    raw.startsWith('fc') || raw.startsWith('fd') ||
    raw.startsWith('fe8') || raw.startsWith('fe9') || raw.startsWith('fea') || raw.startsWith('feb') ||
    raw.startsWith('fec') || raw.startsWith('fed') || raw.startsWith('fee') || raw.startsWith('fef') ||
    raw.startsWith('ff')
  )
}

/** ORCH_ALLOW_PRIVATE_UPSTREAMS: exact hosts (lowercase, no port), comma-separated. */
export function parseAllowedHosts(raw) {
  return new Set(String(raw || '').split(',').map((h) => h.trim().toLowerCase().replace(/^\[|\]$/g, '')).filter(Boolean))
}

const bareHost = (hostname) => String(hostname || '').toLowerCase().replace(/^\[|\]$/g, '').replace(/\.$/, '')

/** Why this host may not be dialled before any lookup, or null. An allowed host is never refused. */
export function hostRefusal(hostname, allowed = new Set()) {
  const host = bareHost(hostname)
  if (allowed.has(host)) return null
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
export function createGuardedLookup({ allowed = new Set(), lookup = dns.lookup } = {}) {
  return function guardedLookup(hostname, options, callback) {
    if (typeof options === 'function') {
      callback = options
      options = {}
    }
    const host = bareHost(hostname)
    const early = hostRefusal(host, allowed)
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

/**
 * fetch for an upstream url: refuses a private or internal host before dialling (an IP literal
 * never reaches the lookup), follows at most MAX_REDIRECTS redirects itself, checking every hop the
 * same way, keeps only Accept and Content-Type when a hop changes origin, and never carries a body
 * to another origin. `dispatcher` must carry
 * createGuardedLookup, which checks every name actually dialled.
 */
export async function guardedFetch(input, init = {}, { allowed = new Set(), fetchImpl = fetch } = {}) {
  let url = new URL(input)
  let method = String(init.method || 'GET').toUpperCase()
  let body = init.body
  const headers = new Headers(init.headers)
  for (let hop = 0; ; hop++) {
    if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new UpstreamRefusedError(bareHost(url.hostname), `scheme ${url.protocol}`)
    const refused = hostRefusal(url.hostname, allowed)
    if (refused) throw new UpstreamRefusedError(bareHost(url.hostname), refused)
    let response
    try {
      response = await fetchImpl(url, { ...init, method, body, headers, redirect: 'manual' })
    } catch (err) {
      throw refusalIn(err) || err
    }
    const location = response.headers.get('location')
    if (response.status < 300 || response.status > 399 || !location) return response
    await response.body?.cancel().catch(() => {})
    if (hop === MAX_REDIRECTS) throw new UpstreamRefusedError(bareHost(url.hostname), `more than ${MAX_REDIRECTS} redirects`)
    const next = new URL(location, url)
    const keepsBody = body != null && (response.status === 307 || response.status === 308)
    // A body (tool arguments, a client secret) never follows a redirect to another origin.
    if (keepsBody && next.origin !== url.origin) throw new UpstreamRefusedError(bareHost(next.hostname), 'redirect to another origin with a body')
    if (next.origin !== url.origin) for (const name of Array.from(headers.keys())) if (!CROSS_ORIGIN_HEADERS.has(name)) headers.delete(name)
    if (response.status === 303 || ((response.status === 301 || response.status === 302) && method === 'POST')) {
      method = 'GET'
      body = undefined
      headers.delete('content-type')
    }
    url = next
  }
}
