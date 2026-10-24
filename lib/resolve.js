// The orchestrator's pure parts: the inbound key check and its rate-limited
// refusal log, the upstream resolution and its cache, and the per-integration
// cache keys (upstream session, client-credentials token, Apify ids). server.js wires them to the
// HTTP server and Supabase; tests drive them directly (node --test).

import { createHash, timingSafeEqual } from 'node:crypto'

export const INBOUND_KEY_HEADER = 'x-orchestrator-key'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

function headerValue(headers, key) {
  const value = headers?.[key]
  const first = Array.isArray(value) ? value[0] : value
  return first ? String(first) : ''
}

const digest = (value) => createHash('sha256').update(String(value), 'utf8').digest()

export const MIN_INBOUND_KEY_LENGTH = 32

/**
 * ORCH_INBOUND_KEY as a list: comma-separated (current,next while rotating), trimmed, empty entries
 * skipped. An entry shorter than MIN_INBOUND_KEY_LENGTH is a typo, not a key ('<current>,x' would
 * otherwise accept 'x'): it is left out of keys and counted in ignored.
 */
export function parseInboundKeys(raw) {
  const entries = String(raw || '').split(',').map((k) => k.trim()).filter(Boolean)
  const keys = entries.filter((k) => k.length >= MIN_INBOUND_KEY_LENGTH)
  return { keys, ignored: entries.length - keys.length }
}

/**
 * Who may use the orchestrator.
 * - 'key': every request must carry one of the keys in x-orchestrator-key.
 * - 'closed': no key is set but a Supabase service key is, or ORCH_INBOUND_KEY
 *   was set with only entries too short to count, so every request is refused:
 *   an open orchestrator would hand out any tenant's credentials.
 * - 'open': no key is set, nothing was ignored, and either there is no service
 *   key or ORCH_ALLOW_UNAUTHENTICATED=true; every request passes.
 * ORCH_ALLOW_UNAUTHENTICATED=true opens 'closed' in both cases.
 * Each key is compared by its SHA-256 digest in constant time, so neither the
 * key nor its length shows in the timing.
 */
export function createInboundGuard({ keys = [], ignoredKeys = 0, serviceKeyConfigured = false, allowUnauthenticated = false }) {
  const digests = keys.map(digest)
  const mode = digests.length ? 'key' : ((serviceKeyConfigured || ignoredKeys > 0) && !allowUnauthenticated ? 'closed' : 'open')

  function check(headers) {
    if (mode === 'open') return { ok: true }
    if (mode === 'closed') return { ok: false, reason: 'not configured' }
    const given = headerValue(headers, INBOUND_KEY_HEADER).trim()
    if (!given) return { ok: false, reason: 'missing' }
    const got = digest(given)
    let match = false
    for (const expected of digests) match = timingSafeEqual(got, expected) || match
    return match ? { ok: true } : { ok: false, reason: 'wrong' }
  }

  const startupLine = {
    key: `[MCP Orchestrator] Inbound key required on every request but /health (${digests.length} key${digests.length === 1 ? '' : 's'} accepted)`,
    closed: `[MCP Orchestrator] WARNING: ORCH_INBOUND_KEY ${ignoredKeys ? `has no entry of at least ${MIN_INBOUND_KEY_LENGTH} characters` : 'is not set'}. Every request but /health is refused (401). Set it, and MCP_ORCHESTRATOR_KEY in Nexus, or set ORCH_ALLOW_UNAUTHENTICATED=true to accept requests without a key.`,
    open: '[MCP Orchestrator] WARNING: ORCH_INBOUND_KEY is not set. Every request is accepted without a key' + (serviceKeyConfigured || ignoredKeys ? ' (ORCH_ALLOW_UNAUTHENTICATED=true).' : ' (no Supabase service key configured).'),
  }[mode]
  // Which entries were ignored is never said: not even a position or length of a key goes to the log.
  const ignoredLine = ignoredKeys
    ? `[MCP Orchestrator] WARNING: ${ignoredKeys} ORCH_INBOUND_KEY entr${ignoredKeys === 1 ? 'y is' : 'ies are'} shorter than ${MIN_INBOUND_KEY_LENGTH} characters and ignored. A key must be at least ${MIN_INBOUND_KEY_LENGTH} characters.`
    : null

  return { mode, check, startupLine, ignoredLine }
}

/**
 * At most one line per key (a remote address) per window; the next line after
 * a quiet window says how many were left out.
 */
export function createRateLimitedLog({ windowMs = 60_000, log = console.warn, now = () => Date.now(), maxKeys = 1_000 } = {}) {
  const seen = new Map()
  return (key, line) => {
    const at = now()
    const entry = seen.get(key)
    if (entry && at - entry.at < windowMs) {
      entry.suppressed += 1
      return
    }
    if (!entry && seen.size >= maxKeys) {
      for (const [k, v] of seen) if (at - v.at >= windowMs) seen.delete(k)
      if (seen.size >= maxKeys) seen.delete(seen.keys().next().value)
    }
    const suppressed = entry?.suppressed || 0
    seen.set(key, { at, suppressed: 0 })
    log(suppressed ? `${line} (${suppressed} more from this address suppressed)` : line)
  }
}

/** The client-credentials token cache key: per integration, so another row naming the same client never gets this row's token. */
export function oauthTokenCacheKey(integrationId, config) {
  return `${integrationId || ''}_${config?.oauthTokenUrl || ''}_${config?.oauthClientId || ''}`
}

/** The Apify run and dataset cache key: by integration id only, a name is not unique across tenants. */
export function apifyCacheKey(integrationId) {
  return integrationId ? `id:${integrationId}` : 'default'
}

/** A header value fit for a log line: printable ASCII only, at most 80 characters. */
export function logLabel(value) {
  return String(value || '').replace(/[^\x20-\x7e]/g, '?').slice(0, 80)
}

/** The integration id header, as a lowercase uuid, or '' when absent. null when present but not a uuid. */
export function integrationIdOf(headers) {
  const raw = headerValue(headers, 'x-mcp-integration-id').trim()
  if (!raw) return ''
  return UUID.test(raw) ? raw.toLowerCase() : null
}

/** The MCP config of an agent_integrations row: an MCP row's config, or an API row's enabled wrapper. */
export function extractMcpConfigFromIntegration(integration) {
  const config = integration?.config || {}
  if (integration?.integration_type === 'mcp') {
    return config
  }
  if (integration?.integration_type === 'api') {
    const wrapperEnabled = config.wrapperEnabled === true
    const wrapper = config.wrapper
    if (wrapperEnabled && wrapper && typeof wrapper === 'object') {
      return wrapper
    }
  }
  return null
}

/**
 * The key a cached upstream session is stored under: the integration id for a
 * row-backed upstream, so two tenants never share a session; a static upstream
 * (MCP_UPSTREAMS) by its configured name or url, in a namespace of its own.
 */
export function sessionKeyFor(upstream) {
  if (upstream?.integrationId) return `id:${upstream.integrationId}`
  return `static:${String(upstream?.name || upstream?.url || '').trim().toLowerCase()}`
}

/**
 * Resolves the upstream of a request.
 * - An x-mcp-integration-id header resolves only by that id: the row must exist,
 *   be MCP-capable and have a url. Nothing else is tried when it does not
 *   resolve, so a call never lands on another integration. is_active is not
 *   checked: it is the chat toggle, and Nexus decides which rows it calls.
 * - x-mcp-integration-name is never used to resolve.
 * - With no id header, the static upstreams (MCP_UPSTREAMS) are tried by tool name,
 *   or the only one when there is exactly one.
 * Resolved rows are cached by id for ttlMs, so an edited row takes effect within it.
 * fetchIntegrationById returns the row or null, and throws when the read fails:
 * a failed refresh keeps serving the expired entry (retried every retryMs) for
 * at most maxStaleMs after its last good read, a missing row drops it. Concurrent
 * reads of one id share one fetchIntegrationById call. An id that resolves
 * nothing is remembered for notFoundTtlMs.
 * The auth headers are built on every resolve from the cached config, so a
 * client-credentials token is never sent past its expiry.
 */
export function createResolver({
  fetchIntegrationById,
  buildAuthHeaders,
  normalizeUrl,
  ttlMs,
  defaultTimeoutMs,
  staticUpstreams = [],
  autoDiscover = true,
  notFoundTtlMs = 10_000,
  retryMs = 30_000,
  maxStaleMs = 60 * 60_000,
  log = console.warn,
  now = () => Date.now(),
}) {
  const integrationCache = new Map()
  const reading = new Map()

  function read(id) {
    let pending = reading.get(id)
    if (!pending) {
      pending = Promise.resolve().then(() => fetchIntegrationById(id)).finally(() => reading.delete(id))
      reading.set(id, pending)
    }
    return pending
  }

  async function withHeaders(entry) {
    const { mcpConfig, ...upstream } = entry.upstream
    return { ...upstream, headers: await buildAuthHeaders(mcpConfig, upstream.integrationId) }
  }

  async function fromRow(id) {
    const entry = integrationCache.get(id)
    if (entry && entry.expiresAt > now()) return entry.upstream ? withHeaders(entry) : null
    if (!autoDiscover || !fetchIntegrationById) return null

    let integration
    try {
      integration = await read(id)
    } catch (error) {
      if (entry?.upstream && now() - entry.readAt <= maxStaleMs) {
        log(`[MCP Orchestrator] Integration ${id} could not be read again (${logLabel(error?.message || error)}); serving the cached entry`)
        entry.expiresAt = now() + Math.min(ttlMs, retryMs)
        return withHeaders(entry)
      }
      if (entry?.upstream) {
        // Too long without a good read: an edit or a delete made meanwhile must not wait for the database.
        integrationCache.delete(id)
        log(`[MCP Orchestrator] Integration ${id} could not be read again (${logLabel(error?.message || error)}); the cached entry is older than ${Math.round(maxStaleMs / 60_000)} min and dropped`)
        return { failed: true }
      }
      log(`[MCP Orchestrator] Integration ${id} could not be read (${logLabel(error?.message || error)})`)
      return { failed: true }
    }

    const mcpConfig = integration && String(integration.id || '').toLowerCase() === id
      ? extractMcpConfigFromIntegration(integration)
      : null
    if (!mcpConfig || !mcpConfig.url) {
      integrationCache.set(id, { upstream: null, expiresAt: now() + notFoundTtlMs })
      return null
    }
    const fresh = {
      upstream: {
        name: integration.name || id,
        url: normalizeUrl(mcpConfig.url),
        timeoutMs: Number(mcpConfig.timeoutMs || mcpConfig.timeout_ms) || defaultTimeoutMs,
        integrationId: id,
        mcpConfig,
      },
      expiresAt: now() + ttlMs,
      readAt: now(),
    }
    integrationCache.set(id, fresh)
    return withHeaders(fresh)
  }

  function fromStatic(toolName) {
    if (toolName) {
      for (const candidate of staticUpstreams) {
        const includes = Array.isArray(candidate?.toolNameIncludes) ? candidate.toolNameIncludes : []
        if (includes.some((entry) => toolName.toLowerCase().includes(String(entry).toLowerCase()))) return candidate
      }
    }
    return staticUpstreams.length === 1 ? staticUpstreams[0] : null
  }

  /** { upstream } or { upstream: null, reason } ('bad_id' | 'not_found' | 'lookup_failed' | 'no_upstream'). */
  async function resolve(headers, toolName) {
    const id = integrationIdOf(headers)
    if (id === null) return { upstream: null, reason: 'bad_id' }
    if (id) {
      const upstream = await fromRow(id)
      if (upstream?.failed) return { upstream: null, reason: 'lookup_failed' }
      return upstream ? { upstream } : { upstream: null, reason: 'not_found' }
    }
    const upstream = fromStatic(toolName)
    return upstream ? { upstream } : { upstream: null, reason: 'no_upstream' }
  }

  return { resolve, integrationCache }
}
