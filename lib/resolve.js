// The orchestrator's pure parts: the inbound key check, the upstream resolution
// and its cache, and the per-upstream session key. server.js wires them to the
// HTTP server and Supabase; tests drive them directly (node --test).

import { timingSafeEqual } from 'node:crypto'

export const INBOUND_KEY_HEADER = 'x-orchestrator-key'

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

function headerValue(headers, key) {
  const value = headers?.[key]
  const first = Array.isArray(value) ? value[0] : value
  return first ? String(first) : ''
}

/**
 * Whether a request may use the orchestrator. With no expected key configured
 * every request passes (`open: true`, the caller warns at startup). Otherwise
 * the x-orchestrator-key header must equal it, compared in constant time; a
 * missing header or one of another length fails.
 */
export function checkInboundKey(headers, expectedKey) {
  if (!expectedKey) return { ok: true, open: true }
  const given = headerValue(headers, INBOUND_KEY_HEADER)
  if (!given) return { ok: false, reason: 'missing' }
  const a = Buffer.from(given, 'utf8')
  const b = Buffer.from(String(expectedKey), 'utf8')
  if (a.length !== b.length) return { ok: false, reason: 'wrong' }
  return timingSafeEqual(a, b) ? { ok: true, open: false } : { ok: false, reason: 'wrong' }
}

/** The startup line when no inbound key is set, or null. */
export function inboundKeyWarning(expectedKey) {
  return expectedKey
    ? null
    : '[MCP Orchestrator] WARNING: ORCH_INBOUND_KEY is not set. Every request is accepted without a key. Set it, and MCP_ORCHESTRATOR_KEY in Nexus, before exposing this service.'
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
 *   be MCP-capable, have a url and not be inactive. Nothing else is tried when it
 *   does not resolve, so a call never lands on another integration.
 * - x-mcp-integration-name is never used to resolve.
 * - With no id header, the static upstreams (MCP_UPSTREAMS) are tried by tool name,
 *   or the only one when there is exactly one.
 * Resolved rows are cached by id for ttlMs, so an edited row takes effect within it.
 */
export function createResolver({
  fetchIntegrationById,
  buildAuthHeaders,
  normalizeUrl,
  ttlMs,
  defaultTimeoutMs,
  staticUpstreams = [],
  autoDiscover = true,
  now = () => Date.now(),
}) {
  const integrationCache = new Map()

  function cached(id) {
    const entry = integrationCache.get(id)
    if (entry && entry.expiresAt > now()) return entry.upstream
    integrationCache.delete(id)
    return null
  }

  async function fromRow(id) {
    const hit = cached(id)
    if (hit) return hit
    if (!autoDiscover || !fetchIntegrationById) return null
    const integration = await fetchIntegrationById(id)
    if (!integration || integration.is_active === false) return null
    if (String(integration.id || '').toLowerCase() !== id) return null
    const mcpConfig = extractMcpConfigFromIntegration(integration)
    if (!mcpConfig || !mcpConfig.url) return null
    const upstream = {
      name: integration.name || id,
      url: normalizeUrl(mcpConfig.url),
      headers: await buildAuthHeaders(mcpConfig, id),
      timeoutMs: Number(mcpConfig.timeoutMs || mcpConfig.timeout_ms) || defaultTimeoutMs,
      integrationId: id,
    }
    integrationCache.set(id, { upstream, expiresAt: now() + ttlMs })
    return upstream
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

  /** { upstream } or { upstream: null, reason } ('bad_id' | 'not_found' | 'no_upstream'). */
  async function resolve(headers, toolName) {
    const id = integrationIdOf(headers)
    if (id === null) return { upstream: null, reason: 'bad_id' }
    if (id) {
      const upstream = await fromRow(id)
      return upstream ? { upstream } : { upstream: null, reason: 'not_found' }
    }
    const upstream = fromStatic(toolName)
    return upstream ? { upstream } : { upstream: null, reason: 'no_upstream' }
  }

  return { resolve, integrationCache }
}
