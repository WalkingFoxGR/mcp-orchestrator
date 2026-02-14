import http from 'node:http'
import { createClient } from '@supabase/supabase-js'

const PORT = Number(process.env.PORT || 3000)
const DEFAULT_TIMEOUT_MS = Number(process.env.ORCH_TIMEOUT_MS || 50000)
const MAX_ARG_BYTES = Number(process.env.ORCH_MAX_ARG_BYTES || 65536)
const MAX_RESULT_BYTES = Number(process.env.ORCH_MAX_RESULT_BYTES || 200000)
const STREAM_RESPONSES = process.env.ORCH_STREAM === 'true'
const SUPABASE_URL = process.env.SUPABASE_URL || ''
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_KEY || ''
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'mcp_tool_cache'
const SUPABASE_INTEGRATIONS_TABLE = process.env.SUPABASE_INTEGRATIONS_TABLE || 'agent_integrations'
const SUPABASE_INTEGRATIONS_CACHE_TTL_MS = Number(process.env.SUPABASE_INTEGRATIONS_CACHE_TTL_MS || 300000)
const AUTO_DISCOVER_UPSTREAMS = process.env.ORCH_AUTO_DISCOVER !== 'false'
const UPSTREAMS = parseJson(process.env.MCP_UPSTREAMS || process.env.UPSTREAMS || '[]') || []
const APIFY_DATASET_CACHE_TTL_MS = Number(process.env.APIFY_DATASET_CACHE_TTL_MS || 600000)

const supabase = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } })
  : null

const sessionCache = new Map()
const oauthTokenCache = new Map()
const integrationCache = new Map()
const upstreamMap = new Map()
const apifyDatasetCache = new Map()
const apifyRunCache = new Map()
const TOOL_SCHEMAS = {
  list_workflows: {
    name: 'list_workflows',
    inputs: {},
    outputType: 'list',
    listItemFields: ['id', 'name', 'active', 'updatedAt'],
  },
  execute_workflow: {
    name: 'execute_workflow',
    inputs: {
      id: {
        type: 'string',
        required: true,
        pattern: /^[a-zA-Z0-9_-]+$/,
        transform: (val) => {
          if (/^\d+$/.test(String(val))) {
            throw new Error('Invalid workflow ID. Use actual ID like "4cwjrvunc564B0XO", not list number.')
          }
          return String(val)
        },
      },
    },
    outputType: 'single',
  },
  'instagram-scraper': {
    name: 'instagram-scraper',
    inputs: {
      resultsLimit: {
        type: 'number',
        default: 5,
        transform: (val) => Number(val) || 5,
      },
    },
    outputType: 'list',
    listItemFields: ['id', 'shortCode', 'caption', 'likesCount', 'commentsCount', 'type'],
    timeout: 50000,
  },
}
const DEFAULT_LIST_FIELDS = ['id', 'name', 'type', 'url', 'status']
for (const upstream of UPSTREAMS) {
  if (upstream && upstream.name) {
    upstreamMap.set(normalizeKey(upstream.name), upstream)
  }
}

function parseJson(value) {
  if (!value) return null
  try {
    return JSON.parse(value)
  } catch {
    return null
  }
}

function normalizeKey(value) {
  return String(value || '').trim().toLowerCase()
}

function normalizeToolKey(value) {
  return String(value || '').trim().toLowerCase().replace(/[\s_]+/g, '-')
}

function parseJsonValue(value) {
  if (typeof value !== 'string') return value
  const trimmed = value.trim()
  if (!trimmed) return value
  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    try {
      return JSON.parse(trimmed)
    } catch {
      return value
    }
  }
  return value
}

function extractListCandidate(value) {
  const parsed = parseJsonValue(value)
  if (Array.isArray(parsed)) return parsed
  if (parsed && typeof parsed === 'object') {
    const record = parsed
    const keys = ['data', 'items', 'results']
    for (const key of keys) {
      const nested = parseJsonValue(record[key])
      if (Array.isArray(nested)) return nested
    }
  }
  return null
}

function getApifyCacheKey(integrationId, integrationName) {
  const raw = integrationId || integrationName || ''
  return String(raw).trim() || 'default'
}

function isApifyToolContext(toolName, integrationLabel) {
  const label = `${integrationLabel || ''} ${toolName || ''}`.toLowerCase()
  return label.includes('apify') || label.includes('scraper')
}

function isApifyGetActorOutputTool(toolName) {
  const normalized = normalizeToolKey(toolName)
  return normalized.includes('get-actor-output')
}

function isApifyGetActorRunTool(toolName) {
  const normalized = normalizeToolKey(toolName)
  return normalized.includes('get-actor-run')
}

function isPlaceholderDatasetId(value) {
  if (value === undefined || value === null) return true
  if (typeof value !== 'string') return true
  const trimmed = value.trim()
  if (!trimmed) return true
  const normalized = trimmed.toLowerCase()
  return new Set([
    'your-dataset-id-here',
    'your-dataset-id',
    'dataset-id',
    'datasetid',
    'dataset_id',
  ]).has(normalized)
}

function getCachedApifyDatasetId(cacheKey) {
  const cached = apifyDatasetCache.get(cacheKey)
  if (cached && cached.expiresAt > Date.now()) return cached.datasetId
  apifyDatasetCache.delete(cacheKey)
  return null
}

function setCachedApifyDatasetId(cacheKey, datasetId) {
  apifyDatasetCache.set(cacheKey, {
    datasetId,
    expiresAt: Date.now() + APIFY_DATASET_CACHE_TTL_MS,
  })
}

function getCachedApifyRunId(cacheKey) {
  const cached = apifyRunCache.get(cacheKey)
  if (cached && cached.expiresAt > Date.now()) return cached.runId
  apifyRunCache.delete(cacheKey)
  return null
}

function setCachedApifyRunId(cacheKey, runId) {
  apifyRunCache.set(cacheKey, {
    runId,
    expiresAt: Date.now() + APIFY_DATASET_CACHE_TTL_MS,
  })
}

function extractApifyDatasetId(value) {
  let payload = value
  if (typeof payload === 'string') {
    const parsed = parseJson(payload)
    if (!parsed) return null
    payload = parsed
  }

  const queue = [payload]
  const seen = new Set()
  while (queue.length > 0) {
    const current = queue.shift()
    if (!current || typeof current !== 'object') continue
    if (seen.has(current)) continue
    seen.add(current)

    if (Array.isArray(current)) {
      for (const item of current) {
        queue.push(item)
      }
      continue
    }

    const record = current
    const candidate = record.defaultDatasetId || record.datasetId
    if (typeof candidate === 'string' && candidate.trim()) {
      return candidate.trim()
    }

    const nestedKeys = ['runInfo', 'actRun', 'run', 'result', 'data', 'output', 'dataset']
    for (const key of nestedKeys) {
      const nested = record[key]
      if (nested && typeof nested === 'object') {
        queue.push(nested)
      }
    }
  }

  return null
}

function extractApifyRunId(value) {
  let payload = value
  if (typeof payload === 'string') {
    const parsed = parseJson(payload)
    if (!parsed) return null
    payload = parsed
  }

  const queue = [payload]
  const seen = new Set()
  while (queue.length > 0) {
    const current = queue.shift()
    if (!current || typeof current !== 'object') continue
    if (seen.has(current)) continue
    seen.add(current)

    if (Array.isArray(current)) {
      for (const item of current) {
        queue.push(item)
      }
      continue
    }

    const record = current
    const candidate = record.runId || record.id
    if (typeof candidate === 'string' && candidate.trim()) {
      return candidate.trim()
    }

    const nestedKeys = ['runInfo', 'actRun', 'run', 'result', 'data', 'output']
    for (const key of nestedKeys) {
      const nested = record[key]
      if (nested && typeof nested === 'object') {
        queue.push(nested)
      }
    }
  }

  return null
}
function normalizeUrl(input) {
  const trimmed = String(input || '').trim()
  if (!trimmed) return trimmed
  if (/^https?:\/\//i.test(trimmed)) return trimmed
  return `https://${trimmed}`
}

function coerceHeaderValue(value) {
  if (Array.isArray(value)) return value[0]
  return value
}

function extractIntegrationHeader(req, key) {
  const value = coerceHeaderValue(req.headers[key])
  return value ? String(value) : ''
}

function parseHeadersValue(value) {
  if (!value) return {}
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value)
      return parsed && typeof parsed === 'object' ? parsed : {}
    } catch {
      return {}
    }
  }
  if (typeof value === 'object') return value
  return {}
}

async function getOAuthToken(cacheKey, config) {
  const cached = oauthTokenCache.get(cacheKey)
  if (cached && cached.expiresAt > Date.now()) {
    return cached.token
  }

  const tokenUrl = config.oauthTokenUrl
  const clientId = config.oauthClientId
  const clientSecret = config.oauthClientSecret
  const scope = config.oauthScope
  const audience = config.oauthAudience

  if (!tokenUrl || !clientId || !clientSecret) return null

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: clientId,
    client_secret: clientSecret,
  })
  if (scope) body.set('scope', scope)
  if (audience) body.set('audience', audience)

  const response = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
    signal: AbortSignal.timeout(15000),
  })

  if (!response.ok) {
    console.error(`[MCP Orchestrator] OAuth token request failed: ${response.status}`)
    return null
  }

  const data = await response.json().catch(() => ({}))
  if (!data.access_token) return null

  const expiresInMs = (data.expires_in || 3600) * 1000
  oauthTokenCache.set(cacheKey, {
    token: data.access_token,
    expiresAt: Date.now() + Math.max(60000, expiresInMs - 60000),
  })

  return data.access_token
}

async function buildAuthHeaders(config) {
  const headers = {}
  const authType = config.authType || 'none'

  if (authType === 'bearer' && config.bearerToken) {
    headers.Authorization = `Bearer ${config.bearerToken}`
  } else if (authType === 'api_key' && config.apiKeyValue) {
    const headerName = config.apiKeyHeader || 'x-api-key'
    headers[headerName] = config.apiKeyValue
  } else if (authType === 'basic' && config.basicUsername && config.basicPassword) {
    const token = Buffer.from(`${config.basicUsername}:${config.basicPassword}`).toString('base64')
    headers.Authorization = `Basic ${token}`
  } else if (authType === 'oauth_access_token' && config.oauthAccessToken) {
    const tokenType = config.oauthTokenType || 'Bearer'
    headers.Authorization = `${tokenType} ${config.oauthAccessToken}`
  } else if (authType === 'oauth_client_credentials') {
    const cacheKey = `${config.oauthTokenUrl || ''}_${config.oauthClientId || ''}`
    const token = await getOAuthToken(cacheKey, config)
    if (token) headers.Authorization = `Bearer ${token}`
  }

  const extraHeaders = parseHeadersValue(config.headers)
  if (extraHeaders && typeof extraHeaders === 'object') {
    Object.assign(headers, extraHeaders)
  }

  return headers
}

function extractMcpConfigFromIntegration(integration) {
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

function getCachedIntegration(key) {
  if (!key) return null
  const cached = integrationCache.get(key)
  if (cached && cached.expiresAt > Date.now()) return cached.upstream
  integrationCache.delete(key)
  return null
}

async function fetchIntegrationConfig(integrationId, integrationName) {
  if (!supabase) return null
  if (!integrationId && !integrationName) return null

  if (integrationId) {
    const { data, error } = await supabase
      .from(SUPABASE_INTEGRATIONS_TABLE)
      .select('id,name,config,integration_type,is_active,is_verified,updated_at')
      .eq('id', integrationId)
      .maybeSingle()
    if (error) {
      console.error('[MCP Orchestrator] Integration lookup failed:', error.message)
      return null
    }
    return data || null
  }

  const { data, error } = await supabase
    .from(SUPABASE_INTEGRATIONS_TABLE)
    .select('id,name,config,integration_type,is_active,is_verified,updated_at')
    .eq('name', integrationName)
    .order('updated_at', { ascending: false })
    .limit(1)
    .maybeSingle()
  if (error) {
    console.error('[MCP Orchestrator] Integration lookup failed:', error.message)
    return null
  }
  return data || null
}

function cacheIntegration(key, upstream) {
  if (!key || !upstream) return
  integrationCache.set(key, {
    upstream,
    expiresAt: Date.now() + SUPABASE_INTEGRATIONS_CACHE_TTL_MS,
  })
}

async function resolveUpstream(req, toolName) {
  const nameHeader = extractIntegrationHeader(req, 'x-mcp-integration-name')
  const idHeader = extractIntegrationHeader(req, 'x-mcp-integration-id')
  const cacheKey = normalizeKey(idHeader || nameHeader)
  let upstream = null

  // Prefer integration ID resolution first to avoid collisions when multiple
  // integrations share the same human-readable name.
  if (idHeader) {
    upstream = getCachedIntegration(normalizeKey(idHeader))
    if (!upstream) {
      upstream = upstreamMap.get(normalizeKey(idHeader))
    }
  }
  if (!upstream && nameHeader) {
    upstream = getCachedIntegration(normalizeKey(nameHeader))
    if (!upstream) {
      upstream = upstreamMap.get(normalizeKey(nameHeader))
    }
  }
  if (!upstream && cacheKey) {
    upstream = getCachedIntegration(cacheKey)
  }
  if (!upstream && AUTO_DISCOVER_UPSTREAMS && supabase && (idHeader || nameHeader)) {
    const integration = await fetchIntegrationConfig(idHeader, nameHeader)
    if (integration && integration.is_active !== false) {
      const mcpConfig = extractMcpConfigFromIntegration(integration)
      if (mcpConfig && mcpConfig.url) {
        const headers = await buildAuthHeaders(mcpConfig)
        upstream = {
          name: integration.name || nameHeader || idHeader,
          url: normalizeUrl(mcpConfig.url),
          headers,
          timeoutMs: Number(mcpConfig.timeoutMs || mcpConfig.timeout_ms) || DEFAULT_TIMEOUT_MS,
          cacheKey: integration.id || idHeader || integration.name,
        }
        cacheIntegration(cacheKey, upstream)
        upstreamMap.set(normalizeKey(integration.id), upstream)
        upstreamMap.set(normalizeKey(integration.name), upstream)
      }
    }
  }
  if (!upstream && toolName) {
    for (const candidate of UPSTREAMS) {
      const includes = Array.isArray(candidate?.toolNameIncludes) ? candidate.toolNameIncludes : []
      if (includes.some((entry) => toolName.toLowerCase().includes(String(entry).toLowerCase()))) {
        upstream = candidate
        break
      }
    }
  }
  if (!upstream && UPSTREAMS.length === 1) {
    upstream = UPSTREAMS[0]
  }
  return upstream || null
}

async function ensureUpstreamSession(upstream) {
  const cacheKey = normalizeKey(upstream.cacheKey || upstream.name || upstream.url)
  const cached = sessionCache.get(cacheKey)
  if (cached && cached.expiresAt > Date.now()) return cached.sessionId

  let sessionId = null
  try {
    const initResponse = await sendRpc(upstream, {
      jsonrpc: '2.0',
      id: Date.now(),
      method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        clientInfo: { name: 'mcp-orchestrator', version: '0.1.0' },
      },
    }, upstream.timeoutMs || DEFAULT_TIMEOUT_MS)

    // Extract session ID from response header (per MCP spec, stateful servers set this)
    sessionId = initResponse?.sessionId || initResponse?.headers?.['mcp-session-id'] || null

    // Check if upstream returned an error in the JSON-RPC response
    if (initResponse?.data?.error) {
      console.warn(`[orchestrator] initialize error from ${upstream.name || upstream.url}: ${initResponse.data.error.message || 'unknown'}`)
      // Still continue — some servers may accept tools/call without init
    }
  } catch (initErr) {
    console.warn(`[orchestrator] initialize failed for ${upstream.name || upstream.url}: ${initErr instanceof Error ? initErr.message : 'unknown'}`)
    // Continue without session — stateless servers or wrappers may still work
    return null
  }

  if (sessionId) {
    sessionCache.set(cacheKey, {
      sessionId,
      expiresAt: Date.now() + 5 * 60 * 1000,
    })
  }

  // Run notification + tools/list in parallel to reduce handshake time
  const notifPromise = sendRpc(upstream, {
    jsonrpc: '2.0',
    method: 'notifications/initialized',
  }, 5000, sessionId).catch(() => null)

  const listPromise = sendRpc(upstream, {
    jsonrpc: '2.0',
    id: Date.now() + 1,
    method: 'tools/list',
    params: {},
  }, upstream.timeoutMs || DEFAULT_TIMEOUT_MS, sessionId).catch((err) => {
    console.warn(`[orchestrator] tools/list warmup failed for ${upstream.name || upstream.url}: ${err instanceof Error ? err.message : 'unknown'}`)
  })

  await Promise.allSettled([notifPromise, listPromise])

  return sessionId
}

function sanitizeJsonText(input) {
  return String(input || '').replace(/[\u0000-\u0019]+/g, ' ')
}

function normalizeParsedArgs(parsed) {
  if (!parsed) return null
  if (typeof parsed === 'object' && !Array.isArray(parsed)) return parsed
  if (typeof parsed === 'string') {
    const trimmed = parsed.trim()
    if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
      try {
        return normalizeParsedArgs(JSON.parse(trimmed))
      } catch {
        return null
      }
    }
  }
  return null
}

function repairToolArguments(rawArgs) {
  const trimmed = String(rawArgs || '').trim()
  if (!trimmed) return {}
  const objectStart = trimmed.indexOf('{')
  const arrayStart = trimmed.indexOf('[')
  let start = objectStart
  let endChar = '}'
  if (arrayStart !== -1 && (arrayStart < objectStart || objectStart === -1)) {
    start = arrayStart
    endChar = ']'
  }
  if (start === -1) return null
  const end = trimmed.lastIndexOf(endChar)
  if (end === -1 || end <= start) return null
  const candidate = sanitizeJsonText(trimmed.slice(start, end + 1))
  try {
    const parsed = JSON.parse(candidate)
    return normalizeParsedArgs(parsed)
  } catch {
    return null
  }
}

function parseToolArguments(rawArgs) {
  if (!rawArgs) return {}
  if (typeof rawArgs === 'object') return rawArgs
  if (typeof rawArgs === 'string') {
    try {
      const parsed = JSON.parse(rawArgs)
      return normalizeParsedArgs(parsed) || {}
    } catch {
      return repairToolArguments(rawArgs) || {}
    }
  }
  return {}
}

function findToolSchema(toolName) {
  if (TOOL_SCHEMAS[toolName]) return TOOL_SCHEMAS[toolName]
  const normalizedTool = normalizeToolKey(toolName)
  for (const [key, schema] of Object.entries(TOOL_SCHEMAS)) {
    const normalizedKey = normalizeToolKey(key)
    if (normalizedTool.includes(normalizedKey)) {
      return schema
    }
  }
  return null
}

function validateToolParams(toolName, params) {
  const schema = findToolSchema(toolName)
  if (!schema) return params

  const validated = { ...params }
  for (const [key, def] of Object.entries(schema.inputs)) {
    if (validated[key] === undefined && def.default !== undefined) {
      validated[key] = def.default
    }

    if (validated[key] !== undefined && typeof def.transform === 'function') {
      validated[key] = def.transform(validated[key])
    }

    if (validated[key] !== undefined && def.pattern instanceof RegExp) {
      if (!def.pattern.test(String(validated[key]))) {
        throw new Error(`Invalid format for ${key}`)
      }
    }

    if (def.required && validated[key] === undefined) {
      throw new Error(`Missing required parameter: ${key}`)
    }
  }

  return validated
}

function buildMinimalN8nWorkflowPayload(name) {
  const safeName = String(name || '').trim() || 'New Workflow'
  return {
    name: safeName,
    nodes: [
      {
        name: 'When clicking "Execute workflow"',
        type: 'n8n-nodes-base.manualTrigger',
        typeVersion: 1,
        position: [260, 560],
        parameters: {},
      },
    ],
    connections: {},
    settings: {},
  }
}

function normalizeCreateWorkflowArgs(toolName, args) {
  if (!toolName.toLowerCase().includes('create_workflow')) return args
  const next = { ...args }
  if (next.workflow && typeof next.workflow === 'object') {
    next.workflow = { ...next.workflow }
  } else if (next.body && typeof next.body === 'object') {
    next.workflow = { ...next.body }
  } else if (next.name) {
    next.workflow = buildMinimalN8nWorkflowPayload(next.name)
  } else {
    next.workflow = buildMinimalN8nWorkflowPayload('New Workflow')
  }

  if (!Array.isArray(next.workflow.nodes) || next.workflow.nodes.length === 0) {
    const name = next.workflow.name || next.name || 'New Workflow'
    next.workflow = buildMinimalN8nWorkflowPayload(name)
  }

  next.name = next.name || next.workflow.name
  return next
}

function enforceArgLimit(toolName, args) {
  const serialized = JSON.stringify(args)
  const size = Buffer.byteLength(serialized, 'utf8')
  if (size <= MAX_ARG_BYTES) return { args, size, truncated: false }
  if (toolName.toLowerCase().includes('create_workflow')) {
    const fallback = normalizeCreateWorkflowArgs(toolName, { name: args?.name || 'New Workflow' })
    return { args: fallback, size, truncated: true }
  }
  return { args, size, truncated: true, error: `Arguments exceed ${MAX_ARG_BYTES} bytes.` }
}

function summarizeToolItem(item) {
  const summary = {}
  const maxFieldLength = 200
  const preferredKeys = [
    'id',
    'name',
    'title',
    'shortCode',
    'url',
    'status',
    'createdAt',
    'updatedAt',
    'timestamp',
    'likesCount',
    'commentsCount',
    'type',
  ]

  for (const key of preferredKeys) {
    if (item?.[key] !== undefined) {
      const value = item[key]
      if (typeof value === 'string' && value.length > maxFieldLength) {
        summary[key] = `${value.slice(0, maxFieldLength)}...`
      } else {
        summary[key] = value
      }
    }
  }

  if (Object.keys(summary).length === 0 && item && typeof item === 'object') {
    const fallbackKeys = Object.keys(item).filter((key) => key !== 'id').slice(0, 6)
    for (const key of fallbackKeys) {
      const value = item[key]
      if (typeof value === 'string' && value.length > maxFieldLength) {
        summary[key] = `${value.slice(0, maxFieldLength)}...`
      } else {
        summary[key] = value
      }
    }
  }

  return summary
}

function summarizeToolResult(result) {
  try {
    let parsed = JSON.parse(result)
    if (typeof parsed === 'string') {
      const trimmed = parsed.trim()
      if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
        parsed = JSON.parse(trimmed)
      }
    }
    if (Array.isArray(parsed)) {
      const items = parsed.map((item) => (item && typeof item === 'object' ? summarizeToolItem(item) : item))
      return JSON.stringify({ _totalCount: parsed.length, items })
    }
    if (parsed && typeof parsed === 'object') {
      const record = parsed
      const list = Array.isArray(record.data)
        ? record.data
        : Array.isArray(record.items)
          ? record.items
          : null
      if (list) {
        const items = list.map((item) => (item && typeof item === 'object' ? summarizeToolItem(item) : item))
        if (Array.isArray(record.data)) {
          return JSON.stringify({ ...record, _totalCount: list.length, data: items })
        }
        return JSON.stringify({ ...record, _totalCount: list.length, items })
      }
    }
  } catch {
    return null
  }
  return null
}

function normalizeToolOutput(toolName, result) {
  const schema = findToolSchema(toolName)

  if (result === null || result === undefined) {
    return { success: false, type: 'raw', error: 'No result' }
  }

  const parsed = parseJsonValue(result)
  if (typeof parsed === 'string') {
    return { success: true, type: 'raw', data: parsed }
  }

  if (Array.isArray(parsed)) {
    return normalizeToolList(parsed, schema)
  }

  if (parsed && typeof parsed === 'object') {
    const record = parsed
    const list = extractListCandidate(record.data)
      || extractListCandidate(record.items)
      || extractListCandidate(record.results)
    if (list) {
      const normalized = normalizeToolList(list, schema)
      return { ...normalized, data: record }
    }
  }

  return { success: true, type: 'single', data: parsed }
}

function normalizeToolList(items, schema) {
  const fields = (schema && schema.listItemFields) || DEFAULT_LIST_FIELDS
  const normalizedItems = items.map((item, index) => {
    if (!item || typeof item !== 'object') {
      return { _index: index + 1, value: item }
    }

    const record = item
    const normalized = { _index: index + 1 }

    if (record.id !== undefined) {
      normalized.id = String(record.id)
    }

    for (const field of fields) {
      if (record[field] === undefined) continue
      const value = record[field]
      if (typeof value === 'string' && value.length > 200) {
        normalized[field] = `${value.slice(0, 200)}...`
      } else {
        normalized[field] = value
      }
    }

    return normalized
  })

  return {
    success: true,
    type: 'list',
    totalCount: items.length,
    shownCount: normalizedItems.length,
    items: normalizedItems,
  }
}

function renderToolList(output) {
  if (!output || output.type !== 'list' || !Array.isArray(output.items)) {
    return JSON.stringify(output?.data ?? output ?? {})
  }

  const items = output.items
  if (items.length === 0) return 'No items found.'

  const fieldSet = new Set()
  fieldSet.add('_index')
  for (const item of items) {
    Object.keys(item || {}).forEach((key) => fieldSet.add(key))
  }
  const fields = Array.from(fieldSet).filter((field) => field !== '_index')
  fields.unshift('#')

  const header = `| ${fields.join(' | ')} |`
  const divider = `| ${fields.map(() => '---').join(' | ')} |`

  const rows = items.map((item) => {
    const cells = fields.map((field) => {
      if (field === '#') return String(item._index)
      const value = item[field]
      if (value === undefined) return ''
      return String(value).replace(/\|/g, '\\|').replace(/\n/g, ' ')
    })
    return `| ${cells.join(' | ')} |`
  })

  const total = output.totalCount ?? items.length
  return [
    `**Total: ${total} items**`,
    '',
    header,
    divider,
    ...rows,
  ].join('\n')
}

function extractResultPayload(result) {
  if (!result || typeof result !== 'object') return { value: result, text: '' }
  if (result.structuredContent !== undefined) {
    const value = result.structuredContent
    return { value, text: typeof value === 'string' ? value : JSON.stringify(value) }
  }
  if (Array.isArray(result.content)) {
    const text = result.content
      .filter((entry) => entry && entry.type === 'text' && typeof entry.text === 'string')
      .map((entry) => entry.text)
      .join('\n')
    return { value: text, text }
  }
  return { value: result, text: JSON.stringify(result) }
}

function applyReliabilityLayer(toolName, upstreamResult) {
  if (!upstreamResult || typeof upstreamResult !== 'object') return upstreamResult
  if (upstreamResult.isError) return upstreamResult

  const payload = extractResultPayload(upstreamResult)
  const normalized = normalizeToolOutput(toolName, payload.value)
  const rendered = normalized.type === 'list'
    ? renderToolList(normalized)
    : JSON.stringify(normalized)

  return {
    ...upstreamResult,
    structuredContent: normalized,
    content: [{ type: 'text', text: rendered }],
  }
}

async function cachePayload(payload, meta) {
  if (!supabase) return null
  try {
    const { data, error } = await supabase
      .from(SUPABASE_TABLE)
      .insert({
        payload,
        meta,
        byte_size: Buffer.byteLength(JSON.stringify(payload), 'utf8'),
      })
      .select('id')
      .single()
    if (error) {
      console.error('[MCP Orchestrator] Supabase cache error:', error.message)
      return null
    }
    return data?.id || null
  } catch (error) {
    console.error('[MCP Orchestrator] Supabase cache failed:', error)
    return null
  }
}

async function normalizeUpstreamResult(toolName, upstreamResult) {
  // Pass through the raw upstream result — the orchestrator should be transparent.
  // Only intervene when the result exceeds the size limit (cache + truncate).
  const payload = extractResultPayload(upstreamResult)
  const resultText = payload.text || (typeof payload.value === 'string' ? payload.value : JSON.stringify(payload.value))
  if (!resultText) return upstreamResult

  if (Buffer.byteLength(resultText, 'utf8') <= MAX_RESULT_BYTES) {
    return upstreamResult
  }

  // Result too large — cache the full payload in Supabase and return a truncated preview
  const summary = summarizeToolResult(resultText)
  const summaryPayload = summary ? parseJson(summary) || summary : resultText.slice(0, 2000)
  const cachedId = await cachePayload({ toolName, result: payload.value }, { toolName, truncated: true })

  const wrapped = typeof summaryPayload === 'string'
    ? { preview: summaryPayload }
    : { ...summaryPayload }

  wrapped._cached = Boolean(cachedId)
  if (cachedId) wrapped._cacheKey = cachedId
  wrapped._truncated = true
  wrapped._originalSize = Buffer.byteLength(resultText, 'utf8')

  return {
    ...upstreamResult,
    structuredContent: wrapped,
    content: [{ type: 'text', text: JSON.stringify(wrapped) }],
  }
}

async function sendRpc(upstream, message, timeoutMs = DEFAULT_TIMEOUT_MS, sessionId = null) {
  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'text/event-stream, application/json',
    ...(upstream.headers || {}),
  }
  if (sessionId) headers['mcp-session-id'] = sessionId

  const res = await fetch(upstream.url, {
    method: 'POST',
    headers,
    body: JSON.stringify(message),
    signal: AbortSignal.timeout(timeoutMs),
  })

  const nextSessionId = res.headers.get('mcp-session-id')
  const contentType = res.headers.get('content-type') || ''
  let data = null

  if (contentType.includes('text/event-stream')) {
    const text = await res.text()
    const line = text.split('\n').find((entry) => entry.startsWith('data:'))
    if (line) {
      const jsonStr = line.slice(5).trim()
      try {
        data = JSON.parse(jsonStr)
      } catch {
        data = null
      }
    }
  } else if (contentType.includes('application/json')) {
    data = await res.json().catch(() => null)
  } else {
    const text = await res.text()
    data = parseJson(text) || null
  }

  return { data, sessionId: nextSessionId, status: res.status }
}

function buildJsonRpcError(id, message, code = -32000) {
  return { jsonrpc: '2.0', id, error: { code, message } }
}

function buildJsonRpcResult(id, result) {
  return { jsonrpc: '2.0', id, result }
}

function respond(res, payload, preferStream) {
  if (preferStream) {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' })
    res.end(`data: ${JSON.stringify(payload)}\n\n`)
    return
  }
  res.writeHead(200, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(payload))
}

async function handleToolCall(req, body) {
  const id = body.id ?? null
  const params = body.params || {}
  const toolName = params.name || ''
  const integrationName = extractIntegrationHeader(req, 'x-mcp-integration-name')
  const integrationId = extractIntegrationHeader(req, 'x-mcp-integration-id')
  const integrationLabel = integrationName || integrationId || 'unknown'
  const apifyCacheKey = getApifyCacheKey(integrationId, integrationName)
  console.log(`[MCP Orchestrator] tools/call ${toolName} integration=${integrationLabel}`)
  let args = parseToolArguments(params.arguments || params.args || {})
  args = normalizeCreateWorkflowArgs(toolName, args)
  if (isApifyGetActorOutputTool(toolName)) {
    const cachedDatasetId = getCachedApifyDatasetId(apifyCacheKey)
    const cachedRunId = getCachedApifyRunId(apifyCacheKey)
    const datasetArg = args?.datasetId
    const needsInjection = isPlaceholderDatasetId(datasetArg)
      || (cachedRunId && cachedDatasetId && String(datasetArg).trim() === String(cachedRunId).trim())
    if (needsInjection && cachedDatasetId) {
      args = { ...args, datasetId: cachedDatasetId }
      console.log(`[MCP Orchestrator] Injected cached Apify dataset ID for ${toolName}`)
    } else if (needsInjection) {
      console.warn(`[MCP Orchestrator] Missing cached Apify dataset ID for ${toolName}`)
    }
  }

  if (isApifyGetActorRunTool(toolName) && (!args?.runId || String(args.runId).trim() === '')) {
    const cachedRunId = getCachedApifyRunId(apifyCacheKey)
    if (cachedRunId) {
      args = { ...args, runId: cachedRunId }
      console.log(`[MCP Orchestrator] Injected cached Apify run ID for ${toolName}`)
    }
  }
  const { args: boundedArgs, error: argError, truncated } = enforceArgLimit(toolName, args)
  if (argError) {
    return buildJsonRpcError(id, argError, -32602)
  }
  let safeArgs = boundedArgs
  try {
    safeArgs = validateToolParams(toolName, boundedArgs)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Invalid tool parameters'
    return buildJsonRpcError(id, message, -32602)
  }
  if (truncated) {
    safeArgs._orchestrator_note = `Arguments truncated to stay under ${MAX_ARG_BYTES} bytes.`
  }

  const upstream = await resolveUpstream(req, toolName)
  if (!upstream) {
    return buildJsonRpcError(id, `No upstream configured for tool ${toolName}`, -32601)
  }

  let sessionId = await ensureUpstreamSession(upstream)
  const rpc = {
    jsonrpc: '2.0',
    id: Date.now(),
    method: 'tools/call',
    params: {
      name: toolName,
      arguments: safeArgs,
    },
  }

  let response = await sendRpc(upstream, rpc, upstream.timeoutMs || DEFAULT_TIMEOUT_MS, sessionId)

  // Stale session retry: if upstream returns "Tool not found" or session error, clear cache and retry once
  if (response.data?.error) {
    const errMsg = (response.data.error.message || '').toLowerCase()
    const errCode = response.data.error.code
    const isSessionError = errMsg.includes('tool not found') || errMsg.includes('session') || errMsg.includes('invalid') || errCode === -32000 || errCode === -32601 || errCode === -32603
    if (isSessionError) {
      console.log(`[MCP Orchestrator] Session error for ${toolName}, retrying with fresh session...`)
      const cacheKey = normalizeKey(upstream.cacheKey || upstream.name || upstream.url)
      sessionCache.delete(cacheKey)
      sessionId = await ensureUpstreamSession(upstream)
      rpc.id = Date.now()
      response = await sendRpc(upstream, rpc, upstream.timeoutMs || DEFAULT_TIMEOUT_MS, sessionId)
    }
  }

  if (!response.data) {
    return buildJsonRpcError(id, `Upstream ${upstream.name || 'MCP'} returned no data`, -32000)
  }

  if (response.data.error) {
    return buildJsonRpcError(id, response.data.error.message || 'Upstream tool error', response.data.error.code || -32000)
  }

  let result = response.data.result
  if (result) {
    if (isApifyToolContext(toolName, integrationLabel)) {
      const payload = extractResultPayload(result)
      let datasetId = extractApifyDatasetId(payload.value)
      if (!datasetId) {
        datasetId = extractApifyDatasetId(result)
      }
      if (datasetId) {
        setCachedApifyDatasetId(apifyCacheKey, datasetId)
        console.log(`[MCP Orchestrator] Cached Apify dataset ID ${datasetId}`)
      }
      let runId = extractApifyRunId(payload.value)
      if (!runId) {
        runId = extractApifyRunId(result)
      }
      if (runId) {
        setCachedApifyRunId(apifyCacheKey, runId)
        console.log(`[MCP Orchestrator] Cached Apify run ID ${runId}`)
      }
    }
    result = await normalizeUpstreamResult(toolName, result)
  }

  return buildJsonRpcResult(id, result)
}

async function handleToolsList(req, body) {
  const id = body.id ?? null
  const params = body.params || {}
  const integrationName = extractIntegrationHeader(req, 'x-mcp-integration-name')
  const integrationId = extractIntegrationHeader(req, 'x-mcp-integration-id')
  const integrationLabel = integrationName || integrationId || 'unknown'
  console.log(`[MCP Orchestrator] tools/list integration=${integrationLabel}`)

  const upstream = await resolveUpstream(req, '')
  if (!upstream) {
    return buildJsonRpcError(id, `No upstream configured for integration ${integrationLabel}`, -32601)
  }

  let sessionId = await ensureUpstreamSession(upstream)
  const rpc = {
    jsonrpc: '2.0',
    id: Date.now(),
    method: 'tools/list',
    params,
  }

  let response = await sendRpc(upstream, rpc, upstream.timeoutMs || DEFAULT_TIMEOUT_MS, sessionId)

  if (response.data?.error) {
    const errMsg = (response.data.error.message || '').toLowerCase()
    const errCode = response.data.error.code
    const isSessionError = errMsg.includes('session') || errCode === -32603
    if (isSessionError) {
      const cacheKey = normalizeKey(upstream.cacheKey || upstream.name || upstream.url)
      sessionCache.delete(cacheKey)
      sessionId = await ensureUpstreamSession(upstream)
      rpc.id = Date.now()
      response = await sendRpc(upstream, rpc, upstream.timeoutMs || DEFAULT_TIMEOUT_MS, sessionId)
    }
  }

  if (!response.data) {
    return buildJsonRpcError(id, `Upstream ${upstream.name || 'MCP'} returned no data`, -32000)
  }

  if (response.data.error) {
    return buildJsonRpcError(
      id,
      response.data.error.message || 'Upstream tools/list error',
      response.data.error.code || -32000
    )
  }

  return buildJsonRpcResult(id, response.data.result || { tools: [] })
}

async function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = ''
    req.on('data', (chunk) => {
      data += chunk.toString('utf8')
      if (data.length > 2 * 1024 * 1024) {
        reject(new Error('Body too large'))
      }
    })
    req.on('end', () => resolve(data))
    req.on('error', reject)
  })
}

const server = http.createServer(async (req, res) => {
  if ((req.method === 'GET' || req.method === 'HEAD') && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ ok: true }))
    return
  }

  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: 'Method not allowed' }))
    return
  }

  try {
    const bodyText = await readBody(req)
    const payload = JSON.parse(bodyText)
    const preferStream = STREAM_RESPONSES && String(req.headers.accept || '').includes('text/event-stream')

    if (Array.isArray(payload)) {
      const results = await Promise.all(payload.map((item) => dispatchRequest(req, item)))
      respond(res, results, preferStream)
      return
    }

    const result = await dispatchRequest(req, payload)
    respond(res, result, preferStream)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error'
    res.writeHead(500, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: message }))
  }
})

async function dispatchRequest(req, body) {
  if (!body || typeof body !== 'object') {
    return buildJsonRpcError(null, 'Invalid request body', -32600)
  }

  const method = body.method
  if (method === 'initialize') {
    const result = {
      protocolVersion: '2024-11-05',
      capabilities: { tools: {} },
      serverInfo: { name: 'mcp-orchestrator', version: '0.1.0' },
    }
    return buildJsonRpcResult(body.id ?? null, result)
  }

  if (method === 'tools/call') {
    return handleToolCall(req, body)
  }

  if (method === 'tools/list') {
    return handleToolsList(req, body)
  }

  if (method === 'notifications/initialized') {
    return buildJsonRpcResult(body.id ?? null, { ok: true })
  }

  // Per MCP spec: ping must be supported
  if (method === 'ping') {
    return buildJsonRpcResult(body.id ?? null, {})
  }

  return buildJsonRpcError(body.id ?? null, `Unsupported method ${method}`, -32601)
}

server.listen(PORT, () => {
  console.log(`[MCP Orchestrator] Listening on port ${PORT}`)
  console.log(`[MCP Orchestrator] Upstreams: ${UPSTREAMS.length}`)
})
