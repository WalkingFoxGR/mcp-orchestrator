# MCP Orchestrator

A smart MCP gateway that brokers multiple MCP servers, normalizes arguments/results, enforces payload limits, and caches oversized results in Supabase.

## Security

The orchestrator adds each integration's stored credentials to the calls it forwards, so it must
never be reachable by anyone but Nexus.

- **Private network only.** Nexus calls it at `http://mcp-orchestrator.railway.internal:3000/`
  (`MCP_ORCHESTRATOR_URL` in Nexus). Keep the Railway public domain **off** for this service.
- **Inbound key.** Set `ORCH_INBOUND_KEY` to a long random value, and the same value as
  `MCP_ORCHESTRATOR_KEY` in Nexus. Every request except `GET`/`HEAD /health` must then carry it in
  the `x-orchestrator-key` header; anything else gets `401 {"error":"Unauthorized"}` and one log line
  (the key is never logged). With `ORCH_INBOUND_KEY` unset the orchestrator accepts every request and
  logs a warning at startup.
- **Integrations by id only.** An upstream is resolved from `x-mcp-integration-id` alone: the
  `agent_integrations` row with that id, if it is an MCP row (or an API row with its wrapper enabled),
  has a URL and is not inactive. `x-mcp-integration-name` appears in log lines only. A row is cached
  by id for `SUPABASE_INTEGRATIONS_CACHE_TTL_MS`, so an edited row takes effect within it, and upstream
  sessions are kept per integration id.

Deploy order when turning the key on: set `ORCH_INBOUND_KEY` here and `MCP_ORCHESTRATOR_KEY` in Nexus,
deploy Nexus (it then sends the key), then deploy this service.

## Railway Deploy (recommended)

1) Create a new Railway project.
2) Deploy from GitHub and set the root to `mcp-orchestrator/`. Do not generate a public domain.
3) Set environment variables:
   - `PORT`: `3000`
- `ORCH_INBOUND_KEY`: the key every request but `/health` must carry in `x-orchestrator-key` (see Security).
- `MCP_UPSTREAMS`: JSON array of upstream MCP servers. Used only for a request with no `x-mcp-integration-id`.
- `ORCH_AUTO_DISCOVER`: `true` by default. If true and Supabase is configured, the orchestrator will look up MCP URLs from your Supabase integrations table.
- `ORCH_TIMEOUT_MS`: e.g. `50000`
- `ORCH_MAX_ARG_BYTES`: e.g. `65536`
- `ORCH_MAX_RESULT_BYTES`: e.g. `200000`
- `ORCH_STREAM`: `true` to respond with SSE when clients request it.
- `SUPABASE_URL`: your Supabase project URL.
- `SUPABASE_SERVICE_ROLE_KEY`: service role key for cache writes.
- `SUPABASE_TABLE`: optional (default `mcp_tool_cache`).
- `SUPABASE_INTEGRATIONS_TABLE`: optional (default `agent_integrations`).
- `SUPABASE_INTEGRATIONS_CACHE_TTL_MS`: optional (default `300000`).

Example `MCP_UPSTREAMS` (optional when auto-discovery is enabled):

```json
[
  {
    "name": "n8n",
    "url": "https://your-mcp-wrapper-n8n.up.railway.app",
    "headers": { "Authorization": "Bearer YOUR_TOKEN" },
    "timeoutMs": 50000,
    "toolNameIncludes": ["workflow", "n8n"]
  },
  {
    "name": "apify",
    "url": "https://your-mcp-apify.up.railway.app",
    "headers": {},
    "timeoutMs": 50000,
    "toolNameIncludes": ["apify", "scraper"]
  }
]
```

## Supabase table (SQL)

```sql
create table if not exists public.mcp_tool_cache (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  payload jsonb not null,
  meta jsonb,
  byte_size integer not null default 0
);
```

## Integration note

Nexus sends `x-mcp-integration-id` (and `x-orchestrator-key`) on every call so the orchestrator can
route to the correct upstream. `x-mcp-integration-name` is for log lines only.

## Auto-discovery (Supabase)

If Supabase is configured, the orchestrator can auto-resolve MCP URLs from your
`agent_integrations` table. This lets you avoid manually maintaining
`MCP_UPSTREAMS`. The orchestrator uses the `x-mcp-integration-id` header to look
up the integration, then reads the MCP URL and auth settings from `config` (or
`config.wrapper` when `wrapperEnabled` is true). An id that does not resolve is a
JSON-RPC error; it never falls back to `MCP_UPSTREAMS`.
