# MCP Orchestrator

A smart MCP gateway that brokers multiple MCP servers, normalizes arguments/results, enforces payload limits, and caches oversized results in Supabase.

## Railway Deploy (recommended)

1) Create a new Railway project.
2) Deploy from GitHub and set the root to `mcp-orchestrator/`.
3) Set environment variables:
   - `PORT`: `3000`
- `MCP_UPSTREAMS`: JSON array of upstream MCP servers.
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

Update your MCP integrations to point to the orchestrator URL. The app sends
`x-mcp-integration-name` so the orchestrator can route to the correct upstream.

## Auto-discovery (Supabase)

If Supabase is configured, the orchestrator can auto-resolve MCP URLs from your
`agent_integrations` table. This lets you avoid manually maintaining
`MCP_UPSTREAMS`. The orchestrator uses `x-mcp-integration-id` or
`x-mcp-integration-name` headers to look up the integration, then reads the MCP
URL and auth settings from `config` (or `config.wrapper` when `wrapperEnabled` is true).
