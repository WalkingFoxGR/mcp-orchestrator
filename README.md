# MCP Orchestrator

A smart MCP gateway that brokers multiple MCP servers, normalizes arguments/results, enforces payload limits, and caches oversized results in Supabase.

## Security

The orchestrator adds each integration's stored credentials to the calls it forwards, so it must
never be reachable by anyone but Nexus.

- **Private network only.** Nexus calls it at `http://mcp-orchestrator.railway.internal:3000/`
  (`MCP_ORCHESTRATOR_URL` in Nexus). Keep the Railway public domain **off** for this service.
- **Inbound key.** Set `ORCH_INBOUND_KEY` to a long random value, and the same value as
  `MCP_ORCHESTRATOR_KEY` in Nexus. Every request except `GET`/`HEAD /health` must then carry it in
  the `x-orchestrator-key` header; anything else gets `401 {"error":"Unauthorized"}`. Refusals are
  logged at most once per remote address per minute, with a count of the ones left out; the key is
  never logged. The value is trimmed, and may be a comma-separated list (see Rotating the key).
  Each key must be at least 32 characters: a shorter entry (a typo such as `<current>,x`) is
  ignored, with a startup warning that never names it.
- **Fails closed.** With `ORCH_INBOUND_KEY` unset (or blank) and a Supabase service key configured,
  every request but `/health` gets 401. So does a key set with no entry of 32 characters, with or
  without a service key. `ORCH_ALLOW_UNAUTHENTICATED=true` (exactly `true`) accepts requests
  without a key anyway; see Rollback order before using it. The startup log says which mode the
  service is in.
- **Integrations by id only.** An upstream is resolved from `x-mcp-integration-id` alone: the
  `agent_integrations` row with that id, if it is an MCP row (or an API row with its wrapper enabled)
  and has a URL. `is_active` is not checked: it is the chat toggle, and Nexus decides which rows it
  calls. `x-mcp-integration-name` appears in log lines only. A row is cached by id for
  `SUPABASE_INTEGRATIONS_CACHE_TTL_MS`, so an edited row takes effect within it; if the database
  cannot be read when the entry expires, the cached row keeps being served (retried every 30 s) for
  at most an hour after its last good read, and an id with no row is remembered for 10 s.
  Concurrent requests for one id share one database read. Upstream sessions, client-credentials tokens and Apify
  run ids are kept per integration id.

- **Upstreams must be public.** Every upstream fetch (initialize, tools/list, tools/call, the
  client-credentials token fetch) refuses loopback, private, link-local, CGNAT (100.64/10),
  192.0.0.0/24, documentation and multicast addresses; unique-local, link-local, site-local, Teredo,
  local-use NAT64 and documentation IPv6, and IPv4-mapped, NAT64 or 6to4 IPv6 that embeds a refused
  IPv4 (any textual form); cloud metadata (169.254.169.254, fd00:ec2::254); and the hostnames
  `localhost`, `*.localhost`, `*.local`, `*.internal` (Railway's private network) and any
  single-label name. The orchestrator's own names (`RAILWAY_PUBLIC_DOMAIN`,
  `RAILWAY_PRIVATE_DOMAIN`, and the legacy `RAILWAY_STATIC_URL` when set) are refused too, whatever they resolve to, even when allow-listed: a row pointing
  back at the orchestrator would call it with that row's custom headers. A custom domain on this
  service is not covered; it should have none. A name is checked on the addresses the connection actually dials (a guarded
  lookup on the undici Agent), so DNS rebinding between a check and the connect does not get
  through. Only 301, 302, 303, 307 and 308 are followed, by hand, at most 5, each hop checked again;
  a hop to another origin keeps only `Accept` and `Content-Type`, and a request body never follows
  one (a same-host http to https upgrade on the default ports counts as the same origin). A refused
  upstream answers JSON-RPC `-32010 upstream_refused` (not -32001, the MCP SDK's timeout code) and
  logs one line with the host and the reason, never a credential.
- **`ORCH_ALLOW_PRIVATE_UPSTREAMS` is a hole by design.** A listed host skips the address check, for
  every tenant: any integration may then point at it. List only operator-owned Railway services
  (`name.railway.internal`), never a DNS name a tenant controls (they could repoint it at the
  metadata address), and scope each entry to its port (`name.railway.internal:8080`); an entry
  without a port opens every port of that host.

Deploy order when turning the key on: set `ORCH_INBOUND_KEY` here and `MCP_ORCHESTRATOR_KEY` in Nexus,
deploy Nexus (it then sends the key), then deploy this service.

The upstream guard (branch `fix/upstream-ssrf-guard`) is built on the inbound key change
(`fix/inbound-auth-id-only`) and merges after it. Before deploying it, check that no integration
and no `MCP_UPSTREAMS` entry points at a private or internal host; an integration that does stops
working (see the warning above before listing anything).

Rollback order. Prefer fixing forward: if calls are refused for a key mismatch, re-sync the key
(the same value in `ORCH_INBOUND_KEY` here and `MCP_ORCHESTRATOR_KEY` in Nexus) instead of rolling
back. Both rollbacks reopen a cross-tenant path that any signed-in Nexus user can drive: a tenant's
own MCP row whose url is this orchestrator, with custom headers naming another tenant's row, makes
the orchestrator call itself and answer with the other tenant's credentials. The key is what stops
it (the self-call never carries it).

- If a rollback is forced, prefer `ORCH_ALLOW_UNAUTHENTICATED=true` with `ORCH_INBOUND_KEY` unset
  over a revert: resolution by id only stays, so the self-loop needs the other row's full uuid,
  and the upstream guard keeps refusing the orchestrator's own names. A revert brings back
  resolution by name, where the other tenant's integration name alone is enough.
- Either way, do it **before** Nexus stops sending the key (a Nexus rollback or an unset
  `MCP_ORCHESTRATOR_KEY`). The other way round, every orchestrator call is refused.

### Rotating the key

1. Add the new key to the list here: `ORCH_INBOUND_KEY=<current>,<new>`. Both are accepted.
2. Switch `MCP_ORCHESTRATOR_KEY` in Nexus to the new key and deploy Nexus.
3. Remove the old key here: `ORCH_INBOUND_KEY=<new>`.

## Railway Deploy (recommended)

1) Create a new Railway project.
2) Deploy from GitHub and set the root to `mcp-orchestrator/`. Do not generate a public domain.
3) Set environment variables:
   - `PORT`: `3000`
- `ORCH_INBOUND_KEY`: the key every request but `/health` must carry in `x-orchestrator-key`, at least 32 characters; comma-separated while rotating (see Security).
- `ORCH_ALLOW_PRIVATE_UPSTREAMS`: comma-separated `host:port` (or `host`, any port; `[v6]:port` for IPv6) an upstream may use although private or internal, e.g. `my-mcp.railway.internal:8080`. Operator-owned Railway services only. Empty by default (see Security).
- `ORCH_ALLOW_UNAUTHENTICATED`: `true` to accept requests without a key when `ORCH_INBOUND_KEY` is unset. Off by default: with a service key configured (or a key set with no valid entry), every request but `/health` is refused (see Security and Rollback order).
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
