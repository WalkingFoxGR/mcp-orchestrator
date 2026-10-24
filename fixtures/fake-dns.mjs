// Preloaded into the server under test by test/server-ssrf.test.js (--import; outside test/, which
// node --test would run as a test file): a few names that resolve to 127.0.0.1, so a hostname the
// static checks pass is refused at connect time by the guarded lookup.
import dns from 'node:dns'

const real = dns.lookup
// orch-self.*: the orchestrator's own names in the self-loop test, so a guard that let them
// through would dial 127.0.0.1 here, never a real address.
const LOCAL = new Set(['rebind.example.invalid', 'token.example.invalid', 'mcp-wrapper.example.invalid', 'orch-self.example.com', 'orch-self.railway.internal'])
dns.lookup = (host, options, callback) => {
  if (typeof options === 'function') {
    callback = options
    options = {}
  }
  if (LOCAL.has(host)) {
    return process.nextTick(callback, null, options?.all ? [{ address: '127.0.0.1', family: 4 }] : '127.0.0.1', 4)
  }
  return real(host, options, callback)
}
