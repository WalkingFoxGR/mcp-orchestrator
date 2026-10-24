// Preloaded into the server under test by test/server-ssrf.test.js (--import; outside test/, which
// node --test would run as a test file): one name that resolves to 127.0.0.1, so a
// hostname the static checks pass is refused at connect time by the guarded lookup.
import dns from 'node:dns'

const real = dns.lookup
dns.lookup = (host, options, callback) => {
  if (typeof options === 'function') {
    callback = options
    options = {}
  }
  if (host === 'rebind.example.invalid') {
    return process.nextTick(callback, null, options?.all ? [{ address: '127.0.0.1', family: 4 }] : '127.0.0.1', 4)
  }
  return real(host, options, callback)
}
