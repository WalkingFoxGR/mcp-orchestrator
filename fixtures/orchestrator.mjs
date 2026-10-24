// Starts server.js as a child process for the server tests (outside test/, which node --test would
// run as a test file). PORT=0: the server binds a port of its own and its "Listening on port" line
// says which, so no port is probed first and then lost to another process.
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const SERVER = fileURLToPath(new URL('../server.js', import.meta.url))

export async function startOrchestrator(env, { nodeArgs = [] } = {}) {
  const child = spawn(process.execPath, [...nodeArgs, SERVER], {
    env: { PATH: process.env.PATH, ...env, PORT: '0' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let output = ''
  child.stdout.on('data', (d) => { output += d })
  child.stderr.on('data', (d) => { output += d })
  const port = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`server did not start: ${output}`)), 5000)
    child.on('exit', (code) => { clearTimeout(timer); reject(new Error(`server exited (${code}): ${output}`)) })
    child.stdout.on('data', () => {
      const m = /Listening on port (\d+)/.exec(output)
      // The startup lines after it are written in the same tick; give them a moment to arrive.
      if (m) { clearTimeout(timer); setTimeout(() => resolve(Number(m[1])), 20) }
    })
  })
  return { url: `http://127.0.0.1:${port}`, child, output: () => output }
}
