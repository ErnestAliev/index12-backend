const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { supervise } = require('../codex/supervisor.cjs');
const { plist } = require('../codex/service.cjs');

function worker() {
  const child = new EventEmitter(); child.signals = [];
  child.kill = signal => { child.signals.push(signal); child.emit('exit', 0, signal); };
  return child;
}

test('supervisor recovers from unavailable daemon and worker crash without replaying jobs', async () => {
  const stop = new AbortController(), children = [], pauses = [];
  let attempts = 0;
  await supervise({ signal: stop.signal, ready: async () => false, log: () => {},
    ensureDaemon: () => { if (++attempts === 1) throw new Error('offline'); },
    spawnWorker: () => { const child = worker(); children.push(child); return child; },
    pause: async ms => {
      pauses.push(ms);
      if (children.length === 1 && ms === 5000) children[0].emit('exit', 1);
      if (children.length === 2) stop.abort();
    }
  });
  assert.equal(attempts, 3);
  assert.equal(children.length, 2);
  assert.deepEqual(pauses, [2000, 5000, 4000, 5000]);
  assert.deepEqual(children[1].signals, ['SIGTERM']);
});

test('a live existing bridge is adopted; an unresponsive replacement is terminated and retried', async () => {
  const stop = new AbortController(), children = [];
  let checks = 0, clock = 0;
  await supervise({ signal: stop.signal, ready: async () => ++checks <= 2, now: () => clock, unhealthyMs: 10000, log: () => {},
    ensureDaemon: () => {}, spawnWorker: () => { const child = worker(); children.push(child); return child; },
    pause: async ms => { clock += ms; if (children.length === 2) stop.abort(); }
  });
  assert.equal(children.length, 2);
  assert.deepEqual(children.map(child => child.signals), [['SIGTERM'], ['SIGTERM']]);
});

test('repeated failures back off to thirty seconds and shutdown stops reconnecting', async () => {
  const stop = new AbortController(), pauses = [];
  await supervise({ signal: stop.signal, ready: async () => false, log: () => {},
    ensureDaemon: () => { throw new Error('network unavailable'); },
    spawnWorker: () => { throw new Error('must not start before daemon is ready'); },
    pause: async ms => { pauses.push(ms); if (pauses.length === 7) stop.abort(); }
  });
  assert.deepEqual(pauses, [2000, 4000, 8000, 16000, 30000, 30000, 30000]);
});

test('launchd starts at login, restarts after a crash and stores no credentials', () => {
  const content = plist({ backendRoot: '/workspace/Index & Co', node: '/opt/homebrew/bin/node', environment: { CODEX_HOME: '/private/codex', INDEX12_MCP_DB_URL: 'database-secret', OPENAI_API_KEY: 'api-secret' } });
  assert.match(content, /<key>RunAtLoad<\/key><true\/>/);
  assert.match(content, /<key>KeepAlive<\/key><true\/>/);
  assert.match(content, /Index &amp; Co\/codex\/supervisor.cjs/);
  assert.match(content, /<key>CODEX_HOME<\/key><string>\/private\/codex<\/string>/);
  assert.doesNotMatch(content, /database-secret|api-secret|INDEX12_MCP_DB_URL|OPENAI_API_KEY/);
});
