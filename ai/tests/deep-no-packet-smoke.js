/* eslint-disable no-console */
const assert = require('assert');
const path = require('path');
const http = require('http');
const express = require('express');

process.env.AI_ALLOW_ALL = 'true';
delete process.env.OPENAI_API_KEY;

const injectModule = (absPath, exportsValue) => {
  require.cache[absPath] = {
    id: absPath,
    filename: absPath,
    loaded: true,
    exports: exportsValue
  };
};

const rootAiDir = path.resolve(__dirname, '..');

injectModule(
  path.resolve(rootAiDir, 'dataProvider.js'),
  () => ({
    buildDataPacket: async () => ({
      meta: {
        today: '15.02.26',
        periodStart: '2026-02-01',
        periodEnd: '2026-02-28',
        source: 'mock'
      },
      accounts: [],
      operations: [],
      totals: {
        open: { current: 0, future: 0 },
        hidden: { current: 0, future: 0 },
        all: { current: 0, future: 0 }
      },
      accountsData: {
        accounts: [],
        openAccounts: [],
        hiddenAccounts: [],
        totals: {
          open: { current: 0, future: 0 },
          hidden: { current: 0, future: 0 },
          all: { current: 0, future: 0 }
        },
        meta: { today: '15.02.26', count: 0, openCount: 0, hiddenCount: 0 }
      },
      operationsSummary: {
        total: 0,
        income: { count: 0, total: 0, fact: { count: 0, total: 0 }, forecast: { count: 0, total: 0 } },
        expense: { count: 0, total: 0, fact: { count: 0, total: 0 }, forecast: { count: 0, total: 0 } },
        transfer: { count: 0, total: 0, fact: { count: 0, total: 0 }, forecast: { count: 0, total: 0 } }
      },
      categorySummary: [],
      tagSummary: [],
      catalogs: {
        categories: [],
        projects: [],
        companies: [],
        contractors: [],
        individuals: []
      },
      dataQualityReport: {
        status: 'ok',
        score: 100,
        counters: {},
        issues: []
      }
    })
  })
);

injectModule(
  path.resolve(rootAiDir, 'contextPacketService.js'),
  () => ({
    enabled: true,
    getMonthlyPacket: async () => null,
    listMonthlyPacketHeaders: async () => [],
    upsertMonthlyPacket: async () => null
  })
);

injectModule(
  path.resolve(rootAiDir, 'memory', 'glossaryService.js'),
  () => ({
    addTerm: async () => ({}),
    ensureSystemGlossary: async () => null,
    getGlossary: async () => [],
    findUnknownTerms: () => [],
    buildGlossaryContext: () => '',
    isWellKnownTerm: () => false
  })
);

injectModule(
  path.resolve(rootAiDir, 'memory', 'userProfileService.js'),
  () => ({
    getProfile: async () => ({ onboardingComplete: true }),
    updateProfile: async () => null,
    completeOnboarding: async () => null,
    buildProfileContext: () => '',
    recordInteraction: async () => null
  })
);

const createAiRouter = require('../aiRoutes');

const app = express();
app.use(express.json({ limit: '2mb' }));

const isAuthenticated = (req, res, next) => {
  req.user = {
    id: 'u_real',
    _id: 'u_real',
    email: 'dev@local.test',
    ownerId: null,
    currentWorkspaceId: 'ws_1'
  };
  next();
};

const deps = {
  mongoose: {
    Types: {
      ObjectId: {
        isValid: () => false
      }
    }
  },
  models: {
    AiContextPacket: {},
    AiGlossary: {},
    AiUserProfile: {}
  },
  isAuthenticated,
  getCompositeUserId: async () => 'u_real_ws_ws_1'
};

app.use('/api/ai', createAiRouter(deps));

function requestJson(port, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      path: '/api/ai/query',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += String(chunk); });
      res.on('end', () => {
        try {
          resolve({
            status: res.statusCode,
            json: JSON.parse(data || '{}')
          });
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  const server = app.listen(0);
  try {
    const port = server.address().port;
    const response = await requestJson(port, {
      mode: 'deep',
      source: 'chat',
      message: 'посчитай прибыль Проект',
      periodFilter: {
        mode: 'custom',
        customStart: '2026-02-01T00:00:00.000Z',
        customEnd: '2026-02-28T23:59:59.999Z'
      }
    });

    assert.strictEqual(response.status, 200, 'HTTP status must be 200');
    const text = String(response?.json?.text || '');
    assert.ok(text.length > 0, 'text response must not be empty');
    assert.ok(
      !text.includes('нет подготовленного пакета данных'),
      'deep response must not return no-packet hard stop'
    );

    console.log('OK deep-no-packet-smoke');
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

main().catch((err) => {
  console.error('FAIL deep-no-packet-smoke:', err?.message || err);
  process.exit(1);
});
