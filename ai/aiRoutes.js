// backend/ai/aiRoutes.js
// AI assistant routes - QUICK + DEEP
//
// ✅ Features:
// - QUICK mode: deterministic fast replies
// - DEEP mode: unified prompt + context packet JSON
// - Hybrid data: snapshot (accounts/companies) + MongoDB (operations)

const express = require('express');
const deepPrompt = require('./prompts/deepPrompt');
const { buildContextPacketPayload, derivePeriodKey } = require('./contextPacketBuilder');
const fs = require('fs');
const path = require('path');

const AIROUTES_VERSION = 'quick-deep-v9.3';
const https = require('https');

// =========================
// Chat session state (in-memory, TTL)
// =========================
// 24h rolling TTL: хранит дневную переписку для «сквозного» дня
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const _chatSessions = new Map();

const _getChatSession = (userId) => {
  const key = String(userId || '');
  if (!key) return null;

  const now = Date.now();
  const cur = _chatSessions.get(key);
  if (cur && cur.expiresAt && cur.expiresAt > now) {
    cur.expiresAt = now + SESSION_TTL_MS;
    return cur;
  }

  const fresh = {
    expiresAt: now + SESSION_TTL_MS,
    prefs: { format: 'short', limit: 50, livingMonthly: null },
    pending: null,
    history: [],
  };
  _chatSessions.set(key, fresh);
  return fresh;
};

// =========================
// CHAT HISTORY HELPERS
// =========================
const HISTORY_MAX_MESSAGES = 200;

const _pushHistory = (userId, role, content) => {
  const s = _getChatSession(userId);
  if (!s) return;
  if (!Array.isArray(s.history)) s.history = [];

  const msg = {
    role: (role === 'assistant') ? 'assistant' : 'user',
    content: String(content || '').trim(),
  };

  if (!msg.content) return;

  s.history.push(msg);
  if (s.history.length > HISTORY_MAX_MESSAGES) {
    s.history = s.history.slice(-HISTORY_MAX_MESSAGES);
  }
  s.expiresAt = Date.now() + SESSION_TTL_MS;
};

const _getHistoryMessages = (userId) => {
  const s = _getChatSession(userId);
  if (!s || !Array.isArray(s.history) || !s.history.length) return [];
  return s.history.slice(-HISTORY_MAX_MESSAGES);
};

const _clearHistory = (userId) => {
  const key = String(userId || '');
  if (!key) return;
  _chatSessions.delete(key);
};

module.exports = function createAiRouter(deps) {
  const {
    mongoose,
    models,
    isAuthenticated,
    getCompositeUserId,
  } = deps;

  const { AiContextPacket } = models;

  // Create data provider for direct database access
  const createDataProvider = require('./dataProvider');
  const dataProvider = createDataProvider({ ...models, mongoose });
  const createContextPacketService = require('./contextPacketService');
  const contextPacketService = createContextPacketService({ AiContextPacket });
  const contextPacketsEnabled = !!contextPacketService?.enabled;
  const quickMode = require('./modes/quickMode');

  const router = express.Router();

  // Метка версии для быстрой проверки деплоя
  const CHAT_VERSION_TAG = 'aiRoutes-quick-deep-v9.3';

  // =========================
  // KZ time helpers (UTC+05:00)
  // =========================
  const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

  const _kzEndOfDay = (d) => {
    const t = new Date(d);
    const shifted = new Date(t.getTime() + KZ_OFFSET_MS);
    shifted.setUTCHours(0, 0, 0, 0);
    const start = new Date(shifted.getTime() - KZ_OFFSET_MS);
    return new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
  };

  const _endOfToday = () => _kzEndOfDay(new Date());

  const _fmtDateKZ = (d) => {
    try {
      const x = new Date(new Date(d).getTime() + KZ_OFFSET_MS);
      const dd = String(x.getUTCDate()).padStart(2, '0');
      const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
      const yy = String(x.getUTCFullYear() % 100).padStart(2, '0');
      return `${dd}.${mm}.${yy}`;
    } catch (_) {
      return String(d);
    }
  };

  const _formatTenge = (n) => {
    const num = Number(n || 0);
    const sign = num < 0 ? '- ' : '';
    try {
      return sign + new Intl.NumberFormat('ru-RU').format(Math.abs(Math.round(num))).split('\u00A0').join(' ') + ' ₸';
    } catch (_) {
      return sign + String(Math.round(Math.abs(num))) + ' ₸';
    }
  };

  const _safeDate = (v) => {
    if (!v) return null;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const _startsWithAny = (text, prefixes = []) => {
    const src = String(text || '');
    for (const prefix of prefixes) {
      if (!prefix) continue;
      if (src.startsWith(prefix)) return true;
    }
    return false;
  };

  const _isValidPeriodKey = (value) => {
    const key = String(value || '');
    if (key.length !== 7) return false;
    if (key[4] !== '-') return false;
    const chars = [key[0], key[1], key[2], key[3], key[5], key[6]];
    for (const ch of chars) {
      if (ch < '0' || ch > '9') return false;
    }
    return true;
  };

  const _extractOpenAiText = (parsed) => {
    const choice = parsed?.choices?.[0] || {};
    const msg = choice?.message || {};

    if (typeof msg.content === 'string' && msg.content.trim()) {
      return msg.content.trim();
    }

    if (Array.isArray(msg.content)) {
      const chunks = msg.content
        .map((part) => {
          if (typeof part === 'string') return part;
          if (!part || typeof part !== 'object') return '';
          if (typeof part.text === 'string') return part.text;
          if (typeof part.content === 'string') return part.content;
          if (typeof part.value === 'string') return part.value;
          return '';
        })
        .filter(Boolean)
        .join('\n')
        .trim();
      if (chunks) return chunks;
    }

    if (typeof msg.refusal === 'string' && msg.refusal.trim()) {
      return `Не могу ответить: ${msg.refusal.trim()}`;
    }

    return '';
  };

  const _isNoAiAnswerText = (text) => {
    const t = String(text || '').trim().toLowerCase();
    return (
      !t
      || t === 'нет ответа от ai.'
      || t === 'нет ответа от ai'
      || t.startsWith('ошибка openai')
      || t.startsWith('ошибка обработки ответа ai')
      || t.startsWith('ошибка связи с ai')
      || t.startsWith('ошибка: timeout')
    );
  };

  const _extractFirstJsonObject = (text) => {
    const src = String(text || '').trim();
    if (!src) return null;

    const direct = (() => {
      try {
        return JSON.parse(src);
      } catch (_) {
        return null;
      }
    })();
    if (direct && typeof direct === 'object') return direct;

    const fenced = src
      .replace(/^```json\s*/i, '')
      .replace(/^```\s*/i, '')
      .replace(/\s*```$/i, '')
      .trim();
    const fencedParsed = (() => {
      try {
        return JSON.parse(fenced);
      } catch (_) {
        return null;
      }
    })();
    if (fencedParsed && typeof fencedParsed === 'object') return fencedParsed;

    const start = src.indexOf('{');
    if (start < 0) return null;

    let depth = 0;
    let inString = false;
    let escaped = false;
    for (let i = start; i < src.length; i += 1) {
      const ch = src[i];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (ch === '\\') {
          escaped = true;
        } else if (ch === '"') {
          inString = false;
        }
        continue;
      }
      if (ch === '"') {
        inString = true;
        continue;
      }
      if (ch === '{') depth += 1;
      if (ch === '}') {
        depth -= 1;
        if (depth === 0) {
          const candidate = src.slice(start, i + 1);
          try {
            return JSON.parse(candidate);
          } catch (_) {
            return null;
          }
        }
      }
    }
    return null;
  };

  const _pathTokens = (pathExpr) => {
    const src = String(pathExpr || '').trim();
    if (!src) return [];
    const tokens = [];
    const rx = /([^[.\]]+)|\[(\d+)\]/g;
    let m;
    while ((m = rx.exec(src)) !== null) {
      if (m[1]) tokens.push(m[1]);
      else if (m[2]) tokens.push(Number(m[2]));
    }
    return tokens;
  };

  const _resolvePath = (root, pathExpr) => {
    const tokens = _pathTokens(pathExpr);
    if (!tokens.length) return { ok: false, value: undefined };
    let cur = root;
    for (const token of tokens) {
      if (cur === null || cur === undefined) return { ok: false, value: undefined };
      if (typeof token === 'number') {
        if (!Array.isArray(cur) || token < 0 || token >= cur.length) return { ok: false, value: undefined };
        cur = cur[token];
      } else {
        if (!Object.prototype.hasOwnProperty.call(cur, token)) return { ok: false, value: undefined };
        cur = cur[token];
      }
    }
    return { ok: true, value: cur };
  };

  const _normalizeComparable = (v) => {
    if (v === null || v === undefined) return null;
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    if (typeof v === 'string') {
      const s = v.trim();
      if (!s) return '';
      const numeric = Number(s.replace(/\s+/g, '').replace(',', '.'));
      if (Number.isFinite(numeric)) return numeric;
      return s;
    }
    if (typeof v === 'boolean') return v;
    return null;
  };

  const _isExpectedMatch = (actual, expected) => {
    const a = _normalizeComparable(actual);
    const b = _normalizeComparable(expected);
    if (a === null || b === null) return false;
    if (typeof a === 'number' && typeof b === 'number') {
      return Math.abs(a - b) < 0.5;
    }
    return String(a) === String(b);
  };

  const _validateGroundedPayload = ({ packet, payload }) => {
    const answer = String(payload?.answer || '').trim();
    const facts = Array.isArray(payload?.facts_used) ? payload.facts_used : [];
    if (!answer) return { ok: false, reason: 'empty_answer' };
    if (!facts.length) return { ok: false, reason: 'no_facts_used' };

    const validatedFacts = [];
    for (const item of facts.slice(0, 30)) {
      const pathExpr = String(item?.path || '').trim();
      if (!pathExpr) continue;
      const resolved = _resolvePath(packet, pathExpr);
      if (!resolved.ok) continue;

      const hasExpected = Object.prototype.hasOwnProperty.call(item || {}, 'value');
      if (hasExpected && !_isExpectedMatch(resolved.value, item.value)) {
        continue;
      }

      validatedFacts.push({
        path: pathExpr,
        value: resolved.value
      });
    }

    if (!validatedFacts.length) return { ok: false, reason: 'facts_not_verified' };
    return {
      ok: true,
      answer,
      validatedFacts
    };
  };

  const _buildDeterministicDeepFallback = ({ packet }) => {
    const n = (v) => {
      const x = Number(v);
      return Number.isFinite(x) ? x : 0;
    };

    const d = packet?.derived || {};
    const totals = d?.totals || {};
    const ops = d?.operationsSummary || {};
    const liq = d?.liquiditySignals || {};

    const current = n(totals?.open?.current ?? totals?.all?.current);
    const forecast = n(totals?.open?.future ?? totals?.all?.future);
    const incForecast = n(ops?.income?.forecast?.total);
    const expForecast = Math.abs(n(ops?.expense?.forecast?.total));
    const hasLiq = !!liq?.available;
    const minDate = String(liq?.minClosingBalance?.date || '');
    const minAmount = n(liq?.minClosingBalance?.amount);
    const minLabel = hasLiq
      ? `${_formatTenge(minAmount)} на ${minDate || 'дате периода'}`
      : 'н/д (нет timeline в контексте)';
    return `Технический fallback по данным пакета: текущий остаток ${_formatTenge(current)}, минимум в периоде ${minLabel}, конец периода ${_formatTenge(forecast)}, плановые поступления ${_formatTenge(incForecast)}, плановые расходы ${_formatTenge(expForecast)}.`;
  };

  const _safeWriteAnalysisJson = ({ userId, payload }) => {
    try {
      const stamp = new Date().toISOString().split(':').join('-').split('.').join('-');
      const dir = path.resolve(__dirname, 'debug-logs');
      fs.mkdirSync(dir, { recursive: true });
      const filename = `analysis-${String(userId || 'unknown')}-${stamp}.json`;
      const filePath = path.join(dir, filename);
      fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');
      return filePath;
    } catch (_) {
      return null;
    }
  };

  const _monthStartUtc = (d) => new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1, 0, 0, 0, 0));
  const _monthEndUtc = (d) => new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0, 23, 59, 59, 999));

  // =========================
  // OpenAI caller (supports model override)
  // =========================
  const _openAiChat = async (messages, { temperature = 0, maxTokens = 2000, modelOverride = null, timeout = 60000 } = {}) => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.warn('OPENAI_API_KEY is missing');
      return 'Ошибка: OPENAI_API_KEY не задан.';
    }

    const defaultModel = process.env.OPENAI_MODEL || 'gpt-4o';
    const model = modelOverride || defaultModel;

    // Reasoning models (o1/o3, gpt-5*) ignore temperature in many cases
    const modelLower = String(model || '').toLowerCase();
    const isReasoningModel = _startsWithAny(modelLower, ['o1', 'o3', 'gpt-5']);

    const payloadObj = {
      model,
      messages,
      max_completion_tokens: maxTokens,
    };
    if (!isReasoningModel) payloadObj.temperature = temperature;

    const payload = JSON.stringify(payloadObj);

    return new Promise((resolve) => {
      const timeoutHandle = setTimeout(() => {
        console.error(`[OpenAI] Request timeout (${timeout}ms)`);
        gptReq.destroy();
        resolve('Ошибка: timeout при запросе к AI.');
      }, timeout);

      const gptReq = https.request(
        {
          hostname: 'api.openai.com',
          path: '/v1/chat/completions',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(payload),
            'Authorization': `Bearer ${apiKey}`
          }
        },
        (resp) => {
          let data = '';
          resp.on('data', (chunk) => { data += chunk; });
          resp.on('end', () => {
            clearTimeout(timeoutHandle);
            try {
              if (resp.statusCode < 200 || resp.statusCode >= 300) {
                console.error(`OpenAI Error ${resp.statusCode}:`, data);
                resolve(`Ошибка OpenAI (${resp.statusCode}).`);
                return;
              }
              const parsed = JSON.parse(data);
              const text = _extractOpenAiText(parsed);
              if (text) {
                resolve(text);
                return;
              }
              const finishReason = parsed?.choices?.[0]?.finish_reason || 'unknown';
              console.warn(`[OpenAI] Empty message content (finish_reason=${finishReason}, model=${model}, maxTokens=${maxTokens})`);
              resolve('Нет ответа от AI.');
            } catch (e) {
              console.error('Parse Error:', e);
              resolve('Ошибка обработки ответа AI.');
            }
          });
        }
      );
      gptReq.on('error', (e) => {
        clearTimeout(timeoutHandle);
        console.error('Request Error:', e);
        resolve('Ошибка связи с AI.');
      });
      gptReq.write(payload);
      gptReq.end();
    });
  };

  // =========================
  // Access control
  // =========================
  const _isAiAllowed = (req) => {
    const AI_ALLOW_ALL = process.env.AI_ALLOW_ALL === 'true';
    if (AI_ALLOW_ALL) return true;

    const allowedEmails = (process.env.AI_ALLOW_EMAILS || '').split(',').map(e => e.trim()).filter(Boolean);
    const userEmail = req.user?.email || '';
    return allowedEmails.includes(userEmail);
  };

  // =========================
  // MAIN AI QUERY ENDPOINT
  // =========================
  router.post('/query', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');

      if (!userIdStr) {
        return res.status(401).json({ error: 'Пользователь не найден' });
      }

      // Check access
      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const q = String(req.body?.message || '').trim();
      if (!q) {
        return res.status(400).json({ error: 'Пустой запрос' });
      }

      const qLower = q.toLowerCase();
      const isDeep = req.body?.mode === 'deep';
      const source = req.body?.source || 'ui';
      const timeline = Array.isArray(req.body?.timeline) ? req.body.timeline : null;
      const requestDebug = req.body?.debugAi === true || String(req.body?.debugAi || '').toLowerCase() === 'true';

      const AI_DEBUG = process.env.AI_DEBUG === 'true';
      const shouldDebugLog = AI_DEBUG || requestDebug;

      if (shouldDebugLog) {
        console.log('[AI_QUERY_IN]', JSON.stringify({
          mode: isDeep ? 'deep' : 'quick',
          source,
          asOf: req?.body?.asOf || null,
          periodFilter: req?.body?.periodFilter || null,
          hasTimeline: !!timeline,
          question: q
        }));
      }

      // =========================
      // Get composite user ID for shared workspaces
      // =========================
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try {
          effectiveUserId = await getCompositeUserId(req);
        } catch (e) {
          console.error('❌ Failed to get composite userId:', e);
        }
      }

      const userIdsList = Array.from(
        new Set([effectiveUserId, req.user?.id || req.user?._id].filter(Boolean).map(String))
      );

      // =========================
      // Build data packet (hybrid mode: snapshot + MongoDB)
      // =========================
      // AI always sees ALL accounts (including hidden) for proper analysis
      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: true, // 🔥 AI always needs full context
        visibleAccountIds: null, // No filtering for AI
        dateRange: req?.body?.periodFilter || null,
        workspaceId: req.user?.currentWorkspaceId || null,
        now: req?.body?.asOf || null,
        snapshot: req?.body?.snapshot || null, // 🔥 HYBRID: accounts/companies from snapshot, operations from MongoDB
      });

      if (timeline) {
        dbData.meta = dbData.meta || {};
        dbData.meta.timeline = timeline;
      }

      // =========================
      // CONTEXT PACKET UPSERT (monthly, for DEEP mode)
      // =========================
      if (isDeep && contextPacketsEnabled) {
        try {
          const nowRef = _safeDate(req?.body?.asOf) || new Date();
          const periodFilter = req?.body?.periodFilter || {};
          const periodStart = _safeDate(periodFilter?.customStart) || _monthStartUtc(nowRef);
          const periodEnd = _safeDate(periodFilter?.customEnd) || _monthEndUtc(nowRef);
          const workspaceId = req.user?.currentWorkspaceId || null;
          const periodKey = derivePeriodKey(periodStart, 'Asia/Almaty');
          const packetUserId = String(effectiveUserId || userIdStr);
          const packetPayload = buildContextPacketPayload({
            dbData,
            promptText: deepPrompt,
            templateVersion: 'deep-v1',
            dictionaryVersion: 'dict-v1'
          });

          let shouldUpsertPacket = true;
          if (periodKey) {
            const existingPacket = await contextPacketService.getMonthlyPacket({
              workspaceId,
              userId: packetUserId,
              periodKey
            });
            const existingHash = String(existingPacket?.stats?.sourceHash || '');
            const nextHash = String(packetPayload?.stats?.sourceHash || '');
            if (existingHash && nextHash && existingHash === nextHash) {
              shouldUpsertPacket = false;
            }
          }

          if (shouldUpsertPacket) {
            await contextPacketService.upsertMonthlyPacket({
              workspaceId,
              userId: packetUserId,
              periodKey,
              periodStart,
              periodEnd,
              timezone: 'Asia/Almaty',
              ...packetPayload
            });
          }
        } catch (packetErr) {
          console.error('[AI][context-packet] upsert failed:', packetErr?.message || packetErr);
        }
      }

      // =========================
      // TRY QUICK MODE (deterministic, fast)
      // Skip if user explicitly chose Deep Mode (preserves conversation context)
      // =========================
      if (!isDeep) {
        const quickResponse = quickMode.handleQuickQuery({
          query: qLower,
          dbData,
          snapshot: req?.body?.snapshot || null,
          formatTenge: _formatTenge
        });

        if (shouldDebugLog) {
          console.log('[AI_QUICK_ANALYSIS]', JSON.stringify({
            operationsCount: Array.isArray(dbData.operations) ? dbData.operations.length : 0,
            transfersCount: Array.isArray(dbData.operations)
              ? dbData.operations.filter(op => op.kind === 'transfer').length
              : 0,
            matched: !!quickResponse
          }));
        }

        if (quickResponse) {
          _pushHistory(userIdStr, 'user', q);
          _pushHistory(userIdStr, 'assistant', quickResponse);
          return res.json({ text: quickResponse });
        }
      }

      // =========================
      // DEEP MODE (CFO-level analysis)
      // =========================
      const deepHistory = [];
      const modelDeep = process.env.OPENAI_MODEL_DEEP || 'gpt-4o';

      const nowRef = _safeDate(req?.body?.asOf) || new Date();
      const periodFilter = req?.body?.periodFilter || {};
      const periodStart = _safeDate(periodFilter?.customStart) || _monthStartUtc(nowRef);
      const periodEnd = _safeDate(periodFilter?.customEnd) || _monthEndUtc(nowRef);
      const workspaceId = req.user?.currentWorkspaceId || null;
      const periodKey = derivePeriodKey(periodStart, 'Asia/Almaty');
      const packetUserId = String(effectiveUserId || userIdStr);

      let packet = null;
      if (contextPacketsEnabled && periodKey) {
        packet = await contextPacketService.getMonthlyPacket({
          workspaceId,
          userId: packetUserId,
          periodKey
        });
      }

      if (!packet) {
        packet = {
          periodKey: periodKey || null,
          periodStart,
          periodEnd,
          timezone: 'Asia/Almaty',
          ...buildContextPacketPayload({
            dbData,
            promptText: deepPrompt,
            templateVersion: 'deep-v1',
            dictionaryVersion: 'dict-v1'
          })
        };
      }

      if (shouldDebugLog) {
        const analysisEnvelope = {
          mode: 'deep',
          model: modelDeep,
          question: q,
          period: {
            key: periodKey || null,
            start: periodStart,
            end: periodEnd,
            timezone: 'Asia/Almaty'
          },
          packetSummary: {
            sourceHash: packet?.stats?.sourceHash || null,
            operationsCount: packet?.stats?.operationsCount || 0,
            accountsCount: packet?.stats?.accountsCount || 0,
            qualityStatus: packet?.dataQuality?.status || null,
            qualityScore: packet?.dataQuality?.score || null
          }
        };

        const analysisFile = _safeWriteAnalysisJson({
          userId: userIdStr,
          payload: {
            request: {
              question: q,
              mode: 'deep',
              asOf: req?.body?.asOf || null,
              periodFilter: req?.body?.periodFilter || null
            },
            analyzed: packet
          }
        });

        console.log('[AI_DEEP_ANALYSIS]', JSON.stringify({
          ...analysisEnvelope,
          analysisFile
        }));
      }

      const groundedMessages = [
        { role: 'system', content: deepPrompt },
        {
          role: 'system',
          content: [
            'Отвечай строго по context_packet_json.',
            'Верни ТОЛЬКО JSON-объект без markdown и без пояснений.',
            'Схема JSON:',
            '{',
            '  "answer": "живой короткий ответ пользователю на русском, без фантазий",',
            '  "facts_used": [',
            '    { "path": "path.to.field", "value": <ожидаемое значение из context_packet_json> }',
            '  ]',
            '}',
            'Требования:',
            '- Используй минимум 2 факта в facts_used.',
            '- path должен указывать на реальные поля context_packet_json.',
            '- value должен совпадать с данными по path.',
            '- В answer используй только подтверждаемые факты из facts_used.',
            '- Все суммы в формате "3 272 059 ₸" (пробелы между тысячами, без запятых).'
          ].join('\n')
        },
        { role: 'system', content: `context_packet_json:\n${JSON.stringify(packet)}` },
        ...deepHistory,
        { role: 'user', content: q }
      ];

      let rawAnswer = await _openAiChat(groundedMessages, {
        modelOverride: modelDeep,
        maxTokens: 1600,
        timeout: 120000
      });
      let groundedValidation = null;

      if (!_isNoAiAnswerText(rawAnswer)) {
        const groundedPayload = _extractFirstJsonObject(rawAnswer);
        if (groundedPayload && typeof groundedPayload === 'object') {
          groundedValidation = _validateGroundedPayload({ packet, payload: groundedPayload });
          if (groundedValidation?.ok) {
            rawAnswer = groundedValidation.answer;
          }
        }
      }

      if (shouldDebugLog) {
        console.log('[AI_DEEP_GROUNDED]', JSON.stringify({
          ok: !!groundedValidation?.ok,
          reason: groundedValidation?.ok ? null : (groundedValidation?.reason || 'parse_or_schema_failed'),
          factsVerified: groundedValidation?.ok ? groundedValidation.validatedFacts.length : 0
        }));
      }

      if (!groundedValidation?.ok && !_isNoAiAnswerText(rawAnswer)) {
        const fallbackModel = process.env.OPENAI_MODEL || 'gpt-4o';
        rawAnswer = await _openAiChat(groundedMessages, {
          modelOverride: fallbackModel,
          maxTokens: 1600,
          timeout: 120000
        });
        const groundedPayloadRetry = _extractFirstJsonObject(rawAnswer);
        if (groundedPayloadRetry && typeof groundedPayloadRetry === 'object') {
          const groundedValidationRetry = _validateGroundedPayload({ packet, payload: groundedPayloadRetry });
          if (groundedValidationRetry?.ok) {
            groundedValidation = groundedValidationRetry;
            rawAnswer = groundedValidationRetry.answer;
          }
        }
      }

      if (_isNoAiAnswerText(rawAnswer) || !groundedValidation?.ok) {
        rawAnswer = _buildDeterministicDeepFallback({ packet });
      }
      const answer = String(rawAnswer || '').trim() || 'Нет ответа от AI.';

      _pushHistory(userIdStr, 'user', q);
      _pushHistory(userIdStr, 'assistant', answer);
      return res.json({ text: answer });

    } catch (error) {
      console.error('AI Query Error:', error);
      return res.status(500).json({ error: 'Ошибка обработки запроса' });
    }
  });

  // =========================
  // HISTORY ROUTES (24h TTL)
  // =========================
  router.get('/history', isAuthenticated, (req, res) => {
    const userId = req.user?._id || req.user?.id;
    if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
    const limit = Math.max(1, Math.min(Number(req.query?.limit) || HISTORY_MAX_MESSAGES, HISTORY_MAX_MESSAGES));
    const hist = _getHistoryMessages(userId).slice(-limit);
    return res.json({ history: hist });
  });

  router.delete('/history', isAuthenticated, (req, res) => {
    const userId = req.user?._id || req.user?.id;
    if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
    _clearHistory(userId);
    return res.json({ ok: true });
  });

  // =========================
  // CONTEXT PACKETS ROUTES
  // =========================
  router.get('/context-packets', isAuthenticated, async (req, res) => {
    try {
      if (!contextPacketsEnabled) {
        return res.status(501).json({ error: 'Context packets disabled' });
      }
      const userId = req.user?._id || req.user?.id;
      if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try { effectiveUserId = await getCompositeUserId(req); } catch (_) { }
      }
      const workspaceId = req.user?.currentWorkspaceId || null;
      const limit = Math.max(1, Math.min(Number(req.query?.limit) || 24, 120));
      const items = await contextPacketService.listMonthlyPacketHeaders({
        workspaceId,
        userId: String(effectiveUserId || userId),
        limit
      });
      return res.json({ items });
    } catch (err) {
      return res.status(500).json({ error: 'Ошибка чтения context packets' });
    }
  });

  router.get('/context-packets/:periodKey', isAuthenticated, async (req, res) => {
    try {
      if (!contextPacketsEnabled) {
        return res.status(501).json({ error: 'Context packets disabled' });
      }
      const periodKey = String(req.params?.periodKey || '').trim();
      if (!_isValidPeriodKey(periodKey)) {
        return res.status(400).json({ error: 'Неверный periodKey, ожидается YYYY-MM' });
      }
      const userId = req.user?._id || req.user?.id;
      if (!userId) return res.status(401).json({ error: 'Пользователь не найден' });
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try { effectiveUserId = await getCompositeUserId(req); } catch (_) { }
      }
      const workspaceId = req.user?.currentWorkspaceId || null;
      const packet = await contextPacketService.getMonthlyPacket({
        workspaceId,
        userId: String(effectiveUserId || userId),
        periodKey
      });
      if (!packet) return res.status(404).json({ error: 'Context packet not found' });
      return res.json({ packet });
    } catch (err) {
      return res.status(500).json({ error: 'Ошибка чтения context packet' });
    }
  });

  // =========================
  // VERSION ROUTE
  // =========================
  router.get('/version', (req, res) => {
    res.json({
      version: AIROUTES_VERSION,
      tag: CHAT_VERSION_TAG,
      contextPackets: contextPacketsEnabled,
      modes: {
        quick: 'modes/quickMode.js',
        deep: 'unified-context-packet'
      }
    });
  });

  return router;
};
