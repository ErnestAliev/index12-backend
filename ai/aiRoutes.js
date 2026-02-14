// backend/ai/aiRoutes.js
// AI assistant routes - MODULAR ARCHITECTURE
//
// ✅ Features:
// - QUICK mode: deterministic lists → modes/quickMode.js
// - CHAT mode: general conversation → modes/chatMode.js  
// - DEEP mode: CFO analysis → modes/deepMode.js
// - DIAG command: diagnostics
// - Hybrid data: snapshot (accounts/companies) + MongoDB (operations)

const express = require('express');

const AIROUTES_VERSION = 'modular-v8.0';
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
    FRONTEND_URL,
    isAuthenticated,
    getCompositeUserId,
  } = deps;

  const { Event, Account, Company, Contractor, Individual, Project, Category } = models;

  // Create data provider for direct database access
  const createDataProvider = require('./dataProvider');
  const dataProvider = createDataProvider({ ...models, mongoose });

  // Import mode handlers
  const quickMode = require('./modes/quickMode');
  const chatMode = require('./modes/chatMode');
  const deepMode = require('./modes/deepMode');

  const router = express.Router();

  // Метка версии для быстрой проверки деплоя
  const CHAT_VERSION_TAG = 'aiRoutes-modular-v8.0';

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
      return sign + new Intl.NumberFormat('ru-RU').format(Math.abs(Math.round(num))).replace(/\u00A0/g, ' ') + ' ₸';
    } catch (_) {
      return sign + String(Math.round(Math.abs(num))) + ' ₸';
    }
  };

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
    const isReasoningModel = /^o[13]/i.test(model) || /^gpt-5/i.test(model);

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
              resolve(parsed?.choices?.[0]?.message?.content || 'Нет ответа от AI.');
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
  // DB data context for LLM
  // =========================
  const _formatDbDataForAi = (data) => {
    const lines = [];
    const meta = data.meta || {};
    const opsSummary = data.operationsSummary || {};
    const totals = data.totals || {};

    lines.push(`Данные БД: период ${meta.periodStart || '?'} — ${meta.periodEnd || meta.today || '?'}`);
    lines.push(`Сегодня: ${meta.today || '?'}`);

    // Accounts
    lines.push('Счета (текущий → прогноз):');
    (data.accounts || []).slice(0, 50).forEach(a => {
      const hiddenMarker = a.isHidden ? ' [скрыт]' : '';
      const curr = _formatTenge(a.currentBalance || 0);
      const fut = _formatTenge(a.futureBalance || 0);
      lines.push(`- ${a.name}${hiddenMarker}: ${curr} → ${fut}`);
    });
    const totalOpen = totals.open?.current ?? 0;
    const totalHidden = totals.hidden?.current ?? 0;
    const totalAll = totals.all?.current ?? (totalOpen + totalHidden);
    lines.push(`Итоги счетов: открытые ${_formatTenge(totalOpen)}, скрытые ${_formatTenge(totalHidden)}, все ${_formatTenge(totalAll)}`);

    // Operations summary
    const inc = opsSummary.income || {};
    const exp = opsSummary.expense || {};
    const tr = opsSummary.transfer || {};
    const trOut = tr.withdrawalOut || {};
    lines.push('Сводка операций:');
    lines.push(`- Доходы: факт ${_formatTenge(inc.fact?.total || 0)} (${inc.fact?.count || 0}), прогноз ${_formatTenge(inc.forecast?.total || 0)} (${inc.forecast?.count || 0})`);
    lines.push(`- Расходы: факт ${_formatTenge(-(exp.fact?.total || 0))} (${exp.fact?.count || 0}), прогноз ${_formatTenge(-(exp.forecast?.total || 0))} (${exp.forecast?.count || 0})`);
    lines.push(`- Переводы: факт ${_formatTenge(tr.fact?.total || 0)} (${tr.fact?.count || 0}), прогноз ${_formatTenge(tr.forecast?.total || 0)} (${tr.forecast?.count || 0})`);
    lines.push(`- Вывод средств (подтип перевода): факт ${_formatTenge(trOut.fact?.total || 0)} (${trOut.fact?.count || 0}), прогноз ${_formatTenge(trOut.forecast?.total || 0)} (${trOut.forecast?.count || 0})`);

    const quality = data.dataQualityReport || null;
    if (quality && quality.status) {
      const statusLabel = String(quality.status || '').toUpperCase();
      const score = Number.isFinite(Number(quality.score)) ? Math.round(Number(quality.score)) : null;
      lines.push(`Качество данных: ${statusLabel}${score !== null ? ` (score ${score}/100)` : ''}`);

      const issues = Array.isArray(quality.issues) ? quality.issues : [];
      if (issues.length) {
        lines.push('Проблемы качества данных:');
        issues.slice(0, 5).forEach((issue) => {
          const sev = String(issue?.severity || 'warn').toUpperCase();
          const msg = issue?.message || issue?.code || 'Проблема данных';
          const count = Number.isFinite(Number(issue?.count)) ? Number(issue.count) : null;
          lines.push(`- [${sev}] ${msg}${count !== null ? ` (${count})` : ''}`);
        });
      }
    }

    // Category breakdown (TOP categories for business identification)
    const catSum = data.categorySummary || [];
    if (catSum.length > 0) {
      const catIncomeFact = (c) => {
        if (c?.incomeFact !== undefined && c?.incomeFact !== null) return Number(c.incomeFact) || 0;
        return Number(c?.income?.fact?.total) || 0;
      };
      const catIncomeForecast = (c) => {
        if (c?.incomeForecast !== undefined && c?.incomeForecast !== null) return Number(c.incomeForecast) || 0;
        return Number(c?.income?.forecast?.total) || 0;
      };
      const catExpenseFactAbs = (c) => {
        if (c?.expenseFact !== undefined && c?.expenseFact !== null) return Math.abs(Number(c.expenseFact) || 0);
        return Math.abs(Number(c?.expense?.fact?.total) || 0);
      };
      const catExpenseForecastAbs = (c) => {
        if (c?.expenseForecast !== undefined && c?.expenseForecast !== null) return Math.abs(Number(c.expenseForecast) || 0);
        return Math.abs(Number(c?.expense?.forecast?.total) || 0);
      };

      const incomeCategories = catSum
        .map(c => ({
          ...c,
          _incomeFact: catIncomeFact(c),
          _incomeForecast: catIncomeForecast(c),
        }))
        .filter(c => (c._incomeFact + c._incomeForecast) > 0)
        .sort((a, b) => (b._incomeFact + b._incomeForecast) - (a._incomeFact + a._incomeForecast))
        .slice(0, 10);

      const expenseCategories = catSum
        .map(c => ({
          ...c,
          _expenseFactAbs: catExpenseFactAbs(c),
          _expenseForecastAbs: catExpenseForecastAbs(c),
        }))
        .filter(c => (c._expenseFactAbs + c._expenseForecastAbs) > 0)
        .sort((a, b) => (b._expenseFactAbs + b._expenseForecastAbs) - (a._expenseFactAbs + a._expenseForecastAbs))
        .slice(0, 10);

      if (incomeCategories.length > 0) {
        lines.push('Топ категории доходов (факт/прогноз):');
        incomeCategories.forEach(c => {
          lines.push(`- ${c.name}: ${_formatTenge(c._incomeFact)} / ${_formatTenge(c._incomeForecast)}`);
        });
      }

      if (expenseCategories.length > 0) {
        lines.push('Топ категории расходов (факт/прогноз):');
        expenseCategories.forEach(c => {
          lines.push(`- ${c.name}: ${_formatTenge(c._expenseFactAbs)} / ${_formatTenge(c._expenseForecastAbs)}`);
        });
      }
    }

    return lines.join('\n');
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
  // DIAGNOSTICS COMMAND
  // =========================
  const _isDiagnosticsQuery = (s) => {
    const t = String(s || '').toLowerCase();
    if (!t) return false;
    if (t.includes('диагност') || t.includes('diagnostic')) return true;
    return /(^|[^a-z])diag([^a-z]|$)/i.test(t);
  };

  const _isFullDiagnosticsQuery = (s) => {
    const t = String(s || '').toLowerCase();
    // Специальный триггер с опечаткой "дикагностика"
    return t.includes('дикагност');
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

      const AI_DEBUG = process.env.AI_DEBUG === 'true';
      let debugInfo = null;

      if (AI_DEBUG) {
        console.log('[AI_DEBUG] query:', qLower, 'deep=', isDeep, 'source=', source);
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
      // DIAGNOSTICS COMMAND
      // =========================
      if (_isFullDiagnosticsQuery(qLower)) {
        const lines = [];
        const meta = dbData.meta || {};

        lines.push('ДИКАГНОСТИКА (полный список)');
        lines.push(`Период: ${meta.periodStart || '?'} — ${meta.periodEnd || meta.today || '?'}`);
        lines.push(`Сегодня: ${meta.today || '?'}`);
        lines.push('');

        // Счета
        const accounts = Array.isArray(dbData.accounts) ? dbData.accounts : [];
        lines.push(`Счета (${accounts.length}):`);
        accounts.forEach(a => lines.push(`- ${a.name || 'Счет'} | cur=${_formatTenge(a.currentBalance || 0)} | hidden=${a.isHidden ? 'yes' : 'no'} | id=${a._id}`));
        lines.push('');

        // Компании
        const companies = Array.isArray(dbData.catalogs?.companies) ? dbData.catalogs.companies : [];
        lines.push(`Компании (${companies.length}):`);
        companies.forEach(c => lines.push(`- ${c.name || c} | id=${c.id || c._id || '?'}`));
        lines.push('');

        // Проекты
        const projects = Array.isArray(dbData.catalogs?.projects) ? dbData.catalogs.projects : [];
        lines.push(`Проекты (${projects.length}):`);
        projects.forEach(p => lines.push(`- ${p.name || p} | id=${p.id || p._id || '?'}`));
        lines.push('');

        // Категории
        const categories = Array.isArray(dbData.catalogs?.categories) ? dbData.catalogs.categories : [];
        lines.push(`Категории (${categories.length}):`);
        categories.forEach(cat => lines.push(`- ${cat.name || cat} | id=${cat.id || cat._id || '?'}`));
        lines.push('');

        // Операции
        const ops = Array.isArray(dbData.operations) ? dbData.operations : [];
        lines.push(`Операций: ${ops.length}`);
        if (dbData?.dataQualityReport?.status) {
          const q = dbData.dataQualityReport;
          lines.push(`Качество данных: ${String(q.status).toUpperCase()} (score ${Math.round(Number(q.score) || 0)}/100)`);
          (Array.isArray(q.issues) ? q.issues : []).slice(0, 10).forEach((issue) => {
            lines.push(`- ${issue.message || issue.code} (${issue.count || 0})`);
          });
        }

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'user', q);
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (_isDiagnosticsQuery(qLower)) {
        const lines = [];
        const meta = dbData.meta || {};
        const opsSummary = dbData.operationsSummary || {};

        lines.push('ДИАГНОСТИКА');
        lines.push(`Период: ${meta.periodStart || '?'} — ${meta.periodEnd || meta.today || '?'}`);
        lines.push(`Сегодня: ${meta.today || '?'}`);
        lines.push('');
        lines.push(`Счетов: ${(dbData.accounts || []).length}`);
        lines.push(`Операций: ${(dbData.operations || []).length}`);
        lines.push(`Доходов (факт): ${_formatTenge(opsSummary.income?.fact?.total || 0)} (${opsSummary.income?.fact?.count || 0})`);
        lines.push(`Расходов (факт): ${_formatTenge(opsSummary.expense?.fact?.total || 0)} (${opsSummary.expense?.fact?.count || 0})`);
        lines.push(`Переводов (факт): ${_formatTenge(opsSummary.transfer?.fact?.total || 0)} (${opsSummary.transfer?.fact?.count || 0})`);
        lines.push(`Вывод средств (факт): ${_formatTenge(opsSummary.transfer?.withdrawalOut?.fact?.total || 0)} (${opsSummary.transfer?.withdrawalOut?.fact?.count || 0})`);
        if (dbData?.dataQualityReport?.status) {
          const q = dbData.dataQualityReport;
          lines.push(`Качество данных: ${String(q.status).toUpperCase()} (score ${Math.round(Number(q.score) || 0)}/100)`);
          const topIssue = Array.isArray(q.issues) && q.issues.length ? q.issues[0] : null;
          if (topIssue) lines.push(`Ключевая проблема: ${topIssue.message || topIssue.code} (${topIssue.count || 0})`);
        }

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'user', q);
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
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

        console.log('[aiRoutes] quickMode returned:', quickResponse ? 'RESPONSE' : 'NULL');
        console.log('[aiRoutes] dbData.operations count:', (dbData.operations || []).length);
        console.log('[aiRoutes] transfers count:', (dbData.operations || []).filter(op => op.kind === 'transfer').length);

        if (quickResponse) {
          _pushHistory(userIdStr, 'user', q);
          _pushHistory(userIdStr, 'assistant', quickResponse);
          return res.json({ text: quickResponse });
        }
      }

      // =========================
      // DEEP MODE (CFO-level analysis)
      // =========================
      if (isDeep) {
        const session = _getChatSession(userIdStr);
        const history = _getHistoryMessages(userIdStr);
        const modelDeep = process.env.OPENAI_MODEL_DEEP || 'gpt-4o';

        const { answer, shouldSaveToHistory } = await deepMode.handleDeepQuery({
          query: q,
          dbData,
          session,
          history,
          openAiChat: _openAiChat,
          formatDbDataForAi: _formatDbDataForAi,
          formatTenge: _formatTenge,
          modelDeep
        });

        if (shouldSaveToHistory) {
          _pushHistory(userIdStr, 'user', q);
          _pushHistory(userIdStr, 'assistant', answer);
        }

        return res.json({ text: answer });
      }

      // =========================
      // CHAT MODE (GPT-4o fallback for general queries)
      // =========================
      const history = _getHistoryMessages(userIdStr);
      const modelChat = process.env.OPENAI_MODEL || 'gpt-4o';

      const response = await chatMode.handleChatQuery({
        query: q,
        dbData,
        history,
        openAiChat: _openAiChat,
        formatDbDataForAi: _formatDbDataForAi,
        modelChat
      });

      _pushHistory(userIdStr, 'user', q);
      _pushHistory(userIdStr, 'assistant', response);
      return res.json({ text: response });

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
  // VERSION ROUTE
  // =========================
  router.get('/version', (req, res) => {
    res.json({
      version: AIROUTES_VERSION,
      tag: CHAT_VERSION_TAG,
      modes: {
        quick: 'modes/quickMode.js',
        chat: 'modes/chatMode.js',
        deep: 'modes/deepMode.js'
      }
    });
  });

  return router;
};
