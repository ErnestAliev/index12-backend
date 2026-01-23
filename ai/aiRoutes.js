// backend/ai/aiRoutes.js
// AI assistant routes - PURE DATABASE MODE
// All data comes from MongoDB via dataProvider (no uiSnapshot)

const express = require('express');

const AIROUTES_VERSION = 'db-only-v5.0';

const https = require('https');

// =========================
// Chat session state (in-memory, TTL)
// =========================
const SESSION_TTL_MS = 30 * 60 * 1000;
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
    prefs: { format: 'short', limit: 50 },
    pending: null,
    history: [],
  };
  _chatSessions.set(key, fresh);
  return fresh;
};

// =========================
// CHAT HISTORY HELPERS
// =========================
const HISTORY_MAX_MESSAGES = 40;

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

  const router = express.Router();

  // =========================
  // KZ time helpers (Asia/Almaty ~ UTC+05:00)
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

  const _openAiChat = async (messages, { temperature = 0, maxTokens = 600 } = {}) => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.warn('OPENAI_API_KEY is missing');
      return 'Ошибка: OPENAI_API_KEY не задан.';
    }

    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';
    const isReasoningModel = /^o[13]|gpt-5/.test(model);

    const payloadObj = {
      model,
      messages,
      max_completion_tokens: maxTokens,
    };
    if (!isReasoningModel) {
      payloadObj.temperature = temperature;
    }
    const payload = JSON.stringify(payloadObj);

    return new Promise((resolve, reject) => {
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
        console.error('Request Error:', e);
        resolve('Ошибка связи с AI.');
      });
      gptReq.write(payload);
      gptReq.end();
    });
  };

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
    lines.push('Сводка операций:');
    lines.push(`- Доходы: факт ${_formatTenge(inc.fact?.total || 0)} (${inc.fact?.count || 0}), прогноз ${_formatTenge(inc.forecast?.total || 0)} (${inc.forecast?.count || 0})`);
    lines.push(`- Расходы: факт ${_formatTenge(-(exp.fact?.total || 0))} (${exp.fact?.count || 0}), прогноз ${_formatTenge(-(exp.forecast?.total || 0))} (${exp.forecast?.count || 0})`);

    return lines.join('\n');
  };

  const _isAiAllowed = (req) => {
    try {
      if ((process.env.AI_ALLOW_ALL || '').toLowerCase() === 'true') return true;
      if (!req.user || !req.user.email) return false;

      const allowEmails = (process.env.AI_ALLOW_EMAILS || '')
        .split(',')
        .map(s => s.trim().toLowerCase())
        .filter(Boolean);

      if (!allowEmails.length && (FRONTEND_URL || '').includes('localhost')) return true;
      return allowEmails.includes(String(req.user.email).toLowerCase());
    } catch (_) {
      return false;
    }
  };

  // =========================
  // Routes
  // =========================
  router.get('/ping', (req, res) => {
    res.json({
      ok: true,
      ts: new Date().toISOString(),
      version: AIROUTES_VERSION,
      mode: 'PURE_DATABASE',
      isAuthenticated: (typeof req.isAuthenticated === 'function') ? req.isAuthenticated() : false,
      email: req.user?.email || null,
    });
  });

  router.post('/query', isAuthenticated, async (req, res) => {
    try {
      if (!_isAiAllowed(req)) return res.status(402).json({ message: 'AI not activated' });

      const userId = req.user?.id || req.user?._id;
      const userIdStr = String(userId);

      const qRaw = (req.body && req.body.message) ? String(req.body.message) : '';
      const q = qRaw.trim();
      if (!q) return res.status(400).json({ message: 'Empty message' });

      const qLower = q.toLowerCase();

      // =========================
      // 🔥 PURE DATABASE MODE
      // All data comes from MongoDB via dataProvider
      // =========================

      // Get effective userId (handles workspace isolation)
      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try {
          effectiveUserId = await getCompositeUserId(req);
        } catch (e) {
          console.error('❌ Failed to get composite userId:', e);
        }
      }

      // Build data packet from database
      const userIdsList = Array.from(
        new Set(
          [effectiveUserId, req.user?.id || req.user?._id].filter(Boolean).map(String)
        )
      );

      if (process.env.AI_DEBUG === '1') {
        console.log('[AI_DEBUG] effectiveUserId:', effectiveUserId, 'allUserIds:', userIdsList, 'workspaceId:', req.user?.currentWorkspaceId);
        console.log('[AI_DEBUG] includeHidden flag:', req?.body?.includeHidden, 'visibleAccountIds:', req?.body?.visibleAccountIds);
      }

      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: true, // всегда берем скрытые для ответа AI
        visibleAccountIds: req?.body?.visibleAccountIds || null,
        dateRange: req?.body?.periodFilter || null,
        workspaceId: req.user?.currentWorkspaceId || null,
        now: req?.body?.asOf || null,
      });

      const debugRequested = process.env.AI_DEBUG === '1' || req?.body?.debugAi === true;
      let debugInfo = null;

      if (debugRequested || req?.body?.includeHidden) {
        const hiddenAccs = (dbData.accounts || []).filter(a => a.isHidden);
        const totalAccs = (dbData.accounts || []).length;
        debugInfo = {
          totalAccounts: totalAccs,
          hiddenCount: hiddenAccs.length,
          hiddenNames: hiddenAccs.map(a => a.name),
        };
        console.log('[AI_DEBUG] accounts total:', totalAccs, 'hidden:', hiddenAccs.length);
        if (hiddenAccs.length) {
          console.log('[AI_DEBUG] hidden list:', hiddenAccs.map(a => `${a.name} (${a._id})`).join(', '));
        }
      }

      // Store user message in history
      _pushHistory(userIdStr, 'user', q);

      // =========================
      // DIAGNOSTICS COMMAND
      // =========================
      const _isDiagnosticsQuery = (s) => {
        const t = String(s || '').toLowerCase();
        if (!t) return false;
        if (t.includes('диагност') || t.includes('агност') || t.includes('diagnostic')) return true;
        return /(^|[^a-z])diag([^a-z]|$)/i.test(t);
      };

      if (_isDiagnosticsQuery(qLower)) {
        const diag = [
          `Диагностика AI (версия: ${AIROUTES_VERSION})`,
          `Режим: PURE DATABASE (MongoDB)`,
          '',
          `Пользователь: ${effectiveUserId}`,
          `Счета: ${dbData.accounts?.length || 0}`,
          `Операции: ${dbData.operations?.length || 0}`,
          '',
          `Доходы (факт): ${_formatTenge(dbData.operationsSummary?.income?.fact?.total || 0)}`,
          `Расходы (факт): ${_formatTenge(dbData.operationsSummary?.expense?.fact?.total || 0)}`,
          '',
          `Проекты: ${dbData.catalogs?.projects?.length || 0}`,
          `Контрагенты: ${dbData.catalogs?.contractors?.length || 0}`,
          `Категории: ${dbData.catalogs?.categories?.length || 0}`,
          `Физлица: ${dbData.catalogs?.individuals?.length || 0}`,
          `Компании: ${dbData.catalogs?.companies?.length || 0}`,
        ];
        const answer = diag.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // ACCOUNTS QUERY
      // =========================
      if (/\b(сч[её]т|счета|касс[аы]|баланс)\b/i.test(qLower)) {
        const lines = [];
        const accounts = dbData.accounts || [];
        const totals = dbData.totals || {};

        const periodStart = dbData.meta?.periodStart || '';
        const periodEnd = dbData.meta?.periodEnd || dbData.meta?.today || _fmtDateKZ(_endOfToday());
        const periodLabel = periodStart ? `с ${periodStart} по ${periodEnd}` : `на ${periodEnd}`;

        lines.push(`Счета за период ${periodLabel}`);
        lines.push('');

        if (!accounts.length) {
          lines.push('Счета не найдены.');
        } else {
          const openAccs = accounts.filter(a => !a.isHidden);
          const hiddenAccs = accounts.filter(a => a.isHidden);

          lines.push('Открытые:');
          if (openAccs.length) {
            for (const acc of openAccs) {
              const balance = acc.currentBalance || 0;
              const name = acc.name || 'Счет';
              lines.push(`${name}: ${_formatTenge(balance)}`);
            }
          } else {
            lines.push('- нет');
          }

          lines.push('');
          lines.push('Скрытые:');
          if (hiddenAccs.length) {
            for (const acc of hiddenAccs) {
              const balance = acc.currentBalance || 0;
              const name = acc.name || 'Счет';
              lines.push(`${name} (скрыт): ${_formatTenge(balance)}`);
            }
          } else {
            lines.push('- нет');
          }

          lines.push('');
          const totalOpen = totals.open?.current ?? 0;
          const totalHidden = totals.hidden?.current ?? 0;
          const totalAll = totals.all?.current ?? (totalOpen + totalHidden);

          lines.push(`Итого по открытым счетам: ${_formatTenge(totalOpen)}`);
          lines.push(`Итого по скрытым счетам: ${_formatTenge(totalHidden)}`);
          lines.push(`Итого по всем счетам: ${_formatTenge(totalAll)}`);
        }

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // INCOME QUERY
      // =========================
      if (/\b(доход|поступлен|приход)\b/i.test(qLower) && !/\bрасход\b/i.test(qLower)) {
        const summary = dbData.operationsSummary || {};
        const incomeData = summary.income || {};

        const lines = [];
        const periodStart = dbData.meta?.periodStart || dbData.meta?.today || '';
        const periodEnd = dbData.meta?.periodEnd || dbData.meta?.today || '';
        const periodLabel = periodStart && periodEnd ? `${periodStart} — ${periodEnd}` : (periodStart || periodEnd || 'не указан');

        lines.push(`Доходы за период ${periodLabel}`);
        lines.push(`Факт: ${_formatTenge(incomeData.fact?.total || 0)} (${incomeData.fact?.count || 0} операций)`);
        lines.push(`Прогноз: ${_formatTenge(incomeData.forecast?.total || 0)} (${incomeData.forecast?.count || 0} операций)`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // EXPENSE QUERY
      // =========================
      if (/\b(расход|трат|затрат)\b/i.test(qLower)) {
        const summary = dbData.operationsSummary || {};
        const expenseData = summary.expense || {};

        const lines = [];
        const periodStart = dbData.meta?.periodStart || '';
        const periodEnd = dbData.meta?.periodEnd || dbData.meta?.today || _fmtDateKZ(_endOfToday());
        const periodLabel = periodStart ? `с ${periodStart} по ${periodEnd}` : `до ${periodEnd}`;

        lines.push(`Расходы за период ${periodLabel}`);
        lines.push('');
        lines.push(`Факт: ${_formatTenge(expenseData.fact?.total ? -expenseData.fact.total : 0)} (${expenseData.fact?.count || 0} операций)`);
        lines.push(`Прогноз: ${_formatTenge(expenseData.forecast?.total ? -expenseData.forecast.total : 0)} (${expenseData.forecast?.count || 0} операций)`);
        lines.push('');
        lines.push(`Итого: ${_formatTenge(expenseData.total ? -expenseData.total : 0)}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // PROJECTS CATALOG
      // =========================
      if (/\b(проек\w*|project)\b/i.test(qLower)) {
        const projects = dbData.catalogs?.projects || [];
        if (!projects.length) {
          const answer = 'Проектов нет.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Проекты:', ...projects.map((p, i) => `${i + 1}. ${p}`), `Всего: ${projects.length}`];

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // CONTRACTORS CATALOG
      // =========================
      if (/\b(контрагент|поставщик|партнёр|партнер)\b/i.test(qLower)) {
        const contractors = dbData.catalogs?.contractors || [];
        if (!contractors.length) {
          const answer = 'Контрагентов нет.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Контрагенты:', ...contractors.map((c, i) => `${i + 1}. ${c}`), `Всего: ${contractors.length}`];

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // INDIVIDUALS CATALOG
      // =========================
      if (/\b(физ\W*лиц|физическ|индивид|person)\b/i.test(qLower)) {
        const individuals = dbData.catalogs?.individuals || [];
        if (!individuals.length) {
          const answer = 'Физических лиц нет.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Физические лица:', ...individuals.map((ind, i) => `${i + 1}. ${ind}`), `Всего: ${individuals.length}`];

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // CATEGORIES CATALOG
      // =========================
      if (/\b(категори|category)\b/i.test(qLower)) {
        const categories = dbData.catalogs?.categories || [];
        if (!categories.length) {
          const answer = 'Категорий нет.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Категории:', ...categories.map((cat, i) => `${i + 1}. ${cat}`), `Всего: ${categories.length}`];

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // COMPANIES CATALOG
      // =========================
      if (/\b(компани|фирм|организаци|company)\b/i.test(qLower)) {
        const companies = dbData.catalogs?.companies || [];
        if (!companies.length) {
          const answer = 'Компании не найдены.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Мои компании', ''];
        companies.forEach((comp, i) => lines.push(`${i + 1}. ${comp}`));
        lines.push('', `Всего: ${companies.length}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // AI GENERATION (OpenAI)
      // Universal fallback for all queries
      // =========================
      const systemPrompt = [
        'Ты финансовый аналитик INDEX12.',
        'Отвечай строго по данным, ничего не придумывай.',
        'Коротко и по делу, без советов и вопросов. Если данных много — либо выведи полный список, либо сгруппируй (например, по категориям/счетам) и явно отметь, что это агрегирование. Ничего важного не обрезай.',
        'Не путай доходы и прибыль: показывай доходы и расходы отдельно, не считай разницу, если это не запросили.',
        'Если спрашивают про доходы или расходы — выведи период, итоги факт/прогноз и при необходимости агрегаты по категориям/счетам. Не нужно перечислять несколько последних операций.',
        'Деньги: "1 234 ₸"; расходы со знаком минус, доходы с плюсом.',
        'Для счетов: перечисли открытые и скрытые отдельно, затем итоги.',
        'Если данных нет — так и напиши, без воды.',
        'Указывай даты операций в формате дд.мм.гг, если они есть.',
      ].join('\n');

      const hiddenAccs = (dbData.accounts || []).filter(a => a.isHidden);
      const openAccs = (dbData.accounts || []).filter(a => !a.isHidden);
      const dataContext = _formatDbDataForAi(dbData);
      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'system', content: dataContext },
        ..._getHistoryMessages(userIdStr)
      ];

      const aiResponse = await _openAiChat(messages);
      _pushHistory(userIdStr, 'assistant', aiResponse);

      if (debugRequested) {
        debugInfo = debugInfo || {};
        debugInfo.hiddenNames = hiddenAccs.map(a => a.name);
        debugInfo.hiddenCount = hiddenAccs.length;
        debugInfo.openNames = openAccs.map(a => a.name);
        debugInfo.openCount = openAccs.length;
        debugInfo.opsSummary = dbData.operationsSummary || {};
        debugInfo.sampleOps = (dbData.operations || []).slice(0, 5).map(op => ({
          date: op.date,
          amount: op.amount,
          rawAmount: op.rawAmount,
          kind: op.kind,
          isFact: op.isFact
        }));
        return res.json({ text: aiResponse, debug: debugInfo });
      }

      return res.json({ text: aiResponse });

    } catch (err) {
      console.error('[AI ERROR]', err);
      return res.status(500).json({ text: `Ошибка AI: ${err.message}` });
    }
  });

  return router;
};
