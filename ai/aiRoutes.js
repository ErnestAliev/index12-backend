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
    const lines = [`ТЕКУЩИЕ ДАННЫЕ (из БД на ${data.meta?.today || 'сегодня'}):`];

    lines.push('СЧЕТА:');
    (data.accounts || []).forEach(a => {
      const hiddenMarker = a.isHidden ? ' [СКРЫТ/ИСКЛЮЧЕН]' : '';
      lines.push(`- ${a.name}${hiddenMarker}: ${_formatTenge(a.currentBalance || 0)} (Прогноз: ${_formatTenge(a.futureBalance || 0)})`);
    });

    lines.push('');
    lines.push('СВОДКА ОПЕРАЦИЙ:');
    const s = data.operationsSummary || {};
    lines.push(`Доходы: Факт ${_formatTenge(s.income?.fact?.total || 0)}, Прогноз ${_formatTenge(s.income?.forecast?.total || 0)}`);
    lines.push(`Расходы: Факт ${_formatTenge(s.expense?.fact?.total || 0)}, Прогноз ${_formatTenge(s.expense?.forecast?.total || 0)}`);

    lines.push('');
    lines.push('КАТАЛОГИ:');
    lines.push(`Проекты: ${(data.catalogs?.projects || []).join(', ')}`);
    lines.push(`Контрагенты: ${(data.catalogs?.contractors || []).join(', ')}`);
    lines.push(`Категории: ${(data.catalogs?.categories || []).map(c => typeof c === 'string' ? c : c.name).join(', ')}`);

    lines.push('');
    lines.push('ПОСЛЕДНИЕ ОПЕРАЦИИ:');
    (data.operations || []).slice(0, 50).forEach(op => {
      lines.push(`${op.date} | ${op.kind} | ${op.amount} | ${op.category || 'Без кат.'} | ${op.description || ''}`);
    });

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

      console.log('🔍 [AI QUERY] ================================================');
      console.log('🔍 req.user:', JSON.stringify(req.user, null, 2));
      console.log('🔍 userId extracted:', userId);
      console.log('🔍 userIdStr:', userIdStr);

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
          console.log('🔍 getCompositeUserId returned:', effectiveUserId);
        } catch (e) {
          console.error('❌ Failed to get composite userId:', e);
        }
      }

      console.log('🔍 effectiveUserId (final):', effectiveUserId);
      console.log('🔍 includeHidden:', req?.body?.includeHidden);
      console.log('🔍 visibleAccountIds:', req?.body?.visibleAccountIds);

      // Build data packet from database
      console.log(`🔍 [AI] Calling dataProvider.buildDataPacket for user: ${effectiveUserId}`);
      const dbData = await dataProvider.buildDataPacket(effectiveUserId, {
        includeHidden: req?.body?.includeHidden !== false,
        visibleAccountIds: req?.body?.visibleAccountIds || null,
      });

      console.log(`🔍 [AI] DB Results - Accounts: ${dbData.accounts?.length || 0}, Ops: ${dbData.operations?.length || 0}`);
      console.log('🔍 [AI] First 3 accounts:', dbData.accounts?.slice(0, 3).map(a => ({ name: a.name, id: a._id })));
      console.log('🔍 ================================================');

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

        lines.push(`Счета. На ${dbData.meta?.today || _fmtDateKZ(_endOfToday())}`);
        lines.push('');

        if (!accounts.length) {
          lines.push('Счета не найдены.');
        } else {
          for (const acc of accounts) {
            const balance = acc.currentBalance || 0;
            const name = acc.name || 'Счет';
            const marker = acc.isHidden ? ' (скрыт)' : '';
            lines.push(`${name}${marker}: ${_formatTenge(balance)}`);
          }

          lines.push('');
          lines.push(`Всего (без скрытых): ${_formatTenge(totals.open?.current || 0)}`);
          if (totals.hidden?.current) {
            lines.push(`Всего (включая скрытые): ${_formatTenge(totals.all?.current || 0)}`);
          }
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
        lines.push(`Доходы. До ${dbData.meta?.today || _fmtDateKZ(_endOfToday())}`);
        lines.push('');
        lines.push(`Факт: ${_formatTenge(incomeData.fact?.total || 0)} (${incomeData.fact?.count || 0} операций)`);
        lines.push(`Прогноз: ${_formatTenge(incomeData.forecast?.total || 0)} (${incomeData.forecast?.count || 0} операций)`);
        lines.push('');
        lines.push(`Итого: ${_formatTenge(incomeData.total || 0)}`);

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
        lines.push(`Расходы. До ${dbData.meta?.today || _fmtDateKZ(_endOfToday())}`);
        lines.push('');
        lines.push(`Факт: ${_formatTenge(expenseData.fact?.total || 0)} (${expenseData.fact?.count || 0} операций)`);
        lines.push(`Прогноз: ${_formatTenge(expenseData.forecast?.total || 0)} (${expenseData.forecast?.count || 0} операций)`);
        lines.push('');
        lines.push(`Итого: ${_formatTenge(expenseData.total || 0)}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // PROJECTS CATALOG
      // =========================
      if (/\b(проект|project)\b/i.test(qLower)) {
        const projects = dbData.catalogs?.projects || [];
        if (!projects.length) {
          const answer = 'Проекты не найдены.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Мои проекты', ''];
        projects.forEach((p, i) => lines.push(`${i + 1}. ${p}`));
        lines.push('', `Всего: ${projects.length}`);

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
          const answer = 'Контрагенты не найдены.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Мои контрагенты', ''];
        contractors.forEach((c, i) => lines.push(`${i + 1}. ${c}`));
        lines.push('', `Всего: ${contractors.length}`);

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
          const answer = 'Физические лица не найдены.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Физические лица', ''];
        individuals.forEach((ind, i) => lines.push(`${i + 1}. ${ind}`));
        lines.push('', `Всего: ${individuals.length}`);

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
          const answer = 'Категории не найдены.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = ['Мои категории', ''];
        categories.forEach((cat, i) => lines.push(`${i + 1}. ${cat}`));
        lines.push('', `Всего: ${categories.length}`);

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
        'Ты финансовый помощник INDEX12.',
        'Твоя задача: отвечать на вопросы пользователя, используя предоставленные данные из базы данных.',
        'ДАННЫЕ РЕАЛЬНЫЕ, не выдумывай их.',
        'Если данных нет (например, 0 операций), так и скажи.',
        'Тон: профессиональный, лаконичный.',
        'Формат денег: 1 234 ₸.',
        'Максимальная длина ответа: 10-15 строк.',
        'Всегда ссылайся на даты из данных.',
      ].join('\n');

      const dataContext = _formatDbDataForAi(dbData);
      console.log(`[AI] Prompt Context - Scounts: ${dbData.accounts?.length || 0}, Ops: ${dbData.operations?.length || 0}`);
      // console.log('[AI] Context Preview:', dataContext.substring(0, 500) + '...');

      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'system', content: dataContext },
        ..._getHistoryMessages(userIdStr)
      ];

      const aiResponse = await _openAiChat(messages);
      _pushHistory(userIdStr, 'assistant', aiResponse);

      return res.json({ text: aiResponse });

    } catch (err) {
      console.error('[AI ERROR]', err);
      return res.status(500).json({ text: `Ошибка AI: ${err.message}` });
    }
  });

  return router;
};
