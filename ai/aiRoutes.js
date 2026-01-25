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

  const _openAiChat = async (messages, { temperature = 0, maxTokens = 600, modelOverride = null } = {}) => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      console.warn('OPENAI_API_KEY is missing');
      return 'Ошибка: OPENAI_API_KEY не задан.';
    }

    const model = modelOverride || process.env.OPENAI_MODEL || 'gpt-4o-mini';
    const isReasoningModel = /^(o\d|gpt-5)/.test(String(model || ''));

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

  const _absExpense = (op) => {
    if (!op || op.isTransfer) return 0;
    const raw = Number(op.rawAmount ?? op.amount ?? 0);
    if (op.kind === 'income') return 0;
    if (op.kind === 'expense' || raw < 0) return Math.abs(raw || 0);
    return 0;
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

    // Contractors summary (top 5 by volume)
    const contractorSummary = (data.contractorSummary || []).slice(0, 5);
    if (contractorSummary.length) {
      lines.push('Контрагенты (топ по обороту):');
      contractorSummary.forEach(c => {
        const vol = (c.incomeFact + c.incomeForecast + c.expenseFact + c.expenseForecast);
        const sharePct = c.share ? Math.round(c.share * 1000) / 10 : 0;
        lines.push(`- ${c.name}: доход +${_formatTenge(c.incomeFact + c.incomeForecast)}, расход -${_formatTenge(c.expenseFact + c.expenseForecast)}, оборот ${_formatTenge(vol)} (${sharePct}%)`);
      });
    }

    // Categories summary (top 5 by volume)
    const categorySummary = (data.categorySummary || []).slice(0, 5);
    if (categorySummary.length) {
      lines.push('Категории (топ по обороту):');
      categorySummary.forEach(cat => {
        const incomeTotal = cat.incomeFact + cat.incomeForecast;
        const expenseTotal = cat.expenseFact + cat.expenseForecast;
        const vol = incomeTotal + expenseTotal;
        const tags = (cat.tags && cat.tags.length) ? ` [${cat.tags.join(', ')}]` : '';
        const incPct = cat.incomeShare ? Math.round(cat.incomeShare * 1000) / 10 : 0;
        const expPct = cat.expenseShare ? Math.round(cat.expenseShare * 1000) / 10 : 0;
        lines.push(`- ${cat.name}${tags}: доход +${_formatTenge(incomeTotal)} (${incPct}%), расход -${_formatTenge(expenseTotal)} (${expPct}%), оборот ${_formatTenge(vol)}`);
      });
    }

    // Days summary (top 3 by volume)
    const daySummary = (data.daySummary || []).slice(0, 3);
    if (daySummary.length) {
      lines.push('Дни (напряжённые по обороту):');
      daySummary.forEach(d => {
        lines.push(`- ${d.dateIso}: доход +${_formatTenge(d.incomeTotal)}, расход -${_formatTenge(d.expenseTotal)}`);
      });
    }

    // Tag summary (rent/payroll/tax/utility/transfer)
    const tagSummary = (data.tagSummary || []).slice(0, 5);
    if (tagSummary.length) {
      lines.push('Теги (по ключевым темам):');
      tagSummary.forEach(t => {
        lines.push(`- ${t.tag}: доход +${_formatTenge(t.incomeFact + t.incomeForecast)}, расход -${_formatTenge(t.expenseFact + t.expenseForecast)}`);
      });
    }

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
  // Finance situation intent
  // =========================
  const _isFinanceSituationQuery = (s) => {
    const t = String(s || '').toLowerCase();
    if (!t) return false;
    // user wants “как дела по деньгам / финансовая картина / ситуация / итоги”
    if (t.includes('финанс') || t.includes('картина') || t.includes('ситуац') || t.includes('итог') || t.includes('как дела')) {
      // if explicitly asked for accounts/balances, let accounts handler answer
      if (/(сч[её]т|счета|касс[аы]|баланс)/i.test(t)) return false;
      return true;
    }
    return false;
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
      const source = req.body?.source || 'freeform';
      const quickKey = req.body?.quickKey || null;
      const isDeep = (req.body?.mode || '').toLowerCase() === 'deep';
      const isQuick = source === 'quick_button' || !!quickKey;
      const isCommand = !isDeep && (isQuick || /(^|\s)(покажи|список|выведи|сколько)\b/i.test(qLower));
      if (process.env.AI_DEBUG === '1') {
        console.log('[AI_DEBUG] query text:', qLower, 'isDeep=', isDeep, 'source=', source);
      }

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
          catalogs: {
            companies: dbData.catalogs?.companies?.length || 0,
            projects: dbData.catalogs?.projects?.length || 0,
            categories: dbData.catalogs?.categories?.length || 0,
            contractors: dbData.catalogs?.contractors?.length || 0,
            individuals: dbData.catalogs?.individuals?.length || 0,
          }
        };
        console.log('[AI_DEBUG] accounts total:', totalAccs, 'hidden:', hiddenAccs.length);
        if (hiddenAccs.length) {
          console.log('[AI_DEBUG] hidden list:', hiddenAccs.map(a => `${a.name} (${a._id})`).join(', '));
        }
        console.log('[AI_DEBUG] catalogs counts:', debugInfo.catalogs);
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
      if (!isDeep && /\b(сч[её]т|счета|касс[аы]|баланс)\b/i.test(qLower)) {
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
      if (!isDeep && (/\b(доход|поступлен|приход)\b/i.test(qLower) && !/\bрасход\b/i.test(qLower))) {
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
      if (!isDeep && (/\b(расход|трат|затрат)\b/i.test(qLower))) {
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
      // FINANCIAL SITUATION (profit-first)
      // =========================
      if (_isFinanceSituationQuery(qLower)) {
        const totals = dbData.totals || {};
        const incFact = Number(dbData.operationsSummary?.income?.fact?.total || 0);
        const expFact = Number(dbData.operationsSummary?.expense?.fact?.total || 0);
        const profitFact = incFact - expFact;

        const totalOpen = Number(totals.open?.current ?? 0);
        const totalHidden = Number(totals.hidden?.current ?? 0);
        const totalAll = Number(totals.all?.current ?? (totalOpen + totalHidden));

        const expSharePct = incFact > 0 ? Math.round((expFact / incFact) * 100) : 0;
        const marginPct = incFact > 0 ? Math.round((profitFact / incFact) * 100) : 0;

        // Quick cash-risk hint (very rough): compare open cash vs 7 days of average expense
        const periodStart = dbData.meta?.periodStart || null;
        const periodEnd = dbData.meta?.periodEnd || dbData.meta?.today || null;
        let days = 30;
        try {
          if (periodStart && periodEnd) {
            const p1 = periodStart.split('.');
            const p2 = periodEnd.split('.');
            if (p1.length === 3 && p2.length === 3) {
              const d1 = new Date(Number('20' + p1[2]), Number(p1[1]) - 1, Number(p1[0]));
              const d2 = new Date(Number('20' + p2[2]), Number(p2[1]) - 1, Number(p2[0]));
              const diff = Math.max(1, Math.round((d2 - d1) / (24 * 60 * 60 * 1000)) + 1);
              days = diff;
            }
          }
        } catch (_) {}

        const avgDailyExp = days > 0 ? (expFact / days) : 0;
        const runwayDaysOpen = avgDailyExp > 0 ? Math.floor(totalOpen / avgDailyExp) : null;

        const lines = [];
        lines.push(`Прибыль (факт): +${_formatTenge(profitFact)}`);
        lines.push(`Маржа: ~${marginPct}% | Расходы: ${_formatTenge(expFact)} (~${expSharePct}% от доходов)`);
        lines.push(`Деньги: открытые ${_formatTenge(totalOpen)}, скрытые ${_formatTenge(totalHidden)}, итого ${_formatTenge(totalAll)}`);

        if (runwayDaysOpen !== null) {
          lines.push(`Открытые покрывают ~${runwayDaysOpen} дн. средних расходов`);
        } else {
          lines.push('Риск кассы по дням: нет данных для расчёта');
        }

        lines.push('');
        lines.push('Вопрос: показать прибыль по проектам или самые крупные расходы?');

        const answer = lines.join('\n').trim();
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // PROJECTS CATALOG
      // =========================
      const projectMention = qLower.includes('проек') || qLower.includes('project');
      const wantsProjectAnalysis = projectMention && (qLower.includes('анализ') || qLower.includes('итог') || qLower.includes('summary') || qLower.includes('успеш') || qLower.includes('лучш') || qLower.includes('прибыл'));
      const wantsProjectSpend = projectMention && (qLower.includes('что потрат') || qLower.includes('на что потрат') || qLower.includes('куда потрат') || (qLower.includes('категор') && qLower.includes('расход')));

      // Специальный сценарий: «самый перспективный/лучший/успешный проект»
      if (projectMention && !isDeep && (qLower.includes('перспектив') || qLower.includes('лучш') || qLower.includes('успеш'))) {
        const ops = Array.isArray(dbData.operations) ? dbData.operations : [];
        const projList = Array.isArray(dbData.catalogs?.projects) ? dbData.catalogs.projects : [];
        const projNameById = new Map();
        projList.forEach(p => {
          if (!p) return;
          if (typeof p === 'string') projNameById.set(p, p);
          else if (p.id) projNameById.set(String(p.id), p.name || p.id);
        });

        const agg = new Map();
        for (const op of ops) {
          if (!op.projectId) continue;
          const id = String(op.projectId);
          if (!agg.has(id)) {
            agg.set(id, { id, name: projNameById.get(id) || `Проект ${id.slice(-4)}`, incFact: 0, incFc: 0, expFact: 0, expFc: 0 });
          }
          const a = agg.get(id);
          if (op.kind === 'income') {
            if (op.isFact) a.incFact += op.amount || 0;
            else a.incFc += op.amount || 0;
          } else if (op.kind === 'expense') {
            if (op.isFact) a.expFact += op.amount || 0;
            else a.expFc += op.amount || 0;
          }
        }

        if (!agg.size) {
          const answer = 'Нет данных по проектам за выбранный период.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const ranked = Array.from(agg.values()).map(p => ({
          ...p,
          profitFact: (p.incFact - p.expFact),
          profitFc: (p.incFc - p.expFc),
          profitTotal: (p.incFact + p.incFc - p.expFact - p.expFc),
        })).sort((a, b) => b.profitTotal - a.profitTotal);

        const top = ranked.slice(0, 3);
        const lines = [`Топ проектов за период ${dbData.meta?.periodStart || ''} — ${dbData.meta?.periodEnd || ''}`];
        top.forEach((p, i) => {
          lines.push(`${i + 1}. ${p.name}: прибыль факт ${_formatTenge(p.profitFact)}, прогноз ${_formatTenge(p.profitFc)}, итог ${_formatTenge(p.profitTotal)}`);
        });

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (projectMention && wantsProjectSpend && !isDeep) {
        const ops = Array.isArray(dbData.operations) ? dbData.operations : [];
        const projList = Array.isArray(dbData.catalogs?.projects) ? dbData.catalogs.projects : [];
        const catList = Array.isArray(dbData.catalogs?.categories) ? dbData.catalogs.categories : [];
        const projNameById = new Map();
        projList.forEach(p => {
          if (!p) return;
          if (typeof p === 'string') projNameById.set(p, p);
          else if (p.id) projNameById.set(String(p.id), p.name || p.id);
        });
        const catNameById = new Map();
        catList.forEach(c => {
          if (!c) return;
          const cid = c.id || c._id;
          if (!cid) return;
          catNameById.set(String(cid), c.name || `Категория ${String(cid).slice(-4)}`);
        });

        const byProject = new Map();
        for (const op of ops) {
          if (op.kind !== 'expense') continue;
          const pid = op.projectId ? String(op.projectId) : null;
          const projName = pid ? (projNameById.get(pid) || `Проект ${pid.slice(-4)}`) : 'Без проекта';
          if (!byProject.has(projName)) byProject.set(projName, new Map());
          const catId = op.categoryId ? String(op.categoryId) : 'Без категории';
          const catMap = byProject.get(projName);
          const prev = catMap.get(catId) || { sum: 0, name: null };
          const catName = catNameById.get(catId) || op.categoryName || op.category || (catId === 'Без категории' ? 'Без категории' : `Категория ${catId.slice(-4)}`);
          prev.sum += op.amount || 0;
          prev.name = catName;
          catMap.set(catId, prev);
        }

        if (!byProject.size) {
          const answer = 'Нет расходов по проектам за выбранный период.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const lines = [`Расходы по проектам за период ${dbData.meta?.periodStart || ''} — ${dbData.meta?.periodEnd || ''}`];
        byProject.forEach((catMap, projName) => {
          lines.push(`${projName}`);
          const sorted = Array.from(catMap.values()).sort((a, b) => Math.abs(b.sum) - Math.abs(a.sum));
          sorted.forEach(c => {
            lines.push(`- ${c.name}: ${_formatTenge(-c.sum)}`);
          });
          lines.push('');
        });
        const answer = lines.join('\n').trim();
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (projectMention && (isCommand || wantsProjectAnalysis) && !isDeep) {
        const projects = dbData.catalogs?.projects || [];
        if (process.env.AI_DEBUG === '1') {
          console.log('[AI_DEBUG] projects branch hit, count=', projects.length, 'sample=', projects.slice(0, 3));
        }
        const wantsAnalysis = wantsProjectAnalysis;

        // Если нужен анализ — считаем по операциям
        if (wantsAnalysis) {
          const ops = Array.isArray(dbData.operations) ? dbData.operations : [];
          const projectMap = new Map();
          projects.forEach(p => {
            const id = typeof p === 'string' ? p : p.id;
            const name = typeof p === 'string' ? p : (p.name || p.id);
            if (id) projectMap.set(String(id), { name, incomeFact: 0, incomeForecast: 0, expenseFact: 0, expenseForecast: 0 });
          });

          for (const op of ops) {
            if (!op.projectId || !projectMap.has(String(op.projectId))) continue;
            const proj = projectMap.get(String(op.projectId));
            if (op.kind === 'income') {
              if (op.isFact) proj.incomeFact += op.amount || 0;
              else proj.incomeForecast += op.amount || 0;
            } else if (op.kind === 'expense') {
              if (op.isFact) proj.expenseFact += op.amount || 0;
              else proj.expenseForecast += op.amount || 0;
            }
          }

          let totalProfitFact = 0;
          projectMap.forEach(p => { totalProfitFact += (p.incomeFact - p.expenseFact); });

          const lines = [`Проекты (анализ) за период ${dbData.meta?.periodStart || ''} — ${dbData.meta?.periodEnd || ''}`];
          if (!projectMap.size) {
            lines.push('- нет данных');
          } else {
            let idx = 1;
            for (const [, p] of projectMap) {
              const profitFact = p.incomeFact - p.expenseFact;
              lines.push(`${idx}. ${p.name}: доход факт ${_formatTenge(p.incomeFact)}, прогноз ${_formatTenge(p.incomeForecast)}; расход факт ${_formatTenge(-p.expenseFact)}, прогноз ${_formatTenge(-p.expenseForecast)}; прибыль факт ${_formatTenge(profitFact)}`);
              idx += 1;
            }
          }

          if (projectMap.size) {
            lines.unshift(`Итого прибыль (факт): ${_formatTenge(totalProfitFact)}`);
          }
          lines.push('');
          lines.push('Показать ТОП по контрагентам?');
          lines.push('Показать на что потратили в проектах?');

          const answer = lines.join('\n');
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        } else {
          const lines = ['Проекты:'];
          if (projects.length) {
            lines.push(...projects.map((p, i) => {
              if (typeof p === 'string') return `${i + 1}. ${p}`;
              return `${i + 1}. ${p.name || p.id || '—'}`;
            }));
          } else {
            lines.push('- нет имен');
          }
          lines.push(`Всего: ${projects.length}`);

          const answer = lines.join('\n');
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }
      }

      // =========================
      // CONTRACTORS CATALOG
      // =========================
      if ((qLower.includes('контраг') || qLower.includes('поставщик') || qLower.includes('партнер') || qLower.includes('партнёр')) && isCommand) {
        const contractors = dbData.catalogs?.contractors || [];
        if (process.env.AI_DEBUG === '1') {
          console.log('[AI_DEBUG] contractors branch hit, count=', contractors.length, 'sample=', contractors.slice(0, 3));
        }
        const lines = ['Контрагенты:'];
        if (contractors.length) {
          lines.push(...contractors.map((c, i) => `${i + 1}. ${c}`));
        } else {
          lines.push('- нет имен');
        }
        lines.push(`Всего: ${contractors.length}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // INDIVIDUALS CATALOG
      // =========================
      if ((qLower.includes('физ') || qLower.includes('индивид') || qLower.includes('person')) && isCommand) {
        const individuals = dbData.catalogs?.individuals || [];
        if (process.env.AI_DEBUG === '1') {
          console.log('[AI_DEBUG] individuals branch hit, count=', individuals.length, 'sample=', individuals.slice(0, 3));
        }
        const lines = ['Физические лица:'];
        if (individuals.length) {
          lines.push(...individuals.map((ind, i) => `${i + 1}. ${ind}`));
        } else {
          lines.push('- нет имен');
        }
        lines.push(`Всего: ${individuals.length}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // CATEGORIES CATALOG
      // =========================
      if ((qLower.includes('категор') || qLower.includes('category')) && isCommand) {
        const categories = dbData.catalogs?.categories || [];
        if (process.env.AI_DEBUG === '1') {
          console.log('[AI_DEBUG] categories branch hit, count=', categories.length, 'sample=', categories.slice(0, 3));
        }
        const lines = ['Категории:'];
        if (categories.length) {
          lines.push(...categories.map((cat, i) => `${i + 1}. ${cat}`));
        } else {
          lines.push('- нет имен');
        }
        lines.push(`Всего: ${categories.length}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // COMPANIES CATALOG
      // =========================
      if ((qLower.includes('компан') || qLower.includes('фирм') || qLower.includes('организаци') || qLower.includes('company')) && isCommand) {
        const companies = dbData.catalogs?.companies || [];
        if (process.env.AI_DEBUG === '1') {
          console.log('[AI_DEBUG] companies branch hit, count=', companies.length, 'sample=', companies.slice(0, 3));
        }
        const lines = ['Мои компании', ''];
        if (companies.length) {
          companies.forEach((comp, i) => lines.push(`${i + 1}. ${comp}`));
        } else {
          lines.push('- нет имен');
        }
        lines.push('', `Всего: ${companies.length}`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // AI GENERATION (OpenAI)
      // Universal fallback for all queries
      // =========================
      const wantsLosses = qLower.includes('теря') || qLower.includes('потер');
      const lossDimension = (() => {
        if (qLower.includes('контраг')) return 'contractor';
        if (qLower.includes('проект')) return 'project';
        if (qLower.includes('счет') || qLower.includes('касс')) return 'account';
        return 'category';
      })();

      if (wantsLosses) {
        const ops = Array.isArray(dbData.operations) ? dbData.operations : [];
        const catalogs = dbData.catalogs || {};

        const nameByDim = {
          category: (id) => {
            const cats = catalogs.categories || [];
            const found = cats.find(c => (c.id || c._id) === id || c === id);
            if (typeof found === 'string') return found;
            return found?.name || 'Без категории';
          },
          contractor: (id) => {
            const list = catalogs.contractors || [];
            const found = list.find(c => (c.id || c._id) === id || c === id);
            if (typeof found === 'string') return found;
            return found?.name || 'Без контрагента';
          },
          project: (id) => {
            const list = catalogs.projects || [];
            const found = list.find(p => (p.id || p._id) === id || p === id);
            if (typeof found === 'string') return found;
            return found?.name || 'Без проекта';
          },
          account: (id) => {
            const list = dbData.accounts || [];
            const found = list.find(a => (a.id || a._id) === id || a === id);
            if (typeof found === 'string') return found;
            return found?.name || 'Без счета';
          }
        };

        const agg = new Map();
        let totalExp = 0;
        for (const op of ops) {
          const amt = _absExpense(op);
          if (amt <= 0) continue;
          totalExp += amt;

          let key = null;
          if (lossDimension === 'contractor') key = op.contractorId || op.contractor || null;
          else if (lossDimension === 'project') key = op.projectId || op.project || null;
          else if (lossDimension === 'account') key = op.accountId || op.account || null;
          else key = op.categoryId || op.category || null;

          if (!key) key = 'none';
          const id = typeof key === 'object' && key._id ? key._id : String(key);
          if (!agg.has(id)) agg.set(id, { id, sum: 0 });
          agg.get(id).sum += amt;
        }

        const items = Array.from(agg.values())
          .filter(it => lossDimension !== 'contractor' ? it.sum > 0 : it.sum > 0) // contractor also exclude 0
          .sort((a, b) => b.sum - a.sum);

        const top = items.slice(0, 3);
        const topSum = top.reduce((s, it) => s + it.sum, 0);

        if (!top.length) {
          const answer = 'Нет расходных операций для расчёта потерь.';
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        const dimLabel = {
          category: 'категориям',
          contractor: 'контрагентам',
          project: 'проектам',
          account: 'счетам'
        }[lossDimension] || 'категориям';

        const lines = [`ТОП-3 по ${dimLabel}:`];
        top.forEach((it, idx) => {
          const name = nameByDim[lossDimension]?.(it.id) || 'Без категории';
          lines.push(`${idx + 1}. ${name} — ${_formatTenge(it.sum)}`);
        });
        lines.push(`Сумма ТОП-3: ${_formatTenge(topSum)}`);
        lines.push(`Итог расходов: ${_formatTenge(totalExp)}`);

        const followUp = (() => {
          if (lossDimension === 'category') return 'Показать ТОП по контрагентам?';
          if (lossDimension === 'contractor') return 'Показать ТОП по проектам?';
          return 'Показать ТОП по категориям?';
        })();
        lines.push(followUp);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      const systemPrompt = (() => {
        const base = [
          'Ты — INDEX12 CFO HARDCORE. Ты говоришь от лица денег пользователя и как финансовый директор.',
          'Цель: живой диалог короткими ходами. Без портянок. Без воды. Только смысл и цифры.',
          '',
          'Формат денег: "1 234 567 ₸". Доход: "+", расход: "-".',
          'Отвечай строго по данным из контекста. Ничего не придумывай.',
          '',
          'Понимай смысл запроса, а не слова. Любая формулировка про деньги сводится к одному из намерений:',
          '1) Где деньги? (счета/ликвидность)',
          '2) Куда ушли? (расходы/структура)',
          '3) Откуда пришли? (доходы/источники)',
          '4) Что осталось? (прибыль/CF)',
          '5) Что может порваться? (кассовый пик/разрыв)',
          '6) Сколько можно инвестировать? (коридор/без боли)',
          '',
          'Всегда выбирай один лучший срез данных: счета ИЛИ категории ИЛИ контрагенты ИЛИ проекты ИЛИ дни. Один ответ — один срез.',
          '',
          'Длина ответа ВСЕГДА короткая: максимум 4–6 строк. Это один ход диалога.',
          'Структура ответа обязательна:',
          '1) Главная цифра (1 строка)',
          '2) Короткая раскладка (2–3 пункта)',
          '3) Жёсткий вывод (1 строка)',
          '4) Следующий шаг (1 вопрос отдельной строкой)',
          '',
          'Правило сигнала: любой вывод должен иметь цифру-основание. Если сигнала нет — скажи: "Сигнала нет — значит сейчас это не проблема".',
          '',
          'Ты помнишь контекст текущего чата и не повторяешь одно и то же. Двигай пользователя вперёд.',
        ];

        const chatRules = [
          'Режим CHAT: отвечай по запросу и веди одним следующим вопросом. Без лишних вариантов.',
        ];

        const deepRules = [
          'Режим DIP (deep): ответ по длине такой же короткий, но умнее: находи сигналы/аномалии, выбирай лучший срез и задавай более точный следующий вопрос.',
          'В DIP можно задать 1 уточняющий вопрос, если без него невозможно дать правильную цифру.',
        ];

        const scenarios = [
          'Сценарий "инвестиции ежемесячно": всегда давай коридор в 3 режимах:',
          '- Агрессивно: … ₸/мес',
          '- Нормально: … ₸/мес',
          '- Спокойно: … ₸/мес',
          'И 1 строка условия: что учтено (резерв/обязательства).',
          'Если ключевого параметра нет — спроси 1 вопрос и остановись (например: "Сколько уходит на жили-были в месяц?").',
          '',
          'Сценарий "куда ушли деньги/на что тратили": покажи структуру расходов по одному срезу (обычно категории).',
          'Сценарий "что на счетах": покажи общий итог и 2–3 самых крупных счета.',
          'Сценарий "по проектам": показывай доход/расход/прибыль по проектам, а операции без projectId учитывай как "Без проекта".',
        ];

        // Собираем промпт
        return [
          ...base,
          '',
          ...(isDeep ? deepRules : chatRules),
          '',
          ...scenarios,
        ].join('\n');
      })();

      const hiddenAccs = (dbData.accounts || []).filter(a => a.isHidden);
      const openAccs = (dbData.accounts || []).filter(a => !a.isHidden);
      const dataContext = _formatDbDataForAi(dbData);
      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'system', content: dataContext },
        ..._getHistoryMessages(userIdStr)
      ];

      // DIP can use a heavier model via env var
      // Preferred key: OPENAI_MODEL_DEEP
      // Backward/typo-friendly fallbacks (in case env was added with a different name)
      const deepModelEnv =
        process.env.OPENAI_MODEL_DEEP ||
        process.env.OPENAI_MODEL_DIP ||
        process.env.OPENAI_MODEL_DEEP_MODE ||
        process.env['OpenAI_модель_дип'] ||
        process.env['OPENAI_модель_дип'] ||
        null;

      const modelOverride = isDeep ? deepModelEnv : null;

      if (process.env.AI_DEBUG === '1') {
        console.log('[AI_DEBUG] modelOverride:', modelOverride || '(none)', 'baseModel:', process.env.OPENAI_MODEL || '(default)');
      }

      const aiResponse = await _openAiChat(messages, {
        modelOverride,
        temperature: 0,
        maxTokens: isDeep ? 900 : 650,
      });
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
        debugInfo.catalogs = debugInfo.catalogs || {
          companies: dbData.catalogs?.companies?.length || 0,
          projects: dbData.catalogs?.projects?.length || 0,
          categories: dbData.catalogs?.categories?.length || 0,
          contractors: dbData.catalogs?.contractors?.length || 0,
          individuals: dbData.catalogs?.individuals?.length || 0,
          projectsSample: (dbData.catalogs?.projects || []).slice(0, 3),
          categoriesSample: (dbData.catalogs?.categories || []).slice(0, 3),
          contractorsSample: (dbData.catalogs?.contractors || []).slice(0, 3),
          individualsSample: (dbData.catalogs?.individuals || []).slice(0, 3),
          companiesSample: (dbData.catalogs?.companies || []).slice(0, 3),
          contractorSummarySample: (dbData.contractorSummary || []).slice(0, 3),
          daySummarySample: (dbData.daySummary || []).slice(0, 3),
          categorySummarySample: (dbData.categorySummary || []).slice(0, 3),
          tagSummarySample: (dbData.tagSummary || []).slice(0, 3),
          outliersSample: dbData.outliers || {},
        };
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
