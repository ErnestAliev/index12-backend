// backend/ai/aiRoutes.js
// AI assistant routes - PURE DATABASE MODE
// All data comes from MongoDB via dataProvider (no uiSnapshot)
//
// ✅ Features:
// - QUICK mode: deterministic lists (accounts / income / expense / catalogs)
// - DIAG command: diagnostics of DB packet
// - DEEP (DIP) mode: CFO dialog (profit/margin/risks/next-step), no UI repetition
// - Separate model for DIP via env: OPENAI_MODEL_DEEP
// - Deterministic investment math (no "выдуманных" цифр)

const express = require('express');

const AIROUTES_VERSION = 'db-only-v5.1';
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
  const _openAiChat = async (messages, { temperature = 0, maxTokens = 550, modelOverride = null } = {}) => {
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

  // =========================
  // Helpers for expenses/income
  // =========================
  const _absExpense = (op) => {
    if (!op || op.isTransfer) return 0;
    const raw = Number(op.rawAmount ?? op.amount ?? 0);
    if (op.kind === 'income') return 0;
    if (op.kind === 'expense' || raw < 0) return Math.abs(raw || 0);
    return 0;
  };

  // =========================
  // Deterministic CFO metrics (code, not LLM)
  // =========================
  const _parseDdMmYy = (s) => {
    try {
      const t = String(s || '').trim();
      const m = t.match(/^(\d{2})\.(\d{2})\.(\d{2})$/);
      if (!m) return null;
      const dd = Number(m[1]);
      const mm = Number(m[2]);
      const yy = Number(m[3]);
      const yyyy = 2000 + yy;
      return new Date(Date.UTC(yyyy, mm - 1, dd, 0, 0, 0));
    } catch (_) {
      return null;
    }
  };

  const _daysBetween = (a, b) => {
    try {
      const A = a instanceof Date ? a : _parseDdMmYy(a);
      const B = b instanceof Date ? b : _parseDdMmYy(b);
      if (!A || !B) return 30;
      const diff = Math.max(1, Math.round((B.getTime() - A.getTime()) / (24 * 60 * 60 * 1000)) + 1);
      return diff;
    } catch (_) {
      return 30;
    }
  };

  const _parseMoneyKzt = (text) => {
    const s = String(text || '').toLowerCase().replace(/₸/g, '');
    // "10 млн", "10m", "10 м"
    const m1 = s.match(/([0-9]+(?:[\.,][0-9]+)?)\s*(млн|миллион|миллиона|миллионов)\b/i);
    if (m1) {
      const v = Number(String(m1[1]).replace(',', '.'));
      if (Number.isFinite(v)) return Math.round(v * 1_000_000);
    }
    const m2 = s.match(/([0-9]+(?:[\.,][0-9]+)?)\s*(м|m)\b/i);
    if (m2) {
      const v = Number(String(m2[1]).replace(',', '.'));
      if (Number.isFinite(v)) return Math.round(v * 1_000_000);
    }
    // "10 000 000"
    const m3 = s.match(/([0-9][0-9\s]{2,})/);
    if (m3) {
      const v = Number(String(m3[1]).replace(/\s+/g, ''));
      if (Number.isFinite(v)) return Math.round(v);
    }
    // "500000"
    const m4 = s.match(/\b([0-9]+(?:[\.,][0-9]+)?)\b/);
    if (m4) {
      const v = Number(String(m4[1]).replace(',', '.'));
      if (Number.isFinite(v)) return Math.round(v);
    }
    return null;
  };

  const _calcCoreMetrics = (dbData) => {
    const summary = dbData?.operationsSummary || {};
    const inc = summary.income || {};
    const exp = summary.expense || {};

    const incFact = Number(inc.fact?.total || 0);
    const expFactRaw = Number(exp.fact?.total || 0);
    const expFact = Math.abs(expFactRaw);

    const profitFact = incFact - expFact;
    const marginPct = incFact > 0 ? Math.round((profitFact / incFact) * 1000) / 10 : 0;

    const totals = dbData?.totals || {};
    const openCash = Number(totals.open?.current ?? 0);
    const hiddenCash = Number(totals.hidden?.current ?? 0);
    const totalCash = Number(totals.all?.current ?? (openCash + hiddenCash));

    const periodStart = dbData?.meta?.periodStart || dbData?.meta?.today || null;
    const periodEnd = dbData?.meta?.periodEnd || dbData?.meta?.today || null;
    const daysPeriod = _daysBetween(periodStart, periodEnd);

    const avgDailyExp = daysPeriod > 0 ? (expFact / daysPeriod) : expFact;
    const runwayDaysOpen = avgDailyExp > 0 ? Math.floor(openCash / avgDailyExp) : null;

    const cats = Array.isArray(dbData?.categorySummary) ? dbData.categorySummary : [];
    const topExpCat = cats
      .map(c => ({ name: c.name || 'Без категории', expFact: Number(c.expenseFact || 0) }))
      .filter(x => x.expFact > 0)
      .sort((a, b) => b.expFact - a.expFact)[0] || null;

    const topExpCatSharePct = (topExpCat && expFact > 0)
      ? Math.round((topExpCat.expFact / expFact) * 1000) / 10
      : 0;

    return {
      incFact,
      expFact,
      profitFact,
      marginPct,
      openCash,
      hiddenCash,
      totalCash,
      daysPeriod,
      avgDailyExp,
      runwayDaysOpen,
      topExpCat,
      topExpCatSharePct,
      periodStart,
      periodEnd,
    };
  };

  // =========================
  // DB data context for LLM (kept but DIP should NOT repeat it)
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

      // QUICK buttons must always stay deterministic and must NOT be treated as DEEP
      const isQuick = source === 'quick_button' || !!quickKey;
      const isDeep = ((req.body?.mode || '').toLowerCase() === 'deep') && !isQuick;

      const isCommand = !isDeep && (isQuick || /(^|\s)(покажи|список|выведи|сколько)\b/i.test(qLower));

      if (process.env.AI_DEBUG === '1') {
        console.log('[AI_DEBUG] query:', qLower, 'deep=', isDeep, 'source=', source);
      }

      // =========================
      // 🔥 PURE DATABASE MODE
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

      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: true,
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
      }

      // History
      _pushHistory(userIdStr, 'user', q);

      // =========================
      // DIAGNOSTICS COMMAND
      // =========================
      const _isDiagnosticsQuery = (s) => {
        const t = String(s || '').toLowerCase();
        if (!t) return false;
        if (t.includes('диагност') || t.includes('diagnostic')) return true;
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
      // QUICK MODE: deterministic endpoints
      // =========================
      if (!isDeep && /\b(сч[её]т|счета|касс[аы]|баланс)\b/i.test(qLower)) {
        const lines = [];
        const accounts = dbData.accounts || [];
        const totals = dbData.totals || {};

        const periodStart = dbData.meta?.periodStart || '';
        const periodEnd = dbData.meta?.periodEnd || dbData.meta?.today || _fmtDateKZ(_endOfToday());
        const periodLabel = periodStart ? `с ${periodStart} по ${periodEnd}` : `на ${periodEnd}`;

        lines.push(`Счета (${periodLabel})`);
        lines.push('');

        if (!accounts.length) {
          lines.push('Счета не найдены.');
        } else {
          const openAccs = accounts.filter(a => !a.isHidden);
          const hiddenAccs = accounts.filter(a => a.isHidden);

          lines.push('Открытые:');
          if (openAccs.length) {
            for (const acc of openAccs) lines.push(`${acc.name || 'Счет'}: ${_formatTenge(acc.currentBalance || 0)}`);
          } else lines.push('- нет');

          lines.push('');
          lines.push('Скрытые:');
          if (hiddenAccs.length) {
            for (const acc of hiddenAccs) lines.push(`${acc.name || 'Счет'} (скрыт): ${_formatTenge(acc.currentBalance || 0)}`);
          } else lines.push('- нет');

          lines.push('');
          const totalOpen = totals.open?.current ?? 0;
          const totalHidden = totals.hidden?.current ?? 0;
          const totalAll = totals.all?.current ?? (totalOpen + totalHidden);
          lines.push(`Итого открытые: ${_formatTenge(totalOpen)}`);
          lines.push(`Итого скрытые: ${_formatTenge(totalHidden)}`);
          lines.push(`Итого все: ${_formatTenge(totalAll)}`);
        }

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (!isDeep && (/\b(доход|поступлен|приход)\b/i.test(qLower) && !/\bрасход\b/i.test(qLower))) {
        const summary = dbData.operationsSummary || {};
        const incomeData = summary.income || {};

        const periodStart = dbData.meta?.periodStart || dbData.meta?.today || '';
        const periodEnd = dbData.meta?.periodEnd || dbData.meta?.today || '';
        const periodLabel = periodStart && periodEnd ? `${periodStart} — ${periodEnd}` : (periodStart || periodEnd || 'не указан');

        const lines = [
          `Доходы (${periodLabel})`,
          `Факт: ${_formatTenge(incomeData.fact?.total || 0)} (${incomeData.fact?.count || 0})`,
          `Прогноз: ${_formatTenge(incomeData.forecast?.total || 0)} (${incomeData.forecast?.count || 0})`,
        ];

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (!isDeep && (/\b(расход|трат|затрат)\b/i.test(qLower))) {
        const summary = dbData.operationsSummary || {};
        const expenseData = summary.expense || {};

        const todayStr = dbData.meta?.today || _fmtDateKZ(_endOfToday());
        const periodStart = dbData.meta?.periodStart || todayStr;
        const periodEndMonth = dbData.meta?.periodEnd || todayStr;

        const wantsContractor = /\b(контраг|кому|на кого|у кого|поставщ|partner|партнер|партнёр)\b/i.test(qLower);
        const cleanName = (name) => String(name || '').replace(/\s*\[[^\]]+\]\s*$/,'').trim() || 'Без названия';

        const factTotal = Math.abs(expenseData.fact?.total || 0);
        const factCount = expenseData.fact?.count || 0;
        const forecastTotal = Math.abs(expenseData.forecast?.total || 0);
        const forecastCount = expenseData.forecast?.count || 0;

        const lines = [];
        lines.push(`Фактические расходы с ${periodStart} по ${todayStr} составили:`);
        lines.push(`- ${_formatTenge(factTotal)} (${factCount} операций).`);
        lines.push('');
        lines.push('Из них:');

        if (wantsContractor) {
          const contrFact = (dbData.contractorSummary || [])
            .map(c => ({ name: cleanName(c.name || 'Без контрагента'), amount: Number(c.expenseFact || 0) }))
            .filter(c => c.amount > 0)
            .sort((a, b) => b.amount - a.amount);

          if (!contrFact.length) lines.push('- нет расходов по контрагентам');
          else {
            contrFact.slice(0, 5).forEach(c => lines.push(`- ${c.name} - ${_formatTenge(Math.abs(c.amount))}`));
            if (contrFact.length > 5) lines.push(`... и ещё ${contrFact.length - 5}`);
          }
        } else {
          const catsFact = (dbData.categorySummary || [])
            .map(c => ({ name: cleanName(c.name || 'Без категории'), amount: Number(c.expenseFact || 0) }))
            .filter(c => c.amount > 0)
            .sort((a, b) => b.amount - a.amount);

          if (!catsFact.length) lines.push('- нет расходов по категориям');
          else {
            catsFact.slice(0, 5).forEach(c => lines.push(`- ${c.name} - ${_formatTenge(Math.abs(c.amount))}`));
            if (catsFact.length > 5) lines.push(`... и ещё ${catsFact.length - 5}`);
          }
        }

        lines.push('');
        lines.push(`С ${todayStr} до конца месяца запланированы расходы на сумму:`);
        lines.push(`- ${_formatTenge(forecastTotal)} (${forecastCount} операций).`);

        if (forecastTotal > 0) {
          lines.push('');
          lines.push('Из них:');

          if (wantsContractor) {
            const contrForecast = (dbData.contractorSummary || [])
              .map(c => ({ name: cleanName(c.name || 'Без контрагента'), amount: Number(c.expenseForecast || 0) }))
              .filter(c => c.amount > 0)
              .sort((a, b) => b.amount - a.amount);

            if (!contrForecast.length) lines.push('- нет запланированных расходов по контрагентам');
            else {
              contrForecast.slice(0, 5).forEach(c => lines.push(`- ${c.name} - ${_formatTenge(Math.abs(c.amount))}`));
              if (contrForecast.length > 5) lines.push(`... и ещё ${contrForecast.length - 5}`);
            }
          } else {
            const catsForecast = (dbData.categorySummary || [])
              .map(c => ({ name: cleanName(c.name || 'Без категории'), amount: Number(c.expenseForecast || 0) }))
              .filter(c => c.amount > 0)
              .sort((a, b) => b.amount - a.amount);

            if (!catsForecast.length) lines.push('- нет запланированных расходов по категориям');
            else {
              catsForecast.slice(0, 5).forEach(c => lines.push(`- ${c.name} - ${_formatTenge(Math.abs(c.amount))}`));
              if (catsForecast.length > 5) lines.push(`... и ещё ${catsForecast.length - 5}`);
            }
          }
        } else {
          lines.push('Прогнозируемых расходов нет.');
        }

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (!isDeep && /\b(перевод(ы|ов)?|трансфер)\b/i.test(qLower)) {
        const transfers = (dbData.operations || []).filter(op => op.kind === 'transfer' && op.isFact);
        const lines = ['ПЕРЕВОДЫ'];

        if (!transfers.length) {
          lines.push('- нет фактических переводов за период');
        } else {
          const pickName = (...candidates) => {
            const hit = candidates.find(v => v && String(v).trim());
            return hit ? String(hit).trim() : null;
          };
          const fmtAmount = (n) => _formatTenge(Math.abs(Number(n || 0))).replace(' ₸', ' т');

          transfers.slice(0, 5).forEach(tr => {
            const amountStr = fmtAmount(tr.amount || tr.rawAmount || 0);
            const fromName = pickName(
              tr.fromCompanyName,
              tr.fromAccountName,
              tr.companyName,
              tr.accountName,
              tr.contractorName,
              tr.fromIndividualName,
              tr.individualName,
              tr.description
            ) || '?';
            const toName = pickName(
              tr.toCompanyName,
              tr.toAccountName,
              tr.companyName,
              tr.toIndividualName,
              tr.contractorName,
              tr.description
            ) || '?';
            lines.push(`${amountStr}: ${fromName}→ ${toName}`);
          });

          if (transfers.length > 5) lines.push(`... и ещё ${transfers.length - 5}`);
        }

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // CATALOGS (quick)
      // =========================
      const _simpleList = (title, arr) => {
        const lines = [title];
        if (Array.isArray(arr) && arr.length) {
          lines.push(...arr.map((x, i) => {
            const name = (x && typeof x === 'object' && x.name) ? x.name : x;
            return `${i + 1}. ${name || '-'}`;
          }));
        } else {
          lines.push('- нет');
        }
        lines.push(`Всего: ${Array.isArray(arr) ? arr.length : 0}`);
        return lines.join('\n');
      };

      if (!isDeep && isCommand && (qLower.includes('контраг') || qLower.includes('поставщик') || qLower.includes('партнер') || qLower.includes('партнёр'))) {
        const answer = _simpleList('Контрагенты:', dbData.catalogs?.contractors || []);
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (!isDeep && isCommand && (qLower.includes('физ') || qLower.includes('индивид') || qLower.includes('person'))) {
        const answer = _simpleList('Физлица:', dbData.catalogs?.individuals || []);
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (!isDeep && isCommand && (qLower.includes('категор') || qLower.includes('category'))) {
        const answer = _simpleList('Категории:', dbData.catalogs?.categories || []);
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      if (!isDeep && isCommand && (qLower.includes('компан') || qLower.includes('организаци') || qLower.includes('company') || qLower.includes('фирм'))) {
        const answer = _simpleList('Компании:', dbData.catalogs?.companies || []);
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // NON-DEEP "Что по деньгам" (deterministic, no LLM)
      // =========================
      if (!isDeep && /ситуац|картина|финанс|что\s+у\s+нас\s+там\s+по\s+деньгам|что\s+по\s+деньгам|по\s+деньгам|прибыл|марж/i.test(qLower)) {
        const m = _calcCoreMetrics(dbData);
        const lines = [];
        lines.push(`Прибыль: +${_formatTenge(m.profitFact)} | Маржа: ${m.marginPct}%`);
        lines.push(`Доход: +${_formatTenge(m.incFact)} | Расход: -${_formatTenge(m.expFact)}`);
        lines.push(`Открытые: ${_formatTenge(m.openCash)} | Скрытые: ${_formatTenge(m.hiddenCash)} | Всего: ${_formatTenge(m.totalCash)}`);
        if (m.runwayDaysOpen !== null) lines.push(`Открытая ликвидность: ~${m.runwayDaysOpen} дней`);

        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // DEEP (DIP) CFO MODE (deterministic)
      // =========================
      if (isDeep) {
        const s = _getChatSession(userIdStr);
        const m = _calcCoreMetrics(dbData);

        const wantsInvest = /инвест|влож|инвестици/i.test(qLower);
        const wantsFinance = /ситуац|картина|финанс|прибыл|марж|как дела|что по деньг/i.test(qLower);
        const wantsTellUnknown = /что-нибудь.*не знаю|удиви|чего я не знаю/i.test(qLower);
        const wantsLosses = /теря|потер|куда ушл|на что трат/i.test(qLower);

        let justSetLiving = false;

        // If awaiting living monthly input
        const maybeMoney = _parseMoneyKzt(q);
        if (s && s.pending && s.pending.type === 'ask_living' && maybeMoney) {
          s.prefs.livingMonthly = maybeMoney;
          s.pending = null;
          justSetLiving = true;
        }

        if (wantsFinance) {
          const lines = [];
          lines.push(`Прибыль (факт): +${_formatTenge(m.profitFact)} | Маржа: ${m.marginPct}%`);
          lines.push(`Доход: +${_formatTenge(m.incFact)} | Расход: -${_formatTenge(m.expFact)}`);

          if (m.runwayDaysOpen !== null) {
            lines.push(`Открытая ликвидность: ~${m.runwayDaysOpen} дней`);
          }

          if (m.topExpCat) {
            lines.push(`Самый тяжелый расход: ${m.topExpCat.name} (~${m.topExpCatSharePct}%)`);
          }

          // quick risk flags
          if (m.profitFact < 0) lines.push(`Риск: период убыточный → инвестиции только из резерва.`);
          else if (m.runwayDaysOpen !== null && m.runwayDaysOpen < 7) lines.push(`Риск: на открытых мало денег → возможен кассовый разрыв.`);

          lines.push('');
          lines.push('Дальше: прибыль по проектам или кассовые риски по дням?');
          const answer = lines.join('\n');
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        if (wantsLosses) {
          // Not TOP. It's classification: structural vs controllable.
          const cats = Array.isArray(dbData.categorySummary) ? dbData.categorySummary : [];
          const expCats = cats.map(c => ({ name: c.name || 'Без категории', expFact: Number(c.expenseFact || 0) })).filter(x => x.expFact > 0);

          const classify = (name) => {
            const n = String(name || '').toLowerCase();
            if (/(коммун|ккх|жкх|свет|вода|отоп|газ|электро)/i.test(n)) return 'structural';
            if (/(налог|кпн|ндс|осмс|енпф|соц|пенс|штраф)/i.test(n)) return 'structural';
            if (/(фот|зарплат|оклад|аванс)/i.test(n)) return 'structural';
            if (/(процент|дивиденд|владельц|эрнест\s*5|комисси)/i.test(n)) return 'structural';
            if (/(ремонт|хоз|канцел|маркет|реклам|достав|транспорт|услуг|подряд|материал|закуп|проч)/i.test(n)) return 'controllable';
            return 'check';
          };

          let structural = 0, controllable = 0, check = 0;
          for (const c of expCats) {
            const cls = classify(c.name);
            if (cls === 'structural') structural += c.expFact;
            else if (cls === 'controllable') controllable += c.expFact;
            else check += c.expFact;
          }

          const pct = (v) => (m.expFact > 0 ? Math.round((v / m.expFact) * 1000) / 10 : 0);

          const lines = [];
          lines.push(`Расходы: -${_formatTenge(m.expFact)} | Прибыль: +${_formatTenge(m.profitFact)} | Маржа: ${m.marginPct}%`);
          lines.push(`Структурно: ${pct(structural)}% | Управляемо: ${pct(controllable)}% | Проверить: ${pct(check)}%`);
          lines.push(pct(controllable) >= 25
            ? 'Вывод: утечки чаще сидят в управляемых расходах (ремонты/услуги/прочее).'
            : 'Вывод: расходы в основном структурные → работаем доходом/арендой/долгами.'
          );
          lines.push('');
          lines.push('Дальше: разложить управляемые по контрагентам или по проектам?');

          const answer = lines.join('\n');
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        if (wantsTellUnknown) {
          const lines = [];
          // "unknown": open liquidity risk + profit margin + hidden share
          const hiddenShare = m.totalCash > 0 ? Math.round((m.hiddenCash / m.totalCash) * 1000) / 10 : 0;
          lines.push(`Факт-прибыль: +${_formatTenge(m.profitFact)} (маржа ${m.marginPct}%)`);
          lines.push(`Скрытые деньги: ${_formatTenge(m.hiddenCash)} (${hiddenShare}%)`);
          if (m.runwayDaysOpen !== null) {
            lines.push(`Открытые держат ~${m.runwayDaysOpen} дней расходов — это твой реальный риск кассы.`);
          } else {
            lines.push('По расходам нет достаточно данных, чтобы оценить кассовый риск.');
          }
          lines.push('');
          lines.push('Дальше: усиливаем прибыль или закрываем кассовые риски?');

          const answer = lines.join('\n');
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        if (wantsInvest || justSetLiving) {
          const living = s?.prefs?.livingMonthly;
          if (!living) {
            if (s) s.pending = { type: 'ask_living', ts: Date.now() };
            const answer = 'Сколько уходит на жили-были в месяц? (пример: 3 млн)';
            _pushHistory(userIdStr, 'assistant', answer);
            return res.json({ text: answer });
          }

          // investment math:
          // if profit covers living -> invest = 50% of free cashflow
          // else invest from hidden reserves = 0.6%/month (≈ 7.2% годовых из резерва)
          const freeMonthly = Math.max(0, m.profitFact - living);

          const lines = [];
          lines.push(`Прибыль: +${_formatTenge(m.profitFact)} /мес`);
          lines.push(`Жили-были: -${_formatTenge(living)} /мес`);

          if (freeMonthly > 0) {
            const invest = Math.round(freeMonthly * 0.5);
            lines.push(`Свободно: +${_formatTenge(freeMonthly)} → инвест ${_formatTenge(invest)} /мес (0.5×)`);
            lines.push('');
            lines.push('Дальше: из потока (безопасно) или из резерва (агрессивно)?');
          } else {
            const invest = Math.round(m.hiddenCash * 0.006);
            lines.push('Поток не покрывает жили-были → инвест только из резерва (скрытые).');
            lines.push(`Ритм: ${_formatTenge(invest)} /мес (~0.6% скрытых)`);
            lines.push('');
            lines.push('Дальше: цель доходности и срок инвестиций?');
          }

          const answer = lines.join('\n');
          _pushHistory(userIdStr, 'assistant', answer);
          return res.json({ text: answer });
        }

        // DIP default if message unknown: profit snapshot + next question
        const lines = [
          `Прибыль: +${_formatTenge(m.profitFact)} | Маржа: ${m.marginPct}%`,
          `Открытые: ${_formatTenge(m.openCash)} | Скрытые: ${_formatTenge(m.hiddenCash)}`,
          '',
          'Что делаем: прибыль по проектам, расходы-утечки или инвестиции?'
        ];
        const answer = lines.join('\n');
        _pushHistory(userIdStr, 'assistant', answer);
        return res.json({ text: answer });
      }

      // =========================
      // AI GENERATION (OpenAI) - fallback
      // =========================
      const systemPrompt = (() => {
        if (isDeep) {
          return [
            'Ты CFO-агент INDEX12. Диалог, коротко.',
            'НЕ повторяй интерфейс (списки счетов/топы) без прямого запроса пользователя.',
            'Всегда начинай с: прибыль/маржа/риски/следующий шаг.',
            'Один следующий вопрос в конце.'
          ].join('\n');
        }
        return [
          'Ты финансовый аналитик INDEX12.',
          'Отвечай строго по данным. 3–4 строки. Без воды.',
          'Не придумывай имена. Если нет — пиши "Без контрагента".'
        ].join('\n');
      })();

      const dataContext = _formatDbDataForAi(dbData);
      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'system', content: dataContext },
        ..._getHistoryMessages(userIdStr),
      ];

      const modelOverride = isDeep ? (process.env.OPENAI_MODEL_DEEP || process.env.OPENAI_MODEL || null) : null;
      const aiResponse = await _openAiChat(messages, { modelOverride });

      _pushHistory(userIdStr, 'assistant', aiResponse);

      if (debugRequested) {
        debugInfo = debugInfo || {};
        debugInfo.opsSummary = dbData.operationsSummary || {};
        debugInfo.sampleOps = (dbData.operations || []).slice(0, 5);
        debugInfo.modelUsed = modelOverride || (process.env.OPENAI_MODEL || 'gpt-4o');
        debugInfo.modelDeep = process.env.OPENAI_MODEL_DEEP || null;
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
