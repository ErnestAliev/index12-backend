// backend/ai/aiRoutes.js
// AI assistant routes extracted from server.js
// Requirements:
// - Unified rule: FACT is always calculated "as of today (KZ)" unless user explicitly asks period/future.
// - Always show the date: "До DD.MM.YY".
// - Money format: thousands + "₸".
// - No default "last 30 days" anywhere.
// - Accounts list must include hidden accounts by default.
// - Source of truth is FRONTEND UI snapshot (uiSnapshot). The AI route must not query Mongo for answering.
// - If uiSnapshot is missing, fallback is aiContext (frontend-prepared), but still NO Mongo queries.
// - Catalog queries (projects/contractors/categories/individuals/prepayments) return numbered lists without sums.

const express = require('express');

// Visible build marker to confirm which aiRoutes.js is running
const AIROUTES_VERSION = 'snapshot-ui-v3.6-diag';

const https = require('https');

// =========================
// Chat session state (in-memory, TTL)
// Keeps short context for live chat (NOT persisted)
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
    prefs: {
      incomeScope: null, // 'current' | 'future' | 'all'
      expenseScope: null,
      format: 'short',   // 'short' | 'detailed'
      limit: 50,
    },
    pending: null,
    history: [],
    lastList: null, // Store last displayed list for numbered references
  };
  _chatSessions.set(key, fresh);
  return fresh;
};

const _setPending = (userId, pending) => {
  const s = _getChatSession(userId);
  if (!s) return;
  s.pending = pending || null;
  s.expiresAt = Date.now() + SESSION_TTL_MS;
};

const _clearPending = (userId) => {
  const s = _getChatSession(userId);
  if (!s) return;
  s.pending = null;
  s.expiresAt = Date.now() + SESSION_TTL_MS;
};

// =========================
// CHAT HISTORY HELPERS
// =========================
const HISTORY_MAX_MESSAGES = 40; // last 40 messages total (user+assistant)

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
  } = deps;

  const {
    Event,
    Account,
    Company,
    Contractor,
    Individual,
    Project,
    Category,
    Prepayment,
    Credit,
  } = models;

  const router = express.Router();

  // =========================
  // KZ time helpers (Asia/Almaty ~ UTC+05:00)
  // =========================
  const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

  const _kzStartOfDay = (d) => {
    const t = new Date(d);
    const shifted = new Date(t.getTime() + KZ_OFFSET_MS);
    shifted.setUTCHours(0, 0, 0, 0);
    return new Date(shifted.getTime() - KZ_OFFSET_MS);
  };

  const _kzEndOfDay = (d) => {
    const start = _kzStartOfDay(d);
    return new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
  };

  const _kzDateFromYMD = (y, mIdx, day) => {
    return new Date(Date.UTC(y, mIdx, day, 0, 0, 0, 0) - KZ_OFFSET_MS);
  };

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

  const _fmtIntRu = (n) => {
    const num = Number(n || 0);
    try {
      return new Intl.NumberFormat('ru-RU').format(Math.round(num)).replace(/\u00A0/g, ' ');
    } catch (_) {
      return String(Math.round(num));
    }
  };

  const _formatTenge = (n) => {
    const num = Number(n || 0);
    const sign = num < 0 ? '- ' : '';
    return sign + _fmtIntRu(Math.abs(num)) + ' ₸';
  };

  const _endOfToday = () => _kzEndOfDay(new Date());

  const _getAsOfFromReq = (req) => {
    const raw = req?.body?.asOf || req?.query?.asOf;
    const todayEnd = _endOfToday();
    if (!raw) return todayEnd;

    const d = new Date(raw);
    if (isNaN(d.getTime())) return todayEnd;

    const tooFarFuture = d.getTime() > (todayEnd.getTime() + 48 * 60 * 60 * 1000);
    if (tooFarFuture) return todayEnd;

    return _kzEndOfDay(d);
  };

  const _parseIsoYMDToKZEnd = (isoYmd) => {
    const s = String(isoYmd || '').trim();
    const m = s.match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})$/);
    if (!m) return null;
    const y = Number(m[1]);
    const mo = Number(m[2]) - 1;
    const d = Number(m[3]);
    if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d)) return null;
    const dt = _kzDateFromYMD(y, mo, d);
    if (Number.isNaN(dt.getTime())) return null;
    return _kzEndOfDay(dt);
  };

  // Fact "as of" date: prefer frontend snapshot day (aiContext.meta.today) if provided,
  // but never go beyond server's today.
  const _pickFactAsOf = (req, aiContext) => {
    const serverTodayEnd = _getAsOfFromReq(req);
    const feIso = aiContext?.meta?.today;
    const feEnd = _parseIsoYMDToKZEnd(feIso);
    if (!feEnd) return serverTodayEnd;
    return (feEnd.getTime() > serverTodayEnd.getTime()) ? serverTodayEnd : feEnd;
  };

  // ✅ user asked: no default "30 days". Use all-time unless user explicitly asked a period.
  const _parseDaysFromQuery = (qLower, fallback = null) => {
    const m = String(qLower || '').match(/\b(\d{1,4})\b\s*(дн(ей|я)?|day|days)\b/i);
    const n = m ? Number(m[1]) : NaN;
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.max(1, Math.min(3650, Math.floor(n)));
  };

  const _parseRuDateFromText = (text, baseDate = null) => {
    const s = String(text || '');

    let m = s.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b/);
    if (m) {
      const dd = Number(m[1]);
      const mm = Number(m[2]);
      let yy = Number(m[3]);
      if (yy < 100) yy = 2000 + yy;
      const d = _kzDateFromYMD(yy, mm - 1, dd);
      if (!Number.isNaN(d.getTime())) return d;
    }

    m = s.match(/\b(\d{4})-(\d{2})-(\d{2})\b/);
    if (m) {
      const yy = Number(m[1]);
      const mm = Number(m[2]);
      const dd = Number(m[3]);
      const d = _kzDateFromYMD(yy, mm - 1, dd);
      if (!Number.isNaN(d.getTime())) return d;
    }

    const months = [
      { re: /\bянвар\w*\b/i, idx: 0 },
      { re: /\bфеврал\w*\b/i, idx: 1 },
      { re: /\bмарт\w*\b/i, idx: 2 },
      { re: /\bапрел\w*\b/i, idx: 3 },
      { re: /\bма[йя]\w*\b/i, idx: 4 },
      { re: /\bиюн\w*\b/i, idx: 5 },
      { re: /\bиюл\w*\b/i, idx: 6 },
      { re: /\bавгуст\w*\b/i, idx: 7 },
      { re: /\bсентябр\w*\b/i, idx: 8 },
      { re: /\bоктябр\w*\b/i, idx: 9 },
      { re: /\bноябр\w*\b/i, idx: 10 },
      { re: /\bдекабр\w*\b/i, idx: 11 }
    ];

    const base = baseDate ? new Date(baseDate) : new Date();

    for (const mo of months) {
      if (mo.re.test(s)) {
        let y = base.getFullYear();
        const yM = s.match(/\b(20\d{2}|\d{2})\b/);
        if (yM) {
          y = Number(yM[1]);
          if (y < 100) y = 2000 + y;
        }
        if (/\bдо\s*конц\w*\b/i.test(s) || /\bконец\b/i.test(s)) {
          return _kzDateFromYMD(y, mo.idx + 1, 0);
        }
        return _kzDateFromYMD(y, mo.idx, 1);
      }
    }

    return null;
  };

  const _startOfDay = (d) => _kzStartOfDay(d);
  const _endOfDay = (d) => _kzEndOfDay(d);

  const _getUserMinEventDate = async (userId) => {
    const first = await Event.findOne({ userId: userId }).sort({ date: 1 }).select('date').lean();
    return first?.date ? _startOfDay(first.date) : _startOfDay(new Date());
  };

  const _getUserMaxEventDate = async (userId) => {
    const last = await Event.findOne({ userId: userId }).sort({ date: -1 }).select('date').lean();
    return last?.date ? _endOfDay(last.date) : _endOfDay(new Date());
  };

  const _resolveRangeFromQuery = async (userId, qLower, nowEndOfToday) => {
    const q = String(qLower || '');
    const todayStart = _startOfDay(nowEndOfToday);
    const tomorrowStart = _startOfDay(new Date(todayStart.getTime() + 24 * 60 * 60 * 1000));

    const wantsFuture = /прогноз|будущ|вперед|вперёд|план/i.test(q);

    const between = q.match(/\bс\s+(.+?)\s+по\s+(.+?)\b/i);
    if (between) {
      const fromD = _parseRuDateFromText(between[1], todayStart);
      const toD = _parseRuDateFromText(between[2], todayStart);
      if (fromD && toD) {
        return { from: _startOfDay(fromD), to: _endOfDay(toD), scope: (toD > nowEndOfToday ? 'mixed' : 'fact') };
      }
    }

    if (/\bдо\b/i.test(q)) {
      const toD = _parseRuDateFromText(q, todayStart);
      if (toD) {
        const to = _endOfDay(toD);
        if (to > nowEndOfToday) return { from: tomorrowStart, to, scope: 'forecast' };
        const minD = await _getUserMinEventDate(userId);
        return { from: minD, to, scope: 'fact' };
      }
    }

    if (/\bза\b/i.test(q) || /\bв\b/i.test(q)) {
      const moAnchor = _parseRuDateFromText(q, todayStart);
      if (moAnchor) {
        const start = new Date(moAnchor.getFullYear(), moAnchor.getMonth(), 1);
        const end = new Date(moAnchor.getFullYear(), moAnchor.getMonth() + 1, 0);
        return { from: _startOfDay(start), to: _endOfDay(end), scope: (end > nowEndOfToday ? 'mixed' : 'fact') };
      }
    }

    const days = _parseDaysFromQuery(q, null);
    if (days != null) {
      const from = new Date(_startOfDay(new Date()));
      from.setTime(from.getTime() - (Math.max(1, days) - 1) * 24 * 60 * 60 * 1000);
      return { from, to: nowEndOfToday, scope: 'fact' };
    }

    if (wantsFuture) {
      const maxD = await _getUserMaxEventDate(userId);
      return { from: tomorrowStart, to: maxD, scope: 'forecast' };
    }

    const minD = await _getUserMinEventDate(userId);
    return { from: minD, to: nowEndOfToday, scope: 'fact' };
  };

  const _parseExplicitLimitFromQuery = (qLower) => {
    const q = String(qLower || '');

    let m = q.match(/\b(топ|top)\s*(\d{1,4})\b/i);
    if (m && m[2]) {
      const n = Number(m[2]);
      if (Number.isFinite(n) && n > 0) return Math.min(5000, Math.floor(n));
    }

    m = q.match(/\b(\d{1,4})\b\s*(стр(ок|оки|ока)?|строк|линии|строч|пункт(ов|а|ы)?|позиц(ий|ии|ия)?|items?)\b/i);
    if (m && m[1]) {
      const n = Number(m[1]);
      if (Number.isFinite(n) && n > 0) return Math.min(5000, Math.floor(n));
    }

    return null;
  };

  const _maybeSlice = (arr, limit) => {
    if (!Array.isArray(arr)) return [];
    if (limit == null) return arr;
    return arr.slice(0, limit);
  };

  const _isIndividualsQuery = (qLower) => /физ\W*лиц|физ\W*лица|физическ\W*лиц|индивид/i.test(String(qLower || ''));
  const _wantsCatalogOnly = (qLower) => {
    const q = String(qLower || '').trim();
    if (/\b(топ|итог|итого|сколько|сумм|доход|расход|баланс|оборот|налог|прогноз|план|перевод|вывод|кредит)\b/i.test(q)) return false;
    if (/\bза\s*\d+\b/i.test(q)) return false;
    if (/\bс\s+.+?\s+по\s+.+?\b/i.test(q)) return false;
    if (/\bдо\b/i.test(q) && /\d|январ|феврал|март|апрел|ма[йя]|июн|июл|август|сентябр|октябр|ноябр|декабр/i.test(q)) return false;
    return true;
  };

  // Output style requirement: "Доходы. До DD.MM.YY"
  const _titleTo = (title, to) => `${title}. До ${_fmtDateKZ(to)}`;

  const _periodTotalsRange = async (userId, from, to, accountMatch = {}) => {
    const rows = await Event.aggregate([
      {
        $match: {
          userId: new mongoose.Types.ObjectId(userId),
          date: { $gte: from, $lte: to },
          excludeFromTotals: { $ne: true },
          isTransfer: { $ne: true },
          type: { $in: ['income', 'expense'] },
          ...accountMatch,
        }
      },
      { $project: { type: 1, absAmount: { $abs: '$amount' } } },
      { $group: { _id: '$type', total: { $sum: '$absAmount' } } }
    ]);

    let income = 0;
    let expense = 0;
    rows.forEach(r => {
      if (r._id === 'income') income = r.total;
      if (r._id === 'expense') expense = r.total;
    });
    return { income, expense, net: income - expense };
  };

  const _countEventsInRange = async (userId, from, to, extraMatch = {}) => {
    const n = await Event.countDocuments({
      userId: new mongoose.Types.ObjectId(userId),
      date: { $gte: from, $lte: to },
      excludeFromTotals: { $ne: true },
      ...extraMatch,
    });
    return Number(n || 0);
  };

  // Taxes accumulative: incomes by company * taxPercent
  const _calcTaxesAccumulativeRange = async (userId, from, to, accountMatch = {}) => {
    const rows = await Event.aggregate([
      {
        $match: {
          userId: new mongoose.Types.ObjectId(userId),
          date: { $gte: from, $lte: to },
          excludeFromTotals: { $ne: true },
          isTransfer: { $ne: true },
          type: 'income',
          companyId: { $ne: null },
          ...accountMatch,
        }
      },
      { $project: { companyId: 1, absAmount: { $abs: '$amount' } } },
      { $group: { _id: '$companyId', income: { $sum: '$absAmount' } } }
    ]);

    if (!rows.length) return { totalTax: 0, items: [] };

    const ids = rows.map(r => r._id).filter(Boolean);
    const companies = await Company.find({ _id: { $in: ids }, userId }).select('name taxPercent').lean();
    const map = new Map(companies.map(c => [c._id.toString(), c]));

    const items = rows.map(r => {
      const c = map.get(String(r._id));
      const percent = Number(c?.taxPercent ?? 0);
      const income = Number(r.income || 0);
      const tax = income * (percent / 100);
      return { companyId: r._id, companyName: c?.name || 'Компания', percent, income, tax };
    }).sort((a, b) => b.tax - a.tax);

    const totalTax = items.reduce((s, x) => s + Number(x.tax || 0), 0);
    return { totalTax, items };
  };

  const _openAiChat = async (messages, { temperature = 0.2, maxTokens = 220 } = {}) => {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      const err = new Error('OPENAI_API_KEY is missing');
      err.code = 'OPENAI_KEY_MISSING';
      throw err;
    }

    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';
    const payload = JSON.stringify({ model, messages, temperature, max_tokens: maxTokens });
    const timeoutMs = Number(process.env.OPENAI_TIMEOUT_MS || 20000);

    return new Promise((resolve, reject) => {
      const req2 = https.request(
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
                let msg = data;
                try {
                  const parsed = JSON.parse(data);
                  msg = parsed?.error?.message || msg;
                } catch (_) { }
                const err = new Error(`OpenAI HTTP ${resp.statusCode}: ${msg}`);
                err.httpStatus = resp.statusCode;
                return reject(err);
              }
              const json = JSON.parse(data);
              const text = json?.choices?.[0]?.message?.content || '';
              resolve(String(text || '').trim());
            } catch (e) {
              reject(new Error(`OpenAI parse error: ${e.message}`));
            }
          });
        }
      );
      req2.setTimeout(timeoutMs, () => { try { req2.destroy(new Error(`OpenAI timeout after ${timeoutMs}ms`)); } catch (_) { } });
      req2.on('error', reject);
      req2.write(payload);
      req2.end();
    });
  };

  const _isAiAllowed = (req) => {
    try {
      if (!req.user || !req.user.email) return false;
      if ((process.env.AI_ALLOW_ALL || '').toLowerCase() === 'true') return true;

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

  // -------------------------
  // Routes
  // -------------------------
  router.get('/ping', (req, res) => {
    res.json({
      ok: true,
      ts: new Date().toISOString(),
      isAuthenticated: (typeof req.isAuthenticated === 'function') ? req.isAuthenticated() : false,
      email: req.user?.email || null,
      ai: {
        model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
        keyPresent: Boolean(process.env.OPENAI_API_KEY),
        allowAll: String(process.env.AI_ALLOW_ALL || '').toLowerCase() === 'true'
      }
    });
  });

  router.post('/query', isAuthenticated, async (req, res) => {
    try {
      if (!_isAiAllowed(req)) return res.status(402).json({ message: 'AI not activated' });

      const userId = req.user.id;
      const userObjId = new mongoose.Types.ObjectId(userId);
      const userIdStr = String(userId);

      const qRaw = (req.body && req.body.message) ? String(req.body.message) : '';
      const q = qRaw.trim();
      if (!q) return res.status(400).json({ message: 'Empty message' });

      const qLower = q.toLowerCase();

      // Cyrillic-safe diagnostics detector (JS  doesn't work with кириллица)
      const _isDiagnosticsQuery = (s) => {
        const t = String(s || '').toLowerCase();
        if (!t) return false;
        // covers: "диагностика", common typos like "иагностика", and English
        if (t.includes('диагност') || t.includes('агност') || t.includes('diagnostic')) return true;
        // short command: diag
        return /(^|[^a-z])diag([^a-z]|$)/i.test(t);
      };

      const explicitLimit = _parseExplicitLimitFromQuery(qLower);

      const aiContext = (req.body && req.body.aiContext) ? req.body.aiContext : null;


      // =========================
      // UI SNAPSHOT MODE (NO MONGO)
      // =========================
      const uiSnapshot = (req.body && req.body.uiSnapshot) ? req.body.uiSnapshot : null;
      const snapWidgets = Array.isArray(uiSnapshot?.widgets) ? uiSnapshot.widgets : [];

      const snapTodayTitleStr = String(uiSnapshot?.meta?.todayStr || _fmtDateKZ(_endOfToday()));
      const snapFutureTitleStr = String(uiSnapshot?.meta?.futureUntilStr || snapTodayTitleStr);

      function _renderDiagnosticsFromSnapshot(uiSnapshotArg, todayDateStr, futureDateStr) {
        const s = uiSnapshotArg || null;
        if (!s) return 'Диагностика: uiSnapshot не получен. Открой главный экран с виджетами и повтори.';

        const widgets = Array.isArray(s.widgets) ? s.widgets : [];
        const widgetKeys = widgets.map(w => w?.key).filter(Boolean);

        const getRows = (w) => {
          if (!w) return [];
          if (Array.isArray(w.rows)) return w.rows;
          if (Array.isArray(w.items)) return w.items;
          if (Array.isArray(w.list)) return w.list;
          if (Array.isArray(w.data)) return w.data;
          if (Array.isArray(w.values)) return w.values;
          if (Array.isArray(w.names)) return w.names.map((name) => ({ name }));
          if (Array.isArray(w.titles)) return w.titles.map((title) => ({ name: title }));
          if (w.rows && typeof w.rows === 'object' && Array.isArray(w.rows.rows)) return w.rows.rows;
          return [];
        };

        const findWidget = (keyOrKeys) => {
          const keys = Array.isArray(keyOrKeys) ? keyOrKeys : [keyOrKeys];
          for (const k of keys) {
            const w = widgets.find(x => x && x.key === k);
            if (w) return w;
          }
          return null;
        };

        const hasWidget = (keyOrKeys) => Boolean(findWidget(keyOrKeys));
        const countRows = (keyOrKeys) => {
          const w = findWidget(keyOrKeys);
          const r = getRows(w);
          return Array.isArray(r) ? r.length : 0;
        };

        const fmtDateKZ = (d) => {
          try {
            const KZ_OFFSET_MS_LOCAL = 5 * 60 * 60 * 1000;
            const x = new Date(new Date(d).getTime() + KZ_OFFSET_MS_LOCAL);
            const dd = String(x.getUTCDate()).padStart(2, '0');
            const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
            const yy = String(x.getUTCFullYear() % 100).padStart(2, '0');
            return `${dd}.${mm}.${yy}`;
          } catch (_) {
            return String(d);
          }
        };

        const parseAnyDateToTs = (any) => {
          const v = (any == null) ? '' : String(any).trim();
          if (!v) return null;

          let m = v.match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})/);
          if (m) {
            const y = Number(m[1]);
            const mo = Number(m[2]) - 1;
            const dd = Number(m[3]);
            const dt = new Date(Date.UTC(y, mo, dd, 0, 0, 0, 0) - (5 * 60 * 60 * 1000));
            return Number.isNaN(dt.getTime()) ? null : dt.getTime();
          }

          m = v.match(/^([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{2,4})$/);
          if (m) {
            const dd = Number(m[1]);
            const mm = Number(m[2]);
            let yy = Number(m[3]);
            if (yy < 100) yy = 2000 + yy;
            const dt = new Date(Date.UTC(yy, mm - 1, dd, 0, 0, 0, 0) - (5 * 60 * 60 * 1000));
            return Number.isNaN(dt.getTime()) ? null : dt.getTime();
          }

          m = v.toLowerCase().match(/\b([0-9]{1,2})\s*(янв|фев|мар|апр|май|мая|июн|июл|авг|сен|сент|окт|ноя|дек)\w*\.?\s*(20\d{2})\b/);
          if (m) {
            const dd = Number(m[1]);
            const yy = Number(m[3]);
            const map = { янв: 0, фев: 1, мар: 2, апр: 3, май: 4, мая: 4, июн: 5, июл: 6, авг: 7, сен: 8, сент: 8, окт: 9, ноя: 10, дек: 11 };
            const mi = map[m[2]] ?? 0;
            const dt = new Date(Date.UTC(yy, mi, dd, 0, 0, 0, 0) - (5 * 60 * 60 * 1000));
            return Number.isNaN(dt.getTime()) ? null : dt.getTime();
          }

          const dt2 = new Date(v);
          if (!Number.isNaN(dt2.getTime())) return dt2.getTime();
          return null;
        };

        const guessKind = (widgetKey, row) => {
          const t = String(row?.type || row?.kind || '').toLowerCase();
          if (t === 'transfer' || row?.isTransfer) return 'transfer';
          if (t === 'withdrawal' || row?.isWithdrawal) return 'withdrawal';
          if (t === 'income') return 'income';
          if (t === 'expense') return 'expense';
          const wk = String(widgetKey || '').toLowerCase();
          if (/transfer|перевод/.test(wk)) return 'transfer';
          if (/withdraw|вывод|снят/.test(wk)) return 'withdrawal';
          if (/income|доход/.test(wk)) return 'income';
          if (/expense|расход/.test(wk)) return 'expense';
          return null;
        };

        const pickDateTs = (row) => {
          const v = row?.date ?? row?.dateIso ?? row?.dateYmd ?? row?.dateStr ?? row?.when ?? row?.whenStr ?? row?.dueDate ?? row?.dueDateStr ?? row?.plannedDate ?? row?.plannedDateStr ?? row?.payDate ?? row?.payDateStr ?? row?.createdAt;
          return parseAnyDateToTs(v);
        };


        const metaToday = s?.meta?.today || s?.meta?.todayIso || s?.meta?.todayYmd || s?.meta?.todayStr;
        const metaFuture = s?.meta?.futureUntil || s?.meta?.futureUntilIso || s?.meta?.futureUntilStr;

        // Always format dates using fmtDateKZ to ensure DD.MM.YY format
        const parsedToday = metaToday ? new Date(metaToday) : new Date();
        const todayStr = !isNaN(parsedToday.getTime()) ? fmtDateKZ(parsedToday) : fmtDateKZ(new Date());

        let futureStr = todayStr;
        if (metaFuture) {
          const parsedFuture = new Date(metaFuture);
          if (!isNaN(parsedFuture.getTime())) {
            futureStr = fmtDateKZ(parsedFuture);
          }
        }


        // Presence + counts
        const seen = {
          accounts: hasWidget('accounts'),
          incomes: hasWidget(['incomeListCurrent', 'incomeList', 'income', 'incomeSummary']),
          expenses: hasWidget(['expenseListCurrent', 'expenseList', 'expense', 'expenseSummary']),
          transfers: hasWidget(['transfersCurrent', 'transfers', 'transferList', 'transfersFuture']),
          withdrawals: hasWidget(['withdrawalListCurrent', 'withdrawalList', 'withdrawals', 'withdrawalsList', 'withdrawalListFuture']),
          taxes: hasWidget(['taxes', 'tax', 'taxList', 'taxesList']),
          credits: hasWidget(['credits', 'credit', 'creditList']),
          prepayments: hasWidget(['prepayments', 'prepaymentList', 'liabilities']),
          projects: hasWidget(['projects', 'projectList']),
          contractors: hasWidget(['contractors', 'contractorList', 'counterparties']),
          individuals: hasWidget(['individuals', 'individualList', 'persons', 'people']),
          categories: hasWidget(['categories', 'categoryList']),
          companies: hasWidget(['companies', 'companyList']),
        };

        let accountsTotal = 0;
        let accountsHidden = 0;
        if (seen.accounts) {
          const w = findWidget('accounts');
          const r = getRows(w);
          accountsTotal = Array.isArray(r) ? r.length : 0;
          accountsHidden = Array.isArray(r) ? r.filter(x => Boolean(x?.isExcluded)).length : 0;
        }

        // Collect operations
        const ops = [];
        const opKeys = [
          'incomeListCurrent', 'expenseListCurrent', 'withdrawalListCurrent', 'transfersCurrent',
          'incomeListFuture', 'expenseListFuture', 'withdrawalListFuture', 'transfersFuture',
          'incomeList', 'income', 'expenseList', 'expense', 'withdrawalList', 'withdrawals', 'withdrawalsList', 'transfers', 'transferList'
        ];

        for (const k of opKeys) {
          const w = findWidget(k);
          if (!w) continue;
          const wk = w?.key || k;
          const rows = getRows(w);
          for (const r of (rows || [])) {
            const ts = pickDateTs(r);
            if (!ts) continue;
            const kind = guessKind(wk, r);
            if (!kind) continue;
            ops.push({ ts, kind });
          }
        }

        try {
          const byDay = s?.storeTimeline?.opsByDay;
          if (byDay && typeof byDay === 'object') {
            for (const dayKey of Object.keys(byDay)) {
              const arr = byDay[dayKey];
              if (!Array.isArray(arr)) continue;
              for (const r of arr) {
                const ts = pickDateTs(r) || parseAnyDateToTs(dayKey);
                if (!ts) continue;
                const kind = guessKind('storeTimeline', r);
                if (!kind) continue;
                ops.push({ ts, kind });
              }
            }
          }
        } catch (_) { }

        const cnt = { income: 0, expense: 0, transfer: 0, withdrawal: 0 };
        let minTs = null;
        let maxTs = null;
        for (const x of ops) {
          if (Object.prototype.hasOwnProperty.call(cnt, x.kind)) cnt[x.kind] += 1;
          if (minTs === null || x.ts < minTs) minTs = x.ts;
          if (maxTs === null || x.ts > maxTs) maxTs = x.ts;
        }

        const opsTotal = cnt.income + cnt.expense + cnt.transfer + cnt.withdrawal;
        const minDate = (minTs != null) ? fmtDateKZ(new Date(minTs)) : '—';
        const maxDate = (maxTs != null) ? fmtDateKZ(new Date(maxTs)) : '—';

        const lines = [];
        lines.push('Диагностика:');
        lines.push(`Факт: до ${todayDateStr}`);
        lines.push(`Прогноз: до ${futureDateStr}`);
        lines.push(`Виджетов: ${widgetKeys.length}`);
        lines.push('Вижу:');
        lines.push(`Счета: ${seen.accounts ? 'да' : 'нет'} (${accountsTotal}${accountsHidden ? `, скрытых ${accountsHidden}` : ''})`);
        lines.push(`Доходы: ${seen.incomes ? 'да' : 'нет'} (строк ${countRows(['incomeListCurrent', 'incomeList', 'income'])})`);
        lines.push(`Расходы: ${seen.expenses ? 'да' : 'нет'} (строк ${countRows(['expenseListCurrent', 'expenseList', 'expense'])})`);
        lines.push(`Переводы: ${seen.transfers ? 'да' : 'нет'} (строк ${countRows(['transfersCurrent', 'transfers', 'transferList', 'transfersFuture'])})`);
        lines.push(`Выводы: ${seen.withdrawals ? 'да' : 'нет'} (строк ${countRows(['withdrawalListCurrent', 'withdrawalList', 'withdrawals', 'withdrawalsList', 'withdrawalListFuture'])})`);
        lines.push(`Налоги: ${seen.taxes ? 'да' : 'нет'} (строк ${countRows(['taxes', 'tax', 'taxList', 'taxesList'])})`);
        lines.push(`Кредиты: ${seen.credits ? 'да' : 'нет'} (строк ${countRows(['credits', 'credit', 'creditList'])})`);
        lines.push(`Предоплаты/обязательства: ${seen.prepayments ? 'да' : 'нет'} (строк ${countRows(['prepayments', 'prepaymentList', 'liabilities'])})`);
        lines.push(`Проекты: ${seen.projects ? 'да' : 'нет'} (${countRows(['projects', 'projectList'])})`);
        lines.push(`Контрагенты: ${seen.contractors ? 'да' : 'нет'} (${countRows(['contractors', 'contractorList', 'counterparties'])})`);
        lines.push(`Физлица: ${seen.individuals ? 'да' : 'нет'} (${countRows(['individuals', 'individualList', 'persons', 'people'])})`);
        lines.push(`Категории: ${seen.categories ? 'да' : 'нет'} (${countRows(['categories', 'categoryList'])})`);
        lines.push(`Компании: ${seen.companies ? 'да' : 'нет'} (${countRows(['companies', 'companyList'])})`);
        lines.push('Операции:');
        lines.push(`Диапазон: ${minDate} — ${maxDate}`);
        lines.push(`Всего: ${opsTotal}`);
        lines.push(`Доходы: ${cnt.income}`);
        lines.push(`Расходы: ${cnt.expense}`);
        lines.push(`Переводы: ${cnt.transfer}`);
        lines.push(`Выводы: ${cnt.withdrawal}`);

        // Widget keys removed per user request

        return lines.join('\n');
      }

      // HARD ROUTING: diagnostics must be deterministic (never OpenAI)
      // Moved below to use snapTodayDDMMYYYY and snapFutureDDMMYYYY

      // For lists like "Мои проекты" we want strict DD.MM.YYYY dates.
      const _fmtDateDDMMYYYY = (any) => {
        const s = String(any || '').trim();
        if (!s) return null;

        // YYYY-MM-DD
        let m = s.match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})$/);
        if (m) return `${m[3]}.${m[2]}.${m[1]}`;

        // DD.MM.YYYY or DD.MM.YY
        m = s.match(/^([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{2,4})$/);
        if (m) {
          const dd = String(m[1]).padStart(2, '0');
          const mm = String(m[2]).padStart(2, '0');
          let yy = String(m[3]);
          if (yy.length === 2) yy = '20' + yy;
          return `${dd}.${mm}.${yy}`;
        }

        // "28 дек. 2025" / "28 дек. 2025 г."
        m = s.toLowerCase().match(/\b([0-9]{1,2})\s*(янв|фев|мар|апр|май|мая|июн|июл|авг|сен|сент|окт|ноя|дек)\w*\.?\s*(20\d{2})\b/);
        if (m) {
          const dd = String(m[1]).padStart(2, '0');
          const mon = m[2];
          const yy = m[3];
          const map = {
            янв: '01', фев: '02', мар: '03', апр: '04',
            май: '05', мая: '05', июн: '06', июл: '07',
            авг: '08', сен: '09', сент: '09', окт: '10',
            ноя: '11', дек: '12',
          };
          const mm = map[mon] || '01';
          return `${dd}.${mm}.${yy}`;
        }

        return null;
      };

      const _fmtTodayDDMMYYYY = () => {
        const d = _endOfToday();
        const x = new Date(new Date(d).getTime() + KZ_OFFSET_MS);
        const dd = String(x.getUTCDate()).padStart(2, '0');
        const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
        const yyyy = String(x.getUTCFullYear());
        return `${dd}.${mm}.${yyyy}`;
      };

      const snapTodayDDMMYYYY =
        _fmtDateDDMMYYYY(uiSnapshot?.meta?.today)
        || _fmtDateDDMMYYYY(uiSnapshot?.meta?.todayIso)
        || _fmtDateDDMMYYYY(uiSnapshot?.meta?.todayYmd)
        || _fmtDateDDMMYYYY(snapTodayTitleStr)
        || _fmtTodayDDMMYYYY();

      const snapFutureDDMMYYYY =
        _fmtDateDDMMYYYY(uiSnapshot?.meta?.futureUntil)
        || _fmtDateDDMMYYYY(uiSnapshot?.meta?.futureUntilIso)
        || _fmtDateDDMMYYYY(uiSnapshot?.meta?.futureUntilStr)
        || _fmtDateDDMMYYYY(snapFutureTitleStr)
        || snapTodayDDMMYYYY;

      // Keep original snapshot strings for existing outputs ("До 28 дек. 2025 г.")
      const snapTodayStr = snapTodayTitleStr;
      const snapFutureStr = snapFutureTitleStr;
      // Default to current operations only - forecasts only when explicitly requested
      const wantsFutureSnap = /\b(прогноз|будущ|план|следующ|вперед|вперёд|после\s*сегодня)\b/i.test(qLower) && !/\b(текущ|сегодня|факт|до\s*сегодня)\b/i.test(qLower);

      const _snapTitleTo = (title, toStr) => `${title}. До ${toStr}`;
      const _findSnapWidget = (keyOrKeys) => {
        const keys = Array.isArray(keyOrKeys) ? keyOrKeys : [keyOrKeys];
        return (snapWidgets || []).find(w => w && keys.includes(w.key)) || null;
      };

      const _renderCatalogFromRows = (title, rows) => {
        const arr = Array.isArray(rows) ? rows : [];
        if (!arr.length) return `${title}: 0`;
        const lines = [`${title}: ${arr.length}`];
        _maybeSlice(arr, explicitLimit).forEach((x, i) => {
          const name = x?.name || x?.title || 'Без имени';
          // Add amounts to catalog lists
          const pf = _pickFactFuture(x);
          const factNum = _moneyToNumber(pf.fact);
          const futNum = _moneyToNumber(pf.fut);
          if (factNum !== 0 || futNum !== 0) {
            lines.push(`${i + 1}) ${name} ₸ ${pf.fact} > ${pf.fut}`);
          } else {
            lines.push(`${i + 1}) ${name}`);
          }
        });
        return lines.join('\n');
      };

      // -------------------------
      // CHAT MODE helpers (snapshot-only)
      // -------------------------
      const _normQ = (s) => String(s || '').toLowerCase().replace(/[\s\t\n\r]+/g, ' ').trim();

      // Parse numbered references like "проект 1", "№1", "номер 1"
      const _parseNumberedRef = (qLower) => {
        const patterns = [
          /(?:проект|категор|контрагент|физлиц|компан)\s*(?:номер|№|#)?\s*(\d+)/i,
          /(?:номер|№|#)\s*(\d+)/i,
          /\b(\d+)\b/
        ];

        for (const pattern of patterns) {
          const m = String(qLower || '').match(pattern);
          if (m && m[1]) {
            const num = Number(m[1]);
            if (Number.isFinite(num) && num > 0 && num <= 1000) {
              return num;
            }
          }
        }
        return null;
      };

      // Calculate totals with and without hidden accounts
      const _calcDualAccountTotals = (accounts) => {
        const arr = Array.isArray(accounts) ? accounts : [];
        let openTotal = 0;
        let hiddenTotal = 0;

        arr.forEach(acc => {
          const balance = _moneyToNumber(acc?.balance || acc?.currentBalance || acc?.factBalance || 0);
          if (acc?.isExcluded || acc?.hidden) {
            hiddenTotal += balance;
          } else {
            openTotal += balance;
          }
        });

        return {
          openTotal,
          allTotal: openTotal + hiddenTotal,
          hiddenTotal
        };
      };

      const _moneyToNumber = (v) => {
        if (v == null) return 0;
        if (typeof v === 'number' && Number.isFinite(v)) return v;
        const s = String(v).replace(/\u00A0/g, ' ').trim();
        if (!s) return 0;
        const neg = /^-/.test(s) || /\(\s*-/.test(s) || /-\s*\d/.test(s);
        const digits = s.replace(/[^0-9]/g, '');
        if (!digits) return 0;
        const n = Number(digits);
        if (!Number.isFinite(n)) return 0;
        return neg ? -n : n;
      };

      const _parseAnyDateToTs = (any, fallbackBase = null) => {
        const s = String(any || '').trim();
        if (!s) return null;

        // ISO
        let m = s.match(/^([0-9]{4})-([0-9]{2})-([0-9]{2})/);
        if (m) {
          const d = _kzDateFromYMD(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
          return Number.isNaN(d.getTime()) ? null : d.getTime();
        }

        // DD.MM.YYYY
        m = s.match(/^([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{2,4})$/);
        if (m) {
          const dd = Number(m[1]);
          const mm = Number(m[2]);
          let yy = Number(m[3]);
          if (yy < 100) yy = 2000 + yy;
          const d = _kzDateFromYMD(yy, mm - 1, dd);
          return Number.isNaN(d.getTime()) ? null : d.getTime();
        }

        // "28 дек. 2025"
        m = s.toLowerCase().match(/\b([0-9]{1,2})\s*(янв|фев|мар|апр|май|мая|июн|июл|авг|сен|сент|окт|ноя|дек)\w*\.?\s*(20\d{2})\b/);
        if (m) {
          const dd = Number(m[1]);
          const yy = Number(m[3]);
          const map = {
            янв: 0, фев: 1, мар: 2, апр: 3,
            май: 4, мая: 4, июн: 5, июл: 6,
            авг: 7, сен: 8, сент: 8, окт: 9,
            ноя: 10, дек: 11,
          };
          const mi = map[m[2]] ?? 0;
          const d = _kzDateFromYMD(yy, mi, dd);
          return Number.isNaN(d.getTime()) ? null : d.getTime();
        }

        // Try native Date
        const d2 = new Date(s);
        if (!Number.isNaN(d2.getTime())) return d2.getTime();

        // As last resort: if only day+month words appear, use base year
        const base = fallbackBase ? new Date(fallbackBase) : new Date();
        const months = [
          { re: /\bянв\w*\b/i, idx: 0 },
          { re: /\bфев\w*\b/i, idx: 1 },
          { re: /\bмар\w*\b/i, idx: 2 },
          { re: /\bапр\w*\b/i, idx: 3 },
          { re: /\bма[йя]\w*\b/i, idx: 4 },
          { re: /\bиюн\w*\b/i, idx: 5 },
          { re: /\bиюл\w*\b/i, idx: 6 },
          { re: /\bавг\w*\b/i, idx: 7 },
          { re: /\bсент?\w*\b/i, idx: 8 },
          { re: /\bокт\w*\b/i, idx: 9 },
          { re: /\bноя\w*\b/i, idx: 10 },
          { re: /\bдек\w*\b/i, idx: 11 },
        ];
        const ddM = s.match(/\b([0-9]{1,2})\b/);
        if (!ddM) return null;
        for (const mo of months) {
          if (mo.re.test(s)) {
            const d = _kzDateFromYMD(base.getFullYear(), mo.idx, Number(ddM[1]));
            return Number.isNaN(d.getTime()) ? null : d.getTime();
          }
        }
        return null;
      };

      const _firstNonEmpty = (vals) => {
        for (const v of (vals || [])) {
          if (v === null || typeof v === 'undefined') continue;
          if (typeof v === 'string' && String(v).trim() === '') continue;
          return v;
        }
        return null;
      };

      const _guessName = (row) => String(_firstNonEmpty([row?.name, row?.title, row?.label, row?.projectName, row?.project, row?.contractorName, row?.contractor, row?.counterparty, row?.companyName, row?.company]) || '—');

      const _guessProject = (row) => String(_firstNonEmpty([row?.projectName, row?.project, row?.projectTitle, row?.project_label, row?.projectLabel, row?.project_name, row?.project?.name, row?.project?.title]) || '—');

      const _guessContractor = (row) => String(_firstNonEmpty([row?.contractorName, row?.contractor, row?.counterparty, row?.counterpartyName, row?.fromName, row?.toName, row?.partyName, row?.contractor?.name]) || '—');

      const _guessCategory = (row) => String(_firstNonEmpty([
        row?.categoryName, row?.category, row?.categoryTitle, row?.category_label, row?.categoryLabel,
        row?.category_name, row?.category?.name, row?.category?.title,
        row?.catName, row?.cat, row?.catTitle,
        row?.labelCategory,
      ]) || '—');

      const _guessAmount = (row) => {
        const v = _firstNonEmpty([
          row?.amount, row?.sum, row?.value, row?.absAmount,
          row?.amountText, row?.sumText, row?.valueText,
          row?.currentAmount, row?.current, row?.fact, row?.factAmount,
          row?.currentText, row?.factText,
          row?.income, row?.incomeAmount, row?.incomeText,
          row?.expense, row?.expenseAmount, row?.expenseText,
        ]);
        return _moneyToNumber(v);
      };

      const _guessType = (widgetKey, row) => {
        const t = String(row?.type || row?.kind || '').toLowerCase();
        if (t === 'income' || t === 'in' || t === 'plus') return 'income';
        if (t === 'expense' || t === 'out' || t === 'minus') return 'expense';

        const wk = String(widgetKey || '').toLowerCase();
        if (/income|доход/.test(wk)) return 'income';
        if (/expense|расход/.test(wk)) return 'expense';

        const amt = _guessAmount(row);
        if (amt > 0) return 'income';
        if (amt < 0) return 'expense';
        return null;
      };

      const _guessDateTs = (row, baseTs = null) => {
        const v = _firstNonEmpty([
          row?.date, row?.dateIso, row?.dateYmd, row?.dateStr,
          row?.when, row?.whenStr, row?.dueDate, row?.dueDateStr,
          row?.plannedDate, row?.plannedDateStr,
          row?.payDate, row?.payDateStr,
          row?.createdAt,
        ]);
        return _parseAnyDateToTs(v, baseTs);
      };

      const _extractIncomeExpense = (row) => {
        // Try to find explicit income/expense numbers; return null if not present.
        const fi = _firstNonEmpty([row?.factIncome, row?.incomeFact, row?.incomeCurrent, row?.currentIncome, row?.income, row?.incomeAmount, row?.incomeText]);
        const fe = _firstNonEmpty([row?.factExpense, row?.expenseFact, row?.expenseCurrent, row?.currentExpense, row?.expense, row?.expenseAmount, row?.expenseText]);
        const pi = _firstNonEmpty([row?.planIncome, row?.futureIncome, row?.incomeFuture, row?.incomePlan, row?.forecastIncome]);
        const pe = _firstNonEmpty([row?.planExpense, row?.futureExpense, row?.expenseFuture, row?.expensePlan, row?.forecastExpense]);

        const hasAny = (fi != null) || (fe != null) || (pi != null) || (pe != null);
        if (!hasAny) return null;

        return {
          factIncome: _moneyToNumber(fi),
          factExpense: Math.abs(_moneyToNumber(fe)),
          planIncome: _moneyToNumber(pi),
          planExpense: Math.abs(_moneyToNumber(pe)),
        };
      };

      const _renderProfitByProjects = (projectsWidget, rows, title = 'Прибыль проектов', showList = false) => {
        const arr = Array.isArray(rows) ? rows : [];
        if (!arr.length) {
          return _wrapBlock(title, projectsWidget || null, ['На экране не вижу список проектов.']);
        }

        const items = arr.map((r) => {
          const name = String(r?.name || r?.title || r?.label || '—');
          const ie = _extractIncomeExpense(r);
          if (ie) {
            const profitFact = Number(ie.factIncome || 0) - Number(ie.factExpense || 0);
            const profitPlan = Number(ie.planIncome || 0) - Number(ie.planExpense || 0);
            return { name, fact: profitFact, fut: profitPlan, mode: 'calc' };
          }
          // Fallback: treat widget's displayed fact/future as "profit as shown".
          const pf = _pickFactFuture(r);
          return { name, fact: _moneyToNumber(pf.fact), fut: _moneyToNumber(pf.fut), mode: 'as_shown' };
        });

        // Sort by fact profit desc
        items.sort((a, b) => (Number(b.fact || 0) - Number(a.fact || 0)));

        const body = [];
        let totalFact = 0;
        let totalFut = 0;

        // Only show list if explicitly requested
        if (showList) {
          _maybeSlice(items, explicitLimit).forEach((x) => {
            totalFact += Number(x.fact || 0);
            totalFut += Number(x.fut || 0);
            body.push(`${x.name} ₸ ${_fmtMoneyInline(x.fact)} > ${_fmtMoneyInline(x.fut)}`);
          });
        } else {
          // Just calculate totals without showing list
          items.forEach((x) => {
            totalFact += Number(x.fact || 0);
            totalFut += Number(x.fut || 0);
          });
        }

        body.push(`Итого прибыль ₸ ${_fmtMoneyInline(totalFact)} > ${_fmtMoneyInline(totalFut)}`);
        body.push(`Проектов: ${items.length}`);

        return _wrapBlock(title, projectsWidget || null, body);
      };

      const _collectUpcoming = (wantType, baseTs) => {
        const out = [];
        (snapWidgets || []).forEach((w) => {
          const wk = w?.key || '';
          const rows = _getRows(w);
          (rows || []).forEach((r) => {
            const ts = _guessDateTs(r, baseTs);
            if (!ts) return;
            const type = _guessType(wk, r);
            if (!type) return;
            if (wantType && type !== wantType) return;

            // Only future-ish (today and beyond)
            if (baseTs != null && ts < baseTs) return;

            const amount = _guessAmount(r);
            if (!amount) return;

            const dateLabel = _fmtDateDDMMYYYY(r?.date || r?.dateIso || r?.dateYmd || r?.dateStr) || _fmtDateKZ(new Date(ts));
            const contractor = _guessContractor(r);
            const project = _guessProject(r);
            const name = _guessName(r);

            out.push({ ts, type, amount, dateLabel, contractor, project, name, widgetKey: wk });
          });
        });
        out.sort((a, b) => (a.ts - b.ts));
        return out;
      };

      const _getSummaryPair = (keyOrKeys) => {
        const w = _findSnapWidget(keyOrKeys);
        if (!w) return null;

        const rows = _getRows(w);
        const r0 = rows && rows.length ? rows[0] : null;

        const factRaw =
          (r0?.currentText ?? r0?.factText ?? w?.currentText ?? w?.factText ?? w?.summaryCurrentText ?? w?.summaryText ?? w?.valueText ?? w?.text ?? r0?.current ?? r0?.fact ?? r0?.value ?? w?.current ?? w?.fact ?? w?.value ?? 0);

        const futureRaw =
          (r0?.futureText ?? r0?.planText ?? w?.futureText ?? w?.planText ?? w?.summaryFutureText ?? w?.summaryText ?? w?.valueText ?? w?.text ?? r0?.future ?? r0?.plan ?? w?.future ?? w?.plan ?? 0);

        const pair = _pickFactFuture({ currentText: factRaw, futureText: futureRaw, factText: factRaw, planText: futureRaw });
        return { widget: w, fact: pair.fact, fut: pair.fut };
      };

      const _renderUpcoming = (title, wantType, baseTs) => {
        const items = _collectUpcoming(wantType, baseTs);
        if (!items.length) {
          // Fallback: if the screen only has summary totals (no dated rows), return the summary instead of a dead-end message.
          const sum = (wantType === 'income')
            ? _getSummaryPair(['incomeList', 'income', 'incomeSummary'])
            : (wantType === 'expense')
              ? _getSummaryPair(['expenseList', 'expense', 'expenseSummary'])
              : null;

          if (sum && sum.widget) {
            const note = (wantType === 'income')
              ? 'На этом экране нет списка доходов с датами/контрагентами — вижу только итог.'
              : 'На этом экране нет списка расходов с датами/контрагентами — вижу только итог.';

            return _wrapBlock(`${title} (итог без списка)`, sum.widget, [
              `₸ ${sum.fact} > ${sum.fut}`,
              note,
              'Чтобы показать ближайшие по датам — открой экран "Операции" / список (где есть строки с датами) и повтори запрос.'
            ]);
          }

          return [
            `${title}:`,
            'На этом экране не вижу будущих операций с датами (доход/расход).',
            'Открой экран/виджет, где есть список операций с датами, и повтори вопрос.'
          ].join('\n');
        }

        const lines = [`${title}:`];
        const sliced = _maybeSlice(items, explicitLimit || 10);
        sliced.forEach((x, i) => {
          const signAmt = (x.type === 'expense') ? -Math.abs(x.amount) : Math.abs(x.amount);
          const who = (x.contractor && x.contractor !== '—') ? x.contractor : x.name;
          const proj = (x.project && x.project !== '—') ? ` | ${x.project}` : '';
          lines.push(`${i + 1}) ${x.dateLabel} | ${who}${proj} | ${_formatTenge(signAmt)}`);
        });
        return lines.join('\n');
      };

      const _openAiChatFromSnapshot = async (qText) => {
        // Diagnostics must be deterministic even in CHAT path
        const qn = _normQ(qText);
        if (_isDiagnosticsQuery(qn)) {
          return _renderDiagnosticsFromSnapshot(uiSnapshot);
        }
        // --- Build compact DATA packet (single source of truth)
        const _safeJson = (obj, maxLen = 15000) => {
          try {
            const s = JSON.stringify(obj);
            if (s.length <= maxLen) return s;
            return s.slice(0, maxLen) + '…';
          } catch (_) {
            return '{}';
          }
        };

        const _stripEmoji = (s) => {
          try {
            return String(s || '')
              // extended pictographic (emoji) + misc symbols blocks
              .replace(/[\u{1F000}-\u{1FAFF}]/gu, '')
              .replace(/[\u{2600}-\u{27BF}]/gu, '')
              .replace(/[\u{FE0F}]/gu, '');
          } catch (_) {
            return String(s || '');
          }
        };

        const _sanitizeAiText = (s) => {
          let out = String(s || '').replace(/\u00A0/g, ' ');
          out = _stripEmoji(out);
          out = out.replace(/[\t\r]+/g, '');
          out = out.trim();

          // Character limits removed per user request - allow full responses
          const maxChars = Number(process.env.AI_MAX_CHARS || 10000);
          const maxLines = Number(process.env.AI_MAX_LINES || 150);
          let lines = out.split('\n').map(x => x.trim()).filter(Boolean);
          if (lines.length > maxLines) lines = lines.slice(0, maxLines);
          out = lines.join('\n');
          if (out.length > maxChars) out = out.slice(0, maxChars).trim();

          return out;
        };

        const _buildDataPacket = () => {
          const listKeys = (snapWidgets || []).map(w => w?.key).filter(Boolean);

          const pickTotals = () => {
            const inc = _getSummaryPair(['incomeList', 'income', 'incomeSummary']);
            const exp = _getSummaryPair(['expenseList', 'expense', 'expenseSummary']);
            const trn = _getSummaryPair(['transfers', 'transferList', 'transfersCurrent', 'transfersFuture']);
            const wdr = _getSummaryPair(['withdrawalList', 'withdrawals', 'withdrawalsList', 'withdrawalListCurrent', 'withdrawalListFuture']);
            const tax = _getSummaryPair(['taxes', 'tax', 'taxList', 'taxesList']);

            return {
              income: inc ? { fact: _moneyToNumber(inc.fact), forecast: _moneyToNumber(inc.fut) } : null,
              expense: exp ? { fact: _moneyToNumber(exp.fact), forecast: _moneyToNumber(exp.fut) } : null,
              transfers: trn ? { fact: _moneyToNumber(trn.fact), forecast: _moneyToNumber(trn.fut) } : null,
              withdrawals: wdr ? { fact: _moneyToNumber(wdr.fact), forecast: _moneyToNumber(wdr.fut) } : null,
              taxes: tax ? { fact: _moneyToNumber(tax.fact), forecast: _moneyToNumber(tax.fut) } : null,
            };
          };

          const pickAccounts = () => {
            const w = _findSnapWidget('accounts');
            if (!w) return [];
            const rows = _getRows(w);
            return (rows || []).map(r => ({
              name: String(r?.name || '—'),
              hidden: Boolean(r?.isExcluded),
              excluded: Boolean(r?.isExcluded),
              factBalance: _moneyToNumber(_pickFactFuture({
                ...r,
                currentText: r?.balanceText ?? r?.currentText ?? r?.factText,
                futureText: r?.futureText ?? r?.planText,
                currentBalance: r?.balance ?? r?.currentBalance ?? r?.factBalance,
                futureBalance: r?.futureBalance ?? r?.planBalance,
              }).fact),
              forecastBalance: _moneyToNumber(_pickFactFuture({
                ...r,
                currentText: r?.balanceText ?? r?.currentText ?? r?.factText,
                futureText: r?.futureText ?? r?.planText,
                currentBalance: r?.balance ?? r?.currentBalance ?? r?.factBalance,
                futureBalance: r?.futureBalance ?? r?.planBalance,
              }).fut),
            }));
          };

          const pickTaxes = () => {
            const w = _findSnapWidget('taxes') || _findSnapWidget('tax') || _findSnapWidget('taxList');
            if (!w) return [];
            const rows = _getRows(w);
            return (rows || []).map(r => ({
              company: String(r?.name || r?.company || r?.companyName || '—'),
              fact: _moneyToNumber(_pickFactFuture({
                ...r,
                currentText: r?.factText ?? r?.currentText,
                futureText: r?.futureText ?? r?.planText,
                currentBalance: r?.fact ?? r?.factBalance,
                futureBalance: r?.fut ?? r?.futureBalance,
              }).fact),
              forecast: _moneyToNumber(_pickFactFuture({
                ...r,
                currentText: r?.factText ?? r?.currentText,
                futureText: r?.futureText ?? r?.planText,
                currentBalance: r?.fact ?? r?.factBalance,
                futureBalance: r?.fut ?? r?.futureBalance,
              }).fut),
            }));
          };

          const pickCatalog = (keys) => {
            const w = _findSnapWidget(keys);
            if (!w) return [];
            const rows = _getRows(w);
            return (rows || []).map(r => String(r?.name || r?.title || r?.label || '—'));
          };

          const pickOps = () => {
            const out = [];
            const baseTs = _kzStartOfDay(new Date()).getTime();
            const all = _opsCollectRows();
            (all || []).forEach(x => {
              const r = x.__row;
              const ts = _guessDateTs(r, baseTs);
              if (!ts) return;
              const kind = _opsGuessKind(x.__wk, r);
              if (!kind) return;
              const amount = _guessAmount(r);
              const date = _fmtDateDDMMYYYY(r?.date || r?.dateIso || r?.dateYmd || r?.dateStr) || _fmtDateKZ(new Date(ts));
              out.push({
                kind,
                date,
                ts,
                amount: Number(amount || 0),
                project: _guessProject(r),
                contractor: _guessContractor(r),
                category: _guessCategory(r),
                name: _guessName(r),
                source: String(x.__wk || ''),
              });
            });
            // Increased limit to ensure all recent operations are included
            out.sort((a, b) => b.ts - a.ts);
            return out.slice(0, 1000);
          };

          const pickTimeline = () => {
            // Group operations by date for better AI understanding
            const byDay = uiSnapshot?.storeTimeline?.opsByDay;
            if (!byDay || typeof byDay !== 'object') return null;

            const timeline = {};
            for (const dateKey of Object.keys(byDay)) {
              const arr = byDay[dateKey];
              if (!Array.isArray(arr)) continue;

              const dayOps = [];
              for (const op of arr) {
                const baseTs = _kzStartOfDay(new Date()).getTime();
                const ts = _guessDateTs(op, baseTs);
                if (!ts) continue;
                const kind = _opsGuessKind('storeTimeline', op);
                if (!kind) continue;

                dayOps.push({
                  kind,
                  amount: _guessAmount(op),
                  project: _guessProject(op),
                  contractor: _guessContractor(op),
                  category: _guessCategory(op),
                  name: _guessName(op),
                });
              }

              if (dayOps.length > 0) {
                timeline[dateKey] = dayOps;
              }
            }

            return Object.keys(timeline).length > 0 ? timeline : null;
          };

          return {
            meta: {
              today: snapTodayDDMMYYYY,
              forecastUntil: snapFutureDDMMYYYY,
              todayTimestamp: _kzStartOfDay(new Date()).getTime(), // Add timestamp for filtering
              widgets: listKeys,
            },
            totals: pickTotals(),
            accounts: pickAccounts(),
            taxes: pickTaxes(), // Add detailed tax breakdown by company
            catalogs: {
              projects: pickCatalog(['projects', 'projectList']),
              contractors: pickCatalog(['contractors', 'contractorList', 'counterparties']),
              categories: pickCatalog(['categories', 'categoryList']),
              individuals: pickCatalog(['individuals', 'individualList', 'persons', 'people']),
              companies: pickCatalog(['companies', 'companyList']),
            },
            operations: pickOps(),
            timeline: pickTimeline(), // Add timeline structure
          };
        };

        const dataPacket = _buildDataPacket();

        const system = [
          'Ты финансовый ассистент INDEX12.',
          'Режим CHAT. Запрещено выдумывать: используй ТОЛЬКО факты и цифры из DATA.',
          'Если факта нет в DATA — так и скажи и укажи, каких данных/какой экран не хватает.',
          'Эмодзи запрещены.',
          'КРИТИЧНО: НИКОГДА не выдумывай операции! Используй ТОЛЬКО операции из DATA.operations и DATA.timeline.',
          'DATA.timeline содержит операции, сгруппированные по датам (ключи - даты в формате YYYY-MM-DD или DD.MM.YYYY).',
          'Если пользователь просит показать операции за период - используй DATA.timeline для точного ответа.',
          'Для каждой даты в DATA.timeline показаны ВСЕ операции этого дня с их суммами, проектами, контрагентами и категориями.',
          'НЕ придумывай суммы, даты, контрагентов или проекты - используй ТОЛЬКО то, что есть в DATA.timeline и DATA.operations.',
          'ВАЖНО: Пользователь хочет видеть РЕЗУЛЬТАТЫ и РАСЧЕТЫ, а не длинные списки операций.',
          'Если речь о прибыльности/эффективности - выводи только ИТОГ и АНАЛИЗ, НЕ списки операций.',
          'Списки операций показывай ТОЛЬКО если пользователь явно попросил "покажи список" или "покажи расходы".',
          'По умолчанию показывай только текущие операции (до сегодня). Прогнозы добавляй только если пользователь явно попросил.',
          '',
          'ЛОГИКА СКРЫТЫХ СЧЕТОВ:',
          '- В DATA.accounts каждый счет имеет поле hidden (true/false)',
          '- Открытые счета: hidden = false',
          '- Скрытые счета: hidden = true',
          '- При расчетах проверь, есть ли скрытые счета с ненулевыми балансами',
          '- Если ВСЕ скрытые счета имеют баланс = 0, НЕ показывай два варианта',
          '- Если есть скрытые счета с балансом ≠ 0, покажи:',
          '  1) Сумма по открытым счетам (hidden = false)',
          '  2) Сумма по всем счетам (открытые + скрытые)',
          '- НЕ дублируй одинаковые суммы - если разницы нет, покажи только один вариант',
          '',
          'ФИЛЬТРАЦИЯ И ГРУППИРОВКА ОПЕРАЦИЙ:',
          '- Когда пользователь просит "расходы по категории X" - фильтруй DATA.operations где kind="expense" И category="X"',
          '- Когда пользователь просит "доходы по проекту Y" - фильтруй DATA.operations где kind="income" И project="Y"',
          '- Когда пользователь просит "сгруппировать по категориям" - группируй операции по полю category и суммируй',
          '- При группировке показывай: Категория → Сумма (например: "Ремонт: -88 320₸")',
          '- Если пользователь просит "показать все" или "не сокращённый список" - покажи ВСЕ операции, не ограничивай 10 штуками',
          '- Если нашёл 26 операций - покажи все 26, а не только первые 10',
          '',
          'КРИТИЧНО - РАЗНИЦА МЕЖДУ ТЕКУЩИМИ И БУДУЩИМИ ОПЕРАЦИЯМИ:',
          '- ТЕКУЩИЕ операции = дата ≤ сегодня (DATA.meta.today)',
          '- БУДУЩИЕ операции (прогнозы) = дата > сегодня',
          '- В DATA.operations каждая операция имеет поле ts (timestamp)',
          '- Для фильтрации используй: op.ts <= DATA.meta.todayTimestamp (текущие) или op.ts > DATA.meta.todayTimestamp (будущие)',
          '- Когда пользователь просит "текущие доходы" - фильтруй DATA.operations где kind="income" И ts <= todayTimestamp',
          '- Когда пользователь просит "прогнозы" или "будущие доходы" - фильтруй где kind="income" И ts > todayTimestamp',
          '- Когда пользователь просит "ближайший доход" - ищи ПЕРВУЮ операцию где kind="income" И ts > todayTimestamp, отсортируй по ts',
          '- Когда пользователь просит доход за конкретную дату - проверь эту дату в DATA.timeline',
          '- DATA.totals содержит fact (текущие) и forecast (будущие) - используй правильное поле',
          '',
          'ФОРМАТ ОТВЕТА ПРИ ПОКАЗЕ ОПЕРАЦИЙ ПО ДНЯМ:',
          'Используй КОМПАКТНЫЙ формат с разделителями между днями:',
          '',
          '----------------',
          'пт, 25 дек. 2025 г.',
          '+50 000 т < Счет < TOO UU < INDEX12 < Маркетинг',
          '----------------',
          'сб, 26 дек. 2025 г.',
          '+500 000 т < Счет < TOO UU < INDEX12 < Маркетинг',
          '+150 000 т < Счет < TOO UU < INDEX12 < Маркетинг',
          '-250 000 т > Счет > Давид > INDEX12 > Маркетинг',
          '----------------',
          'пн, 28 дек. 2025 г.',
          '+100 000 т < Счет < — < — < Перевод',
          '----------------',
          'Операции 27 и 29 декабря отсутствуют. 30 декабря также нет операций.',
          '',
          'ПРАВИЛА ФОРМАТИРОВАНИЯ:',
          '- Разделитель между днями: ровно 16 дефисов "----------------"',
          '- Дата: "пт, 26 дек. 2025 г." (день недели сокращенно, день, месяц сокращенно, год)',
          '- НЕ показывай баланс, итоги по типам, группировки ДОХОДЫ/РАСХОДЫ',
          '- Просто список операций под датой',
          '- Для доходов: знак < (стрелка влево)',
          '- Для расходов: знак > (стрелка вправо)',
          '- Формат суммы: "+500 000 т" или "-250 000 т" (пробелы в тысячах, "т" вместо "₸")',
          '- Порядок: Сумма < Счет < Контрагент < Проект < Категория',
          '- Если поле отсутствует - используй "—"',
          '- В конце укажи дни без операций одной строкой',
          '',
          'КРИТИЧНО ПРИ РАСЧЕТАХ:',
          '- Когда считаешь сумму (налоги, доходы, расходы и т.д.) - суммируй ВСЕ элементы из DATA',
          '- НЕ показывай только первый элемент или часть данных',
          '- Проверь DATA.totals, DATA.catalogs и другие разделы для полной информации',
          '- Если в DATA есть несколько компаний/проектов - учитывай их ВСЕ',
          '- DATA.taxes содержит массив налогов по компаниям [{company, fact, forecast}] - суммируй их ВСЕ',
          '',
          'Отвечай коротко и понятно. Формат денег: разделение тысяч + "₸". Даты: ДД.ММ.ГГГГ.'
        ].join('\n');

        const history = _getHistoryMessages(userIdStr);
        const messages = [
          { role: 'system', content: system },
          { role: 'system', content: `DATA(JSON): ${_safeJson(dataPacket, 15000)}` },
          ...history,
          { role: 'user', content: String(qText || '').trim() }
        ];

        const raw = await _openAiChat(messages, { temperature: 0.2, maxTokens: 1500 });
        const clean = _sanitizeAiText(raw);

        // Persist CHAT history (server-side)
        _pushHistory(userIdStr, 'user', String(qText || '').trim());
        _pushHistory(userIdStr, 'assistant', clean);

        return clean;
      };

      const _warnForecastOff = (w) => {
        if (!wantsFutureSnap) return '';
        if (w?.showFutureBalance) return '';
        return 'Прогноз выключен в виджете — на экране вижу только факт.';
      };

      const _getRows = (w) => {
        if (!w) return [];
        if (Array.isArray(w.rows)) return w.rows;
        if (Array.isArray(w.items)) return w.items;
        if (Array.isArray(w.list)) return w.list;
        if (Array.isArray(w.data)) return w.data;
        if (Array.isArray(w.values)) return w.values;
        if (Array.isArray(w.names)) return w.names.map((name) => ({ name }));
        if (Array.isArray(w.titles)) return w.titles.map((title) => ({ name: title }));
        // Sometimes rows are nested
        if (w.rows && typeof w.rows === 'object' && Array.isArray(w.rows.rows)) return w.rows.rows;
        return [];
      };

      // -------- Render rows exactly like widget: NAME ₸ FACT > FORECAST
      const _fmtMoneyInline = (v) => {
        if (v == null) return '0';
        if (typeof v === 'number' && Number.isFinite(v)) {
          const neg = v < 0;
          const abs = Math.abs(v);
          if (!abs) return '0';
          return (neg ? '- ' : '') + _fmtIntRu(abs);
        }

        const s = String(v).replace(/\u00A0/g, ' ').trim();
        if (!s) return '0';

        // Keep digits/spaces/minus, drop ₸ and other chars
        const cleaned = s
          .replace(/₸/g, '')
          .replace(/[^0-9\s\-]/g, '')
          .trim();

        if (!cleaned) return '0';

        const neg = /^-/.test(cleaned) || /\s-\s/.test(cleaned) || /-\s*\d/.test(cleaned);

        const digits = cleaned.replace(/[^0-9]/g, '');
        if (!digits) return '0';

        const num = Number(digits);
        if (!Number.isFinite(num) || num === 0) return '0';

        return (neg ? '- ' : '') + _fmtIntRu(Math.abs(num));
      };
      const _pickFactFuture = (r) => {
        const _first = (vals) => {
          for (const v of (vals || [])) {
            if (v === null || typeof v === 'undefined') continue;
            if (typeof v === 'string' && String(v).trim() === '') continue;
            return v;
          }
          return null;
        };

        const _p = (obj, path) => {
          try {
            return String(path || '').split('.').reduce((acc, key) => {
              if (!acc || typeof acc !== 'object') return undefined;
              return acc[key];
            }, obj);
          } catch (_) {
            return undefined;
          }
        };

        const _pairFrom = (val) => {
          if (val === null || typeof val === 'undefined') return null;
          const s = String(val);
          if (!s.includes('>')) return null;
          const parts = s.split('>');
          if (parts.length < 2) return null;
          const left = parts[0];
          const right = parts.slice(1).join('>');
          if (String(left).trim() === '' && String(right).trim() === '') return null;
          return { fact: left, fut: right };
        };

        // 1) Some widgets store "FACT > FORECAST" as a single text field.
        const pair = _pairFrom(_first([
          r?.pairText,
          r?.valueText,
          r?.text,
          r?.displayText,
          r?.subtitle,
          r?.subTitle,
          _p(r, 'value.text'),
          _p(r, 'valueText'),
          _p(r, 'summary.text'),
          _p(r, 'summaryText'),
        ]));
        if (pair) {
          return { fact: _fmtMoneyInline(pair.fact), fut: _fmtMoneyInline(pair.fut) };
        }

        // 2) Otherwise try many possible field names (flat + nested).
        const factRaw = _first([
          r?.currentText, r?.factText,
          r?.currentValueText, r?.factValueText,
          r?.currentBalanceText, r?.factBalanceText, r?.balanceText,
          r?.currentAmountText, r?.factAmountText, r?.amountText,
          r?.current, r?.fact,
          r?.currentValue, r?.factValue,
          r?.currentBalance, r?.factBalance, r?.balance,
          r?.currentAmount, r?.factAmount, r?.amount,
          r?.value, r?.sum,
          _p(r, 'current.text'), _p(r, 'fact.text'),
          _p(r, 'current.valueText'), _p(r, 'fact.valueText'),
          _p(r, 'current.value'), _p(r, 'fact.value'),
          _p(r, 'current.balance'), _p(r, 'fact.balance'),
          _p(r, 'current.amount'), _p(r, 'fact.amount'),
          _p(r, 'totals.fact'), _p(r, 'totals.current'),
          _p(r, 'stats.fact'), _p(r, 'stats.current'),
        ]) ?? 0;

        const futureRaw = _first([
          r?.futureText, r?.planText, r?.forecastText,
          r?.futureValueText, r?.planValueText,
          r?.futureBalanceText, r?.planBalanceText,
          r?.futureAmountText, r?.planAmountText,
          r?.future, r?.plan, r?.forecast,
          r?.futureValue, r?.planValue,
          r?.futureBalance, r?.planBalance,
          r?.futureAmount, r?.planAmount,
          _p(r, 'future.text'), _p(r, 'plan.text'), _p(r, 'forecast.text'),
          _p(r, 'future.valueText'), _p(r, 'plan.valueText'),
          _p(r, 'future.value'), _p(r, 'plan.value'),
          _p(r, 'future.balance'), _p(r, 'plan.balance'),
          _p(r, 'future.amount'), _p(r, 'plan.amount'),
          _p(r, 'totals.future'), _p(r, 'totals.plan'),
          _p(r, 'stats.future'), _p(r, 'stats.plan'),
        ]) ?? 0;

        return { fact: _fmtMoneyInline(factRaw), fut: _fmtMoneyInline(futureRaw) };
      };

      const _renderDualFactForecastList = (title, widget, rows) => {
        const arr = Array.isArray(rows) ? rows : [];

        const lines = [
          '===================',
          `${title}:`,
          `Факт: до ${snapTodayDDMMYYYY}`,
          `Прогноз: до ${snapFutureDDMMYYYY}`,
        ];

        if (!arr.length) {
          lines.push(`${title}: 0`);
        } else {
          _maybeSlice(arr, explicitLimit).forEach((x) => {
            const name = x?.name || x?.title || x?.label || 'Без имени';
            const { fact, fut } = _pickFactFuture(x);
            lines.push(`${name} ₸ ${fact} > ${fut}`);
          });
        }

        lines.push('===================');


        return lines.join('\n');
      };

      const _wrapBlock = (title, widget, bodyLines) => {
        const lines = [
          '===================',
          `${title}:`,
          `Факт: до ${snapTodayDDMMYYYY}`,
          `Прогноз: до ${snapFutureDDMMYYYY}`,
          ...(Array.isArray(bodyLines) ? bodyLines : []),
          '===================',
        ];


        return lines.join('\n');
      };

      const _renderDualValueBlock = (title, widget, factRaw, futureRaw) => {
        const { fact, fut } = _pickFactFuture({ currentText: factRaw, futureText: futureRaw, factText: factRaw, planText: futureRaw });
        return _wrapBlock(title, widget, [`₸ ${fact} > ${fut}`]);
      };

      const _renderDualRowsBlock = (title, widget, rows, opts = {}) => {
        const arr = Array.isArray(rows) ? rows : [];
        const nameKey = opts.nameKey || null;

        if (!arr.length) {
          return _wrapBlock(title, widget, [`${title}: 0`]);
        }

        const body = [];
        _maybeSlice(arr, explicitLimit).forEach((r) => {
          const name = nameKey ? (r?.[nameKey]) : (r?.name || r?.title || r?.label || '—');
          const { fact, fut } = _pickFactFuture(r);
          body.push(`${name} ₸ ${fact} > ${fut}`);
        });

        return _wrapBlock(title, widget, body);
      };

      const _renderAccountsBlock = (widget, rows) => {
        const arr = Array.isArray(rows) ? rows : [];
        const includeExcludedInTotal = Boolean(uiSnapshot?.ui?.includeExcludedInTotal);

        const body = [];
        let factTotal = 0;
        let futTotal = 0;

        _maybeSlice(arr, explicitLimit).forEach((r) => {
          const name = r?.name || '—';
          const hidden = r?.isExcluded ? ' (скрыт)' : '';

          const { fact, fut } = _pickFactFuture({
            ...r,
            currentText: r?.balanceText ?? r?.currentText ?? r?.factText,
            futureText: r?.futureText ?? r?.planText,
            currentBalance: r?.balance ?? r?.currentBalance ?? r?.factBalance,
            futureBalance: r?.futureBalance ?? r?.planBalance,
          });

          body.push(`${name}${hidden} ₸ ${fact} > ${fut}`);

          if (!includeExcludedInTotal && r?.isExcluded) return;
          factTotal += _moneyToNumber(fact);
          futTotal += _moneyToNumber(fut);
        });

        body.push(`Итого ₸ ${_fmtMoneyInline(factTotal)} > ${_fmtMoneyInline(futTotal)}`);

        return _wrapBlock('Счета', widget, body);
      };

      const _summaryDual = (keyOrKeys, title) => {
        const w = _findSnapWidget(keyOrKeys);
        if (!w) return null;

        const rows = _getRows(w);
        const r0 = rows && rows.length ? rows[0] : null;

        const factRaw =
          (r0?.currentText ?? r0?.factText ?? w?.currentText ?? w?.factText ?? w?.summaryCurrentText ?? w?.summaryText ?? w?.valueText ?? w?.text ?? r0?.current ?? r0?.fact ?? r0?.value ?? w?.current ?? w?.fact ?? w?.value ?? 0);

        const futureRaw =
          (r0?.futureText ?? r0?.planText ?? w?.futureText ?? w?.planText ?? w?.summaryFutureText ?? w?.summaryText ?? w?.valueText ?? w?.text ?? r0?.future ?? r0?.plan ?? w?.future ?? w?.plan ?? 0);

        return _renderDualValueBlock(title, w, factRaw, futureRaw);
      };

      // =========================
      // REPORTS (snapshot-only)
      // =========================
      const _fmtRangeLines = () => {
        return {
          factLine: `Факт: до ${snapTodayDDMMYYYY}`,
          futLine: `Прогноз: до ${snapFutureDDMMYYYY}`,
        };
      };

      const _looksLikePnL = (s) => {
        const t = String(s || '').toLowerCase();
        return (
          (/отч[её]т/.test(t) && (/(прибыл|убыт)/.test(t) || /p\s*&\s*l|pnl/.test(t))) ||
          /прибыл\w*\s+и\s+убыт/i.test(t) ||
          /p\s*&\s*l|pnl/.test(t)
        );
      };

      const _looksLikeCashFlow = (s) => {
        const t = String(s || '').toLowerCase();
        return (
          (/движ(ени|ение)\s*ден/i.test(t)) ||
          (/ддс\b/i.test(t)) ||
          (/cash\s*flow/i.test(t)) ||
          (/отч[её]т/.test(t) && (/движ/i.test(t) || /cash\s*flow/i.test(t)))
        );
      };

      const _looksLikeBalanceSheet = (s) => {
        const t = String(s || '').toLowerCase();
        return (
          (/баланс\b/i.test(t) && (/отч[её]т|sheet|отчетность|отчётность/i.test(t) || /баланс\b/.test(t))) ||
          (/balance\s*sheet/i.test(t))
        );
      };

      const _getAccountsTotals = () => {
        const acc = _findSnapWidget('accounts');
        if (!acc) return null;
        const rows = _getRows(acc);
        const includeExcludedInTotal = Boolean(uiSnapshot?.ui?.includeExcludedInTotal);

        const factTotal = Array.isArray(rows)
          ? rows.reduce((s, r) => {
            if (!includeExcludedInTotal && r?.isExcluded) return s;
            const pf = _pickFactFuture({
              ...r,
              currentText: r?.balanceText ?? r?.currentText ?? r?.factText,
              futureText: r?.futureText ?? r?.planText,
              currentBalance: r?.balance ?? r?.currentBalance ?? r?.factBalance,
              futureBalance: r?.futureBalance ?? r?.planBalance,
            });
            return s + _moneyToNumber(pf.fact);
          }, 0)
          : 0;

        const futTotal = Array.isArray(rows)
          ? rows.reduce((s, r) => {
            if (!includeExcludedInTotal && r?.isExcluded) return s;
            const pf = _pickFactFuture({
              ...r,
              currentText: r?.balanceText ?? r?.currentText ?? r?.factText,
              futureText: r?.futureText ?? r?.planText,
              currentBalance: r?.balance ?? r?.currentBalance ?? r?.factBalance,
              futureBalance: r?.futureBalance ?? r?.planBalance,
            });
            return s + _moneyToNumber(pf.fut);
          }, 0)
          : 0;

        return { widget: acc, factTotal, futTotal };
      };

      const _renderPnLReport = () => {
        const { factLine, futLine } = _fmtRangeLines();

        const inc = _getSummaryPair(['incomeList', 'income', 'incomeSummary']);
        const exp = _getSummaryPair(['expenseList', 'expense', 'expenseSummary']);

        if (!inc && !exp) {
          return _wrapBlock('Отчёт о прибылях и убытках', null, [
            'На этом экране не вижу итогов доходов/расходов.',
            'Открой главный экран с виджетами "Доходы" и "Расходы" и повтори запрос.'
          ]);
        }

        const incFact = _moneyToNumber(inc?.fact ?? 0);
        const incFut = _moneyToNumber(inc?.fut ?? 0);
        const expFact = _moneyToNumber(exp?.fact ?? 0); // usually negative
        const expFut = _moneyToNumber(exp?.fut ?? 0);

        const netFact = incFact + expFact;
        const netFut = incFut + expFut;

        const body = [
          factLine,
          futLine,
          `Доходы ${_formatTenge(incFact)} > ${_formatTenge(incFut)}`,
          `Расходы ${_formatTenge(expFact)} > ${_formatTenge(expFut)}`,
          `Чистая прибыль ${_formatTenge(netFact)} > ${_formatTenge(netFut)}`,
        ];

        // Optional hint
        body.push('Если нужен разрез — напиши: "прибыльность по проектам".');

        return _wrapBlock('Отчёт о прибылях и убытках', null, body);
      };

      const _renderCashFlowReport = () => {
        const { factLine, futLine } = _fmtRangeLines();

        const inc = _getSummaryPair(['incomeList', 'income', 'incomeSummary']);
        const exp = _getSummaryPair(['expenseList', 'expense', 'expenseSummary']);
        const trn = _getSummaryPair(['transfers', 'transferList']);
        const wdr = _getSummaryPair(['withdrawalList', 'withdrawals', 'withdrawalsList']);

        const acc = _getAccountsTotals();

        const incFact = _moneyToNumber(inc?.fact ?? 0);
        const incFut = _moneyToNumber(inc?.fut ?? 0);
        const expFact = _moneyToNumber(exp?.fact ?? 0);
        const expFut = _moneyToNumber(exp?.fut ?? 0);
        const wdrFact = _moneyToNumber(wdr?.fact ?? 0);
        const wdrFut = _moneyToNumber(wdr?.fut ?? 0);
        const trnFact = _moneyToNumber(trn?.fact ?? 0);
        const trnFut = _moneyToNumber(trn?.fut ?? 0);

        // Net cash flow: incomes + expenses + withdrawals (all signed)
        const netFact = incFact + expFact + wdrFact;
        const netFut = incFut + expFut + wdrFut;

        const body = [
          factLine,
          futLine,
          `Поступления (доходы) ${_formatTenge(incFact)} > ${_formatTenge(incFut)}`,
          `Выплаты (расходы) ${_formatTenge(expFact)} > ${_formatTenge(expFut)}`,
        ];

        // Withdrawals are not always present on screen
        if (wdr) body.push(`Выводы/снятия ${_formatTenge(wdrFact)} > ${_formatTenge(wdrFut)}`);

        body.push(`Чистый денежный поток ${_formatTenge(netFact)} > ${_formatTenge(netFut)}`);

        // Transfers: show volume only (does not change net)
        if (trn) body.push(`Переводы между счетами (оборот) ${_formatTenge(trnFact)} > ${_formatTenge(trnFut)}`);

        // Accounts totals if available
        if (acc) body.push(`Остаток на счетах ${_formatTenge(acc.factTotal)} > ${_formatTenge(acc.futTotal)}`);

        body.push('Если нужно "ближайшие платежи/поступления" — открой экран со списком операций и спроси: "ближайшие доходы" / "ближайшие расходы".');

        return _wrapBlock('Отчёт о движении денег (ДДС)', null, body);
      };

      const _sumWidgetRowsAsNumber = (keyOrKeys) => {
        const w = _findSnapWidget(keyOrKeys);
        if (!w) return null;
        const rows = _getRows(w);
        if (!rows.length) return { widget: w, factTotal: 0, futTotal: 0 };

        let factTotal = 0;
        let futTotal = 0;
        rows.forEach((r) => {
          const pf = _pickFactFuture(r);
          factTotal += _moneyToNumber(pf?.fact ?? 0);
          futTotal += _moneyToNumber(pf?.fut ?? 0);
        });

        return { widget: w, factTotal, futTotal };
      };

      const _renderBalanceSheetReport = () => {
        const { factLine, futLine } = _fmtRangeLines();

        const acc = _getAccountsTotals();
        const cr = _sumWidgetRowsAsNumber(['credits', 'credit', 'creditList']);
        const tx = _sumWidgetRowsAsNumber(['taxes', 'tax', 'taxList', 'taxesList']);
        const pp = _sumWidgetRowsAsNumber(['liabilities', 'prepayments', 'prepaymentList']);

        if (!acc) {
          return _wrapBlock('Баланс (упрощённый)', null, [
            'На этом экране не вижу виджет счетов (accounts).',
            'Открой главный экран со счетами и повтори запрос.'
          ]);
        }

        const assetsFact = Number(acc.factTotal || 0);
        const assetsFut = Number(acc.futTotal || 0);

        // Liabilities: make them positive in the report
        const creditsFact = cr ? Math.abs(Number(cr.factTotal || 0)) : 0;
        const creditsFut = cr ? Math.abs(Number(cr.futTotal || 0)) : 0;
        const taxesFact = tx ? Math.abs(Number(tx.factTotal || 0)) : 0;
        const taxesFut = tx ? Math.abs(Number(tx.futTotal || 0)) : 0;
        const prepFact = pp ? Math.abs(Number(pp.factTotal || 0)) : 0;
        const prepFut = pp ? Math.abs(Number(pp.futTotal || 0)) : 0;

        const liabFact = creditsFact + taxesFact + prepFact;
        const liabFut = creditsFut + taxesFut + prepFut;

        const eqFact = assetsFact - liabFact;
        const eqFut = assetsFut - liabFut;

        const body = [
          factLine,
          futLine,
          `Активы (деньги на счетах) ${_formatTenge(assetsFact)} > ${_formatTenge(assetsFut)}`,
        ];

        if (cr) body.push(`Обязательства: кредиты ${_formatTenge(-creditsFact)} > ${_formatTenge(-creditsFut)}`);
        if (tx) body.push(`Обязательства: налоги ${_formatTenge(-taxesFact)} > ${_formatTenge(-taxesFut)}`);
        if (pp) body.push(`Обязательства: предоплаты ${_formatTenge(-prepFact)} > ${_formatTenge(-prepFut)}`);

        body.push(`Итого обязательства ${_formatTenge(-liabFact)} > ${_formatTenge(-liabFut)}`);
        body.push(`Собственный капитал (упрощённо) ${_formatTenge(eqFact)} > ${_formatTenge(eqFut)}`);

        body.push('Это упрощённый баланс по тому, что видно на экране (без дебиторки/товара/ОС).');

        return _wrapBlock('Баланс (упрощённый)', null, body);
      };

      // =========================
      // OPERATIONS (snapshot-only)
      // =========================
      const _opsGuessKind = (widgetKey, row) => {
        const t = String(row?.type || row?.kind || '').toLowerCase();
        if (t === 'transfer' || row?.isTransfer) return 'transfer';
        if (t === 'withdrawal' || row?.isWithdrawal) return 'withdrawal';
        if (t === 'income') return 'income';
        if (t === 'expense') return 'expense';

        const wk = String(widgetKey || '').toLowerCase();
        if (/transfer|перевод/.test(wk)) return 'transfer';
        if (/withdraw|вывод|снят/.test(wk)) return 'withdrawal';
        if (/income|доход/.test(wk)) return 'income';
        if (/expense|расход/.test(wk)) return 'expense';

        const amt = _guessAmount(row);
        if (amt > 0) return 'income';
        if (amt < 0) return 'expense';
        return null;
      };

      const _opsCollectRows = () => {
        const keys = [
          // Mobile current lists
          'incomeListCurrent', 'expenseListCurrent', 'withdrawalListCurrent', 'transfersCurrent',
          // Potential future lists
          'incomeListFuture', 'expenseListFuture', 'withdrawalListFuture', 'transfersFuture',
          // Common legacy keys
          'incomeList', 'income', 'incomeSummary',
          'expenseList', 'expense', 'expenseSummary',
          'withdrawalList', 'withdrawals', 'withdrawalsList',
          'transfers', 'transferList'
        ];

        const out = [];
        for (const k of keys) {
          const w = _findSnapWidget(k);
          if (!w) continue;
          const wk = w?.key || k;
          const rows = _getRows(w);
          for (const r of (rows || [])) {
            const ts = _guessDateTs(r, _kzStartOfDay(new Date()).getTime());
            if (!ts) continue;
            out.push({ __wk: wk, __ts: ts, __row: r });
          }
        }
        // Desktop timeline (storeTimeline.opsByDay) — include ops even if widgets list doesn't contain dated rows
        try {
          const byDay = uiSnapshot?.storeTimeline?.opsByDay;
          if (byDay && typeof byDay === 'object') {
            const baseTs = _kzStartOfDay(new Date()).getTime();
            for (const dateKey of Object.keys(byDay)) {
              const arr = byDay[dateKey];
              if (!Array.isArray(arr)) continue;
              for (const op of arr) {
                const ts = _guessDateTs(op, baseTs);
                if (!ts) continue;
                out.push({ __wk: 'storeTimeline', __ts: ts, __row: op });
              }
            }
          }
        } catch (_) { }
        return out;
      };

      const _opsExtractUntilTs = (qLower) => {
        // If user explicitly says "до <date>", use that date.
        const m = String(qLower || '').match(/\b(?:до|по)\b\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})/i);
        if (m && m[1]) {
          const d = _parseRuDateFromText(m[1], new Date());
          if (d) return _kzEndOfDay(d).getTime();
        }

        // "до сегодня" / "на сегодня" => use snapshot fact date.
        if (/сегодня|текущ|на\s*сегодня|сего\s*дня/i.test(String(qLower || ''))) {
          const d = _parseRuDateFromText(snapTodayDDMMYYYY, new Date());
          if (d) return _kzEndOfDay(d).getTime();
        }

        // Default: end of today.
        return _endOfToday().getTime();
      };

      const _looksLikeOpsUntil = (qLower, kind) => {
        const t = String(qLower || '').toLowerCase();
        const hasUntil = /\bдо\b/.test(t) && (
          /сегодня|текущ|на\s*сегодня|сего\s*дня/.test(t) ||
          /\bдо\b\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})/.test(t)
        );
        if (!hasUntil) return false;

        if (kind === 'expense') return /(расход|тра(т|чу)|потрат|списан)/.test(t);
        if (kind === 'income') return /(доход|выруч|поступл|поступ)/.test(t);
        if (kind === 'transfer') return /(перевод|трансфер)/.test(t);
        if (kind === 'withdrawal') return /(вывод|сняти|снять|withdraw)/.test(t);
        return false;
      };

      const _opsFmtLine = (x, kindHint = null) => {
        const r = x.__row;
        const dLabel = _fmtDateDDMMYYYY(r?.date || r?.dateIso || r?.dateYmd || r?.dateStr) || _fmtDateKZ(new Date(x.__ts));

        const rawName = _guessName(r);
        let contractor = _guessContractor(r);
        let category = _guessCategory(r);

        // Handle composite "Категория - Контрагент" coming from mobile rows (e.g. "Маркетинг - Давид")
        if ((contractor === '—' || category === '—') && rawName && rawName !== '—') {
          const parts = String(rawName).split(/\s*-\s*/).map(s => String(s).trim()).filter(Boolean);
          if (parts.length === 2) {
            if (category === '—') category = parts[0];
            if (contractor === '—') contractor = parts[1];
          }
        }

        // If contractor is still unknown, show rawName as contractor placeholder
        if (!contractor || contractor === '—') contractor = (rawName && rawName !== '—') ? rawName : '—';

        let amt = _guessAmount(r);
        if (kindHint === 'expense') amt = -Math.abs(Number(amt || 0));
        if (kindHint === 'income') amt = Math.abs(Number(amt || 0));

        return `- ${dLabel} – ${contractor} – ${category} – ${_formatTenge(amt)}`;
      };

      const _detectScopeFromText = (qLower) => {
        const t = String(qLower || '').toLowerCase();
        if (/(\bвсе\b|\bоба\b|полностью|полный|весь|вместе|all)/i.test(t)) return 'all';
        if (/(будущ|прогноз|план|ожидаем|следующ|после\s*сегодня|future)/i.test(t)) return 'future';
        if (/(текущ|сегодня|на\s*сегодня|факт|истор|до\s*сегодня|по\s*сегодня|current)/i.test(t)) return 'current';
        return null;
      };

      const _detectFormatFromText = (qLower) => {
        const t = String(qLower || '').toLowerCase();

        // Detailed / expanded list
        if (/(подроб|детал|разверн|полный\s*список)/i.test(t)) return 'detailed';

        // Compact list: only date + amount
        // Covers:
        // - "только дата и сумма" / "только дата и только сумма"
        // - "включая дату и сумму"
        // - "дата сумма" / "дата, сумма" / "дата + сумма"
        // - inflected forms: "дату", "сумму"
        // - exclusions: "без контрагента", "без категории"
        if (
          /(?:только\s*)?дат[ауеы]?\s*(?:и\s*(?:только\s*)?)?сумм\w*/i.test(t) ||
          /включа\w*\s*дат[ауеы]?\s*(?:и\s*)?сумм\w*/i.test(t) ||
          /\b(дата|дату|даты)\b[\s,;:/\-+]*\b(сумма|сумму|суммы)\b/i.test(t) ||
          /\bdate\b[\s,;:/\-+]*\b(amount|sum)\b/i.test(t) ||
          /(без\s*(контраг|контраген|категор))/i.test(t)
        ) {
          return 'date_amount';
        }

        return 'short';
      };

      const _opsGetRowsForKindScope = (kind, scope) => {
        const k = String(kind || '').toLowerCase();
        const sc = String(scope || 'current').toLowerCase();

        const collect = (keys) => {
          const out = [];
          (keys || []).forEach((key) => {
            const w = _findSnapWidget(key);
            if (!w) return;
            const rows = _getRows(w);
            (rows || []).forEach((r) => out.push({ __wk: w?.key || key, __row: r }));
          });
          return out;
        };

        const map = {
          income: {
            current: ['incomeListCurrent', 'incomeList'],
            future: ['incomeListFuture'],
          },
          expense: {
            current: ['expenseListCurrent', 'expenseList'],
            future: ['expenseListFuture'],
          },
          transfer: {
            current: ['transfersCurrent', 'transfers', 'transferList'],
            future: ['transfersFuture'],
          },
          withdrawal: {
            current: ['withdrawalListCurrent', 'withdrawalList', 'withdrawals', 'withdrawalsList'],
            future: ['withdrawalListFuture'],
          },
        };

        const cur = collect(map[k]?.current || []);
        const fut = collect(map[k]?.future || []);

        // Desktop timeline (storeTimeline.opsByDay): normalize into the same rows format as widgets
        const timeline = [];
        try {
          const byDay = uiSnapshot?.storeTimeline?.opsByDay;
          if (byDay && typeof byDay === 'object') {
            const baseTs = _kzStartOfDay(new Date()).getTime();
            const endTodayTs = _endOfToday().getTime();
            for (const dateKey of Object.keys(byDay)) {
              const arr = byDay[dateKey];
              if (!Array.isArray(arr)) continue;
              for (const op of arr) {
                const ts = _guessDateTs(op, baseTs);
                if (!ts) continue;
                const kk = _opsGuessKind('storeTimeline', op);
                if (kk !== k) continue;

                if (sc === 'current' && ts > endTodayTs) continue;
                if (sc === 'future' && ts <= endTodayTs) continue;

                timeline.push({ __wk: 'storeTimeline', __row: op });
              }
            }
          }
        } catch (_) { }

        if (sc === 'future') return fut.concat(timeline);
        if (sc === 'all') return cur.concat(fut).concat(timeline);
        return cur.concat(timeline);
      };

      const _opsCollectScopedCounts = (kind) => {
        const cur = _opsGetRowsForKindScope(kind, 'current');
        const fut = _opsGetRowsForKindScope(kind, 'future');
        return { curCount: cur.length, futCount: fut.length };
      };

      const _opsFmtLineUnified = (x, kindHint, opts = {}) => {
        const r = x.__row;
        const ts = x.__ts;
        const dLabel = _fmtDateDDMMYYYY(r?.date || r?.dateIso || r?.dateYmd || r?.dateStr) || _fmtDateKZ(new Date(ts));

        let amt = _guessAmount(r);
        if (kindHint === 'expense') amt = -Math.abs(Number(amt || 0));
        if (kindHint === 'income') amt = Math.abs(Number(amt || 0));

        // Compact: only date + amount
        const lineStyle = String(opts?.lineStyle || '').toLowerCase();
        if (lineStyle === 'date_amount') {
          return `- ${dLabel} – ${_formatTenge(amt)}`;
        }

        const rawName = _guessName(r);
        let contractor = _guessContractor(r);
        let category = _guessCategory(r);

        // Handle composite "Категория - Контрагент" (e.g. "Маркетинг - Давид")
        if ((contractor === '—' || category === '—') && rawName && rawName !== '—') {
          const parts = String(rawName).split(/\s*-\s*/).map(s => String(s).trim()).filter(Boolean);
          if (parts.length === 2) {
            if (category === '—') category = parts[0];
            if (contractor === '—') contractor = parts[1];
          }
        }

        if (!contractor || contractor === '—') contractor = (rawName && rawName !== '—') ? rawName : '—';

        const showProject = Boolean(opts.showProject);
        const project = _guessProject(r);
        const projPart = (showProject && project && project !== '—') ? ` – ${project}` : '';

        return `- ${dLabel} – ${contractor} – ${category}${projPart} – ${_formatTenge(amt)}`;
      };

      const _renderOpsList = (kind, scope, opts = {}) => {
        const k = String(kind || '').toLowerCase();
        const sc = String(scope || 'current').toLowerCase();
        const format = String(opts.format || 'short').toLowerCase();
        const showProject = (format === 'detailed');

        const raw = _opsGetRowsForKindScope(k, sc);
        const baseTs = _kzStartOfDay(new Date()).getTime();

        const rows = raw
          .map((x) => ({
            ...x,
            __ts: _guessDateTs(x.__row, baseTs),
            __kind: _opsGuessKind(x.__wk, x.__row)
          }))
          .filter((x) => x.__kind === k)
          .filter((x) => Number.isFinite(x.__ts));

        if (!rows.length) {
          const title = (k === 'income') ? 'Доходы' : (k === 'expense') ? 'Расходы' : 'Операции';
          return [
            `${title}:`,
            'На этом экране не вижу списка операций с датами.',
            'Открой экран/виджет со списком операций (где есть строки с датами) и повтори запрос.'
          ].join('\n');
        }

        // Sort: current -> newest first, future -> ближайшие first, all -> newest first
        rows.sort((a, b) => {
          if (sc === 'future') return a.__ts - b.__ts;
          return b.__ts - a.__ts;
        });

        const safeLimit = Number.isFinite(opts.limit) ? Math.max(1, Math.min(200, Math.floor(opts.limit))) : 50;
        const shown = rows.slice(0, safeLimit);

        const title = (k === 'income') ? 'Доходы' : (k === 'expense') ? 'Расходы' : 'Операции';
        const scopeTitle = (sc === 'future') ? ' (будущие)' : (sc === 'all') ? ' (все)' : ' (текущие)';

        const lines = [`${title}${scopeTitle}:`];
        const lineStyle = (format === 'date_amount') ? 'date_amount' : '';
        shown.forEach((x, i) => lines.push(`${i + 1}) ${_opsFmtLineUnified(x, k, { showProject, lineStyle })}`));

        lines.push(`Найдено: ${rows.length}. Показал: ${shown.length}.`);
        if (!opts.noHints && rows.length > shown.length) {
          lines.push('Скажи: "покажи все" или "топ 50" или "подробно".');
        }

        return lines.join('\n');
      };

      const _renderScopeQuestion = (kind, counts) => {
        const title = (kind === 'income') ? 'Доходы' : 'Расходы';
        return `${title}: вижу текущие ${counts.curCount} и будущие ${counts.futCount}. Что показать: текущие / будущие / все?`;
      };

      const _looksLikeOpsByProject = (qLower, kind) => {
        const t = String(qLower || '').toLowerCase();
        const wants = /(по\s*проектам|по\s*проекту|разрез\s*проект)/.test(t);
        if (!wants) return false;

        if (kind === 'expense') return /(расход|тра(т|чу)|потрат|списан|платеж|платёж|оплат)/.test(t);
        if (kind === 'income') return /(доход|выруч|поступл|поступ)/.test(t);
        if (kind === 'transfer') return /(перевод|трансфер)/.test(t);
        if (kind === 'withdrawal') return /(вывод|сняти|снять|withdraw)/.test(t);
        return false;
      };

      const _renderOpsByProject = (wantKind) => {
        const kind = String(wantKind || '').toLowerCase();
        const scope = _detectScopeFromText(qLower) || 'current';

        const raw = _opsGetRowsForKindScope(kind, scope);
        const baseTs = _kzStartOfDay(new Date()).getTime();

        const rows = raw
          .map((x) => ({
            ...x,
            __ts: _guessDateTs(x.__row, baseTs),
            __kind: _opsGuessKind(x.__wk, x.__row)
          }))
          .filter((x) => x.__kind === kind)
          .filter((x) => Number.isFinite(x.__ts));

        if (!rows.length) {
          const title = (kind === 'income') ? 'Доходы по проектам' : 'Расходы по проектам';
          return [
            `${title}:`,
            'На этом экране не вижу списка операций с датами/проектами.',
            'Открой экран/виджет со списком операций (где в строках есть проект) и повтори запрос.'
          ].join('\n');
        }

        const map = new Map();
        rows.forEach((x) => {
          const p = _guessProject(x.__row);
          const project = (p && p !== '—') ? p : 'Без проекта';

          let amt = _guessAmount(x.__row);
          if (kind === 'expense') amt = -Math.abs(Number(amt || 0));
          if (kind === 'income') amt = Math.abs(Number(amt || 0));

          if (!map.has(project)) map.set(project, { project, count: 0, total: 0 });
          const cur = map.get(project);
          cur.count += 1;
          cur.total += Number(amt || 0);
        });

        const arr = Array.from(map.values()).sort((a, b) => Math.abs(b.total) - Math.abs(a.total));

        const title = (kind === 'income') ? 'Доходы по проектам' : 'Расходы по проектам';
        const limit = explicitLimit || 30;
        const shown = arr.slice(0, limit);

        const lines = [`${title}:`];
        shown.forEach((r, i) => {
          lines.push(`${i + 1}) ${r.project} — ${_formatTenge(r.total)} (${r.count})`);
        });

        if (arr.length > shown.length) lines.push(`…и ещё ${arr.length - shown.length}`);

        return lines.join('\n');
      };

      const _renderOpsUntil = (wantKind) => {
        const untilTs = _opsExtractUntilTs(qLower);
        const all = _opsCollectRows();

        const filtered = all
          .map(x => ({ ...x, __kind: _opsGuessKind(x.__wk, x.__row) }))
          .filter(x => x.__kind === wantKind)
          .filter(x => x.__ts <= untilTs)
          .sort((a, b) => b.__ts - a.__ts);

        if (!filtered.length) {
          const title = (wantKind === 'expense') ? 'Расходы до сегодня'
            : (wantKind === 'income') ? 'Доходы до сегодня'
              : (wantKind === 'transfer') ? 'Переводы до сегодня'
                : 'Выводы до сегодня';

          return _wrapBlock(title, null, [
            'На этом экране не вижу списка операций с датами за прошлые периоды.',
            'Нужно, чтобы мобильная отправляла current-операции (списком с датами).'
          ]);
        }

        const limit = explicitLimit || 35;
        const shown = filtered.slice(0, limit);
        const total = filtered.reduce((s, x) => {
          let amt = _guessAmount(x.__row);
          if (wantKind === 'expense') amt = -Math.abs(Number(amt || 0));
          if (wantKind === 'income') amt = Math.abs(Number(amt || 0));
          return s + Number(amt || 0);
        }, 0);

        const title = (wantKind === 'expense') ? 'Расходы до сегодня'
          : (wantKind === 'income') ? 'Доходы до сегодня'
            : (wantKind === 'transfer') ? 'Переводы до сегодня'
              : 'Выводы до сегодня';

        const body = [];
        body.push(`До: ${snapTodayDDMMYYYY}`);
        body.push(`Найдено: ${filtered.length}`);
        body.push(`Итого: ${_formatTenge(total)}`);
        body.push('');
        shown.forEach(x => body.push(_opsFmtLine(x, wantKind)));
        if (filtered.length > shown.length) body.push(`…и ещё ${filtered.length - shown.length}`);

        return _wrapBlock(title, null, body);
      };

      const _looksLikeOpsByDay = (qLower, kind) => {
        const t = String(qLower || '').toLowerCase();
        const wantsByDay = /(по\s*дням|по\s*датам|за\s*дни|по\s*дня|по\s*дате)/.test(t);
        if (!wantsByDay) return false;

        if (kind === 'expense') return /(расход|тра(т|чу)|потрат|списан|платеж|платёж|оплат)/.test(t);
        if (kind === 'income') return /(доход|выруч|поступл|поступ)/.test(t);
        if (kind === 'transfer') return /(перевод|трансфер)/.test(t);
        if (kind === 'withdrawal') return /(вывод|сняти|снять|withdraw)/.test(t);
        return false;
      };

      const _renderOpsByDay = (wantKind) => {
        const untilTs = _endOfToday().getTime();
        const all = _opsCollectRows();

        const filtered = all
          .map(x => ({ ...x, __kind: _opsGuessKind(x.__wk, x.__row) }))
          .filter(x => x.__kind === wantKind)
          .filter(x => x.__ts <= untilTs);

        if (!filtered.length) {
          const title = (wantKind === 'expense') ? 'Расходы по дням'
            : (wantKind === 'income') ? 'Доходы по дням'
              : (wantKind === 'transfer') ? 'Переводы по дням'
                : 'Выводы по дням';

          return _wrapBlock(title, null, [
            'На этом экране не вижу списка операций с датами за прошлые периоды.',
            'Нужно, чтобы мобильная отправляла current-операции (списком с датами).'
          ]);
        }

        // Group by day label (DD.MM.YYYY)
        const map = new Map();
        filtered.forEach((x) => {
          const r = x.__row;
          const dLabel = _fmtDateDDMMYYYY(r?.date || r?.dateIso || r?.dateYmd || r?.dateStr) || _fmtDateKZ(new Date(x.__ts));
          let amt = _guessAmount(r);
          if (wantKind === 'expense') amt = -Math.abs(Number(amt || 0));
          if (wantKind === 'income') amt = Math.abs(Number(amt || 0));
          if (!map.has(dLabel)) map.set(dLabel, { date: dLabel, count: 0, total: 0 });
          const cur = map.get(dLabel);
          cur.count += 1;
          cur.total += Number(amt || 0);
        });

        // Sort by date descending (use parser)
        const rows = Array.from(map.values()).map((x) => {
          const ts = _parseAnyDateToTs(x.date) || 0;
          return { ...x, ts };
        }).sort((a, b) => b.ts - a.ts);

        const title = (wantKind === 'expense') ? 'Расходы по дням'
          : (wantKind === 'income') ? 'Доходы по дням'
            : (wantKind === 'transfer') ? 'Переводы по дням'
              : 'Выводы по дням';

        const limit = explicitLimit || 40;
        const shown = rows.slice(0, limit);

        const body = [];
        body.push(`До: ${snapTodayDDMMYYYY}`);
        body.push(`Дней: ${rows.length}`);
        body.push('');

        shown.forEach((r) => {
          body.push(`${r.date} — ${_formatTenge(r.total)} (${r.count})`);
        });
        if (rows.length > shown.length) body.push(`…и ещё ${rows.length - shown.length}`);

        return _wrapBlock(title, null, body);
      };


      // =========================
      // QUICK DIAGNOSTICS (snapshot-only)
      // =========================
      const _fmtYYYYFromTs = (ts) => {
        try {
          const d = new Date(Number(ts));
          if (Number.isNaN(d.getTime())) return '—';
          const x = new Date(d.getTime() + KZ_OFFSET_MS);
          const dd = String(x.getUTCDate()).padStart(2, '0');
          const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
          const yyyy = String(x.getUTCFullYear());
          return `${dd}.${mm}.${yyyy}`;
        } catch (_) {
          return '—';
        }
      };

      const _countRowsByKeys = (keys) => {
        const w = _findSnapWidget(keys);
        if (!w) return 0;
        const rows = _getRows(w);
        return Array.isArray(rows) ? rows.length : 0;
      };

      const _hasWidgetByKeys = (keys) => Boolean(_findSnapWidget(keys));

      const _sumCurrentFromOps = (kind) => {
        const endTs = _endOfToday().getTime();
        const all = _opsCollectRows();
        let total = 0;
        all.forEach((x) => {
          const k = _opsGuessKind(x.__wk, x.__row);
          if (k !== kind) return;
          if (Number(x.__ts) > endTs) return;
          let amt = _guessAmount(x.__row);
          if (kind === 'expense') amt = -Math.abs(Number(amt || 0));
          if (kind === 'income') amt = Math.abs(Number(amt || 0));
          total += Number(amt || 0);
        });
        return total;
      };

      const _sumFutureFromOps = (kind) => {
        const endTs = _endOfToday().getTime();
        const all = _opsCollectRows();
        let total = 0;
        all.forEach((x) => {
          const k = _opsGuessKind(x.__wk, x.__row);
          if (k !== kind) return;
          if (Number(x.__ts) <= endTs) return; // Only future operations
          let amt = _guessAmount(x.__row);
          if (kind === 'expense') amt = -Math.abs(Number(amt || 0));
          if (kind === 'income') amt = Math.abs(Number(amt || 0));
          total += Number(amt || 0);
        });
        return total;
      };

      // If we have a UI snapshot, answer STRICTLY from it and return early.
      if (snapWidgets) {
        // QUICK: diagnostics (deterministic)
        if (_isDiagnosticsQuery(qLower)) {
          return res.json({ text: _renderDiagnosticsFromSnapshot(uiSnapshot, snapTodayDDMMYYYY, snapFutureDDMMYYYY) });
        }

        // QUICK: Handle explicit current/future operation sum requests
        // Only trigger for very specific patterns to avoid intercepting general queries
        if (/^(текущие расходы|расходы текущие)$/i.test(qLower.trim())) {
          const total = _sumCurrentFromOps('expense');
          return res.json({ text: `Текущие расходы. До ${snapTodayDDMMYYYY}\n${_formatTenge(total)}` });
        }
        if (/^(текущие доходы|доходы текущие)$/i.test(qLower.trim())) {
          const total = _sumCurrentFromOps('income');
          return res.json({ text: `Текущие доходы. До ${snapTodayDDMMYYYY}\n${_formatTenge(total)}` });
        }
        if (/^(будущие расходы|расходы будущие|прогноз расходов|расходы прогноз)$/i.test(qLower.trim())) {
          const total = _sumFutureFromOps('expense');
          return res.json({ text: `Будущие расходы (прогноз). После ${snapTodayDDMMYYYY}\n${_formatTenge(total)}` });
        }
        if (/^(будущие доходы|доходы будущие|прогноз доходов|доходы прогноз)$/i.test(qLower.trim())) {
          const total = _sumFutureFromOps('income');
          return res.json({ text: `Будущие доходы (прогноз). После ${snapTodayDDMMYYYY}\n${_formatTenge(total)}` });
        }
        // =========================
        // HARD ROUTING: QUICK vs CHAT
        // QUICK -> only deterministic handlers
        // CHAT  -> only OpenAI (DATA + history)
        // =========================
        const isQuickFlag = (req?.body?.isQuickRequest === true) || (String(req?.body?.isQuickRequest || '').toLowerCase() === 'true');
        const qNorm2 = _normQ(qLower);
        const quickKey2 = (req?.body?.quickKey != null) ? String(req.body.quickKey) : '';

        const _resolveQuickIntent = (quickKey, qNorm) => {
          const k = String(quickKey || '').toLowerCase().trim();
          if (k) return k;

          // Skip QUICK mode for temporal queries (user wants current vs future filtering)
          const hasTemporalKeywords =
            qNorm.includes('будущ') ||
            qNorm.includes('текущ') ||
            qNorm.includes('прогноз') ||
            qNorm.includes('ближайш');

          if (hasTemporalKeywords) {
            // Let OpenAI handle temporal filtering
            return '';
          }

          // Skip QUICK mode for filtered queries (user wants filtering by category/project/contractor)
          const hasFilterKeywords =
            qNorm.includes('по категори') ||
            qNorm.includes('по проект') ||
            qNorm.includes('по контрагент') ||
            (qNorm.includes('категория') && (qNorm.includes('расход') || qNorm.includes('доход'))) ||
            qNorm.includes('сгруппир') ||
            qNorm.includes('группир');

          if (hasFilterKeywords) {
            // Let OpenAI handle filtering and grouping
            return '';
          }

          // Skip QUICK mode for analytical questions (user wants calculations, not lists)
          const isAnalyticalQuestion =
            qNorm.includes('сколько') ||
            qNorm.includes('сумма') ||
            qNorm.includes('итого') ||
            qNorm.includes('всего') ||
            qNorm.includes('расчет') ||
            qNorm.includes('посчита') ||
            qNorm.includes('вычисл');

          if (isAnalyticalQuestion) {
            // Let OpenAI handle analytical questions
            return '';
          }

          // Flexible keyword-based matching for LIST requests
          // Only trigger QUICK mode for explicit list requests
          if (qNorm.includes('покажи') || qNorm.includes('список') || qNorm.includes('выведи') || qNorm.includes('отобрази')) {
            if (qNorm.includes('счета') || qNorm.includes('счёт')) return 'accounts';
            if (qNorm.includes('доход')) return 'income';
            if (qNorm.includes('расход')) return 'expense';
            if (qNorm.includes('перевод')) return 'transfer';
            if (qNorm.includes('вывод')) return 'withdrawal';
            if (qNorm.includes('налог')) return 'taxes';
            if (qNorm.includes('проект')) return 'projects';
            if (qNorm.includes('контрагент')) return 'contractors';
            if (qNorm.includes('категори')) return 'categories';
            if (qNorm.includes('физлиц') || qNorm.includes('физ лиц')) return 'individuals';
            if (qNorm.includes('компани')) return 'companies';
            if (qNorm.includes('кредит')) return 'credits';
            if (qNorm.includes('предоплат')) return 'prepayments';
          }

          return '';
        };

        const quickIntent2 = _resolveQuickIntent(quickKey2, qNorm2);
        // Use QUICK mode if intent is recognized OR if explicit quick flag is set
        const isQuickRequest2 = Boolean(isQuickFlag || quickIntent2);

        // If we recognized a quick intent (like "покажи налоги"), use QUICK mode for consistent data
        if (quickIntent2 && !isQuickRequest2) {
          // This shouldn't happen with the logic above, but just in case
          isQuickRequest2 = true;
        }

        // CHAT: always OpenAI, no manual intent handlers (unless quick intent was recognized)
        if (!isQuickRequest2) {
          const text = await _openAiChatFromSnapshot(q);
          return res.json({ text });
        }

        // QUICK: regulated answer only (no clarifications, no emoji)
        if (quickIntent2) {
          const limitQ = explicitLimit || 50;

          if (quickIntent2 === 'accounts') {
            const w = _findSnapWidget('accounts');
            const rows = _getRows(w);
            return res.json({ text: _renderAccountsBlock(w, rows) });
          }

          if (quickIntent2 === 'income') {
            return res.json({ text: _renderOpsList('income', 'current', { format: 'short', limit: limitQ, noHints: true }) });
          }
          if (quickIntent2 === 'expense') {
            return res.json({ text: _renderOpsList('expense', 'current', { format: 'short', limit: limitQ, noHints: true }) });
          }
          if (quickIntent2 === 'transfer') {
            return res.json({ text: _renderOpsList('transfer', 'current', { format: 'short', limit: limitQ, noHints: true }) });
          }
          if (quickIntent2 === 'withdrawal') {
            return res.json({ text: _renderOpsList('withdrawal', 'current', { format: 'short', limit: limitQ, noHints: true }) });
          }

          if (quickIntent2 === 'taxes') {
            // If the taxes widget contains per-company rows, render them.
            // Otherwise fallback to a single total (summary).
            const wTax = _findSnapWidget(['taxes', 'tax', 'taxList', 'taxesList']);
            if (wTax) {
              const rowsTax = _getRows(wTax);
              const hasNamedRows = Array.isArray(rowsTax)
                ? rowsTax.some(r => r && (r.companyName || r.company || r.companyTitle || r.name || r.title || r.label))
                : false;

              if (Array.isArray(rowsTax) && (rowsTax.length > 1 || hasNamedRows)) {
                const body = [];
                _maybeSlice(rowsTax, explicitLimit).forEach((r) => {
                  const name = r?.companyName || r?.company || r?.companyTitle || r?.name || r?.title || r?.label || '—';
                  const { fact, fut } = _pickFactFuture(r);
                  body.push(`${name} ₸ ${fact} > ${fut}`);
                });
                return res.json({ text: _wrapBlock('Налоги', wTax, body) });
              }
            }

            const blk = _summaryDual(['taxes', 'tax', 'taxList', 'taxesList'], 'Налоги');
            return res.json({ text: blk || 'Налоги: на этом экране не вижу виджет налогов.' });
          }

          if (quickIntent2 === 'projects') {
            const w = _findSnapWidget(['projects', 'projectList']);
            return res.json({ text: _renderCatalogFromRows('Мои проекты', _getRows(w)) });
          }
          if (quickIntent2 === 'contractors') {
            const w = _findSnapWidget(['contractors', 'contractorList', 'counterparties']);
            return res.json({ text: _renderCatalogFromRows('Мои контрагенты', _getRows(w)) });
          }
          if (quickIntent2 === 'categories') {
            const w = _findSnapWidget(['categories', 'categoryList']);
            return res.json({ text: _renderCatalogFromRows('Категории', _getRows(w)) });
          }
          if (quickIntent2 === 'individuals') {
            const w = _findSnapWidget(['individuals', 'individualList', 'persons', 'people']);
            return res.json({ text: _renderCatalogFromRows('Физлица', _getRows(w)) });
          }
          if (quickIntent2 === 'companies') {
            const w = _findSnapWidget(['companies', 'companyList']);
            return res.json({ text: _renderCatalogFromRows('Компании', _getRows(w)) });
          }

          return res.json({ text: 'QUICK: команда не поддерживается.' });
        }
        // ---- Debug: list visible snapshot widgets
        if (/(что\s*видишь|какие\s*виджеты|виджеты\s*$|snapshot\s*$|debug\s*$)/i.test(qLower)) {
          const list = (snapWidgets || []).map(w => ({ key: w?.key || null, title: w?.title || w?.name || null }));
          const lines = [`Вижу виджеты на экране: ${list.length}`];
          list.forEach((x, i) => lines.push(`${i + 1}) ${x.key || '—'}${x.title ? ` — ${x.title}` : ''}`));
          return res.json({ text: lines.join('\n') });
        }

        // ---- Reports: P&L / Cash Flow / Balance Sheet (must be checked BEFORE profitability-by-projects)
        if (_looksLikePnL(qLower)) {
          return res.json({ text: _renderPnLReport() });
        }
        if (_looksLikeCashFlow(qLower)) {
          return res.json({ text: _renderCashFlowReport() });
        }
        if (_looksLikeBalanceSheet(qLower)) {
          return res.json({ text: _renderBalanceSheetReport() });
        }

        // ---- Operations list (FACT): "до сегодня" / "до <дата>" (works in both quick + chat)
        if (_looksLikeOpsUntil(qLower, 'expense')) return res.json({ text: _renderOpsUntil('expense') });
        if (_looksLikeOpsUntil(qLower, 'income')) return res.json({ text: _renderOpsUntil('income') });
        if (_looksLikeOpsUntil(qLower, 'transfer')) return res.json({ text: _renderOpsUntil('transfer') });
        if (_looksLikeOpsUntil(qLower, 'withdrawal')) return res.json({ text: _renderOpsUntil('withdrawal') });

        // ---- Operations by day (FACT): "по дням" / "по датам"
        if (_looksLikeOpsByDay(qLower, 'expense')) return res.json({ text: _renderOpsByDay('expense') });
        if (_looksLikeOpsByDay(qLower, 'income')) return res.json({ text: _renderOpsByDay('income') });
        if (_looksLikeOpsByDay(qLower, 'transfer')) return res.json({ text: _renderOpsByDay('transfer') });
        if (_looksLikeOpsByDay(qLower, 'withdrawal')) return res.json({ text: _renderOpsByDay('withdrawal') });

        // ---- Operations by project (FACT): "по проектам"
        if (_looksLikeOpsByProject(qLower, 'expense')) return res.json({ text: _renderOpsByProject('expense') });
        if (_looksLikeOpsByProject(qLower, 'income')) return res.json({ text: _renderOpsByProject('income') });
        if (_looksLikeOpsByProject(qLower, 'transfer')) return res.json({ text: _renderOpsByProject('transfer') });
        if (_looksLikeOpsByProject(qLower, 'withdrawal')) return res.json({ text: _renderOpsByProject('withdrawal') });

        // ---- Profitability guard (works in BOTH quick + chat modes)
        // If user asks about profitability of projects, but the screen already has the Projects widget,
        // we answer from that widget instead of asking for income/expense details.
        const wantsProfitAny = /(прибыл|марж|рентаб|profit|margin|net)/i.test(qLower);
        if (wantsProfitAny && qLower.includes('проект')) {
          const wProj = _findSnapWidget(['projects', 'projectList']);
          if (wProj) {
            return res.json({
              text: _renderProfitByProjects(wProj, _getRows(wProj), 'Прибыль проектов (как в виджете "Мои проекты")')
            });
          }
        }

        // ---- HYBRID MODE: quick buttons vs live chat
        const reqSourceRaw = req?.body?.source ?? req?.body?.mode ?? '';
        const reqSource = String(reqSourceRaw || '').toLowerCase();
        const quickKey = (req?.body?.quickKey != null) ? String(req.body.quickKey) : '';

        const isExplicitQuick = (reqSource === 'quick_button' || reqSource === 'quick' || reqSource === 'button' || reqSource === 'btn' || req?.body?.mode === 'quick' || Boolean(quickKey));
        const isExplicitChat = (reqSource === 'chat' || reqSource === 'voice' || reqSource === 'mic' || req?.body?.mode === 'chat');

        // Back-compat for old clients: treat ONLY very short/standard phrases as quick.
        const qNorm = _normQ(qLower);
        const looksLikeQuickText = (
          qNorm === 'счета' || qNorm === 'счёт' || qNorm === 'покажи счета' || qNorm === 'покажи счёта' ||
          qNorm === 'доходы' || qNorm === 'покажи доходы' ||
          qNorm === 'расходы' || qNorm === 'покажи расходы' ||
          qNorm === 'переводы' || qNorm === 'покажи переводы' ||
          qNorm === 'выводы' || qNorm === 'покажи выводы' ||
          qNorm === 'налоги' || qNorm === 'покажи налоги' ||
          qNorm === 'проекты' || qNorm === 'покажи проекты' ||
          qNorm === 'контрагенты' || qNorm === 'покажи контрагентов' ||
          qNorm === 'категории' || qNorm === 'покажи категории' ||
          qNorm === 'физлица' || qNorm === 'покажи физлица'
        );

        const isQuickRequest = isExplicitQuick || (!isExplicitChat && looksLikeQuickText);

        // CHAT MODE branch (variative answers) — ONLY from snapshot
        if (!isQuickRequest) {
          const baseTs = _kzStartOfDay(new Date()).getTime();

          // ---- Session-aware clarification (one short question)
          const sess = _getChatSession(userIdStr);
          if (sess && sess.pending && sess.pending.type === 'pick_scope') {
            const kind = sess.pending.kind;
            const scopePicked = _detectScopeFromText(qLower);
            if (!scopePicked) {
              return res.json({ text: _renderScopeQuestion(kind, sess.pending.counts) });
            }
            // Save preference and continue with the original pending action
            if (kind === 'income') sess.prefs.incomeScope = scopePicked;
            if (kind === 'expense') sess.prefs.expenseScope = scopePicked;
            _clearPending(userIdStr);

            const formatPicked = _detectFormatFromText(qLower);
            sess.prefs.format = formatPicked;

            const limitPicked = _parseExplicitLimitFromQuery(qLower) || sess.prefs.limit || 50;
            sess.prefs.limit = limitPicked;

            return res.json({
              text: _renderOpsList(kind, scopePicked, { format: sess.prefs.format, limit: sess.prefs.limit })
            });
          }

          // ---- Lists: "список доходов/расходов" (short by default)
          const wantsListWord = /(список|перечень|list)/i.test(qLower);
          const wantsIncome = /(доход|выруч|поступл|поступ)/i.test(qLower);
          const wantsExpense = /(расход|тра(т|чу)|потрат|списан|платеж|платёж|оплат)/i.test(qLower);

          if (wantsListWord && (wantsIncome || wantsExpense)) {
            const kind = wantsIncome ? 'income' : 'expense';
            const scopeExplicit = _detectScopeFromText(qLower);
            const format = _detectFormatFromText(qLower);

            if (sess) sess.prefs.format = format;

            const counts = _opsCollectScopedCounts(kind);
            const hasCur = counts.curCount > 0;
            const hasFut = counts.futCount > 0;

            // If both exist and user didn't specify, ask once
            if (!scopeExplicit && hasCur && hasFut) {
              _setPending(userIdStr, { type: 'pick_scope', kind, counts });
              return res.json({ text: _renderScopeQuestion(kind, counts) });
            }

            // Pick scope: explicit -> saved pref -> available
            const pref = sess ? (kind === 'income' ? sess.prefs.incomeScope : sess.prefs.expenseScope) : null;
            const scope = scopeExplicit || pref || (hasCur ? 'current' : 'future');

            if (sess) {
              if (kind === 'income') sess.prefs.incomeScope = scope;
              if (kind === 'expense') sess.prefs.expenseScope = scope;
            }

            const limit = _parseExplicitLimitFromQuery(qLower) || (sess ? sess.prefs.limit : 50) || 50;
            if (sess) sess.prefs.limit = limit;

            return res.json({ text: _renderOpsList(kind, scope, { format, limit }) });
          }

          // Profit by projects
          // IMPORTANT: do NOT use a word-boundary here — "прибыльность" must match.
          const wantsProfit = /(прибыл\w*|марж\w*|рентаб\w*|profit|margin|net)/i.test(qLower);
          const mentionsProjects = /(\bпроект\w*\b|по\s+проектам)/i.test(qLower);
          const projectsWidgetForProfit = _findSnapWidget(['projects', 'projectList']);

          if (wantsProfit && (mentionsProjects || Boolean(projectsWidgetForProfit))) {
            if (!projectsWidgetForProfit) {
              return res.json({
                text: [
                  'Прибыль проектов:',
                  'Не вижу на экране виджет "Мои проекты".',
                  'Открой главный экран с виджетом проектов и повтори вопрос.'
                ].join('\n')
              });
            }
            const rows = _getRows(projectsWidgetForProfit);
            return res.json({ text: _renderProfitByProjects(projectsWidgetForProfit, rows) });
          }

          // Upcoming incomes / expenses
          if (/(ближайш|скоро|когда|по\s*дат|дата\s*каких|что\s*придет|что\s*придёт)/i.test(qLower) && /(доход|поступл|выруч)/i.test(qLower)) {
            return res.json({ text: _renderUpcoming('Ближайшие доходы', 'income', baseTs) });
          }
          if (/(ближайш|скоро|когда|по\s*дат|дата\s*каких|что\s*спишет|что\s*уйдет|что\s*уйдёт)/i.test(qLower) && /(расход|платеж|платёж|оплат)/i.test(qLower)) {
            return res.json({ text: _renderUpcoming('Ближайшие расходы', 'expense', baseTs) });
          }

          // If user asks "что улучшить" / analysis — let LLM reason from snapshot.
          if (/(улучш|оптимиз|что\s*делать|совет|рекомендац|анализ|проанализ)/i.test(qLower)) {
            const out = await _openAiChatFromSnapshot(q);
            return res.json({ text: out });
          }

          // Default: free-form chat answer from snapshot (LLM).
          const out = await _openAiChatFromSnapshot(q);
          return res.json({ text: out });
        }

        // ---- Catalog-only queries (numbered lists, no sums)
        if (qLower.includes('проект')) {
          const w = _findSnapWidget(['projects', 'projectList']);
          return res.json({ text: _renderDualFactForecastList('Мои проекты', w, _getRows(w)) });
        }
        if (/(компан|companies|company)/i.test(qLower)) {
          const w = _findSnapWidget(['companies', 'companyList', 'companiesList', 'myCompanies']);
          if (!w) {
            return res.json({ text: 'Компании: не вижу виджет "Мои компании" на экране.' });
          }
          return res.json({ text: _renderDualFactForecastList('Мои компании', w, _getRows(w)) });
        }
        if (qLower.includes('контрагент')) {
          const w = _findSnapWidget(['contractors', 'contractorList']);
          return res.json({ text: _renderDualFactForecastList('Мои контрагенты', w, _getRows(w)) });
        }
        if (qLower.includes('категор')) {
          const w = _findSnapWidget(['categories', 'categoryList']);
          return res.json({ text: _renderDualFactForecastList('Категории', w, _getRows(w)) });
        }
        if (_isIndividualsQuery(qLower)) {
          const w = _findSnapWidget(['individuals', 'persons', 'individualList']);
          return res.json({ text: _renderDualFactForecastList('Физлица', w, _getRows(w)) });
        }

        // ---- Totals on accounts (unified block style)
        if (/(всего|итого)/i.test(qLower) && /(счет|счёт|баланс)/i.test(qLower)) {
          const acc = _findSnapWidget('accounts');
          const rows = _getRows(acc);
          const includeExcludedInTotal = Boolean(uiSnapshot?.ui?.includeExcludedInTotal);

          const factTotal = Array.isArray(rows)
            ? rows.reduce((s, r) => {
              if (!includeExcludedInTotal && r?.isExcluded) return s;
              return s + (Number(r?.balance ?? r?.currentBalance ?? r?.factBalance) || 0);
            }, 0)
            : 0;

          const futTotal = Array.isArray(rows)
            ? rows.reduce((s, r) => {
              if (!includeExcludedInTotal && r?.isExcluded) return s;
              return s + (Number(r?.futureBalance ?? r?.planBalance) || 0);
            }, 0)
            : 0;

          return res.json({ text: _renderDualValueBlock('Всего на счетах', acc || null, factTotal, futTotal) });
        }

        // ---- Accounts list (unified block style)
        if (qLower.includes('счет') || qLower.includes('счёт') || qLower.includes('баланс')) {
          const w = _findSnapWidget('accounts');
          if (!w) {
            return res.json({ text: 'Счета: не вижу виджет "Счета/Кассы" на экране.' });
          }
          const rows = _getRows(w);
          return res.json({ text: _renderAccountsBlock(w, rows) });
        }

        // ---- Summary widgets (unified block style)
        const incomeBlock = _summaryDual(['incomeList', 'income', 'incomeSummary'], 'Доходы');
        if (incomeBlock && /(доход|выруч|поступл|поступ)/i.test(qLower)) return res.json({ text: incomeBlock });

        const expenseBlock = _summaryDual(['expenseList', 'expense', 'expenseSummary'], 'Расходы');
        if (expenseBlock && /(расход|тра(т|чу)|потрат|списан)/i.test(qLower)) return res.json({ text: expenseBlock });

        const transfersBlock = _summaryDual(['transfers', 'transferList'], 'Переводы');
        if (transfersBlock && /(перевод|трансфер)/i.test(qLower)) return res.json({ text: transfersBlock });

        const withdrawalsBlock = _summaryDual(['withdrawalList', 'withdrawals', 'withdrawalsList'], 'Выводы');
        if (withdrawalsBlock && /(вывод|выводы|сняти|снять|withdraw)/i.test(qLower)) return res.json({ text: withdrawalsBlock });

        // ---- Taxes ("Мои налоги")
        if (/(налог|налоги|tax)/i.test(qLower)) {
          const w = _findSnapWidget(['taxes', 'tax', 'taxList', 'taxesList']);
          if (!w) {
            return res.json({ text: 'Налоги: не вижу виджет "Мои налоги" на экране.' });
          }

          const rows = _getRows(w);
          const body = [];

          _maybeSlice(rows, explicitLimit).forEach((r) => {
            const name = r?.name || r?.label || '—';
            const { fact, fut } = _pickFactFuture({
              ...r,
              currentText: r?.currentText ?? r?.factText,
              futureText: r?.futureText ?? r?.planText ?? r?.futureDeltaText,
            });
            body.push(`${name} ₸ ${fact} > ${fut}`);
          });

          // Totals if present
          const factTotRaw = (w?.totals?.totalCurrentDebt ?? w?.totals?.totalCurrent ?? w?.totals?.currentTotal ?? null);
          const futTotRaw = (w?.totals?.totalFutureDebt ?? w?.totals?.totalFuture ?? w?.totals?.totalPlan ?? w?.totals?.futureTotal ?? null);
          if (factTotRaw != null || futTotRaw != null) {
            const factTot = factTotRaw != null ? (-Math.abs(Number(factTotRaw) || 0)) : 0;
            const futTot = futTotRaw != null ? (-Math.abs(Number(futTotRaw) || 0)) : 0;
            body.push(`Итого ₸ ${_fmtMoneyInline(factTot)} > ${_fmtMoneyInline(futTot)}`);
          }

          return res.json({ text: _wrapBlock('Мои налоги', w, body) });
        }

        // ---- Prepayments / Liabilities ("Мои предоплаты")
        if (/(предоплат|аванс|предоплаты|liabilit|prepay)/i.test(qLower)) {
          const w = _findSnapWidget(['liabilities', 'prepayments', 'prepaymentList']);
          if (!w) {
            return res.json({ text: 'Предоплаты: не вижу виджет "Мои предоплаты" на экране.' });
          }

          const rows = _getRows(w);
          const body = [];

          _maybeSlice(rows, explicitLimit).forEach((r) => {
            const label = r?.label || r?.name || '—';
            const { fact, fut } = _pickFactFuture({
              ...r,
              currentText: r?.currentText ?? r?.factText,
              futureText: r?.futureText ?? r?.planText,
            });
            body.push(`${label} ₸ ${fact} > ${fut}`);
          });

          return res.json({ text: _wrapBlock('Мои предоплаты', w, body) });
        }

        // ---- Credits ("Мои кредиты")
        if (/(кредит|кредиты|долг|обязательств)/i.test(qLower)) {
          const w = _findSnapWidget(['credits', 'credit', 'creditList']);
          if (!w) {
            return res.json({ text: 'Кредиты: не вижу виджет "Мои кредиты" на экране.' });
          }

          const rows = _getRows(w);
          const body = [];

          _maybeSlice(rows, explicitLimit).forEach((r) => {
            const name = r?.name || r?.label || '—';
            const { fact, fut } = _pickFactFuture({
              ...r,
              currentText: r?.currentText ?? r?.factText,
              futureText: r?.futureText ?? r?.planText,
            });
            body.push(`${name} ₸ ${fact} > ${fut}`);
          });

          return res.json({ text: _wrapBlock('Мои кредиты', w, body) });
        }


        // ---- Fallback: short, snapshot-only answer
        const hint = [
          'Не вижу на экране данных для этого запроса.',
          `Могу по экрану: счета, всего на счетах, доходы, расходы, переводы, выводы, налоги, предоплаты, кредиты, проекты, компании, контрагенты, категории, физлица. (версия: ${AIROUTES_VERSION})`
        ].join('\n');

        return res.json({ text: hint });
      }

      // No uiSnapshot => NO MONGO.
      return res.status(400).json({ message: 'uiSnapshot is required (no-DB mode)' });

      // Legacy Mongo-based path below is kept for reference only. In no-DB mode it must not run.
      const now = _pickFactAsOf(req, aiContext);

      const range = await _resolveRangeFromQuery(userId, qLower, now);
      const rangeFrom = range.from;
      const rangeTo = range.to;

      const dbMaxEventDate = await _getUserMaxEventDate(userId);
      const serverBehind = dbMaxEventDate && (dbMaxEventDate.getTime() < _startOfDay(now).getTime());

      // ✅ includeHidden default TRUE; exclude only if explicit
      const includeHidden = (req?.body?.includeHidden === false)
        ? false
        : !(/\b(без\s*скры|только\s*(откры|видим))\b/i.test(qLower));

      const visibleAccountIdsRaw = Array.isArray(req?.body?.visibleAccountIds) ? req.body.visibleAccountIds : null;
      const visibleAccountIds = (visibleAccountIdsRaw || [])
        .map((id) => {
          try { return new mongoose.Types.ObjectId(String(id)); } catch (_) { return null; }
        })
        .filter(Boolean);

      const accountMatch = (!includeHidden && visibleAccountIds.length)
        ? { accountId: { $in: visibleAccountIds } }
        : {};

      const isShowVerb = /\b(покажи|показать|выведи|вывести|отобрази|сколько|сумм(а|у|ы)?|итог|итого|total|show)\b/i.test(qLower);
      const wantsFutureExplicit = /прогноз|будущ|ближайш|ожидаем|план|следующ|вперед|вперёд|после\s*сегодня/i.test(qLower);
      const useFuture = Boolean(wantsFutureExplicit || range?.scope === 'forecast' || (rangeTo && now && rangeTo.getTime() > now.getTime()));

      const _tomorrowStartFrom = (d) => {
        const s = _startOfDay(d);
        return new Date(s.getTime() + 24 * 60 * 60 * 1000);
      };

      const _pickRange = () => {
        const from = useFuture ? _tomorrowStartFrom(now) : rangeFrom;
        const to = useFuture ? rangeTo : now;
        return { from, to };
      };

      const asksDimension = /проект|категор|контрагент|физ\W*лиц|индивид|счет|счёт|баланс/i.test(qLower);

      // -------------------------
      // Catalogs (lists only, numbered)
      // If DB empty => fallback to aiContext.entities
      // -------------------------
      const _renderCatalog = (title, items) => {
        const arr = Array.isArray(items) ? items : [];
        if (!arr.length) return `${title}: 0`;
        const lines = [`${title}: ${arr.length}`];
        _maybeSlice(arr, explicitLimit).forEach((x, i) => lines.push(`${i + 1}) ${x?.name || x?.title || 'Без имени'}`));
        return lines.join('\n');
      };

      if (qLower.includes('проект')) {
        const dbRows = await Project.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Проекты', dbRows) });

        const fe = aiContext?.entities?.projects || [];
        return res.json({ text: _renderCatalog('Проекты', fe) });
      }

      if (qLower.includes('контрагент')) {
        const dbRows = await Contractor.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Контрагенты', dbRows) });

        const fe = aiContext?.entities?.contractors || [];
        return res.json({ text: _renderCatalog('Контрагенты', fe) });
      }

      if (qLower.includes('категор')) {
        const dbRows = await Category.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1, type: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Категории', dbRows) });

        const fe = aiContext?.entities?.categories || [];
        return res.json({ text: _renderCatalog('Категории', fe) });
      }

      if (_isIndividualsQuery(qLower)) {
        const dbRows = await Individual.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Физлица', dbRows) });

        const fe = aiContext?.entities?.individuals || [];
        return res.json({ text: _renderCatalog('Физлица', fe) });
      }

      // Prepayments catalog
      if ((/предоплат|аванс/i.test(qLower)) && _wantsCatalogOnly(qLower)) {
        const dbRows = await Prepayment.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1 } })
          .sort({ name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Предоплаты', dbRows) });

        const fe = aiContext?.entities?.prepayments || [];
        return res.json({ text: _renderCatalog('Предоплаты', fe) });
      }

      // -------------------------
      // Accounts (always include hidden by default; prefer aiContext snapshot)
      // -------------------------
      if (qLower.includes('счет') || qLower.includes('счёт') || qLower.includes('баланс')) {
        if (aiContext?.balances?.accountsCurrent || aiContext?.balances?.accountsFuture) {
          const scopeLabel = useFuture ? 'Прогноз' : 'Факт';
          const todayIso = aiContext?.meta?.today || null;
          const rangeEndIso = aiContext?.meta?.projection?.rangeEndDate || null;

          const scopeToIso = useFuture ? (rangeEndIso || todayIso) : todayIso;
          const list = useFuture ? aiContext.balances.accountsFuture : aiContext.balances.accountsCurrent;
          const rows = Array.isArray(list) ? list : [];

          const activeRows = rows.filter(a => !a?.isExcluded);
          const hiddenRows = rows.filter(a => !!a?.isExcluded);

          const totalActive = activeRows.reduce((s, a) => s + Number(a?.balance || 0), 0);
          const totalHidden = hiddenRows.reduce((s, a) => s + Number(a?.balance || 0), 0);
          const totalAll = totalActive + totalHidden;

          const toLabel = scopeToIso ? String(scopeToIso).split('-').reverse().join('.') : _fmtDateKZ(now);

          const lines = [
            `Счета (${scopeLabel}). До ${toLabel}`,
          ];

          // list all accounts; no hidden filtering by default
          _maybeSlice(activeRows, explicitLimit).forEach(a => lines.push(`${a?.name || '—'}: ${_formatTenge(a?.balance || 0)}`));
          _maybeSlice(hiddenRows, explicitLimit).forEach(a => lines.push(`${a?.name || '—'} (скрыт): ${_formatTenge(a?.balance || 0)}`));

          lines.push(`Итого: ${_formatTenge(totalAll)}`);

          return res.json({ text: lines.join('\n') });
        }

        // DB fallback (minimal): list accounts and their balances by aggregation
        const to = now;
        const balancesMap = await Event.aggregate([
          { $match: { userId: new mongoose.Types.ObjectId(userId), date: { $lte: to }, excludeFromTotals: { $ne: true } } },
          {
            $project: {
              type: 1,
              absAmount: { $abs: "$amount" },
              isTransfer: 1,
              accountId: 1,
              fromAccountId: 1,
              toAccountId: 1,
            }
          },
          {
            $project: {
              impacts: {
                $cond: {
                  if: { $or: ["$isTransfer", { $eq: ["$type", "transfer"] }] },
                  then: [
                    { id: "$fromAccountId", val: { $multiply: ["$absAmount", -1] } },
                    { id: "$toAccountId", val: "$absAmount" }
                  ],
                  else: [
                    {
                      id: "$accountId",
                      val: {
                        $cond: [
                          { $eq: ["$type", "income"] },
                          "$absAmount",
                          { $multiply: ["$absAmount", -1] }
                        ]
                      }
                    }
                  ]
                }
              }
            }
          },
          { $unwind: "$impacts" },
          { $match: { "impacts.id": { $ne: null } } },
          { $group: { _id: "$impacts.id", total: { $sum: "$impacts.val" } } }
        ]);

        const map = new Map((balancesMap || []).map(x => [String(x._id), Number(x.total || 0)]));

        const accounts = await Account.find({ userId }).select('name isExcluded order').sort({ order: 1, name: 1 }).lean();
        const rows = accounts.map(a => ({ ...a, balance: map.get(String(a._id)) || 0 }));

        const totalAll = rows.reduce((s, a) => s + Number(a?.balance || 0), 0);

        const lines = [
          `Счета (Факт). До ${_fmtDateKZ(now)}`
        ];

        rows.forEach(a => lines.push(`${a?.name || '—'}${a?.isExcluded ? ' (скрыт)' : ''}: ${_formatTenge(a?.balance || 0)}`));
        lines.push(`Итого: ${_formatTenge(totalAll)}`);

        return res.json({ text: lines.join('\n') });
      }

      // -------------------------
      // Income / Expense / Transfers — unified as-of rule + date always
      // -------------------------
      const looksLikeIncome = /(доход|выруч|поступл|поступ)/i.test(qLower);
      const looksLikeExpense = /(расход|тра(т|чу)|потрат|списан)/i.test(qLower);
      const looksLikeTransfer = /(перевод|трансфер)/i.test(qLower);
      const looksLikeTaxes = /налог/i.test(qLower);
      const looksLikeWithdrawals = /(вывод|выводы|сняти|снять|withdraw)/i.test(qLower);
      const looksLikeCredits = /(кредит|кредиты|долг)/i.test(qLower);
      const looksLikePrepayments = /(предоплат|аванс)/i.test(qLower);

      if ((isShowVerb || qLower.trim() === 'доходы' || qLower.trim() === 'доход') && looksLikeIncome && !asksDimension && !looksLikeExpense && !looksLikeTransfer && !looksLikeTaxes) {
        const { from, to } = _pickRange();

        const totals = await _periodTotalsRange(userId, from, to, accountMatch);

        const dbCount = await _countEventsInRange(userId, from, to, { isTransfer: { $ne: true }, ...accountMatch });
        if ((dbCount === 0 || serverBehind) && aiContext?.totals?.income != null) {
          const inc = Number(aiContext?.totals?.income || 0);
          const net = Number(aiContext?.totals?.net ?? (Number(aiContext?.totals?.income || 0) - Number(aiContext?.totals?.expense || 0)));
          const feTo = _parseIsoYMDToKZEnd(aiContext?.meta?.today) || to;

          return res.json({
            text:
              `${_titleTo(useFuture ? 'Ожидаемые доходы' : 'Доходы', feTo)} ${_formatTenge(inc)}\n` +
              `${_titleTo('Чистый доход', feTo)} ${_formatTenge(net)}`
          });
        }

        return res.json({
          text:
            `${_titleTo(useFuture ? 'Ожидаемые доходы' : 'Доходы', to)} ${_formatTenge(totals.income)}\n` +
            `${_titleTo('Чистый доход', to)} ${_formatTenge(totals.net)}`
        });
      }

      if ((isShowVerb || qLower.trim() === 'расходы' || qLower.trim() === 'расход') && looksLikeExpense && !asksDimension && !looksLikeIncome && !looksLikeTransfer && !looksLikeTaxes) {
        const { from, to } = _pickRange();

        const totals = await _periodTotalsRange(userId, from, to, accountMatch);

        const dbCount = await _countEventsInRange(userId, from, to, { isTransfer: { $ne: true }, ...accountMatch });
        if ((dbCount === 0 || serverBehind) && aiContext?.totals?.expense != null) {
          const exp = Number(aiContext?.totals?.expense || 0);
          const net = Number(aiContext?.totals?.net ?? (Number(aiContext?.totals?.income || 0) - Number(aiContext?.totals?.expense || 0)));
          const feTo = _parseIsoYMDToKZEnd(aiContext?.meta?.today) || to;

          return res.json({
            text:
              `${_titleTo(useFuture ? 'Ожидаемые расходы' : 'Расходы', feTo)} ${_formatTenge(-Math.abs(exp))}\n` +
              `${_titleTo('Чистый доход', feTo)} ${_formatTenge(net)}`
          });
        }

        return res.json({
          text:
            `${_titleTo(useFuture ? 'Ожидаемые расходы' : 'Расходы', to)} ${_formatTenge(-Math.abs(totals.expense))}\n` +
            `${_titleTo('Чистый доход', to)} ${_formatTenge(totals.net)}`
        });
      }

      if ((isShowVerb || qLower.trim() === 'переводы' || qLower.trim() === 'перевод') && looksLikeTransfer && !asksDimension && !looksLikeIncome && !looksLikeExpense && !looksLikeTaxes) {
        const { from, to } = _pickRange();

        const rows = await Event.aggregate([
          {
            $match: {
              userId: new mongoose.Types.ObjectId(userId),
              date: { $gte: from, $lte: to },
              excludeFromTotals: { $ne: true },
              ...accountMatch,
              $or: [{ isTransfer: true }, { type: 'transfer' }]
            }
          },
          { $project: { absAmount: { $abs: '$amount' } } },
          { $group: { _id: null, total: { $sum: '$absAmount' } } }
        ]);

        const total = rows?.[0]?.total || 0;

        const dbCount = await _countEventsInRange(userId, from, to, { $or: [{ isTransfer: true }, { type: 'transfer' }], ...accountMatch });
        if ((dbCount === 0 || serverBehind) && aiContext?.totals?.transfers != null) {
          const t = Number(aiContext?.totals?.transfers || 0);
          const feTo = _parseIsoYMDToKZEnd(aiContext?.meta?.today) || to;
          return res.json({ text: `${_titleTo('Переводы', feTo)} ${_formatTenge(t)}` });
        }

        return res.json({ text: `${_titleTo('Переводы', to)} ${_formatTenge(total)}` });
      }

      if ((isShowVerb || qLower.trim() === 'налоги' || qLower.trim() === 'налог') && looksLikeTaxes && !asksDimension && !looksLikeIncome && !looksLikeExpense && !looksLikeTransfer) {
        const { from, to } = _pickRange();

        const pack = await _calcTaxesAccumulativeRange(userId, from, to, accountMatch);

        // Fallback to FE widget precomputed
        const fePack = aiContext?.computed?.taxesAccumulative || null;
        const useFe = ((serverBehind || !pack.items.length) && fePack && typeof fePack.totalTax !== 'undefined');

        const totalTax = useFe ? Number(fePack.totalTax || 0) : Number(pack.totalTax || 0);
        const items = useFe ? (fePack.items || []) : (pack.items || []);

        if (!/по\s*компан/i.test(qLower)) {
          return res.json({ text: `${_titleTo('Налоги', to)} ${_formatTenge(totalTax)}` });
        }

        const lines = [`${_titleTo('Налоги (по компаниям)', to)} ${_formatTenge(totalTax)}`];
        items.forEach(it => lines.push(`${it.companyName || 'Компания'}: ${_formatTenge(it.tax || 0)}`));
        return res.json({ text: lines.join('\n') });
      }

      if ((isShowVerb || qLower.trim() === 'выводы' || qLower.trim() === 'вывод') && looksLikeWithdrawals && !asksDimension) {
        const { from, to } = _pickRange();

        const rows = await Event.aggregate([
          {
            $match: {
              userId: new mongoose.Types.ObjectId(userId),
              date: { $gte: from, $lte: to },
              excludeFromTotals: { $ne: true },
              ...accountMatch,
              $or: [{ isWithdrawal: true }, { type: 'withdrawal' }]
            }
          },
          { $project: { absAmount: { $abs: '$amount' } } },
          { $group: { _id: null, total: { $sum: '$absAmount' } } }
        ]);
        const total = rows?.[0]?.total || 0;

        const dbCount = await _countEventsInRange(userId, from, to, { $or: [{ isWithdrawal: true }, { type: 'withdrawal' }], ...accountMatch });
        if ((dbCount === 0 || serverBehind) && aiContext?.totals?.withdrawals != null) {
          const w = Number(aiContext?.totals?.withdrawals || 0);
          const feTo = _parseIsoYMDToKZEnd(aiContext?.meta?.today) || to;
          return res.json({ text: `${_titleTo('Выводы', feTo)} ${_formatTenge(-Math.abs(w))}` });
        }

        return res.json({ text: `${_titleTo('Выводы', to)} ${_formatTenge(-Math.abs(total))}` });
      }

      if ((isShowVerb || qLower.trim() === 'кредиты' || qLower.trim() === 'кредит') && looksLikeCredits && !asksDimension) {
        const rows = await Credit.aggregate([
          { $match: { userId: new mongoose.Types.ObjectId(userId), isRepaid: { $ne: true } } },
          { $group: { _id: null, debt: { $sum: { $ifNull: ['$totalDebt', 0] } }, monthly: { $sum: { $ifNull: ['$monthlyPayment', 0] } } } }
        ]);

        let debt = rows?.[0]?.debt || 0;
        let monthly = rows?.[0]?.monthly || 0;

        if (!rows.length && aiContext?.balances?.credits) {
          debt = Number(aiContext.balances.credits.debt || 0);
          monthly = Number(aiContext.balances.credits.monthly || 0);
        }

        if (/плат[её]ж|ежемесяч/i.test(qLower)) {
          return res.json({
            text:
              `${_titleTo('Кредиты (долг)', now)} ${_formatTenge(debt)}\n` +
              `${_titleTo('Кредиты (платеж)', now)} ${_formatTenge(monthly)}`
          });
        }
        return res.json({ text: `${_titleTo('Кредиты (долг)', now)} ${_formatTenge(debt)}` });
      }

      if ((isShowVerb || qLower.trim() === 'предоплаты' || qLower.trim() === 'предоплата') && looksLikePrepayments && !asksDimension && /сумм|итог|итого|сколько|оборот/i.test(qLower)) {
        const { from, to } = _pickRange();

        const rows = await Event.aggregate([
          {
            $match: {
              userId: new mongoose.Types.ObjectId(userId),
              date: { $gte: from, $lte: to },
              excludeFromTotals: { $ne: true },
              isTransfer: { $ne: true },
              type: { $in: ['income', 'expense'] },
              ...accountMatch,
              $or: [{ isPrepayment: true }, { prepaymentId: { $ne: null } }]
            }
          },
          { $project: { absAmount: { $abs: '$amount' } } },
          { $group: { _id: null, total: { $sum: '$absAmount' } } }
        ]);

        const total = rows?.[0]?.total || 0;

        const dbCount = await _countEventsInRange(userId, from, to, { isTransfer: { $ne: true }, ...accountMatch });
        if ((dbCount === 0 || serverBehind) && aiContext?.totals?.prepayments != null) {
          const p = Number(aiContext?.totals?.prepayments || 0);
          const feTo = _parseIsoYMDToKZEnd(aiContext?.meta?.today) || to;
          return res.json({ text: `${_titleTo('Предоплаты', feTo)} ${_formatTenge(p)}` });
        }

        return res.json({ text: `${_titleTo('Предоплаты', to)} ${_formatTenge(total)}` });
      }

      // -------------------------
      // Otherwise: use OpenAI (read-only) with aiContext summarized (short).
      // -------------------------
      const system = [
        'Ты финансовый ассистент INDEX12. Правила ответа:',
        '1) По умолчанию считай ФАКТ до сегодняшнего дня пользователя (Алматы/KZ).',
        '2) Всегда указывай дату: \"До DD.MM.YY\".',
        '3) Формат денег: разделение тысяч + \"₸\".',
        '4) Никакой лишней информации, только то, что спросили.',
        '5) Если данных в БД нет или БД отстает — используй aiContext с фронта.'
      ].join('\n');

      const contextBrief = aiContext ? JSON.stringify(aiContext).slice(0, 12000) : '';

      const messages = [
        { role: 'system', content: system },
        ...(contextBrief ? [{ role: 'system', content: `aiContext(JSON, truncated): ${contextBrief}` }] : []),
        { role: 'user', content: q }
      ];

      const text = await _openAiChat(messages, { temperature: 0.2, maxTokens: 260 });

      const out = String(text || '').replace(/\u00A0/g, ' ').trim();
      return res.json({ text: out });

    } catch (err) {
      return res.status(500).json({ message: err.message || 'AI error' });
    }
  });

  return router;
};

function _renderDiagnosticsFromSnapshot() {
  // Deterministic snapshot-only diagnostics (no OpenAI)
  const hasWidget = (keys) => Boolean(_findSnapWidget(keys));
  const rowsCount = (keys) => {
    const w = _findSnapWidget(keys);
    if (!w) return 0;
    const r = _getRows(w);
    return Array.isArray(r) ? r.length : 0;
  };

  // Presence flags
  const seen = {
    accounts: hasWidget('accounts'),
    incomes: hasWidget(['incomeListCurrent', 'incomeList', 'income', 'incomeSummary']),
    expenses: hasWidget(['expenseListCurrent', 'expenseList', 'expense', 'expenseSummary']),
    transfers: hasWidget(['transfersCurrent', 'transfers', 'transferList', 'transfersFuture']),
    withdrawals: hasWidget(['withdrawalListCurrent', 'withdrawalList', 'withdrawals', 'withdrawalsList', 'withdrawalListFuture']),
    taxes: hasWidget(['taxes', 'tax', 'taxList', 'taxesList']),
    credits: hasWidget(['credits', 'credit', 'creditList']),
    prepayments: hasWidget(['prepayments', 'prepaymentList', 'liabilities']),
    projects: hasWidget(['projects', 'projectList']),
    contractors: hasWidget(['contractors', 'contractorList', 'counterparties']),
    individuals: hasWidget(['individuals', 'individualList', 'persons', 'people']),
    categories: hasWidget(['categories', 'categoryList']),
    companies: hasWidget(['companies', 'companyList']),
  };

  // Accounts counts (incl hidden)
  let accountsTotal = 0;
  let accountsHidden = 0;
  if (seen.accounts) {
    const w = _findSnapWidget('accounts');
    const r = _getRows(w);
    accountsTotal = Array.isArray(r) ? r.length : 0;
    accountsHidden = Array.isArray(r) ? r.filter(x => Boolean(x?.isExcluded)).length : 0;
  }

  // Collect ops from widgets + storeTimeline
  const all = _opsCollectRows();
  const baseTs = _kzStartOfDay(new Date()).getTime();

  const cnt = { income: 0, expense: 0, transfer: 0, withdrawal: 0 };
  let minTs = null;
  let maxTs = null;

  (all || []).forEach((x) => {
    const ts = Number(x.__ts);
    if (!Number.isFinite(ts)) return;
    if (minTs === null || ts < minTs) minTs = ts;
    if (maxTs === null || ts > maxTs) maxTs = ts;

    const kind = _opsGuessKind(x.__wk, x.__row);
    if (kind && Object.prototype.hasOwnProperty.call(cnt, kind)) cnt[kind] += 1;
  });

  const opsTotal = cnt.income + cnt.expense + cnt.transfer + cnt.withdrawal;
  const minDate = (minTs != null) ? (_fmtDateDDMMYYYY(_fmtDateKZ(new Date(minTs))) || _fmtDateKZ(new Date(minTs))) : snapTodayDDMMYYYY;
  const maxDate = (maxTs != null) ? (_fmtDateDDMMYYYY(_fmtDateKZ(new Date(maxTs))) || _fmtDateKZ(new Date(maxTs))) : snapTodayDDMMYYYY;

  const widgetsList = (snapWidgets || []).map(w => w?.key).filter(Boolean);

  // Compact summary lines
  const lines = [];
  lines.push('Диагностика:');
  lines.push(`Факт: до ${snapTodayDDMMYYYY}`);
  lines.push(`Прогноз: до ${snapFutureDDMMYYYY}`);
  lines.push(`Виджетов: ${widgetsList.length}`);

  lines.push('Вижу:');
  lines.push(`Счета: ${seen.accounts ? 'да' : 'нет'} (${accountsTotal}${accountsHidden ? `, скрытых ${accountsHidden}` : ''})`);
  lines.push(`Доходы: ${seen.incomes ? 'да' : 'нет'} (строк ${rowsCount(['incomeListCurrent', 'incomeList', 'income'])})`);
  lines.push(`Расходы: ${seen.expenses ? 'да' : 'нет'} (строк ${rowsCount(['expenseListCurrent', 'expenseList', 'expense'])})`);
  lines.push(`Переводы: ${seen.transfers ? 'да' : 'нет'} (строк ${rowsCount(['transfersCurrent', 'transfers', 'transferList', 'transfersFuture'])})`);
  lines.push(`Выводы: ${seen.withdrawals ? 'да' : 'нет'} (строк ${rowsCount(['withdrawalListCurrent', 'withdrawalList', 'withdrawals', 'withdrawalsList', 'withdrawalListFuture'])})`);
  lines.push(`Налоги: ${seen.taxes ? 'да' : 'нет'} (строк ${rowsCount(['taxes', 'tax', 'taxList', 'taxesList'])})`);
  lines.push(`Кредиты: ${seen.credits ? 'да' : 'нет'} (строк ${rowsCount(['credits', 'credit', 'creditList'])})`);
  lines.push(`Предоплаты/обязательства: ${seen.prepayments ? 'да' : 'нет'} (строк ${rowsCount(['prepayments', 'prepaymentList', 'liabilities'])})`);

  lines.push(`Проекты: ${seen.projects ? 'да' : 'нет'} (${rowsCount(['projects', 'projectList'])})`);
  lines.push(`Контрагенты: ${seen.contractors ? 'да' : 'нет'} (${rowsCount(['contractors', 'contractorList', 'counterparties'])})`);
  lines.push(`Физлица: ${seen.individuals ? 'да' : 'нет'} (${rowsCount(['individuals', 'individualList', 'persons', 'people'])})`);
  lines.push(`Категории: ${seen.categories ? 'да' : 'нет'} (${rowsCount(['categories', 'categoryList'])})`);
  lines.push(`Компании: ${seen.companies ? 'да' : 'нет'} (${rowsCount(['companies', 'companyList'])})`);

  lines.push('Операции:');
  lines.push(`Диапазон: ${minDate} — ${maxDate}`);
  lines.push(`Всего: ${opsTotal}`);
  lines.push(`Доходы: ${cnt.income}`);
  lines.push(`Расходы: ${cnt.expense}`);
  lines.push(`Переводы: ${cnt.transfer}`);
  lines.push(`Выводы: ${cnt.withdrawal}`);

  // Show widget keys (trim)
  const keysShown = widgetsList.slice(0, 40);
  if (keysShown.length) {
    lines.push('Ключи виджетов:');
    lines.push(keysShown.join(', '));
    if (widgetsList.length > keysShown.length) lines.push(`…и ещё ${widgetsList.length - keysShown.length}`);
  }

  return lines.join('\n');
}
