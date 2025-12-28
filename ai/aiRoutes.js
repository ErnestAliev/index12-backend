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
const AIROUTES_VERSION = 'snapshot-ui-v3.4';
const https = require('https');

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
                } catch (_) {}
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
      req2.setTimeout(timeoutMs, () => { try { req2.destroy(new Error(`OpenAI timeout after ${timeoutMs}ms`)); } catch (_) {} });
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
      const explicitLimit = _parseExplicitLimitFromQuery(qLower);

      const aiContext = (req.body && req.body.aiContext) ? req.body.aiContext : null;


      // =========================
      // UI SNAPSHOT MODE (NO MONGO)
      // =========================
      const uiSnapshot = (req.body && req.body.uiSnapshot) ? req.body.uiSnapshot : null;
      const snapWidgets = Array.isArray(uiSnapshot?.widgets) ? uiSnapshot.widgets : null;

      const snapTodayTitleStr = String(uiSnapshot?.meta?.todayStr || _fmtDateKZ(_endOfToday()));
      const snapFutureTitleStr = String(uiSnapshot?.meta?.futureUntilStr || snapTodayTitleStr);

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
const wantsFutureSnap = /прогноз|будущ|ближайш|ожидаем|план|следующ|вперед|вперёд|после\s*сегодня/i.test(qLower);

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
          lines.push(`${i + 1}) ${name}`);
        });
        return lines.join('\n');
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

        if (widget && widget.showFutureBalance !== true) {
          lines.push('Совет: Если хотите увидеть прогноз — включите прогноз в виджете.');
        }

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

        if (widget && widget.showFutureBalance !== true) {
          lines.push('Совет: Если хотите увидеть прогноз — включите прогноз в виджете.');
        }

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
          factTotal += (Number(r?.balance ?? r?.currentBalance ?? r?.factBalance) || 0);
          futTotal += (Number(r?.futureBalance ?? r?.planBalance) || 0);
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


      // If we have a UI snapshot, answer STRICTLY from it and return early.
      if (snapWidgets) {
        // ---- Debug: list visible snapshot widgets
        if (/(что\s*видишь|какие\s*виджеты|виджеты\s*$|snapshot\s*$|debug\s*$)/i.test(qLower)) {
          const list = (snapWidgets || []).map(w => ({ key: w?.key || null, title: w?.title || w?.name || null }));
          const lines = [`Вижу виджеты на экране: ${list.length}`];
          list.forEach((x, i) => lines.push(`${i + 1}) ${x.key || '—'}${x.title ? ` — ${x.title}` : ''}`));
          return res.json({ text: lines.join('\n') });
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
