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

      const snapTodayStr = String(uiSnapshot?.meta?.todayStr || _fmtDateKZ(_endOfToday()));
      const snapFutureStr = String(uiSnapshot?.meta?.futureUntilStr || snapTodayStr);

      const wantsFutureSnap = /прогноз|будущ|ближайш|ожидаем|план|следующ|вперед|вперёд|после\s*сегодня/i.test(qLower);

      const _snapTitleTo = (title, toStr) => `${title}. До ${toStr}`;
      const _findSnapWidget = (key) => (snapWidgets || []).find(w => w && w.key === key) || null;

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
        return [];
      };

      // If we have a UI snapshot, answer STRICTLY from it and return early.
      if (snapWidgets) {
        // ---- Catalog-only queries (numbered lists, no sums)
        if (qLower.includes('проект') && _wantsCatalogOnly(qLower)) {
          const w = _findSnapWidget('projects');
          return res.json({ text: _renderCatalogFromRows('Проекты', _getRows(w)) });
        }
        if (qLower.includes('контрагент') && _wantsCatalogOnly(qLower)) {
          const w = _findSnapWidget('contractors');
          return res.json({ text: _renderCatalogFromRows('Контрагенты', _getRows(w)) });
        }
        if (qLower.includes('категор') && _wantsCatalogOnly(qLower)) {
          const w = _findSnapWidget('categories');
          return res.json({ text: _renderCatalogFromRows('Категории', _getRows(w)) });
        }
        if (_isIndividualsQuery(qLower) && _wantsCatalogOnly(qLower)) {
          const w = _findSnapWidget('individuals');
          return res.json({ text: _renderCatalogFromRows('Физлица', _getRows(w)) });
        }

        // ---- Totals on accounts
        if (/(всего|итого)/i.test(qLower) && /(счет|счёт|баланс)/i.test(qLower)) {
          const w = wantsFutureSnap
            ? (_findSnapWidget('futureTotal') || _findSnapWidget('currentTotal'))
            : (_findSnapWidget('currentTotal') || _findSnapWidget('futureTotal'));

          if (w && typeof w.totalBalance !== 'undefined') {
            const toStr = wantsFutureSnap ? snapFutureStr : snapTodayStr;
            const title = wantsFutureSnap ? 'Всего на счетах (с учетом будущих)' : 'Всего на счетах';
            const warn = _warnForecastOff(w);
            const lines = [`${_snapTitleTo(title, toStr)} ${_formatTenge(w.totalBalance)}`];
            if (warn) lines.push(warn);
            return res.json({ text: lines.join('\n') });
          }
        }

        // ---- Accounts list
        if (qLower.includes('счет') || qLower.includes('счёт') || qLower.includes('баланс')) {
          const w = _findSnapWidget('accounts');
          if (!w) {
            return res.json({ text: 'Счета: не вижу виджет "Счета/Кассы" на экране.' });
          }

          const useFuture = Boolean(wantsFutureSnap);
          const toStr = useFuture ? snapFutureStr : snapTodayStr;

          const rows = _getRows(w);
          const includeExcludedInTotal = Boolean(uiSnapshot?.ui?.includeExcludedInTotal);

          const total = rows.reduce((s, r) => {
            if (!includeExcludedInTotal && r?.isExcluded) return s;
            const v = useFuture ? (Number(r?.futureBalance) || 0) : (Number(r?.balance) || 0);
            return s + v;
          }, 0);

          const lines = [`Счета (${useFuture ? 'Прогноз' : 'Факт'}). До ${toStr}`];

          _maybeSlice(rows, explicitLimit).forEach((r) => {
            const name = r?.name || '—';
            const hidden = r?.isExcluded ? ' (скрыт)' : '';
            const moneyText = useFuture
              ? (r?.futureText || _formatTenge(r?.futureBalance || 0))
              : (r?.balanceText || _formatTenge(r?.balance || 0));
            lines.push(`${name}${hidden}: ${moneyText}`);
          });

          lines.push(`Итого: ${_formatTenge(total)}`);

          const warn = _warnForecastOff(w);
          if (warn) lines.push(warn);

          return res.json({ text: lines.join('\n') });
        }

        // ---- Summary widgets: incomes / expenses / transfers / withdrawals
        const _summaryOut = (key, titleFact, titleFuture) => {
          const w = _findSnapWidget(key);
          const rows = _getRows(w);
          if (!rows.length) return null;

          const row = rows[0];
          const useFuture = Boolean(wantsFutureSnap);
          const toStr = useFuture ? snapFutureStr : snapTodayStr;
          const title = useFuture ? titleFuture : titleFact;

          // Try to use prepared text from widget; fallback to numbers
          const valText = useFuture
            ? (row.futureText ?? row.currentText ?? _formatTenge(row.future ?? row.futureBalance ?? row.value ?? 0))
            : (row.currentText ?? _formatTenge(row.current ?? row.balance ?? row.value ?? 0));

          const lines = [`${_snapTitleTo(title, toStr)} ${String(valText || '').trim()}`];
          const warn = _warnForecastOff(w);
          if (warn) lines.push(warn);

          return lines.join('\n');
        };

        const incomeText = _summaryOut('incomeList', 'Доходы', 'Ожидаемые доходы');
        if (incomeText && /(доход|выруч|поступл|поступ)/i.test(qLower)) return res.json({ text: incomeText });

        const expenseText = _summaryOut('expenseList', 'Расходы', 'Ожидаемые расходы');
        if (expenseText && /(расход|тра(т|чу)|потрат|списан)/i.test(qLower)) return res.json({ text: expenseText });

        const transfersText = _summaryOut('transfers', 'Переводы', 'Переводы (прогноз)');
        if (transfersText && /(перевод|трансфер)/i.test(qLower)) return res.json({ text: transfersText });

        const withdrawalsText = _summaryOut('withdrawalList', 'Выводы', 'Выводы (прогноз)');
        if (withdrawalsText && /(вывод|выводы|сняти|снять|withdraw)/i.test(qLower)) return res.json({ text: withdrawalsText });

        // ---- Fallback: short, snapshot-only answer
        const hint = [
          'Не вижу на экране данных для этого запроса.',
          'Могу по экрану: счета, всего на счетах, доходы, расходы, переводы, выводы, проекты, контрагенты, категории, физлица.'
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

      if (qLower.includes('проект') && _wantsCatalogOnly(qLower)) {
        const dbRows = await Project.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Проекты', dbRows) });

        const fe = aiContext?.entities?.projects || [];
        return res.json({ text: _renderCatalog('Проекты', fe) });
      }

      if (qLower.includes('контрагент') && _wantsCatalogOnly(qLower)) {
        const dbRows = await Contractor.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Контрагенты', dbRows) });

        const fe = aiContext?.entities?.contractors || [];
        return res.json({ text: _renderCatalog('Контрагенты', fe) });
      }

      if (qLower.includes('категор') && _wantsCatalogOnly(qLower)) {
        const dbRows = await Category.collection
          .find({ $or: [{ userId: userObjId }, { userId: userIdStr }] }, { projection: { name: 1, order: 1, type: 1 } })
          .sort({ order: 1, name: 1 })
          .toArray();

        if (dbRows.length) return res.json({ text: _renderCatalog('Категории', dbRows) });

        const fe = aiContext?.entities?.categories || [];
        return res.json({ text: _renderCatalog('Категории', fe) });
      }

      if (_isIndividualsQuery(qLower) && _wantsCatalogOnly(qLower)) {
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
