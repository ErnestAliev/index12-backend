// backend/ai/utils/snapshotIntentParser.js
// Deterministic intent parser for snapshot-first AI pipeline.

const MONTHS_RU = [
  { key: 1, re: /январ/i },
  { key: 2, re: /феврал/i },
  { key: 3, re: /март/i },
  { key: 4, re: /апрел/i },
  { key: 5, re: /ма[йя]/i },
  { key: 6, re: /июн/i },
  { key: 7, re: /июл/i },
  { key: 8, re: /август/i },
  { key: 9, re: /сентябр/i },
  { key: 10, re: /октябр/i },
  { key: 11, re: /ноябр/i },
  { key: 12, re: /декабр/i }
];

const toIsoDateKey = (date) => {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '';
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
};

const fromIsoDateKey = (dateKey) => {
  const m = String(dateKey || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 12, 0, 0, 0);
  return Number.isNaN(d.getTime()) ? null : d;
};

const shiftDateKey = (dateKey, deltaDays) => {
  const date = fromIsoDateKey(dateKey);
  if (!date) return '';
  date.setDate(date.getDate() + Number(deltaDays || 0));
  return toIsoDateKey(date);
};

const normalizeText = (text) => String(text || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/\s+/g, ' ')
  .trim();

const parseMonthWord = (text) => {
  for (const month of MONTHS_RU) {
    if (month.re.test(text)) return month.key;
  }
  return null;
};

const parseDateKeyFromQuestion = ({ question, timelineDateKey, fallbackYear }) => {
  const text = String(question || '');
  const norm = normalizeText(text);

  const isoMatch = text.match(/\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b/);
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
  }

  const dmyMatch = text.match(/\b([0-2]?\d|3[01])[./]([0]?\d|1[0-2])[./](\d{2}|\d{4})\b/);
  if (dmyMatch) {
    const dd = String(Number(dmyMatch[1])).padStart(2, '0');
    const mm = String(Number(dmyMatch[2])).padStart(2, '0');
    const yyRaw = Number(dmyMatch[3]);
    const yyyy = dmyMatch[3].length === 2 ? (yyRaw >= 70 ? 1900 + yyRaw : 2000 + yyRaw) : yyRaw;
    return `${yyyy}-${mm}-${dd}`;
  }

  const monthKey = parseMonthWord(norm);
  if (monthKey) {
    const verbalMatch = norm.match(/\b([0-2]?\d|3[01])\s+[а-яa-z]+\s*(\d{4})?\b/i);
    if (verbalMatch) {
      const dd = String(Number(verbalMatch[1])).padStart(2, '0');
      const yyyy = verbalMatch[2]
        ? Number(verbalMatch[2])
        : Number(String(timelineDateKey || '').slice(0, 4) || fallbackYear || new Date().getFullYear());
      const mm = String(monthKey).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}`;
    }
  }

  const baseDateKey = (() => {
    if (timelineDateKey && /^\d{4}-\d{2}-\d{2}$/.test(timelineDateKey)) return timelineDateKey;
    return toIsoDateKey(new Date());
  })();

  if (/(\bсегодня\b|на сегодня)/i.test(norm)) return baseDateKey;
  if (/\bвчера\b/i.test(norm)) return shiftDateKey(baseDateKey, -1);
  if (/\bпозавчера\b/i.test(norm)) return shiftDateKey(baseDateKey, -2);

  return null;
};

const parseTargetMonth = ({ question, timelineDateKey }) => {
  const text = String(question || '');
  const norm = normalizeText(text);

  const isoMonthMatch = text.match(/\b(20\d{2})[-/.](0?[1-9]|1[0-2])\b/);
  if (isoMonthMatch) {
    return {
      year: Number(isoMonthMatch[1]),
      month: Number(isoMonthMatch[2])
    };
  }

  const monthWord = parseMonthWord(norm);
  if (monthWord) {
    const yearMatch = norm.match(/\b(20\d{2})\b/);
    const timelineYear = Number(String(timelineDateKey || '').slice(0, 4) || new Date().getFullYear());
    return {
      year: yearMatch ? Number(yearMatch[1]) : timelineYear,
      month: monthWord
    };
  }

  if (/конец месяца|к концу месяца|на конец месяца/i.test(norm)) {
    const base = fromIsoDateKey(timelineDateKey) || new Date();
    return {
      year: base.getFullYear(),
      month: base.getMonth() + 1
    };
  }

  return null;
};

function parseSnapshotIntent({ question, timelineDateKey = null, snapshot = null }) {
  const q = String(question || '').trim();
  const norm = normalizeText(q);

  const dateKey = parseDateKeyFromQuestion({
    question: q,
    timelineDateKey,
    fallbackYear: Number(String(snapshot?.range?.startDateKey || '').slice(0, 4) || new Date().getFullYear())
  });

  const targetMonth = parseTargetMonth({ question: q, timelineDateKey });

  const insightsRe = /(как cfo|как финансов|что заметить|риски|интерпрет|проанализ|анализ|вывод|стратег)/i;
  const upcomingRe = /(ближайш[^\n]*операц|какие[^\n]*операц[^\n]*когда|что[^\n]*впереди)/i;
  const forecastRe = /(прогноз|сделай\s+прогноз|кон(ец|цу)[^\n]*месяц)/i;
  const openRe = /(открыт[^\n]*счет|на открытых счетах)/i;
  const balanceRe = /(сколько[^\n]*денег|что было|баланс|сколько было)/i;

  if (upcomingRe.test(norm)) {
    return {
      type: 'UPCOMING_OPS',
      dateKey: dateKey || timelineDateKey || null,
      targetMonth: null,
      numeric: true,
      needsLlm: false
    };
  }

  if (forecastRe.test(norm)) {
    return {
      type: 'FORECAST_OPEN_END_OF_MONTH',
      dateKey: null,
      targetMonth,
      numeric: true,
      needsLlm: false
    };
  }

  if (openRe.test(norm) && (dateKey || /сегодня|вчера|позавчера/i.test(norm))) {
    return {
      type: 'OPEN_BALANCES_ON_DATE',
      dateKey: dateKey || timelineDateKey || null,
      targetMonth: null,
      numeric: true,
      needsLlm: false
    };
  }

  if (balanceRe.test(norm) && (dateKey || /сегодня|вчера|позавчера/i.test(norm))) {
    return {
      type: 'BALANCE_ON_DATE',
      dateKey: dateKey || timelineDateKey || null,
      targetMonth: null,
      numeric: true,
      needsLlm: false
    };
  }

  if (openRe.test(norm) && !dateKey) {
    return {
      type: 'OPEN_BALANCES_ON_DATE',
      dateKey: timelineDateKey || null,
      targetMonth: null,
      numeric: true,
      needsLlm: false
    };
  }

  if (insightsRe.test(norm)) {
    return {
      type: 'INSIGHTS',
      dateKey: dateKey || timelineDateKey || null,
      targetMonth,
      numeric: false,
      needsLlm: true
    };
  }

  return {
    type: 'INSIGHTS',
    dateKey: dateKey || timelineDateKey || null,
    targetMonth,
    numeric: false,
    needsLlm: true
  };
}

module.exports = {
  parseSnapshotIntent,
  parseDateKeyFromQuestion,
  parseTargetMonth
};
