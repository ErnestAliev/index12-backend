// backend/ai/utils/snapshotAnswerEngine.js
// Snapshot-first deterministic answer engine for chat branch.

const DATE_KEY_RE = /^\d{4}-\d{2}-\d{2}$/;

const MONTHS_RU_SHORT = [
  'янв.',
  'февр.',
  'мар.',
  'апр.',
  'мая',
  'июн.',
  'июл.',
  'авг.',
  'сент.',
  'окт.',
  'нояб.',
  'дек.'
];

const WEEKDAYS_RU_SHORT = ['вс', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб'];
const MONTHS_RU_PARSE = [
  { month: 1, re: /январ/i },
  { month: 2, re: /феврал/i },
  { month: 3, re: /март/i },
  { month: 4, re: /апрел/i },
  { month: 5, re: /ма[йя]/i },
  { month: 6, re: /июн/i },
  { month: 7, re: /июл/i },
  { month: 8, re: /август/i },
  { month: 9, re: /сентябр/i },
  { month: 10, re: /октябр/i },
  { month: 11, re: /ноябр/i },
  { month: 12, re: /декабр/i }
];

const toNum = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const fmtAbsInt = (value) => {
  const n = Math.round(Math.abs(toNum(value)));
  try {
    return new Intl.NumberFormat('ru-RU').format(n).replace(/\u00A0/g, ' ');
  } catch (_) {
    return String(n);
  }
};

const fmtSignedT = (value) => {
  const n = toNum(value);
  const sign = n < 0 ? '-' : '+';
  return `${sign}${fmtAbsInt(n)} т`;
};

const fmtT = (value) => `${fmtAbsInt(value)} т`;

const fmtPercent = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return '0%';
  return `${Math.round(n)}%`;
};

const normalizeScope = (scope, fallback = 'all') => {
  const raw = String(scope || fallback || 'all').toLowerCase();
  if (raw === 'open' || raw === 'hidden' || raw === 'all') return raw;
  return 'all';
};

const scopeLabelRu = (scope) => {
  const s = normalizeScope(scope);
  if (s === 'open') return 'открытые счета';
  if (s === 'hidden') return 'скрытые счета';
  return 'все счета';
};

const filterBalancesByScope = (balances, scope) => {
  const s = normalizeScope(scope);
  if (s === 'open') return normalizeList(balances).filter((acc) => acc?.isOpen === true);
  if (s === 'hidden') return normalizeList(balances).filter((acc) => acc?.isOpen !== true);
  return normalizeList(balances);
};

const sumBalancesByScope = (day, scope) => {
  return filterBalancesByScope(day?.accountBalances, scope)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
};

const resolveTargetDay = ({ snapshot, intent, timelineDateKey }) => {
  if (intent?.targetMonth && Number.isFinite(Number(intent.targetMonth.year)) && Number.isFinite(Number(intent.targetMonth.month))) {
    const year = Number(intent.targetMonth.year);
    const month = Number(intent.targetMonth.month);
    const targetDayKey = endOfMonthDateKey(year, month);
    const targetDay = findDay(snapshot, targetDayKey);
    if (!targetDay) {
      const monthStart = startOfMonthDateKey(year, month);
      return {
        ok: false,
        text: `Нет dayKey ${targetDayKey} в snapshot; нужен диапазон ${monthStart} — ${targetDayKey}.`
      };
    }
    return { ok: true, day: targetDay };
  }

  if (DATE_KEY_RE.test(String(intent?.dateKey || ''))) {
    const byIntentDate = findDay(snapshot, intent.dateKey);
    if (byIntentDate) return { ok: true, day: byIntentDate };
  }

  if (DATE_KEY_RE.test(String(timelineDateKey || ''))) {
    const byTimeline = findDay(snapshot, timelineDateKey);
    if (byTimeline) return { ok: true, day: byTimeline };
  }

  const fallback = snapshot.days[snapshot.days.length - 1] || null;
  if (!fallback) {
    return { ok: false, text: 'В snapshot нет доступных дней для расчета.' };
  }
  return { ok: true, day: fallback };
};

const sumInflowsByScope = ({ snapshot, fromDateKey, toDateKey, scope, targetDay }) => {
  const effectiveScope = normalizeScope(scope, 'all');
  const targetNames = new Set(
    filterBalancesByScope(targetDay?.accountBalances, effectiveScope)
      .map((acc) => String(acc?.name || '').trim())
      .filter(Boolean)
  );

  const inRange = (dayKey) => {
    if (!DATE_KEY_RE.test(String(dayKey || ''))) return false;
    if (DATE_KEY_RE.test(String(fromDateKey || '')) && String(dayKey) <= String(fromDateKey)) return false;
    if (DATE_KEY_RE.test(String(toDateKey || '')) && String(dayKey) > String(toDateKey)) return false;
    return true;
  };

  return snapshot.days
    .filter((day) => inRange(day?.dateKey))
    .reduce((sum, day) => {
      return sum + normalizeList(day?.lists?.income).reduce((acc, item) => {
        const amount = Math.abs(toNum(item?.amount));
        if (effectiveScope === 'all') return acc + amount;
        const accName = String(item?.accName || '').trim();
        return targetNames.has(accName) ? (acc + amount) : acc;
      }, 0);
    }, 0);
};

const dateFromKey = (dateKey) => {
  const m = String(dateKey || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 12, 0, 0, 0);
  return Number.isNaN(d.getTime()) ? null : d;
};

const dateKeyFromDate = (date) => {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '';
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
};

const normalizeQuestionForNlp = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/\s+/g, ' ')
  .trim();

const addDaysToDateKey = (dateKey, deltaDays) => {
  const date = dateFromKey(dateKey);
  if (!date) return '';
  date.setDate(date.getDate() + Number(deltaDays || 0));
  return dateKeyFromDate(date);
};

const parseDmyToDateKey = ({ day, month, year, fallbackYear }) => {
  const d = Number(day);
  const m = Number(month);
  const yRaw = Number(year);
  const y = Number.isFinite(yRaw)
    ? (String(year).length === 2 ? (yRaw >= 70 ? 1900 + yRaw : 2000 + yRaw) : yRaw)
    : Number(fallbackYear);
  if (!Number.isFinite(d) || !Number.isFinite(m) || !Number.isFinite(y)) return '';
  if (d < 1 || d > 31 || m < 1 || m > 12) return '';

  const dt = new Date(y, m - 1, d, 12, 0, 0, 0);
  if (Number.isNaN(dt.getTime())) return '';
  if (dt.getFullYear() !== y || dt.getMonth() !== (m - 1) || dt.getDate() !== d) return '';
  return dateKeyFromDate(dt);
};

const resolveTimelineDateKeySafe = ({ timelineDateKey, snapshot }) => {
  if (DATE_KEY_RE.test(String(timelineDateKey || ''))) return String(timelineDateKey);
  const endKey = String(snapshot?.range?.endDateKey || '');
  if (DATE_KEY_RE.test(endKey)) return endKey;
  const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
  const lastDayKey = String(days[days.length - 1]?.dateKey || '');
  if (DATE_KEY_RE.test(lastDayKey)) return lastDayKey;
  return '';
};

const clampDateKeyRangeToSnapshot = ({ snapshot, startDateKey, endDateKey }) => {
  if (!DATE_KEY_RE.test(String(startDateKey || '')) || !DATE_KEY_RE.test(String(endDateKey || ''))) return null;

  const snapStart = String(snapshot?.range?.startDateKey || '');
  const snapEnd = String(snapshot?.range?.endDateKey || '');
  if (!DATE_KEY_RE.test(snapStart) || !DATE_KEY_RE.test(snapEnd)) return null;

  const clampedStart = startDateKey < snapStart ? snapStart : startDateKey;
  const clampedEnd = endDateKey > snapEnd ? snapEnd : endDateKey;
  if (clampedStart > clampedEnd) return null;

  return {
    startDateKey: clampedStart,
    endDateKey: clampedEnd,
    wasClamped: clampedStart !== startDateKey || clampedEnd !== endDateKey
  };
};

const resolveMonthFromQuestion = (normText) => {
  const text = String(normText || '');
  for (const item of MONTHS_RU_PARSE) {
    if (item.re.test(text)) return Number(item.month);
  }
  return null;
};

const resolveWeekOrderFromQuestion = (normText) => {
  const text = String(normText || '');
  if (/(первая|первую|1-?я|1-я)/i.test(text)) return 1;
  if (/(вторая|вторую|2-?я|2-я)/i.test(text)) return 2;
  if (/(третья|третью|3-?я|3-я)/i.test(text)) return 3;
  if (/(четвертая|четвертую|4-?я|4-я)/i.test(text)) return 4;
  if (/(пятая|пятую|5-?я|5-я)/i.test(text)) return 5;
  return null;
};

const resolveNthWeekRangeInMonth = ({ year, month, weekOrder }) => {
  const y = Number(year);
  const m = Number(month);
  const n = Number(weekOrder);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(n) || n < 1) return null;

  const monthStart = new Date(y, m - 1, 1, 12, 0, 0, 0);
  const monthEnd = new Date(y, m, 0, 12, 0, 0, 0);
  if (Number.isNaN(monthStart.getTime()) || Number.isNaN(monthEnd.getTime())) return null;

  const firstMonday = new Date(monthStart);
  const dayOfWeek = firstMonday.getDay(); // 0 sunday ... 6 saturday
  const shiftToMonday = dayOfWeek === 0 ? 1 : (dayOfWeek === 1 ? 0 : (8 - dayOfWeek));
  firstMonday.setDate(firstMonday.getDate() + shiftToMonday);

  const start = new Date(firstMonday);
  start.setDate(start.getDate() + ((n - 1) * 7));
  if (start.getTime() > monthEnd.getTime()) return null;

  const end = new Date(start);
  end.setDate(end.getDate() + 6);

  return {
    startDateKey: dateKeyFromDate(start),
    endDateKey: dateKeyFromDate(end),
    source: `nth_week_${n}_month`
  };
};

const resolvePeriodFromQuestion = ({ question, timelineDateKey, snapshot }) => {
  const norm = normalizeQuestionForNlp(question);
  if (!norm) return null;

  const baseKey = resolveTimelineDateKeySafe({ timelineDateKey, snapshot });
  const baseDate = dateFromKey(baseKey) || new Date();
  const baseYear = baseDate.getFullYear();
  const baseMonth = baseDate.getMonth() + 1;

  const isoRangeMatch = norm.match(/с\s*(20\d{2}-\d{2}-\d{2})\s*по\s*(20\d{2}-\d{2}-\d{2})/i);
  if (isoRangeMatch) {
    const startDateKey = String(isoRangeMatch[1]);
    const endDateKey = String(isoRangeMatch[2]);
    if (DATE_KEY_RE.test(startDateKey) && DATE_KEY_RE.test(endDateKey) && startDateKey <= endDateKey) {
      return { startDateKey, endDateKey, source: 'explicit_iso_range' };
    }
  }

  const dmyRangeMatch = norm.match(/с\s*(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\s*по\s*(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?/i);
  if (dmyRangeMatch) {
    const leftYear = dmyRangeMatch[3] || baseYear;
    const rightYear = dmyRangeMatch[6] || leftYear;
    const startDateKey = parseDmyToDateKey({
      day: dmyRangeMatch[1],
      month: dmyRangeMatch[2],
      year: leftYear,
      fallbackYear: baseYear
    });
    const endDateKey = parseDmyToDateKey({
      day: dmyRangeMatch[4],
      month: dmyRangeMatch[5],
      year: rightYear,
      fallbackYear: baseYear
    });
    if (DATE_KEY_RE.test(startDateKey) && DATE_KEY_RE.test(endDateKey) && startDateKey <= endDateKey) {
      return { startDateKey, endDateKey, source: 'explicit_dmy_range' };
    }
  }

  if (/позавчера/i.test(norm)) {
    const key = addDaysToDateKey(baseKey, -2);
    if (DATE_KEY_RE.test(key)) return { startDateKey: key, endDateKey: key, source: 'day_before_yesterday' };
  }

  if (/вчера/i.test(norm)) {
    const key = addDaysToDateKey(baseKey, -1);
    if (DATE_KEY_RE.test(key)) return { startDateKey: key, endDateKey: key, source: 'yesterday' };
  }

  if (/прошл(ый|ого)\s+месяц/i.test(norm)) {
    const prevMonthDate = new Date(baseYear, baseMonth - 2, 1, 12, 0, 0, 0);
    const y = prevMonthDate.getFullYear();
    const m = prevMonthDate.getMonth() + 1;
    return {
      startDateKey: startOfMonthDateKey(y, m),
      endDateKey: endOfMonthDateKey(y, m),
      source: 'previous_month'
    };
  }

  const weekOrder = resolveWeekOrderFromQuestion(norm);
  const asksWeek = /недел/i.test(norm);
  if (asksWeek && Number.isFinite(Number(weekOrder))) {
    const month = resolveMonthFromQuestion(norm) || baseMonth;
    const yearMatch = norm.match(/\b(20\d{2})\b/);
    const year = yearMatch ? Number(yearMatch[1]) : baseYear;
    const weekRange = resolveNthWeekRangeInMonth({
      year,
      month,
      weekOrder
    });
    if (weekRange) return weekRange;
  }

  return null;
};

const toRuDateLabel = (dateKey) => {
  const d = dateFromKey(dateKey);
  if (!d) return String(dateKey || '?');
  const wd = WEEKDAYS_RU_SHORT[d.getDay()];
  const dd = d.getDate();
  const mm = MONTHS_RU_SHORT[d.getMonth()];
  const yyyy = d.getFullYear();
  return `${wd}, ${dd} ${mm} ${yyyy} г.`;
};

const toRuDateShort = (dateKey) => {
  const d = dateFromKey(dateKey);
  if (!d) return String(dateKey || '?');
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${dd}.${mm}.${yyyy}`;
};

const endOfMonthDateKey = (year, month) => {
  if (!Number.isFinite(Number(year)) || !Number.isFinite(Number(month))) return '';
  const end = new Date(Number(year), Number(month), 0, 12, 0, 0, 0);
  return dateKeyFromDate(end);
};

const startOfMonthDateKey = (year, month) => {
  if (!Number.isFinite(Number(year)) || !Number.isFinite(Number(month))) return '';
  const start = new Date(Number(year), Number(month) - 1, 1, 12, 0, 0, 0);
  return dateKeyFromDate(start);
};

const normalizeList = (value) => (Array.isArray(value) ? value : []);

const normalizeCategoryKey = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^a-zа-я0-9]+/gi, '')
  .replace(/(.)\1+/g, '$1')
  .trim();

const categoryMatches = (left, right) => {
  const l = normalizeCategoryKey(left);
  const r = normalizeCategoryKey(right);
  if (!l || !r) return false;
  return l === r || l.includes(r) || r.includes(l);
};

const computeTotalsFromLists = (lists) => {
  const incomeList = normalizeList(lists?.income);
  const expenseList = normalizeList(lists?.expense);
  const withdrawalList = normalizeList(lists?.withdrawal);
  const transferList = normalizeList(lists?.transfer);

  const income = incomeList.reduce((sum, item) => {
    return sum + Math.abs(toNum(item?.amount));
  }, 0);

  const expenseBase = expenseList.reduce((sum, item) => {
    return sum + Math.abs(toNum(item?.amount));
  }, 0);

  const withdrawalExpense = withdrawalList.reduce((sum, item) => {
    return sum + Math.abs(toNum(item?.amount));
  }, 0);

  // Out-of-system transfers are treated as cash outflow in UI totals.
  const transferOutExpense = transferList.reduce((sum, item) => {
    return item?.isOutOfSystemTransfer ? (sum + Math.abs(toNum(item?.amount))) : sum;
  }, 0);

  return {
    income,
    expense: expenseBase + withdrawalExpense + transferOutExpense
  };
};

const normalizeDay = (day) => {
  const accountBalancesRaw = Array.isArray(day?.accountBalances) ? day.accountBalances : [];
  const accountBalances = accountBalancesRaw.map((acc) => ({
    accountId: String(acc?.accountId || acc?._id || ''),
    name: String(acc?.name || acc?.accName || 'Счет'),
    balance: toNum(acc?.balance),
    isOpen: acc?.isOpen === undefined ? !acc?.isExcluded : Boolean(acc?.isOpen),
  }));

  const lists = {
    income: normalizeList(day?.lists?.income),
    expense: normalizeList(day?.lists?.expense),
    withdrawal: normalizeList(day?.lists?.withdrawal),
    transfer: normalizeList(day?.lists?.transfer),
  };

  const totalsIncomeRaw = toNum(day?.totals?.income);
  const totalsExpenseRaw = toNum(day?.totals?.expense);
  const totalsFromLists = computeTotalsFromLists(lists);
  const hasAnyListItems = (
    normalizeList(lists?.income).length
    + normalizeList(lists?.expense).length
    + normalizeList(lists?.withdrawal).length
    + normalizeList(lists?.transfer).length
  ) > 0;
  const totalsIncome = (totalsIncomeRaw === 0 && hasAnyListItems)
    ? totalsFromLists.income
    : totalsIncomeRaw;
  const totalsExpense = (totalsExpenseRaw === 0 && hasAnyListItems)
    ? totalsFromLists.expense
    : totalsExpenseRaw;

  const fallbackTotal = accountBalances.reduce((sum, acc) => sum + toNum(acc.balance), 0);
  const totalBalanceRaw = Number(day?.totalBalance);
  const totalBalance = Number.isFinite(totalBalanceRaw) ? totalBalanceRaw : fallbackTotal;

  return {
    dateKey: String(day?.dateKey || ''),
    dateLabel: String(day?.dateLabel || '') || toRuDateLabel(String(day?.dateKey || '')),
    totalBalance,
    accountBalances,
    totals: {
      income: totalsIncome,
      expense: totalsExpense
    },
    lists
  };
};

function validateTooltipSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    return { ok: false, error: 'payload.tooltipSnapshot отсутствует или некорректен' };
  }

  if (Number(snapshot.schemaVersion) !== 1) {
    return { ok: false, error: 'schemaVersion должен быть равен 1' };
  }

  const daysRaw = Array.isArray(snapshot.days) ? snapshot.days : [];
  if (!daysRaw.length) {
    return { ok: false, error: 'days[] пустой' };
  }

  const badDay = daysRaw.find((day) => !DATE_KEY_RE.test(String(day?.dateKey || '')));
  if (badDay) {
    return { ok: false, error: `некорректный day.dateKey: ${String(badDay?.dateKey || '?')}` };
  }

  const days = daysRaw
    .map(normalizeDay)
    .sort((a, b) => String(a.dateKey).localeCompare(String(b.dateKey)));

  const startDateKey = String(snapshot?.range?.startDateKey || days[0]?.dateKey || '');
  const endDateKey = String(snapshot?.range?.endDateKey || days[days.length - 1]?.dateKey || '');

  if (!DATE_KEY_RE.test(startDateKey) || !DATE_KEY_RE.test(endDateKey)) {
    return { ok: false, error: 'range.startDateKey или range.endDateKey некорректны' };
  }

  const visibilityModeRaw = String(snapshot?.visibilityMode || 'all').toLowerCase();
  const visibilityMode = ['open', 'hidden', 'all'].includes(visibilityModeRaw)
    ? visibilityModeRaw
    : 'all';

  return {
    ok: true,
    snapshot: {
      schemaVersion: 1,
      range: {
        startDateKey,
        endDateKey
      },
      visibilityMode,
      days
    }
  };
}

const findDay = (snapshot, dateKey) => {
  const key = String(dateKey || '');
  if (!DATE_KEY_RE.test(key)) return null;
  return snapshot.days.find((day) => day.dateKey === key) || null;
};

const opCountForDay = (day) => {
  const lists = day?.lists || {};
  return normalizeList(lists.income).length
    + normalizeList(lists.expense).length
    + normalizeList(lists.withdrawal).length
    + normalizeList(lists.transfer).length;
};

const dayHasActivity = (day) => {
  if (!day) return false;
  if (toNum(day?.totals?.income) !== 0 || toNum(day?.totals?.expense) !== 0) return true;
  return opCountForDay(day) > 0;
};

const resolveFactPeriodDays = ({ snapshot, timelineDateKey, targetMonth }) => {
  const sortedDays = [...snapshot.days].sort((a, b) => String(a.dateKey).localeCompare(String(b.dateKey)));
  const asOfKey = DATE_KEY_RE.test(String(timelineDateKey || ''))
    ? String(timelineDateKey)
    : String(snapshot?.range?.endDateKey || sortedDays[sortedDays.length - 1]?.dateKey || '');

  if (!sortedDays.length || !DATE_KEY_RE.test(asOfKey)) {
    return { ok: false, error: 'Недостаточно данных для расчета фактического периода.' };
  }

  let periodDays = sortedDays;
  let periodStart = sortedDays[0].dateKey;
  let periodEnd = sortedDays[sortedDays.length - 1].dateKey;

  if (targetMonth && Number.isFinite(Number(targetMonth.year)) && Number.isFinite(Number(targetMonth.month))) {
    const monthStart = startOfMonthDateKey(Number(targetMonth.year), Number(targetMonth.month));
    const monthEnd = endOfMonthDateKey(Number(targetMonth.year), Number(targetMonth.month));
    periodDays = sortedDays.filter((day) => day.dateKey >= monthStart && day.dateKey <= monthEnd);
    if (!periodDays.length) {
      return {
        ok: false,
        error: `Нет данных за ${monthStart} — ${monthEnd} в snapshot.`
      };
    }
    periodStart = periodDays[0].dateKey;
    periodEnd = periodDays[periodDays.length - 1].dateKey;
  }

  const factEnd = asOfKey < periodStart
    ? ''
    : (asOfKey < periodEnd ? asOfKey : periodEnd);

  if (!factEnd) {
    return {
      ok: true,
      periodStart,
      periodEnd: periodStart,
      factEnd: null,
      days: []
    };
  }

  return {
    ok: true,
    periodStart,
    periodEnd: factEnd,
    factEnd,
    days: periodDays.filter((day) => day.dateKey <= factEnd)
  };
};

const renderCategoryFactByCategory = ({ snapshot, intent, timelineDateKey }) => {
  const metric = String(intent?.metric || 'income').toLowerCase() === 'expense' ? 'expense' : 'income';
  const categoryRaw = String(intent?.categoryRaw || '').trim();
  if (!categoryRaw) {
    return { ok: false, text: 'Не удалось определить категорию в запросе.' };
  }

  const period = resolveFactPeriodDays({
    snapshot,
    timelineDateKey,
    targetMonth: intent?.targetMonth || null
  });
  if (!period.ok) {
    return { ok: false, text: period.error };
  }

  const listKeys = metric === 'income' ? ['income'] : ['expense', 'withdrawal'];
  let total = 0;
  let opCount = 0;
  let categoryDisplay = categoryRaw;

  period.days.forEach((day) => {
    listKeys.forEach((key) => {
      normalizeList(day?.lists?.[key]).forEach((item) => {
        if (!categoryMatches(item?.catName, categoryRaw)) return;
        if (item?.catName && !categoryDisplay) categoryDisplay = String(item.catName);
        if (item?.catName && categoryDisplay === categoryRaw) categoryDisplay = String(item.catName);
        total += Math.abs(toNum(item?.amount));
        opCount += 1;
      });
    });
  });

  const startLabel = toRuDateShort(period.periodStart);
  const endLabel = toRuDateShort(period.periodEnd);
  const suffix = period.factEnd
    ? `за период ${startLabel} — ${endLabel}`
    : `за период ${startLabel} — ${startLabel}`;

  const text = `${metric === 'income' ? 'Факт доходов' : 'Факт расходов'} по категории "${categoryDisplay}" ${suffix}: ${fmtT(total)}. Операций: ${opCount}.`;

  return {
    ok: true,
    text,
    meta: {
      metric,
      category: categoryDisplay,
      periodStart: period.periodStart,
      periodEnd: period.periodEnd,
      total,
      opCount
    }
  };
};

const fmtListOperation = (item, kind) => {
  if (!item || typeof item !== 'object') return null;

  if (kind === 'transfer') {
    const amount = fmtT(toNum(item.amount));
    const from = String(item.fromAccName || '???');
    const to = String(item.toAccName || (item.isOutOfSystemTransfer ? 'Вне системы' : '???'));
    return `${amount}: ${from} → ${to}`;
  }

  const amountRaw = toNum(item.amount);
  const amount = fmtSignedT(
    kind === 'expense' || kind === 'withdrawal'
      ? -Math.abs(amountRaw)
      : Math.abs(amountRaw)
  );

  const acc = String(item.accName || '???');
  const cont = String(item.contName || '---');
  const proj = String(item.projName || '---');
  const cat = String(item.catName || 'Без категории');

  return `${amount} < ${acc} < ${cont} < ${proj} < ${cat}`;
};

const renderSection = (title, items, kind) => {
  const lines = [];
  const safeItems = normalizeList(items);
  if (!safeItems.length) return lines;

  lines.push('----------------');
  lines.push(title);

  safeItems.forEach((item) => {
    const row = fmtListOperation(item, kind);
    if (row) lines.push(row);
  });

  return lines;
};

const renderDayBlock = ({ day, onlyOpen = false, scope = null }) => {
  const dayLabel = String(day?.dateLabel || toRuDateLabel(day?.dateKey));

  const effectiveScope = normalizeScope(scope || (onlyOpen ? 'open' : 'all'));
  const openBalances = filterBalancesByScope(day?.accountBalances, 'open');
  const hiddenBalances = filterBalancesByScope(day?.accountBalances, 'hidden');
  const balances = effectiveScope === 'open'
    ? openBalances
    : (effectiveScope === 'hidden' ? hiddenBalances : [...openBalances, ...hiddenBalances]);

  const totalBalance = balances.reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const income = toNum(day?.totals?.income);
  const expense = toNum(day?.totals?.expense);

  const lines = [
    dayLabel,
    `Баланс общий: ${fmtT(totalBalance)}`,
    '----------------'
  ];

  if (effectiveScope === 'all') {
    lines.push('Остатки на открытых счетах:');
    if (!openBalances.length) lines.push('Нет открытых счетов.');
    else openBalances.forEach((acc) => {
      lines.push(`${String(acc?.name || 'Счет')} — ${fmtT(acc?.balance)}`);
    });

    lines.push('----------------');
    lines.push('Остатки на скрытых счетах:');
    if (!hiddenBalances.length) lines.push('Нет скрытых счетов.');
    else hiddenBalances.forEach((acc) => {
      lines.push(`${String(acc?.name || 'Счет')} — ${fmtT(acc?.balance)}`);
    });
  } else {
    lines.push(effectiveScope === 'open'
      ? 'Остатки на открытых счетах:'
      : 'Остатки на скрытых счетах:');
    if (!balances.length) {
      lines.push(`Нет ${effectiveScope === 'open' ? 'открытых' : 'скрытых'} счетов.`);
    } else {
      balances.forEach((acc) => {
        lines.push(`${String(acc?.name || 'Счет')} — ${fmtT(acc?.balance)}`);
      });
    }
  }

  lines.push('----------------');
  lines.push(`Доход: +${fmtT(income)}`);
  lines.push(`Расход: -${fmtT(expense)}`);

  lines.push(...renderSection('ДОХОДЫ', day?.lists?.income, 'income'));
  const expenseRows = [
    ...normalizeList(day?.lists?.expense),
    ...normalizeList(day?.lists?.withdrawal)
  ];
  lines.push(...renderSection('РАСХОДЫ', expenseRows, 'expense'));
  lines.push(...renderSection('ПЕРЕВОДЫ', day?.lists?.transfer, 'transfer'));

  return lines.join('\n');
};

const computeCategoryFlowsForDay = (day) => {
  const map = new Map();
  const ensure = (name) => {
    const key = String(name || 'Без категории');
    if (!map.has(key)) map.set(key, { income: 0, expense: 0 });
    return map.get(key);
  };

  normalizeList(day?.lists?.income).forEach((item) => {
    ensure(item?.catName).income += Math.abs(toNum(item?.amount));
  });

  normalizeList(day?.lists?.expense).forEach((item) => {
    ensure(item?.catName).expense += Math.abs(toNum(item?.amount));
  });

  normalizeList(day?.lists?.withdrawal).forEach((item) => {
    ensure(item?.catName || 'Вывод средств').expense += Math.abs(toNum(item?.amount));
  });

  return map;
};

const pickTopOp = (items = [], kind = 'income') => {
  const safe = normalizeList(items)
    .map((item) => ({
      item,
      amountAbs: Math.abs(toNum(item?.amount))
    }))
    .filter((row) => row.amountAbs > 0)
    .sort((a, b) => Number(b.amountAbs || 0) - Number(a.amountAbs || 0));

  if (!safe.length) return null;
  const top = safe[0].item;
  const amount = kind === 'expense'
    ? fmtSignedT(-Math.abs(toNum(top?.amount)))
    : fmtSignedT(Math.abs(toNum(top?.amount)));

  const cat = String(top?.catName || (kind === 'expense' ? 'Расход' : 'Доход'));
  const acc = String(top?.accName || '???');
  return { amount, cat, acc };
};

const renderDayInsightsBlock = ({ day, onlyOpen = false, scope = null }) => {
  const income = toNum(day?.totals?.income);
  const expense = toNum(day?.totals?.expense);
  const net = income - expense;
  const effectiveScope = normalizeScope(scope || (onlyOpen ? 'open' : 'all'));

  const openBalance = normalizeList(day?.accountBalances)
    .filter((acc) => acc?.isOpen === true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const hiddenBalance = normalizeList(day?.accountBalances)
    .filter((acc) => acc?.isOpen !== true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const relevantBalance = effectiveScope === 'open'
    ? openBalance
    : (effectiveScope === 'hidden' ? hiddenBalance : (openBalance + hiddenBalance));

  const topIncome = pickTopOp(day?.lists?.income, 'income');
  const expenseRows = [
    ...normalizeList(day?.lists?.expense),
    ...normalizeList(day?.lists?.withdrawal)
  ];
  const topExpense = pickTopOp(expenseRows, 'expense');

  const categoryFlows = computeCategoryFlowsForDay(day);
  const anomalies = Array.from(categoryFlows.entries())
    .map(([name, rec]) => ({
      name,
      income: toNum(rec?.income),
      expense: toNum(rec?.expense),
      gap: toNum(rec?.expense) - toNum(rec?.income)
    }))
    .filter((row) => row.income > 0 && row.expense > row.income)
    .sort((a, b) => Number(b.gap || 0) - Number(a.gap || 0))
    .slice(0, 2);

  const lines = [
    '----------------',
    'ВЫВОДЫ ПО ДНЮ',
    `- Нетто дня: ${fmtSignedT(net)}`
  ];

  if (expense > 0) {
    lines.push(`- Покрытие расходов доходами дня: ${fmtPercent((income / expense) * 100)}`);
  } else if (income > 0) {
    lines.push('- Покрытие расходов доходами дня: расходов в этот день не было');
  } else {
    lines.push('- Покрытие расходов доходами дня: движения по доходам/расходам не было');
  }

  lines.push(`- Ликвидность на конец дня (${scopeLabelRu(effectiveScope)}): ${fmtT(relevantBalance)}`);

  if (topIncome) {
    lines.push(`- Крупнейший доход: ${topIncome.amount} (${topIncome.cat}, ${topIncome.acc})`);
  }

  if (topExpense) {
    lines.push(`- Крупнейший расход: ${topExpense.amount} (${topExpense.cat}, ${topExpense.acc})`);
  }

  if (anomalies.length) {
    const text = anomalies.map((row) => `${row.name}: ${fmtT(row.gap)}`).join('; ');
    lines.push(`- Аномалии (расход > компенсации): ${text}`);
  } else {
    lines.push('- Аномалии (расход > компенсации): не обнаружены');
  }

  if (expense > income && relevantBalance > 0) {
    lines.push('- Совет: день убыточный по потоку, но ликвидность покрывает разрыв.');
  } else if (expense > income && relevantBalance <= 0) {
    lines.push('- Совет: день убыточный по потоку и без ликвидности, нужен перенос/покрытие.');
  } else {
    lines.push('- Совет: критических действий не требуется.');
  }

  return lines.join('\n');
};

const flattenDayOperations = (day) => {
  const output = [];

  normalizeList(day?.lists?.income).forEach((item) => {
    output.push({
      amountAbs: Math.abs(toNum(item?.amount)),
      label: String(item?.catName || 'Доход'),
      line: fmtListOperation(item, 'income')
    });
  });

  normalizeList(day?.lists?.expense).forEach((item) => {
    output.push({
      amountAbs: Math.abs(toNum(item?.amount)),
      label: String(item?.catName || 'Расход'),
      line: fmtListOperation(item, 'expense')
    });
  });

  normalizeList(day?.lists?.withdrawal).forEach((item) => {
    output.push({
      amountAbs: Math.abs(toNum(item?.amount)),
      label: String(item?.catName || 'Вывод средств'),
      line: fmtListOperation(item, 'withdrawal')
    });
  });

  normalizeList(day?.lists?.transfer).forEach((item) => {
    output.push({
      amountAbs: Math.abs(toNum(item?.amount)),
      label: 'Перевод',
      line: fmtListOperation(item, 'transfer')
    });
  });

  return output
    .filter((item) => item.line)
    .sort((a, b) => Number(b.amountAbs || 0) - Number(a.amountAbs || 0));
};

const renderUpcomingOps = ({ snapshot, nowDateKey, limit = 5 }) => {
  const now = DATE_KEY_RE.test(String(nowDateKey || ''))
    ? String(nowDateKey)
    : String(snapshot?.range?.startDateKey || '');

  const candidates = snapshot.days
    .filter((day) => day.dateKey > now)
    .filter(dayHasActivity)
    .sort((a, b) => String(a.dateKey).localeCompare(String(b.dateKey)))
    .slice(0, Math.max(1, Number(limit || 5)));

  if (!candidates.length) {
    return `После ${toRuDateLabel(now)} ближайшие операции в snapshot не найдены.`;
  }

  const lines = [`Ближайшие операции после ${toRuDateLabel(now)}:`];

  candidates.forEach((day) => {
    const count = opCountForDay(day);
    const income = toNum(day?.totals?.income);
    const expense = toNum(day?.totals?.expense);
    lines.push(`- ${day.dateLabel}: операций ${count}, доход +${fmtT(income)}, расход -${fmtT(expense)}`);

    flattenDayOperations(day)
      .slice(0, 3)
      .forEach((entry) => {
        lines.push(`  ${entry.line}`);
      });
  });

  return lines.join('\n');
};

const renderForecastEndOfMonth = ({ snapshot, targetMonth, timelineDateKey, scope = 'all' }) => {
  let year;
  let month;

  if (targetMonth && Number.isFinite(Number(targetMonth.year)) && Number.isFinite(Number(targetMonth.month))) {
    year = Number(targetMonth.year);
    month = Number(targetMonth.month);
  } else {
    const baseDate = dateFromKey(timelineDateKey)
      || dateFromKey(snapshot?.range?.endDateKey)
      || new Date();
    year = baseDate.getFullYear();
    month = baseDate.getMonth() + 1;
  }

  const targetDayKey = endOfMonthDateKey(year, month);
  const targetDay = findDay(snapshot, targetDayKey);

  if (!targetDay) {
    const monthStart = startOfMonthDateKey(year, month);
    return {
      ok: false,
      text: `Нет dayKey ${targetDayKey} в snapshot; нужен диапазон ${monthStart} — ${targetDayKey}.`,
      meta: { targetDayKey, monthStart }
    };
  }

  const effectiveScope = normalizeScope(scope, 'all');

  return {
    ok: true,
    text: [
      `Прогноз балансов на конец месяца (${String(targetDay.dateLabel || toRuDateLabel(targetDayKey))}, ${scopeLabelRu(effectiveScope)}):`,
      renderDayBlock({ day: targetDay, scope: effectiveScope })
    ].join('\n\n'),
    meta: { targetDayKey }
  };
};

const renderInvestCapacity = ({ snapshot, intent, timelineDateKey }) => {
  const resolved = resolveTargetDay({ snapshot, intent, timelineDateKey });
  if (!resolved.ok) return { ok: false, text: resolved.text };

  const targetDay = resolved.day;
  const scope = normalizeScope(intent?.scope, 'all');
  const basis = String(intent?.basis || 'balance').toLowerCase() === 'inflows' ? 'inflows' : 'balance';

  const nowKey = DATE_KEY_RE.test(String(timelineDateKey || ''))
    ? String(timelineDateKey)
    : String(snapshot?.range?.startDateKey || '');

  const byBalance = sumBalancesByScope(targetDay, scope);
  const byInflows = sumInflowsByScope({
    snapshot,
    fromDateKey: nowKey,
    toDateKey: targetDay.dateKey,
    scope,
    targetDay
  });

  const result = basis === 'inflows' ? byInflows : byBalance;
  const basisLabel = basis === 'inflows'
    ? `по поступлениям до ${String(targetDay.dateLabel || toRuDateLabel(targetDay.dateKey))}`
    : `по ликвидности на ${String(targetDay.dateLabel || toRuDateLabel(targetDay.dateKey))}`;

  const lines = [
    `Потенциал инвестиций (${scopeLabelRu(scope)}): ${fmtT(result)}`,
    `> База расчета: ${basisLabel}`,
    `> Ликвидность на целевую дату: ${fmtT(byBalance)}`
  ];

  if (basis === 'inflows') {
    lines.push(`> Поступления в горизонте: ${fmtT(byInflows)}`);
  }

  return {
    ok: true,
    text: lines.join('\n'),
    meta: {
      targetDateKey: targetDay.dateKey,
      scope,
      basis
    }
  };
};

const renderExpenseFeasibility = ({ snapshot, intent, timelineDateKey }) => {
  const requestedAmount = toNum(intent?.requestedAmount);
  if (requestedAmount <= 0) {
    return {
      ok: false,
      text: 'Не удалось определить сумму расхода. Укажи сумму явно, например: 2 000 000.'
    };
  }

  const resolved = resolveTargetDay({ snapshot, intent, timelineDateKey });
  if (!resolved.ok) return { ok: false, text: resolved.text };

  const targetDay = resolved.day;
  const scope = normalizeScope(intent?.scope, 'all');
  const available = sumBalancesByScope(targetDay, scope);
  const delta = available - requestedAmount;
  const canPlan = delta >= 0;

  const lines = [
    `Запланировать расход ${fmtT(requestedAmount)} на ${String(targetDay.dateLabel || toRuDateLabel(targetDay.dateKey))} (${scopeLabelRu(scope)}): ${canPlan ? 'ДА' : 'НЕТ'}.`,
    `> Доступно на дату: ${fmtT(available)}`
  ];

  if (canPlan) {
    lines.push(`> Остаток после расхода: ${fmtT(delta)}`);
  } else {
    lines.push(`> Дефицит: ${fmtT(Math.abs(delta))}`);
  }

  return {
    ok: true,
    text: lines.join('\n'),
    meta: {
      targetDateKey: targetDay.dateKey,
      scope,
      requestedAmount,
      available
    }
  };
};

const accumulateCategoryFlows = (snapshot) => {
  const map = new Map();

  snapshot.days.forEach((day) => {
    normalizeList(day?.lists?.income).forEach((item) => {
      const key = String(item?.catName || 'Без категории');
      if (!map.has(key)) map.set(key, { income: 0, expense: 0 });
      map.get(key).income += Math.abs(toNum(item?.amount));
    });

    normalizeList(day?.lists?.expense).forEach((item) => {
      const key = String(item?.catName || 'Без категории');
      if (!map.has(key)) map.set(key, { income: 0, expense: 0 });
      map.get(key).expense += Math.abs(toNum(item?.amount));
    });

    normalizeList(day?.lists?.withdrawal).forEach((item) => {
      const key = String(item?.catName || 'Вывод средств');
      if (!map.has(key)) map.set(key, { income: 0, expense: 0 });
      map.get(key).expense += Math.abs(toNum(item?.amount));
    });
  });

  return map;
};

const LEDGER_OPERATIONS_LIMIT = 120;
const PERIOD_TOP_OPERATIONS_LIMIT = 10;

const buildLedgerOperations = ({
  snapshot,
  limit = LEDGER_OPERATIONS_LIMIT,
  startDateKey = null,
  endDateKey = null,
  sortByAmountDesc = false,
  enforceMinLimit = true
}) => {
  const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
  const rows = [];
  const hasStartFilter = DATE_KEY_RE.test(String(startDateKey || ''));
  const hasEndFilter = DATE_KEY_RE.test(String(endDateKey || ''));

  const pushRow = (row) => {
    if (!row) return;
    const date = String(row.date || '');
    if (!DATE_KEY_RE.test(date)) return;

    rows.push({
      date,
      type: String(row.type || 'Операция'),
      amount: Math.abs(toNum(row.amount)),
      counterparty: String(row.counterparty || 'Без контрагента'),
      category: String(row.category || 'Без категории'),
      account: String(row.account || 'Без счета')
    });
  };

  days.forEach((day) => {
    const date = String(day?.dateKey || '');
    if (!DATE_KEY_RE.test(date)) return;
    if (hasStartFilter && date < startDateKey) return;
    if (hasEndFilter && date > endDateKey) return;

    normalizeList(day?.lists?.income).forEach((item) => {
      pushRow({
        date,
        type: 'Доход',
        amount: item?.amount,
        counterparty: item?.contName || 'Без контрагента',
        category: item?.catName || 'Без категории',
        account: item?.accName || 'Без счета'
      });
    });

    normalizeList(day?.lists?.expense).forEach((item) => {
      pushRow({
        date,
        type: 'Расход',
        amount: item?.amount,
        counterparty: item?.contName || 'Без контрагента',
        category: item?.catName || 'Без категории',
        account: item?.accName || 'Без счета'
      });
    });

    normalizeList(day?.lists?.withdrawal).forEach((item) => {
      pushRow({
        date,
        type: 'Расход',
        amount: item?.amount,
        counterparty: item?.contName || 'Без контрагента',
        category: item?.catName || 'Вывод средств',
        account: item?.accName || 'Без счета'
      });
    });

    normalizeList(day?.lists?.transfer).forEach((item) => {
      const fromAcc = String(item?.fromAccName || 'Без счета');
      const toAcc = String(item?.toAccName || 'Без счета');
      pushRow({
        date,
        type: 'Перевод',
        amount: item?.amount,
        counterparty: 'Без контрагента',
        category: 'Перевод',
        account: `${fromAcc} -> ${toAcc}`
      });
    });
  });

  if (sortByAmountDesc) {
    rows.sort((a, b) => {
      const diff = toNum(b?.amount) - toNum(a?.amount);
      if (diff !== 0) return diff;
      return String(a?.date || '').localeCompare(String(b?.date || ''));
    });
  }

  const numericLimit = Number(limit);
  const fallbackLimit = enforceMinLimit
    ? Math.max(50, Number(LEDGER_OPERATIONS_LIMIT || 120))
    : rows.length;
  const safeLimit = Number.isFinite(numericLimit) && numericLimit > 0
    ? (enforceMinLimit ? Math.max(50, numericLimit) : Math.max(1, numericLimit))
    : fallbackLimit;
  const operations = rows.slice(0, safeLimit);

  return {
    operations,
    operationsMeta: {
      totalCount: rows.length,
      includedCount: operations.length,
      truncated: rows.length > operations.length,
      limit: safeLimit
    }
  };
};

const computePeriodAnalytics = ({ snapshot, question, timelineDateKey, topLimit = PERIOD_TOP_OPERATIONS_LIMIT }) => {
  const resolvedPeriod = resolvePeriodFromQuestion({
    question,
    timelineDateKey,
    snapshot
  });
  if (!resolvedPeriod) return null;

  const clampedRange = clampDateKeyRangeToSnapshot({
    snapshot,
    startDateKey: resolvedPeriod.startDateKey,
    endDateKey: resolvedPeriod.endDateKey
  });
  if (!clampedRange) return null;

  const periodDays = normalizeList(snapshot?.days).filter((day) => {
    const date = String(day?.dateKey || '');
    return DATE_KEY_RE.test(date)
      && date >= clampedRange.startDateKey
      && date <= clampedRange.endDateKey;
  });
  if (!periodDays.length) return null;

  const totals = periodDays.reduce((acc, day) => {
    acc.income += toNum(day?.totals?.income);
    acc.expense += toNum(day?.totals?.expense);
    return acc;
  }, { income: 0, expense: 0 });
  totals.net = totals.income - totals.expense;

  const topLimitSafe = Math.max(5, Math.min(10, Number(topLimit || PERIOD_TOP_OPERATIONS_LIMIT)));
  const top = buildLedgerOperations({
    snapshot,
    startDateKey: clampedRange.startDateKey,
    endDateKey: clampedRange.endDateKey,
    limit: topLimitSafe,
    sortByAmountDesc: true,
    enforceMinLimit: false
  });

  return {
    label: `${toRuDateShort(clampedRange.startDateKey)} - ${toRuDateShort(clampedRange.endDateKey)}`,
    startDateKey: clampedRange.startDateKey,
    endDateKey: clampedRange.endDateKey,
    totals,
    topOperations: top.operations,
    operationsMeta: {
      totalCount: top.operationsMeta.totalCount,
      topIncludedCount: top.operations.length,
      topLimit: topLimitSafe,
      source: resolvedPeriod.source,
      requestedRange: {
        startDateKey: resolvedPeriod.startDateKey,
        endDateKey: resolvedPeriod.endDateKey
      },
      wasClampedToSnapshot: clampedRange.wasClamped
    }
  };
};

const computeDeterministicFacts = ({ snapshot, timelineDateKey }) => {
  const days = Array.isArray(snapshot?.days) ? snapshot.days : [];
  if (!days.length) {
    return {
      range: {
        startDateKey: snapshot?.range?.startDateKey || '',
        endDateKey: snapshot?.range?.endDateKey || '',
        startDateLabel: toRuDateLabel(snapshot?.range?.startDateKey || ''),
        endDateLabel: toRuDateLabel(snapshot?.range?.endDateKey || ''),
        dayCount: 0
      },
      totals: { income: 0, expense: 0, net: 0 },
      endBalances: { open: 0, hidden: 0, total: 0 },
      anomalies: [],
      upcomingCount: 0,
      nextObligation: null,
      topExpenseDays: [],
      timeline: {
        requestedDateKey: DATE_KEY_RE.test(String(timelineDateKey || '')) ? String(timelineDateKey) : null,
        asOfDateKey: null,
        asOfDateLabel: null,
        hasAsOfInSnapshot: false
      },
      fact: {
        dayCount: 0,
        totals: { income: 0, expense: 0, net: 0 },
        balances: { open: 0, hidden: 0, total: 0 }
      },
      plan: {
        dayCount: 0,
        totals: { income: 0, expense: 0, net: 0 },
        toEndBalances: { open: 0, hidden: 0, total: 0 },
        nextObligation: null
      },
      operations: [],
      operationsMeta: {
        totalCount: 0,
        includedCount: 0,
        truncated: false,
        limit: Math.max(50, Number(LEDGER_OPERATIONS_LIMIT || 120))
      }
    };
  }

  const sumTotals = (rows) => {
    const income = rows.reduce((sum, day) => sum + toNum(day?.totals?.income), 0);
    const expense = rows.reduce((sum, day) => sum + toNum(day?.totals?.expense), 0);
    return { income, expense, net: income - expense };
  };

  const totalIncome = snapshot.days.reduce((sum, day) => sum + toNum(day?.totals?.income), 0);
  const totalExpense = snapshot.days.reduce((sum, day) => sum + toNum(day?.totals?.expense), 0);
  const totalNet = totalIncome - totalExpense;

  const lastDay = snapshot.days[snapshot.days.length - 1] || null;
  const endOpen = normalizeList(lastDay?.accountBalances)
    .filter((acc) => acc?.isOpen === true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const endHidden = normalizeList(lastDay?.accountBalances)
    .filter((acc) => acc?.isOpen !== true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);

  const categoryFlows = accumulateCategoryFlows(snapshot);
  const anomalies = Array.from(categoryFlows.entries())
    .map(([name, rec]) => ({
      name,
      income: toNum(rec?.income),
      expense: toNum(rec?.expense),
      gap: toNum(rec?.expense) - toNum(rec?.income)
    }))
    .filter((row) => row.income > 0 && row.expense > row.income)
    .sort((a, b) => Number(b.gap || 0) - Number(a.gap || 0))
    .slice(0, 5);

  const now = DATE_KEY_RE.test(String(timelineDateKey || ''))
    ? String(timelineDateKey)
    : String(snapshot.range.startDateKey || snapshot.days[0]?.dateKey || '');

  const asOfDay = snapshot.days
    .filter((day) => String(day?.dateKey || '') <= now)
    .slice(-1)[0] || null;

  const asOfKey = String(asOfDay?.dateKey || '');
  const factDays = asOfKey
    ? snapshot.days.filter((day) => String(day?.dateKey || '') <= asOfKey)
    : [];
  const planDays = asOfKey
    ? snapshot.days.filter((day) => String(day?.dateKey || '') > asOfKey)
    : snapshot.days;

  const factTotals = sumTotals(factDays);
  const planTotals = sumTotals(planDays);

  const asOfOpen = normalizeList(asOfDay?.accountBalances)
    .filter((acc) => acc?.isOpen === true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const asOfHidden = normalizeList(asOfDay?.accountBalances)
    .filter((acc) => acc?.isOpen !== true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);

  const upcoming = planDays
    .filter((day) => day.dateKey > now)
    .filter(dayHasActivity)
    .sort((a, b) => String(a.dateKey).localeCompare(String(b.dateKey)));

  const nextObligation = (() => {
    for (const day of upcoming) {
      const expense = toNum(day?.totals?.expense);
      if (expense > 0) {
        return {
          dateKey: day.dateKey,
          dateLabel: day.dateLabel,
          amount: expense
        };
      }
    }
    return null;
  })();

  const expenseDaysTop = snapshot.days
    .map((day) => ({
      dateKey: day.dateKey,
      dateLabel: day.dateLabel,
      expense: toNum(day?.totals?.expense)
    }))
    .filter((x) => x.expense > 0)
    .sort((a, b) => Number(b.expense || 0) - Number(a.expense || 0))
    .slice(0, 3);
  const ledger = buildLedgerOperations({ snapshot });

  return {
    range: {
      startDateKey: snapshot.range.startDateKey,
      endDateKey: snapshot.range.endDateKey,
      startDateLabel: toRuDateLabel(snapshot.range.startDateKey),
      endDateLabel: toRuDateLabel(snapshot.range.endDateKey),
      dayCount: snapshot.days.length
    },
    totals: {
      income: totalIncome,
      expense: totalExpense,
      net: totalNet
    },
    endBalances: {
      open: endOpen,
      hidden: endHidden,
      total: endOpen + endHidden
    },
    anomalies,
    upcomingCount: upcoming.length,
    nextObligation,
    topExpenseDays: expenseDaysTop,
    timeline: {
      requestedDateKey: DATE_KEY_RE.test(String(timelineDateKey || '')) ? String(timelineDateKey) : null,
      asOfDateKey: asOfKey || null,
      asOfDateLabel: asOfDay?.dateLabel || (asOfKey ? toRuDateLabel(asOfKey) : null),
      hasAsOfInSnapshot: Boolean(asOfDay && asOfKey === now)
    },
    fact: {
      dayCount: factDays.length,
      totals: factTotals,
      balances: {
        open: asOfOpen,
        hidden: asOfHidden,
        total: asOfOpen + asOfHidden
      }
    },
    plan: {
      dayCount: planDays.length,
      totals: planTotals,
      toEndBalances: {
        open: endOpen,
        hidden: endHidden,
        total: endOpen + endHidden
      },
      nextObligation
    },
    operations: ledger.operations,
    operationsMeta: ledger.operationsMeta
  };
};

const buildDeterministicInsightsBlock = (facts) => {
  const lines = [
    'Детерминированные факты (из tooltipSnapshot):',
    `- Период: ${facts.range.startDateLabel} — ${facts.range.endDateLabel} (${facts.range.dayCount} дн.)`,
    `- Доход: +${fmtT(facts.totals.income)}`,
    `- Расход: -${fmtT(facts.totals.expense)}`,
    `- Нетто: ${fmtSignedT(facts.totals.net)}`,
    `- Баланс на конец диапазона: ${fmtT(facts.endBalances.total)} (открытые ${fmtT(facts.endBalances.open)}, скрытые ${fmtT(facts.endBalances.hidden)})`
  ];

  if (facts?.timeline?.asOfDateLabel) {
    lines.push(`- Сегодня (asOf): ${facts.timeline.asOfDateLabel}`);
  }
  if (facts?.fact?.totals) {
    lines.push(`- Факт до today: доход +${fmtT(facts.fact.totals.income)}, расход -${fmtT(facts.fact.totals.expense)}, нетто ${fmtSignedT(facts.fact.totals.net)}`);
  }
  if (facts?.plan?.totals) {
    lines.push(`- План после today: доход +${fmtT(facts.plan.totals.income)}, расход -${fmtT(facts.plan.totals.expense)}, нетто ${fmtSignedT(facts.plan.totals.net)}`);
  }

  if (facts.nextObligation) {
    lines.push(`- Ближайшее обязательство: ${facts.nextObligation.dateLabel} — ${fmtT(facts.nextObligation.amount)}`);
  }

  if (Array.isArray(facts.anomalies) && facts.anomalies.length) {
    const top = facts.anomalies
      .slice(0, 3)
      .map((row) => `${row.name}: ${fmtT(row.gap)}`)
      .join('; ');
    lines.push(`- Аномалии (расход > компенсации): ${top}`);
  } else {
    lines.push('- Аномалии (расход > компенсации): не обнаружены');
  }

  return lines.join('\n');
};

function answerFromSnapshot({ snapshot, intent, timelineDateKey }) {
  const type = String(intent?.type || 'INSIGHTS');

  if (type === 'CATEGORY_FACT_BY_CATEGORY') {
    const result = renderCategoryFactByCategory({ snapshot, intent, timelineDateKey });
    return {
      ok: result.ok,
      numeric: true,
      text: result.text,
      meta: result.meta || null
    };
  }

  if (type === 'BALANCE_ON_DATE') {
    const day = findDay(snapshot, intent?.dateKey);
    if (!day) {
      return {
        ok: false,
        numeric: true,
        text: `Не найден день ${String(intent?.dateKey || '?')} в snapshot. Доступный диапазон: ${snapshot.range.startDateKey} — ${snapshot.range.endDateKey}.`
      };
    }
    return {
      ok: true,
      numeric: true,
      text: [
        renderDayBlock({ day, onlyOpen: false }),
        renderDayInsightsBlock({ day, onlyOpen: false })
      ].join('\n'),
      meta: { dateKey: day.dateKey }
    };
  }

  if (type === 'OPEN_BALANCES_ON_DATE') {
    const day = findDay(snapshot, intent?.dateKey);
    if (!day) {
      return {
        ok: false,
        numeric: true,
        text: `Не найден день ${String(intent?.dateKey || '?')} в snapshot. Доступный диапазон: ${snapshot.range.startDateKey} — ${snapshot.range.endDateKey}.`
      };
    }
    return {
      ok: true,
      numeric: true,
      text: [
        renderDayBlock({ day, onlyOpen: true }),
        renderDayInsightsBlock({ day, onlyOpen: true })
      ].join('\n'),
      meta: { dateKey: day.dateKey }
    };
  }

  if (type === 'UPCOMING_OPS') {
    return {
      ok: true,
      numeric: true,
      text: renderUpcomingOps({
        snapshot,
        nowDateKey: intent?.dateKey || timelineDateKey || snapshot.range.startDateKey,
        limit: 5
      })
    };
  }

  if (type === 'INVEST_CAPACITY') {
    const result = renderInvestCapacity({ snapshot, intent, timelineDateKey });
    return {
      ok: result.ok,
      numeric: true,
      text: result.text,
      meta: result.meta || null
    };
  }

  if (type === 'EXPENSE_FEASIBILITY') {
    const result = renderExpenseFeasibility({ snapshot, intent, timelineDateKey });
    return {
      ok: result.ok,
      numeric: true,
      text: result.text,
      meta: result.meta || null
    };
  }

  if (type === 'FORECAST_END_OF_MONTH' || type === 'FORECAST_OPEN_END_OF_MONTH') {
    const scope = type === 'FORECAST_OPEN_END_OF_MONTH'
      ? 'open'
      : normalizeScope(intent?.scope, 'all');
    const forecast = renderForecastEndOfMonth({
      snapshot,
      targetMonth: intent?.targetMonth || null,
      timelineDateKey: timelineDateKey || snapshot.range.endDateKey,
      scope
    });

    return {
      ok: forecast.ok,
      numeric: true,
      text: forecast.text,
      meta: forecast.meta || null
    };
  }

  const facts = computeDeterministicFacts({
    snapshot,
    timelineDateKey: timelineDateKey || intent?.dateKey || snapshot.range.startDateKey
  });
  const deterministicBlock = buildDeterministicInsightsBlock(facts);

  return {
    ok: true,
    numeric: false,
    text: deterministicBlock,
    deterministicBlock,
    facts,
    meta: {
      timelineDateKey: timelineDateKey || intent?.dateKey || null
    }
  };
}

module.exports = {
  validateTooltipSnapshot,
  answerFromSnapshot,
  computeDeterministicFacts,
  computePeriodAnalytics,
  buildDeterministicInsightsBlock,
  toRuDateLabel,
  fmtT,
  fmtSignedT
};
