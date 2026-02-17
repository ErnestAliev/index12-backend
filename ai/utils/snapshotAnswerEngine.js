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

const toRuDateLabel = (dateKey) => {
  const d = dateFromKey(dateKey);
  if (!d) return String(dateKey || '?');
  const wd = WEEKDAYS_RU_SHORT[d.getDay()];
  const dd = d.getDate();
  const mm = MONTHS_RU_SHORT[d.getMonth()];
  const yyyy = d.getFullYear();
  return `${wd}, ${dd} ${mm} ${yyyy} г.`;
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

const renderDayBlock = ({ day, onlyOpen = false }) => {
  const dayLabel = String(day?.dateLabel || toRuDateLabel(day?.dateKey));

  const balances = normalizeList(day?.accountBalances)
    .filter((acc) => (onlyOpen ? acc?.isOpen === true : true));

  const totalBalance = balances.reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const income = toNum(day?.totals?.income);
  const expense = toNum(day?.totals?.expense);

  const lines = [
    dayLabel,
    `Баланс общий: ${fmtT(totalBalance)}`,
    '----------------',
    'Остатки на счетах:'
  ];

  if (!balances.length) {
    lines.push('Нет счетов по выбранному режиму.');
  } else {
    balances.forEach((acc) => {
      lines.push(`${String(acc?.name || 'Счет')} — ${fmtT(acc?.balance)}`);
    });
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

const renderDayInsightsBlock = ({ day, onlyOpen = false }) => {
  const income = toNum(day?.totals?.income);
  const expense = toNum(day?.totals?.expense);
  const net = income - expense;

  const openBalance = normalizeList(day?.accountBalances)
    .filter((acc) => acc?.isOpen === true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const hiddenBalance = normalizeList(day?.accountBalances)
    .filter((acc) => acc?.isOpen !== true)
    .reduce((sum, acc) => sum + toNum(acc?.balance), 0);
  const relevantBalance = onlyOpen ? openBalance : (openBalance + hiddenBalance);

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

  lines.push(`- Ликвидность на конец дня (${onlyOpen ? 'открытые счета' : 'все счета'}): ${fmtT(relevantBalance)}`);

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

const renderForecastOpenEndOfMonth = ({ snapshot, targetMonth, timelineDateKey }) => {
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

  return {
    ok: true,
    text: [
      `Прогноз балансов на конец месяца (${String(targetDay.dateLabel || toRuDateLabel(targetDayKey))}, открытые счета):`,
      renderDayBlock({ day: targetDay, onlyOpen: true })
    ].join('\n\n'),
    meta: { targetDayKey }
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

const computeDeterministicFacts = ({ snapshot, timelineDateKey }) => {
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

  const upcoming = snapshot.days
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
    topExpenseDays: expenseDaysTop
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

  if (type === 'FORECAST_OPEN_END_OF_MONTH') {
    const forecast = renderForecastOpenEndOfMonth({
      snapshot,
      targetMonth: intent?.targetMonth || null,
      timelineDateKey: timelineDateKey || snapshot.range.endDateKey
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
  buildDeterministicInsightsBlock,
  toRuDateLabel,
  fmtT,
  fmtSignedT
};
