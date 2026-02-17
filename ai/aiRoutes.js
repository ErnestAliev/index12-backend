// backend/ai/aiRoutes.js
// Hybrid AI routes:
// - quick_button -> deterministic quick mode
// - chat         -> LLM agent with journal packet context

const express = require('express');

const AIROUTES_VERSION = 'hybrid-v2.1';

module.exports = function createAiRouter(deps) {
  const {
    mongoose,
    models,
    isAuthenticated,
    getCompositeUserId,
  } = deps;

  const { Event, Account, Company, Contractor, Individual, Project, Category, ChatHistory } = models;

  const createDataProvider = require('./dataProvider');
  const dataProvider = createDataProvider({
    mongoose,
    Event,
    Account,
    Company,
    Contractor,
    Individual,
    Project,
    Category
  });

  const quickMode = require('./modes/quickMode');
  const createQuickJournalAdapter = require('./quickJournalAdapter');
  const quickJournalAdapter = createQuickJournalAdapter({ Event });

  // NEW: Import deterministic calculator, intent parser, and conversational agent
  const financialCalculator = require('./utils/financialCalculator');
  const intentParser = require('./utils/intentParser');
  const conversationalAgent = require('./utils/conversationalAgent');

  const router = express.Router();

  const _applyRawSnapshotAccounts = (dbData, rawSnapshot) => {
    const rawAccounts = Array.isArray(rawSnapshot?.accounts) ? rawSnapshot.accounts : [];
    if (!rawAccounts.length) return;

    const mapped = rawAccounts
      .map((a) => {
        const id = a?._id || a?.id || a?.accountId;
        if (!id) return null;
        const isExcluded = !!(a?.isExcluded || a?.excluded || a?.excludeFromTotal || a?.excludedFromTotal);
        const isHidden = !!(a?.isHidden || a?.hidden || isExcluded);
        const currentBalance = Number(a?.balance ?? a?.currentBalance ?? 0);
        const futureBalance = Number(a?.futureBalance ?? currentBalance ?? 0);

        return {
          _id: String(id),
          name: a?.name || a?.accountName || `Счет ${String(id).slice(-4)}`,
          currentBalance: Number.isFinite(currentBalance) ? Math.round(currentBalance) : 0,
          futureBalance: Number.isFinite(futureBalance) ? Math.round(futureBalance) : 0,
          companyId: a?.companyId ? String(a.companyId) : null,
          isHidden,
          isExcluded,
        };
      })
      .filter(Boolean);

    if (!mapped.length) return;

    const openAccounts = mapped.filter((a) => !a.isHidden && !a.isExcluded);
    const hiddenAccounts = mapped.filter((a) => a.isHidden || a.isExcluded);

    const openCurrent = openAccounts.reduce((s, a) => s + (a.currentBalance || 0), 0);
    const openFuture = openAccounts.reduce((s, a) => s + (a.futureBalance || 0), 0);
    const hiddenCurrent = hiddenAccounts.reduce((s, a) => s + (a.currentBalance || 0), 0);
    const hiddenFuture = hiddenAccounts.reduce((s, a) => s + (a.futureBalance || 0), 0);

    dbData.accounts = mapped;
    dbData.totals = {
      open: { current: openCurrent, future: openFuture },
      hidden: { current: hiddenCurrent, future: hiddenFuture },
      all: { current: openCurrent + hiddenCurrent, future: openFuture + hiddenFuture }
    };
    dbData.accountsData = {
      accounts: mapped,
      openAccounts,
      hiddenAccounts,
      totals: dbData.totals,
      meta: {
        today: dbData?.meta?.today || '?',
        count: mapped.length,
        openCount: openAccounts.length,
        hiddenCount: hiddenAccounts.length
      }
    };
  };

  const _applyRawSnapshotCompanies = (dbData, rawSnapshot) => {
    const rawCompanies = Array.isArray(rawSnapshot?.companies) ? rawSnapshot.companies : [];
    if (!rawCompanies.length) return;

    const mapped = rawCompanies
      .map((c) => {
        const id = c?._id || c?.id;
        if (!id) return null;
        return {
          id: String(id),
          name: c?.name || `Компания ${String(id).slice(-4)}`,
          taxRegime: c?.taxRegime || 'simplified',
          taxPercent: (c?.taxPercent != null) ? c.taxPercent : 3,
          identificationNumber: c?.identificationNumber || null
        };
      })
      .filter(Boolean);

    if (!mapped.length) return;
    dbData.catalogs = dbData.catalogs || {};
    dbData.catalogs.companies = mapped;
  };

  const _formatTenge = (n) => {
    const num = Number(n || 0);
    const sign = num < 0 ? '- ' : '';
    try {
      return sign + new Intl.NumberFormat('ru-RU').format(Math.abs(Math.round(num))).split('\u00A0').join(' ') + ' ₸';
    } catch (_) {
      return sign + String(Math.round(Math.abs(num))) + ' ₸';
    }
  };

  const _isAiAllowed = (req) => {
    const AI_ALLOW_ALL = process.env.AI_ALLOW_ALL === 'true';
    if (AI_ALLOW_ALL) return true;

    const allowedEmails = (process.env.AI_ALLOW_EMAILS || '').split(',').map(e => e.trim()).filter(Boolean);
    const userEmail = req.user?.email || '';
    return allowedEmails.includes(userEmail);
  };

  const _toNum = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  };

  const _fmtDDMMYY = (date) => {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '?';
    const dd = String(date.getDate()).padStart(2, '0');
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const yy = String(date.getFullYear()).slice(-2);
    return `${dd}.${mm}.${yy}`;
  };

  const _parseDateLabel = (value) => {
    const raw = String(value || '').trim();
    const m = raw.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
    if (!m) return null;
    const dd = Number(m[1]);
    const mm = Number(m[2]) - 1;
    const yyyy = Number(m[3]);
    const d = new Date(yyyy, mm, dd);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const _resolveRange = (periodFilter, asOf) => {
    const nowRef = (() => {
      if (asOf) {
        const d = new Date(asOf);
        if (!Number.isNaN(d.getTime())) return d;
      }
      return new Date();
    })();

    let start = null;
    let end = null;

    if (periodFilter && periodFilter.mode === 'custom') {
      if (periodFilter.customStart) {
        const d = new Date(periodFilter.customStart);
        if (!Number.isNaN(d.getTime())) start = d;
      }
      if (periodFilter.customEnd) {
        const d = new Date(periodFilter.customEnd);
        if (!Number.isNaN(d.getTime())) end = d;
      }
    }

    if (!start || !end) {
      start = new Date(nowRef.getFullYear(), nowRef.getMonth(), 1, 0, 0, 0, 0);
      end = new Date(nowRef.getFullYear(), nowRef.getMonth() + 1, 0, 23, 59, 59, 999);
    }

    return {
      startTs: start.getTime(),
      endTs: end.getTime(),
      startLabel: _fmtDDMMYY(start),
      endLabel: _fmtDDMMYY(end),
    };
  };

  const _normalizeToken = (value) => String(value || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[^a-zа-я0-9]+/gi, '');

  const _tokenLooksLike = (queryToken, valueToken) => {
    if (!queryToken || !valueToken) return false;
    if (queryToken.includes(valueToken) || valueToken.includes(queryToken)) return true;
    const q5 = queryToken.slice(0, 5);
    const v5 = valueToken.slice(0, 5);
    return q5.length >= 4 && v5.length >= 4 && (queryToken.startsWith(v5) || valueToken.startsWith(q5));
  };

  const _parseRowTs = (row) => {
    const rawDate = row?.date ? new Date(row.date) : null;
    if (rawDate && !Number.isNaN(rawDate.getTime())) return rawDate.getTime();
    const byLabel = _parseDateLabel(row?.dateLabel);
    if (byLabel && !Number.isNaN(byLabel.getTime())) return byLabel.getTime();
    return NaN;
  };

  const _pad2 = (n) => String(Number(n || 0)).padStart(2, '0');

  const _toDayKey = (dateLike) => {
    const d = dateLike instanceof Date ? dateLike : new Date(dateLike);
    if (Number.isNaN(d.getTime())) return '';
    return `${d.getFullYear()}-${_pad2(d.getMonth() + 1)}-${_pad2(d.getDate())}`;
  };

  const _parseRowDayKey = (row) => {
    const rawDate = String(row?.date || '').trim();
    const iso = rawDate.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;

    const rawLabel = String(row?.dateLabel || '').trim();
    const ru = rawLabel.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
    if (ru) return `${ru[3]}-${ru[2]}-${ru[1]}`;

    const ts = _parseRowTs(row);
    if (!Number.isFinite(ts)) return '';
    return _toDayKey(new Date(ts));
  };

  const _normalizeKind = (typeValue) => {
    const t = String(typeValue || '').trim().toLowerCase();
    if (t === 'доход' || t === 'income') return 'income';
    if (t === 'предоплата' || t === 'prepayment') return 'income';
    if (t === 'расход' || t === 'expense') return 'expense';
    if (t === 'перевод' || t === 'transfer' || t === 'вывод средств' || t === 'withdrawal') return 'transfer';
    return null;
  };

  const _isOutOfSystemTransferRow = (row) => {
    if (!row) return false;
    if (row?.isWithdrawal === true) return true;

    const purpose = String(row?.transferPurpose || '').toLowerCase().trim();
    const reason = String(row?.transferReason || '').toLowerCase().trim();
    if (purpose === 'personal' && reason === 'personal_use') return true;

    const t = _normalizeToken(row?.type || '');
    return t.includes('выводсредств') || t.includes('withdrawal');
  };

  const _normalizeStatus = (statusCode, statusLabel) => {
    const sc = String(statusCode || '').trim().toLowerCase();
    if (sc === 'plan') return 'plan';
    if (sc === 'fact') return 'fact';
    const s = String(statusLabel || '').toLowerCase();
    if (s.includes('план')) return 'plan';
    return 'fact';
  };

  const _mkBucket = () => ({ income: 0, expense: 0, transfer: 0, net: 0, count: 0 });

  const _addToBucket = (bucket, kind, amountAbs) => {
    bucket.count += 1;
    if (kind === 'income') bucket.income += amountAbs;
    if (kind === 'expense') bucket.expense += amountAbs;
    if (kind === 'transfer') bucket.transfer += amountAbs;
  };

  const _finalizeBucket = (bucket) => {
    bucket.net = bucket.income - bucket.expense;
    return bucket;
  };

  const _metricValue = (bucket, metric) => {
    if (!bucket) return 0;
    if (metric === 'income') return bucket.income;
    if (metric === 'expense') return bucket.expense;
    if (metric === 'transfer') return bucket.transfer;
    return bucket.net;
  };

  const _fmtMoneyPlain = (value) => {
    const n = Number(value || 0);
    try {
      return new Intl.NumberFormat('ru-RU')
        .format(Math.round(Math.abs(n)))
        .replace(/\u00A0/g, ' ');
    } catch (_) {
      return String(Math.round(Math.abs(n)));
    }
  };

  const _splitTransferAccountLabel = (label) => {
    const raw = String(label || '').trim();
    if (!raw) return null;
    const parts = raw.split(/\s*(?:->|→|=>|➡)\s*/);
    if (parts.length !== 2) return null;
    return {
      from: String(parts[0] || '').trim(),
      to: String(parts[1] || '').trim()
    };
  };

  const _buildAccountNameIndex = (accounts) => {
    const byToken = new Map();
    (accounts || []).forEach((acc) => {
      const token = _normalizeToken(acc?.name || '');
      if (!token) return;
      if (!byToken.has(token)) byToken.set(token, []);
      byToken.get(token).push(acc);
    });
    return byToken;
  };

  const _resolveAccountByLabel = (label, accounts, byToken) => {
    const token = _normalizeToken(label);
    if (!token) return null;

    const exact = byToken.get(token);
    if (exact && exact.length) return exact[0];

    for (const acc of (accounts || [])) {
      const accToken = _normalizeToken(acc?.name || '');
      if (_tokenLooksLike(token, accToken)) return acc;
    }
    return null;
  };

  const _extractId = (value) => {
    if (!value) return '';
    if (typeof value === 'object') {
      if (value._id) return String(value._id);
      if (value.id) return String(value.id);
      return '';
    }
    return String(value);
  };

  const _computeForecastData = ({ rows, asOf, accounts }) => {
    const nowRef = (() => {
      if (asOf) {
        const d = new Date(asOf);
        if (!Number.isNaN(d.getTime())) return d;
      }
      return new Date();
    })();

    const rawAsOfDayKey = (() => {
      const m = String(asOf || '').match(/^(\d{4}-\d{2}-\d{2})/);
      return m ? m[1] : '';
    })();
    const asOfDayKey = rawAsOfDayKey || _toDayKey(nowRef);
    const keyMatch = asOfDayKey.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    const baseYear = keyMatch ? Number(keyMatch[1]) : nowRef.getFullYear();
    const baseMonth = keyMatch ? Number(keyMatch[2]) : (nowRef.getMonth() + 1); // 1..12
    const baseDay = keyMatch ? Number(keyMatch[3]) : nowRef.getDate();

    const monthStartDayKey = `${baseYear}-${_pad2(baseMonth)}-01`;
    const monthEndDate = new Date(baseYear, baseMonth, 0, 23, 59, 59, 999);
    const monthEndDayKey = _toDayKey(monthEndDate);

    const actual = _mkBucket();
    const futureAll = _mkBucket();
    const futurePlan = _mkBucket();

    const futurePlanIncomeByCategory = new Map();
    const futurePlanExpenseByCategory = new Map();
    const factToDateByCategory = new Map();
    const accountList = Array.isArray(accounts) ? accounts : [];
    const accountByName = _buildAccountNameIndex(accountList);
    const projectedBalanceByAccountId = new Map();
    accountList.forEach((acc) => {
      const id = String(acc?._id || '');
      if (!id) return;
      projectedBalanceByAccountId.set(id, Number(acc?.initialBalance || 0));
    });

    const addCategoryFlow = (map, categoryName, kind, amountAbs) => {
      const key = String(categoryName || 'Без категории');
      if (!map.has(key)) {
        map.set(key, { income: 0, expense: 0 });
      }
      const rec = map.get(key);
      if (kind === 'income') rec.income += amountAbs;
      if (kind === 'expense') rec.expense += amountAbs;
    };

    const addDelta = (accountId, delta) => {
      const id = String(accountId || '');
      if (!id || !projectedBalanceByAccountId.has(id)) return;
      projectedBalanceByAccountId.set(id, Number(projectedBalanceByAccountId.get(id) || 0) + Number(delta || 0));
    };

    rows.forEach((row) => {
      const dayKey = _parseRowDayKey(row);
      if (!dayKey) return;

      const kind = _normalizeKind(row?.type);
      if (!kind) return;
      const status = _normalizeStatus(row?.statusCode, row?.status);
      const amountAbs = Math.abs(_toNum(row?.amount));
      const categoryName = String(row?.category || 'Без категории');

      // Month-end account balances: apply all known ops up to end of month (same principle as graph tooltip).
      if (dayKey <= monthEndDayKey && accountList.length) {
        if (kind === 'transfer') {
          const isOutOfSystemTransfer = _isOutOfSystemTransferRow(row);
          const fromIdDirect = _extractId(row?.fromAccountId);
          const toIdDirect = _extractId(row?.toAccountId);

          if (fromIdDirect) addDelta(fromIdDirect, -amountAbs);
          if (!isOutOfSystemTransfer && toIdDirect) addDelta(toIdDirect, amountAbs);

          if (!fromIdDirect && !toIdDirect) {
            const transferParts = _splitTransferAccountLabel(row?.account);
            if (transferParts) {
              const fromAcc = _resolveAccountByLabel(transferParts.from, accountList, accountByName);
              const toAcc = _resolveAccountByLabel(transferParts.to, accountList, accountByName);
              if (fromAcc?._id) addDelta(fromAcc._id, -amountAbs);
              if (!isOutOfSystemTransfer && toAcc?._id) addDelta(toAcc._id, amountAbs);
            }
          }
        } else {
          const accountIdDirect = _extractId(row?.accountId);
          if (accountIdDirect) {
            if (kind === 'income') addDelta(accountIdDirect, amountAbs);
            if (kind === 'expense') addDelta(accountIdDirect, -amountAbs);
          } else {
            const acc = _resolveAccountByLabel(row?.account, accountList, accountByName);
            if (acc?._id) {
              if (kind === 'income') addDelta(acc._id, amountAbs);
              if (kind === 'expense') addDelta(acc._id, -amountAbs);
            }
          }
        }
      }

      if (dayKey < monthStartDayKey || dayKey > monthEndDayKey) return;

      const isFutureDay = dayKey > asOfDayKey;
      if (!isFutureDay) {
        _addToBucket(actual, kind, amountAbs);
        addCategoryFlow(factToDateByCategory, categoryName, kind, amountAbs);
        return;
      }

      _addToBucket(futureAll, kind, amountAbs);
      if (status === 'plan') {
        _addToBucket(futurePlan, kind, amountAbs);
        if (kind === 'income') {
          futurePlanIncomeByCategory.set(categoryName, (futurePlanIncomeByCategory.get(categoryName) || 0) + amountAbs);
        }
        if (kind === 'expense') {
          futurePlanExpenseByCategory.set(categoryName, (futurePlanExpenseByCategory.get(categoryName) || 0) + amountAbs);
        }
      }

      if (!accountList.length) return;
    });

    _finalizeBucket(actual);
    _finalizeBucket(futureAll);
    _finalizeBucket(futurePlan);

    const projectedIncome = actual.income + futureAll.income;
    const projectedExpense = actual.expense + futureAll.expense;
    const projectedOperatingProfit = projectedIncome - projectedExpense;
    const projectedMargin = projectedIncome > 0
      ? Math.round((projectedOperatingProfit / projectedIncome) * 100)
      : 0;

    const currentOpen = accountList
      .filter((a) => !a?.isHidden && !a?.isExcluded)
      .reduce((s, a) => s + Number(a?.balance || 0), 0);
    const currentHidden = accountList
      .filter((a) => a?.isHidden || a?.isExcluded)
      .reduce((s, a) => s + Number(a?.balance || 0), 0);
    const currentTotal = currentOpen + currentHidden;

    let projectedOpen = 0;
    let projectedHidden = 0;
    accountList.forEach((acc) => {
      const id = String(acc?._id || '');
      if (!id) return;
      const endBalanceRaw = Number(projectedBalanceByAccountId.get(id));
      const endBalance = Math.max(0, Number.isFinite(endBalanceRaw) ? endBalanceRaw : Number(acc?.initialBalance || 0));
      if (acc?.isHidden || acc?.isExcluded) projectedHidden += endBalance;
      else projectedOpen += endBalance;
    });

    const topPlanIncome = Array.from(futurePlanIncomeByCategory.entries())
      .sort((a, b) => b[1] - a[1])[0] || null;

    const factUtilities = (() => {
      let income = 0;
      let expense = 0;
      for (const [name, rec] of factToDateByCategory.entries()) {
        const token = _normalizeToken(name);
        if (!token.includes('коммун')) continue;
        income += Number(rec.income || 0);
        expense += Number(rec.expense || 0);
      }
      return { income, expense };
    })();

    const plannedTaxes = (() => {
      let total = 0;
      for (const [name, amount] of futurePlanExpenseByCategory.entries()) {
        const token = _normalizeToken(name);
        if (token.includes('налог')) total += Number(amount || 0);
      }
      return total;
    })();

    const findings = [];
    if (factUtilities.expense > factUtilities.income) {
      findings.push(`Факт расход на коммуналку превышает факт доход на ${_fmtMoneyPlain(factUtilities.expense - factUtilities.income)} ₸`);
    }
    if (plannedTaxes > 0) {
      findings.push(`Планируемые налоги ${_fmtMoneyPlain(plannedTaxes)} ₸ значительно повлияют на будущие расходы`);
    }
    if (futurePlan.expense > futurePlan.income) {
      findings.push(`До конца месяца плановые расходы выше плановых доходов на ${_fmtMoneyPlain(futurePlan.expense - futurePlan.income)} ₸`);
    }

    const endDate = monthEndDate;
    const asOfDate = new Date(baseYear, baseMonth - 1, baseDay, 12, 0, 0, 0);

    return {
      asOfLabel: _fmtDDMMYY(asOfDate),
      periodEndLabel: _fmtDDMMYY(endDate),
      current: {
        openBalance: currentOpen,
        hiddenBalance: currentHidden,
        totalBalance: currentTotal
      },
      projected: {
        openBalance: projectedOpen,
        hiddenBalance: projectedHidden,
        totalBalance: projectedOpen + projectedHidden,
        income: projectedIncome,
        expense: projectedExpense,
        operatingProfit: projectedOperatingProfit,
        marginPercent: projectedMargin,
        liquidityOpen: projectedOpen
      },
      remainingPlan: {
        income: futurePlan.income,
        expense: futurePlan.expense,
        operatingProfit: futurePlan.net,
        topIncomeCategory: topPlanIncome ? String(topPlanIncome[0]) : null,
        topIncomeAmount: topPlanIncome ? Number(topPlanIncome[1] || 0) : 0
      },
      findings
    };
  };

  const _dayKeyToLabel = (dayKey) => {
    const m = String(dayKey || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return String(dayKey || '?');
    return `${m[3]}.${m[2]}.${String(m[1]).slice(-2)}`;
  };

  const _computeRiskData = ({ rows, asOf, accounts, forecastData }) => {
    const nowRef = (() => {
      if (asOf) {
        const d = new Date(asOf);
        if (!Number.isNaN(d.getTime())) return d;
      }
      return new Date();
    })();
    const asOfDayKey = _toDayKey(nowRef);
    const monthEndDate = new Date(nowRef.getFullYear(), nowRef.getMonth() + 1, 0, 23, 59, 59, 999);
    const monthEndDayKey = _toDayKey(monthEndDate);

    const accountList = Array.isArray(accounts) ? accounts : [];
    const openLiquidityNow = accountList
      .filter((a) => !a?.isHidden && !a?.isExcluded)
      .reduce((sum, a) => sum + Number(a?.balance || 0), 0);
    const hiddenLiquidityNow = accountList
      .filter((a) => a?.isHidden || a?.isExcluded)
      .reduce((sum, a) => sum + Number(a?.balance || 0), 0);
    const totalLiquidityNow = openLiquidityNow + hiddenLiquidityNow;

    const futurePlanOutflows = [];
    const outflowByCategory = new Map();
    let transferOutflowTotal = 0;

    rows.forEach((row) => {
      const dayKey = _parseRowDayKey(row);
      if (!dayKey) return;
      if (dayKey <= asOfDayKey || dayKey > monthEndDayKey) return;

      const status = _normalizeStatus(row?.statusCode, row?.status);
      if (status !== 'plan') return;

      const kind = _normalizeKind(row?.type);
      if (!kind) return;
      const amount = Math.abs(_toNum(row?.amount));
      const categoryName = String(row?.category || 'Без категории');

      if (kind === 'income') {
        return;
      }

      if (kind === 'expense') {
        futurePlanOutflows.push({
          dateKey: dayKey,
          dateLabel: _dayKeyToLabel(dayKey),
          label: categoryName,
          categoryKey: categoryName,
          amount
        });
        outflowByCategory.set(categoryName, (outflowByCategory.get(categoryName) || 0) + amount);
        return;
      }

      if (kind === 'transfer' && _isOutOfSystemTransferRow(row)) {
        const transferLabel = String(row?.account || '').trim() || 'Вывод средств';
        futurePlanOutflows.push({
          dateKey: dayKey,
          dateLabel: _dayKeyToLabel(dayKey),
          label: transferLabel,
          categoryKey: 'Вывод средств',
          amount
        });
        transferOutflowTotal += amount;
        outflowByCategory.set('Вывод средств', (outflowByCategory.get('Вывод средств') || 0) + amount);
      }
    });

    const plannedIncome = Number(forecastData?.remainingPlan?.income || 0);
    const plannedExpenseBase = Number(forecastData?.remainingPlan?.expense || 0);
    const plannedExpense = plannedExpenseBase + transferOutflowTotal;
    const hasPlannedFlows = plannedIncome > 0 || plannedExpense > 0;

    // Liquidity views:
    // 1) Plan-only: net effect of planned inflows/outflows for remaining period.
    // 2) Plan + balances: expected month-end liquidity (use forecast snapshot when present).
    // 3) Accounts-only: current liquidity on accounts without planned flows.
    const planOnlyLiquidity = plannedIncome - plannedExpense;
    const projectedOpenRaw = Number(forecastData?.projected?.openBalance);
    const projectedHiddenRaw = Number(forecastData?.projected?.hiddenBalance);
    const projectedTotalRaw = Number(forecastData?.projected?.totalBalance);
    const planPlusOpenLiquidity = Number.isFinite(projectedOpenRaw)
      ? projectedOpenRaw
      : (openLiquidityNow + planOnlyLiquidity);
    const planPlusHiddenLiquidity = Number.isFinite(projectedHiddenRaw)
      ? projectedHiddenRaw
      : hiddenLiquidityNow;
    const planPlusTotalLiquidity = Number.isFinite(projectedTotalRaw)
      ? projectedTotalRaw
      : (planPlusOpenLiquidity + planPlusHiddenLiquidity);

    const plannedGap = plannedExpense - plannedIncome;
    const safetyBuffer = hasPlannedFlows ? Math.round(Math.max(0, plannedExpense) * 0.1) : 0;
    const reserveNeed = hasPlannedFlows ? (Math.max(0, plannedGap) + safetyBuffer) : 0;
    const safeSpend = Math.max(0, openLiquidityNow - reserveNeed);

    const planOnlyCoverageRatio = plannedExpense > 0 ? (plannedIncome / plannedExpense) : null;
    const coverageRatioOpenNow = plannedExpense > 0 ? (openLiquidityNow / plannedExpense) : null;
    const coverageRatioHiddenNow = plannedExpense > 0 ? (hiddenLiquidityNow / plannedExpense) : null;
    const coverageRatioTotalNow = plannedExpense > 0 ? (totalLiquidityNow / plannedExpense) : null;

    const topOutflows = futurePlanOutflows
      .slice()
      .sort((a, b) => {
        if (a.dateKey !== b.dateKey) return a.dateKey.localeCompare(b.dateKey);
        return Number(b.amount || 0) - Number(a.amount || 0);
      })
      .slice(0, 8)
      .map((row) => ({
        ...row,
        categoryTotal: Number(outflowByCategory.get(String(row.categoryKey || row.label || '')) || 0)
      }));

    const topExpenseCategories = Array.from(outflowByCategory.entries())
      .map(([name, amount]) => ({ name, amount: Number(amount || 0) }))
      .sort((a, b) => Number(b.amount || 0) - Number(a.amount || 0))
      .slice(0, 8);

    const deterministicRisks = [];
    if (hasPlannedFlows && plannedGap > 0) {
      deterministicRisks.push(`Плановый разрыв до конца месяца: расходы выше доходов на ${_fmtMoneyPlain(plannedGap)} ₸.`);
    }
    if (hasPlannedFlows && planOnlyCoverageRatio !== null && planOnlyCoverageRatio < 1) {
      deterministicRisks.push(`Плановые доходы покрывают только ${Math.round(planOnlyCoverageRatio * 100)}% плановых расходов.`);
    }
    if (hasPlannedFlows && coverageRatioOpenNow !== null && coverageRatioOpenNow < 1) {
      deterministicRisks.push(`Покрытие плановых расходов открытой ликвидностью ниже 100% (${Math.round(coverageRatioOpenNow * 100)}%).`);
    }
    if (topOutflows.length) {
      const first = topOutflows[0];
      deterministicRisks.push(`Ближайшее крупное списание: ${first.dateLabel} — ${first.label} на ${_fmtMoneyPlain(first.amount)} ₸.`);
    }

    const deterministicActions = [];
    if (hasPlannedFlows && plannedGap > 0) {
      deterministicActions.push(`Сократи или перенеси плановые расходы минимум на ${_fmtMoneyPlain(plannedGap)} ₸ до конца месяца.`);
    }
    if (hasPlannedFlows) {
      if (safeSpend <= 0) {
        deterministicActions.push('Ограничь новые расходы до 0 ₸ до момента подтвержденных поступлений.');
      } else {
        deterministicActions.push(`Зафиксируй лимит новых расходов не выше ${_fmtMoneyPlain(safeSpend)} ₸ на период до конца месяца.`);
      }
    } else {
      deterministicActions.push('Плановых операций до конца месяца нет: контролируй остатки только по счетам.');
    }
    if (topOutflows.length) {
      deterministicActions.push(`Проверь и приоритизируй ближайшие списания начиная с ${topOutflows[0].dateLabel}.`);
    }

    return {
      asOfLabel: _fmtDDMMYY(nowRef),
      periodEndLabel: _fmtDDMMYY(monthEndDate),
      hasPlannedFlows,
      openLiquidityNow,
      hiddenLiquidityNow,
      totalLiquidityNow,
      planOnlyLiquidity,
      planPlusOpenLiquidity,
      planPlusHiddenLiquidity,
      planPlusTotalLiquidity,
      plannedIncome,
      plannedExpense,
      plannedGap,
      safetyBuffer,
      reserveNeed,
      safeSpend,
      planOnlyCoverageRatio,
      coverageRatioOpenNow,
      coverageRatioHiddenNow,
      coverageRatioTotalNow,
      // Backward-compatible alias used by old formatter/debug consumers.
      coverageRatio: coverageRatioOpenNow,
      topOutflows,
      topExpenseCategories,
      deterministicRisks,
      deterministicActions
    };
  };

  const _summarizeRows = (rows) => {
    const summary = {
      fact: _mkBucket(),
      plan: _mkBucket(),
      all: _mkBucket(),
    };

    const byCategory = new Map();
    const byProject = new Map();

    const ensureBreakdown = (map, key) => {
      if (!map.has(key)) {
        map.set(key, {
          name: key,
          fact: _mkBucket(),
          plan: _mkBucket(),
          all: _mkBucket(),
        });
      }
      return map.get(key);
    };

    rows.forEach((row) => {
      const kind = _normalizeKind(row?.type);
      if (!kind) return;
      const status = _normalizeStatus(row?.statusCode, row?.status);
      const amountAbs = Math.abs(_toNum(row?.amount));

      _addToBucket(summary[status], kind, amountAbs);
      _addToBucket(summary.all, kind, amountAbs);

      const category = String(row?.category || 'Без категории');
      const project = String(row?.project || 'Без проекта');
      const catRec = ensureBreakdown(byCategory, category);
      const projRec = ensureBreakdown(byProject, project);

      _addToBucket(catRec[status], kind, amountAbs);
      _addToBucket(catRec.all, kind, amountAbs);
      _addToBucket(projRec[status], kind, amountAbs);
      _addToBucket(projRec.all, kind, amountAbs);
    });

    _finalizeBucket(summary.fact);
    _finalizeBucket(summary.plan);
    _finalizeBucket(summary.all);

    const finalizeMap = (map) => Array.from(map.values())
      .map((item) => ({
        ...item,
        fact: _finalizeBucket(item.fact),
        plan: _finalizeBucket(item.plan),
        all: _finalizeBucket(item.all),
      }));

    return {
      summary,
      byCategory: finalizeMap(byCategory),
      byProject: finalizeMap(byProject),
      rowCount: rows.length,
    };
  };

  const _extractMatchedNames = (question, names) => {
    const q = _normalizeToken(question);
    if (!q) return [];
    return names.filter((name) => {
      const token = _normalizeToken(name);
      return token && _tokenLooksLike(q, token);
    });
  };

  const _inferIntent = (question, analyticsBase) => {
    const q = String(question || '').toLowerCase();
    const isFinancial = /(доход|расход|прибыл|чист|перевод|вывод|фот|аренд|проект|категор|анализ|отчет|итог|план|факт)/i.test(q);

    let metric = 'overview';
    if (/(доход|выруч|поступ)/i.test(q)) metric = 'income';
    else if (/(расход|траты|затрат|издерж)/i.test(q)) metric = 'expense';
    else if (/(перевод|вывод)/i.test(q)) metric = 'transfer';
    else if (/(прибыл|чист)/i.test(q)) metric = 'net';

    const hasPlan = /(план|прогноз|ожида|заплан)/i.test(q);
    const hasFact = /(факт|исполн|уже|реаль|прошл)/i.test(q);
    const statusScope = hasPlan && !hasFact ? 'plan' : hasFact && !hasPlan ? 'fact' : 'both';

    let groupBy = null;
    if (/(по проект|проекты|проектам)/i.test(q)) groupBy = 'project';
    if (/(по катег|категории|категориям)/i.test(q)) groupBy = 'category';

    const categories = _extractMatchedNames(q, (analyticsBase.byCategory || []).map((x) => x.name));
    const projects = _extractMatchedNames(q, (analyticsBase.byProject || []).map((x) => x.name));

    return { isFinancial, metric, statusScope, groupBy, categories, projects };
  };

  const _composeDeterministicAnswer = ({ intent, analytics, period, formatTenge }) => {
    const { metric, statusScope, groupBy } = intent;
    const { summary, byCategory, byProject, rowCount } = analytics;
    const periodLine = `Период: ${period.startLabel} — ${period.endLabel}`;

    if (!rowCount) {
      return `${periodLine}\nДанные по запросу не найдены.`;
    }

    const pickBucket = (scope) => (scope === 'fact' ? summary.fact : scope === 'plan' ? summary.plan : summary.all);
    const bucketFact = pickBucket('fact');
    const bucketPlan = pickBucket('plan');
    const bucketAll = pickBucket('both');

    const lines = [periodLine];

    const metricLabel = metric === 'income'
      ? 'Доходы'
      : metric === 'expense'
        ? 'Расходы'
        : metric === 'transfer'
          ? 'Переводы'
          : metric === 'net'
            ? 'Чистый результат'
            : 'Итоги';

    lines.push(metricLabel + ':');

    const valueByMetric = (bucket) => _metricValue(bucket, metric === 'overview' ? 'net' : metric);
    if (statusScope === 'fact') {
      lines.push(`Факт: ${formatTenge(valueByMetric(bucketFact))}`);
    } else if (statusScope === 'plan') {
      lines.push(`План: ${formatTenge(valueByMetric(bucketPlan))}`);
    } else {
      lines.push(`Факт: ${formatTenge(valueByMetric(bucketFact))}`);
      lines.push(`План: ${formatTenge(valueByMetric(bucketPlan))}`);
      lines.push(`Итого: ${formatTenge(valueByMetric(bucketAll))}`);
    }

    if (metric === 'overview' || metric === 'net') {
      lines.push(`Доходы: ${formatTenge(bucketAll.income)}`);
      lines.push(`Расходы: ${formatTenge(bucketAll.expense)}`);
      lines.push(`Переводы: ${formatTenge(bucketAll.transfer)}`);
    }

    if (groupBy === 'project' || groupBy === 'category') {
      const source = groupBy === 'project' ? byProject : byCategory;
      const sorted = [...source].sort((a, b) => Math.abs(_metricValue(b.all, metric === 'overview' ? 'net' : metric)) - Math.abs(_metricValue(a.all, metric === 'overview' ? 'net' : metric)));
      const top = sorted.slice(0, 12);
      if (top.length) {
        lines.push(groupBy === 'project' ? 'По проектам:' : 'По категориям:');
        top.forEach((item) => {
          if (statusScope === 'fact') {
            lines.push(`${item.name}: ${formatTenge(_metricValue(item.fact, metric === 'overview' ? 'net' : metric))}`);
          } else if (statusScope === 'plan') {
            lines.push(`${item.name}: ${formatTenge(_metricValue(item.plan, metric === 'overview' ? 'net' : metric))}`);
          } else {
            lines.push(`${item.name}: факт ${formatTenge(_metricValue(item.fact, metric === 'overview' ? 'net' : metric))}, план ${formatTenge(_metricValue(item.plan, metric === 'overview' ? 'net' : metric))}, итого ${formatTenge(_metricValue(item.all, metric === 'overview' ? 'net' : metric))}`);
          }
        });
      }
    }

    return lines.join('\n');
  };

  const _buildDeterministicChatResult = ({ question, context, formatTenge }) => {
    const tableContext = context?.tableContext && typeof context.tableContext === 'object' ? context.tableContext : null;
    const rowsRaw = Array.isArray(tableContext?.rows) ? tableContext.rows : [];
    const periodFilter = context?.periodFilter || tableContext?.periodFilter || null;
    const period = _resolveRange(periodFilter, context?.asOf || null);

    const rows = rowsRaw.filter((row) => {
      const ts = _parseRowTs(row);
      if (!Number.isFinite(ts)) return false;
      return ts >= period.startTs && ts <= period.endTs;
    });

    const analyticsBase = _summarizeRows(rows);
    const intent = _inferIntent(question, analyticsBase);
    if (!intent.isFinancial) {
      return { isFinancial: false, text: '' };
    }

    const rowsFiltered = rows.filter((row) => {
      if (intent.categories.length) {
        const c = String(row?.category || '');
        if (!intent.categories.includes(c)) return false;
      }
      if (intent.projects.length) {
        const p = String(row?.project || '');
        if (!intent.projects.includes(p)) return false;
      }
      return true;
    });

    const analytics = _summarizeRows(rowsFiltered);
    const text = _composeDeterministicAnswer({
      intent,
      analytics,
      period,
      formatTenge
    });

    return {
      isFinancial: true,
      text,
      debug: {
        intent,
        period: { start: period.startLabel, end: period.endLabel },
        inputRows: rowsRaw.length,
        periodRows: rows.length,
        filteredRows: rowsFiltered.length
      }
    };
  };

  const _buildLlmContext = (body = {}) => {
    const tableContext = (body?.tableContext && typeof body.tableContext === 'object')
      ? body.tableContext
      : null;

    return {
      periodFilter: body?.periodFilter || null,
      asOf: body?.asOf || null,
      tableContext,
      snapshot: null
    };
  };

  const _callLlmAgent = async ({ question, context }) => {
    const apiKey = process.env.OPENAI_API_KEY;
    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';

    if (!apiKey) {
      return {
        ok: false,
        status: 503,
        text: 'AI временно недоступен: отсутствует OPENAI_API_KEY.'
      };
    }

    const systemPrompt = [
      'Ты AI-ассистент финансовой системы INDEX12.',
      'Отвечай только на русском языке.',
      'Единственный источник данных: operations_table_json.',
      'Статусы: "Исполнено" = факт, "План" = план.',
      'Для расчётов используй operations_table_json.summary как приоритетный источник итогов по текущему срезу.',
      'Для детализации используй operations_table_json.rows.',
      'Типы строк: "Доход", "Расход", "Перевод".',
      'Никогда не пиши "не найдены", если соответствующая сумма в operations_table_json.summary больше 0.',
      'Если rows непустой и содержит нужный тип операций, ответ должен содержать числовой итог.',
      'Всегда различай факт и план в расчётах.',
      'Если пользователь не просил объединять — показывай факт и план раздельно.',
      'Не придумывай числа и факты, которых нет в данных.',
      'Если данных недостаточно — прямо укажи, чего не хватает.',
      'Формат денег: 8 490 000 ₸ (пробелы между тысячами, знак ₸ в конце числа).',
      'Не используй формат 8,490,000 и не используй KZT.',
      'Пиши в обычном тексте, без markdown-разметки: не используй *, **, #, ```.',
      'Ответ делай понятным и коротким, с ключевыми цифрами по запросу пользователя.'
    ].join(' ');

    const userContent = [
      `Вопрос пользователя:\n${question}`,
      '',
      `operations_table_json:\n${JSON.stringify(context?.tableContext || null, null, 2)}`,
      '',
      `meta_json:\n${JSON.stringify({ periodFilter: context?.periodFilter || null, asOf: context?.asOf || null }, null, 2)}`
    ].join('\n');

    let upstream;
    try {
      upstream = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify({
          model,
          temperature: 0,
          max_tokens: 1200,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userContent }
          ]
        })
      });
    } catch (error) {
      return {
        ok: false,
        status: 502,
        text: 'Ошибка сети при обращении к AI.',
        debug: { message: error?.message || String(error) }
      };
    }

    let payload = null;
    try {
      payload = await upstream.json();
    } catch (_) {
      payload = null;
    }

    if (!upstream.ok) {
      return {
        ok: false,
        status: upstream.status || 502,
        text: 'AI сервис вернул ошибку.',
        debug: payload
      };
    }

    const choice = payload?.choices?.[0] || null;
    const finishReason = choice?.finish_reason || null;
    const content = choice?.message?.content;
    const text = typeof content === 'string'
      ? content.trim()
      : Array.isArray(content)
        ? content.map((part) => String(part?.text || '')).join('').trim()
        : '';

    const _normalizeMoneyText = (raw) => {
      let out = String(raw || '');
      if (!out) return out;

      // Strip common markdown artifacts
      out = out.replace(/```[\s\S]*?```/g, (m) => m.replace(/```/g, ''));
      out = out.replace(/^\s*#{1,6}\s*/gm, '');
      out = out.replace(/\*\*(.*?)\*\*/g, '$1');
      out = out.replace(/\*(.*?)\*/g, '$1');
      out = out.replace(/`([^`]+)`/g, '$1');
      out = out.replace(/^\s*\*\s+/gm, '- ');

      // 8,490,000 -> 8 490 000
      out = out.replace(/\b\d{1,3}(?:,\d{3})+\b/g, (m) => m.replace(/,/g, ' '));
      // Replace textual currency marker
      out = out.replace(/\bKZT\b/gi, '₸');
      // Prefix currency -> suffix currency: ₸8 490 000 -> 8 490 000 ₸
      out = out.replace(/₸\s*([0-9][0-9\s]*(?:[.,][0-9]+)?)/g, (_, num) => `${String(num).trim()} ₸`);
      // Keep consistent spacing near currency symbol
      out = out.replace(/(\d)₸/g, '$1 ₸');
      out = out.replace(/\s{2,}/g, ' ');

      return out.trim();
    };

    if (text) {
      return {
        ok: true,
        status: 200,
        text: _normalizeMoneyText(text),
        debug: {
          model,
          finishReason,
          usage: payload?.usage || null
        }
      };
    }

    if (finishReason === 'length') {
      return {
        ok: true,
        status: 200,
        text: 'AI не успел завершить ответ (лимит генерации). Сузьте период или уточните вопрос.',
        debug: {
          model,
          finishReason,
          usage: payload?.usage || null
        }
      };
    }

    return {
      ok: true,
      status: 200,
      text: 'Нет ответа от AI.',
      debug: {
        model,
        finishReason,
        usage: payload?.usage || null
      }
    };
  };

  router.get('/ping', (req, res) => {
    res.json({ ok: true, mode: 'hybrid', version: AIROUTES_VERSION });
  });

  // 🟢 GET /api/ai/history - Load chat history for current timeline date  
  router.get('/history', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');
      if (!userIdStr) return res.status(401).json({ error: 'Пользователь не найден' });
      const asOf = req.query.asOf;
      const timelineDate = asOf
        ? new Date(asOf).toISOString().split('T')[0]
        : new Date().toISOString().split('T')[0];

      const history = await ChatHistory.findOne({
        userId: userIdStr,
        timelineDate
      });

      return res.json({
        messages: history?.messages || [],
        timelineDate
      });
    } catch (error) {
      console.error('[AI History] Load error:', error);
      return res.status(500).json({ error: error.message });
    }
  });

  router.post('/query', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');
      if (!userIdStr) return res.status(401).json({ error: 'Пользователь не найден' });

      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const qRaw = String(req.body?.message ?? '');
      const q = qRaw.trim();
      if (!q) return res.status(400).json({ error: 'Пустой запрос' });

      const source = String(req?.body?.source || 'chat');
      const isQuickButton = source === 'quick_button';

      // 🟢 NEW: Chat history endpoint - GET /api/ai/history
      // Loads messages for current timeline date
      // Used by frontend to restore conversation on page load

      // Chat/source=chat: NEW PIPELINE WITH CHAT HISTORY
      // 0. Load chat history for user + current timeline date
      // 1. Save user message to history
      // 2. Compute metrics from tableContext.rows
      // 3. Parse intent via LLM  
      // 4. Filter data by intent (for financial queries)
      // 5. Use conversational agent (with history context)
      // 6. Save agent response to history
      // 7. Return response
      if (!isQuickButton) {
        const tableContext = req.body?.tableContext || null;
        const rows = Array.isArray(tableContext?.rows) ? tableContext.rows : [];
        const periodFilter = req.body?.periodFilter || tableContext?.periodFilter || null;
        const asOf = req.body?.asOf || null;

        // Step 0: Get current timeline date and load/create chat history
        const timelineDate = asOf ? new Date(asOf).toISOString().split('T')[0] : new Date().toISOString().split('T')[0];

        let chatHistory = await ChatHistory.findOne({
          userId: userIdStr,
          timelineDate
        });

        if (!chatHistory) {
          chatHistory = new ChatHistory({
            userId: userIdStr,
            timelineDate,
            messages: []
          });
        }

        // Step 1: Save user message to history
        chatHistory.messages.push({
          role: 'user',
          content: q,
          timestamp: new Date()
        });
        chatHistory.updatedAt = new Date();

        // Step 2: Compute all metrics deterministically
        const computed = financialCalculator.computeMetrics({
          rows,
          periodFilter,
          asOf
        });

        // Step 3: Parse intent via LLM (for context/metadata only)
        const intentResult = await intentParser.parseIntent({
          question: q,
          availableContext: {
            byCategory: computed.metrics.byCategory,
            byProject: computed.metrics.byProject
          }
        });

        const debugEnabled = req?.body?.debugAi === true;
        const intent = intentResult.ok
          ? intentResult.intent
          : {
            isFinancial: true,
            metric: 'overview',
            scope: 'all',
            status: 'both',
            groupBy: null,
            filters: { categories: [], projects: [] },
            description: 'Финансовый запрос'
          };

        // Step 4: Chat mode always uses conversational LLM branch.
        // Deterministic calculations remain as context + fallback.
        const currentBalanceFromRows = rows
          .filter(r => {
            const status = String(r?.statusCode || r?.status || '').toLowerCase();
            return status === 'fact' || status.includes('исполнено');
          })
          .reduce((sum, row) => {
            const type = String(row?.type || '').toLowerCase();
            const amount = Math.abs(Number(row?.amount) || 0);

            if (type === 'доход' || type === 'income') return sum + amount;
            if (type === 'расход' || type === 'expense') return sum - amount;
            return sum; // Transfers don't affect total balance
          }, 0);

        const accounts = Array.isArray(req.body?.accounts) ? req.body.accounts : [];
        const currentOpenBalance = accounts.length
          ? accounts
            .filter(a => !a.isHidden && !a.isExcluded)
            .reduce((s, a) => s + (Number(a.balance) || 0), 0)
          : currentBalanceFromRows;

        const currentHiddenBalance = accounts.length
          ? accounts
            .filter(a => a.isHidden || a.isExcluded)
            .reduce((s, a) => s + (Number(a.balance) || 0), 0)
          : 0;

        const currentTotalBalance = currentOpenBalance + currentHiddenBalance;

        const responseMode = 'analysis';
        const forecastData = _computeForecastData({
          rows,
          asOf,
          accounts
        });
        const riskData = _computeRiskData({
          rows,
          asOf,
          accounts,
          forecastData
        });

        const futureBalance = {
          current: forecastData?.current?.totalBalance ?? currentTotalBalance,
          plannedIncome: forecastData?.remainingPlan?.income ?? 0,
          plannedExpense: forecastData?.remainingPlan?.expense ?? 0,
          projected: forecastData?.projected?.totalBalance ?? currentTotalBalance,
          change: (forecastData?.projected?.totalBalance ?? currentTotalBalance) - (forecastData?.current?.totalBalance ?? currentTotalBalance)
        };

        const openBalance = currentOpenBalance;
        const hiddenBalance = currentHiddenBalance;

        // Extract hidden accounts data for strategic reserves context
        const hiddenAccountsData = accounts.length
          ? {
            count: accounts.filter(a => a.isHidden || a.isExcluded).length,
            totalCurrent: accounts
              .filter(a => a.isHidden || a.isExcluded)
              .reduce((s, a) => s + (Number(a.balance) || 0), 0),
            totalFuture: Number.isFinite(Number(forecastData?.projected?.hiddenBalance))
              ? Number(forecastData.projected.hiddenBalance)
              : currentHiddenBalance
          }
          : null;

        // Format current date for greeting responses
        const currentDate = (() => {
          if (asOf) {
            const d = new Date(asOf);
            if (!Number.isNaN(d.getTime())) return _fmtDDMMYY(d);
          }
          const now = new Date();
          return now.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' }).replace(/\./g, '.');
        })();

        // Use conversational agent with history context
        const conversationalResult = await conversationalAgent.generateConversationalResponse({
          question: q,
          history: chatHistory.messages.slice(0, -1), // Exclude current user message (already added)
          metrics: computed.metrics,
          period: computed.period,
          currentDate,
          formatCurrency: _formatTenge,
          futureBalance,
          openBalance,
          hiddenBalance,
          hiddenAccountsData,
          accounts: accounts || null,
          riskData,
          forecastData,
          availableContext: {
            byCategory: computed.metrics.byCategory,
            byProject: computed.metrics.byProject
          }
        });

        const responseText = conversationalResult.ok
          ? conversationalResult.text
          : (conversationalResult?.text || 'Не удалось получить ответ модели. Повторите запрос.');

        // Save agent response to history
        chatHistory.messages.push({
          role: 'assistant',
          content: responseText,
          timestamp: new Date(),
          metadata: {
            intent,
            responseMode,
            metrics: {
              fact: computed.metrics.fact,
              plan: computed.metrics.plan,
              total: computed.metrics.total
            }
          }
        });
        await chatHistory.save();

        return res.json({
          text: responseText,
          ...(debugEnabled ? {
            debug: {
              intent,
              period: computed.period,
              rowCounts: computed.rowCounts,
              conversational: conversationalResult.debug || null,
              intentParser: intentResult.ok
                ? (intentResult.debug || null)
                : {
                  error: intentResult.error || 'Intent parser failed',
                  details: intentResult.debug || null
                },
              responseMode,
              risk: riskData,
              forecast: forecastData,
              historyLength: chatHistory.messages.length
            }
          } : {})
        });
      }


      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try {
          effectiveUserId = await getCompositeUserId(req);
        } catch (_) {
          effectiveUserId = userId;
        }
      }

      const dataUserId = String(effectiveUserId || userId);
      const userIdsList = Array.from(
        new Set([effectiveUserId, req.user?.id || req.user?._id].filter(Boolean).map(String))
      );

      const workspaceId = req.user?.currentWorkspaceId || null;
      const requestIncludeHidden = req?.body?.includeHidden === true;
      const requestVisibleAccountIds = Array.isArray(req?.body?.visibleAccountIds)
        ? req.body.visibleAccountIds
        : null;

      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: requestIncludeHidden,
        visibleAccountIds: requestVisibleAccountIds,
        dateRange: req?.body?.periodFilter || null,
        workspaceId,
        now: req?.body?.asOf || null,
        snapshot: req?.body?.snapshot || null,
      });

      // Quick buttons must be consistent with Operations Editor source/rules.
      // Replace operation aggregates with journal-based dataset.
      const quickJournal = await quickJournalAdapter.buildFromJournal({
        userId: dataUserId,
        periodFilter: req?.body?.periodFilter || null,
        asOf: req?.body?.asOf || null,
        categoriesCatalog: dbData?.catalogs?.categories || []
      });

      dbData.operations = quickJournal.operations;
      dbData.operationsSummary = quickJournal.summary;
      dbData.categorySummary = quickJournal.categorySummary;
      dbData.meta = {
        ...(dbData.meta || {}),
        periodStart: quickJournal?.meta?.periodStart || dbData?.meta?.periodStart || '?',
        periodEnd: quickJournal?.meta?.periodEnd || dbData?.meta?.periodEnd || '?'
      };

      // Accounts/companies for quick buttons must come strictly from frontend snapshot.
      _applyRawSnapshotAccounts(dbData, req?.body?.snapshot || null);
      _applyRawSnapshotCompanies(dbData, req?.body?.snapshot || null);

      // Extract explicit action for quick buttons (new action-based routing)
      const quickAction = req.body?.action || null;

      const quickResponse = quickMode.handleQuickQuery({
        action: quickAction,
        query: q.toLowerCase(),
        dbData,
        snapshot: req?.body?.snapshot || null,
        formatTenge: _formatTenge
      });

      if (quickResponse) return res.json({ text: quickResponse });

      return res.json({
        text: 'Режим QUICK: этот запрос не поддержан предустановками. Используйте запросы: анализ, прогноз, счета, доходы, расходы, переводы, компании, проекты, категории, контрагенты, физлица.'
      });
    } catch (error) {
      console.error('AI Query Error:', error);
      return res.status(500).json({ error: 'Ошибка обработки запроса' });
    }
  });

  router.get('/version', (req, res) => {
    res.json({
      version: AIROUTES_VERSION,
      modes: {
        quick: 'modes/quickMode.js',
        chat: 'openai chat completions'
      },
      llm: true,
      deep: false,
      chat: true
    });
  });

  return router;
};
