// backend/ai/aiRoutes.js
// Hybrid AI routes:
// - quick_button -> deterministic quick mode
// - chat         -> LLM agent with journal packet context

const express = require('express');
const path = require('path');
const fs = require('fs/promises');

const AIROUTES_VERSION = 'hybrid-v2.3-hctx-hardening';

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

  const conversationalAgent = require('./utils/conversationalAgent');
  const cfoKnowledgeBase = require('./utils/cfoKnowledgeBase');
  const snapshotAnswerEngine = require('./utils/snapshotAnswerEngine');
  const snapshotIntentParser = require('./utils/snapshotIntentParser');

  const router = express.Router();
  const LLM_SNAPSHOT_DIR = path.resolve(__dirname, 'debug');

  const _normalizeLlmInputSnapshot = (payload) => {
    const payloadObj = payload && typeof payload === 'object' ? payload : {};
    const detFacts = payloadObj?.deterministicFacts && typeof payloadObj.deterministicFacts === 'object'
      ? payloadObj.deterministicFacts
      : null;
    const resolvedHistoricalContext = payloadObj?.historicalContext
      || detFacts?.historicalContext
      || null;

    return {
      ...payloadObj,
      historicalContext: resolvedHistoricalContext,
      deterministicFacts: detFacts
        ? {
            ...detFacts,
            historicalContext: detFacts?.historicalContext || resolvedHistoricalContext || null
          }
        : payloadObj?.deterministicFacts
    };
  };

  const _dumpLlmInputSnapshot = async (payload) => {
    try {
      await fs.mkdir(LLM_SNAPSHOT_DIR, { recursive: true });
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const latestPath = path.join(LLM_SNAPSHOT_DIR, 'llm-input-latest.json');
      const archivePath = path.join(LLM_SNAPSHOT_DIR, `llm-input-${stamp}.json`);
      const normalized = _normalizeLlmInputSnapshot(payload);
      const body = JSON.stringify(normalized, null, 2);
      await fs.writeFile(latestPath, body, 'utf8');
      await fs.writeFile(archivePath, body, 'utf8');
      return { latestPath, archivePath };
    } catch (error) {
      console.error('[AI Snapshot] dump error:', error);
      return null;
    }
  };

  const _listSearchIndexes = async (collection, indexName = '') => {
    if (!collection) return [];

    if (typeof collection.listSearchIndexes === 'function') {
      try {
        const rows = await collection.listSearchIndexes(indexName || undefined).toArray();
        return Array.isArray(rows) ? rows : [];
      } catch (_) {
        // fallback below
      }
    }

    try {
      const rows = await collection.aggregate([{ $listSearchIndexes: {} }]).toArray();
      const list = Array.isArray(rows) ? rows : [];
      if (!indexName) return list;
      return list.filter((r) => String(r?.name || '') === String(indexName));
    } catch (_) {
      return [];
    }
  };

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

  const _normalizeHistoricalContext = (input) => {
    if (!input || typeof input !== 'object') return null;
    const periodsRaw = Array.isArray(input?.periods) ? input.periods : [];
    if (!periodsRaw.length) return null;

    const isDayKey = (value) => /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
    const periods = periodsRaw
      .slice(0, 24)
      .map((row) => {
        const period = String(row?.period || '').trim();
        const relation = String(row?.relation || '').trim();
        const offsetMonths = Number(row?.offsetMonths);
        const startDateKey = String(row?.range?.startDateKey || row?.startDateKey || '').trim();
        const endDateKey = String(row?.range?.endDateKey || row?.endDateKey || '').trim();

        return {
          period,
          relation: relation || null,
          offsetMonths: Number.isFinite(offsetMonths) ? Math.round(offsetMonths) : null,
          range: {
            startDateKey: isDayKey(startDateKey) ? startDateKey : '',
            endDateKey: isDayKey(endDateKey) ? endDateKey : ''
          },
          totals: {
            income: _toNum(row?.totals?.income),
            operational_expense: _toNum(row?.totals?.operational_expense),
            net: _toNum(row?.totals?.net)
          },
          topCategories: (Array.isArray(row?.topCategories) ? row.topCategories : [])
            .slice(0, 10)
            .map((cat) => ({
              category: String(cat?.category || 'Без категории'),
              amount: _toNum(cat?.amount),
              sharePct: _toNum(cat?.sharePct)
            })),
          ownerDraw: {
            amount: _toNum(row?.ownerDraw?.amount),
            byCategory: (Array.isArray(row?.ownerDraw?.byCategory) ? row.ownerDraw.byCategory : [])
              .slice(0, 10)
              .map((cat) => ({
                category: String(cat?.category || 'Вывод средств'),
                amount: _toNum(cat?.amount)
              }))
          },
          endBalances: {
            open: _toNum(row?.endBalances?.open),
            hidden: _toNum(row?.endBalances?.hidden),
            total: _toNum(row?.endBalances?.total)
          }
        };
      })
      .filter((row) => row.period || (row?.range?.startDateKey && row?.range?.endDateKey));

    if (!periods.length) return null;

    return {
      meta: {
        source: String(input?.meta?.source || 'background_analytics_buffer'),
        generatedAt: String(input?.meta?.generatedAt || ''),
        centerPeriod: String(input?.meta?.centerPeriod || ''),
        expectedPeriods: _toNum(input?.meta?.expectedPeriods),
        availablePeriods: _toNum(input?.meta?.availablePeriods),
        isWarm: input?.meta?.isWarm === true,
        isStale: input?.meta?.isStale === true,
        lastBuildReason: String(input?.meta?.lastBuildReason || '')
      },
      periods
    };
  };

  const _normalizeSnapshotChecksum = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    return raw.slice(0, 128);
  };

  const _parseBooleanFlag = (value) => {
    if (value === true) return true;
    if (value === false || value == null) return false;
    const text = String(value).trim().toLowerCase();
    return text === '1' || text === 'true' || text === 'yes';
  };

  const _extractLastSnapshotChecksum = (messages) => {
    const rows = Array.isArray(messages) ? messages : [];
    for (let idx = rows.length - 1; idx >= 0; idx -= 1) {
      const meta = rows[idx]?.metadata || {};
      const checksum = _normalizeSnapshotChecksum(
        meta?.snapshotChecksum
        || meta?.requestMeta?.snapshotChecksum
      );
      if (checksum) return checksum;
    }
    return '';
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

  const _isDayKey = (value) => /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));

  const _startOfMonthDayKey = (year, month) => {
    const y = Number(year);
    const m = Number(month);
    if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return '';
    return _toDayKey(new Date(y, m - 1, 1, 12, 0, 0, 0));
  };

  const _endOfMonthDayKey = (year, month) => {
    const y = Number(year);
    const m = Number(month);
    if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return '';
    return _toDayKey(new Date(y, m, 0, 12, 0, 0, 0));
  };

  const _resolveEffectiveSnapshotRange = ({ snapshot, parsedIntent, periodProbe }) => {
    const snapshotStart = String(snapshot?.range?.startDateKey || '');
    const snapshotEnd = String(snapshot?.range?.endDateKey || '');

    const requestedStart = String(periodProbe?.operationsMeta?.requestedRange?.startDateKey || '');
    const requestedEnd = String(periodProbe?.operationsMeta?.requestedRange?.endDateKey || '');
    if (_isDayKey(requestedStart) && _isDayKey(requestedEnd) && requestedStart <= requestedEnd) {
      return { startDateKey: requestedStart, endDateKey: requestedEnd, source: 'period_probe_requested_range' };
    }

    const targetYear = Number(parsedIntent?.targetMonth?.year);
    const targetMonth = Number(parsedIntent?.targetMonth?.month);
    if (Number.isFinite(targetYear) && Number.isFinite(targetMonth)) {
      const startDateKey = _startOfMonthDayKey(targetYear, targetMonth);
      const endDateKey = _endOfMonthDayKey(targetYear, targetMonth);
      if (_isDayKey(startDateKey) && _isDayKey(endDateKey) && startDateKey <= endDateKey) {
        return { startDateKey, endDateKey, source: 'intent_target_month' };
      }
    }

    if (_isDayKey(snapshotStart) && _isDayKey(snapshotEnd) && snapshotStart <= snapshotEnd) {
      return { startDateKey: snapshotStart, endDateKey: snapshotEnd, source: 'snapshot_range' };
    }

    return { startDateKey: '', endDateKey: '', source: 'empty_range' };
  };

  const _buildSnapshotForRange = ({ snapshot, startDateKey, endDateKey }) => {
    const safeStart = String(startDateKey || '');
    const safeEnd = String(endDateKey || '');
    const daysRaw = Array.isArray(snapshot?.days) ? snapshot.days : [];

    const days = (_isDayKey(safeStart) && _isDayKey(safeEnd) && safeStart <= safeEnd)
      ? daysRaw.filter((day) => {
          const dayKey = String(day?.dateKey || '');
          return _isDayKey(dayKey) && dayKey >= safeStart && dayKey <= safeEnd;
        })
      : [...daysRaw];

    const rangeStart = (_isDayKey(safeStart) && _isDayKey(safeEnd) && safeStart <= safeEnd)
      ? safeStart
      : String(snapshot?.range?.startDateKey || '');
    const rangeEnd = (_isDayKey(safeStart) && _isDayKey(safeEnd) && safeStart <= safeEnd)
      ? safeEnd
      : String(snapshot?.range?.endDateKey || '');

    return {
      ...snapshot,
      range: {
        startDateKey: rangeStart,
        endDateKey: rangeEnd
      },
      days
    };
  };

  const _shouldUseNlpRangeOutsideUi = ({ validatedSnapshot, periodProbe, effectiveRange }) => {
    const requestedStart = String(effectiveRange?.startDateKey || '');
    const requestedEnd = String(effectiveRange?.endDateKey || '');
    if (!_isDayKey(requestedStart) || !_isDayKey(requestedEnd) || requestedStart > requestedEnd) return false;

    const source = String(periodProbe?.operationsMeta?.source || '');
    if (!source || source === 'snapshot_range' || source === 'empty_range') return false;

    const snapStart = String(validatedSnapshot?.range?.startDateKey || '');
    const snapEnd = String(validatedSnapshot?.range?.endDateKey || '');
    const outsideSnapshot = _isDayKey(snapStart) && _isDayKey(snapEnd)
      ? (requestedStart < snapStart || requestedEnd > snapEnd)
      : true;

    const noData = Boolean(periodProbe?.operationsMeta?.noData);
    const noDataReason = String(periodProbe?.operationsMeta?.noDataReason || '');
    const clampConflict = noData && (
      noDataReason === 'requested_range_outside_snapshot'
      || noDataReason === 'no_days_after_clamp'
      || noDataReason === 'no_days_in_requested_range'
    );

    return outsideSnapshot || clampConflict;
  };

  const _forEachDayKey = (startDateKey, endDateKey, visitor) => {
    if (!_isDayKey(startDateKey) || !_isDayKey(endDateKey) || startDateKey > endDateKey || typeof visitor !== 'function') {
      return;
    }
    const current = new Date(`${startDateKey}T12:00:00`);
    const end = new Date(`${endDateKey}T12:00:00`);
    while (!Number.isNaN(current.getTime()) && current.getTime() <= end.getTime()) {
      visitor(_toDayKey(current));
      current.setDate(current.getDate() + 1);
    }
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

  const _buildSnapshotFromJournalOperations = ({ operations, startDateKey, endDateKey, baseSnapshot }) => {
    if (!_isDayKey(startDateKey) || !_isDayKey(endDateKey) || startDateKey > endDateKey) return null;

    const dayMap = new Map();
    _forEachDayKey(startDateKey, endDateKey, (dayKey) => {
      dayMap.set(dayKey, {
        dateKey: dayKey,
        dateLabel: snapshotAnswerEngine.toRuDateLabel(dayKey),
        totalBalance: 0,
        accountBalances: [],
        totals: {
          income: 0,
          expense: 0
        },
        lists: {
          income: [],
          expense: [],
          withdrawal: [],
          transfer: []
        }
      });
    });

    (Array.isArray(operations) ? operations : []).forEach((op) => {
      const dayKey = String(op?.dateIso || '').slice(0, 10);
      if (!_isDayKey(dayKey) || dayKey < startDateKey || dayKey > endDateKey) return;
      const day = dayMap.get(dayKey);
      if (!day) return;

      const kind = _normalizeKind(op?.type || op?.kind || '');
      const amountAbs = Math.abs(_toNum(op?.amount));
      if (!kind || amountAbs <= 0) return;

      const accountName = String(op?.accountName || 'Без счета');
      const counterparty = String(op?.contractorName || op?.individualName || 'Без контрагента');
      const projectName = String(op?.projectName || 'Без проекта');
      const categoryName = String(op?.categoryName || '').trim();

      if (kind === 'income') {
        day.lists.income.push({
          amount: amountAbs,
          accName: accountName,
          contName: counterparty,
          projName: projectName,
          catName: categoryName || 'Без категории'
        });
        day.totals.income += amountAbs;
        return;
      }

      if (kind === 'expense') {
        day.lists.expense.push({
          amount: -amountAbs,
          accName: accountName,
          contName: counterparty,
          projName: projectName,
          catName: categoryName || 'Без категории'
        });
        day.totals.expense += amountAbs;
        return;
      }

      const isOwnerDrawTransfer = _isOutOfSystemTransferRow(op);
      const fromAccName = String(op?.fromAccountName || op?.accountName || 'Без счета');
      const toAccName = isOwnerDrawTransfer
        ? String(op?.toAccountName || 'Вне системы')
        : String(op?.toAccountName || 'Без счета');
      const transferCategory = (() => {
        if (isOwnerDrawTransfer) {
          if (/вывод\s*средств/i.test(categoryName)) return categoryName;
          return 'Вывод средств';
        }
        return categoryName || 'Перевод';
      })();

      day.lists.transfer.push({
        amount: amountAbs,
        fromAccName,
        toAccName,
        isOutOfSystemTransfer: isOwnerDrawTransfer,
        catName: transferCategory
      });

      if (isOwnerDrawTransfer) {
        day.totals.expense += amountAbs;
      }
    });

    const days = Array.from(dayMap.values()).sort((a, b) => String(a?.dateKey || '').localeCompare(String(b?.dateKey || '')));

    return {
      schemaVersion: 1,
      visibilityMode: String(baseSnapshot?.visibilityMode || 'all'),
      range: {
        startDateKey,
        endDateKey
      },
      days
    };
  };

  const _buildSnapshotFromJournalRange = async ({
    userId,
    startDateKey,
    endDateKey,
    asOf,
    baseSnapshot
  }) => {
    if (!_isDayKey(startDateKey) || !_isDayKey(endDateKey) || startDateKey > endDateKey) return null;

    const quickJournal = await quickJournalAdapter.buildFromJournal({
      userId: String(userId || ''),
      periodFilter: {
        mode: 'custom',
        customStart: `${startDateKey}T00:00:00`,
        customEnd: `${endDateKey}T23:59:59.999`
      },
      asOf: asOf || null,
      categoriesCatalog: []
    });

    return _buildSnapshotFromJournalOperations({
      operations: quickJournal?.operations || [],
      startDateKey,
      endDateKey,
      baseSnapshot
    });
  };

  const _buildComparisonDataFromJournal = async ({
    userId,
    periods,
    asOf,
    baseSnapshot
  }) => {
    const periodRows = Array.isArray(periods) ? periods.filter((p) => _isDayKey(p?.startDateKey) && _isDayKey(p?.endDateKey)) : [];
    if (!periodRows.length) return [];

    const sortedByStart = [...periodRows].sort((a, b) => String(a.startDateKey).localeCompare(String(b.startDateKey)));
    const globalStart = String(sortedByStart[0]?.startDateKey || '');
    const globalEnd = String(sortedByStart[sortedByStart.length - 1]?.endDateKey || '');
    if (!_isDayKey(globalStart) || !_isDayKey(globalEnd) || globalStart > globalEnd) return [];

    const journalSnapshot = await _buildSnapshotFromJournalRange({
      userId,
      startDateKey: globalStart,
      endDateKey: globalEnd,
      asOf,
      baseSnapshot
    });
    if (!journalSnapshot) return [];

    return periodRows.map((period, idx) => {
      const periodSnapshot = _buildSnapshotForRange({
        snapshot: journalSnapshot,
        startDateKey: period.startDateKey,
        endDateKey: period.endDateKey
      });
      const periodFacts = snapshotAnswerEngine.computeDeterministicFacts({
        snapshot: periodSnapshot,
        timelineDateKey: String(period?.endDateKey || asOf || '')
      });
      const opCount = Number(periodFacts?.operationsMeta?.totalCount || 0);

      return {
        index: idx + 1,
        key: String(period?.key || `${period.startDateKey}:${period.endDateKey}`),
        label: String(period?.label || `${period.startDateKey} - ${period.endDateKey}`),
        startDateKey: String(period?.startDateKey || ''),
        endDateKey: String(period?.endDateKey || ''),
        totals: {
          income: _toNum(periodFacts?.totals?.income),
          expense: _toNum(periodFacts?.totals?.expense),
          net: _toNum(periodFacts?.totals?.net)
        },
        ownerDraw: {
          amount: _toNum(periodFacts?.ownerDraw?.amount)
        },
        operationsMeta: {
          totalCount: opCount,
          noData: opCount === 0
        }
      };
    });
  };

  const _dayKeyToDate = (dayKey) => {
    const m = String(dayKey || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 12, 0, 0, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const _resolveHistoryBaseMonth = ({ periodAnalytics, snapshot, timelineDateKey }) => {
    const keyCandidates = [
      String(periodAnalytics?.endDateKey || ''),
      String(snapshot?.range?.endDateKey || ''),
      String(snapshot?.range?.startDateKey || ''),
      String(timelineDateKey || '')
    ];
    for (const key of keyCandidates) {
      if (!_isDayKey(key)) continue;
      const dt = _dayKeyToDate(key);
      if (!dt) continue;
      return {
        year: dt.getFullYear(),
        month: dt.getMonth() + 1
      };
    }
    const now = new Date();
    return {
      year: now.getFullYear(),
      month: now.getMonth() + 1
    };
  };

  const _buildPreviousMonthPeriods = ({ baseYear, baseMonth, monthsBack = 2 }) => {
    const y = Number(baseYear);
    const m = Number(baseMonth);
    const depth = Math.max(1, Math.min(12, Number(monthsBack || 2)));
    if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return [];

    const periods = [];
    for (let step = depth; step >= 1; step -= 1) {
      const d = new Date(y, m - 1 - step, 1, 12, 0, 0, 0);
      const year = d.getFullYear();
      const month = d.getMonth() + 1;
      periods.push({
        period: `${year}-${_pad2(month)}`,
        startDateKey: _startOfMonthDayKey(year, month),
        endDateKey: _endOfMonthDayKey(year, month)
      });
    }
    return periods;
  };

  const _buildSurroundingMonthPeriods = ({
    baseYear,
    baseMonth,
    pastMonths = 3,
    futureMonths = 3
  }) => {
    const y = Number(baseYear);
    const m = Number(baseMonth);
    const past = Math.max(0, Math.min(12, Number(pastMonths || 3)));
    const future = Math.max(0, Math.min(12, Number(futureMonths || 3)));

    if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return [];

    const rows = [];
    for (let offset = -past; offset <= future; offset += 1) {
      const d = new Date(y, m - 1 + offset, 1, 12, 0, 0, 0);
      const periodYear = d.getFullYear();
      const periodMonth = d.getMonth() + 1;
      rows.push({
        period: `${periodYear}-${_pad2(periodMonth)}`,
        relation: offset < 0 ? 'past' : (offset > 0 ? 'future' : 'current'),
        offsetMonths: offset,
        startDateKey: _startOfMonthDayKey(periodYear, periodMonth),
        endDateKey: _endOfMonthDayKey(periodYear, periodMonth)
      });
    }
    return rows;
  };

  const _buildHistoryFromJournal = async ({
    userId,
    baseYear,
    baseMonth,
    monthsBack = 2,
    asOf,
    baseSnapshot
  }) => {
    const periods = _buildPreviousMonthPeriods({
      baseYear,
      baseMonth,
      monthsBack
    });
    if (!periods.length) return [];

    const globalStart = String(periods[0]?.startDateKey || '');
    const globalEnd = String(periods[periods.length - 1]?.endDateKey || '');
    if (!_isDayKey(globalStart) || !_isDayKey(globalEnd) || globalStart > globalEnd) return [];

    const journalSnapshot = await _buildSnapshotFromJournalRange({
      userId: String(userId || ''),
      startDateKey: globalStart,
      endDateKey: globalEnd,
      asOf: asOf || null,
      baseSnapshot
    });
    if (!journalSnapshot) return [];

    return periods.map((period) => {
      const periodSnapshot = _buildSnapshotForRange({
        snapshot: journalSnapshot,
        startDateKey: period.startDateKey,
        endDateKey: period.endDateKey
      });
      const facts = snapshotAnswerEngine.computeDeterministicFacts({
        snapshot: periodSnapshot,
        timelineDateKey: period.endDateKey
      });

      return {
        period: period.period,
        income: _toNum(facts?.totals?.income),
        expense: _toNum(facts?.totals?.expense),
        net: _toNum(facts?.totals?.net)
      };
    });
  };

  const _buildHistoricalContextFromJournal = async ({
    userId,
    baseYear,
    baseMonth,
    pastMonths = 3,
    futureMonths = 3,
    asOf,
    baseSnapshot
  }) => {
    const periods = _buildSurroundingMonthPeriods({
      baseYear,
      baseMonth,
      pastMonths,
      futureMonths
    });

    const centerPeriod = `${Number(baseYear)}-${_pad2(baseMonth)}`;
    const toEmptyRow = (period) => ({
      period: String(period?.period || ''),
      relation: String(period?.relation || ''),
      offsetMonths: Number(period?.offsetMonths || 0),
      range: {
        startDateKey: String(period?.startDateKey || ''),
        endDateKey: String(period?.endDateKey || '')
      },
      totals: {
        income: 0,
        operational_expense: 0,
        net: 0
      },
      topCategories: [],
      ownerDraw: { amount: 0, byCategory: [] },
      endBalances: { open: 0, hidden: 0, total: 0 }
    });

    if (!periods.length) {
      return {
        meta: {
          source: 'backend_journal_fallback',
          generatedAt: new Date().toISOString(),
          centerPeriod,
          expectedPeriods: 0,
          availablePeriods: 0,
          isWarm: false,
          isStale: true,
          lastBuildReason: 'invalid_base_month'
        },
        periods: []
      };
    }

    const globalStart = String(periods[0]?.startDateKey || '');
    const globalEnd = String(periods[periods.length - 1]?.endDateKey || '');
    if (!_isDayKey(globalStart) || !_isDayKey(globalEnd) || globalStart > globalEnd) {
      return {
        meta: {
          source: 'backend_journal_fallback',
          generatedAt: new Date().toISOString(),
          centerPeriod,
          expectedPeriods: periods.length,
          availablePeriods: 0,
          isWarm: false,
          isStale: true,
          lastBuildReason: 'invalid_global_range'
        },
        periods: periods.map(toEmptyRow)
      };
    }

    const journalSnapshot = await _buildSnapshotFromJournalRange({
      userId: String(userId || ''),
      startDateKey: globalStart,
      endDateKey: globalEnd,
      asOf: asOf || null,
      baseSnapshot
    });

    const rows = periods.map((period) => {
      if (!journalSnapshot) return toEmptyRow(period);

      const periodSnapshot = _buildSnapshotForRange({
        snapshot: journalSnapshot,
        startDateKey: period.startDateKey,
        endDateKey: period.endDateKey
      });
      const facts = snapshotAnswerEngine.computeDeterministicFacts({
        snapshot: periodSnapshot,
        timelineDateKey: period.endDateKey
      });

      const topCategories = (Array.isArray(facts?.topExpenseCategories) ? facts.topExpenseCategories : [])
        .slice(0, 5)
        .map((row) => ({
          category: String(row?.category || 'Без категории'),
          amount: _toNum(row?.amount),
          sharePct: _toNum(row?.sharePct)
        }));

      return {
        period: String(period?.period || ''),
        relation: String(period?.relation || ''),
        offsetMonths: Number(period?.offsetMonths || 0),
        range: {
          startDateKey: String(period?.startDateKey || ''),
          endDateKey: String(period?.endDateKey || '')
        },
        totals: {
          income: _toNum(facts?.totals?.income),
          operational_expense: _toNum(facts?.totals?.expense),
          net: _toNum(facts?.totals?.net)
        },
        topCategories,
        ownerDraw: {
          amount: _toNum(facts?.ownerDraw?.amount),
          byCategory: (Array.isArray(facts?.ownerDraw?.byCategory) ? facts.ownerDraw.byCategory : [])
            .slice(0, 10)
            .map((row) => ({
              category: String(row?.category || 'Вывод средств'),
              amount: _toNum(row?.amount)
            }))
        },
        endBalances: {
          open: _toNum(facts?.endBalances?.open),
          hidden: _toNum(facts?.endBalances?.hidden),
          total: _toNum(facts?.endBalances?.total)
        }
      };
    });

    return {
      meta: {
        source: journalSnapshot ? 'backend_journal_fallback' : 'backend_journal_fallback_empty',
        generatedAt: new Date().toISOString(),
        centerPeriod,
        expectedPeriods: periods.length,
        availablePeriods: rows.length,
        isWarm: Boolean(journalSnapshot) && rows.length === periods.length,
        isStale: !journalSnapshot,
        lastBuildReason: 'backend_snapshot_chat_fallback'
      },
      periods: rows
    };
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

  const _buildGraphTooltipData = ({ rows, accounts, periodFilter, asOf }) => {
    const safeRows = Array.isArray(rows) ? rows : [];
    const accountList = Array.isArray(accounts) ? accounts : [];
    const range = _resolveRange(periodFilter, asOf);

    const startDate = new Date(range.startTs);
    const endDate = new Date(range.endTs);
    startDate.setHours(0, 0, 0, 0);
    endDate.setHours(0, 0, 0, 0);

    const startDayKey = _toDayKey(startDate);
    const endDayKey = _toDayKey(endDate);

    const asOfDate = (() => {
      if (asOf) {
        const d = new Date(asOf);
        if (!Number.isNaN(d.getTime())) return d;
      }
      return new Date();
    })();
    const asOfDayKey = _toDayKey(asOfDate);

    const accountById = new Map();
    const accountNameIndex = _buildAccountNameIndex(accountList);
    const runningByAccountId = new Map();

    accountList.forEach((acc) => {
      const id = String(acc?._id || '');
      if (!id) return;
      accountById.set(id, acc);
      const startBalance = Number(acc?.initialBalance ?? acc?.balance ?? 0);
      runningByAccountId.set(id, Number.isFinite(startBalance) ? startBalance : 0);
    });

    const dayAggByKey = new Map();
    const opsByDayKey = new Map();

    const ensureAgg = (dayKey) => {
      if (!dayAggByKey.has(dayKey)) {
        dayAggByKey.set(dayKey, {
          income: 0,
          expense: 0,
          transferOut: 0,
          transferInternal: 0,
          opCount: 0
        });
      }
      return dayAggByKey.get(dayKey);
    };

    const ensureOps = (dayKey) => {
      if (!opsByDayKey.has(dayKey)) opsByDayKey.set(dayKey, []);
      return opsByDayKey.get(dayKey);
    };

    safeRows.forEach((row) => {
      const dayKey = _parseRowDayKey(row);
      if (!dayKey) return;
      if (dayKey < startDayKey || dayKey > endDayKey) return;

      const kind = _normalizeKind(row?.type);
      if (!kind) return;

      const amountAbs = Math.abs(_toNum(row?.amount));
      const status = _normalizeStatus(row?.statusCode, row?.status);
      const outOfSystemTransfer = kind === 'transfer' && _isOutOfSystemTransferRow(row);

      const agg = ensureAgg(dayKey);
      agg.opCount += 1;
      if (kind === 'income') agg.income += amountAbs;
      if (kind === 'expense') agg.expense += amountAbs;
      if (kind === 'transfer' && outOfSystemTransfer) {
        agg.transferOut += amountAbs;
        agg.expense += amountAbs;
      }
      if (kind === 'transfer' && !outOfSystemTransfer) {
        agg.transferInternal += amountAbs;
      }

      ensureOps(dayKey).push({
        id: String(row?.id || row?._id || ''),
        kind,
        status,
        amount: amountAbs,
        account: String(row?.account || ''),
        accountId: _extractId(row?.accountId),
        fromAccountId: _extractId(row?.fromAccountId),
        toAccountId: _extractId(row?.toAccountId),
        category: String(row?.category || 'Без категории'),
        project: String(row?.project || 'Без проекта'),
        contractor: String(row?.contractor || ''),
        owner: String(row?.owner || ''),
        outOfSystemTransfer
      });
    });

    const dayKeys = [];
    for (let dt = new Date(startDate); dt.getTime() <= endDate.getTime(); dt.setDate(dt.getDate() + 1)) {
      dayKeys.push(_toDayKey(dt));
    }

    const addDelta = (accountId, delta) => {
      const id = String(accountId || '');
      if (!id || !runningByAccountId.has(id)) return;
      runningByAccountId.set(id, Number(runningByAccountId.get(id) || 0) + Number(delta || 0));
    };

    const applyOperationToBalances = (op) => {
      if (!op || !accountList.length) return;
      const amount = Math.abs(Number(op.amount || 0));
      if (!amount) return;

      if (op.kind === 'transfer') {
        const fromId = String(op.fromAccountId || '');
        const toId = String(op.toAccountId || '');

        if (fromId) addDelta(fromId, -amount);
        if (!op.outOfSystemTransfer && toId) addDelta(toId, amount);

        if (!fromId && !toId) {
          const transferParts = _splitTransferAccountLabel(op.account);
          if (!transferParts) return;
          const fromAcc = _resolveAccountByLabel(transferParts.from, accountList, accountNameIndex);
          const toAcc = _resolveAccountByLabel(transferParts.to, accountList, accountNameIndex);
          if (fromAcc?._id) addDelta(fromAcc._id, -amount);
          if (!op.outOfSystemTransfer && toAcc?._id) addDelta(toAcc._id, amount);
        }
        return;
      }

      const accountId = String(op.accountId || '');
      const accountResolved = accountId
        ? accountById.get(accountId)
        : _resolveAccountByLabel(op.account, accountList, accountNameIndex);
      if (!accountResolved?._id) return;

      if (op.kind === 'income') addDelta(accountResolved._id, amount);
      if (op.kind === 'expense') addDelta(accountResolved._id, -amount);
    };

    const daily = [];
    const accountBalancesByDay = [];

    dayKeys.forEach((dayKey) => {
      const dayOps = opsByDayKey.get(dayKey) || [];
      dayOps.forEach((op) => applyOperationToBalances(op));

      const balances = [];
      let openBalance = 0;
      let hiddenBalance = 0;
      accountList.forEach((acc) => {
        const id = String(acc?._id || '');
        if (!id) return;
        const raw = Number(runningByAccountId.get(id) || 0);
        const balance = Math.max(0, raw);
        const isHidden = !!(acc?.isHidden || acc?.isExcluded);
        if (isHidden) hiddenBalance += balance;
        else openBalance += balance;

        balances.push({
          accountId: id,
          name: String(acc?.name || 'Счет'),
          isHidden: !!acc?.isHidden,
          isExcluded: !!acc?.isExcluded,
          balance
        });
      });

      const agg = dayAggByKey.get(dayKey) || {
        income: 0,
        expense: 0,
        transferOut: 0,
        transferInternal: 0,
        opCount: 0
      };

      daily.push({
        dayKey,
        dateLabel: _dayKeyToLabel(dayKey),
        income: Number(agg.income || 0),
        expense: Number(agg.expense || 0),
        transferOut: Number(agg.transferOut || 0),
        transferInternal: Number(agg.transferInternal || 0),
        opCount: Number(agg.opCount || 0),
        net: Number(agg.income || 0) - Number(agg.expense || 0),
        balanceOpen: openBalance,
        balanceHidden: hiddenBalance,
        balanceTotal: openBalance + hiddenBalance
      });

      accountBalancesByDay.push({
        dayKey,
        dateLabel: _dayKeyToLabel(dayKey),
        accounts: balances
      });
    });

    const pickByDayKey = (targetDayKey) => {
      const exact = accountBalancesByDay.find((x) => x.dayKey === targetDayKey);
      if (exact) return exact.accounts;
      if (targetDayKey < startDayKey) return [];
      if (targetDayKey > endDayKey) {
        const last = accountBalancesByDay[accountBalancesByDay.length - 1];
        return Array.isArray(last?.accounts) ? last.accounts : [];
      }
      return [];
    };

    const operationsByDay = dayKeys
      .map((dayKey) => ({
        dayKey,
        dateLabel: _dayKeyToLabel(dayKey),
        items: (opsByDayKey.get(dayKey) || []).slice().sort((a, b) => Number(b.amount || 0) - Number(a.amount || 0))
      }))
      .filter((x) => x.items.length > 0);

    return {
      period: {
        startDayKey,
        endDayKey,
        startLabel: range.startLabel,
        endLabel: range.endLabel
      },
      asOfDayKey,
      daily,
      accountBalancesByDay,
      operationsByDay,
      accountBalancesAtAsOf: pickByDayKey(asOfDayKey),
      accountBalancesAtPeriodEnd: pickByDayKey(endDayKey)
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
    const model = process.env.OPENAI_MODEL || 'gpt-4o';

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

  router.get('/rag/health', isAuthenticated, async (req, res) => {
    try {
      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const dbUrl = String(process.env.DB_URL || '');
      const atlas = dbUrl.startsWith('mongodb+srv://');
      const openAiKeyPresent = Boolean(process.env.OPENAI_KEY || process.env.OPENAI_API_KEY);
      const embeddingModel = String(process.env.OPENAI_EMBED_MODEL || 'text-embedding-3-small');
      const collectionName = String(process.env.RAG_KB_COLLECTION || 'ai_cfo_knowledge');
      const vectorIndex = String(process.env.RAG_KB_VECTOR_INDEX || 'vector_index');

      const readyState = Number(mongoose?.connection?.readyState || 0);
      const connected = readyState === 1;
      const dbName = connected
        ? String(mongoose?.connection?.db?.databaseName || mongoose?.connection?.name || '')
        : null;

      let docsCount = null;
      let sampleDocIds = [];
      let vectorIndexes = [];
      let vectorIndexExists = false;
      let diagnosticsError = null;

      if (connected && mongoose?.connection?.db) {
        try {
          const col = mongoose.connection.db.collection(collectionName);
          docsCount = await col.countDocuments({});
          const sample = await col.find(
            {},
            { projection: { _id: 0, id: 1, title: 1 } }
          ).limit(5).toArray();
          sampleDocIds = (Array.isArray(sample) ? sample : [])
            .map((row) => String(row?.id || row?.title || '').trim())
            .filter(Boolean);

          vectorIndexes = await _listSearchIndexes(col, vectorIndex);
          vectorIndexExists = Array.isArray(vectorIndexes) && vectorIndexes.length > 0;
        } catch (error) {
          diagnosticsError = String(error?.message || error);
        }
      }

      const probeEnabled = String(req.query?.probe || '').toLowerCase() === 'true';
      let probe = null;

      if (probeEnabled) {
        try {
          const probeResult = await cfoKnowledgeBase.retrieveCfoContext({
            question: 'проверка rag ликвидность налоги',
            responseIntent: { intent: 'advisory' },
            accountContext: { mode: 'liquidity' },
            advisoryFacts: {
              nextExpenseLiquidity: {
                hasCashGap: false,
                expense: 0,
                postExpenseOpenFmt: '0 т'
              }
            },
            derivedSemantics: { monthForecastNet: 0, monthForecastNetFmt: '0 т' },
            scenarioCalculator: { enabled: false, hasLifeSpendConstraint: false },
            limit: 3
          });

          probe = {
            ok: true,
            source: String(probeResult?.source || ''),
            atlas: probeResult?.atlas || null,
            itemCount: Array.isArray(probeResult?.items) ? probeResult.items.length : 0
          };
        } catch (error) {
          probe = {
            ok: false,
            error: String(error?.message || error)
          };
        }
      }

      const ok = atlas
        && connected
        && openAiKeyPresent
        && vectorIndexExists
        && (Number(docsCount || 0) > 0);

      return res.json({
        ok,
        status: ok ? 'ready' : 'degraded',
        atlas: {
          detected: atlas,
          dbUrlKind: atlas ? 'mongodb+srv' : 'mongodb/other'
        },
        mongodb: {
          connected,
          readyState,
          dbName
        },
        rag: {
          collection: collectionName,
          docsCount,
          sampleDocIds,
          vectorIndex,
          vectorIndexExists,
          vectorIndexCount: Array.isArray(vectorIndexes) ? vectorIndexes.length : 0
        },
        openai: {
          keyPresent: openAiKeyPresent,
          embeddingModel
        },
        diagnosticsError,
        probe
      });
    } catch (error) {
      console.error('[AI RAG Health] Error:', error);
      return res.status(500).json({
        ok: false,
        status: 'error',
        error: String(error?.message || error)
      });
    }
  });

  const _extractTimelineDate = (raw) => {
    const value = String(raw || '').trim();
    const direct = value.match(/^(\d{4}-\d{2}-\d{2})$/);
    if (direct) return direct[1];
    const fromIso = value.match(/^(\d{4}-\d{2}-\d{2})T/);
    if (fromIso) return fromIso[1];
    return null;
  };

  // 🟢 GET /api/ai/history - Load chat history for current timeline date  
  router.get('/history', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');
      if (!userIdStr) return res.status(401).json({ error: 'Пользователь не найден' });

      const timelineDate =
        _extractTimelineDate(req.query?.timelineDate) ||
        _extractTimelineDate(req.query?.asOf) ||
        new Date().toISOString().slice(0, 10);

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

  // DELETE /api/ai/history - reset chat history (all or keep one day)
  router.delete('/history', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');
      if (!userIdStr) return res.status(401).json({ error: 'Пользователь не найден' });

      const keepDate =
        _extractTimelineDate(req.query?.keepDate) ||
        _extractTimelineDate(req.query?.timelineDate) ||
        null;

      const filter = keepDate
        ? { userId: userIdStr, timelineDate: { $ne: keepDate } }
        : { userId: userIdStr };

      const result = await ChatHistory.deleteMany(filter);
      return res.json({
        ok: true,
        deletedCount: Number(result?.deletedCount || 0),
        keepDate: keepDate || null
      });
    } catch (error) {
      console.error('[AI History] Reset error:', error);
      return res.status(500).json({ error: error.message });
    }
  });

  // GET /api/ai/llm-input/latest - Download latest LLM input snapshot JSON
  router.get('/llm-input/latest', isAuthenticated, async (req, res) => {
    try {
      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const filePath = path.join(LLM_SNAPSHOT_DIR, 'llm-input-latest.json');
      try {
        await fs.access(filePath);
      } catch (_) {
        return res.status(404).json({ error: 'Снапшот не найден. Сначала выполните запрос к агенту.' });
      }

      let normalizedBody = null;
      try {
        const raw = await fs.readFile(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        const normalized = _normalizeLlmInputSnapshot(parsed);
        normalizedBody = JSON.stringify(normalized, null, 2);
        if (normalizedBody !== raw) {
          await fs.writeFile(filePath, normalizedBody, 'utf8');
        }
      } catch (normalizeError) {
        console.warn('[AI Snapshot] latest snapshot normalize failed:', normalizeError?.message || normalizeError);
      }

      if (normalizedBody) {
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename=\"llm-input-latest.json\"');
        return res.status(200).send(normalizedBody);
      }

      return res.download(filePath, 'llm-input-latest.json');
    } catch (error) {
      console.error('[AI Snapshot] Download latest error:', error);
      return res.status(500).json({ error: 'Ошибка скачивания снапшота' });
    }
  });

  // GET /api/ai/llm-input/archive - List available archived snapshots
  router.get('/llm-input/archive', isAuthenticated, async (req, res) => {
    try {
      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      let files = [];
      try {
        files = await fs.readdir(LLM_SNAPSHOT_DIR);
      } catch (_) {
        files = [];
      }

      const archiveFiles = files
        .filter((name) => /^llm-input-[0-9]{4}-[0-9]{2}-[0-9]{2}T.*\.json$/.test(name))
        .sort((a, b) => b.localeCompare(a));

      return res.json({ files: archiveFiles });
    } catch (error) {
      console.error('[AI Snapshot] List archive error:', error);
      return res.status(500).json({ error: 'Ошибка получения списка снапшотов' });
    }
  });

  // GET /api/ai/llm-input/archive/:file - Download archived snapshot by filename
  router.get('/llm-input/archive/:file', isAuthenticated, async (req, res) => {
    try {
      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const fileName = String(req.params?.file || '').trim();
      if (!/^llm-input-[0-9]{4}-[0-9]{2}-[0-9]{2}T.*\.json$/.test(fileName)) {
        return res.status(400).json({ error: 'Неверное имя файла снапшота' });
      }

      const filePath = path.join(LLM_SNAPSHOT_DIR, fileName);
      try {
        await fs.access(filePath);
      } catch (_) {
        return res.status(404).json({ error: 'Снапшот не найден' });
      }

      return res.download(filePath, fileName);
    } catch (error) {
      console.error('[AI Snapshot] Download archive error:', error);
      return res.status(500).json({ error: 'Ошибка скачивания снапшота' });
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
      const isQuickButton = source === 'quick_button' && Boolean(req?.body?.action);

      if (!isQuickButton) {
        const debugEnabled = req?.body?.debugAi === true;
        const tooltipSnapshotRaw = (() => {
          if (req?.body?.tooltipSnapshot) return req.body.tooltipSnapshot;
          if (req?.body?.snapshot?.tooltipSnapshot) return req.body.snapshot.tooltipSnapshot;
          if (req?.body?.tableContext?.tooltipSnapshot) return req.body.tableContext.tooltipSnapshot;
          if (req?.body?.snapshot?.schemaVersion && Array.isArray(req?.body?.snapshot?.days)) return req.body.snapshot;
          return null;
        })();
        if (!tooltipSnapshotRaw) {
          const bodyKeys = Object.keys(req?.body || {});
          const snapshotKeys = req?.body?.snapshot && typeof req.body.snapshot === 'object'
            ? Object.keys(req.body.snapshot)
            : [];
          return res.status(400).json({
            error: 'Отсутствует tooltipSnapshot. Обновите страницу и повторите запрос.',
            debug: {
              source,
              bodyKeys,
              hasSnapshot: !!req?.body?.snapshot,
              snapshotKeys
            }
          });
        }
        const validation = snapshotAnswerEngine.validateTooltipSnapshot(tooltipSnapshotRaw);
        if (!validation.ok) {
          return res.status(400).json({
            error: `Некорректный tooltipSnapshot: ${validation.error}`
          });
        }

        const validatedSnapshot = validation.snapshot;
        const asOf = req.body?.asOf || null;
        const timelineDateRaw = String(req?.body?.timelineDate || '').trim();
        const timelineDate = (() => {
          const direct = timelineDateRaw.match(/^(\d{4}-\d{2}-\d{2})/);
          if (direct) return direct[1];
          const fromAsOf = String(asOf || '').match(/^(\d{4}-\d{2}-\d{2})/);
          if (fromAsOf) return fromAsOf[1];
          return new Date().toISOString().slice(0, 10);
        })();
        let effectiveUserId = userId;
        if (typeof getCompositeUserId === 'function') {
          try {
            effectiveUserId = await getCompositeUserId(req);
          } catch (_) {
            effectiveUserId = userId;
          }
        }

        const parsedIntent = snapshotIntentParser.parseSnapshotIntent({
          question: q,
          timelineDateKey: timelineDate,
          snapshot: validatedSnapshot
        });
        const comparisonQuery = snapshotAnswerEngine.resolveComparisonQueryFromQuestion({
          question: q,
          timelineDateKey: timelineDate,
          snapshot: validatedSnapshot
        });
        const isComparisonMode = Array.isArray(comparisonQuery?.periods) && comparisonQuery.periods.length >= 2;

        const periodProbe = isComparisonMode
          ? null
          : snapshotAnswerEngine.computePeriodAnalytics({
            snapshot: validatedSnapshot,
            question: q,
            timelineDateKey: timelineDate,
            disableSnapshotClamp: true
          });

        const effectiveRange = isComparisonMode
          ? {
            startDateKey: String(validatedSnapshot?.range?.startDateKey || ''),
            endDateKey: String(validatedSnapshot?.range?.endDateKey || ''),
            source: 'comparison_mode_keep_snapshot'
          }
          : _resolveEffectiveSnapshotRange({
            snapshot: validatedSnapshot,
            parsedIntent,
            periodProbe
          });

        const snapshot = isComparisonMode
          ? validatedSnapshot
          : _buildSnapshotForRange({
            snapshot: validatedSnapshot,
            startDateKey: effectiveRange.startDateKey,
            endDateKey: effectiveRange.endDateKey
          });
        let periodAnalyticsSnapshot = snapshot;

        if (!isComparisonMode && _shouldUseNlpRangeOutsideUi({ validatedSnapshot, periodProbe, effectiveRange })) {
          try {
            const byJournalRangeSnapshot = await _buildSnapshotFromJournalRange({
              userId: String(effectiveUserId || userId || ''),
              startDateKey: effectiveRange.startDateKey,
              endDateKey: effectiveRange.endDateKey,
              asOf: timelineDate,
              baseSnapshot: validatedSnapshot
            });
            if (byJournalRangeSnapshot) {
              periodAnalyticsSnapshot = byJournalRangeSnapshot;
            }
          } catch (journalRangeError) {
            console.warn('[AI Snapshot] NLP range journal fallback failed:', journalRangeError?.message || journalRangeError);
          }
        }

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

        const snapshotChecksum = _normalizeSnapshotChecksum(req?.body?.snapshotChecksum);
        const requestedDataChanged = _parseBooleanFlag(req?.body?.isDataChanged);
        const lastSnapshotChecksum = _extractLastSnapshotChecksum(chatHistory?.messages);
        const checksumChanged = Boolean(snapshotChecksum && lastSnapshotChecksum && snapshotChecksum !== lastSnapshotChecksum);
        const checksumBaselineMissing = Boolean(
          snapshotChecksum
          && !lastSnapshotChecksum
          && Array.isArray(chatHistory?.messages)
          && chatHistory.messages.length > 0
        );
        const isDataChangedEffective = requestedDataChanged || checksumChanged || checksumBaselineMissing;
        const historyResetApplied = isDataChangedEffective && Array.isArray(chatHistory.messages) && chatHistory.messages.length > 0;
        if (historyResetApplied) {
          chatHistory.messages = [];
        }

        chatHistory.messages.push({
          role: 'user',
          content: q,
          timestamp: new Date(),
          metadata: {
            snapshotChecksum: snapshotChecksum || null,
            isDataChanged: isDataChangedEffective,
            requestedDataChanged,
            checksumChanged,
            checksumBaselineMissing,
            historyResetApplied
          }
        });
        chatHistory.updatedAt = new Date();

        const deterministicFacts = snapshotAnswerEngine.computeDeterministicFacts({
          snapshot,
          timelineDateKey: timelineDate,
        });
        let historicalContext = _normalizeHistoricalContext(req?.body?.historicalContext);
        const periodAnalytics = isComparisonMode
          ? null
          : snapshotAnswerEngine.computePeriodAnalytics({
            snapshot: periodAnalyticsSnapshot,
            question: q,
            timelineDateKey: timelineDate,
            disableSnapshotClamp: true
          });
        if (periodAnalytics) {
          deterministicFacts.periodAnalytics = periodAnalytics;
        }
        if (isComparisonMode) {
          const comparisonData = await _buildComparisonDataFromJournal({
            userId: String(effectiveUserId || userId || ''),
            periods: comparisonQuery?.periods || [],
            asOf: timelineDate,
            baseSnapshot: validatedSnapshot
          });
          deterministicFacts.comparisonData = comparisonData;
          deterministicFacts.comparisonMeta = {
            mode: 'comparison',
            source: String(comparisonQuery?.source || 'comparison_query'),
            compareKeyword: Boolean(comparisonQuery?.compareKeyword),
            requestedPeriods: Array.isArray(comparisonQuery?.periods) ? comparisonQuery.periods : [],
            generatedCount: Array.isArray(comparisonData) ? comparisonData.length : 0
          };
        }
        const historyBase = _resolveHistoryBaseMonth({
          periodAnalytics,
          snapshot: periodAnalyticsSnapshot || snapshot,
          timelineDateKey: timelineDate
        });
        try {
          const historyPeriods = _buildPreviousMonthPeriods({
            baseYear: historyBase.year,
            baseMonth: historyBase.month,
            monthsBack: 2
          });
          const history = await _buildHistoryFromJournal({
            userId: String(effectiveUserId || userId || ''),
            baseYear: historyBase.year,
            baseMonth: historyBase.month,
            monthsBack: 2,
            asOf: timelineDate,
            baseSnapshot: validatedSnapshot
          });
          const fallbackHistory = historyPeriods.map((row) => ({
            period: String(row?.period || ''),
            income: 0,
            expense: 0,
            net: 0
          }));
          const mergedHistory = (() => {
            const direct = Array.isArray(history) ? history : [];
            if (!direct.length) return fallbackHistory;
            const byPeriod = new Map();
            direct.forEach((row) => {
              byPeriod.set(String(row?.period || ''), {
                period: String(row?.period || ''),
                income: _toNum(row?.income),
                expense: _toNum(row?.expense),
                net: _toNum(row?.net)
              });
            });
            return historyPeriods.map((row) => {
              const key = String(row?.period || '');
              return byPeriod.get(key) || {
                period: key,
                income: 0,
                expense: 0,
                net: 0
              };
            });
          })();

          deterministicFacts.history = mergedHistory;
          deterministicFacts.historyMeta = {
            basePeriod: `${historyBase.year}-${_pad2(historyBase.month)}`,
            monthsBack: 2,
            generatedCount: mergedHistory.length
          };
        } catch (historyError) {
          deterministicFacts.history = _buildPreviousMonthPeriods({
            baseYear: historyBase.year,
            baseMonth: historyBase.month,
            monthsBack: 2
          }).map((row) => ({
            period: String(row?.period || ''),
            income: 0,
            expense: 0,
            net: 0
          }));
          deterministicFacts.historyMeta = {
            basePeriod: `${historyBase.year}-${_pad2(historyBase.month)}`,
            monthsBack: 2,
            generatedCount: Array.isArray(deterministicFacts.history) ? deterministicFacts.history.length : 0,
            error: String(historyError?.message || historyError || 'history_generation_failed')
          };
          console.warn('[AI Snapshot] History generation failed:', historyError?.message || historyError);
        }

        if (!historicalContext) {
          try {
            historicalContext = await _buildHistoricalContextFromJournal({
              userId: String(effectiveUserId || userId || ''),
              baseYear: historyBase.year,
              baseMonth: historyBase.month,
              pastMonths: 3,
              futureMonths: 3,
              asOf: timelineDate,
              baseSnapshot: validatedSnapshot
            });
          } catch (historicalContextError) {
            console.warn('[AI Snapshot] Historical context fallback failed:', historicalContextError?.message || historicalContextError);
          }
        }

        if (!historicalContext || !Array.isArray(historicalContext?.periods)) {
          historicalContext = {
            meta: {
              source: 'missing',
              generatedAt: new Date().toISOString(),
              centerPeriod: `${historyBase.year}-${_pad2(historyBase.month)}`,
              expectedPeriods: 0,
              availablePeriods: 0,
              isWarm: false,
              isStale: true,
              lastBuildReason: 'no_context_available'
            },
            periods: []
          };
        }
        deterministicFacts.historicalContext = historicalContext;

        if (parsedIntent?.type === 'CATEGORY_FACT_BY_CATEGORY') {
          const deterministic = snapshotAnswerEngine.answerFromSnapshot({
            snapshot,
            intent: parsedIntent,
            timelineDateKey: timelineDate
          });

          const responseText = deterministic?.ok
            ? String(deterministic?.text || '').trim()
            : (String(deterministic?.text || '').trim() || 'Не удалось рассчитать показатель по категории.');
          const responseMode = 'snapshot_category_fact';

          const llmInputSnapshot = await _dumpLlmInputSnapshot({
            generatedAt: new Date().toISOString(),
            mode: 'snapshot_deterministic_category_fact',
            question: q,
            source,
            timelineDate,
            parsedIntent,
            historicalContext,
            deterministicFacts,
            periodAnalytics,
            deterministicResult: {
              ok: deterministic?.ok === true,
              text: deterministic?.text || '',
              meta: deterministic?.meta || null
            },
            tooltipSnapshot: snapshot
          });

          chatHistory.messages.push({
            role: 'assistant',
            content: responseText,
            timestamp: new Date(),
            metadata: {
              responseMode,
              intent: parsedIntent,
              deterministicFacts,
              periodAnalytics,
              deterministicMeta: deterministic?.meta || null,
              requestMeta: {
                snapshotChecksum: snapshotChecksum || null,
                isDataChanged: isDataChangedEffective,
                requestedDataChanged,
                checksumChanged,
                checksumBaselineMissing,
                historyResetApplied
              }
            }
          });
          await chatHistory.save();

          return res.json({
            text: responseText,
            ...(debugEnabled ? {
              debug: {
                timelineDate,
                responseMode,
                parsedIntent,
                deterministicFacts,
                periodAnalytics,
                deterministicMeta: deterministic?.meta || null,
                snapshotChecksum: snapshotChecksum || null,
                isDataChanged: isDataChangedEffective,
                requestedDataChanged,
                checksumChanged,
                checksumBaselineMissing,
                historyResetApplied,
                historyLength: chatHistory.messages.length,
                llmInputSnapshot
              }
            } : {})
          });
        }

        const llmResult = await conversationalAgent.generateSnapshotChatResponse({
          question: q,
          history: isDataChangedEffective ? [] : chatHistory.messages.slice(0, -1),
          snapshot,
          deterministicFacts,
          periodAnalytics,
          snapshotMeta: {
            range: snapshot.range,
            visibilityMode: snapshot.visibilityMode,
            timelineDate
          }
        });

        const llmErrorText = String(llmResult?.text || '').trim();
        const llmErrorCode = String(llmResult?.debug?.code || '').trim();
        const isQuotaError = !llmResult?.ok && (
          llmErrorCode === 'quota_exceeded'
          || /(^|[\s:])429([\s:]|$)/i.test(llmErrorText)
          || /quota|billing/i.test(llmErrorText)
        );
        const isQualityGateError = !llmResult?.ok && (
          llmErrorCode === 'quality_gate_failed'
          || /^LLM ответ отклонен контролем качества/i.test(llmErrorText)
          || /QUALITY_GATE_/i.test(llmErrorText)
        );

        const fallbackDeterministicText = snapshotAnswerEngine.buildDeterministicInsightsBlock(deterministicFacts);
        const responseText = llmResult?.ok
          ? String(llmResult?.text || '').trim()
          : (
              isQuotaError
                ? `${fallbackDeterministicText}\n\nLLM временно недоступен (лимит API 429). Проверьте billing/квоту OpenAI.`
                : (isQualityGateError
                    ? `${fallbackDeterministicText}\n\nLLM ответ отклонен контролем качества. Показываю проверенный детерминированный срез.`
                    : (llmErrorText || 'Не удалось сформировать ответ. Проверьте данные snapshot.'))
            );
        const responseMode = llmResult?.ok
          ? 'llm_snapshot_chat'
          : (isQuotaError
              ? 'snapshot_fallback_quota'
              : (isQualityGateError ? 'snapshot_fallback_quality_gate' : 'llm_snapshot_chat_error'));
        const qualityGateFromLlm = llmResult?.debug?.qualityGate || null;
        const qualityGate = (() => {
          if (qualityGateFromLlm) {
            const audit = qualityGateFromLlm?.audit || {};
            return {
              applied: true,
              passed: Boolean(audit?.ok === true),
              attempts: Number(qualityGateFromLlm?.attempts || 0),
              errors: Array.isArray(audit?.errors) ? audit.errors : [],
              warnings: Array.isArray(audit?.warnings) ? audit.warnings : []
            };
          }
          if (isQualityGateError) {
            return {
              applied: true,
              passed: false,
              attempts: 0,
              errors: [llmErrorText || 'quality_gate_failed'],
              warnings: []
            };
          }
          return {
            applied: false,
            passed: null,
            attempts: 0,
            errors: [],
            warnings: []
          };
        })();
        const discriminatorLog = {
          applied: Boolean(qualityGate?.applied),
          passed: qualityGate?.passed === true,
          attempts: Number(qualityGate?.attempts || 0),
          errors: Array.isArray(qualityGate?.errors) ? qualityGate.errors : [],
          warnings: Array.isArray(qualityGate?.warnings) ? qualityGate.warnings : [],
          mode: responseMode,
          llmErrorCode: llmErrorCode || null,
          llmErrorText: llmErrorText || null,
          llmQualityGateRaw: qualityGateFromLlm || null
        };

        const llmInputSnapshot = await _dumpLlmInputSnapshot({
          generatedAt: new Date().toISOString(),
          mode: 'snapshot_llm_chat',
          question: q,
          source,
          timelineDate,
          historicalContext,
          deterministicFacts,
          periodAnalytics,
          llm: llmResult ? {
            ok: llmResult.ok,
            text: llmResult.text,
            debug: llmResult.debug || null
          } : null,
          requestMeta: {
            asOf,
            periodFilter: req?.body?.periodFilter || null,
            snapshotChecksum: snapshotChecksum || null,
            isDataChanged: isDataChangedEffective,
            requestedDataChanged,
            checksumChanged,
            checksumBaselineMissing,
            historyResetApplied,
            historicalContextSummary: {
              hasHistoricalContext: Boolean(historicalContext),
              periodsCount: Array.isArray(historicalContext?.periods) ? historicalContext.periods.length : 0,
              source: String(historicalContext?.meta?.source || '')
            },
            tableContextSummary: {
              hasTableContext: !!req?.body?.tableContext,
              rowCount: Array.isArray(req?.body?.tableContext?.rows)
                ? req.body.tableContext.rows.length
                : 0
            }
          },
          discriminatorLog,
          tooltipSnapshot: snapshot
        });

        chatHistory.messages.push({
          role: 'assistant',
          content: responseText,
          timestamp: new Date(),
          metadata: {
            responseMode,
            deterministicFacts,
            periodAnalytics,
            qualityGate,
            discriminatorLog,
            llm: llmResult?.debug || null,
            requestMeta: {
              snapshotChecksum: snapshotChecksum || null,
              isDataChanged: isDataChangedEffective,
              requestedDataChanged,
              checksumChanged,
              checksumBaselineMissing,
              historyResetApplied
            }
          }
        });
        await chatHistory.save();

        return res.json({
          text: responseText,
          qualityGate,
          discriminatorLog,
          ...(debugEnabled ? {
            debug: {
              timelineDate,
              deterministicFacts,
              periodAnalytics,
              qualityGate,
              discriminatorLog,
              snapshotChecksum: snapshotChecksum || null,
              isDataChanged: isDataChangedEffective,
              requestedDataChanged,
              checksumChanged,
              checksumBaselineMissing,
              historyResetApplied,
              llm: llmResult?.debug || null,
              responseMode,
              historyLength: chatHistory.messages.length,
              llmInputSnapshot
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
