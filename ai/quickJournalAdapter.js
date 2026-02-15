// backend/ai/quickJournalAdapter.js
// Build quick-mode operations from the same source/rules as Operations Editor.

function _pickId(value) {
  if (!value) return null;
  if (typeof value === 'object') return value._id ? String(value._id) : (value.id ? String(value.id) : null);
  return String(value);
}

function _pickName(value) {
  if (!value || typeof value !== 'object') return null;
  return value.name || value.title || value.label || null;
}

function _fmtDateDDMMYY(d) {
  if (!d || !(d instanceof Date) || Number.isNaN(d.getTime())) return null;
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yy = String(d.getFullYear()).slice(2);
  return `${dd}.${mm}.${yy}`;
}

function _startOfDayLocal(date) {
  const d = new Date(date.getTime());
  d.setHours(0, 0, 0, 0);
  return d;
}

function _endOfDayLocal(date) {
  const d = new Date(date.getTime());
  d.setHours(23, 59, 59, 999);
  return d;
}

function _resolveRange(periodFilter, asOf) {
  const nowRef = (() => {
    if (asOf) {
      const parsed = new Date(asOf);
      if (!Number.isNaN(parsed.getTime())) return parsed;
    }
    return new Date();
  })();

  let start = null;
  let end = null;

  if (periodFilter && periodFilter.mode === 'custom') {
    if (periodFilter.customStart) {
      const rawStart = String(periodFilter.customStart);
      const parsedStart = new Date(rawStart);
      if (!Number.isNaN(parsedStart.getTime())) {
        start = /T\d{2}:\d{2}/.test(rawStart) ? parsedStart : _startOfDayLocal(parsedStart);
      }
    }
    if (periodFilter.customEnd) {
      const rawEnd = String(periodFilter.customEnd);
      const parsedEnd = new Date(rawEnd);
      if (!Number.isNaN(parsedEnd.getTime())) {
        end = /T\d{2}:\d{2}/.test(rawEnd) ? parsedEnd : _endOfDayLocal(parsedEnd);
      }
    }
  }

  if (!start || !end) {
    start = _startOfDayLocal(new Date(nowRef.getFullYear(), nowRef.getMonth(), 1));
    end = _endOfDayLocal(new Date(nowRef.getFullYear(), nowRef.getMonth() + 1, 0));
  }

  return { start, end, nowRef };
}

function _isPersonalTransferWithdrawal(op) {
  return !!op
    && op.transferPurpose === 'personal'
    && op.transferReason === 'personal_use';
}

function _normalizeTypeLabel(op) {
  if (op?.isWorkAct) return 'Акт выполненных работ';
  if (op?.type === 'withdrawal' || op?.isWithdrawal) return 'Вывод средств';
  if (op?.type === 'transfer' || op?.isTransfer || _isPersonalTransferWithdrawal(op)) return 'Перевод';
  if (op?.type === 'prepayment') return 'Предоплата';
  if (op?.type === 'income') return 'Доход';
  if (op?.type === 'expense') return 'Расход';
  return 'Операция';
}

function _normalizeStatusLabel(op, todayStartTs) {
  const opDate = new Date(op?.date);
  let opTs = NaN;
  if (!Number.isNaN(opDate.getTime())) {
    opDate.setHours(0, 0, 0, 0);
    opTs = opDate.getTime();
  }
  const isPlan = op?.status === 'plan' || (!Number.isNaN(opTs) && opTs > todayStartTs);
  return isPlan ? 'План' : 'Исполнено';
}

function _shouldIncludeInOperationsEditor(op) {
  if (!op) return false;
  if (op.isSplitParent) return false;
  if (op.excludeFromTotals && !op.offsetIncomeId) return false;
  return true;
}

function mergeLegacyInterCompanyTransfers(events = []) {
  const passthrough = [];
  const groupedLegacy = new Map();

  events.forEach((event) => {
    const hasGroup = !!event?.transferGroupId;
    const isModernTransfer = event?.isTransfer === true || event?.type === 'transfer';

    if (!hasGroup || isModernTransfer) {
      passthrough.push(event);
      return;
    }

    if (!groupedLegacy.has(event.transferGroupId)) {
      groupedLegacy.set(event.transferGroupId, []);
    }
    groupedLegacy.get(event.transferGroupId).push(event);
  });

  groupedLegacy.forEach((items, groupId) => {
    if (!Array.isArray(items) || items.length !== 2) {
      passthrough.push(...items);
      return;
    }

    const outgoing = items.find(item => Number(item?.amount) < 0 || item?.type === 'expense');
    const incoming = items.find(item => Number(item?.amount) > 0 || item?.type === 'income');

    if (!outgoing || !incoming) {
      passthrough.push(...items);
      return;
    }

    const absAmount = Math.max(
      Math.abs(Number(outgoing.amount) || 0),
      Math.abs(Number(incoming.amount) || 0)
    );

    passthrough.push({
      ...incoming,
      _id: incoming._id,
      _id2: outgoing._id,
      type: 'transfer',
      isTransfer: true,
      transferPurpose: incoming.transferPurpose || outgoing.transferPurpose || 'inter_company',
      amount: absAmount,
      fromAccountId: outgoing.accountId || outgoing.fromAccountId || null,
      toAccountId: incoming.accountId || incoming.toAccountId || null,
      fromCompanyId: outgoing.companyId || outgoing.fromCompanyId || null,
      toCompanyId: incoming.companyId || incoming.toCompanyId || null,
      fromIndividualId: outgoing.individualId || outgoing.fromIndividualId || null,
      toIndividualId: incoming.individualId || incoming.toIndividualId || null,
      categoryId: incoming.categoryId || outgoing.categoryId || null,
      accountId: null,
      companyId: null,
      individualId: null,
      description: incoming.description || outgoing.description || 'Межкомпанийский перевод',
      transferGroupId: groupId,
      cellIndex: Math.min(
        Number.isFinite(Number(outgoing.cellIndex)) ? Number(outgoing.cellIndex) : 0,
        Number.isFinite(Number(incoming.cellIndex)) ? Number(incoming.cellIndex) : 0
      ),
      date: incoming.date || outgoing.date,
      dateKey: incoming.dateKey || outgoing.dateKey,
      dayOfYear: incoming.dayOfYear || outgoing.dayOfYear
    });
  });

  passthrough.sort((a, b) => {
    const dateA = new Date(a?.date || 0).getTime();
    const dateB = new Date(b?.date || 0).getTime();
    if (dateA !== dateB) return dateA - dateB;

    const cellA = Number.isFinite(Number(a?.cellIndex)) ? Number(a.cellIndex) : 0;
    const cellB = Number.isFinite(Number(b?.cellIndex)) ? Number(b.cellIndex) : 0;
    if (cellA !== cellB) return cellA - cellB;

    const createdA = new Date(a?.createdAt || 0).getTime();
    const createdB = new Date(b?.createdAt || 0).getTime();
    return createdA - createdB;
  });

  return passthrough;
}

function _buildSummary(operations) {
  const bucket = (kind) => operations.filter(op => op.kind === kind);
  const split = (rows) => ({
    all: rows,
    fact: rows.filter(r => r.isFact),
    forecast: rows.filter(r => !r.isFact),
  });
  const amount = (rows) => rows.reduce((s, r) => s + (Number(r.amount) || 0), 0);

  const income = split(bucket('income'));
  const expense = split(bucket('expense'));
  const transfer = split(bucket('transfer'));
  const withdrawal = split(transfer.all.filter(op => op.isPersonalTransferWithdrawal));

  return {
    total: operations.length,
    income: {
      count: income.all.length,
      total: amount(income.all),
      fact: { count: income.fact.length, total: amount(income.fact) },
      forecast: { count: income.forecast.length, total: amount(income.forecast) },
    },
    expense: {
      count: expense.all.length,
      total: amount(expense.all),
      fact: { count: expense.fact.length, total: amount(expense.fact) },
      forecast: { count: expense.forecast.length, total: amount(expense.forecast) },
    },
    transfer: {
      count: transfer.all.length,
      total: amount(transfer.all),
      fact: { count: transfer.fact.length, total: amount(transfer.fact) },
      forecast: { count: transfer.forecast.length, total: amount(transfer.forecast) },
      withdrawalOut: {
        count: withdrawal.all.length,
        total: amount(withdrawal.all),
        fact: { count: withdrawal.fact.length, total: amount(withdrawal.fact) },
        forecast: { count: withdrawal.forecast.length, total: amount(withdrawal.forecast) },
      }
    }
  };
}

function _buildCategorySummary(operations, categoriesCatalog = []) {
  const categoryMap = new Map();
  (Array.isArray(categoriesCatalog) ? categoriesCatalog : []).forEach((c) => {
    const id = c?.id || c?._id;
    if (!id) return;
    categoryMap.set(String(id), { name: c?.name || `Категория ${String(id).slice(-4)}`, type: c?.type || null });
  });

  const summaryMap = new Map();

  operations.forEach((op) => {
    if (op.kind !== 'income' && op.kind !== 'expense') return;
    if (!op.categoryId) return;

    const cid = String(op.categoryId);
    if (!summaryMap.has(cid)) {
      const meta = categoryMap.get(cid) || { name: op.categoryName || `Категория ${cid.slice(-4)}`, type: null };
      summaryMap.set(cid, {
        id: cid,
        name: meta.name,
        type: meta.type,
        incomeFact: 0,
        incomeForecast: 0,
        expenseFact: 0,
        expenseForecast: 0,
      });
    }

    const rec = summaryMap.get(cid);
    if (op.kind === 'income') {
      if (op.isFact) rec.incomeFact += op.amount || 0;
      else rec.incomeForecast += op.amount || 0;
    } else {
      if (op.isFact) rec.expenseFact += op.amount || 0;
      else rec.expenseForecast += op.amount || 0;
    }
  });

  const totalIncome = operations
    .filter(op => op.kind === 'income')
    .reduce((s, op) => s + (op.amount || 0), 0);
  const totalExpense = operations
    .filter(op => op.kind === 'expense')
    .reduce((s, op) => s + (op.amount || 0), 0);

  return Array.from(summaryMap.values())
    .map((row) => {
      const volume = row.incomeFact + row.incomeForecast + row.expenseFact + row.expenseForecast;
      const incomeShare = totalIncome ? (row.incomeFact + row.incomeForecast) / totalIncome : 0;
      const expenseShare = totalExpense ? (row.expenseFact + row.expenseForecast) / totalExpense : 0;
      return {
        ...row,
        volume,
        tags: [],
        incomeShare,
        expenseShare,
      };
    })
    .sort((a, b) => b.volume - a.volume);
}

function _normalizeOperation(op, todayStartTs, startTs, endTs) {
  if (!_shouldIncludeInOperationsEditor(op)) return null;

  const opDate = new Date(op?.date);
  if (Number.isNaN(opDate.getTime())) return null;
  const opTs = opDate.getTime();
  if (opTs < startTs || opTs > endTs) return null;

  const typeLabel = _normalizeTypeLabel(op);
  const statusLabel = _normalizeStatusLabel(op, todayStartTs);

  let kind = null;
  if (typeLabel === 'Доход') kind = 'income';
  else if (typeLabel === 'Расход') kind = 'expense';
  else if (typeLabel === 'Перевод' || typeLabel === 'Вывод средств') kind = 'transfer';
  else return null;

  const rawAmount = Number(op?.amount);
  const safeRawAmount = Number.isFinite(rawAmount) ? rawAmount : 0;
  const amount = Math.abs(safeRawAmount);

  const categoryId = _pickId(op?.categoryId);
  const projectId = _pickId(op?.projectId);
  const contractorId = _pickId(op?.contractorId);
  const counterpartyIndividualId = _pickId(op?.counterpartyIndividualId);

  return {
    _id: _pickId(op?._id || op?.id),
    date: _fmtDateDDMMYY(opDate),
    dateIso: opDate.toISOString().slice(0, 10),
    ts: opTs,
    type: kind,
    kind,
    isFact: statusLabel === 'Исполнено',
    amount,
    rawAmount: safeRawAmount,
    isTransfer: kind === 'transfer',
    isWithdrawal: !!(op?.isWithdrawal || typeLabel === 'Вывод средств'),
    isPersonalTransferWithdrawal: _isPersonalTransferWithdrawal(op),
    transferPurpose: op?.transferPurpose || null,
    transferReason: op?.transferReason || null,
    description: op?.description || null,
    accountId: _pickId(op?.accountId),
    accountName: _pickName(op?.accountId),
    fromAccountId: _pickId(op?.fromAccountId),
    toAccountId: _pickId(op?.toAccountId),
    fromAccountName: _pickName(op?.fromAccountId),
    toAccountName: _pickName(op?.toAccountId),
    projectId,
    projectName: _pickName(op?.projectId),
    contractorId,
    categoryId,
    categoryName: _pickName(op?.categoryId),
    companyId: _pickId(op?.companyId),
    fromCompanyId: _pickId(op?.fromCompanyId),
    toCompanyId: _pickId(op?.toCompanyId),
    companyName: _pickName(op?.companyId),
    fromCompanyName: _pickName(op?.fromCompanyId),
    toCompanyName: _pickName(op?.toCompanyId),
    individualId: _pickId(op?.individualId),
    counterpartyIndividualId,
    fromIndividualId: _pickId(op?.fromIndividualId),
    toIndividualId: _pickId(op?.toIndividualId),
    individualName: _pickName(op?.individualId),
    fromIndividualName: _pickName(op?.fromIndividualId),
    toIndividualName: _pickName(op?.toIndividualId),
    contractorName: _pickName(op?.contractorId) || _pickName(op?.counterpartyIndividualId) || null,
  };
}

module.exports = function createQuickJournalAdapter({ Event }) {
  async function buildFromJournal({ userId, periodFilter, asOf, categoriesCatalog = [] }) {
    const { start, end, nowRef } = _resolveRange(periodFilter, asOf);
    const startTs = start.getTime();
    const endTs = end.getTime();
    const today = _startOfDayLocal(nowRef);
    const todayStartTs = today.getTime();

    const events = await Event.find({ userId })
      .lean()
      .sort({ date: 1 })
      .populate('accountId companyId contractorId counterpartyIndividualId projectId categoryId individualId fromAccountId toAccountId fromCompanyId toCompanyId fromIndividualId toIndividualId');

    const merged = mergeLegacyInterCompanyTransfers(events);
    const operations = merged
      .map((op) => _normalizeOperation(op, todayStartTs, startTs, endTs))
      .filter(Boolean);

    return {
      operations,
      summary: _buildSummary(operations),
      categorySummary: _buildCategorySummary(operations, categoriesCatalog),
      meta: {
        periodStart: _fmtDateDDMMYY(start),
        periodEnd: _fmtDateDDMMYY(end),
      }
    };
  }

  return {
    buildFromJournal
  };
};
