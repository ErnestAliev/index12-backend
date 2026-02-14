// ai/contextPacketBuilder.js
// Builds deterministic context packet payload + source hash

const crypto = require('crypto');

function derivePeriodKey(dateInput, timezone = 'Asia/Almaty') {
    const d = new Date(dateInput);
    if (Number.isNaN(d.getTime())) return null;
    const dtf = new Intl.DateTimeFormat('en-CA', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit'
    });
    const parts = dtf.formatToParts(d);
    const year = parts.find(p => p.type === 'year')?.value;
    const month = parts.find(p => p.type === 'month')?.value;
    if (!year || !month) return null;
    return `${year}-${month}`;
}

function _stableClone(value) {
    if (Array.isArray(value)) return value.map(_stableClone);
    if (value && typeof value === 'object' && !(value instanceof Date)) {
        const out = {};
        Object.keys(value).sort().forEach((k) => {
            out[k] = _stableClone(value[k]);
        });
        return out;
    }
    if (value instanceof Date) return value.toISOString();
    return value;
}

function _hashPayload(payload) {
    const normalized = JSON.stringify(_stableClone(payload));
    return crypto.createHash('sha256').update(normalized).digest('hex');
}

function _toFiniteNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
}

function _extractLiquiditySignals(dbData) {
    const timelineRaw = Array.isArray(dbData?.meta?.timeline) ? dbData.meta.timeline : [];
    if (!timelineRaw.length) {
        return {
            available: false,
            reason: 'timeline_absent'
        };
    }

    const periodStartDate = dbData?.meta?.periodStart ? new Date(dbData.meta.periodStart) : null;
    const periodEndDate = dbData?.meta?.periodEnd ? new Date(dbData.meta.periodEnd) : null;
    const hasPeriodStart = !!(periodStartDate && Number.isFinite(periodStartDate.getTime()));
    const hasPeriodEnd = !!(periodEndDate && Number.isFinite(periodEndDate.getTime()));
    const periodStartTs = hasPeriodStart ? periodStartDate.getTime() : null;
    const periodEndTs = hasPeriodEnd ? periodEndDate.getTime() : null;

    const timeline = timelineRaw
        .map((row) => {
            const date = row?.date ? new Date(row.date) : null;
            const ts = (date && Number.isFinite(date.getTime())) ? date.getTime() : null;
            return {
                date: row?.date || null,
                ts,
                income: _toFiniteNumber(row?.income),
                expense: Math.abs(_toFiniteNumber(row?.expense)),
                offsetExpense: Math.abs(_toFiniteNumber(row?.offsetExpense)),
                withdrawal: Math.abs(_toFiniteNumber(row?.withdrawal)),
                closingBalance: _toFiniteNumber(row?.closingBalance)
            };
        })
        .filter((row) => {
            if (!Number.isFinite(row.ts)) return false;
            if (Number.isFinite(periodStartTs) && row.ts < periodStartTs) return false;
            if (Number.isFinite(periodEndTs) && row.ts > periodEndTs) return false;
            return true;
        })
        .filter((row) => Number.isFinite(row.ts))
        .sort((a, b) => a.ts - b.ts);

    if (!timeline.length) {
        return {
            available: false,
            reason: 'timeline_invalid'
        };
    }

    let minDay = timeline[0];
    let maxExpenseDay = timeline[0];
    for (const day of timeline) {
        if (day.closingBalance < minDay.closingBalance) minDay = day;
        if (day.expense > maxExpenseDay.expense) maxExpenseDay = day;
    }

    const maxDailyExpense = _toFiniteNumber(maxExpenseDay.expense);
    const lowCashThreshold = Math.max(200_000, Math.round(maxDailyExpense * 0.25));
    const lowCashDays = timeline
        .filter((day) => day.closingBalance <= lowCashThreshold)
        .slice(0, 12)
        .map((day) => ({
            date: day.date,
            closingBalance: day.closingBalance,
            income: day.income,
            expense: day.expense
        }));

    const firstNegativeDay = timeline.find((day) => day.closingBalance < 0) || null;
    const firstLowCashDay = lowCashDays.length ? lowCashDays[0] : null;
    const endDay = timeline[timeline.length - 1];
    const startDay = timeline[0];

    return {
        available: true,
        period: {
            startDate: startDay?.date || null,
            endDate: endDay?.date || null
        },
        minClosingBalance: {
            date: minDay?.date || null,
            amount: _toFiniteNumber(minDay?.closingBalance)
        },
        endClosingBalance: _toFiniteNumber(endDay?.closingBalance),
        firstNegativeDay: firstNegativeDay
            ? { date: firstNegativeDay.date, amount: firstNegativeDay.closingBalance }
            : null,
        lowCashThreshold,
        firstLowCashDay,
        lowCashDaysCount: lowCashDays.length,
        lowCashDays,
        maxDailyExpense: {
            date: maxExpenseDay?.date || null,
            amount: _toFiniteNumber(maxExpenseDay?.expense)
        }
    };
}

function buildContextPacketPayload({
    dbData,
    promptText = '',
    templateVersion = 'deep-v1',
    dictionaryVersion = 'dict-v1'
}) {
    const normalized = {
        accounts: dbData?.accounts || [],
        events: dbData?.operations || [],
        categories: dbData?.catalogs?.categories || [],
        projects: dbData?.catalogs?.projects || [],
        companies: dbData?.catalogs?.companies || [],
        contractors: dbData?.catalogs?.contractors || [],
        individuals: dbData?.catalogs?.individuals || []
    };

    const derived = {
        meta: dbData?.meta || {},
        totals: dbData?.totals || {},
        accountsData: dbData?.accountsData || {},
        operationsSummary: dbData?.operationsSummary || {},
        categorySummary: dbData?.categorySummary || [],
        tagSummary: dbData?.tagSummary || [],
        liquiditySignals: _extractLiquiditySignals(dbData)
    };

    const dictionary = {
        entities: {
            account: 'Счет движения денег',
            event: 'Операция (income/expense/transfer, факт/прогноз, дата, сумма)',
            category: 'Категория операции',
            project: 'Проект/филиал',
            owner: 'Владелец счета (юрлицо/физлицо)'
        },
        rules: [
            'Внутренний transfer между своими счетами не считать прибылью',
            'Если запрошены только открытые счета — скрытые исключать',
            'При конфликте источников явно показывать расхождение'
        ]
    };

    const dataQuality = dbData?.dataQualityReport || {};
    const prompt = {
        templateVersion,
        dictionaryVersion,
        text: String(promptText || '')
    };

    const hashSource = {
        prompt,
        normalized,
        derived,
        dataQuality
    };
    const sourceHash = _hashPayload(hashSource);

    return {
        prompt,
        dictionary,
        normalized,
        derived,
        dataQuality,
        stats: {
            operationsCount: Array.isArray(normalized.events) ? normalized.events.length : 0,
            accountsCount: Array.isArray(normalized.accounts) ? normalized.accounts.length : 0,
            sourceHash
        }
    };
}

module.exports = {
    derivePeriodKey,
    buildContextPacketPayload
};
