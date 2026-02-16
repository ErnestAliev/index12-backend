// backend/ai/utils/financialCalculator.js
// Deterministic financial calculations from tableContext.rows
// NO LLM - pure code-based calculations

/**
 * Normalize amount sign based on operation type
 * @param {string} type - Operation type: "Доход", "Расход", "Перевод"
 * @param {number} amount - Raw amount from row
 * @returns {number} Normalized amount with correct sign
 */
function normalizeAmount(type, amount) {
    const abs = Math.abs(Number(amount) || 0);
    const typeNorm = String(type || '').toLowerCase().trim();

    if (typeNorm === 'доход' || typeNorm === 'income') {
        return abs; // Income: always positive
    }
    if (typeNorm === 'расход' || typeNorm === 'expense') {
        return -abs; // Expense: always negative
    }
    // Transfer: keep as-is (handled separately)
    return abs;
}

/**
 * Normalize status
 * @param {string} statusCode - Status code: "fact", "plan"
 * @param {string} statusLabel - Status label: "Исполнено", "План"
 * @returns {string} "fact" or "plan"
 */
function normalizeStatus(statusCode, statusLabel) {
    const sc = String(statusCode || '').trim().toLowerCase();
    if (sc === 'plan') return 'plan';
    if (sc === 'fact') return 'fact';
    const s = String(statusLabel || '').toLowerCase();
    if (s.includes('план')) return 'plan';
    return 'fact'; // Default to fact
}

/**
 * Parse row timestamp
 * @param {Object} row - Table row
 * @returns {number} Timestamp in milliseconds
 */
function parseRowTimestamp(row) {
    const rawDate = row?.date ? new Date(row.date) : null;
    if (rawDate && !Number.isNaN(rawDate.getTime())) {
        return rawDate.getTime();
    }

    // Try parsing from dateLabel (DD.MM.YYYY)
    const label = String(row?.dateLabel || '').trim();
    const m = label.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
    if (m) {
        const dd = Number(m[1]);
        const mm = Number(m[2]) - 1;
        const yyyy = Number(m[3]);
        const d = new Date(yyyy, mm, dd);
        if (!Number.isNaN(d.getTime())) {
            return d.getTime();
        }
    }

    return NaN;
}

/**
 * Filter rows by period
 * @param {Array} rows - All rows
 * @param {Object} period - {startTs, endTs}
 * @returns {Array} Filtered rows
 */
function filterRowsByPeriod(rows, period) {
    if (!period || !Number.isFinite(period.startTs) || !Number.isFinite(period.endTs)) {
        return rows;
    }

    return rows.filter((row) => {
        const ts = parseRowTimestamp(row);
        if (!Number.isFinite(ts)) return false;
        return ts >= period.startTs && ts <= period.endTs;
    });
}

/**
 * Calculate aggregates from rows
 * @param {Array} rows - Filtered rows
 * @returns {Object} Computed metrics
 */
function calculateAggregates(rows) {
    const result = {
        fact: {
            income: 0,
            expense: 0,
            net: 0,
            transfer: 0,
            count: 0
        },
        plan: {
            income: 0,
            expense: 0,
            net: 0,
            transfer: 0,
            count: 0
        },
        total: {
            income: 0,
            expense: 0,
            net: 0,
            transfer: 0,
            count: 0
        },
        byCategory: {},
        byProject: {},
        evidenceRows: []
    };

    rows.forEach((row) => {
        const type = String(row?.type || '').trim();
        const typeNorm = type.toLowerCase();
        const status = normalizeStatus(row?.statusCode, row?.status);
        const amount = normalizeAmount(type, row?.amount);
        const amountAbs = Math.abs(amount);

        // Determine operation kind
        let kind = null;
        if (typeNorm === 'доход' || typeNorm === 'income') kind = 'income';
        else if (typeNorm === 'расход' || typeNorm === 'expense') kind = 'expense';
        else if (typeNorm === 'перевод' || typeNorm === 'transfer') kind = 'transfer';

        if (!kind) return; // Skip unknown types

        // Update main aggregates
        const bucket = status === 'plan' ? result.plan : result.fact;
        bucket.count++;
        result.total.count++;

        if (kind === 'income') {
            bucket.income += amountAbs;
            result.total.income += amountAbs;
        } else if (kind === 'expense') {
            bucket.expense += amountAbs;
            result.total.expense += amountAbs;
        } else if (kind === 'transfer') {
            bucket.transfer += amountAbs;
            result.total.transfer += amountAbs;
        }

        // Category aggregation
        const category = String(row?.category || 'Без категории').trim();
        if (!result.byCategory[category]) {
            result.byCategory[category] = {
                name: category,
                fact: { income: 0, expense: 0, net: 0, transfer: 0, count: 0 },
                plan: { income: 0, expense: 0, net: 0, transfer: 0, count: 0 },
                total: { income: 0, expense: 0, net: 0, transfer: 0, count: 0 }
            };
        }
        const catBucket = status === 'plan' ? result.byCategory[category].plan : result.byCategory[category].fact;
        catBucket.count++;
        result.byCategory[category].total.count++;

        if (kind === 'income') {
            catBucket.income += amountAbs;
            result.byCategory[category].total.income += amountAbs;
        } else if (kind === 'expense') {
            catBucket.expense += amountAbs;
            result.byCategory[category].total.expense += amountAbs;
        } else if (kind === 'transfer') {
            catBucket.transfer += amountAbs;
            result.byCategory[category].total.transfer += amountAbs;
        }

        // Project aggregation
        const project = String(row?.project || 'Без проекта').trim();
        if (!result.byProject[project]) {
            result.byProject[project] = {
                name: project,
                fact: { income: 0, expense: 0, net: 0, transfer: 0, count: 0 },
                plan: { income: 0, expense: 0, net: 0, transfer: 0, count: 0 },
                total: { income: 0, expense: 0, net: 0, transfer: 0, count: 0 }
            };
        }
        const projBucket = status === 'plan' ? result.byProject[project].plan : result.byProject[project].fact;
        projBucket.count++;
        result.byProject[project].total.count++;

        if (kind === 'income') {
            projBucket.income += amountAbs;
            result.byProject[project].total.income += amountAbs;
        } else if (kind === 'expense') {
            projBucket.expense += amountAbs;
            result.byProject[project].total.expense += amountAbs;
        } else if (kind === 'transfer') {
            projBucket.transfer += amountAbs;
            result.byProject[project].total.transfer += amountAbs;
        }

        // Store row as evidence (limit to top 100)
        if (result.evidenceRows.length < 100) {
            result.evidenceRows.push({
                date: row?.dateLabel || row?.date || '?',
                type,
                status: row?.status || row?.statusCode || '?',
                amount: row?.amount,
                category,
                project,
                description: row?.description || row?.comment || ''
            });
        }
    });

    // Calculate net values
    result.fact.net = result.fact.income - result.fact.expense;
    result.plan.net = result.plan.income - result.plan.expense;
    result.total.net = result.total.income - result.total.expense;

    Object.values(result.byCategory).forEach((cat) => {
        cat.fact.net = cat.fact.income - cat.fact.expense;
        cat.plan.net = cat.plan.income - cat.plan.expense;
        cat.total.net = cat.total.income - cat.total.expense;
    });

    Object.values(result.byProject).forEach((proj) => {
        proj.fact.net = proj.fact.income - proj.fact.expense;
        proj.plan.net = proj.plan.income - proj.plan.expense;
        proj.total.net = proj.total.income - proj.total.expense;
    });

    return result;
}

/**
 * Get top movers by metric
 * @param {Object} metrics - Computed metrics
 * @param {string} groupBy - "category" or "project"
 * @param {string} metric - "income", "expense", "net", "transfer"
 * @param {string} status - "fact", "plan", "total"
 * @param {number} limit - Max items to return
 * @returns {Array} Top movers sorted by absolute value
 */
function getTopMovers(metrics, groupBy = 'category', metric = 'net', status = 'total', limit = 10) {
    const source = groupBy === 'project' ? metrics.byProject : metrics.byCategory;

    return Object.values(source)
        .map((item) => ({
            name: item.name,
            value: item[status][metric] || 0
        }))
        .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
        .slice(0, limit);
}

/**
 * Main entry point: compute all metrics from tableContext.rows
 * @param {Object} params
 * @param {Array} params.rows - tableContext.rows
 * @param {Object} params.periodFilter - Period filter from frontend
 * @param {string} params.asOf - AsOf date
 * @returns {Object} Complete metrics
 */
function computeMetrics({ rows, periodFilter, asOf }) {
    // Resolve period range
    const period = resolvePeriod(periodFilter, asOf);

    // Filter rows by period
    const filteredRows = filterRowsByPeriod(rows, period);

    // Calculate aggregates
    const metrics = calculateAggregates(filteredRows);

    return {
        period,
        metrics,
        rowCounts: {
            input: rows.length,
            afterPeriodFilter: filteredRows.length
        }
    };
}

/**
 * Resolve period from filter
 * @param {Object} periodFilter
 * @param {string} asOf
 * @returns {Object} {startTs, endTs, startLabel, endLabel}
 */
function resolvePeriod(periodFilter, asOf) {
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

    const formatDDMMYY = (date) => {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '?';
        const dd = String(date.getDate()).padStart(2, '0');
        const mm = String(date.getMonth() + 1).padStart(2, '0');
        const yy = String(date.getFullYear()).slice(-2);
        return `${dd}.${mm}.${yy}`;
    };

    return {
        startTs: start.getTime(),
        endTs: end.getTime(),
        startLabel: formatDDMMYY(start),
        endLabel: formatDDMMYY(end)
    };
}

module.exports = {
    computeMetrics,
    getTopMovers,
    normalizeAmount,
    normalizeStatus,
    parseRowTimestamp,
    filterRowsByPeriod,
    calculateAggregates,
    resolvePeriod
};
