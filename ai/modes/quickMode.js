// backend/ai/modes/quickMode.js
// Quick Mode: Fast deterministic responses for buttons and commands
// NO LLM - pure data formatting

/**
 * Handle quick button queries (accounts, companies, etc.)
 * @param {Object} params
 * @param {string} params.query - User query (lowercase)
 * @param {Object} params.dbData - Data packet from dataProvider
 * @param {Object} params.snapshot - Snapshot data from frontend
 * @param {Function} params.formatTenge - Currency formatter
 * @returns {string|null} Formatted response or null if not handled
 */
function handleQuickQuery({ query, dbData, snapshot, formatTenge }) {
    const qLower = String(query || '').toLowerCase().trim();

    console.log('[quickMode] query:', qLower);

    // =====================
    // ACCOUNTS QUERY
    // =====================
    if (/(сч[её]т|счета|касс|баланс)/i.test(qLower)) {
        console.log('[quickMode] Matched: ACCOUNTS');
        return handleAccountsQuery({ dbData, formatTenge });
    }

    // =====================
    // INCOME QUERY
    // =====================
    if (/(доход|поступлен|приход)/i.test(qLower) && !/(перевод|трансфер)/i.test(qLower)) {
        console.log('[quickMode] Matched: INCOME');
        return handleIncomeQuery({ dbData, formatTenge });
    }

    // =====================
    // EXPENSE QUERY
    // =====================
    if (/(расход|трат|затрат)/i.test(qLower) && !/(перевод|трансфер)/i.test(qLower)) {
        console.log('[quickMode] Matched: EXPENSE');
        return handleExpenseQuery({ dbData, formatTenge });
    }

    // =====================
    // TRANSFERS QUERY
    // =====================
    if (/(перевод|трансфер|transfer)/i.test(qLower)) {
        console.log('[quickMode] Matched: TRANSFERS');
        return handleTransfersQuery({ dbData, formatTenge });
    }

    // =====================
    // FINANCIAL ANALYSIS
    // =====================
    if (/(анализ|ситуац|картина|как дела|финанс|отч[её]т)/i.test(qLower) && !/(проект)/i.test(qLower)) {
        console.log('[quickMode] Matched: ANALYSIS');
        return handleAnalysisQuery({ dbData, formatTenge });
    }

    // =====================
    // COMPANIES QUERY
    // =====================
    if (/компани/i.test(qLower)) {
        console.log('[quickMode] Matched: COMPANIES');
        return handleCompaniesQuery({ dbData, formatTenge });
    }

    // =====================
    // PROJECTS QUERY
    // =====================
    if (/проект/i.test(qLower)) {
        console.log('[quickMode] Matched: PROJECTS');
        return handleProjectsQuery({ dbData, formatTenge, query: qLower });
    }

    // =====================
    // CATALOGS (contractors, individuals, categories)
    // =====================
    if (/контраг/i.test(qLower)) {
        console.log('[quickMode] Matched: CONTRACTORS');
        return handleContractorsQuery({ dbData });
    }

    if (/физ|фл\b/i.test(qLower)) {
        console.log('[quickMode] Matched: INDIVIDUALS');
        return handleIndividualsQuery({ dbData });
    }

    if (/категор/i.test(qLower)) {
        console.log('[quickMode] Matched: CATEGORIES');
        return handleCategoriesQuery({ dbData });
    }

    console.log('[quickMode] No match - returning null');
    // Not handled by quick mode
    return null;
}

// =====================
// ACCOUNTS
// =====================
function handleAccountsQuery({ dbData, formatTenge }) {
    const lines = [];
    const today = dbData.meta?.today || '?';
    lines.push(`Счета (на ${today})`);
    lines.push('');

    // Get accounts and filter
    const allAccounts = dbData.accounts || [];
    const open = allAccounts.filter(a => !a.isHidden && !a.isExcluded);
    const hidden = allAccounts.filter(a => a.isHidden || a.isExcluded);

    if (open.length) {
        lines.push('Открытые:');
        open.forEach(a => {
            lines.push(`${a.name}: ${formatTenge(a.currentBalance)}`);
        });
    } else {
        lines.push('Открытые: —');
    }

    lines.push('');

    if (hidden.length) {
        lines.push('Скрытые:');
        hidden.forEach(a => {
            lines.push(`${a.name}: ${formatTenge(a.currentBalance)}`);
        });
    } else {
        lines.push('Скрытые: —');
    }

    lines.push('');
    const totals = dbData.totals || {};
    lines.push(`Итого открытые: ${formatTenge(totals.open?.current || 0)}`);
    lines.push(`Итого скрытые: ${formatTenge(totals.hidden?.current || 0)}`);
    lines.push(`Итого все: ${formatTenge(totals.all?.current || 0)}`);

    return lines.join('\n');
}

// =====================
// INCOME
// =====================
function handleIncomeQuery({ dbData, formatTenge }) {
    const lines = [];
    const summary = dbData.operationsSummary || {};
    const inc = summary.income || {};
    const periodStart = dbData.meta?.periodStart || '?';
    const periodEnd = dbData.meta?.periodEnd || '?';

    lines.push(`Доходы (${periodStart} — ${periodEnd})`);
    lines.push('');

    if (inc.fact && inc.fact.total) {
        const count = inc.fact.count || 0;
        lines.push(`Факт: ${formatTenge(inc.fact.total)} (${count} операций)`);
    } else {
        lines.push('Факт: 0 ₸');
    }

    if (inc.forecast && inc.forecast.total) {
        const count = inc.forecast.count || 0;
        lines.push(`Прогноз: ${formatTenge(inc.forecast.total)} (${count} операций)`);
    } else {
        lines.push('Прогноз: 0 ₸');
    }

    return lines.join('\n');
}

// =====================
// EXPENSE
// =====================
function handleExpenseQuery({ dbData, formatTenge }) {
    const lines = [];
    const summary = dbData.operationsSummary || {};
    const exp = summary.expense || {};
    const periodStart = dbData.meta?.periodStart || '?';
    const periodEnd = dbData.meta?.periodEnd || '?';

    lines.push(`Расходы (${periodStart} — ${periodEnd})`);
    lines.push('');

    // Show fact expenses
    if (exp.fact && exp.fact.total) {
        const count = exp.fact.count || 0;
        lines.push(`Факт: ${formatTenge(Math.abs(exp.fact.total))} (${count} операций)`);

        // Show top categories
        const catSum = dbData.categorySummary || [];
        const topCats = catSum
            .filter(c => c.expense && c.expense.fact && c.expense.fact.total)
            .sort((a, b) => Math.abs(b.expense.fact.total) - Math.abs(a.expense.fact.total))
            .slice(0, 5);

        if (topCats.length) {
            lines.push('');
            lines.push('Топ категории:');
            topCats.forEach(c => {
                const amt = Math.abs(c.expense.fact.total);
                lines.push(`• ${c.name}: ${formatTenge(amt)}`);
            });
        }
    } else {
        lines.push('Факт: 0 ₸');
    }

    if (exp.forecast && exp.forecast.total) {
        const count = exp.forecast.count || 0;
        lines.push('');
        lines.push(`Прогноз: ${formatTenge(Math.abs(exp.forecast.total))} (${count} операций)`);
    }

    return lines.join('\n');
}

// =====================
// FINANCIAL ANALYSIS
// =====================
function handleAnalysisQuery({ dbData, formatTenge }) {
    const lines = [];
    const periodStart = dbData.meta?.periodStart || '?';
    const periodEnd = dbData.meta?.periodEnd || '?';
    const summary = dbData.operationsSummary || {};

    lines.push(`Анализ (${periodStart} — ${periodEnd})`);
    lines.push('==============');
    lines.push('');

    // Get accounts
    const allAccounts = dbData.accounts || [];
    const openAccountIds = new Set(
        allAccounts.filter(a => !a.isHidden && !a.isExcluded).map(a => String(a._id))
    );
    const hiddenAccountIds = new Set(
        allAccounts.filter(a => a.isHidden || a.isExcluded).map(a => String(a._id))
    );

    // Calculate for open accounts
    const openIncome = (dbData.operations || [])
        .filter(op => op.kind === 'income' && op.isFact && op.accountId && openAccountIds.has(String(op.accountId)))
        .reduce((sum, op) => sum + (op.amount || 0), 0);

    const openExpense = (dbData.operations || [])
        .filter(op => op.kind === 'expense' && op.isFact && op.accountId && openAccountIds.has(String(op.accountId)))
        .reduce((sum, op) => sum + Math.abs(op.rawAmount || op.amount || 0), 0);

    const openProfit = openIncome - openExpense;
    const openMargin = openIncome > 0 ? Math.round((openProfit / openIncome) * 100) : 0;

    // Calculate for hidden accounts
    const hiddenIncome = (dbData.operations || [])
        .filter(op => op.kind === 'income' && op.isFact && op.accountId && hiddenAccountIds.has(String(op.accountId)))
        .reduce((sum, op) => sum + (op.amount || 0), 0);

    const hiddenExpense = (dbData.operations || [])
        .filter(op => op.kind === 'expense' && op.isFact && op.accountId && hiddenAccountIds.has(String(op.accountId)))
        .reduce((sum, op) => sum + Math.abs(op.rawAmount || op.amount || 0), 0);

    const hiddenProfit = hiddenIncome - hiddenExpense;
    const hiddenMargin = hiddenIncome > 0 ? Math.round((hiddenProfit / hiddenIncome) * 100) : 0;

    // Totals
    const totalIncome = openIncome + hiddenIncome;
    const totalExpense = openExpense + hiddenExpense;
    const totalProfit = totalIncome - totalExpense;
    const totalMargin = totalIncome > 0 ? Math.round((totalProfit / totalIncome) * 100) : 0;

    // Build response
    lines.push('Открытые счета:');
    lines.push(`Доходы = ${formatTenge(openIncome)}`);
    lines.push(`Расходы = ${formatTenge(openExpense)}`);
    lines.push(`Прибыль = ${formatTenge(openProfit)}`);
    lines.push(`Маржа = ${openMargin}%`);
    lines.push('');
    lines.push('==============');
    lines.push('');
    lines.push('Скрытые счета:');
    lines.push(`Доходы = ${formatTenge(hiddenIncome)}`);
    lines.push(`Расходы = ${formatTenge(hiddenExpense)}`);
    lines.push(`Прибыль = ${formatTenge(hiddenProfit)}`);
    lines.push(`Маржа = ${hiddenMargin}%`);
    lines.push('');
    lines.push('==============');
    lines.push('');
    lines.push('Суммарно:');
    lines.push(`Доходы = ${formatTenge(totalIncome)}`);
    lines.push(`Расходы = ${formatTenge(totalExpense)}`);
    lines.push(`Прибыль = ${formatTenge(totalProfit)}`);
    lines.push(`Маржа = ${totalMargin}%`);

    return lines.join('\n');
}

// =====================
// TRANSFERS
// =====================
function handleTransfersQuery({ dbData, formatTenge }) {
    const lines = [];
    const transfers = (dbData.operations || []).filter(op => op.kind === 'transfer');

    lines.push('Переводы:');
    lines.push('');

    if (!transfers.length) {
        lines.push('Переводы не найдены');
        return lines.join('\n');
    }

    // Prioritize fact, then forecast
    const fact = transfers.filter(t => t.isFact);
    const forecast = transfers.filter(t => !t.isFact);

    if (fact.length) {
        lines.push('Факт:');
        fact.slice(0, 10).forEach(t => {
            const from = t.fromAccountName || '?';
            const to = t.toAccountName || '?';
            const amt = Math.abs(t.amount || 0);
            lines.push(`• ${formatTenge(amt)}: ${from} → ${to}`);
        });
    }

    if (forecast.length) {
        lines.push('');
        lines.push('Прогноз:');
        forecast.slice(0, 10).forEach(t => {
            const from = t.fromAccountName || '?';
            const to = t.toAccountName || '?';
            const amt = Math.abs(t.amount || 0);
            lines.push(`• ${formatTenge(amt)}: ${from} → ${to}`);
        });
    }

    return lines.join('\n');
}

// =====================
// COMPANIES
// =====================
function handleCompaniesQuery({ dbData, formatTenge }) {
    const lines = [];
    const accounts = dbData.accounts || [];
    const companies = dbData.catalogs?.companies || [];

    lines.push('Компании:');
    lines.push('');

    if (!companies.length) {
        lines.push('Компании не найдены');
        return lines.join('\n');
    }

    const companyBalances = new Map();
    accounts.forEach(acc => {
        if (!acc.companyId) return;
        const cid = String(acc.companyId);
        const bal = acc.currentBalance || 0;
        companyBalances.set(cid, (companyBalances.get(cid) || 0) + bal);
    });

    companies.forEach(c => {
        const cid = String(c.id || c._id);
        const bal = companyBalances.get(cid) || 0;
        lines.push(`${c.name}: ${formatTenge(bal)}`);
    });

    return lines.join('\n');
}

// =====================
// PROJECTS
// =====================
function handleProjectsQuery({ dbData, formatTenge, query }) {
    const projects = dbData.catalogs?.projects || [];

    // Check if specific project name in query
    const specificProject = projects.find(p =>
        query.includes((p.name || '').toLowerCase())
    );

    if (specificProject) {
        return buildProjectReport(specificProject, dbData, formatTenge);
    }

    return buildAllProjectsReport(projects, dbData, formatTenge);
}

function buildProjectReport(project, dbData, formatTenge) {
    const ops = (dbData.operations || []).filter(op => String(op.projectId || '') === String(project.id || project._id));
    const periodStart = dbData.meta?.periodStart || '?';
    const periodEnd = dbData.meta?.periodEnd || '?';

    let factIncome = 0, factExpense = 0, forecastIncome = 0, forecastExpense = 0;
    let factCount = 0, forecastCount = 0;

    ops.forEach(op => {
        if (op.kind === 'income') {
            if (op.isFact) { factIncome += op.amount || 0; factCount++; }
            else { forecastIncome += op.amount || 0; forecastCount++; }
        } else if (op.kind === 'expense') {
            if (op.isFact) { factExpense += op.amount || 0; factCount++; }
            else { forecastExpense += op.amount || 0; forecastCount++; }
        }
    });

    const factNet = factIncome - factExpense;
    const forecastNet = forecastIncome - forecastExpense;

    const lines = [];
    lines.push(`Проект: ${project.name}`);
    lines.push(`Период: ${periodStart} — ${periodEnd}`);
    lines.push('');
    lines.push(`Прибыль (факт): ${formatTenge(factNet)} (${factCount} операций)`);
    lines.push(`Прибыль (прогноз): ${formatTenge(forecastNet)} (${forecastCount} операций)`);

    if (!ops.length) {
        lines.push('Операции по проекту в выбранном периоде не найдены.');
    }

    return lines.join('\n');
}

function buildAllProjectsReport(projects, dbData, formatTenge) {
    const ops = dbData.operations || [];
    const projectStats = new Map();

    ops.forEach(op => {
        const pid = String(op.projectId || '');
        if (!pid) return;

        if (!projectStats.has(pid)) {
            const proj = projects.find(p => String(p.id || p._id) === pid);
            projectStats.set(pid, {
                name: proj?.name || `Проект ${pid.slice(-4)}`,
                factIncome: 0,
                factExpense: 0,
                forecastIncome: 0,
                forecastExpense: 0
            });
        }

        const stat = projectStats.get(pid);
        if (op.kind === 'income') {
            if (op.isFact) stat.factIncome += op.amount || 0;
            else stat.forecastIncome += op.amount || 0;
        } else if (op.kind === 'expense') {
            if (op.isFact) stat.factExpense += op.amount || 0;
            else stat.forecastExpense += op.amount || 0;
        }
    });

    const lines = [];
    lines.push('Проекты:');
    lines.push('');

    if (!projectStats.size) {
        lines.push('Операции по проектам не найдены');
        return lines.join('\n');
    }

    const sorted = Array.from(projectStats.values())
        .sort((a, b) => {
            const aNet = (a.factIncome - a.factExpense + a.forecastIncome - a.forecastExpense);
            const bNet = (b.factIncome - b.factExpense + b.forecastIncome - b.forecastExpense);
            return bNet - aNet;
        });

    sorted.forEach(s => {
        const factNet = s.factIncome - s.factExpense;
        const forecastNet = s.forecastIncome - s.forecastExpense;
        lines.push(`${s.name}: факт ${formatTenge(factNet)}, прогноз ${formatTenge(forecastNet)}`);
    });

    return lines.join('\n');
}

// =====================
// CATALOGS
// =====================
function handleContractorsQuery({ dbData }) {
    const contractors = dbData.catalogs?.contractors || [];
    const lines = ['Контрагенты:', ''];

    if (!contractors.length) {
        lines.push('Контрагенты не найдены');
    } else {
        contractors.forEach(c => lines.push(`• ${c.name}`));
    }

    return lines.join('\n');
}

function handleIndividualsQuery({ dbData }) {
    const individuals = dbData.catalogs?.individuals || [];
    const lines = ['Физлица:', ''];

    if (!individuals.length) {
        lines.push('Физлица не найдены');
    } else {
        individuals.forEach(i => lines.push(`• ${i.name}`));
    }

    return lines.join('\n');
}

function handleCategoriesQuery({ dbData }) {
    const categories = dbData.catalogs?.categories || [];
    const lines = ['Категории:', ''];

    if (!categories.length) {
        lines.push('Категории не найдены');
    } else {
        categories.forEach(c => lines.push(`• ${c.name}`));
    }

    return lines.join('\n');
}

module.exports = {
    handleQuickQuery
};
