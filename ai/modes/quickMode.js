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
function handleQuickQuery({ action, query, dbData, snapshot, formatTenge }) {
    // NEW: If explicit action provided (from quick button), route directly
    if (action) {
        console.log('[quickMode] Action-based routing:', action);

        switch (action) {
            case 'analysis':
                return handleAnalysisQuery({ dbData });

            case 'forecast':
                return handleForecastQuery({ dbData });

            case 'accounts':
                return handleAccountsQuery({ dbData, formatTenge });

            case 'income':
                return handleIncomeQuery({ dbData, formatTenge });

            case 'expense':
                return handleExpenseQuery({ dbData, formatTenge });

            case 'transfers':
                return handleTransfersQuery({ dbData, formatTenge, withdrawalsOnly: false });

            case 'withdrawals':
                return handleTransfersQuery({ dbData, formatTenge, withdrawalsOnly: true });

            case 'companies':
                return handleCompaniesQuery({ dbData, formatTenge });

            case 'projects':
                return handleProjectsQuery({ dbData, formatTenge, query: '' });

            case 'contractors':
                return handleContractorsQuery({ dbData });

            case 'individuals':
                return handleIndividualsQuery({ dbData });

            case 'categories':
                return handleCategoriesQuery({ dbData });

            default:
                console.log('[quickMode] Unknown action:', action);
                return null;
        }
    }

    // FALLBACK: Legacy text-based routing for backwards compatibility
    // (Used when users type queries in chat mode or old API calls)
    const qLower = String(query || '').toLowerCase().trim();
    console.log('[quickMode] Text-based routing (fallback):', qLower);

    const asksTransfers = /(перевод|трансфер|transfer)/i.test(qLower);
    const asksWithdrawals = /(вывод\s+средств|снят[иея]|личн(ая|ый|ую)\s+(карт|счет))/i.test(qLower);
    const asksDeterministicAnalysis = /^(анализ|сделай\s+анализ|анализ\s+состояния)$/i.test(qLower);
    const asksDeterministicForecast = /^(прогноз|сделай\s+прогноз|прогноз\s+на\s+конец\s+месяца)$/i.test(qLower)
        || /(до\s+конца\s+месяца|конец\s+месяца)/i.test(qLower);

    // =====================
    // DETERMINISTIC ANALYSIS / FORECAST
    // =====================
    if (asksDeterministicForecast) {
        console.log('[quickMode] Matched: FORECAST');
        return handleForecastQuery({ dbData });
    }
    if (asksDeterministicAnalysis) {
        console.log('[quickMode] Matched: ANALYSIS');
        return handleAnalysisQuery({ dbData });
    }

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
    if (/(доход|поступлен|приход)/i.test(qLower) && !asksTransfers && !asksWithdrawals) {
        console.log('[quickMode] Matched: INCOME');
        return handleIncomeQuery({ dbData, formatTenge });
    }

    // =====================
    // EXPENSE QUERY
    // =====================
    if (/(расход|трат|затрат)/i.test(qLower) && !asksTransfers && !asksWithdrawals) {
        console.log('[quickMode] Matched: EXPENSE');
        return handleExpenseQuery({ dbData, formatTenge });
    }

    // =====================
    // TRANSFERS QUERY
    // =====================
    if (asksTransfers || asksWithdrawals) {
        console.log('[quickMode] Matched: TRANSFERS');
        return handleTransfersQuery({
            dbData,
            formatTenge,
            withdrawalsOnly: asksWithdrawals && !asksTransfers
        });
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
    lines.push('==============');
    lines.push('');

    // Show fact income grouped by category
    if (inc.fact && inc.fact.total) {
        const count = inc.fact.count || 0;

        // Get all categories with income
        const catSum = dbData.categorySummary || [];
        const categories = catSum
            .filter(c => c.incomeFact && c.incomeFact !== 0)
            .sort((a, b) => Math.abs(b.incomeFact) - Math.abs(a.incomeFact));

        if (categories.length) {
            categories.forEach(c => {
                const amt = Math.abs(c.incomeFact);
                lines.push(`${c.name}: ${formatTenge(amt)}`);
            });
            lines.push('');
            lines.push('==============');
            lines.push('');
            lines.push(`Итого: ${formatTenge(inc.fact.total)} (${count} операций)`);
        } else {
            lines.push(`Итого: ${formatTenge(inc.fact.total)} (${count} операций)`);
            lines.push('');
            lines.push('Категории не указаны');
        }
    } else {
        lines.push('Доходы не найдены');
    }

    if (inc.forecast && inc.forecast.total) {
        const count = inc.forecast.count || 0;
        lines.push('');
        lines.push('==============');
        lines.push('');
        lines.push(`Прогноз доходов: ${formatTenge(inc.forecast.total)} (${count} операций)`);
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
    lines.push('==============');
    lines.push('');

    // Show fact expenses grouped by category
    if (exp.fact && exp.fact.total) {
        const count = exp.fact.count || 0;

        // Get all categories with expenses
        const catSum = dbData.categorySummary || [];
        const categories = catSum
            .filter(c => c.expenseFact && c.expenseFact !== 0)
            .sort((a, b) => Math.abs(b.expenseFact) - Math.abs(a.expenseFact));

        if (categories.length) {
            categories.forEach(c => {
                const amt = Math.abs(c.expenseFact);
                lines.push(`${c.name}: ${formatTenge(amt)}`);
            });
            lines.push('');
            lines.push('==============');
            lines.push('');
            lines.push(`Итого: ${formatTenge(Math.abs(exp.fact.total))} (${count} операций)`);
        } else {
            lines.push(`Итого: ${formatTenge(Math.abs(exp.fact.total))} (${count} операций)`);
            lines.push('');
            lines.push('Категории не указаны');
        }
    } else {
        lines.push('Расходы не найдены');
    }

    if (exp.forecast && exp.forecast.total) {
        const count = exp.forecast.count || 0;
        lines.push('');
        lines.push('==============');
        lines.push('');
        lines.push(`Прогноз расходов: ${formatTenge(Math.abs(exp.forecast.total))} (${count} операций)`);
    }

    return lines.join('\n');
}

// =====================
// FINANCIAL ANALYSIS
// =====================
function _normalizeToken(value) {
    return String(value || '')
        .toLowerCase()
        .replace(/ё/g, 'е')
        .replace(/[^a-zа-я0-9]+/gi, '');
}

function _fmtMoneyPlain(value, { signed = false } = {}) {
    const n = Number(value || 0);
    const abs = Math.round(Math.abs(n));
    const base = new Intl.NumberFormat('ru-RU').format(abs).replace(/\u00A0/g, ' ');
    if (!signed) return base;
    return n < 0 ? `-${base}` : base;
}

function _fmtMoneyTenge(value, { signed = false } = {}) {
    return `${_fmtMoneyPlain(value, { signed })} ₸`;
}

// Smart context formatting: use "млн" for millions to improve readability
function _fmtMoneyContext(value) {
    const n = Number(value || 0);
    const abs = Math.abs(n);

    if (abs >= 1000000) {
        // Display as millions with 1 decimal place
        const millions = abs / 1000000;
        const formatted = millions.toFixed(1).replace('.', ',');
        return `${formatted} млн`;
    }

    // Below 1M, use regular formatting without ₸ symbol
    return _fmtMoneyPlain(abs);
}


function _collectUtilitiesFact(dbData) {
    let income = 0;
    let expense = 0;
    (dbData.operations || []).forEach((op) => {
        if (!op?.isFact) return;
        if (op?.kind !== 'income' && op?.kind !== 'expense') return;
        const token = _normalizeToken(op?.categoryName || '');
        if (!token.includes('коммун')) return;
        if (op.kind === 'income') income += Number(op.amount || 0);
        if (op.kind === 'expense') expense += Number(op.amount || 0);
    });
    return { income, expense };
}

function _collectPlannedTaxes(dbData) {
    let total = 0;
    (dbData.operations || []).forEach((op) => {
        if (op?.isFact) return;
        if (op?.kind !== 'expense') return;
        const token = _normalizeToken(op?.categoryName || '');
        if (!token.includes('налог')) return;
        total += Number(op.amount || 0);
    });
    return total;
}

function _getTopForecastIncomeCategory(dbData) {
    const rows = Array.isArray(dbData.categorySummary) ? dbData.categorySummary : [];
    const top = rows
        .filter((row) => Number(row?.incomeForecast || 0) > 0)
        .sort((a, b) => Number(b.incomeForecast || 0) - Number(a.incomeForecast || 0))[0];
    return top?.name ? String(top.name) : '';
}

function handleAnalysisQuery({ dbData }) {
    const lines = [];
    const asOfLabel = dbData.meta?.today || '?';
    const summary = dbData.operationsSummary || {};

    // Fact data
    const factIncome = Number(summary?.income?.fact?.total || 0);
    const factExpense = Number(summary?.expense?.fact?.total || 0);
    const factProfit = factIncome - factExpense;

    // Plan data (remaining forecast)
    const plannedIncome = Number(summary?.income?.forecast?.total || 0);
    const plannedExpense = Number(summary?.expense?.forecast?.total || 0);
    const plannedGap = plannedExpense - plannedIncome;

    // Totals (projected full period: fact + plan)
    const totalIncome = factIncome + plannedIncome;
    const totalExpense = factExpense + plannedExpense;
    const totalProfit = totalIncome - totalExpense;

    // Balances
    const openBalance = Number(dbData?.totals?.open?.current || 0);
    const hiddenBalance = Number(dbData?.totals?.hidden?.current || 0);
    const totalBalance = Number(dbData?.totals?.all?.current || (openBalance + hiddenBalance));

    // Percentages
    const margin = totalIncome > 0 ? Math.round((totalProfit / totalIncome) * 100) : 0;
    const expenseRatio = totalIncome > 0 ? Math.round((totalExpense / totalIncome) * 100) : 0;

    // Greeting
    lines.push('Привет.');
    lines.push('');

    // 1. P&L Section with logical chains
    lines.push('1. Финансовый результат (P&L)');

    if (totalIncome > 0) {
        const incomeChain = [];
        incomeChain.push(`Факт доходов (${_fmtMoneyPlain(factIncome)} ₸)`);
        if (plannedIncome > 0) {
            incomeChain.push(`План остатка (${_fmtMoneyPlain(plannedIncome)} ₸)`);
            incomeChain.push(`Прогноз выручки ${_fmtMoneyPlain(totalIncome)} ₸`);
        }

        const expenseChain = [];
        if (factExpense > 0 && plannedExpense > 0) {
            expenseChain.push(`Расходы (Факт ${_fmtMoneyContext(factExpense)} + План ${_fmtMoneyContext(plannedExpense)}) составляют ${expenseRatio}% от выручки`);
        } else if (factExpense > 0) {
            expenseChain.push(`Расходы ${_fmtMoneyPlain(factExpense)} ₸ составляют ${expenseRatio}% от выручки`);
        }

        expenseChain.push(`Прогноз чистой прибыли ${_fmtMoneyPlain(totalProfit)} ₸`);
        expenseChain.push(`Рентабельность месяца ${margin > 50 ? 'высокая' : margin > 30 ? 'средняя' : 'низкая'} (${margin}%)`);

        lines.push(incomeChain.join(' + ') + ' = ' + incomeChain[incomeChain.length - 1] + ' > ' + expenseChain.join(' > ') + '.');
    } else {
        lines.push(`Текущий баланс ${_fmtMoneyTenge(totalBalance)}`);
    }

    lines.push('');

    // 2. Cash Flow Section with contextualization
    lines.push('2. Динамика Cash Flow');

    if (plannedIncome > 0 || plannedExpense > 0) {
        const cashFlowChain = [];

        if (plannedGap > 0) {
            cashFlowChain.push(`Плановые расходы (${_fmtMoneyPlain(plannedExpense)} ₸) превышают плановые поступления (${_fmtMoneyPlain(plannedIncome)} ₸) на ${_fmtMoneyPlain(plannedGap)} ₸`);
            cashFlowChain.push('Формируется технический дефицит');

            // Contextualization: check if gap is covered by existing surplus
            if (factProfit > plannedGap) {
                cashFlowChain.push(`Разрыв перекрывается накопленным фактическим профицитом месяца (${_fmtMoneyContext(factProfit)})`);
                cashFlowChain.push('Вмешательство и урезание костов не требуются');
            } else {
                cashFlowChain.push(`Накопленный профицит (${_fmtMoneyContext(factProfit)}) НЕ перекрывает разрыв`);
                cashFlowChain.push(`Требуется оптимизация расходов минимум на ${_fmtMoneyPlain(plannedGap - factProfit)} ₸`);
            }
        } else if (plannedGap < 0) {
            cashFlowChain.push(`Плановые поступления (${_fmtMoneyPlain(plannedIncome)} ₸) превышают расходы (${_fmtMoneyPlain(plannedExpense)} ₸) на ${_fmtMoneyPlain(Math.abs(plannedGap))} ₸`);
            cashFlowChain.push('Формируется плановый профицит');
            cashFlowChain.push('Финансовое состояние стабильное');
        } else {
            cashFlowChain.push('Плановые поступления и расходы сбалансированы');
        }

        lines.push(cashFlowChain.join(' > ') + '.');
    } else {
        lines.push('Плановых операций до конца периода нет.');
    }

    lines.push('');

    // 3. Liquidity Focus
    lines.push('3. Фокус внимания (Ликвидность)');

    const liquidityChain = [];
    liquidityChain.push(`Текущий общий баланс ${_fmtMoneyPlain(totalBalance)} ₸ (открытые ${_fmtMoneyPlain(openBalance)} ₸, скрытые ${_fmtMoneyPlain(hiddenBalance)} ₸)`);

    // Check for upcoming large expenses from operations
    const upcomingExpenses = (dbData.operations || [])
        .filter(op => !op.isFact && op.kind === 'expense' && op.amount > 500000)
        .sort((a, b) => new Date(a.date) - new Date(b.date))
        .slice(0, 3);

    if (upcomingExpenses.length > 0) {
        const firstExpense = upcomingExpenses[0];
        const dateStr = firstExpense.dateLabel || new Date(firstExpense.date).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
        liquidityChain.push(`Ближайшее крупное списание: ${dateStr} — ${firstExpense.categoryName || 'расход'} на ${_fmtMoneyPlain(firstExpense.amount)} ₸`);

        if (openBalance > firstExpense.amount * 1.2) {
            liquidityChain.push('Денег достаточно с запасом');
        } else if (openBalance > firstExpense.amount) {
            liquidityChain.push('Денег достаточно, но с запасом впритык');
        } else {
            liquidityChain.push('Недостаточно средств на открытых счетах');
        }
    } else {
        liquidityChain.push('Крупных списаний в ближайшее время не запланировано');
    }

    lines.push(liquidityChain.join(' > ') + '.');

    return lines.join('\n');
}

function handleForecastQuery({ dbData }) {
    const lines = [];
    const endLabel = dbData.meta?.periodEnd || '?';
    const summary = dbData.operationsSummary || {};

    const projectedIncome = Number(summary?.income?.total || 0);
    const projectedExpense = Number(summary?.expense?.total || 0);
    const projectedProfit = projectedIncome - projectedExpense;
    const projectedMargin = projectedIncome > 0 ? Math.round((projectedProfit / projectedIncome) * 100) : 0;

    const plannedIncome = Number(summary?.income?.forecast?.total || 0);
    const plannedExpense = Number(summary?.expense?.forecast?.total || 0);
    const plannedProfit = plannedIncome - plannedExpense;

    const projectedOpen = Number((dbData?.totals?.open?.future ?? dbData?.totals?.open?.current ?? 0));
    const projectedHidden = Number((dbData?.totals?.hidden?.future ?? dbData?.totals?.hidden?.current ?? 0));
    const projectedTotal = Number(dbData?.totals?.all?.future ?? (projectedOpen + projectedHidden));

    const topIncomeCategory = _getTopForecastIncomeCategory(dbData);
    const utilities = _collectUtilitiesFact(dbData);
    const plannedTaxes = _collectPlannedTaxes(dbData);
    const findings = [];

    if (utilities.expense > utilities.income) {
        findings.push(`Факт расход на коммуналку превышает факт доход на ${_fmtMoneyPlain(utilities.expense - utilities.income)} ₸.`);
    }
    if (plannedTaxes > 0) {
        findings.push(`Планируемые налоги ${_fmtMoneyPlain(plannedTaxes)} ₸ значительно повлияют на будущие расходы.`);
    }
    if (plannedExpense > plannedIncome) {
        findings.push(`До конца месяца плановые расходы превышают плановые доходы на ${_fmtMoneyPlain(plannedExpense - plannedIncome)} ₸.`);
    }

    lines.push(`Баланс на ${endLabel}`);
    lines.push(`- Открытые: ${_fmtMoneyTenge(projectedOpen)}`);
    lines.push(`- Скрытые: ${_fmtMoneyTenge(projectedHidden)}`);
    lines.push(`- Итого: ${_fmtMoneyTenge(projectedTotal)}`);
    lines.push('');
    lines.push('Метрики:');
    lines.push(`- Маржа: ${projectedMargin}% (доход ${_fmtMoneyPlain(projectedIncome)}, расход ${_fmtMoneyPlain(projectedExpense)})`);
    lines.push(`- Ликвидность: ${_fmtMoneyPlain(projectedOpen)} на открытых счетах`);
    lines.push(`- Операционная прибыль: ${_fmtMoneyPlain(projectedProfit, { signed: true })}`);
    lines.push('');
    lines.push('Прогноз:');
    lines.push(`- Планируемый расход: ${_fmtMoneyTenge(plannedExpense)}`);
    lines.push(`- Ожидаемый доход: ${_fmtMoneyTenge(plannedIncome)}${topIncomeCategory ? ` (${topIncomeCategory})` : ''}`);
    lines.push(`- Ожидаемая операционная прибыль: ${_fmtMoneyTenge(plannedProfit, { signed: true })}`);
    lines.push('');
    lines.push('Находки:');
    if (findings.length) {
        findings.forEach((item) => lines.push(`- ${item}`));
    } else {
        lines.push('- Критичных аномалий не найдено.');
    }

    return lines.join('\n');
}

// =====================
// TRANSFERS
// =====================
function handleTransfersQuery({ dbData, formatTenge, withdrawalsOnly = false }) {
    const lines = [];
    const isWithdrawalTransfer = (op) => !!(
        op?.isPersonalTransferWithdrawal ||
        (op?.transferPurpose === 'personal' && op?.transferReason === 'personal_use') ||
        (op?.isWithdrawal === true && op?.kind === 'transfer')
    );
    const allTransfers = (dbData.operations || []).filter(op => op.kind === 'transfer');
    const transfers = withdrawalsOnly ? allTransfers.filter(isWithdrawalTransfer) : allTransfers;

    lines.push(withdrawalsOnly ? 'Вывод средств (переводы на личные цели):' : 'Переводы:');
    lines.push('');

    if (!transfers.length) {
        lines.push(withdrawalsOnly ? 'Вывод средств не найден' : 'Переводы не найдены');
        return lines.join('\n');
    }

    // Prioritize fact, then forecast
    const fact = transfers.filter(t => t.isFact);
    const forecast = transfers.filter(t => !t.isFact);

    if (fact.length) {
        lines.push('Факт:');
        fact.slice(0, 10).forEach(t => {
            const from = t.fromAccountName || t.fromCompanyName || t.fromIndividualName || '?';
            const to = t.toAccountName || t.toCompanyName || t.toIndividualName || '?';
            const amt = Math.abs(t.amount || 0);
            if (isWithdrawalTransfer(t)) {
                const toLabel = t.toAccountName || t.toIndividualName || 'Личные нужды';
                lines.push(`• ${formatTenge(amt)}: ${from} → ${toLabel} (вывод средств)`);
                return;
            }
            lines.push(`• ${formatTenge(amt)}: ${from} → ${to}`);
        });
    }

    if (forecast.length) {
        lines.push('');
        lines.push('Прогноз:');
        forecast.slice(0, 10).forEach(t => {
            const from = t.fromAccountName || t.fromCompanyName || t.fromIndividualName || '?';
            const to = t.toAccountName || t.toCompanyName || t.toIndividualName || '?';
            const amt = Math.abs(t.amount || 0);
            if (isWithdrawalTransfer(t)) {
                const toLabel = t.toAccountName || t.toIndividualName || 'Личные нужды';
                lines.push(`• ${formatTenge(amt)}: ${from} → ${toLabel} (вывод средств)`);
                return;
            }
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
    const periodStart = dbData.meta?.periodStart || '?';
    const periodEnd = dbData.meta?.periodEnd || '?';

    lines.push(`Операции по проектам (${periodStart} — ${periodEnd})`);
    lines.push('==============');
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

    // INCOME SECTION
    lines.push('Доходы:');
    let totalIncFact = 0, totalIncForecast = 0;
    sorted.forEach(s => {
        totalIncFact += s.factIncome;
        totalIncForecast += s.forecastIncome;
        lines.push(`${s.name}: ${formatTenge(s.factIncome)} / ${formatTenge(s.forecastIncome)}`);
    });
    lines.push('');
    lines.push('==============');
    lines.push('');
    lines.push(`Итого доходы: ${formatTenge(totalIncFact)} / ${formatTenge(totalIncForecast)}`);
    lines.push('');
    lines.push('==============');
    lines.push('');

    // EXPENSE SECTION
    lines.push('Расходы:');
    let totalExpFact = 0, totalExpForecast = 0;
    sorted.forEach(s => {
        totalExpFact += s.factExpense;
        totalExpForecast += s.forecastExpense;
        lines.push(`${s.name}: ${formatTenge(Math.abs(s.factExpense))} / ${formatTenge(Math.abs(s.forecastExpense))}`);
    });
    lines.push('');
    lines.push('==============');
    lines.push('');
    lines.push(`Итого расходы: ${formatTenge(Math.abs(totalExpFact))} / ${formatTenge(Math.abs(totalExpForecast))}`);
    lines.push('');
    lines.push('==============');
    lines.push('');

    // PROFIT SECTION
    lines.push('Прибыль:');
    let totalProfitFact = 0, totalProfitForecast = 0;
    sorted.forEach(s => {
        const profitFact = s.factIncome - s.factExpense;
        const profitForecast = s.forecastIncome - s.forecastExpense;
        totalProfitFact += profitFact;
        totalProfitForecast += profitForecast;
        lines.push(`${s.name}: ${formatTenge(profitFact)} / ${formatTenge(profitForecast)}`);
    });
    lines.push('');
    lines.push('==============');
    lines.push('');
    lines.push(`Итого прибыль: ${formatTenge(totalProfitFact)} / ${formatTenge(totalProfitForecast)}`);

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
