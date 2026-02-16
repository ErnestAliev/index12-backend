// ai/utils/conversationalAgent.js
// Conversational AI agent with memory, hypothesis generation, and multi-turn dialogue

const _extractMoney = (line) => {
    const m = String(line || '').match(/:\s*([0-9][0-9\s]*)\s*₸/i);
    if (!m) return null;
    const compact = String(m[1] || '').replace(/\s+/g, '').trim();
    if (!/^\d+$/.test(compact)) return null;
    return {
        formatted: String(m[1]).replace(/\s+/g, ' ').trim(),
        numeric: Number(compact)
    };
};

const _formatMoneyNumber = (value) => {
    const n = Number(value || 0);
    try {
        return new Intl.NumberFormat('ru-RU')
            .format(Math.round(Math.abs(n)))
            .replace(/\u00A0/g, ' ');
    } catch (_) {
        return String(Math.round(Math.abs(n)));
    }
};

const _normalizeBalanceBlock = (rawText) => {
    const text = String(rawText || '').trim();
    if (!text) return text;

    const lines = text.split(/\r?\n/);
    const balanceIdx = lines.findIndex((line) => /^\s*Баланс\s+на\b/i.test(line));
    if (balanceIdx < 0) return text;

    const balanceLine = String(lines[balanceIdx] || '').trim();
    const m = balanceLine.match(/^Баланс\s+на\s+([0-9]{2}\.[0-9]{2}\.[0-9]{2,4})(?:\s*[:\-]\s*([0-9][0-9\s]*)\s*₸)?/i);
    if (!m) return text;

    const dateLabel = m[1];
    const headerTotal = m[2] ? String(m[2]).replace(/\s+/g, ' ').trim() : null;

    const openIdx = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Открытые\s*:/i.test(line));
    const hiddenIdx = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Скрытые\s*:/i.test(line));
    const totalIdx = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Итого\s*:/i.test(line));

    lines[balanceIdx] = `Баланс на ${dateLabel}`;

    if (openIdx < 0 || hiddenIdx < 0) {
        return lines.join('\n').trim();
    }

    const openMoney = _extractMoney(lines[openIdx]);
    const hiddenMoney = _extractMoney(lines[hiddenIdx]);
    const totalMoney = totalIdx >= 0 ? _extractMoney(lines[totalIdx]) : null;

    let totalFormatted = totalMoney?.formatted || headerTotal || null;
    if (!totalFormatted && openMoney && hiddenMoney) {
        totalFormatted = _formatMoneyNumber((openMoney.numeric || 0) + (hiddenMoney.numeric || 0));
    }

    if (!totalFormatted) {
        return lines.join('\n').trim();
    }

    if (totalIdx >= 0) {
        lines.splice(totalIdx, 1);
    }

    const hiddenIdxAfterDelete = lines.findIndex((line, idx) => idx > balanceIdx && /^\s*-\s*Скрытые\s*:/i.test(line));
    const insertAt = hiddenIdxAfterDelete >= 0 ? hiddenIdxAfterDelete + 1 : balanceIdx + 1;
    lines.splice(insertAt, 0, `- Итого: ${totalFormatted} ₸`);

    return lines.join('\n').trim();
};

const _extractFindingsFromText = (rawText) => {
    const text = String(rawText || '').trim();
    if (!text) return [];

    const lines = text.split(/\r?\n/);
    const start = lines.findIndex((line) => /^\s*Находки\s*:/i.test(line));
    if (start < 0) return [];

    const findings = [];
    for (let i = start + 1; i < lines.length; i++) {
        const ln = String(lines[i] || '').trim();
        if (!ln) {
            if (findings.length) break;
            continue;
        }
        if (/^[A-Za-zА-Яа-я0-9 _-]+\s*:$/.test(ln) && !ln.startsWith('-')) break;
        if (/^-+\s+/.test(ln)) {
            findings.push(ln.replace(/^-+\s*/, '').trim());
        }
    }
    return findings.filter(Boolean);
};

const _composeForecastResponse = (rawText, forecastData) => {
    if (!forecastData || typeof forecastData !== 'object') {
        return _normalizeBalanceBlock(rawText);
    }

    const projected = forecastData.projected || {};
    const remainingPlan = forecastData.remainingPlan || {};
    const findingsFromLlm = _extractFindingsFromText(rawText);
    const findingsFallback = Array.isArray(forecastData.findings) ? forecastData.findings.filter(Boolean) : [];
    const findings = findingsFromLlm.length ? findingsFromLlm : findingsFallback;

    const topIncomeCategory = String(remainingPlan.topIncomeCategory || '').trim();
    const incomeTail = topIncomeCategory ? ` (${topIncomeCategory})` : '';

    const lines = [
        `Баланс на ${forecastData.periodEndLabel || '?'}`,
        `- Открытые: ${_formatMoneyNumber(projected.openBalance || 0)} ₸`,
        `- Скрытые: ${_formatMoneyNumber(projected.hiddenBalance || 0)} ₸`,
        `- Итого: ${_formatMoneyNumber(projected.totalBalance || 0)} ₸`,
        '',
        'Метрики:',
        `- Маржа: ${Math.round(Number(projected.marginPercent || 0))}% (доход ${_formatMoneyNumber(projected.income || 0)}, расход ${_formatMoneyNumber(projected.expense || 0)})`,
        `- Ликвидность: ${_formatMoneyNumber(projected.liquidityOpen || 0)} на открытых счетах`,
        `- Операционная прибыль: ${_formatMoneyNumber(projected.operatingProfit || 0)}`,
        '',
        'Прогноз:',
        `- Планируемый расход: ${_formatMoneyNumber(remainingPlan.expense || 0)} ₸`,
        `- Ожидаемый доход: ${_formatMoneyNumber(remainingPlan.income || 0)} ₸${incomeTail}`,
        `- Ожидаемая операционная прибыль: ${_formatMoneyNumber(projected.operatingProfit || 0)} ₸`,
        '',
        'Находки:'
    ];

    if (findings.length) {
        findings.forEach((item) => lines.push(`- ${item}`));
    } else {
        lines.push('- Критичных аномалий не найдено.');
    }

    return lines.join('\n').trim();
};

/**
 * Generate conversational response with context from chat history
 * @param {Object} params
 * @param {string} params.question - Current user question
 * @param {Array} params.history - Chat history messages [{role, content, timestamp, metadata}]
 * @param {Object} params.metrics - Computed financial metrics
 * @param {Object} params.period - Period info
 * @param {Function} params.formatCurrency - Currency formatter
 * @param {Object} params.availableContext - Available categories, projects, etc
 * @param {Object|null} params.forecastData - Deterministic forecast snapshot
 * @returns {Promise<{ok: boolean, text: string, debug: Object}>}
 */
async function generateConversationalResponse({
    question,
    history = [],
    metrics,
    period,
    currentDate = null,
    formatCurrency,
    futureBalance = null,
    openBalance = null,
    hiddenBalance = null,
    hiddenAccountsData = null,
    accounts = null,
    forecastData = null,
    availableContext = {}
}) {
    const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
    if (!OPENAI_KEY) {
        console.error('[conversationalAgent] No OpenAI API key found');
        return {
            ok: false,
            text: 'Извините, AI временно недоступен.',
            debug: { error: 'No API key' }
        };
    }

    // Build conversation context from history
    const conversationMessages = history.slice(-10).map(msg => ({
        role: msg.role,
        content: msg.content
    }));

    // Prepare financial insights
    const insights = [];

    if (metrics.plan.expense > 0) {
        insights.push(`Запланировано расходов: ${formatCurrency(metrics.plan.expense)}`);
    }
    if (metrics.fact.income > 0) {
        insights.push(`Факт доходы: ${formatCurrency(metrics.fact.income)}`);
    }
    if (metrics.total.net !== 0) {
        insights.push(`Чистый результат: ${formatCurrency(metrics.total.net)}`);
    }

    // Top categories
    const topCategories = Object.values(metrics.byCategory || {})
        .filter(cat => Math.abs(cat.total.net) > 0)
        .sort((a, b) => Math.abs(b.total.net) - Math.abs(a.total.net))
        .slice(0, 3);

    if (topCategories.length > 0) {
        const catNames = topCategories.map(c => c.name).join(', ');
        insights.push(`Основные категории: ${catNames}`);
    }

    // Top projects  
    const topProjects = Object.values(metrics.byProject || {})
        .filter(proj => proj.total.net !== 0)
        .sort((a, b) => Math.abs(b.total.net) - Math.abs(a.total.net))
        .slice(0, 2);

    if (topProjects.length > 0) {
        const projNames = topProjects.map(p => p.name).join(', ');
        insights.push(`Активные проекты: ${projNames}`);
    }

    // Detect user's tone
    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' :
        /\b(вы|ваш|ваши|вас|вам)\b/i.test(question) ? 'вы' : 'ты';

    // Detect if this is a greeting (new conversation start)
    const isGreeting = /^(привет|здравствуй|добрый день|доброе утро|добрый вечер|hi|hello)/i.test(question.trim());

    const systemPrompt = [
        'Ты AI-финансист INDEX12. Стиль: эксперт, аналитик, краткий.',
        `Обращение: на "${userTone}".`,
        '',
        'КРИТИЧЕСКИ ВАЖНО - Факт vs План:',
        'Факт = УЖЕ случилось (статус "Исполнено")',
        'План = БУДЕТ в будущем (дата > сегодня)',
        'ЭТО НЕ бюджет vs факт! ЭТО прошлое vs будущее!',
        '',
        'Примеры использования:',
        '- "Факт доход 18 600 000 ₸" = уже получили деньги',
        '- "План доход 3 600 000 ₸" = ожидаем получить в будущем',
        'Ты финансовый аналитик. Отвечай КРАТКО, КОНКРЕТНО, БЕЗ ВОДЫ.',
        'Если ниже передан блок FORECAST_DATA, используй его значения как единственный источник чисел для блоков Баланс/Метрики/Прогноз.',
        '',
        '🚨 ОБЯЗАТЕЛЬНЫЙ ФОРМАТ ОТВЕТА (НЕ ОТКЛОНЯЙСЯ):',
        'Баланс на [текущую дату]',
        '- Открытые: [сумма] ₸',
        '- Скрытые: [сумма] ₸',
        '- Итого: [общая сумма] ₸',
        '',
        'Метрики:',
        '- Маржа: [%] (доход [сумма], расход [сумма])',
        '- Ликвидность: [сумма] на открытых счетах',
        '- Операционная прибыль: [сумма]',
        '',
        'Находки:',
        '- [только если есть аномалии, которые можно исправить]',
        '',
        '❌ СТРОГО ЗАПРЕЩЕНО:',
        '- "все идет хорошо", "стабильность", "положительная динамика" - ПУСТЫЕ СЛОВА',
        '- "контролируй налоги", "следи за налогами", "учитывай налоги", "налоги могут повлиять", "запланированные налоговые расходы", "налоги повлияют на баланс" - НА ЭТО НЕЛЬЗЯ ПОВЛИЯТЬ!',
        '- Упоминать "Без проекта" - это техническая категория, игнорируй',
        '- Любые фразы без ЦИФР и ДОКАЗАТЕЛЬСТВ',
        '- Упоминать факторы, на которые пользователь не может повлиять',
        '- Использовать сокращения чисел (50.378M, 164K) - ТОЛЬКО ПОЛНЫЕ ЧИСЛА!',
        '',
        '✅ ПРИМЕР ИДЕАЛЬНОГО ОТВЕТА:',
        'Баланс на 16.02.26',
        '- Открытые: 4 285 000 ₸',
        '- Скрытые: 46 378 000 ₸',
        '- Итого: 50 663 000 ₸',
        '',
        'Метрики:',
        '- Маржа: 68% (доход 19 770 000, расход 6 212 000)',
        '- Ликвидность: 4 285 000 на открытых счетах',
        '- Операционная прибыль: 15 097 000',
        '',
        'Находки:',
        '- Расход на коммуналку превышает доход на 1 666 000 ₸',
        '',
        '❌ ПРИМЕР ПЛОХОГО ОТВЕТА:',
        '"У тебя все идет хорошо... стабильность поступлений... налоги могут повлиять..."',
        '',
        'ВАЖНО О БАЛАНСАХ:',
        'Открытые счета = НЕ isHidden И НЕ isExcluded',
        'Скрытые счета = isHidden ИЛИ isExcluded',
        'ВСЕГДА показывай каждый счет отдельно с его текущим балансом!',
        '',
        'СТРАТЕГИЧЕСКИЕ РЕЗЕРВЫ:',
        'Если видишь скрытые счета → называй "стратегический резерв" или просто их название',
        'При вопросах об инвестициях → спроси про месячные расходы',
        ''
    ].join(' ');

    // Prepare detailed category data
    const categoryDetails = [];
    Object.entries(availableContext.byCategory || {}).forEach(([name, data]) => {
        // 🟢 Skip "Без проекта" - technical category with no value
        if (name === 'Без проекта') return;

        const parts = [];
        if (data.fact.income > 0) parts.push(`факт доход ${formatCurrency(data.fact.income)}`);
        if (data.fact.expense > 0) parts.push(`факт расход ${formatCurrency(data.fact.expense)}`);
        if (data.plan.income > 0) parts.push(`план доход ${formatCurrency(data.plan.income)}`);
        if (data.plan.expense > 0) parts.push(`план расход ${formatCurrency(data.plan.expense)}`);
        if (parts.length > 0) {
            categoryDetails.push(`${name}: ${parts.join(', ')}`);
        }
    });

    const userContent = [
        `Текущий вопрос: ${question}`,
        `Текущая дата: ${currentDate || period.endLabel}`,  // Use passed currentDate or fallback to period end
        '',
        ...(insights.length > 0 ? ['Финансовый контекст:', ...insights, ''] : []),
        `Период данных: ${period.startLabel} — ${period.endLabel}`,
        '',
        ...(accounts && accounts.length > 0 ? [
            'СЧЕТА (только итоги):',
            `- Открытые: ${formatCurrency(openBalance || 0)}`,
            `- Скрытые: ${formatCurrency(hiddenBalance || 0)}`,
            ''
        ] : []),
        ...(futureBalance ? [
            'ПРОГНОЗ НА КОНЕЦ ПЕРИОДА:',
            `Текущий баланс: ${formatCurrency(futureBalance.current)}`,
            `План доходы: +${formatCurrency(futureBalance.plannedIncome)}`,
            `План расходы: -${formatCurrency(futureBalance.plannedExpense)}`,
            `Итоговый баланс: ${formatCurrency(futureBalance.projected)}`,
            ''
        ] : []),
        ...(forecastData ? [
            'FORECAST_DATA (используй числа без изменений):',
            JSON.stringify(forecastData, null, 2),
            ''
        ] : []),
        ...(categoryDetails.length > 0 ? [
            'НАПОМИНАНИЕ: факт = УЖЕ случилось, план = БУДЕТ в будущем',
            'Данные по категориям:',
            ...categoryDetails,
            ''
        ] : []),
        'ВАЖНО: У тебя есть ВСЕ данные по счетам и категориям выше. Используй их для ответа.'
    ].join('\n');

    try {
        const messages = [
            { role: 'system', content: systemPrompt },
            // If greeting, ignore history to start fresh
            ...(isGreeting ? [] : conversationMessages),
            { role: 'user', content: userContent }
        ];

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${OPENAI_KEY}`
            },
            body: JSON.stringify({
                model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
                messages,
                temperature: 0.7,
                max_tokens: 500  // Increased for detailed calculations with multiple categories
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error('[conversationalAgent] OpenAI API error:', response.status, errorText);
            return {
                ok: false,
                text: `Привет! ${insights[0] || 'Все в порядке.'}`,
                debug: { error: 'API error', status: response.status }
            };
        }

        const data = await response.json();
        const rawText = data.choices?.[0]?.message?.content?.trim();
        const text = forecastData
            ? _composeForecastResponse(rawText, forecastData)
            : _normalizeBalanceBlock(rawText);

        if (!text) {
            return {
                ok: true,
                text: `Привет! ${insights[0] || 'Все в порядке.'}`,
                debug: { fallback: true, reason: 'Empty LLM response' }
            };
        }

        return {
            ok: true,
            text,
            debug: {
                model: data.model,
                usage: data.usage,
                historyLength: conversationMessages.length
            }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: `Привет! ${insights[0] || 'Все в порядке.'}`,
            debug: { error: err.message }
        };
    }
}

/**
 * Verify calculation for a specific category
 * @param {string} categoryName - Category to verify
 * @param {Object} metrics - Computed metrics
 * @param {Function} formatCurrency - Currency formatter
 * @returns {string} Verification result
 */
function verifyCalculation(categoryName, metrics, formatCurrency) {
    const categoryData = metrics.byCategory?.[categoryName];

    if (!categoryData) {
        return `Категория "${categoryName}" не найдена в данных.`;
    }

    const lines = [];
    lines.push(`Расчёты по категории "${categoryName}":`);

    if (categoryData.fact.income > 0) {
        lines.push(`- Факт доходы: ${formatCurrency(categoryData.fact.income)}`);
    }
    if (categoryData.fact.expense > 0) {
        lines.push(`- Факт расходы: ${formatCurrency(categoryData.fact.expense)}`);
    }
    if (categoryData.plan.income > 0) {
        lines.push(`- План доходы: ${formatCurrency(categoryData.plan.income)}`);
    }
    if (categoryData.plan.expense > 0) {
        lines.push(`- План расходы: ${formatCurrency(categoryData.plan.expense)}`);
    }
    if (categoryData.total.net !== 0) {
        lines.push(`- Итого: ${formatCurrency(categoryData.total.net)}`);
    }

    return lines.join('\n');
}

module.exports = {
    generateConversationalResponse,
    verifyCalculation
};
