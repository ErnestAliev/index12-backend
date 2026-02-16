// ai/utils/conversationalAgent.js
// Conversational AI agent with memory, hypothesis generation, and multi-turn dialogue

/**
 * Generate conversational response with context from chat history
 * @param {Object} params
 * @param {string} params.question - Current user question
 * @param {Array} params.history - Chat history messages [{role, content, timestamp, metadata}]
 * @param {Object} params.metrics - Computed financial metrics
 * @param {Object} params.period - Period info
 * @param {Function} params.formatCurrency - Currency formatter
 * @param {Object} params.availableContext - Available categories, projects, etc
 * @returns {Promise<{ok: boolean, text: string, debug: Object}>}
 */
async function generateConversationalResponse({
    question,
    history = [],
    metrics,
    period,
    formatCurrency,
    futureBalance = null,
    openBalance = null,
    hiddenBalance = null,
    hiddenAccountsData = null,
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
        '',
        '🚨 ФОРМАТ ОТВЕТА:',
        '1. БАЛАНС (открытые/скрытые/итого)',
        '2. КЛЮЧЕВЫЕ МЕТРИКИ (маржа, ликвидность, операционная прибыль)',
        '3. НАХОДКИ (только если есть аномалии с цифрами)',
        '',
        '❌ ЗАПРЕЩЕНО:',
        '- "все идет хорошо", "стабильность", "положительная динамика" - ПУСТЫЕ СЛОВА',
        '- "контролируй налоги", "следи за расходами" - БЕЗ КОНКРЕТИКИ = МУСОР',
        '- Упоминать "Без проекта" - это техническая категория, игнорируй',
        '- Любые фразы без ЦИФР и ДОКАЗАТЕЛЬСТВ',
        '',
        '✅ ПРИМЕР ХОРОШЕГО ОТВЕТА:',
        'Баланс на конец февраля: 50.7M ₸',
        '- Открытые: 4.2M ₸',
        '- Скрытые: 46.5M ₸',
        '',
        'Метрики:',
        '- Маржа: 67% (доход 22.2M, расход 7.3M)',
        '- Ликвидность: 4.2M на открытых счетах',
        '- Операционная прибыль: 14.9M',
        '',
        'Находки:',
        '- [только если есть реальные аномалии]',
        '',
        '❌ ПРИМЕР ПЛОХОГО ОТВЕТА:',
        '"У тебя все идет хорошо... стабильность поступлений... контролируй налоги..."',
        '',
        'СТРАТЕГИЧЕСКИЕ РЕЗЕРВЫ:',
        'Если видишь скрытые счета → называй "стратегический резерв"',
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
        '',
        ...(insights.length > 0 ? ['Финансовый контекст:', ...insights, ''] : []),
        `Период: ${period.startLabel} — ${period.endLabel}`,
        '',
        ...(futureBalance ? [
            'ПРОГНОЗ НА КОНЕЦ ПЕРИОДА:',
            `Текущий баланс: ${formatCurrency(futureBalance.current)}`,
            `  - Открытые счета: ${formatCurrency(openBalance || 0)}`,
            `  - Скрытые счета: ${formatCurrency(hiddenBalance || 0)}`,
            `План доходы: +${formatCurrency(futureBalance.plannedIncome)}`,
            `План расходы: -${formatCurrency(futureBalance.plannedExpense)}`,
            `Итоговый баланс: ${formatCurrency(futureBalance.projected)}`,
            ''
        ] : []),
        ...(hiddenAccountsData && hiddenAccountsData.totalCurrent > 0 ? [
            'СТРАТЕГИЧЕСКИЙ РЕЗЕРВ:',
            `Резервный фонд: ${formatCurrency(hiddenAccountsData.totalCurrent)}`,
            `Прогноз резервов: ${formatCurrency(hiddenAccountsData.totalFuture)}`,
            `(отдельно от основного учёта, ${hiddenAccountsData.count} счетов)`,
            ''
        ] : []),
        ...(categoryDetails.length > 0 ? [
            'НАПОМИНАНИЕ: факт = УЖЕ случилось, план = БУДЕТ в будущем',
            'Данные по категориям:',
            ...categoryDetails,
            ''
        ] : []),
        'ВАЖНО: У тебя есть ВСЕ данные (факт + план) по категориям выше. Используй их для расчётов.'
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
        const text = data.choices?.[0]?.message?.content?.trim();

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
