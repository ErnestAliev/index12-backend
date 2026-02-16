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
        '- Общий анализ: факт 18 600 000 ₸ + план 3 600 000 ₸ = всего 22 200 000 ₸',
        '',
        ...(isGreeting ? [
            'ПРИВЕТСТВИЕ - пользователь не знает на что обратить внимание.',
            'Анализируй ОБЩУЮ картину, НЕ детали категорий.',
            '',
            'ПРИОРИТЕТЫ:',
            '1. Кассовый разрыв: общие план расходы > общие факт доходы → ПРЕДУПРЕДИ',
            '2. Крупные запланированные расходы: общая сумма > 30% баланса → обрати внимание + успокой',
            '3. Позитивная динамика: факт доходы значительно больше плана → похвали',
            '4. Стабильно: краткая сводка с общими цифрами',
            '',
            'ЗАПРЕЩЕНО в приветствии: упоминать конкретные названия категорий/проектов',
            'Формат: Привет! [общая финансовая картина с ОБЩИМИ цифрами]',
            ''
        ] : []),
        'ОБЯЗАТЕЛЬНАЯ ПОСЛЕДОВАТЕЛЬНОСТЬ:',
        '1. ВЫЧИСЛИ: для КАЖДОЙ категории сложи факт+план = итог',
        '2. СРАВНИ: найди максимум среди итогов',
        '3. ОТВЕТЬ: назови категорию с максимальной суммой',
        '',
        'Пример вычислений:',
        '- Аренда: факт 18 600 000 ₸ + план 3 600 000 ₸ = 22 200 000 ₸',
        '- Налоги: факт 0 ₸ + план 3 121 328 ₸ = 3 121 328 ₸',
        '→ Ответ: "Больше всего на аренду (22 200 000 ₸)"',
        '',
        'Стиль: КРАТКОСТЬ (1-3 предложения), полные суммы, по делу'
    ].join(' ');

    // Prepare detailed category data
    const categoryDetails = [];
    Object.entries(availableContext.byCategory || {}).forEach(([name, data]) => {
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
                max_tokens: 250  // Allow detailed insights with context
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
