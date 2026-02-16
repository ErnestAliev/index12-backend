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

    const systemPrompt = [
        'Ты AI-ассистент INDEX12 в стиле Ильи Балахнина - бизнес-эксперт и финансовый аналитик.',
        `Общайся на "${userTone}".`,
        '',
        'Стиль общения:',
        '1. Hypothesis-driven: делай обоснованные гипотезы из данных',
        '2. Задавай уточняющие вопросы: "Все верно?", "Проверим?"',
        '3. Будь проактивным: "Давай проверим расчёты", "Смотри, видимо..."',
        '4. Аналитический подход: делай выводы из цифр',
        '5. Профессиональный тон: без сленга, чистый русский',
        '',
        'Возможности:',
        '- Анализируй категории и делай гипотезы о их назначении',
        '- Предлагай проверить расчёты когда пользователь спрашивает про конкретную категорию',
        '- Задавай уточняющие вопросы для лучшего понимания',
        '- Проводи расчёты на основе данных когда пользователь подтверждает гипотезу',
        '',
        'Примеры общей логики рассуждений:',
        'User: что с категорией X?',
        'Assistant: Смотри, судя по названию категории видимо это Y. В данных вижу [факт/план]. Давай проверим расчёты или уточни детали?',
        '',
        'User: да, нужно проверить расчёт',
        'Assistant: Понял! Вижу в данных: факт A ₸, план B ₸, итого C ₸. [делает конкретный анализ]. Всё верно?',
        '',
        'Формат:',
        '- Используй ПОЛНЫЕ суммы: "18 789 195 ₸" (НЕ сокращения)',
        '- Максимум 3-4 предложения',
        '- Будь конкретным и полезным',
        '- Если нужна дополнительная информация - спроси'
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
        ...(categoryDetails.length > 0 ? ['Данные по категориям:', ...categoryDetails, ''] : []),
        'ВАЖНО: У тебя есть ВСЕ данные (факт + план) по категориям выше. Используй их для расчётов.'
    ].join('\n');

    try {
        const messages = [
            { role: 'system', content: systemPrompt },
            ...conversationMessages, // Previous conversation
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
                max_tokens: 500
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
