// ai/utils/conversationalAgent.js
// Conversational AI agent with memory and context-first financial analysis

/**
 * Generate conversational response with context from chat history
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
    riskData = null,
    availableContext = {}
}) {
    const OPENAI_KEY = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
    if (!OPENAI_KEY) {
        console.error('[conversationalAgent] No OpenAI API key found');
        return {
            ok: false,
            text: 'AI временно недоступен: не найден API ключ.',
            debug: { error: 'No API key' }
        };
    }

    const conversationMessages = history
        .slice(-6) // Сокращаем историю, чтобы сбить "шаблонную инерцию"
        .map((msg) => ({
            role: msg.role,
            content: msg.content
        }));

    const userTone = /\b(ты|твой|твои|тебя|тебе)\b/i.test(question) ? 'ты' : 'вы';

    // --- JS PRE-CALCULATION (Hard Facts) ---
    // Считаем цифры заранее, чтобы LLM не галлюцинировала с математикой
    const safeSpend = Number(riskData?.safeSpend || 0);
    const hiddenTotal = Number(hiddenBalance || 0);
    const investPotential = hiddenTotal + safeSpend;
    const projectedBal = futureBalance?.projected || 0;

    // Аномалии для контекста (если есть)
    const anomalies = [];
    if (riskData?.topOutflows) {
        // Пример простой проверки транзитов (если бы она была реализована в financialCalculator)
        // Здесь мы полагаемся на то, что модель найдет их в JSON
    }

    const systemPrompt = [
        'ТЫ — ФИНАНСОВЫЙ ДИРЕКТОР (CFO) с опытом 15 лет. Стиль: Илья Балахнин.',
        `Обращайся на "${userTone}".`,
        '',
        'КЛЮЧЕВОЕ ТРЕБОВАНИЕ: меньше текста, больше цифр.',
        '',
        'ФОРМАТ ОТВЕТА (ОБЯЗАТЕЛЬНО):',
        '1. Ответ 4-8 строк.',
        '2. Минимум 70% строк должны содержать числовой показатель.',
        '3. Формат сумм: 20 252 195 ₸ (без "20 млн", "196к").',
        '4. Каждая строка: метрика -> число -> короткий вывод.',
        '5. Без длинных вводных абзацев.',
        '',
        'ЗАПРЕТНЫЕ ФОРМУЛИРОВКИ:',
        '- "Прогноз выглядит следующим образом"',
        '- "Основные факторы, влияющие..."',
        '- "В целом мы в стабильной позиции"',
        '- "положительно сказывается на общей картине"',
        '- "уверенно смотреть в будущее"',
        '',
        'ПРАВИЛА СОДЕРЖАНИЯ:',
        '1. Сначала используй "ВЫЧИСЛЕННЫЕ ФАКТЫ" как источник истины.',
        '2. Взаимозачеты не трактуй как отток денег.',
        '3. Налоги и коммуналка — жесткие расходы: не предлагай "просто сократить".',
        '4. Не выводи технические названия режимов/шаблонов.',
        '',
        'ЕСЛИ ВОПРОС ПРО ПРОГНОЗ (например: "какой прогноз?"):',
        'Выведи строго в этом формате:',
        'Прогноз на конец периода: [CALC_PROJECTED_BALANCE]',
        '- План доходов до конца: [число]',
        '- План расходов до конца: [число]',
        '- Разрыв плана: [число]',
        '- Операционные: [число]',
        '- Резервы: [число]',
        '- Ближайшее списание: [дата] — [сумма]',
        'Вывод: [1 короткая строка, максимум 12 слов].',
        '',
        'ЕСЛИ ВОПРОС "КАК ДЕЛА?":',
        '- Прибыль: [число] и маржа [число]%',
        '- Деньги: операционные [число], резервы [число]',
        '- Риски: 1-2 пункта только с цифрами.',
        '',
        'ЕСЛИ ТОЧЕЧНЫЙ ВОПРОС:',
        '- Дай прямой ответ цифрой в первой строке.',
        '- Ниже максимум 2 строки обоснования с цифрами.'
    ].join('\n');

    const userContent = [
        `ВОПРОС ПОЛЬЗОВАТЕЛЯ: "${question}"`,
        `ДАТА: ${currentDate || period?.endLabel || '?'}`,
        '',
        '--- ВЫЧИСЛЕННЫЕ ФАКТЫ (ИСТОЧНИК ПРАВДЫ) ---',
        `CALC_INVEST_POTENTIAL: ${formatCurrency(investPotential)} (Сумма: Скрытые ${formatCurrency(hiddenTotal)} + Свободная операционка ${formatCurrency(safeSpend)})`,
        `CALC_PROJECTED_BALANCE: ${formatCurrency(projectedBal)}`,
        `CALC_SAFE_SPEND: ${formatCurrency(safeSpend)}`,
        '',
        ...(accounts && accounts.length ? [
            '--- СЧЕТА ---',
            `Операционные (Рабочие): ${formatCurrency(openBalance || 0)}`,
            `Резервы (Скрытые): ${formatCurrency(hiddenBalance || 0)}`,
            ''
        ] : []),
        ...(Object.keys(metrics?.byCategory || {}).length ? [
            '--- ДАННЫЕ ПО КАТЕГОРИЯМ (ИСКАТЬ АНОМАЛИИ ЗДЕСЬ) ---',
            JSON.stringify(metrics.byCategory, (key, value) => {
                if (['all', 'count', 'name'].includes(key)) return undefined;
                return value;
            }, 2),
            ''
        ] : []),
        'Отвечай коротко: 4-8 строк, максимум цифр, минимум текста.'
    ].join('\n');

    try {
        const messages = [
            { role: 'system', content: systemPrompt },
            ...conversationMessages,
            { role: 'user', content: userContent }
        ];

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${OPENAI_KEY}`
            },
            body: JSON.stringify({
                model: process.env.OPENAI_MODEL || 'gpt-4o',
                messages,
                temperature: 0.1, // Низкая температура для точности данных
                max_tokens: 450
            })
        });

        if (!response.ok) throw new Error(`API Status ${response.status}`);
        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();

        if (!text) throw new Error('Empty response');

        return {
            ok: true,
            text,
            debug: { model: data.model, usage: data.usage }
        };
    } catch (err) {
        console.error('[conversationalAgent] Error:', err);
        return {
            ok: false,
            text: 'Не удалось сформировать ответ. Проверьте данные.',
            debug: { error: err.message }
        };
    }
}

function verifyCalculation() { return ''; }

module.exports = {
    generateConversationalResponse,
    verifyCalculation
};
