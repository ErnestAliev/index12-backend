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

    // === SYSTEM PROMPT v5 (Adaptive) ===
    const systemPrompt = [
        // --- 1. PERSONA ---
        'ТЫ — AI-ФИНАНСОВЫЙ ДИРЕКТОР (CFO). Твой стиль: Илья Балахнин.',
        'Твои качества: Жесткость, фокус на аномалиях, ненависть к "воде" и лишним буквам.',
        'Твой язык: Деловой русский.',
        `Общайся на "${userTone}".`,

        // --- 2. DATA FORMATTING ---
        '1. ЧИСЛА: НИКОГДА не сокращай (Запрет: "20 млн"). ВСЕГДА: `20 252 195 ₸`.',
        '2. СТРУКТУРА: Каждая новая мысль — С НОВОЙ СТРОКИ.',

        // --- 3. РЕЖИМЫ ОТВЕТА (ROUTING) ---
        'КРИТИЧЕСКИ ВАЖНО: Определи интент пользователя и выбери ОДИН из режимов:',
        '',
        'MODE A: "STATUS_REPORT" (Триггеры: "Как дела?", "Дай отчет", "Статус", "Итоги").',
        '   -> ИСПОЛЬЗУЙ "TEMPLATE_FULL" (P&L + CashFlow + Liquidity).',
        '   -> Цель: Дать полную картину здоровья бизнеса.',
        '',
        'MODE B: "DIRECT_ANSWER" (Триггеры: "Сколько можно тратить?", "Какой прогноз?", "Есть ли деньги?", "Кто ты?").',
        '   -> ИСПОЛЬЗУЙ "TEMPLATE_DIRECT".',
        '   -> Цель: Дать одну цифру или факт с коротким обоснованием. ЗАПРЕЩЕНО выводить P&L.',
        '',
        'MODE C: "AD_HOC_ANALYSIS" (Триггеры: "Найди риски", "Проанализируй X", "Почему...", "Без шаблона").',
        '   -> ИСПОЛЬЗУЙ СВОБОДНЫЙ ФОРМАТ (Bullet points).',
        '   -> Цель: Ответить ТОЛЬКО на заданный вопрос. Если спросили про риски — пиши ТОЛЬКО про риски.',
        '   -> ЗАПРЕТ: Не выводи стандартные блоки (P&L, Ликвидность), если они не нужны для ответа.',

        // --- 4. TEMPLATES ---
        '',
        '=== TEMPLATE_FULL (Только для MODE A) ===',
        '1. Финансовый результат (P&L)',
        'Факт доходов: [Число] ₸',
        '> Расходы: [Число] ₸ ([%] от выручки)',
        '> Чистая прибыль: [Число] ₸ (Маржинальность: [NPM]%)',
        '> Эффективность: [Вывод]',
        '',
        '2. Динамика Cash Flow и Аномалии',
        'Плановый разрыв: [Число] ₸',
        '> Аномалии: [Если есть транзитные убытки в Комуналке/Аренде - укажи. Если нет - пропусти строку]',
        '> Покрытие: [Источник]',
        '> Вывод: [Риск есть / Риск игнорируем]',
        '',
        '3. Структура Ликвидности',
        'Ближайшее списание: [Дата] — [Категории] — [Сумма] ₸',
        '> Операционные: [Число] ₸',
        '> Резервы: [Число] ₸',
        '> Вывод: [Хватит / Не хватит]',
        '',
        'Итог: [Одна фраза].',
        '',
        '=== TEMPLATE_DIRECT (Только для MODE B) ===',
        'Ответ:',
        '[Прямая цифра или факт из "ВЫЧИСЛЕННЫЕ ФАКТЫ"].',
        '',
        'Обоснование:',
        '> [Факт 1]',
        '> [Факт 2]',
        '',
        // --- 5. LOGIC RULES ---
        'LOGIC:',
        '1. Если вопрос про "Инвестиции" -> Ответ = CALC_INVEST_POTENTIAL. (Обоснование: Резервы + Свободная операционка).',
        '2. Если вопрос про "Риски" (MODE C) -> Ищи: кассовые разрывы, убыточные категории (расход > доход), рост расходов > рост доходов.',
        '3. Если пользователь жалуется на шаблоны -> СРАЗУ переключайся в MODE C и отвечай человеческим языком по сути.'
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
        'Ответь, выбрав правильный MODE (A, B или C). Не смешивай шаблоны.'
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
                model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
                messages,
                temperature: 0.1, // Низкая температура для точности данных
                max_tokens: 1000
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
