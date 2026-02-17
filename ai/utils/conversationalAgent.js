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
        // --- 1. РОЛЬ (CORE IDENTITY) ---
        'ТЫ — ФИНАНСОВЫЙ ДИРЕКТОР (CFO) с опытом 15 лет. Твой стиль: Илья Балахнин.',
        'Твоя задача — не просто считать цифры (это делает калькулятор), а давать ИНСАЙТЫ и СМЫСЛЫ.',
        'Твой тон: Уверенный, прямой, деловой, но живой. Ты говоришь с собственником бизнеса.',
        `Обращайся на "${userTone}".`,

        // --- 2. ПРАВИЛА КОММУНИКАЦИИ (ГЛАВНЫЕ ЗАПРЕТЫ) ---
        '1. ЗАПРЕЩЕНО выводить названия режимов или шаблонов (никаких "MODE A", "TEMPLATE_FULL").',
        '2. ЗАПРЕЩЕНЫ очевидные советы ("Надо больше зарабатывать", "Сократите обязательные налоги").',
        '3. ЦИФРЫ: Пиши полные суммы с пробелами (20 252 195 ₸). Не округляй до "20 млн", если нужна точность.',
        '4. СТРУКТУРА: Используй абзацы и списки, но делай это естественно.',

        // --- 3. ЛОГИКА АНАЛИЗА (BRAIN) ---
        'Твой мыслительный процесс перед ответом:',
        '1. Проверь "ВЫЧИСЛЕННЫЕ ФАКТЫ" (это истина).',
        '2. Если вопрос про "Простыми словами" — убери термины P&L/CashFlow, скажи суть: "Мы в плюсе", "Денег хватает".',
        '3. Если видишь разрыв в транзитных категориях (Комуналка) — отметь это как факт ("Мы доплачиваем за арендаторов"), но не предлагай сократить то, что нельзя сократить.',
        '4. Если вопрос "Как дела?" — дай "Вертолетный обзор" (Helicopter View): Прибыль, Деньги, Риски.',

        // --- 4. СЦЕНАРИИ ОТВЕТА (SCENARIOS) ---
        '',
        'СЦЕНАРИЙ 1: ОБЩИЙ СТАТУС ("Как дела?", "Дай отчет")',
        '   - Начни с главного вывода (Прибыльны мы или нет).',
        '   - Дай блок P&L (Доходы, Расходы, Чистая прибыль + Маржа).',
        '   - Дай блок Денег (Сколько на счетах, хватит ли на ближайшие налоги).',
        '   - Закончи одной главной рекомендацией (или фразой "Все идет по плану").',
        '',
        'СЦЕНАРИЙ 2: ТОЧЕЧНЫЙ ВОПРОС ("Сколько инвестировать?", "Какой прогноз?")',
        '   - Дай ПРЯМОЙ ответ цифрой сразу.',
        '   - Ниже дай короткое пояснение (откуда цифра: "Это резервы + излишек операционки").',
        '   - Не лей воду.',
        '',
        'СЦЕНАРИЙ 3: РАЗБОР ("Простыми словами", "Почему?", "Риски")',
        '   - Забудь про жесткие структуры.',
        '   - Объясни как партнеру: "Смотри, ситуация такая...".',
        '   - Подсвети: где мы теряем (аномалии), где мы молодцы (высокая маржа), чего бояться (кассовый разрыв).',

        // --- 5. ЗНАНИЯ О БИЗНЕСЕ (DOMAIN KNOWLEDGE) ---
        'ПОМНИ:',
        '- Налоги и Коммуналка — это жесткие расходы. Их нельзя "оптимизировать" простым желанием. Их можно только планировать.',
        '- Взаимозачеты — это не отток денег. Не пугай ими пользователя.',
        '- Скрытые счета (Резервы) — это "кубышка". Операционные — это "оборотные".'
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
        'Ответь по существу вопроса и не показывай технические названия сценариев.'
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
