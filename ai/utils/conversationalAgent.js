// ai/utils/conversationalAgent.js
// Conversational AI agent with memory and context-first financial analysis
const fs = require('fs/promises');
const path = require('path');

async function dumpLlmInputSnapshot(payload) {
    try {
        const dir = path.resolve(__dirname, '..', 'debug');
        const stamp = new Date().toISOString().replace(/[:.]/g, '-');
        const latestPath = path.join(dir, 'llm-input-latest.json');
        const archivePath = path.join(dir, `llm-input-${stamp}.json`);

        await fs.mkdir(dir, { recursive: true });
        const body = JSON.stringify(payload, null, 2);
        await fs.writeFile(latestPath, body, 'utf8');
        await fs.writeFile(archivePath, body, 'utf8');

        return { latestPath, archivePath };
    } catch (err) {
        console.error('[conversationalAgent] Snapshot dump error:', err?.message || err);
        return null;
    }
}

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
    graphTooltipData = null,
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
    const graphTooltipDigest = (() => {
        if (!graphTooltipData || typeof graphTooltipData !== 'object') return null;

        const daily = Array.isArray(graphTooltipData.daily) ? graphTooltipData.daily : [];
        const balancesByDay = Array.isArray(graphTooltipData.accountBalancesByDay)
            ? graphTooltipData.accountBalancesByDay
            : [];
        const operationsByDay = Array.isArray(graphTooltipData.operationsByDay)
            ? graphTooltipData.operationsByDay
            : [];

        return {
            period: graphTooltipData.period || null,
            asOfDayKey: graphTooltipData.asOfDayKey || null,
            dayCount: daily.length,
            daily,
            accountBalancesByDay: balancesByDay,
            operationsByDay: operationsByDay.map((day) => ({
                dayKey: day?.dayKey || '',
                dateLabel: day?.dateLabel || '',
                opCount: Array.isArray(day?.items) ? day.items.length : 0,
                income: (Array.isArray(day?.items) ? day.items : [])
                    .filter((item) => item?.kind === 'income')
                    .reduce((sum, item) => sum + Number(item?.amount || 0), 0),
                expense: (Array.isArray(day?.items) ? day.items : [])
                    .filter((item) => item?.kind === 'expense')
                    .reduce((sum, item) => sum + Number(item?.amount || 0), 0),
                transfer: (Array.isArray(day?.items) ? day.items : [])
                    .filter((item) => item?.kind === 'transfer')
                    .reduce((sum, item) => sum + Number(item?.amount || 0), 0)
            })),
            accountBalancesAtAsOf: Array.isArray(graphTooltipData.accountBalancesAtAsOf)
                ? graphTooltipData.accountBalancesAtAsOf
                : [],
            accountBalancesAtPeriodEnd: Array.isArray(graphTooltipData.accountBalancesAtPeriodEnd)
                ? graphTooltipData.accountBalancesAtPeriodEnd
                : []
        };
    })();

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
        ...(graphTooltipDigest ? [
            '--- ДАННЫЕ ИЗ ГРАФИКОВЫХ ТУЛТИПОВ (АГРЕГАЦИИ И БАЛАНСЫ) ---',
            JSON.stringify(graphTooltipDigest, null, 2),
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

        const model = process.env.OPENAI_MODEL || 'gpt-4o';
        const llmRequest = {
            model,
            messages,
            temperature: 0.1,
            max_tokens: 450
        };

        const snapshotInfo = await dumpLlmInputSnapshot({
            generatedAt: new Date().toISOString(),
            question,
            period,
            currentDate,
            llmInput: {
                systemPrompt,
                userContent,
                conversationMessagesUsed: conversationMessages,
                request: llmRequest
            },
            serviceData: {
                metrics,
                futureBalance,
                openBalance,
                hiddenBalance,
                hiddenAccountsData,
                accounts,
                forecastData,
                riskData,
                graphTooltipData,
                availableContext
            },
            computedFacts: {
                safeSpend,
                hiddenTotal,
                investPotential,
                projectedBal
            }
        });

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${OPENAI_KEY}`
            },
            body: JSON.stringify(llmRequest)
        });

        if (!response.ok) throw new Error(`API Status ${response.status}`);
        const data = await response.json();
        const text = String(data?.choices?.[0]?.message?.content || '').trim();

        if (!text) throw new Error('Empty response');

        return {
            ok: true,
            text,
            debug: {
                model: data.model,
                usage: data.usage,
                llmInputSnapshot: snapshotInfo
            }
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
