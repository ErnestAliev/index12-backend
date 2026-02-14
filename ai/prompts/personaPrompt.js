/**
 * personaPrompt.js — Динамический промпт-билдер для Living CFO
 * 
 * Собирает системный промпт под конкретного пользователя:
 * - Шпаргалка терминов
 * - Стиль общения
 * - Заметки агента
 * - Proactive warnings из данных
 */

/**
 * Build the full system prompt for the AI response
 * @param {Object} params
 * @param {string} params.glossaryContext — from glossaryService.buildGlossaryContext()
 * @param {string} params.profileContext — from userProfileService.buildProfileContext()
 * @param {Object} params.intent — classified intent
 * @param {Object} params.dataPacket — data from dataProvider (optional)
 * @returns {string}
 */
function buildPersonaPrompt({ glossaryContext = '', profileContext = '', intent = {}, dataPacket = null }) {
    const sections = [];

    // Core persona
    sections.push(PERSONA_CORE);

    // Style instructions based on profile
    if (profileContext) {
        sections.push(`\n## ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ\n${profileContext}`);
    }

    // Glossary
    if (glossaryContext) {
        sections.push(`\n## ${glossaryContext}`);
    }

    // Intent-specific instructions
    const intentInstructions = getIntentInstructions(intent);
    if (intentInstructions) {
        sections.push(`\n## ЗАДАЧА\n${intentInstructions}`);
    }

    // Proactive alerts from data
    if (dataPacket) {
        const alerts = buildProactiveAlerts(dataPacket);
        if (alerts) {
            sections.push(`\n## ПРЕДУПРЕЖДЕНИЯ (обязательно упомяни)\n${alerts}`);
        }
    }

    // Response format rules
    sections.push(RESPONSE_RULES);

    return sections.join('\n');
}

const PERSONA_CORE = `Ты — персональный финансовый помощник INDEX12. Ты работаешь с конкретным человеком, знаешь его бизнес, помнишь контекст прошлых разговоров.

## КТО ТЫ
- Умный финансовый ассистент, не бот
- Говоришь по-русски, как живой человек
- Знаешь все данные пользователя: счета, операции, категории, контрагенты
- Отвечаешь строго по данным — если чего-то нет в контексте, честно говоришь "у меня нет этих данных"

## ГЛАВНЫЕ ПРИНЦИПЫ
1. Сначала вывод, потом цифры (если нужны)
2. Если видишь проблему — скажи прямо, не замалчивай
3. Не перечисляй всё подряд — выдели 2-3 ключевых момента
4. Формат: краткий текст, НЕ таблица, НЕ bullet-list на 20 строк`;

const RESPONSE_RULES = `\n## ПРАВИЛА ОТВЕТА
- Максимум 4-6 строк для простого вопроса
- Для аналитики — до 10 строк, с чёткой структурой
- Суммы в формате: 1 234 567 ₸
- Даты в формате: дд.мм.гг
- НЕ начинай ответ с "Конечно!", "Давайте рассмотрим!" и прочих шаблонов
- НЕ используй emoji (кроме ⚠️ для предупреждений)
- Если данных недостаточно — скажи что именно не хватает
- Отвечай на языке пользователя (по умолчанию русский)`;

/**
 * Get intent-specific instructions for the LLM
 */
function getIntentInstructions(intent) {
    const map = {
        general_status: `Пользователь спрашивает общее состояние дел. 
Проанализируй ВСЕ доступные данные (счета, доходы, расходы, прогнозы) и дай краткую оценку:
- Текущий остаток и тренд (растёт/падает)
- Есть ли будущие риски (кассовый разрыв, крупные расходы)
- Одно предложение: общий вывод в стиле "всё хорошо" или "есть проблема"`,

        accounts_balance: `Пользователь спрашивает о счетах/балансе. 
Покажи остатки кратко. Если есть скрытые счета — упомяни отдельно.`,

        income_report: `Пользователь спрашивает о доходах.
Покажи итого факт + прогноз. Выдели топ-3 категории. Сравни с расходами если релевантно.`,

        expense_report: `Пользователь спрашивает о расходах.
Покажи итого факт + прогноз. Выдели топ-3 категории. Упомяни если расходы растут.`,

        cashflow_analysis: `Пользователь просит анализ ликвидности/кассовых разрывов.
Проанализируй будущие поступления и расходы по дням. Найди даты с минимальным остатком.
Если есть кассовый разрыв — дай конкретную дату и сумму нехватки.`,

        deep_analysis: `Это сложный аналитический запрос. 
Проанализируй данные глубоко, используй все доступные метрики.
Дай конкретные цифры и рекомендации.`,

        casual_chat: `Это разговорный вопрос, не связанный напрямую с финансами.
Ответь дружелюбно, но кратко. Предложи помощь с финансовыми вопросами.`
    };

    return map[intent?.intent] || map.deep_analysis;
}

/**
 * Build proactive alerts from data packet
 * Returns warning text or empty string
 */
function buildProactiveAlerts(dataPacket) {
    if (!dataPacket) return '';
    const alerts = [];

    // Check cash gap risk
    const totals = dataPacket.totals || dataPacket.accountsData?.totals;
    if (totals) {
        const currentBalance = totals.open?.current || totals.all?.current || 0;
        const forecastBalance = totals.open?.future || totals.all?.future || 0;

        if (forecastBalance < 0) {
            alerts.push(`⚠️ Прогнозный остаток ОТРИЦАТЕЛЬНЫЙ: ${_fmtTenge(forecastBalance)}. Кассовый разрыв!`);
        } else if (forecastBalance < currentBalance * 0.3 && currentBalance > 0) {
            alerts.push(`⚠️ Прогнозный остаток сильно снижается: ${_fmtTenge(currentBalance)} → ${_fmtTenge(forecastBalance)}`);
        }
    }

    // Check income vs expense ratio
    const summary = dataPacket.operationsSummary;
    if (summary) {
        const income = (summary.income?.fact?.total || 0) + (summary.income?.forecast?.total || 0);
        const expense = (summary.expense?.fact?.total || 0) + (summary.expense?.forecast?.total || 0);

        if (expense > 0 && income > 0 && expense > income * 1.2) {
            alerts.push(`⚠️ Расходы (${_fmtTenge(expense)}) превышают доходы (${_fmtTenge(income)}) на ${Math.round((expense / income - 1) * 100)}%`);
        }
    }

    // Check for large upcoming expenses
    if (Array.isArray(dataPacket.operations)) {
        const now = Date.now();
        const futureExpenses = dataPacket.operations
            .filter(op => op.kind === 'expense' && !op.isFact && op.ts > now)
            .sort((a, b) => b.amount - a.amount);

        if (futureExpenses.length > 0 && futureExpenses[0].amount > 500000) {
            const top = futureExpenses[0];
            alerts.push(`⚠️ Крупный предстоящий расход: ${_fmtTenge(top.amount)} (${top.categoryName || top.description || top.date})`);
        }
    }

    return alerts.join('\n');
}

function _fmtTenge(amount) {
    const abs = Math.abs(Math.round(amount));
    const formatted = abs.toLocaleString('ru-RU');
    return `${amount < 0 ? '-' : ''}${formatted} ₸`;
}

module.exports = {
    buildPersonaPrompt,
    buildProactiveAlerts,
    getIntentInstructions
};
