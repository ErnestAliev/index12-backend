/**
 * intentClassifier.js — Классификация намерения пользователя
 * 
 * Двухуровневая система:
 * 1. Быстрые regex-паттерны (детерминированные, без LLM)
 * 2. LLM-классификация через gpt-4o-mini для неформальных/сложных вопросов
 * 
 * Intents:
 * - accounts_balance — запрос по счетам/балансу
 * - income_report — по доходам
 * - expense_report — по расходам
 * - transfers_report — по переводам
 * - companies_report — по компаниям
 * - projects_report — по проектам
 * - general_status — "как дела?", "что нового?"
 * - cashflow_analysis — прогноз, кассовые разрывы
 * - glossary_question — "что такое X?"
 * - glossary_teach — "ФОТ — это фонд оплаты труда"
 * - deep_analysis — сложный аналитический запрос
 * - casual_chat — разговорный
 */

// Regex patterns matching the existing quickMode patterns
// NOTE: \b does NOT work with Cyrillic in JS regex — don't use it for Russian patterns
const QUICK_PATTERNS = [
    {
        intent: 'accounts_balance',
        pattern: /(сч[её]т|счета|касс[аы]?|баланс|остат[оке]|деньг|средств)/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'income_report',
        pattern: /(доход|поступлен|приход|выручк|заработ|получи[лт])/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'expense_report',
        pattern: /(расход|тра[тч]|платеж|оплат|выплат|затрат)/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'transfers_report',
        pattern: /(перевод(?:ы|ов)?|transfer)/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'companies_report',
        pattern: /(компан)/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'projects_report',
        pattern: /(проект)/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'contractors_report',
        pattern: /(контрагент|поставщик|клиент(?:ы|ов)?)/i,
        needsData: true,
        deterministic: true
    },
    {
        intent: 'catalogs_report',
        pattern: /(справочник|каталог|категор)/i,
        needsData: true,
        deterministic: true
    }
];

// Patterns that indicate general status queries
const STATUS_PATTERNS = [
    /(как\s+(у\s+нас\s+)?дел[аы])/i,
    /(как\s+(мы|обстоят))/i,
    /(что\s+нового)/i,
    /(обзор|сводк|итог|резюме|общ[аи][яй]?\s+картин)/i,
    /(оцен[иь]\s+месяц|оценк[аеу]\s+месяц)/i,
    /(обща[яй]\s+ситуаци)/i,
    /(выживем|протянем|хватит)/i,
    /(всё\s+ли\s+(ок|хорошо|норм))/i
];

// Patterns for glossary interactions
const GLOSSARY_QUESTION = /(что\s+такое|что\s+значит|что\s+означает|расшифруй|поясни)\s+(.+)/i;
const GLOSSARY_TEACH = /^(.{1,30})\s*[-—=:]\s*(.+)/i;

// Patterns for cashflow/stress testing
const CASHFLOW_PATTERNS = [
    /(кассов|ликвидност|cash\s*flow|денежн\w*\s*поток)/i,
    /(стресс|разрыв|нехватк|дефицит|gap)/i,
    /(прогноз|forecast|хватит\s+ли|протянем)/i,
    /(сдвин|перенес|если\s+перенести|что\s+если|что\s+будет)/i
];


/**
 * Try quick regex classification (deterministic, no LLM)
 */
function tryQuickRegex(message) {
    const text = String(message || '').trim().toLowerCase();
    if (!text) return null;

    // 1. Check glossary question
    const glossaryQ = text.match(GLOSSARY_QUESTION);
    if (glossaryQ) {
        return {
            intent: 'glossary_question',
            term: glossaryQ[2].trim(),
            needsData: false,
            deterministic: false
        };
    }

    // 2. Check glossary teach (short format: "ФОТ — фонд оплаты труда")
    if (text.length < 80) {
        const glossaryT = text.match(GLOSSARY_TEACH);
        if (glossaryT && glossaryT[1].trim().length <= 20) {
            return {
                intent: 'glossary_teach',
                term: glossaryT[1].trim(),
                meaning: glossaryT[2].trim(),
                needsData: false,
                deterministic: false
            };
        }
    }

    // 3. Check cashflow patterns (before general status)
    for (const pattern of CASHFLOW_PATTERNS) {
        if (pattern.test(text)) {
            return {
                intent: 'cashflow_analysis',
                needsData: true,
                deterministic: false
            };
        }
    }

    // 4. Check general status patterns
    for (const pattern of STATUS_PATTERNS) {
        if (pattern.test(text)) {
            return {
                intent: 'general_status',
                needsData: true,
                deterministic: false
            };
        }
    }

    // 5. Check quick deterministic patterns
    for (const { intent, pattern, needsData, deterministic } of QUICK_PATTERNS) {
        if (pattern.test(text)) {
            return { intent, needsData, deterministic };
        }
    }

    return null;
}

/**
 * LLM-based intent classification for complex/ambiguous queries
 * Uses gpt-4o-mini for speed and cost (~$0.0001/call)
 */
async function llmClassify(message, { openAiChat } = {}) {
    if (!openAiChat) {
        // Fallback: treat as deep analysis
        return {
            intent: 'deep_analysis',
            needsData: true,
            deterministic: false
        };
    }

    try {
        const classifyPrompt = `Ты классификатор намерений для финансового ассистента. 
Определи намерение пользователя из сообщения.

Ответь ОДНИМ СЛОВОМ — intent:
- general_status (общий вопрос о состоянии дел)
- accounts_balance (вопрос о счетах/балансе)
- income_report (вопрос о доходах)
- expense_report (вопрос о расходах)
- cashflow_analysis (прогноз, кассовые разрывы, стресс-тест)
- deep_analysis (сложный аналитический вопрос)
- casual_chat (обычный разговор, не связанный с финансами)

Сообщение: "${message}"
Intent:`;

        const response = await openAiChat(
            [{ role: 'user', content: classifyPrompt }],
            {
                temperature: 0,
                maxTokens: 20,
                modelOverride: 'gpt-4o-mini',
                timeout: 5000
            }
        );

        const raw = String(response || '').trim().toLowerCase().replace(/[^a-z_]/g, '');
        const VALID_INTENTS = [
            'general_status', 'accounts_balance', 'income_report',
            'expense_report', 'cashflow_analysis', 'deep_analysis', 'casual_chat'
        ];

        const intent = VALID_INTENTS.includes(raw) ? raw : 'deep_analysis';
        return {
            intent,
            needsData: intent !== 'casual_chat',
            deterministic: false
        };
    } catch (err) {
        console.error('[intentClassifier] LLM classify error:', err.message);
        return {
            intent: 'deep_analysis',
            needsData: true,
            deterministic: false
        };
    }
}

/**
 * Main classify function — regex first, then LLM
 */
async function classifyIntent(message, { openAiChat } = {}) {
    const quick = tryQuickRegex(message);
    if (quick) return quick;

    return llmClassify(message, { openAiChat });
}

module.exports = {
    classifyIntent,
    tryQuickRegex,
    llmClassify
};
