/**
 * conversationEngine.js — Центральный оркестратор Living CFO
 * 
 * Единая точка входа для обработки AI-запросов.
 * Координирует: Memory → Intent → Data → Prompt → LLM → Memory update
 */
const { classifyIntent } = require('./intentClassifier');
const { buildPersonaPrompt, buildProactiveAlerts } = require('../prompts/personaPrompt');
const { buildOnboardingMessage } = require('../prompts/onboardingPrompt');

/**
 * Create the conversation engine
 * @param {Object} deps — dependencies injected from aiRoutes
 * @param {Object} deps.glossaryService
 * @param {Object} deps.profileService
 * @param {Function} deps.openAiChat — function to call OpenAI
 * @param {Function} deps.buildDataPacket — from dataProvider
 * @param {Object} deps.quickMode — existing quickMode handler
 * @param {Function} deps.formatTenge — currency formatter
 */
function createConversationEngine({
    glossaryService,
    profileService,
    openAiChat,
    buildDataPacket,
    quickMode,
    formatTenge
}) {

    /**
     * Main entry point — process a user message
     */
    async function processMessage({
        userId,
        workspaceId = null,
        message,
        mode = 'freeform',
        chatHistory = [],
        dataPacketOptions = {},
        dbData = null // Pre-built data packet (from existing flow)
    }) {
        const text = String(message || '').trim();
        if (!text) {
            return { text: 'Напиши что-нибудь, и я помогу разобраться.', intent: null };
        }

        // 1. Load profile and glossary
        const [profile, glossaryEntries] = await Promise.all([
            profileService.getProfile(userId, { workspaceId }),
            glossaryService.getGlossary(userId, { workspaceId })
        ]);

        // 2. Check onboarding
        if (!profile.onboardingComplete) {
            return await handleOnboarding({
                userId, workspaceId, message: text, profile, glossaryEntries,
                dataPacketOptions, dbData
            });
        }

        // 3. Classify intent
        const intent = await classifyIntent(text, { openAiChat });

        // 4. Handle glossary interactions directly
        if (intent.intent === 'glossary_question') {
            return handleGlossaryQuestion({ userId, term: intent.term, glossaryEntries, profile });
        }
        if (intent.intent === 'glossary_teach') {
            return await handleGlossaryTeach({ userId, workspaceId, term: intent.term, meaning: intent.meaning });
        }

        // 5. Build data packet if needed
        let dataPacket = dbData;
        if (intent.needsData && !dataPacket) {
            try {
                dataPacket = await buildDataPacket(userId, dataPacketOptions);
            } catch (err) {
                console.error('[conversationEngine] buildDataPacket error:', err.message);
            }
        }

        // 6. Deep mode: always use LLM (quickMode is handled separately in aiRoutes for quick/chat)

        // 7. Build persona prompt with full context
        const glossaryContext = glossaryService.buildGlossaryContext(glossaryEntries);
        const profileContext = profileService.buildProfileContext(profile);
        const systemPrompt = buildPersonaPrompt({
            glossaryContext,
            profileContext,
            intent,
            dataPacket
        });

        // 8. Build messages for LLM
        const messages = [
            { role: 'system', content: systemPrompt }
        ];

        // Add data context
        if (dataPacket) {
            const dataContext = buildDataContext(dataPacket);
            messages.push({ role: 'system', content: `ДАННЫЕ:\n${dataContext}` });
        }

        // Add chat history (last 6 messages max for context window management)
        const recentHistory = chatHistory.slice(-6);
        for (const msg of recentHistory) {
            messages.push({
                role: msg.role === 'assistant' ? 'assistant' : 'user',
                content: msg.content || msg.text || ''
            });
        }

        // Add current message
        messages.push({ role: 'user', content: text });

        // 9. Call LLM
        let response;
        try {
            const modelOverride = (intent.intent === 'casual_chat') ? 'gpt-4o-mini' : null;
            response = await openAiChat(messages, {
                temperature: 0.3,
                maxTokens: 1500,
                modelOverride,
                timeout: 60000
            });
        } catch (err) {
            console.error('[conversationEngine] LLM error:', err.message);
            // Deterministic fallback
            if (dataPacket) {
                response = buildFallbackResponse(dataPacket, intent);
            } else {
                response = 'Произошла ошибка при обработке запроса. Попробуй ещё раз.';
            }
        }

        const responseText = String(response || '').trim();

        // 10. Post-processing: record interaction and extract learnings
        profileService.recordInteraction(userId, { workspaceId }).catch(() => { });

        return {
            text: responseText,
            intent,
            source: 'llm'
        };
    }

    /**
     * Handle onboarding flow for new users
     */
    async function handleOnboarding({ userId, workspaceId = null, message, profile, glossaryEntries, dataPacketOptions, dbData }) {
        let dataPacket = dbData;
        if (!dataPacket) {
            try {
                dataPacket = await buildDataPacket(userId, dataPacketOptions);
            } catch (err) {
                console.error('[conversationEngine] onboarding data error:', err.message);
            }
        }

        // Check for unknown terms
        const categories = dataPacket?.catalogs?.categories || [];
        const unknownTerms = glossaryService.findUnknownTerms(glossaryEntries, categories);

        // If user is sending a message during onboarding, check if they're teaching a term
        const teachMatch = String(message).match(/^(.{1,30})\s*[-—=:]\s*(.+)/i);
        if (teachMatch && teachMatch[1].trim().length <= 20) {
            const term = teachMatch[1].trim();
            const meaning = teachMatch[2].trim();
            await glossaryService.addTerm(userId, { term, meaning, source: 'user', workspaceId });

            // Check if more unknown terms remain
            const updatedGlossary = await glossaryService.getGlossary(userId, { workspaceId });
            const remaining = glossaryService.findUnknownTerms(updatedGlossary, categories);

            if (remaining.length === 0) {
                await profileService.completeOnboarding(userId, { workspaceId });
                return {
                    text: `Отлично, запомнил: ${term} = ${meaning}. Теперь мне всё понятно! Спрашивай что угодно.`,
                    intent: { intent: 'onboarding' },
                    source: 'onboarding'
                };
            }

            const nextTerm = remaining[0].name;
            return {
                text: `Запомнил: ${term} = ${meaning}. А что значит "${nextTerm}"?`,
                intent: { intent: 'onboarding' },
                source: 'onboarding'
            };
        }

        // First interaction — check if any data-question, if so, complete onboarding and answer
        const quickIntent = require('./intentClassifier').tryQuickRegex(message);
        if (quickIntent && quickIntent.needsData) {
            // User is asking about data → auto-complete onboarding and answer
            await profileService.completeOnboarding(userId, { workspaceId });
            return processMessage({
                userId, workspaceId, message, chatHistory: [],
                dataPacketOptions, dbData: dataPacket
            });
        }

        // Generate onboarding greeting
        if (!profile.interactionCount || profile.interactionCount === 0) {
            const greetingText = buildOnboardingMessage({
                dataPacket,
                unknownTerms,
                profile
            });

            // Mark that we've interacted but DON'T complete onboarding yet  
            // (wait for term explanations if there are unknowns)
            if (unknownTerms.length === 0) {
                await profileService.completeOnboarding(userId, { workspaceId });
            } else {
                await profileService.recordInteraction(userId, { workspaceId });
            }

            return {
                text: greetingText,
                intent: { intent: 'onboarding' },
                source: 'onboarding'
            };
        }

        // Continuing onboarding conversation — use LLM
        await profileService.completeOnboarding(userId, { workspaceId });
        return processMessage({
            userId, workspaceId, message, chatHistory: [],
            dataPacketOptions, dbData: dataPacket
        });
    }

    /**
     * Handle "что такое X?" questions
     */
    function handleGlossaryQuestion({ userId, term, glossaryEntries, profile }) {
        const entry = glossaryEntries.find(
            g => String(g.term).toLowerCase() === String(term).toLowerCase()
        );

        if (entry) {
            return {
                text: `${entry.term} — ${entry.meaning}`,
                intent: { intent: 'glossary_question' },
                source: 'glossary'
            };
        }

        return {
            text: `Я пока не знаю что значит "${term}". Расскажи — я запомню! Напиши в формате: ${term} — [определение]`,
            intent: { intent: 'glossary_question' },
            source: 'glossary'
        };
    }

    /**
     * Handle user teaching a term
     */
    async function handleGlossaryTeach({ userId, workspaceId = null, term, meaning }) {
        const saved = await glossaryService.addTerm(userId, { term, meaning, source: 'user', workspaceId });
        if (saved) {
            return {
                text: `Запомнил: ${term} = ${meaning}`,
                intent: { intent: 'glossary_teach' },
                source: 'glossary'
            };
        }
        return {
            text: 'Не удалось сохранить термин. Попробуй ещё раз.',
            intent: { intent: 'glossary_teach' },
            source: 'glossary'
        };
    }

    /**
     * Build compact data context string for LLM
     */
    function buildDataContext(dataPacket) {
        if (!dataPacket) return 'Нет данных.';
        const parts = [];

        // Period
        if (dataPacket.meta) {
            parts.push(`Период: ${dataPacket.meta.periodStart} — ${dataPacket.meta.periodEnd}, сегодня: ${dataPacket.meta.today}`);
        }

        // Account totals
        const totals = dataPacket.totals || dataPacket.accountsData?.totals;
        if (totals) {
            parts.push(`\nОстатки:`);
            if (totals.open) {
                parts.push(`  Открытые счета: сейчас ${_formatTenge(totals.open.current)}, прогноз ${_formatTenge(totals.open.future)}`);
            }
            if (totals.hidden && (totals.hidden.current !== 0 || totals.hidden.future !== 0)) {
                parts.push(`  Скрытые счета: сейчас ${_formatTenge(totals.hidden.current)}, прогноз ${_formatTenge(totals.hidden.future)}`);
            }
            if (totals.all) {
                parts.push(`  Всего: сейчас ${_formatTenge(totals.all.current)}, прогноз ${_formatTenge(totals.all.future)}`);
            }
        }

        // Accounts list
        if (Array.isArray(dataPacket.accounts) && dataPacket.accounts.length > 0) {
            parts.push(`\nСчета (${dataPacket.accounts.length}):`);
            for (const acc of dataPacket.accounts.slice(0, 15)) {
                const hidden = acc.isHidden ? ' [скрыт]' : '';
                parts.push(`  ${acc.name || acc._id}: ${_formatTenge(acc.currentBalance || 0)} → ${_formatTenge(acc.futureBalance || 0)}${hidden}`);
            }
        }

        // Operations summary
        const ops = dataPacket.operationsSummary;
        if (ops) {
            parts.push(`\nОперации (${ops.total || 0}):`);
            if (ops.income) {
                parts.push(`  Доходы: факт ${_formatTenge(ops.income.fact?.total || 0)} (${ops.income.fact?.count || 0}), прогноз ${_formatTenge(ops.income.forecast?.total || 0)} (${ops.income.forecast?.count || 0})`);
            }
            if (ops.expense) {
                parts.push(`  Расходы: факт ${_formatTenge(ops.expense.fact?.total || 0)} (${ops.expense.fact?.count || 0}), прогноз ${_formatTenge(ops.expense.forecast?.total || 0)} (${ops.expense.forecast?.count || 0})`);
            }
            if (ops.transfer) {
                parts.push(`  Переводы: ${ops.transfer.count || 0}, сумма ${_formatTenge(ops.transfer.total || 0)}`);
            }
        }

        // Category summary (top 8)
        if (Array.isArray(dataPacket.categorySummary) && dataPacket.categorySummary.length > 0) {
            parts.push(`\nТоп категории:`);
            for (const cat of dataPacket.categorySummary.slice(0, 8)) {
                const inc = cat.incomeFact + cat.incomeForecast;
                const exp = cat.expenseFact + cat.expenseForecast;
                const tag = cat.tags?.length ? ` [${cat.tags.join(',')}]` : '';
                if (inc > 0) parts.push(`  ${cat.name}: доход ${_formatTenge(inc)}${tag}`);
                if (exp > 0) parts.push(`  ${cat.name}: расход ${_formatTenge(exp)}${tag}`);
            }
        }

        // Contractor summary (top 5)
        if (Array.isArray(dataPacket.contractorSummary) && dataPacket.contractorSummary.length > 0) {
            parts.push(`\nТоп контрагенты:`);
            for (const c of dataPacket.contractorSummary.slice(0, 5)) {
                const inc = c.incomeFact + (c.incomeForecast || 0);
                const exp = c.expenseFact + (c.expenseForecast || 0);
                parts.push(`  ${c.name}: доход ${_formatTenge(inc)}, расход ${_formatTenge(exp)}`);
            }
        }

        // Companies
        if (Array.isArray(dataPacket.catalogs?.companies) && dataPacket.catalogs.companies.length > 0) {
            parts.push(`\nКомпании: ${dataPacket.catalogs.companies.map(c => c.name).join(', ')}`);
        }

        // Projects
        if (Array.isArray(dataPacket.catalogs?.projects) && dataPacket.catalogs.projects.length > 0) {
            parts.push(`Проекты: ${dataPacket.catalogs.projects.map(p => p.name).join(', ')}`);
        }

        // Outliers
        if (dataPacket.outliers) {
            if (Array.isArray(dataPacket.outliers.income) && dataPacket.outliers.income.length > 0) {
                parts.push(`\nКрупнейшие доходы:`);
                for (const op of dataPacket.outliers.income.slice(0, 3)) {
                    parts.push(`  ${op.date} ${_formatTenge(op.amount)} ${op.categoryName || op.description || ''}`);
                }
            }
            if (Array.isArray(dataPacket.outliers.expense) && dataPacket.outliers.expense.length > 0) {
                parts.push(`Крупнейшие расходы:`);
                for (const op of dataPacket.outliers.expense.slice(0, 3)) {
                    parts.push(`  ${op.date} ${_formatTenge(op.amount)} ${op.categoryName || op.description || ''}`);
                }
            }
        }

        // Individual operations (for detailed queries like "покажи список операций")
        if (Array.isArray(dataPacket.operations) && dataPacket.operations.length > 0) {
            const ops_list = dataPacket.operations;
            parts.push(`\nОперации (${ops_list.length} шт.):`);
            for (const op of ops_list.slice(0, 50)) {
                const kind = op.kind || op.type || '?';
                const amount = Math.abs(op.amount || 0);
                const d = op.date ? new Date(op.date) : null;
                const dateStr = d ? `${String(d.getUTCDate()).padStart(2, '0')}.${String(d.getUTCMonth() + 1).padStart(2, '0')}.${String(d.getUTCFullYear()).slice(-2)}` : '??';
                const cat = op.categoryName || op.category || '';
                const contractor = op.contractorName || op.contractor || '';
                const desc = op.description || op.comment || '';
                const isFact = op.isFact ? 'факт' : 'план';
                const account = op.accountName || '';
                const parts2 = [dateStr, kind, _formatTenge(amount), isFact];
                if (cat) parts2.push(`кат:${cat}`);
                if (contractor) parts2.push(`контр:${contractor}`);
                if (account) parts2.push(`счёт:${account}`);
                if (desc) parts2.push(desc.slice(0, 40));
                parts.push(`  ${parts2.join(' | ')}`);
            }
            if (ops_list.length > 50) {
                parts.push(`  ... ещё ${ops_list.length - 50} операций`);
            }
        }

        // Timeline (daily cashflow) — critical for cashflow analysis
        const timeline = dataPacket.meta?.timeline;
        if (Array.isArray(timeline) && timeline.length > 0) {
            // Find min balance and key metrics
            let minBal = Infinity, minDate = '';
            let totalInc = 0, totalExp = 0;
            const significantDays = [];

            for (const day of timeline) {
                const bal = day.closingBalance || 0;
                const d = day.date ? new Date(day.date) : null;
                const dateStr = d ? `${String(d.getUTCDate()).padStart(2, '0')}.${String(d.getUTCMonth() + 1).padStart(2, '0')}` : '??';

                if (bal < minBal) {
                    minBal = bal;
                    minDate = dateStr;
                }
                totalInc += (day.income || 0);
                totalExp += (day.expense || 0);

                // Track days with significant movement
                if ((day.income || 0) > 0 || (day.expense || 0) > 0) {
                    significantDays.push({
                        date: dateStr,
                        income: day.income || 0,
                        expense: day.expense || 0,
                        balance: bal
                    });
                }
            }

            const firstBal = timeline[0]?.closingBalance || 0;
            const lastBal = timeline[timeline.length - 1]?.closingBalance || 0;

            parts.push(`\nДенежный поток (${timeline.length} дней):`);
            parts.push(`  Начало: ${_formatTenge(firstBal)}, конец: ${_formatTenge(lastBal)}`);
            parts.push(`  Всего поступлений: ${_formatTenge(totalInc)}, расходов: ${_formatTenge(totalExp)}`);
            parts.push(`  Минимальный остаток: ${_formatTenge(minBal)} (${minDate})`);
            if (minBal < 0) {
                parts.push(`  ⚠️ КАССОВЫЙ РАЗРЫВ: ${minDate}, нехватка ${_formatTenge(Math.abs(minBal))}`);
            }

            // Show top 10 significant days
            if (significantDays.length > 0) {
                parts.push(`  Ключевые дни:`);
                for (const day of significantDays.slice(0, 10)) {
                    const inc = day.income > 0 ? `+${_formatTenge(day.income)}` : '';
                    const exp = day.expense > 0 ? `-${_formatTenge(day.expense)}` : '';
                    const moves = [inc, exp].filter(Boolean).join(' ');
                    parts.push(`    ${day.date}: ${moves} → ${_formatTenge(day.balance)}`);
                }
                if (significantDays.length > 10) {
                    parts.push(`    ... ещё ${significantDays.length - 10} дней с движением`);
                }
            }
        }

        return parts.join('\n');

    }

    /**
     * Build deterministic fallback when LLM fails
     */
    function buildFallbackResponse(dataPacket, intent) {
        const totals = dataPacket.totals || dataPacket.accountsData?.totals;
        const ops = dataPacket.operationsSummary;

        const parts = [];

        if (totals?.all) {
            parts.push(`Остаток: ${_formatTenge(totals.all.current)}, прогноз: ${_formatTenge(totals.all.future)}`);
        }
        if (ops?.income) {
            parts.push(`Доходы: ${_formatTenge(ops.income.total || 0)}`);
        }
        if (ops?.expense) {
            parts.push(`Расходы: ${_formatTenge(ops.expense.total || 0)}`);
        }

        return parts.length > 0
            ? parts.join('\n')
            : 'Данные загружаются, попробуй через несколько секунд.';
    }

    // Helper
    function _formatTenge(amount) {
        const abs = Math.abs(Math.round(amount || 0));
        const formatted = abs.toLocaleString('ru-RU');
        return `${amount < 0 ? '-' : ''}${formatted} ₸`;
    }

    return {
        processMessage
    };
}

module.exports = { createConversationEngine };
