/**
 * glossaryService.js — Шпаргалка терминов пользователя
 * 
 * Хранит и предоставляет пользовательские определения терминов:
 * "ФОТ" → "Фонд оплаты труда", "К1" → "Касса Астана" и т.д.
 * 
 * Агент проверяет шпаргалку перед ответом и может:
 * 1. Использовать известные термины для лучшего понимания
 * 2. Обнаруживать неизвестные термины в категориях
 * 3. Предлагать определения (source: 'ai_inferred')
 */
module.exports = function createGlossaryService({ AiGlossary }) {
    if (!AiGlossary) throw new Error('AiGlossary model is required');

    /**
     * Get all glossary terms for a user
     */
    async function getGlossary(userId) {
        try {
            return await AiGlossary.find({ userId }).sort({ term: 1 }).lean();
        } catch (err) {
            console.error('[glossaryService] getGlossary error:', err.message);
            return [];
        }
    }

    /**
     * Look up a specific term
     */
    async function lookupTerm(userId, term) {
        try {
            const normalized = String(term).trim().toLowerCase();
            return await AiGlossary.findOne({
                userId,
                term: { $regex: new RegExp(`^${_escapeRegex(normalized)}$`, 'i') }
            }).lean();
        } catch (err) {
            console.error('[glossaryService] lookupTerm error:', err.message);
            return null;
        }
    }

    /**
     * Add or update a term
     */
    async function addTerm(userId, { term, meaning, source = 'user', confidence = 1.0, workspaceId = null }) {
        try {
            const normalized = String(term).trim();
            if (!normalized || !meaning) return null;

            const existing = await AiGlossary.findOne({
                userId,
                term: { $regex: new RegExp(`^${_escapeRegex(normalized)}$`, 'i') }
            });

            if (existing) {
                // Don't overwrite user-defined with ai-inferred
                if (existing.source === 'user' && source === 'ai_inferred') {
                    return existing;
                }
                existing.meaning = meaning;
                existing.source = source;
                existing.confidence = confidence;
                existing.updatedAt = new Date();
                await existing.save();
                return existing.toObject();
            }

            const entry = new AiGlossary({
                userId,
                workspaceId,
                term: normalized,
                meaning: String(meaning).trim(),
                source,
                confidence
            });
            await entry.save();
            return entry.toObject();
        } catch (err) {
            // Duplicate key — already exists
            if (err.code === 11000) {
                return await lookupTerm(userId, term);
            }
            console.error('[glossaryService] addTerm error:', err.message);
            return null;
        }
    }

    /**
     * Remove a term from glossary
     */
    async function removeTerm(userId, term) {
        try {
            const normalized = String(term).trim();
            await AiGlossary.deleteOne({
                userId,
                term: { $regex: new RegExp(`^${_escapeRegex(normalized)}$`, 'i') }
            });
            return true;
        } catch (err) {
            console.error('[glossaryService] removeTerm error:', err.message);
            return false;
        }
    }

    /**
     * Find category names that have no glossary definition
     * Returns array of { name, type } objects
     */
    function findUnknownTerms(glossaryEntries, categories) {
        if (!Array.isArray(categories) || categories.length === 0) return [];

        const knownTerms = new Set(
            (glossaryEntries || []).map(g => String(g.term).toLowerCase())
        );

        // Common terms that don't need explanation
        const WELL_KNOWN = new Set([
            'аренда', 'зарплата', 'налоги', 'коммунальные', 'транспорт',
            'питание', 'реклама', 'связь', 'интернет', 'канцтовары',
            'ремонт', 'оборудование', 'страхование', 'юридические',
            'бухгалтерские', 'обучение', 'подписки', 'хозтовары',
            'продажи', 'услуги', 'консультации', 'разработка',
            'дизайн', 'маркетинг', 'логистика', 'доставка',
        ]);

        const unknowns = [];
        for (const cat of categories) {
            const name = String(cat.name || '').trim();
            if (!name) continue;
            const lower = name.toLowerCase();

            // Skip if already in glossary or well-known
            if (knownTerms.has(lower)) continue;
            if (WELL_KNOWN.has(lower)) continue;

            // Flag abbreviations and ambiguous names (≤4 chars or all-caps)
            const isAbbreviation = name.length <= 4 || name === name.toUpperCase();
            const isAmbiguous = /^[а-яА-Я]{1,4}$/i.test(name) || /^\d/.test(name);

            if (isAbbreviation || isAmbiguous) {
                unknowns.push({
                    name,
                    type: cat.type || null,
                    reason: isAbbreviation ? 'abbreviation' : 'ambiguous'
                });
            }
        }

        return unknowns;
    }

    /**
     * Build glossary context string for LLM prompt injection
     * Returns empty string if no terms
     */
    function buildGlossaryContext(glossaryEntries) {
        if (!Array.isArray(glossaryEntries) || glossaryEntries.length === 0) {
            return '';
        }

        const lines = glossaryEntries.map(g => {
            const conf = g.confidence < 1.0 ? ` (предположительно)` : '';
            return `• ${g.term} = ${g.meaning}${conf}`;
        });

        return `Шпаргалка терминов:\n${lines.join('\n')}`;
    }

    // Helpers
    function _escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    return {
        getGlossary,
        lookupTerm,
        addTerm,
        removeTerm,
        findUnknownTerms,
        buildGlossaryContext
    };
};
