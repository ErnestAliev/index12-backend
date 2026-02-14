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

    const WELL_KNOWN_TERMS = new Set([
        'аренда', 'доход', 'доходы', 'поступление', 'поступления', 'выручка', 'приход',
        'расход', 'расходы', 'затраты', 'перевод', 'переводы',
        'зарплата', 'фот', 'налоги', 'коммунальные', 'комуналка', 'коммуналка',
        'транспорт', 'питание', 'реклама', 'связь', 'интернет', 'канцтовары',
        'ремонт', 'оборудование', 'страхование', 'юридические', 'бухгалтерские',
        'обучение', 'подписки', 'хозтовары', 'продажи', 'услуги', 'консультации',
        'разработка', 'дизайн', 'маркетинг', 'логистика', 'доставка'
    ]);

    const SYSTEM_TERMS = [
        { term: 'аренда', meaning: 'Платежи/поступления по аренде объектов или помещений' },
        { term: 'доход', meaning: 'Поступление денег в бизнес' },
        { term: 'расход', meaning: 'Списание денег из бизнеса' },
        { term: 'перевод', meaning: 'Перемещение денег между счетами' },
        { term: 'коммуналка', meaning: 'Коммунальные расходы: свет, вода, тепло и т.д.' },
        { term: 'фот', meaning: 'Фонд оплаты труда' },
    ];

    const PROJECT_TERM_PREFIX = '__p:';

    function _normalizeTerm(term) {
        return String(term || '').trim().toLowerCase();
    }

    function _buildStoredTerm(term, projectId = null) {
        const normalized = _normalizeTerm(term);
        if (!normalized) return '';
        const pid = String(projectId || '').trim();
        if (!pid) return normalized;
        return `${PROJECT_TERM_PREFIX}${pid}::${normalized}`;
    }

    function _parseStoredTerm(rawTerm) {
        const term = String(rawTerm || '').trim();
        if (!term) return { term: '', projectId: null };
        const re = new RegExp(`^${PROJECT_TERM_PREFIX.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^:]+)::(.+)$`, 'i');
        const match = term.match(re);
        if (!match) return { term, projectId: null };
        return {
            projectId: String(match[1] || '').trim() || null,
            term: String(match[2] || '').trim()
        };
    }

    function _toPublicRow(row) {
        const parsed = _parseStoredTerm(row?.term);
        return {
            ...row,
            storedTerm: String(row?.term || ''),
            term: parsed.term || String(row?.term || ''),
            projectId: row?.projectId ? String(row.projectId) : parsed.projectId
        };
    }

    function isWellKnownTerm(term) {
        return WELL_KNOWN_TERMS.has(_normalizeTerm(term));
    }

    function _workspaceWhere(workspaceId, { includeGlobal = true, exact = false } = {}) {
        if (exact) {
            return { workspaceId: workspaceId || null };
        }
        if (workspaceId && includeGlobal) {
            return {
                $or: [
                    { workspaceId },
                    { workspaceId: null },
                    { workspaceId: { $exists: false } }
                ]
            };
        }
        return { workspaceId: workspaceId || null };
    }

    function _pickWorkspaceScoped(entries, workspaceId) {
        if (!Array.isArray(entries) || !entries.length) return [];
        if (!workspaceId) return entries;

        const exact = [];
        const global = [];
        for (const entry of entries) {
            const ws = entry?.workspaceId ? String(entry.workspaceId) : null;
            if (ws && String(workspaceId) === ws) exact.push(entry);
            else if (!ws) global.push(entry);
        }
        return exact.length ? exact : global;
    }

    /**
     * Get all glossary terms for a user
     */
    async function getGlossary(userId, { workspaceId = null, includeGlobal = true } = {}) {
        try {
            const where = {
                userId,
                ..._workspaceWhere(workspaceId, { includeGlobal })
            };
            const rows = await AiGlossary.find(where).sort({ term: 1, updatedAt: -1 }).lean();
            const scoped = _pickWorkspaceScoped(rows, workspaceId);
            const byTerm = new Map();
            scoped.forEach((row) => {
                const parsed = _parseStoredTerm(row?.term);
                const key = `${parsed.projectId || '*'}::${String(parsed.term || '').toLowerCase()}`;
                if (!key || byTerm.has(key)) return;
                byTerm.set(key, _toPublicRow(row));
            });
            return Array.from(byTerm.values()).sort((a, b) => String(a.term || '').localeCompare(String(b.term || '')));
        } catch (err) {
            console.error('[glossaryService] getGlossary error:', err.message);
            return [];
        }
    }

    /**
     * Look up a specific term
     */
    async function lookupTerm(userId, term, { workspaceId = null, includeGlobal = true, projectId = null } = {}) {
        try {
            const normalized = _normalizeTerm(term);
            if (!normalized) return null;
            const scopedTerm = _buildStoredTerm(normalized, projectId);
            const termCandidates = Array.from(new Set(
                [scopedTerm, normalized].filter(Boolean)
            ));
            const where = {
                userId,
                term: { $in: termCandidates },
                ..._workspaceWhere(workspaceId, { includeGlobal })
            };
            const rows = await AiGlossary.find(where).sort({ updatedAt: -1 }).lean();
            const scoped = _pickWorkspaceScoped(rows, workspaceId);
            const wantedProjectId = String(projectId || '').trim() || null;
            let fallback = null;
            for (const row of scoped) {
                const parsed = _parseStoredTerm(row?.term);
                if (wantedProjectId && parsed.projectId && String(parsed.projectId) === wantedProjectId) {
                    return _toPublicRow(row);
                }
                if (!parsed.projectId && !fallback) {
                    fallback = _toPublicRow(row);
                }
            }
            return fallback || (scoped[0] ? _toPublicRow(scoped[0]) : null);
        } catch (err) {
            console.error('[glossaryService] lookupTerm error:', err.message);
            return null;
        }
    }

    /**
     * Add or update a term
     */
    async function addTerm(userId, { term, meaning, source = 'user', confidence = 1.0, workspaceId = null, projectId = null }) {
        try {
            const normalized = _normalizeTerm(term);
            const normalizedMeaning = String(meaning || '').trim();
            if (!normalized || !normalizedMeaning) return null;
            const storedTerm = _buildStoredTerm(normalized, projectId);

            const existing = await AiGlossary.findOne({
                userId,
                term: { $regex: new RegExp(`^${_escapeRegex(storedTerm)}$`, 'i') },
                ..._workspaceWhere(workspaceId, { exact: true })
            });

            if (existing) {
                // Don't overwrite explicit user definitions with non-user sources.
                if (existing.source === 'user' && source !== 'user') {
                    return _toPublicRow(existing.toObject ? existing.toObject() : existing);
                }
                existing.meaning = normalizedMeaning;
                existing.source = source;
                existing.confidence = confidence;
                existing.updatedAt = new Date();
                await existing.save();
                return _toPublicRow(existing.toObject());
            }

            const entry = new AiGlossary({
                userId,
                workspaceId: workspaceId || null,
                term: storedTerm,
                meaning: normalizedMeaning,
                source,
                confidence
            });
            await entry.save();
            return _toPublicRow(entry.toObject());
        } catch (err) {
            // Duplicate key — already exists
            if (err.code === 11000) {
                return await lookupTerm(userId, term, { workspaceId, includeGlobal: false, projectId });
            }
            console.error('[glossaryService] addTerm error:', err.message);
            return null;
        }
    }

    /**
     * Remove a term from glossary
     */
    async function removeTerm(userId, term, { workspaceId = null, includeGlobal = false, projectId = null } = {}) {
        try {
            const normalized = _normalizeTerm(term);
            const storedTerm = _buildStoredTerm(normalized, projectId);
            const where = {
                userId,
                term: { $regex: new RegExp(`^${_escapeRegex(storedTerm)}$`, 'i') },
                ..._workspaceWhere(workspaceId, { includeGlobal, exact: !includeGlobal })
            };
            await AiGlossary.deleteOne(where);
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
    function findUnknownTerms(glossaryEntries, categories, projects = []) {
        const categoriesSafe = Array.isArray(categories) ? categories : [];
        const projectsSafe = Array.isArray(projects) ? projects : [];
        if (!categoriesSafe.length && !projectsSafe.length) return [];

        const knownTerms = new Set(
            (glossaryEntries || []).map((g) => _normalizeTerm(g?.term))
        );

        const unknowns = [];
        const seen = new Set();
        const walk = (item, entity = 'category') => {
            const name = String(item?.name || '').trim();
            if (!name) return;
            const lower = _normalizeTerm(name);
            if (!lower) return;

            if (knownTerms.has(lower)) return;
            if (WELL_KNOWN_TERMS.has(lower)) return;

            // Flag abbreviations and ambiguous names (≤4 chars or all-caps)
            const isAbbreviation = name.length <= 4 || name === name.toUpperCase();
            const isAmbiguous = /^[а-яА-Я]{1,4}$/i.test(name) || /^\d/.test(name);

            if (!isAbbreviation && !isAmbiguous) return;
            const dedupKey = `${entity}::${lower}`;
            if (seen.has(dedupKey)) return;
            seen.add(dedupKey);
            unknowns.push({
                name,
                entity,
                type: item?.type || null,
                reason: isAbbreviation ? 'abbreviation' : 'ambiguous'
            });
        };

        categoriesSafe.forEach((cat) => walk(cat, 'category'));
        projectsSafe.forEach((project) => walk(project, 'project'));

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
            const scope = g?.projectId ? ` [project:${String(g.projectId).slice(-6)}]` : '';
            return `• ${g.term}${scope} = ${g.meaning}${conf}`;
        });

        return `Шпаргалка терминов:\n${lines.join('\n')}`;
    }

    async function ensureSystemGlossary(userId, { workspaceId = null } = {}) {
        if (!userId) return [];
        const upserts = [];
        for (const item of SYSTEM_TERMS) {
            upserts.push(
                addTerm(userId, {
                    workspaceId,
                    term: item.term,
                    meaning: item.meaning,
                    source: 'system',
                    confidence: 1.0
                })
            );
        }
        return Promise.all(upserts);
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
        ensureSystemGlossary,
        isWellKnownTerm,
        findUnknownTerms,
        buildGlossaryContext
    };
};
