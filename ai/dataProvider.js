// ai/dataProvider.js
// Direct database access layer for AI assistant
// Replaces fragile uiSnapshot parsing with reliable MongoDB queries

/**
 * Creates a data provider for AI assistant queries.
 * @param {Object} deps - Dependencies containing Mongoose models
 * @returns {Object} Data provider with query methods
 */
module.exports = function createDataProvider(deps) {
    const { mongoose, Account, Company, Project, Category, Contractor, Individual, Event } = deps;

    // ========================
    // HELPER FUNCTIONS
    // ========================

    // Kazakhstan timezone helpers (UTC+5)
    const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

    const _kzNow = () => {
        const utc = new Date();
        return new Date(utc.getTime() + KZ_OFFSET_MS);
    };


    const _fmtDateDDMMYY = (d) => {
        if (!d || !(d instanceof Date) || isNaN(d.getTime())) return null;
        const dd = String(d.getDate()).padStart(2, '0');
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const yy = String(d.getFullYear()).slice(2);
        return `${dd}.${mm}.${yy}`;
    };

    // Local time helpers (use client-provided "now" when есть, иначе fallback KZ)
    const _localStartOfDay = (d) => {
        const x = new Date(d.getTime());
        x.setHours(0, 0, 0, 0);
        return x;
    };

    const _localEndOfDay = (d) => {
        const x = new Date(d.getTime());
        x.setHours(23, 59, 59, 999);
        return x;
    };

    const _resolveNow = (nowOverride) => {
        if (nowOverride) {
            const d = new Date(nowOverride);
            if (!isNaN(d.getTime())) return d;
        }
        return _kzNow();
    };

    // Helper for userId queries (matches both ObjectId and String, supports arrays)
    const _uQuery = (userId) => {
        const ids = Array.isArray(userId) ? userId : [userId];
        const variants = [];
        for (const id of ids) {
            if (!id) continue;
            const str = String(id);
            variants.push(str);
            try {
                if (mongoose.Types.ObjectId.isValid(id)) {
                    variants.push(new mongoose.Types.ObjectId(str));
                }
            } catch (e) { }
        }
        return { $in: Array.from(new Set(variants)) };
    };

    // For models that definitely use ObjectId (Account, Project, etc.)
    const _uObjId = (userId) => {
        try {
            if (mongoose.Types.ObjectId.isValid(userId)) {
                return new mongoose.Types.ObjectId(String(userId));
            }
        } catch (e) { }
        return userId;
    };

    // ========================
    // ACCOUNT QUERIES
    // ========================

    /**
     * Get all accounts with calculated balances
     * @param {string} userId - User ID (composite or regular)
     * @param {Object} options - Query options
     * @param {boolean} options.includeHidden - Include hidden/excluded accounts
     * @param {Array<string>} options.visibleAccountIds - Only include these account IDs
     * @returns {Promise<Object>} Accounts data with balances
     */
    async function getAccounts(userId, options = {}) {
        const { includeHidden = false, visibleAccountIds = null, workspaceId = null, now = null } = options;
        const nowRef = _resolveNow(now);
        if (process.env.AI_DEBUG === '1') {
            console.log('[AI_DEBUG] buildDataPacket userId=', userId, 'workspaceId=', workspaceId, 'period=', periodFilter);
        }

        // Build query
        const query = { userId: _uQuery(userId) };
        if (workspaceId) {
            const wsStr = String(workspaceId);
            const wsId = _uObjId(workspaceId);
            const wsVariants = [wsStr, wsId];
            query.$or = [
                { workspaceId: { $in: wsVariants } },
                { workspaceId: { $exists: false } },
                { workspaceId: null }
            ];
        }

        if (!includeHidden && visibleAccountIds && Array.isArray(visibleAccountIds) && visibleAccountIds.length > 0) {
            query._id = {
                $in: visibleAccountIds.map(id => {
                    try { return new mongoose.Types.ObjectId(id); } catch { return id; }
                })
            };
        }

        // Fetch accounts without populate (we'll do manual lookups if needed)
        let accounts = await Account.find(query).lean();
        // Fallbacks: legacy data without workspaceId or mismatched type
        if (workspaceId) {
            const legacyAccs = await Account.find({ userId: _uQuery(userId), $or: [{ workspaceId: { $exists: false } }, { workspaceId: null }] }).lean();
            const allAccsNoFilter = await Account.find({ userId: _uQuery(userId) }).lean();
            // Merge all unique accounts by _id if fallback has more
            const accMap = new Map();
            [...accounts, ...legacyAccs, ...allAccsNoFilter].forEach(a => { if (a && a._id) accMap.set(String(a._id), a); });
            accounts = Array.from(accMap.values());
        }

        if (process.env.AI_DEBUG === '1') {
            const hiddenList = accounts.filter(a => {
                const isExcluded = !!(a.isExcluded || a.excluded || a.excludeFromTotal || a.excludedFromTotal);
                const isHiddenFlag = !!(a.hidden || a.isHidden);
                return isExcluded || isHiddenFlag;
            }).map(a => `${a.name} (${a._id})`);
            console.log('[AI_DEBUG] getAccounts query=', JSON.stringify(query), 'count=', accounts.length, 'hiddenFound=', hiddenList.length);
            if (hiddenList.length) console.log('[AI_DEBUG] hidden accounts:', hiddenList.join(', '));
        }

        // Calculate balances for each account
        const accountsWithBalances = await Promise.all(accounts.map(async (acc) => {
            const isExcluded = !!(acc.isExcluded || acc.excluded || acc.excludeFromTotal || acc.excludedFromTotal);
            const isHiddenFlag = !!(acc.hidden || acc.isHidden);
            const isHidden = isHiddenFlag || isExcluded; // Исключённые считаем скрытыми, как в UI

            // Skip hidden accounts if not requested
            if (!includeHidden && isHidden) {
                return null;
            }

            // Get all operations for this account
            // ⚠️ Event.userId is often a String in this DB, while Account.userId is ObjectId
            const opsQuery = {
                userId: _uQuery(userId),
                $or: [
                    { accountId: acc._id },
                    { fromAccountId: acc._id },
                    { toAccountId: acc._id }
                ]
            };

            const allOps = await Event.find(opsQuery).lean();

            // Calculate current balance (up to nowRef)
            const currentOps = allOps.filter(op => new Date(op.date) <= nowRef);
            let currentBalance = acc.initialBalance || 0;
            for (const op of currentOps) {
                if (String(op.accountId) === String(acc._id)) {
                    // Regular operation on this account
                    currentBalance += (op.amount || 0);
                } else if (String(op.toAccountId) === String(acc._id)) {
                    // Transfer TO this account (income)
                    currentBalance += Math.abs(op.amount || 0);
                } else if (String(op.fromAccountId) === String(acc._id)) {
                    // Transfer FROM this account (expense)
                    currentBalance -= Math.abs(op.amount || 0);
                }
            }

            // Calculate future balance (all operations)
            let futureBalance = acc.initialBalance || 0;
            for (const op of allOps) {
                if (String(op.accountId) === String(acc._id)) {
                    futureBalance += (op.amount || 0);
                } else if (String(op.toAccountId) === String(acc._id)) {
                    futureBalance += Math.abs(op.amount || 0);
                } else if (String(op.fromAccountId) === String(acc._id)) {
                    futureBalance -= Math.abs(op.amount || 0);
                }
            }

            return {
                _id: String(acc._id),
                name: acc.name || 'Без названия',
                currentBalance: Math.round(currentBalance),
                futureBalance: Math.round(futureBalance),
                isHidden,
                isExcluded,
            };
        }));

        // Filter nulls and separate by visibility
        const validAccounts = accountsWithBalances.filter(Boolean);
        const openAccounts = validAccounts.filter(a => !a.isHidden);
        const hiddenAccounts = validAccounts.filter(a => a.isHidden);

        // Calculate totals
        const openCurrentTotal = openAccounts.reduce((s, a) => s + a.currentBalance, 0);
        const openFutureTotal = openAccounts.reduce((s, a) => s + a.futureBalance, 0);
        const hiddenCurrentTotal = hiddenAccounts.reduce((s, a) => s + a.currentBalance, 0);
        const hiddenFutureTotal = hiddenAccounts.reduce((s, a) => s + a.futureBalance, 0);

        return {
            accounts: validAccounts,
            openAccounts,
            hiddenAccounts,
            totals: {
                open: { current: openCurrentTotal, future: openFutureTotal },
                hidden: { current: hiddenCurrentTotal, future: hiddenFutureTotal },
                all: {
                    current: openCurrentTotal + hiddenCurrentTotal,
                    future: openFutureTotal + hiddenFutureTotal
                }
            },
            meta: {
                today: _fmtDateDDMMYY(nowRef),
                count: validAccounts.length,
                openCount: openAccounts.length,
                hiddenCount: hiddenAccounts.length,
            }
        };
    }

    // ========================
    // OPERATIONS QUERIES
    // ========================

    /**
     * Get operations within a date range
     * @param {string} userId - User ID
     * @param {Object} dateRange - Date range { start: Date, end: Date }
     * @param {Object} options - Query options
     * @returns {Promise<Array>} Normalized operations
     */
    async function getOperations(userId, dateRange = {}, options = {}) {
        const { excludeTransfers = false, excludeInterCompany = true, workspaceId = null, includeHidden = false, now = null } = options;
        const nowRef = _resolveNow(now);
        const nowTs = nowRef.getTime();

        // Default to all-time if no range specified
        const start = dateRange.start || new Date('2020-01-01');
        const end = dateRange.end || new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);

        // Event.userId is Mixed, try both
        const query = {
            userId: _uQuery(userId),
            date: { $gte: start, $lte: end }
        };
        if (workspaceId) {
            const wsStr = String(workspaceId);
            const wsId = _uObjId(workspaceId);
            const wsVariants = [wsStr, wsId];
            query.$or = [
                { workspaceId: { $in: wsVariants } },
                { workspaceId: { $exists: false } },
                { workspaceId: null }
            ];
        }

        // Add date range to query
        // query.date = { $gte: start, $lte: end }; // This line is now redundant

        // Fetch operations without populate (using lean for performance)
        let operations = await Event.find(query)
            .sort({ date: -1 })
            .lean();
        if (workspaceId) {
            const fallbackQuery = {
                userId: _uQuery(userId),
                date: { $gte: start, $lte: end }
            };
            const legacyOps = await Event.find(fallbackQuery).sort({ date: -1 }).lean();
            const map = new Map();
            [...operations, ...legacyOps].forEach(op => {
                if (op && op._id) map.set(String(op._id), op);
            });
            operations = Array.from(map.values());
        }

        // Get accounts for intermediary check (use same userId variants)
        const accountsQuery = { userId: _uQuery(userId) };
        if (workspaceId) {
            const wsStr = String(workspaceId);
            const wsId = _uObjId(workspaceId);
            const wsVariants = [wsStr, wsId];
            accountsQuery.$or = [
                { workspaceId: { $in: wsVariants } },
                { workspaceId: { $exists: false } },
                { workspaceId: null }
            ];
        }
        let accounts = await Account.find(accountsQuery).lean();
        if (!accounts.length && workspaceId) {
            accounts = await Account.find({ userId: _uQuery(userId) }).lean();
        }
        const accountIndividualIds = new Set(
            accounts
                .filter(a => a.individualId)
                .map(a => String(a.individualId))
        );

        // Filter and normalize operations
        const normalized = [];

        for (const op of operations) {
            // Skip inter-company transfers if requested
            if (excludeInterCompany && !includeHidden && op.fromCompanyId && op.toCompanyId) {
                continue;
            }

            // Skip retail write-offs
            if (op.isRetailWriteOff || op.retailWriteOff) {
                continue;
            }

            // Skip intermediary individuals (linked to accounts) only when not forcing includeHidden
            const opIndividualId = op.individualId?._id || op.individualId;
            if (!includeHidden && opIndividualId && accountIndividualIds.has(String(opIndividualId))) {
                continue;
            }


            // Normalize amount (support both amount and sum fields)
            const rawAmount = (typeof op.amount === 'number')
                ? op.amount
                : (typeof op.sum === 'number' ? op.sum : 0);
            const absAmount = Math.abs(rawAmount || 0);

            // Determine operation kind - ONLY income, expense, or transfer
            let kind = 'unknown';
            const type = String(op.type || '').toLowerCase();

            if (op.isTransfer || type === 'transfer') {
                kind = 'transfer';
                if (excludeTransfers) continue;
            } else if (type === 'income' || (rawAmount > 0 && !op.isTransfer)) {
                kind = 'income';
            } else if (type === 'expense' || (rawAmount < 0)) {
                kind = 'expense';
            }

            // Determine fact vs forecast
            const opDate = new Date(op.date);
            const isFact = opDate.getTime() <= nowTs;

            normalized.push({
                _id: String(op._id),
                date: _fmtDateDDMMYY(opDate),
                dateIso: opDate.toISOString().slice(0, 10),
                ts: opDate.getTime(),
                type: kind, // 'income', 'expense', 'transfer', or 'unknown'
                kind,
                isFact,
                amount: absAmount,
                rawAmount,
                description: op.description || null,
            });
        }

        // Calculate summary
        const incomeOps = normalized.filter(o => o.kind === 'income');
        const expenseOps = normalized.filter(o => o.kind === 'expense');
        const factIncomeOps = incomeOps.filter(o => o.isFact);
        const forecastIncomeOps = incomeOps.filter(o => !o.isFact);
        const factExpenseOps = expenseOps.filter(o => o.isFact);
        const forecastExpenseOps = expenseOps.filter(o => !o.isFact);

        return {
            operations: normalized,
            summary: {
                total: normalized.length,
                income: {
                    count: incomeOps.length,
                    total: incomeOps.reduce((s, o) => s + o.amount, 0),
                    fact: {
                        count: factIncomeOps.length,
                        total: factIncomeOps.reduce((s, o) => s + o.amount, 0)
                    },
                    forecast: {
                        count: forecastIncomeOps.length,
                        total: forecastIncomeOps.reduce((s, o) => s + o.amount, 0)
                    }
                },
                expense: {
                    count: expenseOps.length,
                    total: expenseOps.reduce((s, o) => s + o.amount, 0),
                    fact: {
                        count: factExpenseOps.length,
                        total: factExpenseOps.reduce((s, o) => s + o.amount, 0)
                    },
                    forecast: {
                        count: forecastExpenseOps.length,
                        total: forecastExpenseOps.reduce((s, o) => s + o.amount, 0)
                    }
                }
            },
            meta: {
                today: _fmtDateDDMMYY(nowRef),
                rangeStart: _fmtDateDDMMYY(start),
                rangeEnd: _fmtDateDDMMYY(end)
            }
        };
    }

    // ========================
    // CATALOG QUERIES
    // ========================

    const _buildWsCondition = (workspaceId) => {
        if (!workspaceId) return null;
        const wsStr = String(workspaceId);
        const wsId = _uObjId(workspaceId);
        const variants = [wsStr, wsId];
        return {
            $or: [
                { workspaceId: { $in: variants } },
                { workspaceId: { $exists: false } },
                { workspaceId: null }
            ]
        };
    };

    async function getCompanies(userId, workspaceId = null) {
        const q = { userId: _uQuery(userId) };
        const docs = await Company.find(q).select('name').lean();
        const idsFromEvents = await Event.distinct('companyId', { userId: _uQuery(userId), companyId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Company.find({ _id: { $in: idsFromEvents } }).select('name').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(c => { if (c && c._id) map.set(String(c._id), c); });
        const names = Array.from(map.values()).map(c => c.name || `Компания ${String(c._id).slice(-4)}`).filter(Boolean);
        return names;
    }

    async function getProjects(userId, workspaceId = null) {
        const q = { userId: _uQuery(userId) };
        const docs = await Project.find(q).select('name title label projectName').lean();
        const idsFromEvents = await Event.distinct('projectId', { userId: _uQuery(userId), projectId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Project.find({ _id: { $in: idsFromEvents } }).select('name title label projectName').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(p => { if (p && p._id) map.set(String(p._id), p); });
        // Если нет документов Project, но есть id из событий — добавим заглушки
        idsFromEvents.forEach(id => {
            if (!map.has(String(id))) {
                map.set(String(id), { _id: id, name: null });
            }
        });
        const names = Array.from(map.values()).map(p => {
            const name = p.name || p.title || p.label || p.projectName;
            return (name && String(name).trim()) ? String(name).trim() : `Проект ${String(p._id).slice(-4)}`;
        }).filter(Boolean);
        return names;
    }

    async function getCategories(userId, workspaceId = null) {
        const q = { userId: _uQuery(userId) };
        const docs = await Category.find(q).select('name type').lean();
        const idsFromEvents = await Event.distinct('categoryId', { userId: _uQuery(userId), categoryId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Category.find({ _id: { $in: idsFromEvents } }).select('name type').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(c => { if (c && c._id) map.set(String(c._id), c); });
        idsFromEvents.forEach(id => { if (!map.has(String(id))) map.set(String(id), { _id: id, name: null }); });
        return Array.from(map.values())
            .map(c => ({ name: c.name || `Категория ${String(c._id).slice(-4)}`, type: c.type }))
            .filter(c => c.name);
    }

    async function getContractors(userId, workspaceId = null) {
        const q = { userId: _uQuery(userId) };
        const docs = await Contractor.find(q).select('name').lean();
        const idsFromEvents = await Event.distinct('contractorId', { userId: _uQuery(userId), contractorId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Contractor.find({ _id: { $in: idsFromEvents } }).select('name').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(c => { if (c && c._id) map.set(String(c._id), c); });
        idsFromEvents.forEach(id => { if (!map.has(String(id))) map.set(String(id), { _id: id, name: null }); });
        return Array.from(map.values())
            .map(c => c.name || `Контрагент ${String(c._id).slice(-4)}`)
            .filter(Boolean);
    }

    async function getIndividuals(userId, workspaceId = null) {
        const q = { userId: _uQuery(userId) };
        const docs = await Individual.find(q).select('name').lean();
        const idsFromEvents = await Event.distinct('individualId', { userId: _uQuery(userId), individualId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Individual.find({ _id: { $in: idsFromEvents } }).select('name').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(i => { if (i && i._id) map.set(String(i._id), i); });
        idsFromEvents.forEach(id => { if (!map.has(String(id))) map.set(String(id), { _id: id, name: null }); });
        return Array.from(map.values())
            .map(i => i.name || `Физлицо ${String(i._id).slice(-4)}`)
            .filter(Boolean);
    }

    // ========================
    // COMBINED DATA PACKET
    // ========================

    /**
     * Build complete data packet for AI from database
     * @param {string} userId - User ID
     * @param {Object} options - Options { dateRange, includeHidden, visibleAccountIds }
     * @returns {Promise<Object>} Data packet for AI
     */
    async function buildDataPacket(userId, options = {}) {
        const { dateRange: periodFilter, includeHidden = false, visibleAccountIds = null, workspaceId = null, now = null } = options;
        const nowRef = _resolveNow(now);

        // ✅ Parse dateRange from periodFilter
        let start = null;
        let end = null;

        if (periodFilter && periodFilter.mode === 'custom') {
            if (periodFilter.customStart) {
                const parsed = new Date(periodFilter.customStart);
                start = _localStartOfDay(parsed);
            }
            if (periodFilter.customEnd) {
                const parsed = new Date(periodFilter.customEnd);
                end = _localEndOfDay(parsed);
            }
        }

        // ✅ If no date range, default to current month
        if (!start || !end) {
            const nowLocal = nowRef || new Date();
            start = _localStartOfDay(new Date(nowLocal.getFullYear(), nowLocal.getMonth(), 1));
            end = _localEndOfDay(new Date(nowLocal.getFullYear(), nowLocal.getMonth() + 1, 0));
        }

        const [accountsData, operationsData, companies, projects, categories, contractors, individuals] =
            await Promise.all([
                getAccounts(userId, { includeHidden, visibleAccountIds, workspaceId, now: nowRef }),
                getOperations(userId, { start, end }, { workspaceId, includeHidden, now: nowRef }),
                getCompanies(userId, workspaceId),
                getProjects(userId, workspaceId),
                getCategories(userId, workspaceId),
                getContractors(userId, workspaceId),
                getIndividuals(userId, workspaceId)
            ]);

        return {
            meta: {
                today: _fmtDateDDMMYY(nowRef),
                periodStart: _fmtDateDDMMYY(start),
                periodEnd: _fmtDateDDMMYY(end),
                forecastUntil: operationsData.meta.rangeEnd,
                todayTimestamp: nowRef.getTime(),
                source: 'database'
            },
            totals: accountsData.totals,
            accounts: accountsData.accounts,
            operations: operationsData.operations,
            operationsSummary: operationsData.summary,
            catalogs: {
                companies,
                projects,
                categories: categories.map(c => c.name),
                contractors,
                individuals
            }
        };
    }

    // Return public API
    return {
        getAccounts,
        getOperations,
        getCompanies,
        getProjects,
        getCategories,
        getContractors,
        getIndividuals,
        buildDataPacket,
        // Expose helpers for testing
        _kzNow,
        _fmtDateDDMMYY
    };
};
