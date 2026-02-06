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
        const { includeHidden = false, visibleAccountIds = null, workspaceId = null, now = null, end = null } = options;
        const nowRef = _resolveNow(now);
        const endRef = end ? _localEndOfDay(end) : _localEndOfDay(nowRef);

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

            const allowIds = (visibleAccountIds && Array.isArray(visibleAccountIds) && visibleAccountIds.length)
                ? new Set(visibleAccountIds.map(id => String(id)))
                : null;

            const accMap = new Map();
            const maybeAdd = (a) => {
                if (!a || !a._id) return;
                const id = String(a._id);
                if (allowIds && !allowIds.has(id)) return;
                accMap.set(id, a);
            };

            [...accounts, ...legacyAccs, ...allAccsNoFilter].forEach(maybeAdd);
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

            // Calculate future balance (operations up to endRef)
            const futureOps = allOps.filter(op => new Date(op.date) <= endRef);

            let futureBalance = acc.initialBalance || 0;
            for (const op of futureOps) {
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
                companyId: acc.companyId ? String(acc.companyId) : null,
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

        // Get accounts for intermediary check (use same userId variants) and names for transfers
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
        // Дополнительно загрузим все аккаунты пользователя (без workspace-фильтра) для имени переводов
        const allUserAccounts = await Account.find({ userId: _uQuery(userId) }).lean();
        const accMap = new Map();
        const addAcc = (a) => { if (a && a._id) accMap.set(String(a._id), a); };
        accounts.forEach(addAcc);
        allUserAccounts.forEach(addAcc);
        accounts = Array.from(accMap.values());

        const accNameById = new Map(accounts.map(a => [String(a._id), a.name || 'Счет']));
        const accountIndividualIds = new Set(
            accounts
                .filter(a => a.individualId)
                .map(a => String(a.individualId))
        );

        // Companies & individuals maps for transfers
        let companies = [];
        try {
            companies = await Company.find({ userId: _uQuery(userId) }).lean();
        } catch (_) { companies = []; }
        const companyNameById = new Map(companies.map(c => [String(c._id), c.name || 'Компания']));

        let individuals = [];
        try {
            individuals = await Individual.find({ userId: _uQuery(userId) }).lean();
        } catch (_) { individuals = []; }
        const individualNameById = new Map(individuals.map(i => [String(i._id), i.name || 'Физлицо']));

        // Filter and normalize operations
        const normalized = [];

        for (const op of operations) {
            // Управленческие родители и исключенные из итогов не должны попадать в расчеты AI,
            // НО взаимозачетные расходы (offsetIncomeId) нужно учитывать как расходы.
            if (op.excludeFromTotals && !op.offsetIncomeId) continue;
            if (op.isSplitParent) continue;

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
                accountId: op.accountId?._id ? String(op.accountId._id) : (op.accountId ? String(op.accountId) : null),
                fromAccountId: op.fromAccountId?._id ? String(op.fromAccountId._id) : (op.fromAccountId ? String(op.fromAccountId) : null),
                toAccountId: op.toAccountId?._id ? String(op.toAccountId._id) : (op.toAccountId ? String(op.toAccountId) : null),
                fromAccountName: op.fromAccountId?.name || accNameById.get(String(op.fromAccountId || '')) || null,
                toAccountName: op.toAccountId?.name || accNameById.get(String(op.toAccountId || '')) || null,
                projectId: op.projectId?._id ? String(op.projectId._id) : (op.projectId ? String(op.projectId) : null),
                contractorId: op.contractorId?._id ? String(op.contractorId._id) : (op.contractorId ? String(op.contractorId) : null),
                categoryId: op.categoryId?._id ? String(op.categoryId._id) : (op.categoryId ? String(op.categoryId) : null),
                companyId: op.companyId?._id ? String(op.companyId._id) : (op.companyId ? String(op.companyId) : null),
                fromCompanyId: op.fromCompanyId?._id ? String(op.fromCompanyId._id) : (op.fromCompanyId ? String(op.fromCompanyId) : null),
                toCompanyId: op.toCompanyId?._id ? String(op.toCompanyId._id) : (op.toCompanyId ? String(op.toCompanyId) : null),
                companyName: op.companyId?.name || companyNameById.get(String(op.companyId || '')) || null,
                fromCompanyName: op.fromCompanyId?.name || companyNameById.get(String(op.fromCompanyId || '')) || null,
                toCompanyName: op.toCompanyId?.name || companyNameById.get(String(op.toCompanyId || '')) || null,
                individualId: op.individualId?._id ? String(op.individualId._id) : (op.individualId ? String(op.individualId) : null),
                counterpartyIndividualId: op.counterpartyIndividualId?._id ? String(op.counterpartyIndividualId._id) : (op.counterpartyIndividualId ? String(op.counterpartyIndividualId) : null),
                fromIndividualId: op.fromIndividualId?._id ? String(op.fromIndividualId._id) : (op.fromIndividualId ? String(op.fromIndividualId) : null),
                toIndividualId: op.toIndividualId?._id ? String(op.toIndividualId._id) : (op.toIndividualId ? String(op.toIndividualId) : null),
                individualName: op.individualId?.name || individualNameById.get(String(op.individualId || '')) || null,
                fromIndividualName: op.fromIndividualId?.name || individualNameById.get(String(op.fromIndividualId || '')) || null,
                toIndividualName: op.toIndividualId?.name || individualNameById.get(String(op.toIndividualId || '')) || null,
            });
        }

        // Calculate summary
        const incomeOps = normalized.filter(o => o.kind === 'income');
        const expenseOps = normalized.filter(o => o.kind === 'expense');
        const factIncomeOps = incomeOps.filter(o => o.isFact);
        const forecastIncomeOps = incomeOps.filter(o => !o.isFact);
        const factExpenseOps = expenseOps.filter(o => o.isFact);
        const forecastExpenseOps = expenseOps.filter(o => !o.isFact);

        // Пересчёт по датам (для поиска напряжённых дней)
        const dayMap = new Map(); // dateIso -> aggregate
        const _accDay = (iso, op, field) => {
            if (!iso) return;
            if (!dayMap.has(iso)) {
                dayMap.set(iso, {
                    dateIso: iso,
                    date: op.date,
                    ts: op.ts,
                    incomeFact: 0,
                    incomeForecast: 0,
                    expenseFact: 0,
                    expenseForecast: 0,
                });
            }
            const d = dayMap.get(iso);
            d[field] += op.amount || 0;
        };
        for (const op of normalized) {
            if (op.kind === 'income') {
                _accDay(op.dateIso, op, op.isFact ? 'incomeFact' : 'incomeForecast');
            } else if (op.kind === 'expense') {
                _accDay(op.dateIso, op, op.isFact ? 'expenseFact' : 'expenseForecast');
            }
        }
        const daySummary = Array.from(dayMap.values()).map(d => {
            const incomeTotal = d.incomeFact + d.incomeForecast;
            const expenseTotal = d.expenseFact + d.expenseForecast;
            const volume = incomeTotal + expenseTotal;
            return { ...d, incomeTotal, expenseTotal, volume };
        }).sort((a, b) => b.volume - a.volume);

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
            },
            daySummary
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
        const docs = await Company.find(q).select('name taxRegime taxPercent identificationNumber').lean();
        const idsFromEvents = await Event.distinct('companyId', { userId: _uQuery(userId), companyId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Company.find({ _id: { $in: idsFromEvents } }).select('name taxRegime taxPercent identificationNumber').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(c => { if (c && c._id) map.set(String(c._id), c); });
        idsFromEvents.forEach(id => { if (!map.has(String(id))) map.set(String(id), { _id: id, name: null }); });
        return Array.from(map.values())
            .map(c => ({
                id: String(c._id),
                name: c.name || `Компания ${String(c._id).slice(-4)}`,
                taxRegime: c.taxRegime || 'simplified',
                taxPercent: c.taxPercent != null ? c.taxPercent : 3,
                identificationNumber: c.identificationNumber || null
            }))
            .filter(c => c.name);
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
        const items = Array.from(map.values()).map(p => {
            const name = p.name || p.title || p.label || p.projectName;
            return {
                id: String(p._id),
                name: (name && String(name).trim()) ? String(name).trim() : `Проект ${String(p._id).slice(-4)}`
            };
        }).filter(p => p.name);
        return items;
    }

    async function getCategories(userId, workspaceId = null) {
        const q = { userId: _uQuery(userId) };
        const docs = await Category.find(q).select('name type').lean();
        const idsFromEvents = await Event.distinct('categoryId', { userId: _uQuery(userId), categoryId: { $ne: null } });
        const extraDocs = idsFromEvents.length ? await Category.find({ _id: { $in: idsFromEvents } }).select('name type').lean() : [];
        const map = new Map();
        [...docs, ...extraDocs].forEach(c => { if (c && c._id) map.set(String(c._id), c); });
        idsFromEvents.forEach(id => { if (!map.has(String(id))) map.set(String(id), { _id: id, name: null, type: null }); });
        return Array.from(map.values())
            .map(c => ({ id: String(c._id), name: c.name || `Категория ${String(c._id).slice(-4)}`, type: c.type || null }))
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
            .map(c => ({ id: String(c._id), name: c.name || `Контрагент ${String(c._id).slice(-4)}` }))
            .filter(c => c.name);
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
            .map(i => ({ id: String(i._id), name: i.name || `Физлицо ${String(i._id).slice(-4)}` }))
            .filter(i => i.name);
    }

    // ========================
    // COMBINED DATA PACKET
    // ========================

    /**
     * Build complete data packet for AI from database
     * @param {string} userId - User ID
     * @param {Object} options - Options { dateRange, includeHidden, visibleAccountIds, snapshot }
     * @returns {Promise<Object>} Data packet for AI
     */
    async function buildDataPacket(userId, options = {}) {
        const { dateRange: periodFilter, includeHidden = false, visibleAccountIds = null, workspaceId = null, now = null, snapshot = null } = options;
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

        // 🔥 HYBRID MODE: Use snapshot for accounts/companies if available, MongoDB for everything else
        const useSnapshotAccounts = snapshot && Array.isArray(snapshot.accounts) && snapshot.accounts.length > 0;
        const useSnapshotCompanies = snapshot && Array.isArray(snapshot.companies) && snapshot.companies.length > 0;

        if (useSnapshotAccounts) {
            console.log(`[dataProvider] 🔥 Using SNAPSHOT for accounts (${snapshot.accounts.length} accounts)`);
        }
        if (useSnapshotCompanies) {
            console.log(`[dataProvider] 🔥 Using SNAPSHOT for companies (${snapshot.companies.length} companies)`);
        }

        // Build promises array conditionally
        const promises = [];

        // Accounts: snapshot priority, fallback to MongoDB
        if (useSnapshotAccounts) {
            promises.push(Promise.resolve({
                accounts: snapshot.accounts.map(a => ({
                    _id: String(a._id || a.id || a.accountId || ''),
                    name: a.name || a.accountName || 'Счет',
                    currentBalance: Math.round(Number(a.balance ?? a.currentBalance ?? 0)),
                    futureBalance: Math.round(Number(a.futureBalance ?? a.balance ?? 0)),
                    companyId: a.companyId ? String(a.companyId) : null,
                    isHidden: !!(a.isHidden || a.hidden || a.isExcluded || a.excluded || a.excludeFromTotal),
                    isExcluded: !!(a.isExcluded || a.excluded || a.excludeFromTotal)
                })).filter(a => a._id),
                openAccounts: [],
                hiddenAccounts: [],
                totals: {
                    open: { current: 0, future: 0 },
                    hidden: { current: 0, future: 0 },
                    all: { current: 0, future: 0 }
                },
                meta: { today: _fmtDateDDMMYY(nowRef), count: 0, openCount: 0, hiddenCount: 0 }
            }));
        } else {
            promises.push(getAccounts(userId, { includeHidden, visibleAccountIds, workspaceId, now: nowRef }));
        }

        // Operations: always from MongoDB
        promises.push(getOperations(userId, { start, end }, { workspaceId, includeHidden, now: nowRef }));

        // Companies: snapshot priority, fallback to MongoDB
        if (useSnapshotCompanies) {
            promises.push(Promise.resolve(snapshot.companies.map(c => ({
                id: String(c._id || c.id || ''),
                name: c.name || `Компания ${String(c._id || c.id || '').slice(-4)}`
            })).filter(c => c.id && c.name)));
        } else {
            promises.push(getCompanies(userId, workspaceId));
        }

        // Projects, Categories, Contractors, Individuals: always from MongoDB
        promises.push(getProjects(userId, workspaceId));
        promises.push(getCategories(userId, workspaceId));
        promises.push(getContractors(userId, workspaceId));
        promises.push(getIndividuals(userId, workspaceId));

        const [accountsData, operationsData, companies, projects, categories, contractors, individuals] = await Promise.all(promises);

        // Recalculate totals if using snapshot accounts
        if (useSnapshotAccounts && accountsData.accounts) {
            const openAccs = accountsData.accounts.filter(a => !a.isHidden);
            const hiddenAccs = accountsData.accounts.filter(a => a.isHidden);
            const openCurrent = openAccs.reduce((s, a) => s + (a.currentBalance || 0), 0);
            const openFuture = openAccs.reduce((s, a) => s + (a.futureBalance || 0), 0);
            const hiddenCurrent = hiddenAccs.reduce((s, a) => s + (a.currentBalance || 0), 0);
            const hiddenFuture = hiddenAccs.reduce((s, a) => s + (a.futureBalance || 0), 0);

            accountsData.openAccounts = openAccs;
            accountsData.hiddenAccounts = hiddenAccs;
            accountsData.totals = {
                open: { current: openCurrent, future: openFuture },
                hidden: { current: hiddenCurrent, future: hiddenFuture },
                all: { current: openCurrent + hiddenCurrent, future: openFuture + hiddenFuture }
            };
            accountsData.meta = {
                today: _fmtDateDDMMYY(nowRef),
                count: accountsData.accounts.length,
                openCount: openAccs.length,
                hiddenCount: hiddenAccs.length
            };
        }

        // Обогатим операции человекочитаемыми названиями (категория/контрагент/физлицо/счета/компании)
        if (Array.isArray(operationsData.operations)) {
            const accountNameById = new Map();
            accountsData.accounts.forEach(a => {
                if (!a || !a._id) return;
                accountNameById.set(String(a._id), a.name || `Счет ${String(a._id).slice(-4)}`);
            });

            const projectNameById = new Map();
            projects.forEach(p => {
                const pid = p.id || p._id;
                if (!pid) return;
                projectNameById.set(String(pid), p.name || `Проект ${String(pid).slice(-4)}`);
            });

            const catNameById = new Map();
            categories.forEach(c => {
                const cid = c.id || c._id;
                if (!cid) return;
                catNameById.set(String(cid), c.name || `Категория ${String(cid).slice(-4)}`);
            });

            const companyNameById = new Map();
            companies.forEach(c => {
                const cid = c.id || c._id;
                if (!cid) return;
                companyNameById.set(String(cid), c.name || `Компания ${String(cid).slice(-4)}`);
            });

            const contractorNameById = new Map();
            contractors.forEach(c => {
                const cid = c.id || c._id;
                if (!cid) return;
                contractorNameById.set(String(cid), c.name || `Контрагент ${String(cid).slice(-4)}`);
            });

            const individualNameById = new Map();
            individuals.forEach(i => {
                const iid = i.id || i._id;
                if (!iid) return;
                individualNameById.set(String(iid), i.name || `Физлицо ${String(iid).slice(-4)}`);
            });

            operationsData.operations.forEach(op => {
                const catId = op.categoryId ? String(op.categoryId) : null;
                const contrId = op.contractorId ? String(op.contractorId) : null;
                const indivContrId = op.counterpartyIndividualId ? String(op.counterpartyIndividualId) : null;
                const projId = op.projectId ? String(op.projectId) : null;
                const accId = op.accountId ? String(op.accountId) : null;
                const fromAccId = op.fromAccountId ? String(op.fromAccountId) : null;
                const toAccId = op.toAccountId ? String(op.toAccountId) : null;
                const companyId = op.companyId ? String(op.companyId) : null;
                const fromCompanyId = op.fromCompanyId ? String(op.fromCompanyId) : null;
                const toCompanyId = op.toCompanyId ? String(op.toCompanyId) : null;
                const individualId = op.individualId ? String(op.individualId) : null;
                const fromIndividualId = op.fromIndividualId ? String(op.fromIndividualId) : null;
                const toIndividualId = op.toIndividualId ? String(op.toIndividualId) : null;

                if (catId && !op.categoryName) op.categoryName = catNameById.get(catId) || op.categoryName;
                if (projId && !op.projectName) op.projectName = projectNameById.get(projId) || op.projectName;
                if (accId && !op.accountName) op.accountName = accountNameById.get(accId) || op.accountName;
                if (fromAccId && !op.fromAccountName) op.fromAccountName = accountNameById.get(fromAccId) || op.fromAccountName;
                if (toAccId && !op.toAccountName) op.toAccountName = accountNameById.get(toAccId) || op.toAccountName;
                if (companyId && !op.companyName) op.companyName = companyNameById.get(companyId) || op.companyName;
                if (fromCompanyId && !op.fromCompanyName) op.fromCompanyName = companyNameById.get(fromCompanyId) || op.fromCompanyName;
                if (toCompanyId && !op.toCompanyName) op.toCompanyName = companyNameById.get(toCompanyId) || op.toCompanyName;
                if (individualId && !op.individualName) op.individualName = individualNameById.get(individualId) || op.individualName;
                if (fromIndividualId && !op.fromIndividualName) op.fromIndividualName = individualNameById.get(fromIndividualId) || op.fromIndividualName;
                if (toIndividualId && !op.toIndividualName) op.toIndividualName = individualNameById.get(toIndividualId) || op.toIndividualName;

                // Контрагент: сначала из contractorId, fallback на counterpartyIndividualId
                let contractorName = contrId ? contractorNameById.get(contrId) : null;
                if (!contractorName && indivContrId) contractorName = individualNameById.get(indivContrId);
                if (contractorName && !op.contractorName) op.contractorName = contractorName;

                // Если contractorId отсутствует, но есть физлицо-получатель — подставим его как contractorId для дальнейшей агрегации
                if (!op.contractorId && indivContrId) {
                    op.contractorId = indivContrId;
                }
            });
        }

        // Contractor summary (по операциям)
        const contractorMap = new Map();
        (contractors || []).forEach(c => { if (c?.id) contractorMap.set(String(c.id), c.name || c.id); });
        const contractorSummaryMap = new Map();
        (operationsData.operations || []).forEach(op => {
            const cid = op.contractorId ? String(op.contractorId) : null;
            if (!cid) return;
            if (!contractorSummaryMap.has(cid)) {
                contractorSummaryMap.set(cid, {
                    id: cid,
                    name: contractorMap.get(cid) || `Контрагент ${cid.slice(-4)}`,
                    incomeFact: 0,
                    incomeForecast: 0,
                    expenseFact: 0,
                    expenseForecast: 0,
                });
            }
            const rec = contractorSummaryMap.get(cid);
            if (op.kind === 'income') {
                if (op.isFact) rec.incomeFact += op.amount || 0;
                else rec.incomeForecast += op.amount || 0;
            } else if (op.kind === 'expense') {
                if (op.isFact) rec.expenseFact += op.amount || 0;
                else rec.expenseForecast += op.amount || 0;
            }
        });
        const contractorSummary = Array.from(contractorSummaryMap.values()).sort((a, b) => {
            const aVol = a.incomeFact + a.incomeForecast + a.expenseFact + a.expenseForecast;
            const bVol = b.incomeFact + b.incomeForecast + b.expenseFact + b.expenseForecast;
            return bVol - aVol;
        });

        // Category summary (по операциям)
        const categoryMap = new Map();
        (categories || []).forEach(c => { if (c?.id) categoryMap.set(String(c.id), { name: c.name, type: c.type }); });
        const categorySummaryMap = new Map();
        (operationsData.operations || []).forEach(op => {
            const cid = op.categoryId ? String(op.categoryId) : null;
            if (!cid) return;
            if (!categorySummaryMap.has(cid)) {
                const meta = categoryMap.get(cid) || { name: `Категория ${cid.slice(-4)}`, type: null };
                categorySummaryMap.set(cid, {
                    id: cid,
                    name: meta.name,
                    type: meta.type,
                    incomeFact: 0,
                    incomeForecast: 0,
                    expenseFact: 0,
                    expenseForecast: 0,
                });
            }
            const rec = categorySummaryMap.get(cid);
            if (op.kind === 'income') {
                if (op.isFact) rec.incomeFact += op.amount || 0;
                else rec.incomeForecast += op.amount || 0;
            } else if (op.kind === 'expense') {
                if (op.isFact) rec.expenseFact += op.amount || 0;
                else rec.expenseForecast += op.amount || 0;
            }
        });
        // Мапа тегов по ключевым словам (распространяется как на категории, так и на описания операций)
        const TAG_RULES = [
            { tag: 'rent', keywords: ['аренд', 'rent', 'lease', 'шаляпина', 'акмекен', 'пушкина'] },
            { tag: 'payroll', keywords: ['фот', 'зарплат', 'оклад', 'salary', 'payroll', 'прем', 'бонуст'] },
            { tag: 'tax', keywords: ['налог', 'ндс', 'нпн', 'пн', 'соц', 'ипн'] },
            { tag: 'utility', keywords: ['коммун', 'utility', 'газ', 'свет', 'электр', 'вода', 'тепло'] },
            { tag: 'transfer', keywords: ['перевод', 'трансфер', 'между компаниями'] },
        ];

        const _tagByText = (text) => {
            const t = String(text || '').toLowerCase();
            if (!t) return new Set();
            const found = new Set();
            TAG_RULES.forEach(rule => {
                if (rule.keywords.some(k => t.includes(k))) found.add(rule.tag);
            });
            return found;
        };

        // Распространяем теги на категории и операции
        const categoryTags = new Map();
        (categories || []).forEach(c => {
            const tags = _tagByText(c.name);
            categoryTags.set(String(c.id), tags);
        });

        // Пополняем теги из описаний операций (если категория не подсказала)
        (operationsData.operations || []).forEach(op => {
            const cid = op.categoryId ? String(op.categoryId) : null;
            if (!cid) return;
            const opTags = _tagByText(op.description);
            if (!opTags.size) return;
            const cur = categoryTags.get(cid) || new Set();
            opTags.forEach(t => cur.add(t));
            categoryTags.set(cid, cur);
        });

        const totalIncomeAll = operationsData.summary.income.total || 0;
        const totalExpenseAll = operationsData.summary.expense.total || 0;

        const categorySummary = Array.from(categorySummaryMap.values()).map(cat => {
            const vol = cat.incomeFact + cat.incomeForecast + cat.expenseFact + cat.expenseForecast;
            const tags = Array.from(categoryTags.get(cat.id) || []);
            const incomeShare = totalIncomeAll ? (cat.incomeFact + cat.incomeForecast) / totalIncomeAll : 0;
            const expenseShare = totalExpenseAll ? (cat.expenseFact + cat.expenseForecast) / totalExpenseAll : 0;
            return { ...cat, volume: vol, tags, incomeShare, expenseShare };
        }).sort((a, b) => b.volume - a.volume);

        // Tag-level aggregation (rent/payroll/tax/utility/transfer)
        const tagSummaryMap = new Map();
        categorySummary.forEach(cat => {
            (cat.tags || []).forEach(tag => {
                if (!tagSummaryMap.has(tag)) {
                    tagSummaryMap.set(tag, {
                        tag,
                        incomeFact: 0,
                        incomeForecast: 0,
                        expenseFact: 0,
                        expenseForecast: 0,
                        categories: new Set()
                    });
                }
                const rec = tagSummaryMap.get(tag);
                rec.incomeFact += cat.incomeFact || 0;
                rec.incomeForecast += cat.incomeForecast || 0;
                rec.expenseFact += cat.expenseFact || 0;
                rec.expenseForecast += cat.expenseForecast || 0;
                if (cat.name) rec.categories.add(cat.name);
            });
        });
        const tagSummary = Array.from(tagSummaryMap.values()).map(t => ({
            tag: t.tag,
            incomeFact: t.incomeFact,
            incomeForecast: t.incomeForecast,
            expenseFact: t.expenseFact,
            expenseForecast: t.expenseForecast,
            volume: t.incomeFact + t.incomeForecast + t.expenseFact + t.expenseForecast,
            categories: Array.from(t.categories)
        })).sort((a, b) => b.volume - a.volume);

        const contractorSummaryWithShare = contractorSummary.map(c => {
            const vol = c.incomeFact + c.incomeForecast + c.expenseFact + c.expenseForecast;
            const share = (totalIncomeAll + totalExpenseAll) ? vol / (totalIncomeAll + totalExpenseAll) : 0;
            return { ...c, volume: vol, share };
        });

        // Аномалии: топ операции по сумме
        const opsForOutliers = Array.isArray(operationsData.operations) ? operationsData.operations : [];
        const incomeOutliers = opsForOutliers
            .filter(o => o.kind === 'income')
            .sort((a, b) => b.amount - a.amount)
            .slice(0, 3);
        const expenseOutliers = opsForOutliers
            .filter(o => o.kind === 'expense')
            .sort((a, b) => b.amount - a.amount)
            .slice(0, 3);

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
            accountsData,
            operations: operationsData.operations,
            operationsSummary: operationsData.summary,
            catalogs: {
                companies,
                projects,
                // Сохраняем id+name, чтобы маршруты могли сопоставлять категории по id
                categories,
                contractors,
                individuals
            },
            contractorSummary: contractorSummaryWithShare,
            categorySummary,
            tagSummary,
            outliers: {
                income: incomeOutliers,
                expense: expenseOutliers
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
