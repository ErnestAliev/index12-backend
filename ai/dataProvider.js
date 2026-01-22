// ai/dataProvider.js
// Direct database access layer for AI assistant
// Replaces fragile uiSnapshot parsing with reliable MongoDB queries

/**
 * Creates a data provider for AI assistant queries.
 * @param {Object} deps - Dependencies containing Mongoose models
 * @returns {Object} Data provider with query methods
 */
module.exports = function createDataProvider(deps) {
    const { Account, Company, Project, Category, Contractor, Individual, Event } = deps;

    // ========================
    // HELPER FUNCTIONS
    // ========================

    // Kazakhstan timezone helpers (UTC+5)
    const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

    const _kzNow = () => {
        const utc = new Date();
        return new Date(utc.getTime() + KZ_OFFSET_MS);
    };

    const _kzStartOfDay = (d) => {
        const kz = new Date(d.getTime());
        kz.setHours(0, 0, 0, 0);
        return kz;
    };

    const _kzEndOfDay = (d) => {
        const kz = new Date(d.getTime());
        kz.setHours(23, 59, 59, 999);
        return kz;
    };

    const _fmtDateDDMMYY = (d) => {
        if (!d || !(d instanceof Date) || isNaN(d.getTime())) return null;
        const dd = String(d.getDate()).padStart(2, '0');
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const yy = String(d.getFullYear()).slice(2);
        return `${dd}.${mm}.${yy}`;
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
        const { includeHidden = false, visibleAccountIds = null } = options;

        // Build query
        const query = { userId };
        if (visibleAccountIds && Array.isArray(visibleAccountIds) && visibleAccountIds.length > 0) {
            const mongoose = require('mongoose');
            query._id = {
                $in: visibleAccountIds.map(id => {
                    try { return new mongoose.Types.ObjectId(id); } catch { return id; }
                })
            };
        }

        // Fetch accounts with populated references
        const accounts = await Account.find(query)
            .populate('companyId', 'name')
            .populate('individualId', 'name')
            .populate('contractorId', 'name')
            .lean();

        // Get today for fact/forecast split
        const today = _kzStartOfDay(_kzNow());
        const todayEnd = _kzEndOfDay(_kzNow());

        // Calculate balances for each account
        const accountsWithBalances = await Promise.all(accounts.map(async (acc) => {
            const isHidden = !!(acc.isExcluded || acc.hidden || acc.isHidden);

            // Skip hidden accounts if not requested
            if (!includeHidden && isHidden) return null;

            // Get all operations for this account
            const allOps = await Event.find({
                userId,
                $or: [
                    { accountId: acc._id },
                    { fromAccountId: acc._id },
                    { toAccountId: acc._id }
                ]
            }).lean();

            // Calculate current balance (up to today)
            const currentOps = allOps.filter(op => new Date(op.date) <= todayEnd);
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
                company: acc.companyId?.name || null,
                individual: acc.individualId?.name || null,
                contractor: acc.contractorId?.name || null,
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
                today: _fmtDateDDMMYY(_kzNow()),
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
        const { excludeTransfers = false, excludeInterCompany = true } = options;

        // Default to all-time if no range specified
        const start = dateRange.start || new Date('2020-01-01');
        const end = dateRange.end || new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);

        // Fetch operations with populated references
        const operations = await Event.find({
            userId,
            date: { $gte: start, $lte: end }
        })
            .populate('categoryId', 'name')
            .populate('accountId', 'name individualId')
            .populate('companyId', 'name')
            .populate('contractorId', 'name')
            .populate('projectId', 'name')
            .populate('individualId', 'name')
            .sort({ date: -1 })
            .lean();

        // Get accounts for intermediary check
        const accounts = await Account.find({ userId }).lean();
        const accountIndividualIds = new Set(
            accounts
                .filter(a => a.individualId)
                .map(a => String(a.individualId))
        );

        // Filter and normalize operations
        const today = _kzStartOfDay(_kzNow());
        const normalized = [];

        for (const op of operations) {
            // Skip inter-company transfers if requested
            if (excludeInterCompany && op.fromCompanyId && op.toCompanyId) {
                continue;
            }

            // Skip retail write-offs
            if (op.isRetailWriteOff || op.retailWriteOff) {
                continue;
            }

            // Skip intermediary individuals (linked to accounts)
            const opIndividualId = op.individualId?._id || op.individualId;
            if (opIndividualId && accountIndividualIds.has(String(opIndividualId))) {
                continue;
            }

            // Determine operation kind
            let kind = 'unknown';
            const type = String(op.type || '').toLowerCase();
            if (op.isTransfer || type === 'transfer') {
                kind = 'transfer';
                if (excludeTransfers) continue;
            } else if (op.isWithdrawal || type === 'withdrawal') {
                kind = 'withdrawal';
            } else if (type === 'income' || (op.amount && op.amount > 0 && !op.isTransfer)) {
                kind = 'income';
            } else if (type === 'expense' || (op.amount && op.amount < 0)) {
                kind = 'expense';
            }

            // Determine fact vs forecast
            const opDate = new Date(op.date);
            const isFact = opDate <= today;

            normalized.push({
                _id: String(op._id),
                date: _fmtDateDDMMYY(opDate),
                dateIso: opDate.toISOString().slice(0, 10),
                ts: opDate.getTime(),
                kind,
                isFact,
                amount: Math.abs(op.amount || 0),
                rawAmount: op.amount || 0,
                category: op.categoryId?.name || null,
                account: op.accountId?.name || null,
                company: op.companyId?.name || null,
                contractor: op.contractorId?.name || null,
                project: op.projectId?.name || null,
                individual: op.individualId?.name || null,
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
                today: _fmtDateDDMMYY(_kzNow()),
                rangeStart: _fmtDateDDMMYY(start),
                rangeEnd: _fmtDateDDMMYY(end)
            }
        };
    }

    // ========================
    // CATALOG QUERIES
    // ========================

    async function getCompanies(userId) {
        const companies = await Company.find({ userId }).select('name').lean();
        return companies.map(c => c.name).filter(Boolean);
    }

    async function getProjects(userId) {
        const projects = await Project.find({ userId }).select('name').lean();
        return projects.map(p => p.name).filter(Boolean);
    }

    async function getCategories(userId) {
        const categories = await Category.find({ userId }).select('name type').lean();
        return categories.map(c => ({ name: c.name, type: c.type })).filter(c => c.name);
    }

    async function getContractors(userId) {
        const contractors = await Contractor.find({ userId }).select('name').lean();
        return contractors.map(c => c.name).filter(Boolean);
    }

    async function getIndividuals(userId) {
        const individuals = await Individual.find({ userId }).select('name').lean();
        return individuals.map(i => i.name).filter(Boolean);
    }

    // ========================
    // COMBINED DATA PACKET
    // ========================

    /**
     * Build complete data packet for AI from database
     * @param {string} userId - User ID
     * @param {Object} options - Options
     * @returns {Promise<Object>} Data packet for AI
     */
    async function buildDataPacket(userId, options = {}) {
        const { dateRange, includeHidden = false, visibleAccountIds = null } = options;

        const [accountsData, operationsData, companies, projects, categories, contractors, individuals] =
            await Promise.all([
                getAccounts(userId, { includeHidden, visibleAccountIds }),
                getOperations(userId, dateRange, {}),
                getCompanies(userId),
                getProjects(userId),
                getCategories(userId),
                getContractors(userId),
                getIndividuals(userId)
            ]);

        return {
            meta: {
                today: _fmtDateDDMMYY(_kzNow()),
                forecastUntil: operationsData.meta.rangeEnd,
                todayTimestamp: _kzNow().getTime(),
                source: 'database' // Mark that this is from DB, not uiSnapshot
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
