// backend/ai/dataProvider.js
// Direct database access for AI data retrieval
// Replaces frontend uiSnapshot dependency

module.exports = function createDataProvider(deps) {
  const { Account, Company, Project, Category, Contractor, Individual, Event } = deps;

  // KZ time helpers (Asia/Almaty ~ UTC+05:00)
  const KZ_OFFSET_MS = 5 * 60 * 60 * 1000;

  const _kzStartOfDay = (d) => {
    const t = new Date(d);
    const shifted = new Date(t.getTime() + KZ_OFFSET_MS);
    shifted.setUTCHours(0, 0, 0, 0);
    return new Date(shifted.getTime() - KZ_OFFSET_MS);
  };

  const _kzEndOfDay = (d) => {
    const start = _kzStartOfDay(d);
    return new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
  };

  const _fmtDateKZ = (d) => {
    try {
      const x = new Date(new Date(d).getTime() + KZ_OFFSET_MS);
      const dd = String(x.getUTCDate()).padStart(2, '0');
      const mm = String(x.getUTCMonth() + 1).padStart(2, '0');
      const yy = String(x.getUTCFullYear() % 100).padStart(2, '0');
      return `${dd}.${mm}.${yy}`;
    } catch (_) {
      return String(d);
    }
  };

  // Filter helpers matching frontend mainStore.js logic
  const _isInterCompanyOp = (op) => {
    // Check if operation has both fromCompanyId and toCompanyId
    const from = op.fromCompanyId?._id || op.fromCompanyId;
    const to = op.toCompanyId?._id || op.toCompanyId;
    if (from && to) return true;

    // Also check category name (legacy support)
    if (op.categoryId) {
      const catName = (op.categoryId.name || '').toLowerCase().trim();
      if (['меж.комп', 'межкомпаний', 'inter-comp'].includes(catName)) return true;
    }
    return false;
  };

  const _isRetailWriteOff = (op) => {
    if (!op) return false;
    if (op.type !== 'expense') return false;
    if (op.accountId) return false;
    // Check if counterpartyIndividualId matches retail individual
    // Note: We'll need to pass retailIndividualId or check it separately
    return !!(op.isRetailWriteOff || op.retailWriteOff);
  };

  const _isIntermediaryIndividual = (op, accounts) => {
    // Check if operation's individualId matches any account's individualId
    const opIndId = op.individualId?._id || op.individualId;
    if (!opIndId) return false;

    return accounts.some(acc => {
      const accIndId = acc.individualId?._id || acc.individualId;
      return accIndId && String(accIndId) === String(opIndId);
    });
  };

  const _isCreditIncome = (op) => {
    // Check if category is credit-related (can be enhanced later)
    return false; // Simplified for now
  };

  /**
   * Get accounts with balance calculations
   * @param {string} userId - User ID
   * @param {object} options - Options
   * @param {boolean} options.includeHidden - Include excluded accounts
   * @param {Date} options.asOf - Calculate balance as of this date (default: today)
   * @returns {Promise<Array>} Array of account objects with balances
   */
  async function getAccounts(userId, options = {}) {
    const { includeHidden = true, asOf = new Date() } = options;
    
    const query = { userId };
    if (!includeHidden) {
      query.isExcluded = { $ne: true };
    }

    const accounts = await Account.find(query)
      .populate('companyId individualId contractorId')
      .lean()
      .sort({ order: 1, name: 1 });

    const todayEnd = _kzEndOfDay(asOf);
    const todayStart = _kzStartOfDay(asOf);

    // Get all operations up to asOf date for balance calculation
    const operations = await Event.find({
      userId,
      date: { $lte: todayEnd }
    })
      .populate('categoryId accountId companyId contractorId projectId individualId')
      .lean();

    // Calculate balances for each account
    const accountsWithBalances = accounts.map(acc => {
      let factBalance = Number(acc.initialBalance || 0);
      let forecastBalance = Number(acc.initialBalance || 0);

      // Process operations for this account
      operations.forEach(op => {
        const opDate = new Date(op.date);
        const absAmt = Math.abs(Number(op.amount) || 0);

        // Handle transfers
        if (op.isTransfer || op.type === 'transfer') {
          const fromAccId = op.fromAccountId?._id || op.fromAccountId;
          const toAccId = op.toAccountId?._id || op.toAccountId;
          const accId = acc._id.toString();

          if (fromAccId && String(fromAccId) === accId) {
            factBalance -= absAmt;
            forecastBalance -= absAmt;
          }
          if (toAccId && String(toAccId) === accId) {
            factBalance += absAmt;
            forecastBalance += absAmt;
          }
        } else {
          // Regular operations
          const opAccId = op.accountId?._id || op.accountId;
          if (!opAccId || String(opAccId) !== acc._id.toString()) return;

          const amt = Number(op.amount) || 0;
          if (op.isWithdrawal || op.type === 'expense') {
            factBalance -= absAmt;
            forecastBalance -= absAmt;
          } else if (op.type === 'income') {
            factBalance += amt;
            forecastBalance += amt;
          }
        }
      });

      // For forecast balance, also include future operations
      // (This is a simplified version - full forecast would need future operations)
      // For now, forecastBalance = factBalance (can be enhanced later)

      return {
        name: acc.name || '—',
        currentBalance: factBalance,
        futureBalance: forecastBalance,
        factBalance: factBalance,
        forecastBalance: forecastBalance,
        isExcluded: Boolean(acc.isExcluded),
        hidden: Boolean(acc.isExcluded),
        excluded: Boolean(acc.isExcluded),
        company: acc.companyId?.name || null,
        individual: acc.individualId?.name || null,
        contractor: acc.contractorId?.name || null,
        _id: acc._id,
        companyId: acc.companyId?._id || acc.companyId,
        individualId: acc.individualId?._id || acc.individualId,
        contractorId: acc.contractorId?._id || acc.contractorId,
      };
    });

    return accountsWithBalances;
  }

  /**
   * Get operations with date range and filters
   * @param {string} userId - User ID
   * @param {object} dateRange - Date range { start: Date, end: Date }
   * @param {object} options - Options
   * @returns {Promise<Array>} Array of normalized operations
   */
  async function getOperations(userId, dateRange, options = {}) {
    const { start, end } = dateRange;
    if (!start || !end) {
      throw new Error('Date range required: { start: Date, end: Date }');
    }

    const startDate = _kzStartOfDay(start);
    const endDate = _kzEndOfDay(end);

    // Query operations in date range
    const operations = await Event.find({
      userId,
      date: { $gte: startDate, $lte: endDate }
    })
      .populate('categoryId accountId companyId contractorId projectId individualId counterpartyIndividualId')
      .lean()
      .sort({ date: -1 });

    // Get accounts for intermediary check
    const accounts = await Account.find({ userId }).lean();

    // Normalize and filter operations
    const normalized = [];
    const todayStart = _kzStartOfDay(new Date());

    operations.forEach(op => {
      const opDate = new Date(op.date);
      const opTs = opDate.getTime();
      const baseTs = todayStart.getTime();

      // Determine operation kind
      let kind = null;
      if (op.isTransfer || op.type === 'transfer') {
        kind = 'transfer';
      } else if (op.isWithdrawal || op.type === 'withdrawal') {
        kind = 'withdrawal';
      } else if (op.type === 'income') {
        kind = 'income';
      } else if (op.type === 'expense') {
        kind = 'expense';
      }

      if (!kind) return;

      // Filter: Exclude inter-company ops, retail writeoffs, and intermediaries from income/expense
      if (kind === 'income' || kind === 'expense') {
        if (_isInterCompanyOp(op)) return;
        if (_isRetailWriteOff(op)) return;
        if (_isIntermediaryIndividual(op, accounts)) return;
        if (_isCreditIncome(op) && kind === 'income') return;
      }

      // Build normalized operation
      const normalizedOp = {
        kind,
        date: _fmtDateKZ(opDate),
        ts: opTs,
        amount: Number(op.amount || 0),
        project: op.projectId?.name || null,
        contractor: op.contractorId?.name || null,
        individual: op.individualId?.name || null,
        category: op.categoryId?.name || null,
        name: op.description || op.categoryId?.name || op.contractorId?.name || '—',
        source: 'database',
        // Include IDs for reference
        _id: op._id,
        accountId: op.accountId?._id || op.accountId,
        companyId: op.companyId?._id || op.companyId,
        categoryId: op.categoryId?._id || op.categoryId,
        contractorId: op.contractorId?._id || op.contractorId,
        projectId: op.projectId?._id || op.projectId,
        individualId: op.individualId?._id || op.individualId,
      };

      normalized.push(normalizedOp);
    });

    return normalized;
  }

  /**
   * Get companies list
   */
  async function getCompanies(userId) {
    const companies = await Company.find({ userId })
      .lean()
      .sort({ order: 1, name: 1 });
    return companies.map(c => c.name || '—').filter(Boolean);
  }

  /**
   * Get projects list
   */
  async function getProjects(userId) {
    const projects = await Project.find({ userId })
      .lean()
      .sort({ order: 1, name: 1 });
    return projects.map(p => p.name || '—').filter(Boolean);
  }

  /**
   * Get categories list
   */
  async function getCategories(userId) {
    const categories = await Category.find({ userId })
      .lean()
      .sort({ order: 1, name: 1 });
    return categories.map(c => c.name || '—').filter(Boolean);
  }

  /**
   * Get contractors list
   */
  async function getContractors(userId) {
    const contractors = await Contractor.find({ userId })
      .lean()
      .sort({ order: 1, name: 1 });
    return contractors.map(c => c.name || '—').filter(Boolean);
  }

  /**
   * Get individuals list
   */
  async function getIndividuals(userId) {
    const individuals = await Individual.find({ userId })
      .lean()
      .sort({ order: 1, name: 1 });
    return individuals.map(i => i.name || '—').filter(Boolean);
  }

  return {
    getAccounts,
    getOperations,
    getCompanies,
    getProjects,
    getCategories,
    getContractors,
    getIndividuals,
  };
};
