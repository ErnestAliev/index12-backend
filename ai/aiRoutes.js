// backend/ai/aiRoutes.js
// QUICK-only AI routes (deterministic, no LLM)

const express = require('express');

const AIROUTES_VERSION = 'quick-only-v1.0';

module.exports = function createAiRouter(deps) {
  const {
    mongoose,
    models,
    isAuthenticated,
    getCompositeUserId,
  } = deps;

  const { Event, Account, Company, Contractor, Individual, Project, Category } = models;

  const createDataProvider = require('./dataProvider');
  const dataProvider = createDataProvider({
    mongoose,
    Event,
    Account,
    Company,
    Contractor,
    Individual,
    Project,
    Category
  });

  const quickMode = require('./modes/quickMode');
  const createQuickJournalAdapter = require('./quickJournalAdapter');
  const quickJournalAdapter = createQuickJournalAdapter({ Event });
  const router = express.Router();

  const _applyRawSnapshotAccounts = (dbData, rawSnapshot) => {
    const rawAccounts = Array.isArray(rawSnapshot?.accounts) ? rawSnapshot.accounts : [];
    if (!rawAccounts.length) return;

    const mapped = rawAccounts
      .map((a) => {
        const id = a?._id || a?.id || a?.accountId;
        if (!id) return null;
        const isExcluded = !!(a?.isExcluded || a?.excluded || a?.excludeFromTotal || a?.excludedFromTotal);
        const isHidden = !!(a?.isHidden || a?.hidden || isExcluded);
        const currentBalance = Number(a?.balance ?? a?.currentBalance ?? 0);
        const futureBalance = Number(a?.futureBalance ?? currentBalance ?? 0);

        return {
          _id: String(id),
          name: a?.name || a?.accountName || `Счет ${String(id).slice(-4)}`,
          currentBalance: Number.isFinite(currentBalance) ? Math.round(currentBalance) : 0,
          futureBalance: Number.isFinite(futureBalance) ? Math.round(futureBalance) : 0,
          companyId: a?.companyId ? String(a.companyId) : null,
          isHidden,
          isExcluded,
        };
      })
      .filter(Boolean);

    if (!mapped.length) return;

    const openAccounts = mapped.filter((a) => !a.isHidden && !a.isExcluded);
    const hiddenAccounts = mapped.filter((a) => a.isHidden || a.isExcluded);

    const openCurrent = openAccounts.reduce((s, a) => s + (a.currentBalance || 0), 0);
    const openFuture = openAccounts.reduce((s, a) => s + (a.futureBalance || 0), 0);
    const hiddenCurrent = hiddenAccounts.reduce((s, a) => s + (a.currentBalance || 0), 0);
    const hiddenFuture = hiddenAccounts.reduce((s, a) => s + (a.futureBalance || 0), 0);

    dbData.accounts = mapped;
    dbData.totals = {
      open: { current: openCurrent, future: openFuture },
      hidden: { current: hiddenCurrent, future: hiddenFuture },
      all: { current: openCurrent + hiddenCurrent, future: openFuture + hiddenFuture }
    };
    dbData.accountsData = {
      accounts: mapped,
      openAccounts,
      hiddenAccounts,
      totals: dbData.totals,
      meta: {
        today: dbData?.meta?.today || '?',
        count: mapped.length,
        openCount: openAccounts.length,
        hiddenCount: hiddenAccounts.length
      }
    };
  };

  const _applyRawSnapshotCompanies = (dbData, rawSnapshot) => {
    const rawCompanies = Array.isArray(rawSnapshot?.companies) ? rawSnapshot.companies : [];
    if (!rawCompanies.length) return;

    const mapped = rawCompanies
      .map((c) => {
        const id = c?._id || c?.id;
        if (!id) return null;
        return {
          id: String(id),
          name: c?.name || `Компания ${String(id).slice(-4)}`,
          taxRegime: c?.taxRegime || 'simplified',
          taxPercent: (c?.taxPercent != null) ? c.taxPercent : 3,
          identificationNumber: c?.identificationNumber || null
        };
      })
      .filter(Boolean);

    if (!mapped.length) return;
    dbData.catalogs = dbData.catalogs || {};
    dbData.catalogs.companies = mapped;
  };

  const _formatTenge = (n) => {
    const num = Number(n || 0);
    const sign = num < 0 ? '- ' : '';
    try {
      return sign + new Intl.NumberFormat('ru-RU').format(Math.abs(Math.round(num))).split('\u00A0').join(' ') + ' ₸';
    } catch (_) {
      return sign + String(Math.round(Math.abs(num))) + ' ₸';
    }
  };

  const _isAiAllowed = (req) => {
    const AI_ALLOW_ALL = process.env.AI_ALLOW_ALL === 'true';
    if (AI_ALLOW_ALL) return true;

    const allowedEmails = (process.env.AI_ALLOW_EMAILS || '').split(',').map(e => e.trim()).filter(Boolean);
    const userEmail = req.user?.email || '';
    return allowedEmails.includes(userEmail);
  };

  router.get('/ping', (req, res) => {
    res.json({ ok: true, mode: 'quick-only', version: AIROUTES_VERSION });
  });

  router.post('/query', isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?._id || req.user?.id;
      const userIdStr = String(userId || '');
      if (!userIdStr) return res.status(401).json({ error: 'Пользователь не найден' });

      if (!_isAiAllowed(req)) {
        return res.status(402).json({ error: 'AI недоступен для вашего аккаунта' });
      }

      const qRaw = String(req.body?.message ?? '');
      const q = qRaw.trim();
      if (!q) return res.status(400).json({ error: 'Пустой запрос' });

      let effectiveUserId = userId;
      if (typeof getCompositeUserId === 'function') {
        try {
          effectiveUserId = await getCompositeUserId(req);
        } catch (_) {
          effectiveUserId = userId;
        }
      }

      const dataUserId = String(effectiveUserId || userId);
      const userIdsList = Array.from(
        new Set([effectiveUserId, req.user?.id || req.user?._id].filter(Boolean).map(String))
      );

      const workspaceId = req.user?.currentWorkspaceId || null;
      const requestIncludeHidden = req?.body?.includeHidden === true;
      const requestVisibleAccountIds = Array.isArray(req?.body?.visibleAccountIds)
        ? req.body.visibleAccountIds
        : null;

      const dbData = await dataProvider.buildDataPacket(userIdsList, {
        includeHidden: requestIncludeHidden,
        visibleAccountIds: requestVisibleAccountIds,
        dateRange: req?.body?.periodFilter || null,
        workspaceId,
        now: req?.body?.asOf || null,
        snapshot: req?.body?.snapshot || null,
      });

      // Quick buttons must be consistent with Operations Editor source/rules.
      // Replace operation aggregates with journal-based dataset.
      if (String(req?.body?.source || '') === 'quick_button') {
        const quickJournal = await quickJournalAdapter.buildFromJournal({
          userId: dataUserId,
          periodFilter: req?.body?.periodFilter || null,
          asOf: req?.body?.asOf || null,
          categoriesCatalog: dbData?.catalogs?.categories || []
        });

        dbData.operations = quickJournal.operations;
        dbData.operationsSummary = quickJournal.summary;
        dbData.categorySummary = quickJournal.categorySummary;
        dbData.meta = {
          ...(dbData.meta || {}),
          periodStart: quickJournal?.meta?.periodStart || dbData?.meta?.periodStart || '?',
          periodEnd: quickJournal?.meta?.periodEnd || dbData?.meta?.periodEnd || '?'
        };

        // Accounts/companies for quick buttons must come strictly from frontend snapshot.
        _applyRawSnapshotAccounts(dbData, req?.body?.snapshot || null);
        _applyRawSnapshotCompanies(dbData, req?.body?.snapshot || null);
      }

      const quickResponse = quickMode.handleQuickQuery({
        query: q.toLowerCase(),
        dbData,
        snapshot: req?.body?.snapshot || null,
        formatTenge: _formatTenge
      });

      if (quickResponse) return res.json({ text: quickResponse });

      return res.json({
        text: 'Режим QUICK: этот запрос не поддержан предустановками. Используйте запросы по счетам, доходам, расходам, переводам, компаниям, проектам, категориям, контрагентам или физлицам.'
      });
    } catch (error) {
      console.error('AI Quick Query Error:', error);
      return res.status(500).json({ error: 'Ошибка обработки запроса' });
    }
  });

  router.get('/version', (req, res) => {
    res.json({
      version: AIROUTES_VERSION,
      modes: {
        quick: 'modes/quickMode.js'
      },
      llm: false,
      deep: false,
      chat: false
    });
  });

  return router;
};
