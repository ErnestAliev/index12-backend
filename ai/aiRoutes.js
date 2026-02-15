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

      const workspaceId = req.user?.currentWorkspaceId || null;
      const requestIncludeHidden = req?.body?.includeHidden === true;
      const requestVisibleAccountIds = Array.isArray(req?.body?.visibleAccountIds)
        ? req.body.visibleAccountIds
        : null;

      const dbData = await dataProvider.buildDataPacket(dataUserId, {
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
