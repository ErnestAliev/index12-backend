// backend/ai/aiRoutes.js
// Hybrid AI routes:
// - quick_button -> deterministic quick mode
// - chat         -> LLM agent with journal packet context

const express = require('express');

const AIROUTES_VERSION = 'hybrid-v2.0';

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

  const _buildLlmContext = (body = {}) => {
    const journalPacket = (body?.journalPacket && typeof body.journalPacket === 'object')
      ? body.journalPacket
      : null;
    const snapshot = (body?.snapshot && typeof body.snapshot === 'object')
      ? body.snapshot
      : null;

    return {
      periodFilter: body?.periodFilter || null,
      asOf: body?.asOf || null,
      journalPacket,
      snapshot: snapshot
        ? {
            accounts: Array.isArray(snapshot.accounts) ? snapshot.accounts : [],
            companies: Array.isArray(snapshot.companies) ? snapshot.companies : []
          }
        : null
    };
  };

  const _detectStatusScope = (questionRaw = '') => {
    const q = String(questionRaw || '').toLowerCase();

    const hasBoth = /(факт\s*и\s*план|план\s*и\s*факт|сравн|разниц|оба|вместе|включая план)/i.test(q);
    if (hasBoth) return 'both';

    const hasPlan = /(план|планируем|прогноз|ожида|будет|предстоит|заплан)/i.test(q);
    if (hasPlan) return 'plan';

    const hasFact = /(факт|исполнено|уже|составил|составили|получили|поступил|поступило|потратили|потрачено|за прошед)/i.test(q);
    if (hasFact) return 'fact';

    // Default for business questions: factual results.
    return 'fact';
  };

  const _applyStatusScopeToContext = (context, statusScopeHint) => {
    const scoped = {
      ...(context || {}),
      statusScopeHint: statusScopeHint || 'fact'
    };

    const jp = scoped?.journalPacket;
    if (!jp || !Array.isArray(jp.operations)) return scoped;

    const opsFact = jp.operations.filter((op) => String(op?.status || '') === 'Исполнено');
    const opsPlan = jp.operations.filter((op) => String(op?.status || '') === 'План');

    const nextPacket = {
      ...jp,
      operationsFact: opsFact,
      operationsPlan: opsPlan
    };

    if (statusScopeHint === 'fact') {
      nextPacket.operations = opsFact;
    } else if (statusScopeHint === 'plan') {
      nextPacket.operations = opsPlan;
    }

    scoped.journalPacket = nextPacket;
    return scoped;
  };

  const _callLlmAgent = async ({ question, context }) => {
    const apiKey = process.env.OPENAI_API_KEY;
    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';

    if (!apiKey) {
      return {
        ok: false,
        status: 503,
        text: 'AI временно недоступен: отсутствует OPENAI_API_KEY.'
      };
    }

    const systemPrompt = [
      'Ты AI-ассистент финансовой системы INDEX12.',
      'Отвечай только на русском языке.',
      'Главный источник данных: journal_packet_json (если есть).',
      'Статусы: "Исполнено" = факт, "План" = план.',
      'Поле status_scope_hint обязательно к исполнению:',
      '- fact: использовать только операции со статусом "Исполнено".',
      '- plan: использовать только операции со статусом "План".',
      '- both: показывать факт и план раздельно.',
      'Всегда различай факт и план в расчётах.',
      'Если пользователь не просил объединять — показывай факт и план раздельно.',
      'Не придумывай числа и факты, которых нет в данных.',
      'Если данных недостаточно — прямо укажи, чего не хватает.',
      'Формат денег: 8 490 000 ₸ (пробелы между тысячами, знак ₸ в конце числа).',
      'Не используй формат 8,490,000 и не используй KZT.',
      'Пиши в обычном тексте, без markdown-разметки: не используй *, **, #, ```.',
      'Ответ делай понятным и коротким, с ключевыми цифрами по запросу пользователя.'
    ].join(' ');

    const userContent = [
      `Вопрос пользователя:\n${question}`,
      '',
      `journal_packet_json:\n${JSON.stringify(context?.journalPacket || null, null, 2)}`,
      '',
      `status_scope_hint:\n${String(context?.statusScopeHint || 'fact')}`,
      '',
      `snapshot_json:\n${JSON.stringify(context?.snapshot || null, null, 2)}`,
      '',
      `meta_json:\n${JSON.stringify({ periodFilter: context?.periodFilter || null, asOf: context?.asOf || null }, null, 2)}`
    ].join('\n');

    let upstream;
    try {
      upstream = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify({
          model,
          temperature: 0.2,
          max_tokens: 1200,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userContent }
          ]
        })
      });
    } catch (error) {
      return {
        ok: false,
        status: 502,
        text: 'Ошибка сети при обращении к AI.',
        debug: { message: error?.message || String(error) }
      };
    }

    let payload = null;
    try {
      payload = await upstream.json();
    } catch (_) {
      payload = null;
    }

    if (!upstream.ok) {
      return {
        ok: false,
        status: upstream.status || 502,
        text: 'AI сервис вернул ошибку.',
        debug: payload
      };
    }

    const choice = payload?.choices?.[0] || null;
    const finishReason = choice?.finish_reason || null;
    const content = choice?.message?.content;
    const text = typeof content === 'string'
      ? content.trim()
      : Array.isArray(content)
        ? content.map((part) => String(part?.text || '')).join('').trim()
        : '';

    const _normalizeMoneyText = (raw) => {
      let out = String(raw || '');
      if (!out) return out;

      // Strip common markdown artifacts
      out = out.replace(/```[\s\S]*?```/g, (m) => m.replace(/```/g, ''));
      out = out.replace(/^\s*#{1,6}\s*/gm, '');
      out = out.replace(/\*\*(.*?)\*\*/g, '$1');
      out = out.replace(/\*(.*?)\*/g, '$1');
      out = out.replace(/`([^`]+)`/g, '$1');
      out = out.replace(/^\s*\*\s+/gm, '- ');

      // 8,490,000 -> 8 490 000
      out = out.replace(/\b\d{1,3}(?:,\d{3})+\b/g, (m) => m.replace(/,/g, ' '));
      // Replace textual currency marker
      out = out.replace(/\bKZT\b/gi, '₸');
      // Prefix currency -> suffix currency: ₸8 490 000 -> 8 490 000 ₸
      out = out.replace(/₸\s*([0-9][0-9\s]*(?:[.,][0-9]+)?)/g, (_, num) => `${String(num).trim()} ₸`);
      // Keep consistent spacing near currency symbol
      out = out.replace(/(\d)₸/g, '$1 ₸');
      out = out.replace(/\s{2,}/g, ' ');

      return out.trim();
    };

    if (text) {
      return {
        ok: true,
        status: 200,
        text: _normalizeMoneyText(text),
        debug: {
          model,
          finishReason,
          usage: payload?.usage || null
        }
      };
    }

    if (finishReason === 'length') {
      return {
        ok: true,
        status: 200,
        text: 'AI не успел завершить ответ (лимит генерации). Сузьте период или уточните вопрос.',
        debug: {
          model,
          finishReason,
          usage: payload?.usage || null
        }
      };
    }

    return {
      ok: true,
      status: 200,
      text: 'Нет ответа от AI.',
      debug: {
        model,
        finishReason,
        usage: payload?.usage || null
      }
    };
  };

  router.get('/ping', (req, res) => {
    res.json({ ok: true, mode: 'hybrid', version: AIROUTES_VERSION });
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

      const source = String(req?.body?.source || 'chat');
      const isQuickButton = source === 'quick_button';

      // Chat/source=chat must always go to LLM (no deterministic gate).
      if (!isQuickButton) {
        const statusScopeHint = _detectStatusScope(q);
        const baseContext = _buildLlmContext(req.body || {});
        const context = _applyStatusScopeToContext(baseContext, statusScopeHint);
        const llmResult = await _callLlmAgent({ question: q, context });
        const debugEnabled = req?.body?.debugAi === true;

        if (!llmResult.ok) {
          return res.status(llmResult.status || 500).json({
            error: llmResult.text,
            ...(debugEnabled ? { debug: llmResult.debug || null } : {})
          });
        }

        return res.json({
          text: llmResult.text,
          ...(debugEnabled ? { debug: llmResult.debug || null } : {})
        });
      }

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
      console.error('AI Query Error:', error);
      return res.status(500).json({ error: 'Ошибка обработки запроса' });
    }
  });

  router.get('/version', (req, res) => {
    res.json({
      version: AIROUTES_VERSION,
      modes: {
        quick: 'modes/quickMode.js',
        chat: 'openai chat completions'
      },
      llm: true,
      deep: false,
      chat: true
    });
  });

  return router;
};
