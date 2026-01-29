// Quick answers handler (snapshot-driven)
// Exports only snapshot handler for now to isolate quick_button logic.

module.exports.handleSnapshot = function handleSnapshot({ req, res, formatTenge }) {
  try {
    const snap = req.body?.snapshot;
    const qRaw = String(req.body?.message || '').trim().toLowerCase();
    const includeHidden = !!req.body?.includeHidden; // kept for compatibility
    const fmt = typeof formatTenge === 'function'
      ? formatTenge
      : (n) => {
          const num = Number(n || 0);
          const sign = num < 0 ? '- ' : '';
          return sign + Math.abs(Math.round(num)).toLocaleString('ru-RU') + ' ₸';
        };

    const isAccountsQuery = /сч[её]т|счета|касс|баланс/.test(qRaw);
    const isCompaniesQuery = /компан/i.test(qRaw);

    if (!snap) return res.status(400).json({ text: 'snapshot not provided' });
    if (!isAccountsQuery && !isCompaniesQuery) {
      return res.json({ text: 'Этот тестовый маршрут поддерживает пока запросы про счета и компании.' });
    }

    const rawAccs = snap.accounts || snap.currentAccountBalances || [];
    const accounts = rawAccs.map(a => {
      const hiddenFlag = !!(
        a.isHidden ||
        a.hidden ||
        a.isExcluded ||
        a.excluded ||
        a.excludeFromTotal
      );
      return {
        _id: String(a._id || a.id || a.accountId || ''),
        name: a.name || a.accountName || 'Счет',
        companyId: a.companyId ? String(a.companyId) : null,
        currentBalance: Math.round(Number(a.currentBalance ?? a.balance ?? 0)),
        futureBalance: Math.round(Number(a.futureBalance ?? a.balance ?? 0)),
        isHidden: hiddenFlag,
      };
    }).filter(a => a._id);

    const openAccs = accounts.filter(a => !a.isHidden);
    const hiddenAccs = accounts.filter(a => a.isHidden);
    const sum = (arr, field) => arr.reduce((s, x) => s + Number(x[field] || 0), 0);

    // Companies
    if (isCompaniesQuery) {
      const companies = Array.isArray(snap.companies) ? snap.companies : [];
      const nameById = new Map(companies.map(c => [String(c._id || c.id), c.name || 'Без названия']));
      const accPool = includeHidden ? [...openAccs, ...hiddenAccs] : openAccs;
      const agg = new Map();
      const add = (acc) => {
        const cid = acc.companyId ? String(acc.companyId) : 'null';
        const name = cid === 'null' ? 'Без компании' : (nameById.get(cid) || 'Без названия');
        const cur = agg.get(cid) || { name, total: 0 };
        cur.total += acc.futureBalance;
        agg.set(cid, cur);
      };
      accPool.forEach(add);
      const rows = Array.from(agg.values()).sort((a, b) => b.total - a.total);
      const totalAll = rows.reduce((s, r) => s + r.total, 0);

      const lines = [];
      lines.push('Компании (snapshot, баланс счетов)');
      rows.forEach(r => lines.push(`${r.name}: ${fmt(r.total)}`));
      lines.push('');
      lines.push(`Итого: ${fmt(totalAll)}`);
      if (!includeHidden) lines.push('(Скрытые счета не включены)');
      return res.json({ text: lines.join('\n') });
    }

    // Accounts
    const totalOpen = sum(openAccs, 'futureBalance');
    const totalHidden = sum(hiddenAccs, 'futureBalance');
    const totalAll = totalOpen + totalHidden;

    const lines = [];
    lines.push('Счета (snapshot)');
    lines.push('');
    lines.push('Открытые:');
    if (openAccs.length) openAccs.forEach(acc => lines.push(`${acc.name}: ${fmt(acc.futureBalance)}`));
    else lines.push('- нет');
    lines.push('');
    lines.push('Скрытые:');
    if (hiddenAccs.length) hiddenAccs.forEach(acc => lines.push(`${acc.name} (скрыт): ${fmt(acc.futureBalance)}`));
    else lines.push('- нет');
    lines.push('');
    lines.push(`Итого открытые: ${fmt(totalOpen)}`);
    lines.push(`Итого скрытые: ${fmt(totalHidden)}`);
    lines.push(`Итого все: ${fmt(totalAll)}`);

    return res.json({ text: lines.join('\n') });
  } catch (err) {
    console.error('[AI SNAPSHOT ERROR]', err);
    return res.status(500).json({ text: `Ошибка snapshot: ${err.message}` });
  }
};
