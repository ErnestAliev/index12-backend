const toNum = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const normalizeText = (value) => String(value || '')
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^a-zа-я0-9\s]+/gi, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const uniqueNames = (items) => {
  const out = [];
  const seen = new Set();
  (Array.isArray(items) ? items : []).forEach((value) => {
    const raw = String(value || '').trim();
    const norm = normalizeText(raw);
    if (!raw || !norm || seen.has(norm)) return;
    seen.add(norm);
    out.push(raw);
  });
  return out.sort((a, b) => a.localeCompare(b, 'ru'));
};

const isPlaceholderEntityName = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return true;
  const norm = normalizeText(raw);
  if (!norm) return true;
  return /^без\s+/.test(norm);
};

const collectUniqueOperationValues = (state, key) => {
  const ops = Array.isArray(state?.operations) ? state.operations : [];
  return uniqueNames(
    ops
      .map((row) => String(row?.[key] || '').trim())
      .filter((value) => value && !isPlaceholderEntityName(value))
  );
};

const buildSnapshotSchemaAwareness = (state) => {
  const categories = collectUniqueOperationValues(state, 'category');
  const projects = collectUniqueOperationValues(state, 'project');
  const accounts = collectUniqueOperationValues(state, 'account');
  const counterparties = collectUniqueOperationValues(state, 'counterparty');
  const allNorm = Array.from(new Set(
    [...categories, ...projects, ...accounts, ...counterparties]
      .map((item) => normalizeText(item))
      .filter(Boolean)
  ));

  return {
    categories,
    projects,
    accounts,
    counterparties,
    allNorm,
    counts: {
      categories: categories.length,
      projects: projects.length,
      accounts: accounts.length,
      counterparties: counterparties.length,
      total: toNum(categories.length + projects.length + accounts.length + counterparties.length)
    }
  };
};

module.exports = {
  buildSnapshotSchemaAwareness,
  collectUniqueOperationValues,
  isPlaceholderEntityName
};
