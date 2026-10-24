const ALL_PROJECTS = '__all_projects__';

function normalizeProjectScope(data = {}, existing = null) {
    const result = { ...data };
    const has = key => Object.prototype.hasOwnProperty.call(data, key);
    if (has('projectScope') && !['all', 'selected'].includes(data.projectScope)) {
        throw new Error('Некорректный режим проектов');
    }
    const scope = data.projectScope || (has('projectId') || has('projectIds') ? 'selected' : existing?.projectScope);
    if (scope === 'all') {
        if (existing?.isSplitChild || data.isSplitChild) throw new Error('Измените проекты в основном платеже, а не в его доле');
        Object.assign(result, { projectScope: 'all', projectId: null, projectIds: [], splitMeta: [], isSplitParent: false });
        if (existing?.isSplitParent || data.isSplitParent) result.excludeFromTotals = false;
    } else if (scope) {
        result.projectScope = 'selected';
    }
    if (result.projectId === ALL_PROJECTS || result.projectIds?.includes(ALL_PROJECTS)) {
        throw new Error('Для всех проектов используйте projectScope: all');
    }
    return result;
}

function equalProjectShares(projectIds, amount) {
    const ids = [...new Set(projectIds.map(value => String(value?._id || value)).filter(Boolean))];
    const minorUnits = Math.round(Math.abs(Number(amount)) * 100);
    if (!ids.length || !Number.isSafeInteger(minorUnits)) throw new Error('Некорректная сумма распределения');
    const base = Math.floor(minorUnits / ids.length);
    const remainder = minorUnits % ids.length;
    const sign = Number(amount) < 0 ? -1 : 1;
    return ids.map((projectId, index) => ({ projectId, amount: sign * (base + (index < remainder ? 1 : 0)) / 100 }));
}

module.exports = { ALL_PROJECTS, normalizeProjectScope, equalProjectShares };
