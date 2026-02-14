// ai/contextPacketService.js
// Stores and retrieves monthly AI context packets (prompt + normalized data + derived metrics)

function _toObjectIdOrNull(raw, mongooseLike) {
    if (!raw) return null;
    try {
        if (mongooseLike?.Types?.ObjectId?.isValid(raw)) {
            return new mongooseLike.Types.ObjectId(String(raw));
        }
    } catch (_) { }
    return raw;
}

function _periodKeyFromDate(dateInput, timezone = 'Asia/Almaty') {
    const d = new Date(dateInput);
    if (Number.isNaN(d.getTime())) return null;
    const dtf = new Intl.DateTimeFormat('en-CA', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit'
    });
    const parts = dtf.formatToParts(d);
    const year = parts.find(p => p.type === 'year')?.value;
    const month = parts.find(p => p.type === 'month')?.value;
    if (!year || !month) return null;
    return `${year}-${month}`;
}

function _normalizePeriodKey(rawPeriodKey, periodStart, timezone = 'Asia/Almaty') {
    const v = String(rawPeriodKey || '').trim();
    if (/^\d{4}-\d{2}$/.test(v)) return v;
    return _periodKeyFromDate(periodStart, timezone);
}

module.exports = function createContextPacketService(deps = {}) {
    const { AiContextPacket } = deps;
    if (!AiContextPacket) {
        return {
            enabled: false,
            getMonthlyPacket: async () => null,
            listMonthlyPacketHeaders: async () => [],
            upsertMonthlyPacket: async () => null
        };
    }

    const mongooseLike = AiContextPacket.base || null;

    async function getMonthlyPacket({
        workspaceId = null,
        userId,
        periodKey
    }) {
        if (!userId || !periodKey) return null;
        const ws = _toObjectIdOrNull(workspaceId, mongooseLike);
        return AiContextPacket.findOne({
            workspaceId: ws || null,
            userId: String(userId),
            periodKey: String(periodKey)
        }).lean();
    }

    async function listMonthlyPacketHeaders({
        workspaceId = null,
        userId,
        limit = 24
    }) {
        if (!userId) return [];
        const ws = _toObjectIdOrNull(workspaceId, mongooseLike);
        const safeLimit = Math.max(1, Math.min(Number(limit) || 24, 120));
        return AiContextPacket.find({
            workspaceId: ws || null,
            userId: String(userId)
        })
            .select({ periodKey: 1, version: 1, updatedAt: 1, stats: 1, dataQuality: 1 })
            .sort({ periodKey: -1 })
            .limit(safeLimit)
            .lean();
    }

    async function upsertMonthlyPacket({
        workspaceId = null,
        userId,
        periodKey = null,
        periodStart,
        periodEnd,
        timezone = 'Asia/Almaty',
        prompt = {},
        dictionary = {},
        normalized = {},
        derived = {},
        dataQuality = {},
        stats = {}
    }) {
        if (!userId || !periodStart || !periodEnd) return null;

        const resolvedPeriodKey = _normalizePeriodKey(periodKey, periodStart, timezone);
        if (!resolvedPeriodKey) return null;

        const ws = _toObjectIdOrNull(workspaceId, mongooseLike);
        const userIdStr = String(userId);
        const filter = {
            workspaceId: ws || null,
            userId: userIdStr,
            periodKey: resolvedPeriodKey
        };

        const existing = await AiContextPacket.findOne(filter).select({ version: 1 }).lean();
        const nextVersion = (Number(existing?.version) || 0) + 1;

        const safeNormalized = normalized && typeof normalized === 'object' ? normalized : {};
        const safeStats = {
            operationsCount: Number(stats?.operationsCount) || (Array.isArray(safeNormalized.events) ? safeNormalized.events.length : 0),
            accountsCount: Number(stats?.accountsCount) || (Array.isArray(safeNormalized.accounts) ? safeNormalized.accounts.length : 0),
            sourceHash: String(stats?.sourceHash || '')
        };

        await AiContextPacket.findOneAndUpdate(
            filter,
            {
                $set: {
                    periodStart: new Date(periodStart),
                    periodEnd: new Date(periodEnd),
                    timezone,
                    version: nextVersion,
                    prompt: prompt && typeof prompt === 'object' ? prompt : {},
                    dictionary: dictionary && typeof dictionary === 'object' ? dictionary : {},
                    normalized: safeNormalized,
                    derived: derived && typeof derived === 'object' ? derived : {},
                    dataQuality: dataQuality && typeof dataQuality === 'object' ? dataQuality : {},
                    stats: safeStats
                }
            },
            { upsert: true, new: true, setDefaultsOnInsert: true }
        );

        return AiContextPacket.findOne(filter).lean();
    }

    return {
        enabled: true,
        getMonthlyPacket,
        listMonthlyPacketHeaders,
        upsertMonthlyPacket
    };
};
