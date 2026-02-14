#!/usr/bin/env node
// ai/scripts/backfillContextPackets.js
// Backfills monthly ai_context_packets from historical events

const path = require('path');
const mongoose = require('mongoose');
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });

const createDataProvider = require('../dataProvider');
const createContextPacketService = require('../contextPacketService');
const { buildContextPacketPayload } = require('../contextPacketBuilder');
const deepPrompt = require('../prompts/deepPrompt');

function parseArgs(argv) {
    const out = {};
    argv.forEach((arg) => {
        if (!arg.startsWith('--')) return;
        const [k, ...rest] = arg.slice(2).split('=');
        out[k] = rest.join('=');
    });
    return out;
}

function parseMonthRange(monthStr) {
    if (!/^\d{4}-\d{2}$/.test(String(monthStr || ''))) return null;
    const [yRaw, mRaw] = monthStr.split('-');
    const y = Number(yRaw);
    const m = Number(mRaw);
    if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return null;
    const start = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0, 0));
    const end = new Date(Date.UTC(y, m, 0, 23, 59, 59, 999));
    return { y, m, start, end };
}

function uFilter(raw) {
    if (!raw) return null;
    const variants = [String(raw)];
    try {
        if (mongoose.Types.ObjectId.isValid(raw)) {
            variants.push(new mongoose.Types.ObjectId(String(raw)));
        }
    } catch (_) { }
    return { $in: variants };
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    const dryRun = String(args.dryRun || '').toLowerCase() === 'true';
    const userArg = args.user || null;
    const workspaceArg = args.workspace || null;
    const monthArg = args.month || null; // YYYY-MM

    if (!process.env.DB_URL) {
        throw new Error('DB_URL missing in environment');
    }

    await mongoose.connect(process.env.DB_URL);

    const anySchema = new mongoose.Schema({}, { strict: false });
    const Event = mongoose.models.Event || mongoose.model('Event', anySchema, 'events');
    const Account = mongoose.models.Account || mongoose.model('Account', anySchema, 'accounts');
    const Company = mongoose.models.Company || mongoose.model('Company', anySchema, 'companies');
    const Contractor = mongoose.models.Contractor || mongoose.model('Contractor', anySchema, 'contractors');
    const Individual = mongoose.models.Individual || mongoose.model('Individual', anySchema, 'individuals');
    const Project = mongoose.models.Project || mongoose.model('Project', anySchema, 'projects');
    const Category = mongoose.models.Category || mongoose.model('Category', anySchema, 'categories');

    const packetSchema = new mongoose.Schema({
        workspaceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Workspace', default: null, index: true },
        userId: { type: String, required: true, index: true },
        periodKey: { type: String, required: true, index: true },
        periodStart: { type: Date, required: true },
        periodEnd: { type: Date, required: true },
        timezone: { type: String, default: 'Asia/Almaty' },
        version: { type: Number, default: 1 },
        prompt: { type: mongoose.Schema.Types.Mixed, default: {} },
        dictionary: { type: mongoose.Schema.Types.Mixed, default: {} },
        normalized: { type: mongoose.Schema.Types.Mixed, default: {} },
        derived: { type: mongoose.Schema.Types.Mixed, default: {} },
        dataQuality: { type: mongoose.Schema.Types.Mixed, default: {} },
        stats: { type: mongoose.Schema.Types.Mixed, default: {} }
    }, {
        collection: 'ai_context_packets',
        minimize: false,
        timestamps: true
    });
    packetSchema.index({ workspaceId: 1, userId: 1, periodKey: 1 }, { unique: true });
    const AiContextPacket = mongoose.models.AiContextPacket || mongoose.model('AiContextPacket', packetSchema);

    const dataProvider = createDataProvider({ mongoose, Event, Account, Company, Contractor, Individual, Project, Category });
    const contextPacketService = createContextPacketService({ AiContextPacket });

    const match = {};
    const uf = uFilter(userArg);
    if (uf) match.userId = uf;
    if (workspaceArg) {
        const wsVariants = [workspaceArg];
        try {
            if (mongoose.Types.ObjectId.isValid(workspaceArg)) {
                wsVariants.push(new mongoose.Types.ObjectId(String(workspaceArg)));
            }
        } catch (_) { }
        match.workspaceId = { $in: wsVariants };
    }

    const monthRange = parseMonthRange(monthArg);
    if (monthRange) {
        match.date = { $gte: monthRange.start, $lte: monthRange.end };
    }

    const groups = await Event.aggregate([
        { $match: match },
        {
            $project: {
                userId: 1,
                workspaceId: { $ifNull: ['$workspaceId', null] },
                y: { $year: '$date' },
                m: { $month: '$date' }
            }
        },
        {
            $group: {
                _id: { userId: '$userId', workspaceId: '$workspaceId', y: '$y', m: '$m' },
                operationsCount: { $sum: 1 }
            }
        },
        { $sort: { '_id.y': 1, '_id.m': 1 } }
    ]);

    if (!groups.length) {
        console.log('No historical months found for backfill.');
        await mongoose.disconnect();
        return;
    }

    let createdOrUpdated = 0;
    let skipped = 0;

    for (const g of groups) {
        const y = Number(g?._id?.y);
        const m = Number(g?._id?.m);
        const userIdRaw = g?._id?.userId;
        const workspaceRaw = g?._id?.workspaceId || null;
        if (!y || !m || !userIdRaw) continue;

        const periodStart = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0, 0));
        const periodEnd = new Date(Date.UTC(y, m, 0, 23, 59, 59, 999));
        const periodKey = `${String(y)}-${String(m).padStart(2, '0')}`;
        const userId = String(userIdRaw);

        const dbData = await dataProvider.buildDataPacket([userId], {
            includeHidden: true,
            visibleAccountIds: null,
            workspaceId: workspaceRaw || null,
            now: periodEnd.toISOString(),
            snapshot: null,
            dateRange: {
                mode: 'custom',
                customStart: periodStart.toISOString(),
                customEnd: periodEnd.toISOString()
            }
        });

        const payload = buildContextPacketPayload({
            dbData,
            promptText: deepPrompt,
            templateVersion: 'deep-v1',
            dictionaryVersion: 'dict-v1'
        });

        const existing = await contextPacketService.getMonthlyPacket({
            workspaceId: workspaceRaw || null,
            userId,
            periodKey
        });
        const existingHash = String(existing?.stats?.sourceHash || '');
        const nextHash = String(payload?.stats?.sourceHash || '');
        const changed = !existingHash || !nextHash || existingHash !== nextHash;

        if (!changed) {
            skipped += 1;
            console.log(`[skip] ${periodKey} user=${userId} ws=${workspaceRaw || 'null'} hash unchanged`);
            continue;
        }

        if (!dryRun) {
            await contextPacketService.upsertMonthlyPacket({
                workspaceId: workspaceRaw || null,
                userId,
                periodKey,
                periodStart,
                periodEnd,
                timezone: 'Asia/Almaty',
                ...payload
            });
        }

        createdOrUpdated += 1;
        console.log(`[upsert] ${periodKey} user=${userId} ws=${workspaceRaw || 'null'} ops=${payload.stats.operationsCount}`);
    }

    console.log(`Done. upserted=${createdOrUpdated}, skipped=${skipped}, dryRun=${dryRun ? 'yes' : 'no'}`);
    await mongoose.disconnect();
}

main().catch(async (err) => {
    console.error('Backfill failed:', err?.message || err);
    try { await mongoose.disconnect(); } catch (_) { }
    process.exit(1);
});
