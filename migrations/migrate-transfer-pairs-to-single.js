#!/usr/bin/env node

'use strict';

require('dotenv').config();
const mongoose = require('mongoose');

const DB_URL = process.env.DB_URL;

const argList = process.argv.slice(2);
const argSet = new Set(argList);

const showHelp = argSet.has('--help') || argSet.has('-h');
const isExecute = argSet.has('--execute');
const isDryRun = !isExecute || argSet.has('--dry-run');

const parseArgValue = (prefix) => {
  const hit = argList.find((arg) => arg.startsWith(prefix));
  if (!hit) return null;
  const value = hit.slice(prefix.length);
  return value ? value.trim() : null;
};

const limitRaw = parseArgValue('--limit=');
const limit = limitRaw && Number.isFinite(Number(limitRaw)) ? Math.max(0, Number(limitRaw)) : null;
const onlyGroupId = parseArgValue('--group=');

if (showHelp) {
  console.log('');
  console.log('Migration: legacy inter-company transfer pairs -> single transfer event');
  console.log('');
  console.log('Usage:');
  console.log('  node migrations/migrate-transfer-pairs-to-single.js --dry-run');
  console.log('  node migrations/migrate-transfer-pairs-to-single.js --execute');
  console.log('');
  console.log('Optional flags:');
  console.log('  --limit=N          Apply only first N planned actions (execute mode)');
  console.log('  --group=GROUP_ID   Process only one transferGroupId');
  console.log('');
  process.exit(0);
}

if (!DB_URL) {
  console.error('❌ Missing DB_URL in environment');
  process.exit(1);
}

const eventSchema = new mongoose.Schema({}, { strict: false, collection: 'events' });
const Event = mongoose.model('Event', eventSchema);

const toNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const toIdString = (value) => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object' && value._id) return String(value._id);
  return String(value);
};

const isModernTransfer = (doc) => doc?.isTransfer === true || doc?.type === 'transfer';

const pickMinCellIndex = (...values) => {
  const normalized = values
    .map((v) => toNumber(v, NaN))
    .filter((v) => Number.isFinite(v) && v >= 0);

  if (!normalized.length) return 0;
  return Math.min(...normalized);
};

const deriveIncomingLegacy = (legacyItems) => {
  return legacyItems.find((item) => toNumber(item.amount, 0) > 0 || item.type === 'income') || null;
};

const deriveOutgoingLegacy = (legacyItems) => {
  return legacyItems.find((item) => toNumber(item.amount, 0) < 0 || item.type === 'expense') || null;
};

const buildTransferUpdate = ({ incoming, outgoing, modern }) => {
  const sourceIncoming = incoming || modern || null;
  const sourceOutgoing = outgoing || modern || null;

  const amount = Math.max(
    Math.abs(toNumber(incoming?.amount, 0)),
    Math.abs(toNumber(outgoing?.amount, 0)),
    Math.abs(toNumber(modern?.amount, 0))
  );

  return {
    type: 'transfer',
    isTransfer: true,
    amount,
    transferPurpose: modern?.transferPurpose || 'inter_company',
    transferReason: modern?.transferReason || null,

    fromAccountId: sourceOutgoing?.accountId || sourceOutgoing?.fromAccountId || modern?.fromAccountId || null,
    toAccountId: sourceIncoming?.accountId || sourceIncoming?.toAccountId || modern?.toAccountId || null,

    fromCompanyId: sourceOutgoing?.companyId || sourceOutgoing?.fromCompanyId || modern?.fromCompanyId || null,
    toCompanyId: sourceIncoming?.companyId || sourceIncoming?.toCompanyId || modern?.toCompanyId || null,

    fromIndividualId: sourceOutgoing?.individualId || sourceOutgoing?.fromIndividualId || modern?.fromIndividualId || null,
    toIndividualId: sourceIncoming?.individualId || sourceIncoming?.toIndividualId || modern?.toIndividualId || null,

    categoryId: modern?.categoryId || sourceIncoming?.categoryId || sourceOutgoing?.categoryId || null,

    accountId: null,
    companyId: null,
    individualId: null,
    contractorId: null,
    counterpartyIndividualId: null,

    description:
      modern?.description ||
      sourceIncoming?.description ||
      sourceOutgoing?.description ||
      'Межкомпанийский перевод',

    cellIndex: pickMinCellIndex(sourceIncoming?.cellIndex, sourceOutgoing?.cellIndex, modern?.cellIndex)
  };
};

const getGroupKey = (doc) => `${toIdString(doc.userId)}::${toIdString(doc.transferGroupId)}`;

const createActionPlan = (groups) => {
  const actions = [];
  const stats = {
    totalGroups: groups.size,
    alreadyModern: 0,
    convertLegacyPair: 0,
    cleanupPartial: 0,
    skippedAmbiguous: 0
  };

  for (const [groupKey, items] of groups.entries()) {
    const modern = items.filter(isModernTransfer);
    const legacy = items.filter((item) => !isModernTransfer(item));

    if (onlyGroupId) {
      const groupId = groupKey.split('::')[1] || '';
      if (groupId !== onlyGroupId) continue;
    }

    if (modern.length === 1 && legacy.length === 0) {
      stats.alreadyModern += 1;
      continue;
    }

    if (modern.length === 0 && legacy.length === 2) {
      const incoming = deriveIncomingLegacy(legacy);
      const outgoing = deriveOutgoingLegacy(legacy);

      if (!incoming || !outgoing || toIdString(incoming._id) === toIdString(outgoing._id)) {
        stats.skippedAmbiguous += 1;
        continue;
      }

      actions.push({
        mode: 'convert_pair',
        groupKey,
        keeperId: incoming._id,
        deleteIds: [outgoing._id],
        update: buildTransferUpdate({ incoming, outgoing, modern: null })
      });
      stats.convertLegacyPair += 1;
      continue;
    }

    if (modern.length === 1 && legacy.length === 1) {
      const modernDoc = modern[0];
      const legacyDoc = legacy[0];

      const incoming = (toNumber(legacyDoc.amount, 0) > 0 || legacyDoc.type === 'income') ? legacyDoc : null;
      const outgoing = (toNumber(legacyDoc.amount, 0) < 0 || legacyDoc.type === 'expense') ? legacyDoc : null;

      actions.push({
        mode: 'cleanup_partial',
        groupKey,
        keeperId: modernDoc._id,
        deleteIds: [legacyDoc._id],
        update: buildTransferUpdate({ incoming, outgoing, modern: modernDoc })
      });
      stats.cleanupPartial += 1;
      continue;
    }

    stats.skippedAmbiguous += 1;
  }

  return { actions, stats };
};

const printSummary = ({ stats, actions }) => {
  console.log('');
  console.log('=== Transfer Migration Plan ===');
  console.log(`Groups scanned:        ${stats.totalGroups}`);
  console.log(`Already modern:        ${stats.alreadyModern}`);
  console.log(`Convert legacy pairs:  ${stats.convertLegacyPair}`);
  console.log(`Cleanup partial pairs: ${stats.cleanupPartial}`);
  console.log(`Skipped ambiguous:     ${stats.skippedAmbiguous}`);
  console.log(`Planned actions:       ${actions.length}`);
  console.log('');

  if (actions.length > 0) {
    const preview = actions.slice(0, 5);
    console.log('Preview actions:');
    preview.forEach((action, idx) => {
      console.log(
        `  ${idx + 1}. [${action.mode}] group=${action.groupKey.split('::')[1]} keeper=${toIdString(action.keeperId)} delete=${action.deleteIds.map(toIdString).join(',')}`
      );
    });
    if (actions.length > preview.length) {
      console.log(`  ... and ${actions.length - preview.length} more`);
    }
    console.log('');
  }
};

const run = async () => {
  const startedAt = Date.now();

  console.log('🔄 Connecting to MongoDB...');
  await mongoose.connect(DB_URL);
  console.log('✅ Connected');

  const baseQuery = {
    transferGroupId: { $exists: true, $nin: [null, ''] }
  };

  if (onlyGroupId) {
    baseQuery.transferGroupId = onlyGroupId;
  }

  const docs = await Event.find(baseQuery)
    .select([
      '_id',
      'userId',
      'transferGroupId',
      'type',
      'isTransfer',
      'amount',
      'accountId',
      'companyId',
      'individualId',
      'contractorId',
      'counterpartyIndividualId',
      'fromAccountId',
      'toAccountId',
      'fromCompanyId',
      'toCompanyId',
      'fromIndividualId',
      'toIndividualId',
      'categoryId',
      'description',
      'cellIndex',
      'transferPurpose',
      'transferReason',
      'createdAt'
    ].join(' '))
    .lean();

  console.log(`📥 Loaded events with transferGroupId: ${docs.length}`);

  const groups = new Map();
  docs.forEach((doc) => {
    const key = getGroupKey(doc);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(doc);
  });

  const { actions, stats } = createActionPlan(groups);
  printSummary({ stats, actions });

  if (isDryRun) {
    console.log('🧪 Dry-run mode. No changes applied.');
    return;
  }

  const runnableActions = limit !== null ? actions.slice(0, limit) : actions;
  if (limit !== null) {
    console.log(`⚙️  Execute limit applied: ${runnableActions.length}/${actions.length}`);
  }

  let updated = 0;
  let deleted = 0;
  let failed = 0;

  for (const action of runnableActions) {
    try {
      const updateRes = await Event.updateOne({ _id: action.keeperId }, { $set: action.update });

      if (updateRes.matchedCount === 0) {
        failed += 1;
        console.warn(`⚠️  Keeper not found for group ${action.groupKey}`);
        continue;
      }

      updated += updateRes.modifiedCount || 0;

      const deleteRes = await Event.deleteMany({ _id: { $in: action.deleteIds } });
      deleted += deleteRes.deletedCount || 0;
    } catch (error) {
      failed += 1;
      console.error(`❌ Failed group ${action.groupKey}:`, error.message);
    }
  }

  console.log('');
  console.log('=== Migration Result ===');
  console.log(`Actions attempted: ${runnableActions.length}`);
  console.log(`Rows updated:      ${updated}`);
  console.log(`Rows deleted:      ${deleted}`);
  console.log(`Failures:          ${failed}`);
  console.log(`Elapsed:           ${Math.round((Date.now() - startedAt) / 1000)}s`);
};

run()
  .catch((error) => {
    console.error('❌ Migration error:', error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await mongoose.connection.close();
      console.log('👋 Disconnected from MongoDB');
    } catch (_) {
      // no-op
    }
  });
