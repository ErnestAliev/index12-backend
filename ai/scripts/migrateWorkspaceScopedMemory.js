#!/usr/bin/env node
/* eslint-disable no-console */

const path = require('path');
const mongoose = require('mongoose');
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });

async function ensureWorkspaceField(db, collectionName) {
  await db.collection(collectionName).updateMany(
    { workspaceId: { $exists: false } },
    { $set: { workspaceId: null } }
  );
}

async function dropIndexIfExists(db, collectionName, matchFn) {
  const indexes = await db.collection(collectionName).indexes();
  for (const idx of indexes) {
    if (!idx?.name) continue;
    if (!matchFn(idx)) continue;
    await db.collection(collectionName).dropIndex(idx.name);
    console.log(`[drop] ${collectionName}.${idx.name}`);
  }
}

async function main() {
  if (!process.env.DB_URL) {
    throw new Error('DB_URL missing');
  }

  await mongoose.connect(process.env.DB_URL);
  const db = mongoose.connection.db;

  await ensureWorkspaceField(db, 'ai_glossary');
  await ensureWorkspaceField(db, 'ai_user_profiles');

  await dropIndexIfExists(db, 'ai_glossary', (idx) => {
    const key = idx?.key || {};
    return idx.unique === true
      && key.userId === 1
      && key.term === 1
      && !Object.prototype.hasOwnProperty.call(key, 'workspaceId');
  });

  await dropIndexIfExists(db, 'ai_user_profiles', (idx) => {
    const key = idx?.key || {};
    return idx.unique === true
      && key.userId === 1
      && !Object.prototype.hasOwnProperty.call(key, 'workspaceId');
  });

  await db.collection('ai_glossary').createIndex(
    { userId: 1, workspaceId: 1, term: 1 },
    { unique: true, background: true }
  );
  console.log('[create] ai_glossary (userId, workspaceId, term) unique');

  await db.collection('ai_user_profiles').createIndex(
    { userId: 1, workspaceId: 1 },
    { unique: true, background: true }
  );
  console.log('[create] ai_user_profiles (userId, workspaceId) unique');

  await mongoose.disconnect();
  console.log('Done');
}

main().catch(async (err) => {
  console.error('Migration failed:', err?.message || err);
  try { await mongoose.disconnect(); } catch (_) { }
  process.exit(1);
});

