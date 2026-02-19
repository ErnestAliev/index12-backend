#!/usr/bin/env node
// Seed/update CFO KB in MongoDB Atlas + create Vector Search index.

const path = require('path');
const mongoose = require('mongoose');
const dotenv = require('dotenv');
const { getDefaultKnowledgeEntries } = require('../utils/cfoKnowledgeBase');

dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const DB_URL = String(process.env.DB_URL || '').trim();
const OPENAI_KEY = String(process.env.OPENAI_KEY || process.env.OPENAI_API_KEY || '').trim();
const EMBED_MODEL = String(process.env.OPENAI_EMBED_MODEL || 'text-embedding-3-small').trim();
const COLLECTION = String(process.env.RAG_KB_COLLECTION || 'ai_cfo_knowledge').trim();
const VECTOR_INDEX = String(process.env.RAG_KB_VECTOR_INDEX || 'vector_index').trim();

const toDim = (model) => {
  const m = String(model || '').toLowerCase();
  if (m.includes('text-embedding-3-large')) return 3072;
  if (m.includes('text-embedding-3-small')) return 1536;
  if (m.includes('text-embedding-ada-002')) return 1536;
  return 1536;
};

const isAtlas = (uri) => String(uri || '').startsWith('mongodb+srv://');

async function createEmbedding(text) {
  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${OPENAI_KEY}`
    },
    body: JSON.stringify({
      model: EMBED_MODEL,
      input: String(text || '')
    })
  });

  if (!response.ok) {
    let detail = '';
    try {
      const data = await response.json();
      detail = String(data?.error?.message || '').trim();
    } catch (_) {
      // ignore parse issues
    }
    throw new Error(`Embedding API ${response.status}${detail ? `: ${detail}` : ''}`);
  }

  const data = await response.json();
  const vector = data?.data?.[0]?.embedding;
  if (!Array.isArray(vector) || vector.length === 0) {
    throw new Error('Embedding vector is empty');
  }
  return vector;
}

async function listSearchIndexes(collection, indexName) {
  if (typeof collection.listSearchIndexes === 'function') {
    try {
      const rows = await collection.listSearchIndexes(indexName).toArray();
      return Array.isArray(rows) ? rows : [];
    } catch (_) {
      // fallback below
    }
  }

  try {
    const rows = await collection.aggregate([{ $listSearchIndexes: {} }]).toArray();
    if (!indexName) return Array.isArray(rows) ? rows : [];
    return (Array.isArray(rows) ? rows : []).filter((r) => String(r?.name || '') === String(indexName));
  } catch (_) {
    return [];
  }
}

async function ensureVectorIndex(db, collection, indexName, dimensions) {
  const existing = await listSearchIndexes(collection, indexName);
  if (existing.length > 0) {
    return { created: false, reason: 'already_exists' };
  }

  const definition = {
    fields: [
      {
        type: 'vector',
        path: 'embedding',
        numDimensions: Number(dimensions),
        similarity: 'cosine'
      }
    ]
  };

  if (typeof collection.createSearchIndex === 'function') {
    try {
      await collection.createSearchIndex({
        name: indexName,
        type: 'vectorSearch',
        definition
      });
      return { created: true, reason: 'createSearchIndex' };
    } catch (err) {
      const msg = String(err?.message || err);
      if (/already exists/i.test(msg)) return { created: false, reason: 'already_exists_race' };
      throw err;
    }
  }

  await db.command({
    createSearchIndexes: collection.collectionName,
    indexes: [
      {
        name: indexName,
        type: 'vectorSearch',
        definition
      }
    ]
  });
  return { created: true, reason: 'db_command' };
}

async function main() {
  if (!DB_URL) throw new Error('DB_URL is not set');
  if (!isAtlas(DB_URL)) {
    throw new Error('DB_URL is not mongodb+srv:// (MongoDB Atlas required for Atlas Vector Search)');
  }
  if (!OPENAI_KEY) throw new Error('OPENAI_KEY/OPENAI_API_KEY is not set');

  const entries = getDefaultKnowledgeEntries();
  if (!entries.length) throw new Error('Knowledge base entries are empty');

  console.log(`[RAG] Connecting to MongoDB Atlas...`);
  await mongoose.connect(DB_URL, {
    serverSelectionTimeoutMS: 10000,
    socketTimeoutMS: 45000
  });
  console.log(`[RAG] Connected.`);

  try {
    const db = mongoose.connection.db;
    const collection = db.collection(COLLECTION);

    const now = new Date();
    const docs = [];
    for (const row of entries) {
      const sourceText = [
        row.title,
        row.advice,
        `tags: ${(row.tags || []).join(', ')}`
      ].join('\n');
      const embedding = await createEmbedding(sourceText);

      docs.push({
        id: row.id,
        title: row.title,
        advice: row.advice,
        tags: Array.isArray(row.tags) ? row.tags : [],
        source: 'default_seed',
        model: EMBED_MODEL,
        embedding,
        updatedAt: now
      });
      console.log(`[RAG] Embedded: ${row.id} (${embedding.length})`);
    }

    if (!docs.length) throw new Error('No docs to upsert');

    const operations = docs.map((doc) => ({
      updateOne: {
        filter: { id: doc.id },
        update: {
          $set: doc,
          $setOnInsert: { createdAt: now }
        },
        upsert: true
      }
    }));
    const wr = await collection.bulkWrite(operations, { ordered: false });
    console.log(`[RAG] Upserted docs. matched=${wr.matchedCount} modified=${wr.modifiedCount} upserted=${wr.upsertedCount}`);

    const dim = toDim(EMBED_MODEL);
    const idx = await ensureVectorIndex(db, collection, VECTOR_INDEX, dim);
    console.log(`[RAG] Vector index ${idx.created ? 'created' : 'exists'}: ${VECTOR_INDEX} (${dim} dims, ${idx.reason})`);

    const indexes = await listSearchIndexes(collection, VECTOR_INDEX);
    console.log(`[RAG] Search index visible: ${indexes.length > 0 ? 'yes' : 'no'} (name=${VECTOR_INDEX})`);

    console.log('[RAG] Done.');
  } finally {
    await mongoose.disconnect();
    console.log('[RAG] Disconnected.');
  }
}

main().catch((err) => {
  console.error(`[RAG] Failed: ${String(err?.message || err)}`);
  process.exit(1);
});

