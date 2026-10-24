#!/usr/bin/env node
'use strict';
// Operator-only provisioning. Never exposed as an MCP tool.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { MongoClient, ObjectId } = require('mongoose').mongo;
const { workspaceAccess } = require('../utils/operationAccess');
const { hashToken } = require('../utils/mcpOperations');

(async () => {
    const args = process.argv.slice(2);
    const root = path.resolve(__dirname, '..');
    const file = process.env.INDEX12_MCP_CONFIG || path.join(root, '.mcp.local.json');
    const config = JSON.parse(fs.readFileSync(file, 'utf8'));
    const option = args.indexOf('--api-url');
    const apiUrl = option >= 0 ? args[option + 1] : config.apiUrl;
    const modeOption = args.indexOf('--confirmation-mode');
    const requestedMode = modeOption >= 0 ? args[modeOption + 1] : config.confirmationMode || 'form';
    if (!['form','direct'].includes(requestedMode)) throw new Error('--confirmation-mode принимает form или direct.');
    if (!apiUrl) throw new Error('Укажите --api-url https://адрес-backend-INDEX12');
    const url = new URL(apiUrl);
    if (url.username || url.password || url.search || url.hash || (url.pathname !== '/' && url.pathname !== '')) throw new Error('Укажите корневой адрес backend без пути, параметров и учётных данных.');
    if (url.protocol !== 'https:' && !(url.protocol === 'http:' && ['localhost','127.0.0.1','[::1]'].includes(url.hostname))) throw new Error('Требуется HTTPS; HTTP допустим только для localhost.');
    const ping = await fetch(`${url.origin}/api/mcp/status`, { redirect: 'error', signal: AbortSignal.timeout(20000) });
    if (ping.status !== 401) throw new Error('Обновлённый API MCP ещё не доступен по этому адресу. Сначала разверните backend.');
    const actorId = config.actorId || config.ownerId;
    if (![actorId,config.ownerId,config.workspaceId].every(v => /^[a-f\d]{24}$/i.test(v || ''))) throw new Error('Проверьте ID в локальной конфигурации.');
    const envFile = path.join(root, '.env');
    const uri = process.env.INDEX12_MCP_DB_URL || (fs.existsSync(envFile) && require('dotenv').parse(fs.readFileSync(envFile)).DB_URL);
    if (!uri) throw new Error('Не настроена база INDEX12.');
    const client = new MongoClient(uri, { serverSelectionTimeoutMS: 15000 });
    try {
        await client.connect(); const db = client.db();
        const actor = await db.collection('users').findOne({ _id: new ObjectId(actorId) }, { projection: { _id: 1 } });
        const workspace = await db.collection('workspaces').findOne({ _id: new ObjectId(config.workspaceId), userId: config.ownerId });
        const access = actor && workspace ? workspaceAccess(workspace, actorId) : null;
        if (!access) throw new Error('У пользователя нет доступа к рабочей области.');
        const scopes = args.includes('--read-only') || access.role === 'analyst' ? ['read'] : ['read','create','update','entities:create','imports:execute'];
        const confirmationMode = scopes.includes('imports:execute') ? requestedMode : 'form';
        if (confirmationMode === 'direct') scopes.push('imports:direct');
        const token = `idx12_${crypto.randomBytes(32).toString('base64url')}`;
        const expiresAt = new Date(Date.now() + 90 * 86400000);
        await db.collection('mcpCredentials').insertOne({ _id: hashToken(token), actorId, ownerId: config.ownerId, workspaceId: config.workspaceId, scopes, confirmationMode, planSigningKey: crypto.randomBytes(32).toString('hex'), createdAt: new Date(), expiresAt, revokedAt: null, source: 'local_operator' });
        const temp = `${file}.tmp`;
        try {
            fs.writeFileSync(temp, JSON.stringify({ ...config, actorId, apiUrl: url.origin, apiToken: token, confirmationMode }, null, 2) + '\n', { mode: 0o600 });
            fs.chmodSync(temp, 0o600); fs.renameSync(temp, file);
        } catch (error) {
            await db.collection('mcpCredentials').updateOne({ _id: hashToken(token) }, { $set: { revokedAt: new Date() } });
            throw error;
        }
        if (config.apiToken) await db.collection('mcpCredentials').updateOne({ _id: hashToken(config.apiToken) }, { $set: { revokedAt: new Date() } });
        console.log(JSON.stringify({ configured: true, apiUrl: url.origin, role: access.role, scopes, confirmationMode, expiresAt: expiresAt.toISOString(), tokenStoredLocally: true }));
    } finally { await client.close(); }
})().catch(error => { console.error(error.name === 'MongoServerError' || /mongo|querySrv|ENOTFOUND|timed out/i.test(error.message) ? 'Не удалось подключиться к базе INDEX12.' : error.message); process.exitCode = 1; });
