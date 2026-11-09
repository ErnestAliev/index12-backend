'use strict';
// Explicitly run to provision an isolated sample tenant for OpenAI review.
// This never reads or copies any existing user's messages or financial data.
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, randomBytes } = require('node:crypto');
const { MongoClient, ObjectId } = require('mongodb');
const { hashPassword } = require('../team-chat/domain.cjs');
const { loginKey } = require('../team-chat/access.cjs');
const root = path.resolve(__dirname, '..'), file = path.join(root, '.plugin-review.local.json');

async function main() {
  if (fs.existsSync(file)) {
    console.log('Reviewer credentials already exist in the ignored private file; no changes made.');
    return;
  }
  const env = require('dotenv').parse(fs.readFileSync(path.join(root, '.env')));
  if (!env.DB_URL) throw new Error('DB_URL is required');
  const client = new MongoClient(env.DB_URL, { serverSelectionTimeoutMS: 15000 });
  const ownerId = new ObjectId(), workspaceId = new ObjectId(), createdAt = new Date();
  const suffix = randomBytes(5).toString('hex');
  const employees = ['employee', 'manager'].map(role => ({ id: randomUUID(), role, name: `OpenAI ${role} ${suffix}`, password: randomBytes(24).toString('base64url') }));
  const privateData = { createdAt, workspaceId: String(workspaceId), ownerId: String(ownerId), accounts: employees, sample: { ownTaskId: randomUUID(), otherTaskId: randomUUID(), messageId: randomUUID() }, loginUrl: 'https://api.index12.com/team-mcp/connect', mcpUrl: 'https://api.index12.com/team-mcp' };
  // Write once before provisioning so a retry cannot silently create more tenants.
  fs.writeFileSync(file, JSON.stringify(privateData, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  try {
    await client.connect();
    const db = client.db(), session = client.startSession();
    try {
      const members = await Promise.all(employees.map(async account => ({ _id: account.id, workspaceId: String(workspaceId), name: account.name, loginKey: loginKey(account.name), role: account.role, password: await hashPassword(account.password), status: 'active', requireInstalledApp: false, authEpoch: 0, version: 0, createdAt, createdBy: 'owner:' + ownerId, reviewFixture: 'index12-team-v1' })));
      const [employee, manager] = members;
      const task = { workspaceId: String(workspaceId), timing: 'none', dueDate: null, dueTime: null, parentTaskId: null, status: 'open', attachments: [], createdAt, updatedAt: createdAt, version: 0, creatorId: manager._id, creatorName: manager.name };
      await session.withTransaction(async () => {
        const options = { session };
        await db.collection('users').insertOne({ _id: ownerId, name: 'INDEX12 OpenAI Review Sample', email: 'index12-review-' + suffix + '@example.invalid', role: 'admin', ownerId: null, currentWorkspaceId: workspaceId, createdAt, reviewFixture: 'index12-team-v1' }, options);
        await db.collection('workspaces').insertOne({ _id: workspaceId, userId: String(ownerId), name: 'INDEX12 · OpenAI Review Sample', isDefault: true, sharedWith: [], isShared: false, createdAt, reviewFixture: 'index12-team-v1' }, options);
        await db.collection('teamChatMembers').insertMany(members, options);
        await db.collection('teamChatTasks').insertMany([
          { ...task, _id: privateData.sample.ownTaskId, description: 'Review the sample welcome report', assigneeIds: [employee._id], assignees: [{ actorId: employee._id, name: employee.name }] },
          { ...task, _id: privateData.sample.otherTaskId, description: 'Private manager sample task', assigneeIds: [manager._id], assignees: [{ actorId: manager._id, name: manager.name }] },
        ], options);
        await db.collection('teamChatMessages').insertMany([
          { _id: privateData.sample.messageId, workspaceId: String(workspaceId), authorId: manager._id, authorName: manager.name, kind: 'text', text: 'Welcome to the INDEX12 review workspace. The sample report is ready.', attachments: [], version: 0, createdAt, updatedAt: createdAt },
          { _id: randomUUID(), workspaceId: String(workspaceId), authorId: manager._id, authorName: manager.name, kind: 'text', text: 'Manager-only sample task detail.', taskId: privateData.sample.otherTaskId, attachments: [], version: 0, createdAt, updatedAt: createdAt },
        ], options);
      });
    } finally { await session.endSession(); }
    console.log('Created an isolated sample workspace and two reviewer accounts. Credentials saved privately; no existing data accessed.');
  } catch (error) {
    console.error('Reviewer provisioning failed. Keep the private file for diagnosis; do not create another tenant blindly.');
    throw error;
  } finally { await client.close(); }
}
main().catch(() => { process.exitCode = 1; });
