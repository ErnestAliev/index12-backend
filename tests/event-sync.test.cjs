const assert = require('node:assert/strict');
const { test } = require('node:test');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const vm = require('node:vm');

// Exercise the actual HTTP handlers with controlled database completion. Loading
// server.js itself would connect to the configured database and start a listener.
const source = readFileSync(resolve(__dirname, '../server.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
const dateKey = date => new Date(date).toISOString().slice(0, 10);

function loadHandler(method, endMarker, Event) {
  let handler;
  const emitted = [];
  const rebuilds = [];
  const app = { [method]: (_path, ...handlers) => { handler = handlers.at(-1); } };
  const context = {
    app, Event, mongoose: { Types: { ObjectId: class {} } },
    checkWorkspacePermission: () => (_req, _res, next) => next(),
    canEdit: () => {}, canDelete: () => {},
    getCompositeUserId: async () => 'owner',
    normalizeEventCategoryFields: data => data,
    getCurrentWorkspaceActorRole: () => 'admin',
    ensureManagerCanAccessOperationAccounts: async () => {},
    hasMeaningfulEventChanges: () => true,
    _getDateKey: dateKey, _getDayOfYear: () => 279,
    triggerContextPacketRebuildByDates: payload => rebuilds.push(payload),
    emitToWorkspace: (...args) => emitted.push(args),
  };
  const start = source.indexOf(`app.${method}('/api/events/:id'`);
  const end = source.indexOf(endMarker, start);
  assert.ok(start >= 0 && end > start);
  vm.runInNewContext(source.slice(start, end), context);
  const req = {
    params: { id: 'operation' }, body: {}, workspaceRole: 'admin',
    user: { id: 'owner', currentWorkspaceId: 'workspace' },
  };
  const res = {
    statusCode: 200, body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; },
  };
  return { handler, req, res, emitted, rebuilds };
}

test('edit revisions are server-owned, atomic, and identical in HTTP and socket output', async () => {
  const before = { _id: 'operation', date: new Date('2026-10-06T12:00:00Z'), dateKey: '2026-10-06', amount: -100, syncVersion: 2 };
  let finishWrite;
  let update;
  const Event = {
    findOne: async () => before,
    findOneAndUpdate: async (_filter, mutation) => {
      update = mutation;
      await new Promise(resolve => { finishWrite = resolve; });
      return { ...before, ...mutation.$set, syncVersion: before.syncVersion + mutation.$inc.syncVersion, populate: async () => {} };
    },
  };
  const { handler, req, res, emitted } = loadHandler('put', '// 🟢 UPDATED: Allow managers', Event);
  req.body = { amount: -250, syncVersion: 999 };
  const saving = handler(req, res);
  await tick();
  assert.equal(emitted.length, 0);
  assert.equal(res.body, null);
  assert.equal(update.$inc.syncVersion, 1);
  assert.equal(update.$set.syncVersion, undefined);
  finishWrite();
  await saving;
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.syncVersion, 3);
  assert.equal(res.body.amount, -250);
  assert.equal(emitted[0][2], 'operation_updated');
  assert.equal(emitted[0][3].syncVersion, res.body.syncVersion);
  assert.deepEqual(Array.from(emitted[0][4].affectedDateKeys), ['2026-10-06']);
});

test('split deletion is announced only after children are removed and includes their IDs and dates', async () => {
  const parent = { _id: 'operation', date: new Date('2026-10-06T12:00:00Z'), isSplitParent: true };
  const children = [{ _id: 'child-a', date: new Date('2026-10-06T12:00:00Z') }, { _id: 'child-b', date: new Date('2026-10-07T12:00:00Z') }];
  let finishChildren;
  let childFilter;
  const Event = {
    findOne: async () => parent,
    deleteOne: async () => {},
    find: () => ({ select: () => ({ lean: async () => children }) }),
    deleteMany: async filter => {
      childFilter = filter;
      await new Promise(resolve => { finishChildren = resolve; });
    },
  };
  const { handler, req, res, emitted } = loadHandler('delete', "app.post('/api/transfers'", Event);
  const deleting = handler(req, res);
  await tick();
  assert.equal(emitted.length, 0);
  assert.equal(res.body, null);
  assert.equal(childFilter.userId, 'owner');
  finishChildren();
  await deleting;
  assert.equal(res.statusCode, 200);
  assert.equal(emitted[0][2], 'operation_deleted');
  assert.equal(emitted[0][3], 'operation');
  assert.deepEqual(Array.from(emitted[0][4].deletedOperationIds), ['operation', 'child-a', 'child-b']);
  assert.deepEqual(Array.from(emitted[0][4].affectedDateKeys), ['2026-10-06', '2026-10-07']);
});

test('deleting an already deleted operation succeeds without an extra broadcast', async () => {
  const { handler, req, res, emitted } = loadHandler('delete', "app.post('/api/transfers'", {
    findOne: async () => null,
  });
  await handler(req, res);
  assert.equal(res.statusCode, 200);
  assert.match(res.body.message, /Already deleted/);
  assert.equal(emitted.length, 0);
});

test('a database write failure produces an error without broadcasting a saved operation', async () => {
  const { handler, req, res, emitted } = loadHandler('put', '// 🟢 UPDATED: Allow managers', {
    findOne: async () => ({ _id: 'operation', date: new Date('2026-10-06T12:00:00Z') }),
    findOneAndUpdate: async () => { throw new Error('Write failed'); },
  });
  req.body = { amount: -250 };
  await handler(req, res);
  assert.equal(res.statusCode, 400);
  assert.equal(res.body.message, 'Write failed');
  assert.equal(emitted.length, 0);
});

test('legacy transfer deletion removes both halves in one scoped database call', async () => {
  const parent = { _id: 'operation', date: new Date('2026-10-06T12:00:00Z'), transferGroupId: 'legacy-group' };
  let deleted;
  const { handler, req, res, emitted } = loadHandler('delete', "app.post('/api/transfers'", {
    findOne: async () => parent,
    find: () => ({ select: () => ({ lean: async () => [parent, { ...parent, _id: 'second-half' }] }) }),
    deleteMany: async filter => { deleted = filter; },
    deleteOne: async () => { throw new Error('Transfer halves must not be deleted separately'); },
  });
  req.query = { cascadeTransfer: 'true' };
  await handler(req, res);
  assert.equal(res.statusCode, 200);
  assert.equal(deleted.transferGroupId, 'legacy-group');
  assert.equal(deleted.userId, 'owner');
  assert.deepEqual(Array.from(emitted[0][4].deletedOperationIds), ['operation', 'second-half']);
});

test('workspace broadcasts preserve anti-echo metadata alongside deleted IDs', () => {
  let broadcast;
  let excluded;
  const room = {
    except(id) { excluded = id; return this; },
    emit(...args) { broadcast = args; },
  };
  const context = {
    console: { log: () => {}, warn: () => {} },
    buildSocketMeta: req => ({ sourceSocketId: req.headers['x-socket-id'], sourceClientInstanceId: req.headers['x-client-instance-id'] }),
  };
  const start = source.indexOf('const emitToWorkspace =');
  const end = source.indexOf('const emitEntityEvent =', start);
  vm.runInNewContext(source.slice(start, end) + '\nglobalThis.broadcastToWorkspace = emitToWorkspace;', context);
  context.broadcastToWorkspace({
    io: { to: () => room }, headers: { 'x-socket-id': 'sender', 'x-client-instance-id': 'client' },
  }, 'workspace', 'operation_deleted', 'operation', { deletedOperationIds: ['operation', 'child'] });
  assert.equal(excluded, 'sender');
  assert.equal(broadcast[2].sourceSocketId, 'sender');
  assert.equal(broadcast[2].sourceClientInstanceId, 'client');
  assert.deepEqual(broadcast[2].deletedOperationIds, ['operation', 'child']);
});
