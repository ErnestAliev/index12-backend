const assert = require('node:assert/strict');
const { test } = require('node:test');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const vm = require('node:vm');
const { withDaySlotLock } = require('../utils/daySlotLock');

// Exercise the actual HTTP handlers with controlled database completion. Loading
// server.js itself would connect to the configured database and start a listener.
const source = readFileSync(resolve(__dirname, '../server.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
const dateKey = date => new Date(date).toISOString().slice(0, 10);

function loadHandler(method, endMarker, Event, route = '/api/events/:id') {
  let handler;
  const emitted = [];
  const rebuilds = [];
  const app = { [method]: (_path, ...handlers) => { handler = handlers.at(-1); } };
  const context = {
    app, Event, mongoose: { Types: { ObjectId: class {} } },
    checkWorkspacePermission: () => (_req, _res, next) => next(),
    canEdit: () => {}, canDelete: () => {},
    isAuthenticated: () => {},
    console: { log: () => {} },
    withDaySlotLock,
    getCompositeUserId: async () => 'owner',
    normalizeEventCategoryFields: data => data,
    getCurrentWorkspaceActorRole: () => 'admin',
    ensureManagerCanAccessOperationAccounts: async () => {},
    hasMeaningfulEventChanges: () => true,
    _getDateKey: dateKey, _getDayOfYear: () => 279,
    triggerContextPacketRebuildByDates: payload => rebuilds.push(payload),
    emitToWorkspace: (...args) => emitted.push(args),
  };
  const helperStart = source.indexOf('const getFirstFreeCellIndex =');
  const helperEnd = source.indexOf('const findCategoryByName =', helperStart);
  vm.runInNewContext(source.slice(helperStart, helperEnd) + '\nglobalThis.getFirstFreeCellIndex = getFirstFreeCellIndex;', context);
  const start = source.indexOf(`app.${method}('${route}'`);
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

function creationModel() {
  const rows = [{ _id: 'source', userId: 'owner', dateKey: '2026-10-09', cellIndex: 0 }];
  let sequence = 0;
  class Event {
    constructor(data) { Object.assign(this, data, { _id: `copy-${++sequence}` }); }
    async save() { await tick(); rows.push(this); }
    async populate() { return this; }
    static async find(filter) {
      return rows.filter(row => row.userId === filter.userId && row.dateKey === filter.dateKey);
    }
    static async findOne(filter) {
      return rows.find(row => Object.entries(filter).every(([key, value]) => row[key] === value));
    }
  }
  return { Event, rows };
}

function loadCreation(Event, route = '/api/events') {
  const end = route === '/api/events' ? '// 🟢 UPDATED: Use canEdit middleware' : "app.post('/api/import/operations'";
  const result = loadHandler('post', end, Event, route);
  result.req.body = { date: '2026-10-09T12:00:00Z', dateKey: '2026-10-09', amount: 100, type: 'income', cellIndex: 0 };
  return result;
}

test('creating a copy with the source slot reallocates it on the server', async () => {
  const { Event, rows } = creationModel();
  const { handler, req, res } = loadCreation(Event);
  await handler(req, res);
  assert.equal(res.statusCode, 201);
  assert.equal(res.body.cellIndex, 1);
  assert.deepEqual(rows.map(row => row.cellIndex), [0, 1]);
});

test('creating an operation preserves a requested slot when it is free', async () => {
  const { Event } = creationModel();
  const { handler, req, res } = loadCreation(Event);
  req.body.cellIndex = 4;
  await handler(req, res);
  assert.equal(res.statusCode, 201);
  assert.equal(res.body.cellIndex, 4);
});

test('simultaneous event and transfer copies cannot allocate the same slot', async () => {
  const { Event, rows } = creationModel();
  const event = loadCreation(Event);
  const transfer = loadCreation(Event, '/api/transfers');
  Object.assign(transfer.req.body, { fromAccountId: 'from', toAccountId: 'to', transferPurpose: 'internal' });
  await Promise.all([event.handler(event.req, event.res), transfer.handler(transfer.req, transfer.res)]);
  assert.equal(event.res.statusCode, 201);
  assert.equal(transfer.res.statusCode, 201);
  assert.deepEqual(rows.map(row => row.cellIndex), [0, 1, 2]);
});

test('personal transfer copies also allocate a free slot on the server', async () => {
  const { Event } = creationModel();
  const { handler, req, res } = loadCreation(Event, '/api/transfers');
  Object.assign(req.body, { fromAccountId: 'from', toAccountId: 'to', transferPurpose: 'personal', transferReason: 'personal_use' });
  await handler(req, res);
  assert.equal(res.statusCode, 201);
  assert.equal(res.body.cellIndex, 1);
});

test('split children share their parent chip slot instead of creating extra timeline slots', async () => {
  const { Event, rows } = creationModel();
  rows[0].isSplitParent = true;
  const { handler, req, res } = loadCreation(Event);
  Object.assign(req.body, { parentOpId: 'source', isSplitChild: true, cellIndex: 5 });
  await handler(req, res);
  assert.equal(res.statusCode, 201);
  assert.equal(res.body.cellIndex, 0);
});

test('a split child cannot borrow a parent slot from another day', async () => {
  const { Event, rows } = creationModel();
  rows[0].isSplitParent = true;
  const { handler, req, res } = loadCreation(Event);
  Object.assign(req.body, { parentOpId: 'source', isSplitChild: true, dateKey: '2026-10-10' });
  await handler(req, res);
  assert.equal(res.statusCode, 400);
  assert.equal(rows.length, 1);
});

test('a failed creation releases the day lock so the next copy can be saved', async () => {
  const { Event, rows } = creationModel();
  const save = Event.prototype.save;
  Event.prototype.save = async function () { throw new Error('Save failed'); };
  const failed = loadCreation(Event);
  await failed.handler(failed.req, failed.res);
  assert.equal(failed.res.statusCode, 400);
  Event.prototype.save = save;
  const next = loadCreation(Event);
  await next.handler(next.req, next.res);
  assert.equal(next.res.statusCode, 201);
  assert.equal(next.res.body.cellIndex, 1);
  assert.deepEqual(rows.map(row => row.cellIndex), [0, 1]);
});
