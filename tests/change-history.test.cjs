"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const express = require("express");
const { AsyncLocalStorage } = require("node:async_hooks");
const { createHistoryEngine } = require("../history/engine.cjs");
const { createHistoryRouter } = require("../history/router.cjs");
const { diff, revert, operationChanges } = require("../history/changes.cjs");
const clone = structuredClone;
const get = (o, path) => path.split(".").reduce((v, k) => v?.[k], o);
function matches(row, q) {
  return Object.entries(q).every(([key, value]) => {
    if (key === "$and") return value.every((v) => matches(row, v));
    if (key === "$or") return value.some((v) => matches(row, v));
    const actual = get(row, key);
    if (value && typeof value === "object" && !(value instanceof Date))
      return Object.entries(value).every(([op, v]) =>
        op === "$ne"
          ? actual !== v
          : op === "$in"
            ? v.includes(actual)
            : op === "$gte"
              ? actual >= v
              : op === "$lte"
                ? actual <= v
                : op === "$lt"
                  ? actual < v
                  : op === "$gt"
                    ? actual > v
                    : false,
      );
    return actual instanceof Date ? +actual === +value : actual === value;
  });
}
function fixture() {
  let data = {
    changeHistory: [],
    changeHistoryLocks: [],
    appearanceTemplateLibraries: [],
    events: [],
  };
  const tx = new AsyncLocalStorage(),
    hooks = {};
  let failJournal = false;
  const db = {
    collection(name) {
      const list = () => data[name] || (data[name] = []);
      return {
        createIndex: async () => {},
        findOne: async (q) => clone(list().find((r) => matches(r, q)) || null),
        find: (q) => ({
          sort(order) {
            this.order = order;
            return this;
          },
          limit(n) {
            this.n = n;
            return this;
          },
          async toArray() {
            let rows = list().filter((r) => matches(r, q));
            for (const [k, d] of Object.entries(this.order || {}).reverse())
              rows = rows.toSorted((a, b) =>
                get(a, k) > get(b, k) ? d : get(a, k) < get(b, k) ? -d : 0,
              );
            return clone(rows.slice(0, this.n));
          },
        }),
        async updateOne(q, u, options) {
          let row = list().find((r) => matches(r, q));
          if (!row && options?.upsert) {
            row = { ...q, ...clone(u.$setOnInsert) };
            list().push(row);
          }
          if (!row) return { modifiedCount: 0 };
          if (u.$set) Object.assign(row, clone(u.$set));
          for (const k of Object.keys(u.$unset || {})) delete row[k];
          for (const [k, v] of Object.entries(u.$inc || {}))
            row[k] = (row[k] || 0) + v;
          return { modifiedCount: 1 };
        },
        async insertOne(row) {
          if (list().some((r) => r._id === row._id)) throw Error("duplicate");
          list().push(clone(row));
        },
        async insertMany(rows, options) {
          assert.equal(options.session, tx.getStore());
          if (failJournal) throw Error("journal unavailable");
          list().push(...clone(rows));
        },
      };
    },
  };
  const mongoose = {
    models: {},
    connection: {
      db,
      async transaction(fn) {
        const before = clone(data),
          session = { id: "transaction" };
        try {
          return await tx.run(session, () => fn(session));
        } catch (e) {
          data = before;
          throw e;
        }
      },
    },
  };
  const engine = createHistoryEngine({ mongoose });
  const schema = {
    pre(name, ...args) {
      hooks[`pre:${name}`] = args.at(-1);
    },
    post(name, ...args) {
      hooks[`post:${name}`] = args.at(-1);
    },
  };
  engine.plugin(schema);
  class Event {
    constructor(row) {
      Object.assign(this, clone(row));
      this.$locals = {};
      this.isNew = !data.events.some((r) => r._id === this._id);
    }
    toObject() {
      return Object.fromEntries(
        Object.entries(clone(this)).filter(
          ([key]) => !["$locals", "isNew"].includes(key),
        ),
      );
    }
    async save() {
      await hooks["pre:save"].call(this);
      const row = this.toObject(),
        i = data.events.findIndex((r) => r._id === this._id);
      if (i < 0) data.events.push(row);
      else data.events[i] = row;
      hooks["post:save"].call(this, this);
      return this;
    }
    static find(q) {
      return {
        limit(n) {
          this.n = n;
          return this;
        },
        lean: async function () {
          return clone(
            data.events.filter((row) => matches(row, q)).slice(0, this.n),
          );
        },
      };
    }
    static findOne(q) {
      return {
        lean: async () => clone(data.events.find((r) => matches(r, q)) || null),
      };
    }
    static findById(id) {
      return this.findOne({ _id: id });
    }
    static async exists(q) {
      return data.events.some((r) => matches(r, q));
    }
  }
  const Workspace = {
    findById: () => ({
      lean: async () => ({
        _id: "aaaaaaaaaaaaaaaaaaaaaaaa",
        userId: "owner",
        sharedWith: [
          {
            userId: "manager",
            role: "manager",
            accessibleAccountIds: ["cash"],
          },
          { userId: "analyst", role: "analyst" },
        ],
      }),
    }),
  };
  const emitted = [],
    invoked = [];
  const handlers = {
    update: async (req, res) => {
      invoked.push(clone(req.body));
      const row = data.events.find((r) => r._id === req.params.id);
      assert.equal(req.mcpContext.expectedVersion, row.syncVersion || 0);
      const saved = await new Event({
        ...row,
        ...req.body,
        syncVersion: (row.syncVersion || 0) + 1,
      }).save();
      res.json(saved.toObject());
    },
    delete: async (req, res) => {
      const query = { model: Event, getFilter: () => ({ _id: req.params.id }) };
      await hooks["pre:deleteOne"].call(query);
      data.events = data.events.filter((r) => r._id !== req.params.id);
      await hooks["post:deleteOne"].call(query, { deletedCount: 1 });
      res.json({ ok: true });
    },
  };
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    req.user = {
      id: req.headers.actor || "owner",
      currentWorkspaceId: req.headers.workspace || "aaaaaaaaaaaaaaaaaaaaaaaa",
      name: "Тест",
    };
    next();
  });
  app.use(
    "/api/change-history",
    createHistoryRouter({
      mongoose,
      Event,
      Workspace,
      engine,
      handlers,
      isAuthenticated: (req, res, next) =>
        req.headers.actor === "anonymous" ? res.sendStatus(401) : next(),
      ensureManagerCanAccessOperationAccounts: async () => {},
      getFirstFreeCellIndex: async (day, user, preferred) => {
        const used = data.events
          .filter((r) => r.dateKey === day && r.userId === user)
          .map((r) => r.cellIndex);
        let i = preferred;
        while (used.includes(i)) i++;
        return i;
      },
      emitToWorkspace: (...args) => {
        if (
          engine.current()?.undo &&
          /^operation_(added|updated|deleted)$/.test(args[2])
        )
          args[4] = { ...args[4], historyReceipt: true };
        if (!engine.defer(() => emitted.push(args))) emitted.push(args);
      },
      frontendUrl: "https://index12.com",
    }),
  );
  return {
    engine,
    Event,
    hooks,
    emitted,
    invoked,
    app,
    get data() {
      return data;
    },
    failJournal() {
      failJournal = true;
    },
  };
}
const operation = (extras = {}) => ({
  _id: "operation",
  userId: "owner",
  workspaceId: "aaaaaaaaaaaaaaaaaaaaaaaa",
  createdBy: "owner",
  type: "income",
  amount: 1400000,
  date: "2026-10-12T07:00:00.000Z",
  dateKey: "2026-285",
  dayOfYear: 285,
  cellIndex: 0,
  accountId: "cash",
  description: "Новая заметка",
  syncVersion: 2,
  ...extras,
});
const entry = (extras = {}) => ({
  _id: "entry",
  kind: "operation",
  workspaceId: "aaaaaaaaaaaaaaaaaaaaaaaa",
  operationId: "operation",
  operationType: "income",
  storageUserId: "owner",
  accountIds: ["cash"],
  actorId: "owner",
  createdAt: new Date("2026-11-01T12:00:00Z"),
  undoneAt: null,
  snapshot: operation(),
  action: "updated",
  change: operationChanges(operation({ amount: 1200000 }), operation())[0],
  ...extras,
});
async function serve(t, f) {
  const server = await new Promise((resolve) => {
    const server = f.app.listen(0, "127.0.0.1", () => resolve(server));
  });
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}/api/change-history`;
  return async (path = "", body, headers = {}) => {
    const r = await fetch(base + path, {
      method: body ? "POST" : "GET",
      headers: {
        "Content-Type": "application/json",
        origin: "https://index12.com",
        ...headers,
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    return { status: r.status, body: await r.json().catch(() => null) };
  };
}
test("leaf undo preserves later unrelated fields, sparse inheritance and null tombstones", () => {
  const before = {
    devices: {
      desktop: { light: { colors: { income: "#123456", expense: "#654321" } } },
      tablet: { light: { colors: { income: null } } },
    },
  };
  const after = clone(before);
  after.devices.desktop.light.colors.income = "#abcdef";
  const current = clone(after);
  current.devices.desktop.light.colors.expense = "#999999";
  const restored = revert(current, diff(before, after));
  assert.equal(restored.devices.desktop.light.colors.income, "#123456");
  assert.equal(restored.devices.desktop.light.colors.expense, "#999999");
  assert.equal(restored.devices.tablet.light.colors.income, null);
  current.devices.desktop.light.colors.income = "#000000";
  assert.throws(() => revert(current, diff(before, after)), {
    statusCode: 409,
  });
  assert.equal(revert({ a: [2], b: 3 }, diff({ a: [1] }, { a: [2] })).b, 3);
});
test("operations and audit commit together; failed audit rolls back data and suppresses notifications", async () => {
  const f = fixture(),
    req = {
      method: "POST",
      path: "/events",
      body: { amount: 1 },
      user: { id: "owner", currentWorkspaceId: "aaaaaaaaaaaaaaaaaaaaaaaa" },
    },
    res = {
      code: 200,
      status(c) {
        this.code = c;
        return this;
      },
      json(body) {
        this.body = body;
        return this;
      },
    };
  const handler = f.engine.wrapHandler(async (req, res) => {
    await new f.Event(operation()).save();
    f.engine.defer(() => f.emitted.push("saved"));
    assert.equal(f.emitted.length, 0);
    res.status(201).json({ ok: true });
  });
  f.failJournal();
  await handler(req, res);
  assert.equal(res.code, 500);
  assert.equal(f.data.events.length, 0);
  assert.equal(f.data.changeHistory.length, 0);
  assert.equal(f.emitted.length, 0);
  const g = fixture();
  await g.engine.wrapHandler(async (req, res) => {
    await new g.Event(operation()).save();
    g.engine.defer(() => g.emitted.push("saved"));
    res.status(201).json({ ok: true });
  })(req, res);
  assert.equal(res.code, 201);
  assert.equal(g.data.events.length, 1);
  assert.equal(g.data.changeHistory[0].action, "created");
  assert.equal(g.emitted.length, 1);
});
test("amount and date create independent rows; sync-only writes produce no history", async () => {
  const f = fixture();
  f.data.events.push(operation({ amount: 1200000 }));
  const req = {
      method: "PUT",
      path: "/events",
      body: {},
      user: { id: "owner", currentWorkspaceId: "aaaaaaaaaaaaaaaaaaaaaaaa" },
    },
    res = {
      status() {
        return this;
      },
      json() {
        return this;
      },
    };
  await f.engine.wrapHandler(async (req, res) => {
    await new f.Event(
      operation({
        date: "2026-10-13T07:00:00.000Z",
        dateKey: "2026-286",
        dayOfYear: 286,
      }),
    ).save();
    res.json({ ok: true });
  })(req, res);
  assert.deepEqual(
    f.data.changeHistory.map((r) => r.change.field),
    ["date", "amount"],
  );
  const count = f.data.changeHistory.length;
  await f.engine.wrapHandler(async (req, res) => {
    await new f.Event({
      ...f.data.events[0],
      syncVersion: 5,
      updatedVia: "http",
    }).save();
    res.json({ ok: true });
  })(req, res);
  assert.equal(f.data.changeHistory.length, count);
});
test("HTTP undo requires confirmation, changes only the amount, rejects stale and repeated undo", async (t) => {
  const f = fixture();
  f.data.events.push(operation());
  f.data.changeHistory.push(entry());
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", {})).status, 400);
  assert.equal(f.invoked.length, 0);
  const undone = await request("/entry/undo", { confirm: true });
  assert.equal(undone.status, 200);
  assert.equal(undone.body.changes.length, 1);
  assert.equal(undone.body.changes[0].before.amount, 1400000);
  assert.equal(undone.body.changes[0].after.amount, 1200000);
  assert.equal(f.data.events[0].amount, 1200000);
  assert.equal(f.data.events[0].description, "Новая заметка");
  assert.equal(f.data.events[0].dateKey, "2026-285");
  assert.deepEqual(Object.keys(f.invoked[0]), ["amount"]);
  assert.ok(f.data.changeHistory[0].undoneAt);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 409);
  f.data.changeHistory.push(entry({ _id: "stale" }));
  assert.equal((await request("/stale/undo", { confirm: true })).status, 409);
  assert.equal(f.invoked.length, 1);
});
test("workspace, role and account restrictions apply to listing and undo", async (t) => {
  const f = fixture();
  f.data.events.push(operation());
  f.data.changeHistory.push(
    entry(),
    entry({ _id: "foreign", workspaceId: "foreign" }),
    entry({ _id: "hidden", accountIds: ["private"] }),
  );
  const request = await serve(t, f);
  assert.equal(
    (await request("", undefined, { actor: "anonymous" })).status,
    401,
  );
  assert.deepEqual(
    (await request("", undefined, { actor: "manager" })).body.items.map(
      (r) => r.id,
    ),
    ["entry"],
  );
  assert.equal(
    (await request("/entry/undo", { confirm: true }, { actor: "manager" }))
      .status,
    403,
  );
  assert.equal(
    (await request("/entry/undo", { confirm: true }, { actor: "analyst" }))
      .status,
    403,
  );
  assert.equal((await request("/foreign/undo", { confirm: true })).status, 403);
  assert.equal(
    (
      await request(
        "/entry/undo",
        { confirm: true },
        { origin: "https://evil.example" },
      )
    ).status,
    403,
  );
});
test("deleted split operation restores original IDs, amounts and free cells on each original day", async (t) => {
  const f = fixture(),
    parent = operation({ isSplitParent: true }),
    a = operation({
      _id: "child-a",
      parentOpId: "operation",
      isSplitChild: true,
      amount: 600000,
    }),
    b = operation({
      _id: "child-b",
      parentOpId: "operation",
      isSplitChild: true,
      amount: 800000,
      dateKey: "2026-286",
    });
  f.data.events.push(
    operation({ _id: "occupied" }),
    operation({ _id: "occupied-next", dateKey: "2026-286" }),
  );
  f.data.changeHistory.push(
    entry({
      action: "deleted",
      snapshots: [parent, a, b].map((before) => ({ before, after: null })),
    }),
  );
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 200);
  assert.equal(f.data.events.length, 5);
  for (const id of ["operation", "child-a", "child-b"])
    assert.equal(f.data.events.find((r) => r._id === id).cellIndex, 1);
  assert.equal(f.data.events.find((r) => r._id === "child-b").amount, 800000);
  assert.equal(f.emitted.length, 4);
  assert.ok(f.emitted.slice(0, 3).every((args) => args[4].historyReceipt));
  const batch = f.emitted.find((args) => args[2] === "operations_reverted");
  assert.equal(batch[3].changes.length, 3);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 409);
  assert.equal(f.data.events.length, 5);
});
test("local settings use one device, selective paths and idempotent completion", async (t) => {
  const f = fixture(),
    request = await serve(t, f),
    change = {
      id: "setting-1",
      key: "homeWindow",
      deviceId: "desktop",
      before: { width: 30, height: 100 },
      after: { width: 40, height: 100 },
    };
  assert.equal((await request("/preferences", change)).status, 201);
  assert.equal((await request("/preferences", change)).status, 200);
  assert.equal(f.data.changeHistory.length, 1);
  assert.equal(
    (
      await request("/setting-1/undo", {
        confirm: true,
        deviceId: "tablet",
        current: change.after,
      })
    ).status,
    409,
  );
  assert.equal(
    (
      await request("/setting-1/undo", {
        confirm: true,
        deviceId: "desktop",
        current: { width: 50, height: 100 },
      })
    ).status,
    409,
  );
  const result = await request("/setting-1/undo", {
    confirm: true,
    deviceId: "desktop",
    current: { width: 40, height: 200 },
  });
  assert.equal(result.status, 200);
  assert.equal(result.body.preference.changes.length, 1);
  assert.deepEqual(
    revert({ width: 40, height: 200 }, result.body.preference.changes),
    { width: 30, height: 200 },
  );
  const receipt = {
    requestId: result.body.preference.requestId,
    deviceId: "desktop",
    applied: true,
  };
  assert.equal((await request("/setting-1/complete", receipt)).status, 200);
  assert.equal((await request("/setting-1/complete", receipt)).status, 200);
  assert.ok(f.data.changeHistory[0].undoneAt);
  assert.equal(
    (await request("/preferences", { ...change, id: "evil", key: "__proto__" }))
      .status,
    400,
  );
});
test("template undo preserves a newer mobile profile while reverting a desktop colour", async (t) => {
  const f = fixture(),
    before = {
      name: "Тема",
      config: {
        devices: {
          desktop: { light: { color: "#123456" } },
          mobile: { light: { color: "#aaaaaa" } },
        },
      },
    },
    after = clone(before);
  after.config.devices.desktop.light.color = "#654321";
  f.data.appearanceTemplateLibraries.push({
    _id: "owner",
    version: 2,
    activeThemeId: "template",
    themes: [
      {
        id: "template",
        ...clone(after),
        revision: 2,
        deviceRevisions: { desktop: 2, tablet: 1, mobile: 3 },
      },
    ],
  });
  f.data.appearanceTemplateLibraries[0].themes[0].config.devices.mobile.light.color =
    "#bbbbbb";
  f.data.changeHistory.push(
    entry({
      kind: "setting",
      target: "template",
      targetId: "template",
      changes: diff(before, after),
    }),
  );
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 200);
  const theme = f.data.appearanceTemplateLibraries[0].themes[0];
  assert.equal(theme.config.devices.desktop.light.color, "#123456");
  assert.equal(theme.config.devices.mobile.light.color, "#bbbbbb");
  assert.equal(theme.deviceRevisions.desktop, 3);
  assert.equal(theme.deviceRevisions.mobile, 3);
});

test("date undo preserves a newer amount and asks the normal handler to allocate a free cell", async (t) => {
  const f = fixture(),
    before = operation({
      date: "2026-10-11T07:00:00.000Z",
      dateKey: "2026-284",
      dayOfYear: 284,
      cellIndex: 5,
    }),
    after = operation();
  f.data.events.push(operation({ amount: 1700000 }));
  f.data.changeHistory.push(
    entry({
      change: operationChanges(before, after).find((c) => c.field === "date"),
    }),
  );
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 200);
  assert.equal(f.data.events[0].amount, 1700000);
  assert.equal(f.data.events[0].dateKey, "2026-284");
  assert.equal(f.invoked[0].cellIndex, 5);
});
test("undoing creation never deletes a split child edited later", async (t) => {
  const f = fixture(),
    parent = operation({ isSplitParent: true }),
    child = operation({
      _id: "child",
      isSplitChild: true,
      parentOpId: "operation",
      amount: 500000,
    });
  f.data.events.push(parent, { ...child, description: "Новая правка" });
  f.data.changeHistory.push(
    entry({
      action: "created",
      snapshot: parent,
      snapshots: [parent, child].map((after) => ({ before: null, after })),
    }),
  );
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 409);
  assert.equal(f.data.events.length, 2);
  assert.equal(f.data.changeHistory[0].undoneAt, null);
});
test("failed restoration rolls back every earlier restored split child and sends no socket event", async (t) => {
  const f = fixture(),
    parent = operation({ isSplitParent: true }),
    child = operation({
      _id: "child",
      isSplitChild: true,
      parentOpId: "operation",
    });
  f.data.events.push(child);
  f.data.changeHistory.push(
    entry({
      action: "deleted",
      snapshots: [parent, child].map((before) => ({ before, after: null })),
    }),
  );
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 409);
  assert.equal(f.data.events.length, 1);
  assert.equal(f.data.events[0]._id, "child");
  assert.equal(f.emitted.length, 0);
  assert.equal(f.data.changeHistory[0].undoneAt, null);
});

test("date undo returns the parent and only its shares in one committed receipt", async (t) => {
  const f = fixture(),
    parent = operation({ isSplitParent: true }),
    before = {
      ...parent,
      date: "2026-10-11T07:00:00.000Z",
      dateKey: "2026-284",
      dayOfYear: 284,
      cellIndex: 5,
    };
  const a = operation({
      _id: "share-a",
      parentOpId: "operation",
      isSplitChild: true,
      amount: 600000,
    }),
    b = operation({
      _id: "share-b",
      parentOpId: "operation",
      isSplitChild: true,
      amount: 800000,
      description: "Более поздняя заметка",
    }),
    unrelated = operation({ _id: "unrelated", amount: 42 });
  f.data.events.push(parent, a, b, unrelated);
  f.data.changeHistory.push(
    entry({
      snapshot: parent,
      change: operationChanges(before, parent).find((c) => c.field === "date"),
    }),
  );
  const request = await serve(t, f),
    result = await request("/entry/undo", { confirm: true });
  assert.equal(result.status, 200);
  assert.equal(result.body.changes.length, 3);
  assert.equal(f.data.changeHistory.length, 1);
  assert.equal(f.emitted.length, 1);
  assert.deepEqual(f.emitted[0][3].changes, result.body.changes);
  for (const id of ["operation", "share-a", "share-b"]) {
    const op = f.data.events.find((r) => r._id === id);
    assert.equal(op.dateKey, before.dateKey);
    assert.equal(op.cellIndex, 5);
  }
  assert.equal(f.data.events.find((r) => r._id === "share-a").amount, 600000);
  assert.equal(
    f.data.events.find((r) => r._id === "share-b").description,
    b.description,
  );
  assert.deepEqual(
    f.data.events.find((r) => r._id === "unrelated"),
    unrelated,
  );
});
test("newer movement of a share blocks date undo without touching parent or another operation", async (t) => {
  const f = fixture(),
    parent = operation({ isSplitParent: true }),
    child = operation({
      _id: "share",
      parentOpId: "operation",
      isSplitChild: true,
      dateKey: "2026-286",
    }),
    before = { ...parent, dateKey: "2026-284" };
  f.data.events.push(parent, child);
  f.data.changeHistory.push(
    entry({
      snapshot: parent,
      change: operationChanges(before, parent).find((c) => c.field === "date"),
    }),
  );
  const original = clone(f.data.events),
    request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 409);
  assert.deepEqual(f.data.events, original);
  assert.equal(f.emitted.length, 0);
  assert.equal(f.data.changeHistory[0].undoneAt, null);
});
for (const field of ["date", "amount"])
  test(`legacy transfer ${field} undo keeps both halves consistent and excludes unrelated records`, async (t) => {
    const f = fixture(),
      positive = operation({ isTransfer: true, transferGroupId: "legacy" }),
      negative = operation({
        _id: "out",
        type: "expense",
        isTransfer: true,
        transferGroupId: "legacy",
        amount: -1400000,
      }),
      unrelated = operation({ _id: "other" });
    const before =
      field === "date"
        ? {
            ...positive,
            date: "2026-10-11T07:00:00.000Z",
            dateKey: "2026-284",
            dayOfYear: 284,
            cellIndex: 5,
          }
        : { ...positive, amount: 1200000 };
    f.data.events.push(positive, negative, unrelated);
    f.data.changeHistory.push(
      entry({
        snapshot: positive,
        change: operationChanges(before, positive).find(
          (c) => c.field === field,
        ),
      }),
    );
    const request = await serve(t, f),
      result = await request("/entry/undo", { confirm: true });
    assert.equal(result.status, 200);
    assert.equal(result.body.changes.length, 2);
    assert.equal(f.emitted.length, 1);
    assert.equal(f.data.changeHistory.length, 1);
    const restored = f.data.events.find((r) => r._id === "operation"),
      other = f.data.events.find((r) => r._id === "out");
    assert.equal(restored.amount, -other.amount);
    assert.equal(restored.dateKey, other.dateKey);
    assert.equal(restored.cellIndex, other.cellIndex);
    assert.deepEqual(
      f.data.events.find((r) => r._id === "other"),
      unrelated,
    );
  });
test("undo creation responds with the deleted operation receipt after the transaction commits", async (t) => {
  const f = fixture(),
    op = operation();
  f.data.events.push(op);
  f.data.changeHistory.push(
    entry({ action: "created", snapshots: [{ before: null, after: op }] }),
  );
  const request = await serve(t, f),
    result = await request("/entry/undo", { confirm: true });
  assert.equal(result.status, 200);
  assert.equal(f.data.events.length, 0);
  assert.deepEqual(result.body.changes, [{ before: op, after: null }]);
  assert.equal(f.emitted.length, 1);
  assert.equal(f.emitted[0][2], "operations_reverted");
});

test("a share cannot be restored while its parent operation is still deleted", async (t) => {
  const f = fixture(),
    child = operation({
      _id: "share",
      isSplitChild: true,
      parentOpId: "missing-parent",
    });
  f.data.changeHistory.push(
    entry({
      action: "deleted",
      operationId: child._id,
      snapshot: child,
      snapshots: [{ before: child, after: null }],
    }),
  );
  const request = await serve(t, f);
  assert.equal((await request("/entry/undo", { confirm: true })).status, 409);
  assert.equal(f.data.events.length, 0);
  assert.equal(f.data.changeHistory[0].undoneAt, null);
  assert.equal(f.emitted.length, 0);
});
