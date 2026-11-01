"use strict";
const { AsyncLocalStorage } = require("node:async_hooks");
const { randomUUID } = require("node:crypto");
const {
  clone,
  diff,
  operationChanges,
  accountIds,
  operationType,
} = require("./changes.cjs");
function createHistoryEngine({ mongoose }) {
  const storage = new AsyncLocalStorage();
  const db = () => mongoose.connection.db,
    entries = () => db().collection("changeHistory");
  let indexes;
  async function ready() {
    if (!indexes)
      indexes = Promise.all([
        entries().createIndex({ workspaceId: 1, createdAt: -1, _id: -1 }),
        entries().createIndex({ actorId: 1, kind: 1, createdAt: -1, _id: -1 }),
      ]).catch((e) => {
        indexes = null;
        throw e;
      });
    await indexes;
  }
  const current = () => storage.getStore();
  const collection = (raw) =>
    new Proxy(raw, {
      get(target, key) {
        if (
          [
            "findOne",
            "updateOne",
            "insertOne",
            "insertMany",
            "deleteOne",
          ].includes(key)
        )
          return (...args) => {
            const i = ["updateOne"].includes(key) ? 2 : 1;
            args[i] = {
              ...args[i],
              ...(current()?.session ? { session: current().session } : {}),
            };
            return target[key](...args);
          };
        const v = target[key];
        return typeof v === "function" ? v.bind(target) : v;
      },
    });
  function defer(callback) {
    const ctx = current();
    if (!ctx) return false;
    ctx.notifications.push(callback);
    return true;
  }
  function capture(before, after) {
    const ctx = current();
    if (!ctx) return;
    const id = String(after?._id || before?._id);
    const previous = ctx.operations.get(id);
    ctx.operations.set(id, {
      before: previous ? previous.before : clone(before),
      after: clone(after),
    });
  }
  function plugin(schema) {
    schema.pre("save", async function () {
      if (!current()) return;
      this.$locals.historyBefore = this.isNew
        ? null
        : await this.constructor.findById(this._id).lean();
    });
    schema.post("save", function (doc) {
      if (current())
        capture(
          this.$locals.historyBefore,
          doc.toObject({ depopulate: true, flattenObjectIds: true }),
        );
    });
    for (const name of [
      "findOneAndUpdate",
      "updateOne",
      "updateMany",
      "findOneAndDelete",
      "deleteOne",
      "deleteMany",
    ]) {
      schema.pre(name, { query: true, document: false }, async function () {
        if (!current()) return;
        let query = this.model.find(this.getFilter());
        if (!["updateMany", "deleteMany"].includes(name))
          query = query.limit(1);
        this._historyBefore = await query.lean();
      });
      schema.post(
        name,
        { query: true, document: false },
        async function (result) {
          if (!current() || !this._historyBefore) return;
          if (name.includes("Delete") || name.startsWith("delete")) {
            if (result?.deletedCount === 0 || result === null) return;
            for (const row of this._historyBefore) capture(row, null);
          } else {
            if (result?.matchedCount === 0 || result === null) return;
            for (const row of this._historyBefore) {
              const after = await this.model.findById(row._id).lean();
              if (after) capture(row, after);
            }
          }
        },
      );
    }
    schema.post("insertMany", function (docs) {
      if (current())
        for (const doc of docs)
          capture(
            null,
            doc.toObject
              ? doc.toObject({ depopulate: true, flattenObjectIds: true })
              : doc,
          );
    });
  }
  function captureTemplate(before, patch) {
    const ctx = current();
    if (!ctx || ctx.undo) return;
    const prior = ctx.templates;
    ctx.templates = {
      before: prior ? prior.before : clone(before),
      after: { ...clone(before), ...clone(patch) },
    };
  }
  async function flush(ctx) {
    if (ctx.undo) return;
    const req = ctx.req,
      actorId = String(req.user.id || req.user._id),
      workspaceId = String(req.user.currentWorkspaceId || ""),
      createdAt = new Date(),
      batchId = randomUUID();
    const base = {
      actorId,
      actorName: req.user.name || req.user.email || "",
      workspaceId,
      createdAt,
      batchId,
      undoneAt: null,
    };
    const rows = [],
      changes = [...ctx.operations.values()],
      seenGroups = new Set();
    for (const item of changes) {
      const op = item.after || item.before;
      if (!op) continue;
      if (
        op.isSplitChild &&
        changes.some(
          (x) => String((x.before || x.after)?._id) === String(op.parentOpId),
        )
      )
        continue;
      let snapshots = [item];
      if (!item.after && op.transferGroupId) {
        if (seenGroups.has(op.transferGroupId)) continue;
        seenGroups.add(op.transferGroupId);
        snapshots = changes.filter(
          (x) => !x.after && x.before?.transferGroupId === op.transferGroupId,
        );
      } else if (op.isSplitParent && (!item.before || !item.after))
        snapshots = [
          item,
          ...changes.filter(
            (x) => String((x.before || x.after)?.parentOpId) === String(op._id),
          ),
        ];
      const common = {
        ...base,
        kind: "operation",
        operationId: String(op._id),
        operationType: operationType(op),
        storageUserId: String(op.userId),
        accountIds: accountIds(
          snapshots.flatMap((x) => [x.before, x.after].filter(Boolean)),
        ),
        snapshot: clone(op),
      };
      if (!item.before || !item.after)
        rows.push({
          ...common,
          _id: randomUUID(),
          action: item.after ? "created" : "deleted",
          snapshots,
        });
      else
        for (const change of operationChanges(item.before, item.after))
          rows.push({
            ...common,
            _id: randomUUID(),
            action: "updated",
            change,
          });
    }
    if (ctx.templates) {
      const { before, after } = ctx.templates;
      for (const id of new Set([
        ...before.themes.map((t) => t.id),
        ...after.themes.map((t) => t.id),
      ])) {
        const old = before.themes.find((t) => t.id === id),
          next = after.themes.find((t) => t.id === id);
        if (!old || !next) {
          rows.push({
            ...base,
            _id: randomUUID(),
            kind: "setting",
            target: "template",
            targetId: id,
            label: next?.name || old?.name,
            action: next ? "created" : "deleted",
            before: old || null,
            after: next || null,
          });
          continue;
        }
        const fields = diff(
          { name: old.name, config: old.config },
          { name: next.name, config: next.config },
        );
        for (const change of fields)
          rows.push({
            ...base,
            _id: randomUUID(),
            kind: "setting",
            target: "template",
            targetId: id,
            label: next.name,
            action: "updated",
            changes: [change],
          });
      }
      if (before.activeThemeId !== after.activeThemeId)
        rows.push({
          ...base,
          _id: randomUUID(),
          kind: "setting",
          target: "activeTemplate",
          label: "Выбранная тема",
          action: "updated",
          before: before.activeThemeId,
          after: after.activeThemeId,
          beforeName:
            before.themes.find((t) => t.id === before.activeThemeId)?.name ||
            "По умолчанию",
          afterName:
            after.themes.find((t) => t.id === after.activeThemeId)?.name ||
            "По умолчанию",
        });
    }
    if (rows.length) await entries().insertMany(rows, { session: ctx.session });
  }
  function wrapHandler(handler) {
    return async (req, res, next) => {
      if (current() || req.method === "GET") return handler(req, res, next);
      let body,
        code = 200,
        ctx;
      const originalBody = clone(req.body),
        lockId = String(
          req.user?.currentWorkspaceId || req.user?.id || req.user?._id || "",
        );
      if (!req.user || !lockId) return handler(req, res, next);
      try {
        await ready();
        try {
          await db()
            .collection("changeHistoryLocks")
            .updateOne(
              { _id: lockId },
              { $setOnInsert: { revision: 0 } },
              { upsert: true },
            );
        } catch (e) {
          if (e.code !== 11000) throw e;
        }
        await mongoose.connection.transaction(
          async (session) => {
            await db()
              .collection("changeHistoryLocks")
              .updateOne(
                { _id: lockId },
                { $inc: { revision: 1 } },
                { session },
              );
            req.body = clone(originalBody);
            body = undefined;
            code = 200;
            ctx = {
              req,
              session,
              operations: new Map(),
              templates: null,
              notifications: [],
              undo: req.historyUndo === true || req.path === "/import",
            };
            const buffered = Object.create(res);
            buffered.status = function (value) {
              code = value;
              return this;
            };
            buffered.json = function (value) {
              body = value;
              return this;
            };
            await storage.run(ctx, async () => {
              await handler(req, buffered, next);
              if (code >= 400)
                throw Object.assign(new Error("History transaction rejected"), {
                  responseBody: body,
                  statusCode: code,
                });
              await flush(ctx);
            });
          },
          {
            readPreference: "primary",
            readConcern: { level: "snapshot" },
            writeConcern: { w: "majority" },
          },
        );
        for (const callback of ctx.notifications) {
          try {
            callback();
          } catch (error) {
            console.error("Post-commit notification failed:", error.message);
          }
        }
        return res.status(code).json(body);
      } catch (error) {
        if (error.responseBody)
          return res.status(error.statusCode).json(error.responseBody);
        if (next) return next(error);
        return res.status(error.statusCode || 500).json({
          message: error.message || "Не удалось сохранить изменение.",
        });
      }
    };
  }
  return {
    current,
    plugin,
    wrapHandler,
    captureTemplate,
    collection,
    defer,
    ready,
    entries,
  };
}
module.exports = { createHistoryEngine };
