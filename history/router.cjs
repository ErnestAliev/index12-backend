"use strict";
const express = require("express");
const { randomUUID } = require("node:crypto");
const { isDeepStrictEqual } = require("node:util");
const { workspaceAccess } = require("../utils/operationAccess");
const { scopeQuery } = require("../mcp/finance.cjs");
const { clone, diff, revert, EDIT_KEYS } = require("./changes.cjs");
const PREFERENCES = new Set([
  "appearance",
  "theme",
  "dashboardLayout",
  "dashboardForecastState",
  "accountVisibilityMode",
  "universalEditModalTabOrder",
  "codexWindow",
  "homeWindow",
  "mobileWindow",
  "journalWindow",
]);
const fail = (message, statusCode = 409) => {
  throw Object.assign(new Error(message), { statusCode });
};
const validId = (id) => typeof id === "string" && /^[\w-]{1,100}$/.test(id);
function createHistoryRouter({
  mongoose,
  Event,
  Workspace,
  engine,
  isAuthenticated,
  handlers,
  ensureManagerCanAccessOperationAccounts,
  getFirstFreeCellIndex,
  emitToWorkspace,
  frontendUrl,
  allowedOrigins = [],
}) {
  const router = express.Router(),
    db = () => mongoose.connection.db,
    col = () => engine.collection(db().collection("changeHistory"));
  const allowed = new Set(
    [
      ...allowedOrigins,
      frontendUrl,
      "http://localhost:5173",
      "http://127.0.0.1:5173",
    ]
      .filter(Boolean)
      .map((v) => new URL(v).origin),
  );
  if (frontendUrl) {
    const u = new URL(frontendUrl);
    if (!["localhost", "127.0.0.1"].includes(u.hostname)) {
      u.hostname = u.hostname.startsWith("www.")
        ? u.hostname.slice(4)
        : `www.${u.hostname}`;
      allowed.add(u.origin);
    }
  }
  router.use(isAuthenticated, async (req, res, next) => {
    try {
      res.set("Cache-Control", "private, no-store");
      if (
        req.method !== "GET" &&
        req.headers.origin &&
        !allowed.has(req.headers.origin)
      )
        fail("Недопустимый источник запроса.", 403);
      req.historyActor = String(req.user.id || req.user._id);
      req.historyAccess = null;
      if (req.user.currentWorkspaceId) {
        const workspace = await Workspace.findById(
          req.user.currentWorkspaceId,
        ).lean();
        const access =
          workspace && workspaceAccess(workspace, req.historyActor);
        if (access) {
          req.cachedWorkspace = workspace;
          req.historyAccess = access;
          req.workspaceRole = access.role;
        }
      }
      await engine.ready();
      next();
    } catch (e) {
      next(e);
    }
  });
  const wrap = (fn) => engine.wrapHandler(fn);
  function canAccess(row, req, write = false) {
    if (row.kind === "setting") return row.actorId === req.historyActor;
    const access = req.historyAccess;
    if (!access || row.workspaceId !== String(req.user.currentWorkspaceId))
      return false;
    if (
      access.accountIds &&
      row.accountIds?.some((id) => !access.accountIds.includes(id))
    )
      return false;
    if (
      write &&
      (access.role === "analyst" ||
        (access.role === "manager" &&
          String(row.snapshot?.createdBy) !== req.historyActor))
    )
      return false;
    return true;
  }
  const safe = (row) => {
    const { _id, ...out } = row;
    delete out.pending;
    return { id: _id, ...out };
  };
  router.get("/", async (req, res, next) => {
    try {
      const clauses = [{ kind: "setting", actorId: req.historyActor }];
      if (req.historyAccess)
        clauses.push({
          kind: "operation",
          workspaceId: String(req.user.currentWorkspaceId),
        });
      const query = { $and: [{ $or: clauses }] },
        kind = req.query.kind;
      if (["operation", "setting"].includes(kind)) query.$and.push({ kind });
      if (
        ["income", "expense", "transfer", "withdrawal"].includes(
          req.query.operationType,
        )
      )
        query.$and.push({ operationType: req.query.operationType });
      const time = {};
      for (const [name, op] of [
        ["from", "$gte"],
        ["to", "$lte"],
      ])
        if (req.query[name]) {
          const date = new Date(req.query[name]);
          if (!Number.isFinite(date.getTime()))
            fail("Неверная дата фильтра.", 400);
          time[op] = date;
        }
      if (Object.keys(time).length) query.$and.push({ createdAt: time });
      const order = req.query.order === "asc" ? 1 : -1,
        limit = Math.max(1, Math.min(100, parseInt(req.query.limit, 10) || 50));
      if (req.query.cursor) {
        let c;
        try {
          c = JSON.parse(
            Buffer.from(String(req.query.cursor), "base64url").toString(),
          );
        } catch {
          fail("Неверная страница журнала.", 400);
        }
        const at = new Date(c.at);
        if (!validId(c.id) || !Number.isFinite(at.getTime()))
          fail("Неверная страница журнала.", 400);
        const op = order === 1 ? "$gt" : "$lt";
        query.$and.push({
          $or: [
            { createdAt: { [op]: at } },
            { createdAt: at, _id: { [op]: c.id } },
          ],
        });
      }
      const docs = await col()
        .find(query)
        .sort({ createdAt: order, _id: order })
        .limit(limit + 1)
        .toArray();
      const page = docs.slice(0, limit),
        items = [];
      for (const row of page) {
        if (!canAccess(row, req)) continue;
        const out = safe(row);
        out.canUndo = !row.undoneAt && canAccess(row, req, true);
        out.pending = !!(
          row.pending && new Date(row.pending.expiresAt) > new Date()
        );
        if (row.kind === "operation") {
          // Resolve names within this workspace, including for archived/deleted operations.
          const modelFields = {
            accountId: "Account",
            fromAccountId: "Account",
            toAccountId: "Account",
            companyId: "Company",
            individualId: "Individual",
            contractorId: "Contractor",
            counterpartyIndividualId: "Individual",
            categoryId: "Category",
            subcategoryId: "Subcategory",
            projectId: "Project",
            fromCompanyId: "Company",
            toCompanyId: "Company",
            fromIndividualId: "Individual",
            toIndividualId: "Individual",
          };
          out.details = {};
          out.entities = {};
          const scope = scopeQuery(row.storageUserId, row.workspaceId);
          await Promise.all(
            Object.entries(modelFields).map(async ([field, model]) => {
              if (!mongoose.models[model]) return;
              const ids = [
                ...new Set(
                  [
                    row.snapshot?.[field],
                    row.change?.before[field],
                    row.change?.after[field],
                    ...(field === "categoryId"
                      ? [
                          ...(row.snapshot?.categoryIds || []),
                          ...(row.change?.before.categoryIds || []),
                          ...(row.change?.after.categoryIds || []),
                        ]
                      : []),
                    ...(field === "projectId"
                      ? (row.snapshot?.splitMeta || []).map((s) => s.projectId)
                      : []),
                  ]
                    .filter(Boolean)
                    .map(String),
                ),
              ];
              if (!ids.length) return;
              const entities = await mongoose.models[model]
                .find({ _id: { $in: ids }, ...scope })
                .select("name")
                .lean();
              for (const entity of entities)
                out.entities[String(entity._id)] = entity.name;
              if (row.snapshot?.[field])
                out.details[field] =
                  out.entities[String(row.snapshot[field])] ||
                  String(row.snapshot[field]);
            }),
          );
        }
        items.push(out);
      }
      const last = page.at(-1);
      res.json({
        items,
        nextCursor:
          docs.length > limit && last
            ? Buffer.from(
                JSON.stringify({ at: last.createdAt, id: last._id }),
              ).toString("base64url")
            : null,
      });
    } catch (e) {
      next(e);
    }
  });
  router.post(
    "/preferences",
    wrap(async (req, res) => {
      engine.current().undo = true;
      const { id, key, before, after, deviceId, deviceLabel, touchedAt } =
        req.body || {};
      if (
        !validId(id) ||
        !validId(deviceId) ||
        !PREFERENCES.has(key) ||
        JSON.stringify({ before, after }).length > 100000
      )
        fail("Некорректное изменение настроек.", 400);
      const changes = diff(before, after);
      if (!changes.length) return res.json({ ok: true });
      if (changes.length > 500 || changes.some((c) => c.path.length > 12))
        fail("Слишком много параметров в одном изменении.", 400);
      const entry = {
        _id: id,
        kind: "setting",
        target: "preference",
        key,
        label: key,
        action: "updated",
        actorId: req.historyActor,
        actorName: req.user.name || req.user.email || "",
        workspaceId: String(req.user.currentWorkspaceId || ""),
        deviceId,
        deviceLabel: String(deviceLabel || "").slice(0, 30),
        changes,
        createdAt:
          Number.isFinite(touchedAt) &&
          touchedAt > Date.now() - 365 * 86400000 &&
          touchedAt < Date.now() + 300000
            ? new Date(touchedAt)
            : new Date(),
        undoneAt: null,
      };
      const existing = await col().findOne({ _id: id });
      if (existing) {
        if (existing.actorId !== req.historyActor)
          fail("Запись недоступна.", 403);
        return res.json({ ok: true });
      }
      await col().insertMany(
        changes.map((change, index) => ({
          ...entry,
          _id: index ? `${id}_${index}` : id,
          changes: [change],
        })),
      );
      res.status(201).json({ ok: true });
    }),
  );
  async function invoke(kind, req, body, id, current) {
    const inner = Object.create(req);
    inner.body = body;
    inner.params = { id };
    Object.defineProperty(inner, "query", {
      value: { cascadeTransfer: "true" },
    });
    inner.historyUndo = true;
    inner.mcpContext = {
      scope: scopeQuery(
        String(current.userId),
        String(req.user.currentWorkspaceId),
      ),
      expectedVersion: current.syncVersion || 0,
    };
    let result,
      status = 200;
    await handlers[kind](inner, {
      status(c) {
        status = c;
        return this;
      },
      json(value) {
        result = value;
        return this;
      },
    });
    if (status >= 400)
      fail(result?.message || "Не удалось отменить изменение.", status);
    return result;
  }
  async function validateReferences(req, row) {
    const names = {
      accountId: "Account",
      fromAccountId: "Account",
      toAccountId: "Account",
      companyId: "Company",
      individualId: "Individual",
      contractorId: "Contractor",
      counterpartyIndividualId: "Individual",
      categoryId: "Category",
      subcategoryId: "Subcategory",
      projectId: "Project",
      fromCompanyId: "Company",
      toCompanyId: "Company",
      fromIndividualId: "Individual",
      toIndividualId: "Individual",
    };
    const scope = scopeQuery(
      String(row.userId),
      String(req.user.currentWorkspaceId),
    );
    for (const [field, name] of Object.entries(names))
      if (
        row[field] &&
        mongoose.models[name] &&
        !(await mongoose.models[name].exists({ _id: row[field], ...scope }))
      )
        fail("Связанный счёт или справочник удалён. Сначала восстановите его.");
    for (const [model, ids] of [
      ["Category", row.categoryIds || []],
      ["Project", (row.splitMeta || []).map((s) => s.projectId)],
    ])
      for (const id of ids)
        if (
          mongoose.models[model] &&
          !(await mongoose.models[model].exists({ _id: id, ...scope }))
        )
          fail("Связанный справочник удалён. Сначала восстановите его.");
    await ensureManagerCanAccessOperationAccounts(req, {}, row);
  }
  async function undoOperation(row, req) {
    const scope = scopeQuery(row.storageUserId, row.workspaceId),
      current = await Event.findOne({ _id: row.operationId, ...scope }).lean();
    if (row.action === "updated") {
      if (!current) fail("Операция удалена. Сначала восстановите её.");
      await ensureManagerCanAccessOperationAccounts(req, {}, current);
      for (const key of row.change.keys)
        if (
          !isDeepStrictEqual(
            current[key] ?? null,
            row.change.after[key] ?? null,
          ) &&
          !isDeepStrictEqual(
            clone(current[key]) ?? null,
            row.change.after[key] ?? null,
          )
        )
          fail(
            "Это поле уже изменено после выбранной записи. Отмена не выполнена.",
          );
      if (
        (current.offsetIncomeId || current.relatedEventId) &&
        ["amount", "offset", "type"].includes(row.change.field)
      )
        fail(
          "Операция связана с другой записью. Сначала отмените связанные изменения.",
        );
      let patch = clone(row.change.before);
      if (row.change.field === "project" && patch.splitMeta?.length)
        patch.projectIds = patch.splitMeta.map((s) => s.projectId);
      if (row.change.field === "date") delete patch.cellIndex;
      await validateReferences(req, { ...current, ...patch });
      await invoke("update", req, patch, row.operationId, current);
    } else if (row.action === "created") {
      if (!current) fail("Операция уже удалена.");
      if (
        EDIT_KEYS.some(
          (k) =>
            !isDeepStrictEqual(
              clone(current[k]) ?? null,
              clone(row.snapshot[k]) ?? null,
            ),
        )
      )
        fail(
          "После создания операция была изменена. Сначала отмените последующие правки.",
        );
      for (const snapshot of row.snapshots || []) {
        if (
          !snapshot.after ||
          String(snapshot.after._id) === String(current._id)
        )
          continue;
        const child = await Event.findOne({
          _id: snapshot.after._id,
          ...scope,
        }).lean();
        if (
          !child ||
          EDIT_KEYS.some(
            (k) =>
              !isDeepStrictEqual(
                clone(child[k]) ?? null,
                clone(snapshot.after[k]) ?? null,
              ),
          )
        )
          fail(
            "После создания доля операции была изменена. Сначала отмените последующие правки.",
          );
      }
      const related = await Event.exists({
        ...scope,
        _id: { $ne: current._id },
        $or: [{ offsetIncomeId: current._id }, { relatedEventId: current._id }],
      });
      if (related)
        fail("У операции появились связанные записи. Сначала отмените их.");
      await invoke("delete", req, {}, row.operationId, current);
    } else {
      if (current)
        fail("Операция уже существует. Восстановление не выполнено.");
      const snapshots = row.snapshots.map((x) => x.before).filter(Boolean),
        cells = new Map();
      for (const snapshot of snapshots) {
        if (await Event.exists({ _id: snapshot._id }))
          fail("Одна из удалённых записей уже восстановлена.");
        await validateReferences(req, snapshot);
        for (const field of ["offsetIncomeId", "relatedEventId"])
          if (
            snapshot[field] &&
            !snapshots.some((s) => String(s._id) === String(snapshot[field])) &&
            !(await Event.exists({ _id: snapshot[field], ...scope }))
          )
            fail("Связанная операция удалена. Сначала восстановите её.");
        const data = {
          ...snapshot,
          syncVersion: (snapshot.syncVersion || 0) + 1,
          updatedBy: req.historyActor,
          updatedByRole: req.workspaceRole,
          updatedVia: "history",
        };
        const parent = snapshots.find(
          (s) => String(s._id) === String(snapshot.parentOpId),
        );
        const owner = parent || snapshot,
          slotKey = `${owner._id}:${snapshot.dateKey}`;
        if (!cells.has(slotKey))
          cells.set(
            slotKey,
            await getFirstFreeCellIndex(
              snapshot.dateKey,
              row.storageUserId,
              owner.cellIndex,
            ),
          );
        data.cellIndex = cells.get(slotKey);
        const restored = await new Event(data).save();
        emitToWorkspace(
          req,
          req.user.currentWorkspaceId,
          "operation_added",
          restored,
        );
      }
    }
  }
  async function undoTemplate(row, req) {
    const collection = engine.collection(
      db().collection("appearanceTemplateLibraries"),
    );
    const library = await collection.findOne({ _id: req.historyActor });
    if (!library) fail("Библиотека тем недоступна.");
    let themes = library.themes,
      activeThemeId = library.activeThemeId;
    if (row.target === "activeTemplate") {
      if (activeThemeId !== row.after) fail("Выбранная тема уже изменена.");
      if (row.before && !themes.some((t) => t.id === row.before))
        fail("Предыдущая тема удалена.");
      activeThemeId = row.before;
    } else {
      const item = themes.find((t) => t.id === row.targetId);
      let updated;
      if (row.action === "updated") {
        if (!item) fail("Тема удалена.");
        const restored = revert(
          { name: item.name, config: item.config },
          row.changes,
        );
        updated = {
          ...item,
          ...restored,
          revision: item.revision + 1,
          deviceRevisions: { ...item.deviceRevisions },
          updatedAt: new Date().toISOString(),
        };
        for (const device of new Set(
          row.changes
            .map((c) => c.path[2])
            .filter((v) => ["desktop", "tablet", "mobile"].includes(v)),
        ))
          updated.deviceRevisions[device]++;
      } else if (row.action === "created") {
        if (
          !item ||
          !isDeepStrictEqual(
            { name: item.name, config: item.config },
            { name: row.after.name, config: row.after.config },
          )
        )
          fail("Тема уже изменена или удалена.");
        themes = themes.filter((t) => t.id !== item.id);
        if (activeThemeId === item.id) activeThemeId = null;
      } else {
        if (
          item ||
          themes.length >= 40 ||
          themes.some(
            (t) =>
              t.name.toLocaleLowerCase() ===
              row.before.name.toLocaleLowerCase(),
          )
        )
          fail("Тема с таким именем уже существует или достигнут лимит тем.");
        updated = {
          ...row.before,
          revision: row.before.revision + 1,
          updatedAt: new Date().toISOString(),
        };
      }
      if (updated)
        themes = item
          ? themes.map((t) => (t.id === updated.id ? updated : t))
          : [...themes, updated];
    }
    const result = await collection.updateOne(
      { _id: req.historyActor, version: library.version },
      {
        $set: { themes, activeThemeId, selectionSet: true },
        $inc: { version: 1 },
      },
    );
    if (!result.modifiedCount)
      fail("Настройки темы уже изменились. Обновите журнал.");
  }
  router.post(
    "/:id/undo",
    wrap(async (req, res) => {
      const ctx = engine.current();
      ctx.undo = true;
      if (req.body?.confirm !== true)
        fail("Подтвердите отмену изменения.", 400);
      const row = await col().findOne({ _id: req.params.id });
      if (!row || !canAccess(row, req, true))
        fail("Нет доступа к отмене этого изменения.", 403);
      if (row.undoneAt) fail("Это изменение уже отменено.");
      if (row.pending && new Date(row.pending.expiresAt) > new Date())
        fail("Отмена этой записи уже выполняется.");
      if (row.target === "preference") {
        if (req.body.deviceId !== row.deviceId)
          fail(
            "Настройка относится к другому устройству. Откройте журнал на нём.",
          );
        revert(req.body.current, row.changes);
        const requestId = randomUUID();
        await col().updateOne(
          { _id: row._id, undoneAt: null },
          {
            $set: {
              pending: { requestId, expiresAt: new Date(Date.now() + 120000) },
            },
          },
        );
        return res.json({
          preference: {
            id: row._id,
            requestId,
            key: row.key,
            changes: row.changes,
          },
        });
      }
      if (row.kind === "operation") await undoOperation(row, req);
      else await undoTemplate(row, req);
      await col().updateOne(
        { _id: row._id, undoneAt: null },
        {
          $set: { undoneAt: new Date(), undoneBy: req.historyActor },
          $unset: { pending: "" },
        },
      );
      res.json({ ok: true, kind: row.kind });
    }),
  );
  router.post(
    "/:id/complete",
    wrap(async (req, res) => {
      engine.current().undo = true;
      const { requestId, applied, deviceId } = req.body || {};
      if (!validId(requestId) || typeof applied !== "boolean")
        fail("Некорректное подтверждение.", 400);
      const row = await col().findOne({
        _id: req.params.id,
        actorId: req.historyActor,
        target: "preference",
        deviceId,
      });
      if (!row) fail("Подтверждение отмены недоступно.", 403);
      if (row.completedRequestId === requestId) return res.json({ ok: true });
      if (row.undoneAt || row.pending?.requestId !== requestId)
        fail("Подтверждение отмены устарело.");
      await col().updateOne(
        { _id: row._id, "pending.requestId": requestId },
        {
          $unset: { pending: "" },
          $set: {
            completedRequestId: requestId,
            ...(applied
              ? { undoneAt: new Date(), undoneBy: req.historyActor }
              : {}),
          },
        },
      );
      res.json({ ok: true });
    }),
  );
  router.use((error, req, res, next) => {
    if (!error.statusCode)
      console.error("Change history request failed:", error);
    if (res.headersSent) return next(error);
    res.status(error.statusCode || 500).json({
      message: error.statusCode
        ? error.message
        : "Не удалось выполнить действие. Изменения не сохранены.",
    });
  });
  return router;
}
module.exports = { createHistoryRouter, PREFERENCES };
