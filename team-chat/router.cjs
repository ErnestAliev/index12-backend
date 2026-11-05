"use strict";
const express = require("express");
const { EventEmitter } = require("node:events");
const { randomUUID, randomBytes, createHash } = require("node:crypto");
const { Types } = require("mongoose");
const {
  roles,
  activeStates,
  fail,
  text,
  phone,
  hashPassword,
  verifyPassword,
  decision,
  money,
  forecast,
} = require("./domain.cjs");
const {
  dateOnly,
  today,
  localInstant,
} = require("../business/task-domain.cjs");
const {
  safeName,
  fileMeta,
  fileBuffer,
  FILE_LIMIT,
  TOTAL_LIMIT,
} = require("../codex/files.cjs");
function createTeamChatRouter({
  db,
  Workspace,
  Company,
  Account,
  Category,
  Contractor,
  Subcategory,
  Project,
  allocateCell,
  emitToWorkspace,
  parseInvoice,
  Event,
  getWorkspaceStorageUserId,
  createOperation,
  history,
  frontendUrl,
  getBalances,
}) {
  const router = express.Router(),
    bus = new EventEmitter();
  bus.setMaxListeners(1000);
  const coll = (name) => db().collection("teamChat" + name);
  const sessions = require("./session.cjs").createChatSessions({ coll });
  const identity = (p) => ({
    workspaceId: p.workspaceId,
    name: p.workspace.name,
    actorId: p.actorId,
    displayName: p.name,
    role: p.role,
    status: p.status,
    owner: p.owner,
    roles,
  });
  const opts = () =>
    history?.current()?.session ? { session: history.current().session } : {};
  const wrap = (fn) => (req, res, next) =>
    Promise.resolve(fn(req, res, next)).catch(next);
  const allowedOrigins = new Set([
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5192",
  ]);
  const front = new URL(frontendUrl);
  allowedOrigins.add(front.origin);
  const alternate = new URL(front.origin);
  alternate.hostname = alternate.hostname.startsWith("www.")
    ? alternate.hostname.slice(4)
    : "www." + alternate.hostname;
  allowedOrigins.add(alternate.origin);
  frontendUrl = front.origin;
  let indexes;
  const ready = () =>
    (indexes ||= Promise.all([
      coll("Sessions").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      coll("Members").createIndex(
        { workspaceId: 1, phone: 1 },
        { unique: true },
      ),
      coll("Messages").createIndex({ workspaceId: 1, createdAt: -1, _id: -1 }),
      coll("Requests").createIndex({ workspaceId: 1, createdAt: -1 }),
      coll("Requests").createIndex({
        scope: 1,
        state: 1,
        "placement.dateKey": 1,
      }),
      coll("Requests").createIndex(
        { workspaceId: 1, sourceFileId: 1 },
        {
          unique: true,
          partialFilterExpression: { sourceFileId: { $type: "string" } },
        },
      ),
      coll("Requests").createIndex(
        { workspaceId: 1, sourceSha256: 1 },
        {
          unique: true,
          partialFilterExpression: { sourceSha256: { $type: "string" } },
        },
      ),
      coll("Files").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      coll("Attempts").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
    ]).catch((e) => {
      indexes = null;
      throw e;
    }));
  function notify(req) {
    const send = () => bus.emit(req.team.workspaceId);
    if (!history?.defer(send)) send();
  }
  router.use(
    wrap(async (req, res, next) => {
      res.set("Cache-Control", "private, no-store");
      if (
        req.method !== "GET" &&
        req.headers.origin &&
        !allowedOrigins.has(req.headers.origin)
      )
        fail("Недопустимый источник запроса", 403);
      await ready();
      next();
    }),
  );
  async function invitation(token) {
    const row = await coll("Invites").findOne({
      _id: createHash("sha256").update(String(token)).digest("hex"),
      revokedAt: null,
    });
    if (!row || !(await Workspace.exists({ _id: row.workspaceId })))
      fail("Приглашение недействительно", 404);
    return row;
  }
  router.get(
    "/invite/:token",
    wrap(async (req, res) => {
      const invite = await invitation(req.params.token),
        workspace = await Workspace.findById(invite.workspaceId)
          .select("name")
          .lean();
      res.json({ name: workspace.name });
    }),
  );
  router.get(
    "/manifest/:token",
    wrap(async (req, res) => {
      const invite = await invitation(text(req.params.token, 100));
      res.type("application/manifest+json").json({
        id: "/team/" + invite.workspaceId,
        name: "INDEX12 — Чат сотрудников",
        short_name: "INDEX12 Чат",
        start_url: "/team/" + req.params.token,
        scope: "/team/",
        display: "standalone",
        background_color: "#191919",
        theme_color: "#191919",
        icons: [
          { src: "/pwa-192x192.png", sizes: "192x192", type: "image/png" },
          { src: "/pwa-512x512.png", sizes: "512x512", type: "image/png" },
        ],
      });
    }),
  );
  router.post(
    "/auth",
    wrap(async (req, res) => {
      const invite = await invitation(text(req.body.token, 100)),
        number = phone(req.body.phone);
      const bucket = Math.floor(Date.now() / 600000),
        key = createHash("sha256")
          .update(`${req.ip}:${invite.workspaceId}:${bucket}`)
          .digest("hex");
      const attempts = await coll("Attempts").findOneAndUpdate(
        { _id: key },
        {
          $inc: { count: 1 },
          $setOnInsert: { expiresAt: new Date(Date.now() + 1200000) },
        },
        { upsert: true, returnDocument: "after" },
      );
      if (attempts.count > 30)
        fail("Слишком много попыток. Попробуйте через 10 минут", 429);
      let member = await coll("Members").findOne({
        workspaceId: invite.workspaceId,
        phone: number,
      });
      if (req.body.register === true) {
        if (member) fail("Номер уже зарегистрирован. Войдите с паролем", 409);
        const password = await hashPassword(req.body.password);
        member = {
          _id: randomUUID(),
          workspaceId: invite.workspaceId,
          phone: number,
          name: text(req.body.name, 100),
          password,
          role: "employee",
          status: "pending",
          version: 0,
          createdAt: new Date(),
        };
        try {
          await coll("Members").insertOne(member);
        } catch (e) {
          if (e.code === 11000) fail("Номер уже зарегистрирован", 409);
          throw e;
        }
      } else if (
        !member ||
        !(await verifyPassword(req.body.password, member.password))
      )
        fail("Проверьте номер телефона и пароль", 401);
      if (member.status === "rejected" || member.status === "revoked")
        fail("Доступ к чату не одобрен", 403);
      // A separate persistent cookie cannot authenticate to INDEX12 financial APIs.
      await sessions.issue(req, res, member._id);
      bus.emit(member.workspaceId);
      res.json({ status: member.status });
    }),
  );
  router.use(
    wrap(async (req, res, next) => {
      const persistentId = await sessions.memberId(req, res);
      const chatOnly =
        req.headers["x-index12-chat-only"] === "1" ||
        req.query.chatOnly === "1";
      const wanted =
        req.headers["x-index12-workspace-id"] ||
        req.query.workspaceId ||
        (chatOnly && persistentId ? null : req.user?.currentWorkspaceId);
      let workspace =
        wanted && Types.ObjectId.isValid(wanted)
          ? await Workspace.findById(wanted).lean()
          : null;
      if (
        workspace &&
        (!chatOnly || !persistentId) &&
        req.user &&
        String(workspace.userId) === String(req.user.id || req.user._id)
      ) {
        req.user.currentWorkspaceId = workspace._id;
        req.cachedWorkspace = workspace;
        req.workspaceRole = "admin";
        const settings = await coll("Settings").findOne({
          _id: String(workspace._id),
        });
        req.team = {
          workspaceId: String(workspace._id),
          actorId: "owner:" + workspace.userId,
          owner: true,
          role: ["manager", "director"].includes(settings?.ownerRole)
            ? settings.ownerRole
            : "manager",
          name: req.user.name || "Владелец",
          status: "active",
          workspace,
        };
      } else {
        const memberId = persistentId || req.session?.teamChatMemberId;
        const member =
          memberId && (await coll("Members").findOne({ _id: memberId }));
        if (!member || (wanted && String(wanted) !== member.workspaceId))
          fail("Войдите в чат по приглашению", 401);
        if (!["pending", "active"].includes(member.status))
          fail("Доступ к чату закрыт", 403);
        workspace = await Workspace.findById(member.workspaceId).lean();
        if (!workspace) fail("Чат недоступен", 404);
        req.team = { ...member, actorId: member._id, owner: false, workspace };
        if (!persistentId) await sessions.issue(req, res, member._id);
      }
      req.team.scope = String(getWorkspaceStorageUserId(workspace));
      next();
    }),
  );
  router.get(
    "/me",
    wrap(async (req, res) => {
      const p = req.team;
      res.json(identity(p));
    }),
  );
  router.post(
    "/logout",
    wrap(async (req, res) => {
      await sessions.clear(req, res);
      delete req.session.teamChatMemberId;
      await new Promise((resolve, reject) =>
        req.session.save((e) => (e ? reject(e) : resolve())),
      );
      res.json({ ok: true });
    }),
  );
  router.use((req, res, next) =>
    req.team.status === "active"
      ? next()
      : res.status(403).json({ message: "Ожидается одобрение владельца" }),
  );
  const owner = (req) => {
    if (!req.team.owner)
      fail("Только владелец настраивает участников чата", 403);
  };
  const scopeFilter = (req) => ({
    userId: {
      $in: /^[a-f\d]{24}$/i.test(req.team.scope)
        ? [req.team.scope, new Types.ObjectId(req.team.scope)]
        : [req.team.scope],
    },
  });
  router.get(
    "/events",
    wrap(async (req, res) => {
      res.set({
        "Content-Type": "text/event-stream",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      });
      res.flushHeaders();
      const send = () => res.write("event: changed\ndata: {}\n\n");
      bus.on(req.team.workspaceId, send);
      send();
      const heartbeat = setInterval(() => res.write(": keepalive\n\n"), 15000);
      req.on("close", () => {
        clearInterval(heartbeat);
        bus.off(req.team.workspaceId, send);
      });
    }),
  );
  router.get(
    "/state",
    wrap(async (req, res) => {
      const p = req.team,
        filter = { workspaceId: p.workspaceId };
      const before = req.query.before,
        messageFilter = {
          ...filter,
          ...(before ? { _id: { $lt: before } } : {}),
        };
      // Sortable server timestamps + a compound cursor retain history without a silent fixed list.
      if (before) {
        const cursor = await coll("Messages").findOne({
          _id: before,
          ...filter,
        });
        if (!cursor) fail("Сообщение не найдено", 404);
        delete messageFilter._id;
        messageFilter.$or = [
          { createdAt: { $lt: cursor.createdAt } },
          { createdAt: cursor.createdAt, _id: { $lt: cursor._id } },
        ];
      }
      const rows = await coll("Messages")
        .find(messageFilter)
        .sort({ createdAt: -1, _id: -1 })
        .limit(51)
        .toArray();
      const requestFilter = { ...filter, deletedAt: { $exists: false } };
      const requests = await coll("Requests")
        .find(requestFilter)
        .sort({ createdAt: -1 })
        .limit(501)
        .toArray();
      const allActive = await coll("Requests")
        .find({
          ...filter,
          state: { $in: activeStates },
        })
        .toArray();
      const placements = p.owner
        ? await coll("Requests")
            .find({
              workspaceId: p.workspaceId,
              state: { $in: activeStates },
              placement: { $exists: true },
              operationId: { $exists: false },
            })
            .toArray()
        : [];
      const fileIds = rows
        .filter((m) => !m.deletedAt)
        .flatMap((m) => (m.attachments || []).map((f) => f.fileId));
      const fileStates = await coll("Files")
        .find(
          { _id: { $in: fileIds }, workspaceId: p.workspaceId },
          {
            projection: {
              mimeType: 1,
              invoiceDismissed: 1,
              invoiceRequestId: 1,
            },
          },
        )
        .toArray();
      for (const message of rows)
        message.attachments = (message.attachments || []).map((file) => ({
          ...file,
          isPdf:
            fileStates.find((f) => f._id === file.fileId)?.mimeType ===
              "application/pdf" || /\.pdf$/i.test(file.name),
          invoiceRequestId:
            fileStates.find((f) => f._id === file.fileId)?.invoiceRequestId ||
            null,
          invoiceDismissed: !!fileStates.find((f) => f._id === file.fileId)
            ?.invoiceDismissed,
        }));
      const checkedIds = String(req.query.messageIds || "")
        .split(",")
        .filter((id) => /^[a-f\d-]{36}$/i.test(id))
        .slice(0, 1000);
      const deletedMessageIds = checkedIds.length
        ? (
            await coll("Messages")
              .find(
                {
                  ...filter,
                  _id: { $in: checkedIds },
                  deletedAt: { $exists: true },
                },
                { projection: { _id: 1 } },
              )
              .toArray()
          ).map((m) => m._id)
        : [];
      const companies =
        p.role === "employee"
          ? []
          : await Company.find(scopeFilter(req)).select("_id name").lean();
      const members = p.owner
        ? await coll("Members")
            .find(filter, { projection: { password: 0 } })
            .sort({ createdAt: -1 })
            .toArray()
        : [];
      const financial =
        p.role !== "employee" && getBalances
          ? await getBalances(req.team, allActive)
          : null;
      res.json({
        identity: identity(p),
        messages: rows
          .slice(0, 50)
          .reverse()
          .map((m) =>
            m.deletedAt
              ? { _id: m._id, deletedAt: m.deletedAt, createdAt: m.createdAt }
              : m,
          ),
        deletedMessageIds,
        hasMore: rows.length > 50,
        requests: requests.slice(0, 500),
        placements,
        requestsTruncated: requests.length > 500,
        forecastCents: forecast(allActive),
        companies,
        members,
        financial,
      });
    }),
  );
  router.post(
    "/invite",
    wrap(async (req, res) => {
      owner(req);
      const token = randomBytes(32).toString("base64url");
      await coll("Invites").insertOne({
        _id: createHash("sha256").update(token).digest("hex"),
        workspaceId: req.team.workspaceId,
        revokedAt: null,
        createdAt: new Date(),
      });
      res.json({ url: frontendUrl + "/team/" + token });
    }),
  );
  router.patch(
    "/settings",
    wrap(async (req, res) => {
      owner(req);
      if (!["manager", "director"].includes(req.body.ownerRole))
        fail("Выберите роль управляющего или руководителя");
      await coll("Settings").updateOne(
        { _id: req.team.workspaceId },
        { $set: { ownerRole: req.body.ownerRole } },
        { upsert: true },
      );
      notify(req);
      res.json({ ok: true });
    }),
  );
  router.patch(
    "/members/:id",
    wrap(async (req, res) => {
      owner(req);
      if (
        !Object.hasOwn(roles, req.body.role) ||
        !["active", "rejected", "revoked"].includes(req.body.status) ||
        !Number.isInteger(req.body.expectedVersion)
      )
        fail("Проверьте роль и решение");
      const row = await coll("Members").findOneAndUpdate(
        {
          _id: req.params.id,
          workspaceId: req.team.workspaceId,
          version: req.body.expectedVersion,
        },
        {
          $set: {
            role: req.body.role,
            status: req.body.status,
            reviewedAt: new Date(),
            reviewedBy: req.team.actorId,
          },
          $inc: { version: 1 },
        },
        { returnDocument: "after" },
      );
      if (!row) fail("Заявка сотрудника изменилась. Обновите список", 409);
      notify(req);
      res.json({ ok: true });
    }),
  );
  router.post(
    "/files",
    express.raw({ type: "application/octet-stream", limit: FILE_LIMIT }),
    wrap(async (req, res) => {
      let name;
      try {
        name = safeName(decodeURIComponent(req.headers["x-file-name"] || ""));
      } catch {}
      if (!name || !Buffer.isBuffer(req.body) || !req.body.length)
        fail("Выберите непустой файл");
      const row = {
        _id: randomUUID(),
        workspaceId: req.team.workspaceId,
        actorId: req.team.actorId,
        name,
        mimeType:
          req.body.subarray(0, 5).toString() === "%PDF-"
            ? "application/pdf"
            : "application/octet-stream",
        size: req.body.length,
        data: req.body,
        expiresAt: new Date(Date.now() + 86400000),
      };
      await coll("Files").insertOne(row);
      res.status(201).json({ file: fileMeta(row) });
    }),
  );
  router.get(
    "/files/:id",
    wrap(async (req, res) => {
      const row = await coll("Files").findOne({
        _id: req.params.id,
        workspaceId: req.team.workspaceId,
        $or: [{ linked: true }, { actorId: req.team.actorId }],
      });
      if (!row) fail("Файл недоступен", 404);
      res.set({
        "Content-Type": "application/octet-stream",
        "X-Content-Type-Options": "nosniff",
        "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(row.name)}`,
      });
      res.send(fileBuffer(row));
    }),
  );
  router.post(
    "/messages",
    wrap(async (req, res) => {
      const id = text(req.body.id, 80),
        content = text(req.body.text || "", 10000, false),
        ids = req.body.fileIds || [];
      if (
        !/^[a-f\d-]{36}$/i.test(id) ||
        !Array.isArray(ids) ||
        ids.length > 5 ||
        (!content && !ids.length)
      )
        fail("Введите сообщение или прикрепите файл");
      const attachments = [];
      for (const fileId of ids) {
        const file = await coll("Files").findOne({
          _id: fileId,
          workspaceId: req.team.workspaceId,
          actorId: req.team.actorId,
        });
        if (!file) fail("Файл недоступен", 404);
        attachments.push(fileMeta(file));
      }
      if (attachments.reduce((sum, f) => sum + f.size, 0) > TOTAL_LIMIT)
        fail("Общий размер файлов — до 25 МБ");
      const row = {
        _id: id,
        workspaceId: req.team.workspaceId,
        authorId: req.team.actorId,
        authorName: req.team.name,
        text: content,
        attachments,
        createdAt: new Date(),
      };
      const old = await coll("Messages").findOne({ _id: id });
      if (old) {
        if (
          old.workspaceId !== row.workspaceId ||
          old.authorId !== row.authorId ||
          old.text !== row.text ||
          JSON.stringify(old.attachments) !== JSON.stringify(attachments)
        )
          fail("Ключ сообщения уже использован", 409);
        return res.json({ message: old, replayed: true });
      }
      try {
        await coll("Messages").insertOne(row);
      } catch (e) {
        if (e.code === 11000)
          fail("Сообщение уже отправлено. Обновите чат", 409);
        throw e;
      }
      await coll("Files").updateMany(
        {
          _id: { $in: ids },
          workspaceId: row.workspaceId,
          actorId: row.authorId,
        },
        { $set: { linked: true }, $unset: { expiresAt: "" } },
      );
      notify(req);
      res.status(201).json({ message: row });
    }),
  );
  router.delete(
    "/messages/:id",
    wrap(async (req, res) => {
      const row = await coll("Messages").findOne({
        _id: req.params.id,
        workspaceId: req.team.workspaceId,
      });
      if (!row) fail("Сообщение не найдено", 404);
      if (!req.team.owner && row.authorId !== req.team.actorId)
        fail("Можно удалить только своё сообщение", 403);
      if (!row.deletedAt)
        await coll("Messages").updateOne(
          { _id: row._id, workspaceId: row.workspaceId },
          { $set: { deletedAt: new Date(), deletedBy: req.team.actorId } },
        );
      notify(req);
      res.json({ deletedMessageId: row._id });
    }),
  );
  require("./request-routes.cjs").registerRequestRoutes(router, {
    coll,
    wrap,
    scopeFilter,
    owner,
    notify,
    Company,
    Workspace,
    Contractor,
    Account,
    Category,
    Subcategory,
    Project,
    Event,
    history,
    allocateCell,
    emitToWorkspace,
    parseInvoice,
  });
  router.post(
    "/requests/:id/decision",
    wrap(async (req, res) => {
      const row = await coll("Requests").findOne({
        _id: req.params.id,
        workspaceId: req.team.workspaceId,
      });
      if (!row || row.deletedAt) fail("Заявка не найдена", 404);
      if (
        req.body.action === "approve" &&
        row.state === "manager_review" &&
        !row.placement
      )
        fail("Сначала дополните заявку и разместите её на таймлайне", 409);
      const actingRole =
        row.state === "manager_review" && req.team.owner
          ? "manager"
          : req.team.role;
      const state = decision(row.state, actingRole, req.body.action),
        note = text(req.body.note || "", 1000, false);
      if (!Number.isInteger(req.body.expectedVersion))
        fail("Прочитайте актуальную заявку");
      const updated = await coll("Requests").findOneAndUpdate(
        {
          _id: row._id,
          workspaceId: row.workspaceId,
          version: req.body.expectedVersion,
          state: row.state,
        },
        {
          $set: { state, updatedAt: new Date() },
          $inc: { version: 1 },
          $push: {
            history: {
              actorId: req.team.actorId,
              name: req.team.name,
              role: actingRole,
              action: req.body.action,
              note,
              at: new Date(),
            },
          },
        },
        { returnDocument: "after" },
      );
      if (!updated) fail("Заявка уже изменилась", 409);
      notify(req);
      res.json({ request: updated });
    }),
  );
  router.post(
    "/requests/:id/convert",
    wrap(async (req, res) => {
      owner(req);
      if (!["manager", "director"].includes(req.team.role))
        fail("Операцию создаёт управляющий или руководитель", 403);
      const write = history.wrapHandler(async (request, output) => {
        const row = await coll("Requests").findOne(
          { _id: request.params.id, workspaceId: request.team.workspaceId },
          opts(),
        );
        if (!row || row.deletedAt) fail("Заявка не найдена", 404);
        if (row.operationId)
          return output.json({ operationId: row.operationId, replayed: true });
        if (row.state !== "ready")
          fail("Нужно согласование управляющего и руководителя", 409);
        if (row.version !== request.body.expectedVersion)
          fail("Заявка изменилась", 409);
        if (!row.placement)
          fail("Дополните и повторно согласуйте реквизиты заявки", 409);
        const supplied = request.body;
        for (const field of ["accountId", "categoryId"])
          if (supplied[field] && supplied[field] !== row.placement[field])
            fail("Реквизиты отличаются от утверждённых руководителем", 409);
        if (supplied.dueDate && supplied.dueDate !== row.dueDate)
          fail("Дата отличается от утверждённой руководителем", 409);
        request.body = {
          ...row.placement,
          expectedVersion: supplied.expectedVersion,
          dueDate: row.dueDate,
        };
        if (
          typeof request.body.accountId !== "string" ||
          !/^[a-f\d]{24}$/i.test(request.body.accountId)
        )
          fail("Выберите счёт");
        if (
          request.body.categoryId &&
          (typeof request.body.categoryId !== "string" ||
            !/^[a-f\d]{24}$/i.test(request.body.categoryId))
        )
          fail("Выберите категорию");
        const account = await Account.findOne({
          _id: request.body.accountId,
          companyId: row.companyId,
          ...scopeFilter(request),
        }).lean();
        if (!account) fail("Выберите счёт этой компании", 404);
        if (
          request.body.categoryId &&
          !(await Category.exists({
            _id: request.body.categoryId,
            ...scopeFilter(request),
          }))
        )
          fail("Категория недоступна", 404);
        const dueDate =
          request.body.dueDate === undefined
            ? row.dueDate
            : dateOnly(request.body.dueDate);
        if (!dueDate) fail("Укажите дату операции");
        const before = request.body;
        let operation,
          code = 200;
        const buffer = Object.create(output);
        buffer.status = function (value) {
          code = value;
          return this;
        };
        buffer.json = function (value) {
          operation = value;
          return this;
        };
        request.body = {
          type: "expense",
          amount: -row.amountCents / 100,
          date: localInstant(dueDate, "12:00"),
          accountId: String(account._id),
          companyId: row.companyId,
          categoryId: before.categoryId || null,
          ...row.placement,
          comment: row.comment || "",
          cellIndex: row.placement.cellIndex,
        };
        request.teamChatOperation = true;
        request.teamChatRequestId = row._id;
        try {
          await createOperation(request, buffer);
        } finally {
          request.body = before;
          delete request.teamChatOperation;
          delete request.teamChatRequestId;
        }
        if (code >= 400 || !operation?._id)
          fail(
            operation?.message || "Не удалось создать операцию",
            code >= 400 ? code : 500,
          );
        await coll("Requests").updateOne(
          { _id: row._id, workspaceId: row.workspaceId },
          {
            $set: {
              state: "recorded",
              operationId: String(operation._id),
              recordedAt: new Date(),
              recordedBy: request.team.actorId,
            },
            $inc: { version: 1 },
          },
          opts(),
        );
        notify(request);
        output
          .status(201)
          .json({ operationId: String(operation._id), operation });
      });
      await write(req, res, (error) => {
        throw error;
      });
    }),
  );
  router.use((error, req, res, _next) => {
    const status =
      error.status ||
      error.statusCode ||
      (error.name === "CastError" || error.code === 11000 ? 400 : 500);
    if (status >= 500) console.error("[Team chat]", error.name);
    res.status(status).json({
      message:
        status >= 500
          ? "Не удалось выполнить действие"
          : error.code === 11000
            ? "Запись уже существует"
            : error.message,
    });
  });
  return router;
}
module.exports = { createTeamChatRouter };
