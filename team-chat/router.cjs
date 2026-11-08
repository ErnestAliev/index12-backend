"use strict";
const express = require("express");
const { EventEmitter } = require("node:events");
const { randomUUID, randomBytes, createHash } = require("node:crypto");
const { Types } = require("mongoose");
const {
  roles,
  fail,
  text,
  phone,
  hashPassword,
  verifyPassword,
} = require("./domain.cjs");
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
  User,
  getWorkspaceStorageUserId,
  history,
  frontendUrl,
  onTasksChanged = () => {},
}) {
  const router = express.Router(),
    bus = new EventEmitter();
  bus.setMaxListeners(1000);
  const coll = (name) => db().collection("teamChat" + name);
  const sessions = require("./session.cjs").createChatSessions({ coll });
  const push = require("./push.cjs").createChatPush({ coll, Workspace });
  router.startPush = () => push.start();
  const identity = (p) => ({
    workspaceId: p.workspaceId,
    name: p.workspace.name,
    actorId: p.actorId,
    displayName: p.name,
    position: p.position || "",
    role: p.role,
    status: p.status === "active" && p.requireInstalledApp && !p.installationConfirmed ? "installation_required" : p.status,
    owner: p.owner,
    permissions: { manageMembers: p.owner === true },
    roles,
  });
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
  const invites = require("./invites.cjs").createInvites({
    coll,
    Workspace,
    frontendUrl,
  });
  const access = require("./access.cjs").createAccess({ db, coll, invites, sessions, Workspace, frontendUrl, notify });
  let indexes;
  const ready = () =>
    (indexes ||= Promise.all([
      coll("Invites").createIndex(
        { tokenHash: 1 },
        {
          unique: true,
          partialFilterExpression: { tokenHash: { $type: "string" } },
        },
      ),
      coll("Invites").createIndex({ previousTokenHashes: 1 }, { sparse: true }),
      coll("Invites").createIndex({ workspaceId: 1, createdAt: -1 }),
      coll("Recoveries").createIndex({
        workspaceId: 1,
        status: 1,
        expiresAt: 1,
      }),
      coll("Recoveries").createIndex(
        { expiresAt: 1 },
        { expireAfterSeconds: 0 },
      ),
      coll("Sessions").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      access.ensureIndexes(),
      coll("Messages").createIndex({ workspaceId: 1, createdAt: -1, _id: -1 }),
      coll("Messages").createIndex({ workspaceId: 1, "replyTo.messageId": 1 }),
      coll("MessageReactions").createIndex({ workspaceId: 1, messageId: 1 }),
      coll("MessageReads").createIndex({
        workspaceId: 1,
        messageId: 1,
        actorId: 1,
      }),
      coll("Tasks").createIndex({ workspaceId: 1, createdAt: -1 }),
      coll("Tasks").createIndex({ workspaceId: 1, parentTaskId: 1, deletedAt: 1 }),
      coll("Messages").createIndex({
        workspaceId: 1,
        taskId: 1,
        createdAt: -1,
        _id: -1,
      }),
      coll("Files").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      coll("Attempts").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
    ]).catch((e) => {
      indexes = null;
      throw e;
    }));
  function notify(req, event) {
    const send = () => {
      bus.emit(req.team.workspaceId);
      if (event)
        void push
          .enqueue({
            workspaceId: req.team.workspaceId,
            actorId: req.team.actorId,
            ...event,
          })
          .catch(() =>
            console.error("[Team chat] Could not queue notification"),
          );
    };
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
  const invitation = (token, options) => invites.lookup(token, options);
  router.get(
    "/invite/:token",
    wrap(async (req, res) => {
      const invite = await invitation(req.params.token, { allowClosed: true }),
        workspace = await Workspace.findById(invite.workspaceId)
          .select("name userId")
          .lean();
      const creator =
        !invite.creatorName && User
          ? await User.findById(workspace.userId).select("name").lean()
          : null;
      const role = Object.hasOwn(roles, invite.role || "")
        ? invite.role
        : "employee";
      res.json({
        name: workspace.name,
        workspaceId: invite.workspaceId,
        inviterName: invite.creatorName || creator?.name || "Владелец",
        role,
        roleName: roles[role],
        joinable: invite.joinable,
        namedLogin: invite.namedLogin === true,
      });
    }),
  );
  router.get(
    "/manifest/:token",
    wrap(async (req, res) => {
      const invite = await invitation(text(req.params.token, 100), {
        allowClosed: true,
      });
      res.type("application/manifest+json").json({
        id: "/team/" + invite.workspaceId,
        name: "Chat",
        short_name: "Chat",
        start_url: "/team/" + req.params.token,
        scope: "/team/",
        display: "standalone",
        background_color: "#191919",
        theme_color: "#191919",
        icons: [
          { src: "/chat-192.png", sizes: "192x192", type: "image/png" },
          {
            src: "/chat-512.png",
            sizes: "512x512",
            type: "image/png",
            purpose: "any maskable",
          },
        ],
      });
    }),
  );
  router.get("/push-config", (_req, res) => {
    const keys = require("./push.cjs").pushKeys(process.env);
    res.json({
      configured: !!(keys.publicKey && keys.privateKey),
      publicKey: keys.publicKey || null,
    });
  });
  const recovery = require("./recovery.cjs").createRecovery({
    db,
    coll,
    invites,
    Workspace,
    frontendUrl,
    notify,
  });
  recovery.registerPublic(router, wrap);
  access.registerPublic(router, wrap);
  router.post(
    "/auth",
    wrap(async (req, res) => {
      const invite = await invitation(text(req.body.token, 100), {
          allowClosed: req.body.register !== true,
        }),
        number = phone(req.body.phone);
      if (invite.namedLogin && req.body.register === true)
        fail("Регистрация по этой ссылке закрыта", 403);
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
        if (member) {
          const closed =
            member.status === "revoked" || member.status === "rejected";
          throw Object.assign(
            new Error(
              member.status === "revoked"
                ? "Доступ к чату отозван владельцем. Попросите вернуть доступ"
                : member.status === "rejected"
                  ? "Заявка на доступ отклонена владельцем"
                  : "Номер уже зарегистрирован. Введите свой пароль",
            ),
            {
              status: closed ? 403 : 409,
              code: closed ? "CHAT_ACCESS_CLOSED" : "PHONE_REGISTERED",
            },
          );
        }
        const password = await hashPassword(req.body.password);
        member = {
          _id: randomUUID(),
          workspaceId: invite.workspaceId,
          phone: number,
          name: text(req.body.name, 100),
          password,
          role: Object.hasOwn(roles, invite.role || "")
            ? invite.role
            : "employee",
          status: "pending",
          version: 0,
          createdAt: new Date(),
        };
        try {
          await coll("Members").insertOne(member);
          const workspace = await Workspace.findById(member.workspaceId)
            .select("userId")
            .lean();
          void push
            .enqueue({
              workspaceId: member.workspaceId,
              actorId: member._id,
              onlyActor: "owner:" + workspace.userId,
              key: "join:" + member._id,
              title: "Запрос в Chat",
              body: member.name + " просит доступ",
            })
            .catch(() => {});
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
        fail(
          member.status === "revoked"
            ? "Доступ к чату отозван владельцем. Попросите вернуть доступ"
            : "Заявка на доступ отклонена владельцем",
          403,
        );
      // A separate persistent cookie cannot authenticate to INDEX12 financial APIs.
      await sessions.issue(req, res, member._id, member.authEpoch || 0);
      bus.emit(member.workspaceId);
      res.json({ status: member.status });
    }),
  );
  router.use(
    wrap(async (req, res, next) => {
      const persistentId = await sessions.memberId(req, res);
      const memberId = persistentId || req.session?.teamChatMemberId;
      const member = memberId
        ? await coll("Members").findOne({ _id: memberId })
        : null;
      const chatOnly =
        req.headers["x-index12-chat-only"] === "1" ||
        req.query.chatOnly === "1";
      const wanted =
        req.headers["x-index12-workspace-id"] ||
        req.query.workspaceId ||
        (chatOnly && member
          ? member.workspaceId
          : req.user?.currentWorkspaceId || member?.workspaceId);
      let workspace =
        wanted && Types.ObjectId.isValid(wanted)
          ? await Workspace.findById(wanted).lean()
          : null;
      if (
        !chatOnly && workspace &&
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
          role: Object.hasOwn(roles, settings?.ownerRole || "")
            ? settings.ownerRole
            : "manager",
          name: settings?.ownerName || req.user.name || "Владелец",
          position: settings?.ownerPosition || "",
          status: "active",
          workspace,
        };
      } else {
        if (
          !member ||
          (wanted && String(wanted) !== member.workspaceId) ||
          (persistentId
            ? (req.teamSessionEpoch || 0) !== (member.authEpoch || 0)
            : !!member.authEpoch)
        ) {
          await sessions.clear(req, res);
          if (req.session) delete req.session.teamChatMemberId;
          fail("Войдите в чат по приглашению", 401);
        }
        if (
          !["pending", "active", "revoked", "rejected"].includes(member.status)
        )
          fail("Доступ к чату закрыт", 403);
        workspace = await Workspace.findById(member.workspaceId).lean();
        if (!workspace) fail("Чат недоступен", 404);
        req.team = { ...member, actorId: member._id, owner: false, workspace, installationConfirmed: req.teamSessionInstalled === true };
        if (!persistentId)
          await sessions.issue(req, res, member._id, member.authEpoch || 0);
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
      if (
        typeof req.body.endpoint === "string" &&
        req.body.endpoint.length <= 4096
      )
        await coll("PushSubscriptions").deleteOne({
          _id: createHash("sha256").update(req.body.endpoint).digest("hex"),
          actorId: req.team.actorId,
          workspaceId: req.team.workspaceId,
        });
      await sessions.clear(req, res);
      delete req.session.teamChatMemberId;
      await new Promise((resolve, reject) =>
        req.session.save((e) => (e ? reject(e) : resolve())),
      );
      res.json({ ok: true });
    }),
  );
  router.post("/installation", wrap(async (req, res) => {
    if (req.team.owner || !req.team.requireInstalledApp) return res.json({ identity: identity(req.team) });
    if (req.team.status !== "active") fail("Доступ к чату закрыт", 403);
    if (req.body.installed !== true) fail("Добавьте приложение на главный экран", 403);
    if (!(await sessions.confirmInstalled(req))) fail("Войдите в приложение заново", 401);
    res.json({ identity: identity({ ...req.team, installationConfirmed: true }) });
  }));
  router.use((req, res, next) => req.team.status === "active" && req.team.requireInstalledApp && !req.team.installationConfirmed
    ? res.status(403).json({ code: "CHAT_INSTALL_REQUIRED", message: "Добавьте Chat на главный экран" })
    : next());
  router.use((req, res, next) =>
    ["pending", "active"].includes(req.team.status)
      ? next()
      : res.status(403).json({
          message:
            req.team.status === "revoked"
              ? "Доступ к чату отозван владельцем"
              : "Заявка на доступ отклонена владельцем",
        }),
  );
  push.register(router, {
    wrap,
    invitation: (token) => invitation(token, { allowClosed: true }),
  });
  router.use((req, res, next) =>
    req.team.status === "active"
      ? next()
      : res.status(403).json({ message: "Ожидается одобрение владельца" }),
  );
  const owner = (req) => {
    if (!req.team.owner)
      fail("Только владелец настраивает участников чата", 403);
  };
  recovery.registerOwner(router, { wrap, owner });
  access.registerOwner(router, { wrap, owner });
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
  const tasks = require("./tasks.cjs").registerTaskRoutes(router, {
    coll,
    wrap,
    notify,
    User,
    db,
    onTasksChanged,
  });
  async function messagePage(p, before, taskId = null) {
    const filter = { workspaceId: p.workspaceId },
      messageFilter = {
        ...filter,
        deletedAt: { $exists: false },
        kind: { $nin: ["request", ...(taskId ? ["task"] : [])] },
        ...(taskId
          ? { taskId }
          : { $or: [{ taskId: { $exists: false } }, { kind: "task" }] }),
      };
    if (before) {
      if (typeof before !== "string" || !/^[a-f\d-]{36}$/i.test(before))
        fail("Некорректная страница истории");
      const cursor = await coll("Messages").findOne({ _id: before, ...filter });
      if (
        !cursor ||
        (taskId
          ? cursor.taskId !== taskId || cursor.kind === "task"
          : cursor.taskId && cursor.kind !== "task")
      )
        fail("Сообщение не найдено", 404);
      messageFilter.$and = [
        {
          $or: [
            { createdAt: { $lt: cursor.createdAt } },
            { createdAt: cursor.createdAt, _id: { $lt: cursor._id } },
          ],
        },
      ];
    }
    return coll("Messages")
      .find(messageFilter, {
        projection: { editHistory: 0, approvalArchive: 0 },
      })
      .sort({ createdAt: -1, _id: -1 })
      .limit(51)
      .toArray();
  }
  router.get(
    "/messages",
    wrap(async (req, res) => {
      if (req.query.taskId) await tasks.load(req, req.query.taskId);
      const rows = await messagePage(
        req.team,
        req.query.before,
        req.query.taskId || null,
      );
      const watched = String(req.query.messageIds || "")
        .split(",")
        .filter((id) => /^[a-f\d-]{36}$/i.test(id))
        .slice(0, 150);
      const updates =
        req.query.taskId && watched.length
          ? await coll("Messages")
              .find(
                {
                  workspaceId: req.team.workspaceId,
                  taskId: req.query.taskId,
                  kind: { $ne: "task" },
                  _id: { $in: watched },
                },
                { projection: { editHistory: 0, approvalArchive: 0 } },
              )
              .toArray()
          : [];
      const messageUpdates = updates.filter((m) => !m.deletedAt),
        deletedMessageIds = updates
          .filter((m) => m.deletedAt)
          .map((m) => m._id);
      await messageActions.decorate(req, [...rows, ...messageUpdates]);
      res.json({
        messages: rows.slice(0, 50).reverse(),
        hasMore: rows.length > 50,
        messageUpdates,
        deletedMessageIds,
      });
    }),
  );
  router.get(
    "/state",
    wrap(async (req, res) => {
      const checkedAt = new Date();
      const p = req.team,
        filter = { workspaceId: p.workspaceId };
      const [rows, members, recoveryCount, people] = await Promise.all([
        messagePage(p, req.query.before),
        p.owner
          ? coll("Members")
              .find(filter, { projection: { password: 0 } })
              .sort({ createdAt: -1 })
              .toArray()
          : [],
        p.owner
          ? coll("Recoveries").countDocuments({
              ...filter,
              status: "pending",
              expiresAt: { $gt: new Date() },
            })
          : 0,
        (async () => {
          const [staff, settings, user] = await Promise.all([
            coll("Members")
              .find(
                { ...filter, status: { $in: ["active", "invited"] } },
                { projection: { _id: 1, name: 1, role: 1, position: 1, status: 1 } },
              )
              .toArray(),
            coll("Settings").findOne({ _id: p.workspaceId }),
            User
              ? User.findById(p.workspace.userId).select("name").lean()
              : null,
          ]);
          return [
            {
              _id: "owner:" + p.workspace.userId,
              name: settings?.ownerName || user?.name || (p.owner ? p.name : "Владелец"),
              position: settings?.ownerPosition || "",
              role: settings?.ownerRole || "manager",
              owner: true,
            },
            ...staff,
          ];
        })(),
      ]);
      const checkedIds = String(req.query.messageIds || "")
        .split(",")
        .filter((id) => /^[a-f\d-]{36}$/i.test(id))
        .slice(0, 150);
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
      const since = new Date(req.query.updatedSince || 0);
      const messageUpdates = checkedIds.length
        ? await coll("Messages")
            .find(
              {
                ...filter,
                _id: { $in: checkedIds },
                deletedAt: { $exists: false },
                ...(Number.isFinite(since.getTime())
                  ? { updatedAt: { $gte: since } }
                  : {}),
              },
              { projection: { editHistory: 0, approvalArchive: 0 } },
            )
            .limit(1000)
            .toArray()
        : [];
      await messageActions.decorate(req, [...rows, ...messageUpdates]);
      res.json({
        identity: identity(p),
        checkedAt,
        messageUpdates,
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
        tasks: await tasks.summaries(req),
        members: p.owner ? [{ ...people[0], status: "active", version: 0 }, ...members] : members,
        people,
        recoveryCount,
      });
    }),
  );
  router.get(
    "/invites",
    wrap(async (req, res) => {
      owner(req);
      res.json({ invites: await invites.list(req.team.workspaceId) });
    }),
  );
  router.post(
    "/invite",
    wrap(async (req, res) => {
      owner(req);
      const invite = await invites.create(
        req.team,
        req.body.role || "employee",
      );
      notify(req);
      res.json({ invite, url: invite.url });
    }),
  );
  router.patch(
    "/invites/:id",
    wrap(async (req, res) => {
      owner(req);
      const invite = await invites.change(req.team, req.params.id, req.body);
      notify(req);
      res.json({ invite, url: invite.url });
    }),
  );
  router.delete(
    "/invites/:id",
    wrap(async (req, res) => {
      owner(req);
      await invites.change(req.team, req.params.id, req.body, true);
      notify(req);
      res.json({ deletedInviteId: req.params.id });
    }),
  );
  router.patch(
    "/settings",
    wrap(async (req, res) => {
      owner(req);
      const changes = {};
      if (Object.hasOwn(req.body, "ownerRole")) {
        if (!Object.hasOwn(roles, req.body.ownerRole)) fail("Выберите роль");
        changes.ownerRole = req.body.ownerRole;
      }
      if (Object.hasOwn(req.body, "ownerName")) changes.ownerName = text(req.body.ownerName, 100);
      if (Object.hasOwn(req.body, "ownerPosition")) changes.ownerPosition = text(req.body.ownerPosition, 160, false);
      if (!Object.keys(changes).length) fail("Укажите данные профиля чата");
      await coll("Settings").updateOne(
        { _id: req.team.workspaceId },
        { $set: changes },
        { upsert: true },
      );
      notify(req);
      const profile = { ...req.team, role: changes.ownerRole || req.team.role, name: changes.ownerName || req.team.name, position: changes.ownerPosition ?? req.team.position ?? "" };
      res.json({
        ok: true,
        identity: identity(profile),
        member: { _id: profile.actorId, name: profile.name, position: profile.position, role: profile.role, owner: true, status: "active", version: 0 },
      });
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
      notify(
        req,
        row.status === "active"
          ? {
              key: `member:${row._id}:${row.version}`,
              onlyActor: row._id,
              title: "Chat · Доступ открыт",
              body: "Ваша роль: " + roles[row.role],
              allowPending: true,
            }
          : undefined,
      );
      const { password: _password, ...member } = row;
      res.json({ member });
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
            : req.body
                  .subarray(0, 8)
                  .equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))
              ? "image/png"
              : req.body[0] === 255 &&
                  req.body[1] === 216 &&
                  req.body[2] === 255
                ? "image/jpeg"
                : /^GIF8[79]a/.test(req.body.subarray(0, 6).toString())
                  ? "image/gif"
                  : req.body.subarray(0, 4).toString() === "RIFF" &&
                      req.body.subarray(8, 12).toString() === "WEBP"
                    ? "image/webp"
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
      const taskId = req.body.taskId || null;
      if (taskId) await tasks.load(req, taskId);
      const old = await coll("Messages").findOne({ _id: id });
      if (
        old &&
        (old.workspaceId !== req.team.workspaceId ||
          old.authorId !== req.team.actorId)
      )
        fail("Ключ сообщения уже использован", 409);
      let replyTo;
      if (req.body.replyToId) {
        const sourceId = req.body.replyToId;
        if (
          typeof sourceId !== "string" ||
          !/^[a-f\d-]{36}$/i.test(sourceId) ||
          sourceId === id
        )
          fail("Выберите исходное сообщение");
        if (old?.replyTo?.messageId === sourceId) replyTo = old.replyTo;
        else {
          const source = await coll("Messages").findOne({
            _id: sourceId,
            workspaceId: req.team.workspaceId,
            deletedAt: { $exists: false },
          });
          if (
            !source ||
            (source.taskId || null) !== taskId ||
            source.kind === "task"
          )
            fail("Исходное сообщение удалено или недоступно", 404);
          replyTo = {
            messageId: source._id,
            authorId: source.authorId,
            authorName: source.authorName,
            text: String(source.text || "").slice(0, 1000),
            fileName: source.attachments?.[0]?.name || "",
            fileCount: source.attachments?.length || 0,
            contentVersion: Number(source.contentVersion || 0),
          };
        }
      }
      const mentionIds = req.body.mentionIds || [];
      if (
        !Array.isArray(mentionIds) ||
        mentionIds.length > 30 ||
        mentionIds.some((i) => typeof i !== "string" || i.length > 100)
      )
        fail("Проверьте отмеченных сотрудников");
      const mentioned = mentionIds.length
        ? await coll("Members")
            .find(
              {
                _id: { $in: mentionIds },
                workspaceId: req.team.workspaceId,
                status: "active",
              },
              { projection: { _id: 1, name: 1 } },
            )
            .toArray()
        : [];
      if (mentionIds.includes("owner:" + req.team.workspace.userId)) {
        const settings = await coll("Settings").findOne({ _id: req.team.workspaceId });
        const user = User
          ? await User.findById(req.team.workspace.userId).select("name").lean()
          : null;
        mentioned.push({
          _id: "owner:" + req.team.workspace.userId,
          name: settings?.ownerName || user?.name || "Владелец",
        });
      }
      if (new Set(mentionIds).size !== mentioned.length)
        fail("Отмеченный сотрудник недоступен", 404);
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
        ...(taskId ? { taskId } : {}),
        workspaceId: req.team.workspaceId,
        authorId: req.team.actorId,
        authorName: req.team.name,
        text: content,
        attachments,
        createdAt: new Date(),
        updatedAt: new Date(),
        version: 0,
        contentVersion: 0,
        ...(replyTo ? { replyTo } : {}),
        ...(mentioned.length
          ? {
              mentions: mentioned.map((p) => ({
                actorId: p._id,
                name: p.name,
              })),
            }
          : {}),
      };
      if (old) {
        if (
          old.workspaceId !== row.workspaceId ||
          old.authorId !== row.authorId ||
          old.text !== row.text ||
          (old.taskId || null) !== taskId ||
          old.kind === "task" ||
          (old.replyTo?.messageId || null) !== (replyTo?.messageId || null) ||
          JSON.stringify(old.mentions || []) !==
            JSON.stringify(row.mentions || []) ||
          JSON.stringify(old.attachments) !== JSON.stringify(attachments)
        )
          fail("Ключ сообщения уже использован", 409);
        await messageActions.decorate(req, [old]);
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
      notify(req, {
        key: "message:" + row._id,
        title: req.team.workspace.name + " · " + req.team.name,
        body: row.text || "Файл в чате",
        taskId: row.taskId,
        messageId: row._id,
      });
      await messageActions.decorate(req, [row]);
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
      if (row.kind === "task") fail("Откройте задачу для изменения", 409);
      if (!req.team.owner && row.authorId !== req.team.actorId)
        fail("Можно удалить только своё сообщение", 403);
      if (!row.deletedAt)
        await coll("Messages").updateOne(
          { _id: row._id, workspaceId: row.workspaceId },
          { $set: { deletedAt: new Date(), deletedBy: req.team.actorId } },
        );
      await coll("Messages").updateMany(
        { workspaceId: req.team.workspaceId, "replyTo.messageId": row._id },
        { $set: { updatedAt: new Date() } },
      );
      notify(req);
      res.json({ deletedMessageId: row._id });
    }),
  );
  const messageActions = require("./message-routes.cjs").registerMessageRoutes(
    router,
    { coll, wrap, notify, Workspace },
  );
  require("./search.cjs").registerSearchRoutes(router, { coll, wrap, tasks, decorate: messageActions.decorate });
  router.use((_req, res) =>
    res.status(404).json({ message: "Маршрут чата не найден" }),
  );
  router.use((error, req, res, _next) => {
    const status =
      error.status ||
      error.statusCode ||
      (error.name === "CastError" || error.code === 11000 ? 400 : 500);
    if (status >= 500) console.error("[Team chat]", error.name);
    res.status(status).json({
      ...(typeof error.code === "string" && /^(CHAT_|PHONE_)/.test(error.code)
        ? { code: error.code }
        : {}),
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
