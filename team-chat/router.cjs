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
  User,
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
  const push = require("./push.cjs").createChatPush({ coll, Workspace });
  router.startPush = () => push.start();
  const identity = (p) => ({
    workspaceId: p.workspaceId,
    name: p.workspace.name,
    actorId: p.actorId,
    displayName: p.name,
    role: p.role,
    status: p.status,
    owner: p.owner,
    permissions: { manageMembers: p.owner === true },
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
  const invites = require("./invites.cjs").createInvites({
    coll,
    Workspace,
    frontendUrl,
  });
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
      coll("Members").createIndex(
        { workspaceId: 1, phone: 1 },
        { unique: true },
      ),
      coll("Messages").createIndex({ workspaceId: 1, createdAt: -1, _id: -1 }),
      coll("Messages").createIndex({ workspaceId: 1, "replyTo.messageId": 1 }),
      coll("MessageReactions").createIndex({ workspaceId: 1, messageId: 1 }),
      coll("MessageReads").createIndex({ workspaceId: 1, messageId: 1 }),
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
  router.post(
    "/auth",
    wrap(async (req, res) => {
      const invite = await invitation(text(req.body.token, 100), {
          allowClosed: req.body.register !== true,
        }),
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
        workspace &&
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
          name: req.user.name || "Владелец",
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
        req.team = { ...member, actorId: member._id, owner: false, workspace };
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
  async function messagePage(p, before) {
    const filter = { workspaceId: p.workspaceId },
      messageFilter = { ...filter, deletedAt: { $exists: false } };
    if (before) {
      if (typeof before !== "string" || !/^[a-f\d-]{36}$/i.test(before))
        fail("Некорректная страница истории");
      const cursor = await coll("Messages").findOne({ _id: before, ...filter });
      if (!cursor) fail("Сообщение не найдено", 404);
      messageFilter.$or = [
        { createdAt: { $lt: cursor.createdAt } },
        { createdAt: cursor.createdAt, _id: { $lt: cursor._id } },
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
      const rows = await messagePage(req.team, req.query.before);
      await messageActions.decorate(req, rows);
      res.json({
        messages: rows.slice(0, 50).reverse(),
        hasMore: rows.length > 50,
      });
    }),
  );
  router.get(
    "/state",
    wrap(async (req, res) => {
      const checkedAt = new Date();
      const p = req.team,
        filter = { workspaceId: p.workspaceId };
      const [
        rows,
        requests,
        allActive,
        companies,
        members,
        recoveryCount,
        people,
      ] = await Promise.all([
        messagePage(p, req.query.before),
        coll("Requests")
          .find({ ...filter, deletedAt: { $exists: false } })
          .sort({ createdAt: -1 })
          .limit(501)
          .toArray(),
        coll("Requests")
          .find({ ...filter, state: { $in: activeStates } })
          .toArray(),
        !p.owner && p.role === "employee"
          ? []
          : Company.find(scopeFilter(req)).select("_id name").lean(),
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
                { ...filter, status: "active" },
                { projection: { _id: 1, name: 1, role: 1 } },
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
              name: user?.name || (p.owner ? p.name : "Владелец"),
              role: settings?.ownerRole || "manager",
              owner: true,
            },
            ...staff,
          ];
        })(),
      ]);
      const placements = p.owner
        ? allActive.filter((r) => r.placement && !r.operationId)
        : [];
      const financialPromise = Promise.resolve(
        req.query.section !== "members" &&
          (req.query.section !== "messages" ||
            allActive.some((r) => r.placement)) &&
          (p.owner || p.role !== "employee") &&
          getBalances
          ? getBalances(req.team, allActive)
          : p.owner || p.role !== "employee"
            ? undefined
            : null,
      ).then(
        (value) => ({ value }),
        (error) => ({ error }),
      );
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
      const financialResult = await financialPromise;
      if (financialResult.error) throw financialResult.error;
      const financial = financialResult.value;
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
        requests: requests.slice(0, 500),
        placements,
        requestsTruncated: requests.length > 500,
        forecastCents: forecast(allActive),
        companies,
        members,
        people,
        recoveryCount,
        financial,
        ...(p.owner || p.role !== "employee"
          ? {
              activeRequestCosts: allActive.map((r) => ({
                _id: r._id,
                state: r.state,
                amountCents: r.amountCents,
                companyId: r.companyId,
                dueDate: r.dueDate,
                operationId: r.operationId || null,
                placement: r.placement
                  ? { accountId: r.placement.accountId }
                  : null,
              })),
            }
          : {}),
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
      if (!Object.hasOwn(roles, req.body.ownerRole)) fail("Выберите роль");
      await coll("Settings").updateOne(
        { _id: req.team.workspaceId },
        { $set: { ownerRole: req.body.ownerRole } },
        { upsert: true },
      );
      notify(req);
      res.json({
        ok: true,
        identity: identity({ ...req.team, role: req.body.ownerRole }),
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
          if (!source) fail("Исходное сообщение удалено или недоступно", 404);
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
        const user = User
          ? await User.findById(req.team.workspace.userId).select("name").lean()
          : null;
        mentioned.push({
          _id: "owner:" + req.team.workspace.userId,
          name: user?.name || "Владелец",
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
      if (req.body.action === "reject" && !note)
        fail("Укажите причину отклонения");
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
          $set: {
            state,
            rejectionReason: req.body.action === "reject" ? note : null,
            updatedAt: new Date(),
          },
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
      notify(req, {
        key: `request:${updated._id}:${updated.version}`,
        title: "Chat · Решение по заявке",
        body: updated.title,
      });
      res.json({ request: updated });
    }),
  );
  router.post(
    "/requests/:id/convert",
    wrap(async (req, res) => {
      owner(req);
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
