"use strict";
const { createHash } = require("node:crypto");
const { fail, text } = require("./domain.cjs");
const { dateOnly } = require("../business/task-domain.cjs");
const { calendarTask } = require("../business/chat-calendar.cjs");
const { fileMeta, TOTAL_LIMIT } = require("../codex/files.cjs");
const uuid = (value) =>
  typeof value === "string" && /^[a-f\d-]{36}$/i.test(value);
function taskInput(body) {
  if (!body || typeof body !== "object" || Array.isArray(body))
    fail("Проверьте задачу");
  const description = text(body.description, 10000);
  if (!Array.isArray(body.assigneeIds)) fail("Выберите ответственного");
  const assigneeIds = [...new Set(body.assigneeIds)];
  if (
    !Array.isArray(body.assigneeIds) ||
    !assigneeIds.length ||
    assigneeIds.length > 30 ||
    assigneeIds.some((id) => typeof id !== "string" || id.length > 100)
  )
    fail("Выберите ответственного");
  if (!["urgent", "deadline", "none"].includes(body.timing))
    fail("Укажите когда выполнить задачу");
  const dueDate = body.timing === "none" ? null : dateOnly(body.dueDate);
  if (body.timing !== "none" && !dueDate) fail("Выберите дату");
  const dueTime = body.timing === "none" ? null : body.dueTime || null;
  if (dueTime && !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(dueTime))
    fail("Выберите время");
  const status = body.status || "open";
  if (!["open", "progress", "delayed", "done"].includes(status))
    fail("Выберите статус");
  const fileIds = body.fileIds || [];
  if (body.parentTaskId != null && !uuid(body.parentTaskId))
    fail("Проверьте родительскую задачу");
  if (
    !Array.isArray(fileIds) ||
    fileIds.length > 5 ||
    fileIds.some((id) => !uuid(id)) ||
    new Set(fileIds).size !== fileIds.length
  )
    fail("Проверьте файлы");
  return {
    description,
    assigneeIds,
    timing: body.timing,
    dueDate,
    dueTime,
    status,
    fileIds,
    ...(body.parentTaskId ? { parentTaskId: body.parentTaskId } : {}),
  };
}
function registerTaskRoutes(router, { coll, wrap, notify, User, db, onTasksChanged = () => {} }) {
  const filter = (req) => ({ workspaceId: req.team.workspaceId });
  async function load(
    req,
    id = req.params.id,
    includeDeleted = false,
    options = {},
  ) {
    if (!uuid(id)) fail("Задача недоступна", 404);
    const task = await coll("Tasks").findOne(
      {
        _id: id,
        ...filter(req),
        ...(includeDeleted ? {} : { deletedAt: { $exists: false } }),
      },
      options,
    );
    if (!task) fail("Задача недоступна", 404);
    return task;
  }
  async function guardParent(req, parentTaskId, session) {
    await load(req, parentTaskId, false, { session });
    const changed = await coll("Tasks").updateOne(
      { _id: parentTaskId, ...filter(req), deletedAt: { $exists: false } },
      { $inc: { subtaskRevision: 1 } },
      { session },
    );
    if (!changed.modifiedCount) fail("Родительская задача недоступна", 404);
  }
  async function summaries(req, ids) {
    const match = {
      ...filter(req),
      deletedAt: { $exists: false },
      ...(ids ? { _id: { $in: ids } } : {}),
    };
    return coll("Tasks")
      .aggregate([
        { $match: match },
        { $sort: { createdAt: -1, _id: -1 } },
        {
          $lookup: {
            from: "teamChatMessages",
            let: { task: "$_id", workspace: "$workspaceId" },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ["$taskId", "$$task"] },
                      { $eq: ["$workspaceId", "$$workspace"] },
                    ],
                  },
                  kind: { $ne: "task" },
                  deletedAt: { $exists: false },
                },
              },
              {
                $facet: {
                  latest: [
                    { $sort: { createdAt: -1, _id: -1 } },
                    { $limit: 1 },
                    {
                      $project: {
                        _id: 1,
                        authorName: 1,
                        text: 1,
                        attachments: 1,
                        createdAt: 1,
                      },
                    },
                  ],
                  unread: [
                    { $match: { authorId: { $ne: req.team.actorId } } },
                    {
                      $lookup: {
                        from: "teamChatMessageReads",
                        let: {
                          message: "$_id",
                          workspace: "$workspaceId",
                          revision: { $ifNull: ["$contentVersion", 0] },
                        },
                        pipeline: [
                          {
                            $match: {
                              actorId: req.team.actorId,
                              visibilityConfirmed: true,
                              $expr: {
                                $and: [
                                  { $eq: ["$workspaceId", "$$workspace"] },
                                  { $eq: ["$messageId", "$$message"] },
                                  { $gte: ["$contentVersion", "$$revision"] },
                                ],
                              },
                            },
                          },
                          { $limit: 1 },
                        ],
                        as: "reads",
                      },
                    },
                    { $match: { reads: { $size: 0 } } },
                    { $count: "count" },
                  ],
                  mention: [
                    {
                      $match: {
                        authorId: { $ne: req.team.actorId },
                        "mentions.actorId": req.team.actorId,
                      },
                    },
                    {
                      $lookup: {
                        from: "teamChatMessages",
                        let: {
                          task: "$taskId",
                          workspace: "$workspaceId",
                          since: "$createdAt",
                        },
                        pipeline: [
                          {
                            $match: {
                              authorId: req.team.actorId,
                              kind: { $ne: "task" },
                              deletedAt: { $exists: false },
                              $expr: {
                                $and: [
                                  { $eq: ["$taskId", "$$task"] },
                                  { $eq: ["$workspaceId", "$$workspace"] },
                                  { $gt: ["$createdAt", "$$since"] },
                                ],
                              },
                            },
                          },
                          { $limit: 1 },
                          { $project: { _id: 1 } },
                        ],
                        as: "replies",
                      },
                    },
                    { $match: { replies: { $size: 0 } } },
                    { $sort: { createdAt: 1, _id: 1 } },
                    { $limit: 1 },
                    { $project: { _id: 1 } },
                  ],
                },
              },
            ],
            as: "summary",
          },
        },
      ])
      .toArray()
      .then((rows) =>
        rows.map((row) => {
          const { summary, createHash: _hash, ...task } = row,
            data = summary[0] || {};
          return {
            ...task,
            latest: data.latest?.[0] || null,
            unreadCount: data.unread?.[0]?.count || 0,
            mentioned: !!data.mention?.length,
            mentionMessageId: data.mention?.[0]?._id || null,
          };
        }),
      );
  }
  async function people(req, ids) {
    const staff = await coll("Members")
      .find(
        { ...filter(req), _id: { $in: ids }, status: { $in: ["active", "invited"] } },
        { projection: { _id: 1, name: 1 } },
      )
      .toArray();
    const ownerId = "owner:" + req.team.workspace.userId;
    if (ids.includes(ownerId)) {
      const settings = await coll("Settings").findOne({ _id: req.team.workspaceId });
      const owner = User
        ? await User.findById(req.team.workspace.userId).select("name").lean()
        : null;
      staff.push({
        _id: ownerId,
        name: settings?.ownerName || owner?.name || (req.team.owner ? req.team.name : "Владелец"),
      });
    }
    if (staff.length !== ids.length) fail("Ответственный недоступен", 404);
    return ids.map((id) => ({
      actorId: id,
      name: staff.find((p) => p._id === id).name,
    }));
  }
  async function attachments(req, ids, old) {
    const rows = [];
    for (const id of ids) {
      const file = await coll("Files").findOne({ _id: id, ...filter(req) });
      if (
        !file ||
        (file.actorId !== req.team.actorId &&
          !old?.attachments?.some((f) => f.fileId === id))
      )
        fail("Файл недоступен", 404);
      rows.push(fileMeta(file));
    }
    if (rows.reduce((n, f) => n + f.size, 0) > TOTAL_LIMIT)
      fail("Общий размер файлов — до 25 МБ");
    return rows;
  }
  async function pin(req, ids) {
    if (ids.length)
      await coll("Files").updateMany(
        { ...filter(req), _id: { $in: ids } },
        { $set: { linked: true }, $unset: { expiresAt: "" } },
      );
  }
  router.get(
    "/tasks",
    wrap(async (req, res) => res.json({ tasks: await summaries(req) })),
  );
  router.get(
    "/tasks/:id",
    wrap(async (req, res) => {
      const row = await load(req);
      res.json({ task: (await summaries(req, [row._id]))[0] });
    }),
  );
  router.post(
    "/tasks",
    wrap(async (req, res) => {
      if (!uuid(req.body.id)) fail("Проверьте задачу");
      const input = taskInput(req.body),
        id = req.body.id;
      if (input.parentTaskId === id) fail("Задача не может быть своей подзадачей");
      if (input.parentTaskId) await load(req, input.parentTaskId);
      const fingerprint = createHash("sha256")
        .update(JSON.stringify(input))
        .digest("hex");
      let row = await coll("Tasks").findOne({ _id: id });
      if (
        row &&
        (row.workspaceId !== req.team.workspaceId ||
          row.creatorId !== req.team.actorId ||
          row.createHash !== fingerprint ||
          row.deletedAt)
      )
        fail("Ключ задачи уже использован", 409);
      if (!row) {
        const { fileIds, ...data } = input;
        row = {
          _id: id,
          ...filter(req),
          ...data,
          assignees: await people(req, input.assigneeIds),
          attachments: await attachments(req, fileIds),
          creatorId: req.team.actorId,
          creatorName: req.team.name,
          createdAt: new Date(),
          updatedAt: new Date(),
          version: 0,
          createHash: fingerprint,
        };
        if (await coll("Messages").findOne({ _id: id }))
          fail("Ключ задачи уже использован", 409);
        await pin(req, fileIds);
        try {
          if (row.parentTaskId) {
            const session = db().client.startSession();
            try {
              await session.withTransaction(async () => {
                await guardParent(req, row.parentTaskId, session);
                await coll("Tasks").insertOne(row, { session });
              });
            } finally { await session.endSession(); }
          } else await coll("Tasks").insertOne(row);
        } catch (e) {
          if (e.code === 11000) fail("Задача уже создана. Обновите чат", 409);
          throw e;
        }
      }
      // The deterministic cover id makes a retry repair an interrupted creation.
      if (!row.parentTaskId) await coll("Messages").updateOne(
        { _id: id, ...filter(req) },
        {
          $setOnInsert: {
            _id: id,
            ...filter(req),
            kind: "task",
            taskId: id,
            authorId: row.creatorId,
            authorName: row.creatorName,
            text: "",
            attachments: [],
            createdAt: row.createdAt,
            updatedAt: row.createdAt,
            version: 0,
          },
        },
        { upsert: true },
      );
      notify(req, {
        key: "task:" + id,
        title: req.team.name,
        body: row.description,
        taskId: id,
      });
      onTasksChanged(req);
      res.status(201).json({ task: (await summaries(req, [id]))[0] });
    }),
  );
  router.patch(
    "/tasks/:id",
    wrap(async (req, res) => {
      const old = await load(req);
      if (Object.hasOwn(req.body, "parentTaskId") &&
          (req.body.parentTaskId || null) !== (old.parentTaskId || null))
        fail("Родительскую задачу нельзя менять");
      if (
        !req.team.owner &&
        old.creatorId !== req.team.actorId &&
        !old.assigneeIds.includes(req.team.actorId)
      )
        fail("Изменять задачу может её автор или ответственный", 403);
      if (
        !Number.isInteger(req.body.expectedVersion) ||
        req.body.expectedVersion !== old.version
      )
        fail("Задача изменилась. Откройте её заново", 409);
      const input = taskInput({
        ...old,
        fileIds: old.attachments.map((f) => f.fileId),
        ...req.body,
      });
      const { fileIds, ...data } = input;
      data.assignees = await people(req, input.assigneeIds);
      data.attachments = await attachments(req, fileIds, old);
      await pin(req, fileIds);
      const result = await coll("Tasks").updateOne(
        {
          _id: old._id,
          ...filter(req),
          version: old.version,
          deletedAt: { $exists: false },
        },
        {
          $set: { ...data, updatedAt: new Date(), updatedBy: req.team.actorId },
          $inc: { version: 1 },
        },
      );
      if (!result.modifiedCount)
        fail("Задача изменилась. Откройте её заново", 409);
      notify(req);
      onTasksChanged(req);
      const task = (await summaries(req, [old._id]))[0];
      res.json({ task, calendarTask: calendarTask(task) });
    }),
  );
  router.delete(
    "/tasks/:id",
    wrap(async (req, res) => {
      const session = db().client.startSession();
      let replayed = false;
      try {
        await session.withTransaction(async () => {
          const task = await load(req, req.params.id, true, { session });
          if (!req.team.owner && task.creatorId !== req.team.actorId)
            fail("Удалять задачу может её автор или владелец", 403);
          if (task.deletedAt) {
            replayed = true;
            return;
          }
          if (await coll("Tasks").findOne(
            { ...filter(req), parentTaskId: task._id, deletedAt: { $exists: false } },
            { session, projection: { _id: 1 } },
          )) fail("Сначала удалите подзадачи", 409);
          if (
            !Number.isInteger(req.body.expectedVersion) ||
            req.body.expectedVersion !== task.version
          )
            fail("Задача изменилась. Откройте её заново", 409);
          const at = new Date();
          const result = await coll("Tasks").updateOne(
            {
              _id: task._id,
              ...filter(req),
              version: task.version,
              deletedAt: { $exists: false },
            },
            {
              $set: {
                deletedAt: at,
                deletedBy: req.team.actorId,
                updatedAt: at,
              },
              $inc: { version: 1 },
            },
            { session },
          );
          if (!result.modifiedCount)
            fail("Задача изменилась. Откройте её заново", 409);
          await coll("Messages").updateMany(
            { ...filter(req), taskId: task._id, deletedAt: { $exists: false } },
            {
              $set: {
                deletedAt: at,
                deletedBy: req.team.actorId,
                updatedAt: at,
                deletedWithTask: task._id,
                taskDeletionVersion: task.version + 1,
              },
            },
            { session },
          );
        });
      } finally {
        await session.endSession();
      }
      notify(req);
      onTasksChanged(req);
      res.json({ deletedTaskId: req.params.id, replayed });
    }),
  );
  router.post('/tasks/:id/restore', wrap(async (req, res) => {
    if (!Number.isInteger(req.body.expectedVersion) || req.body.expectedVersion < 0) fail('Укажите версию удалённой задачи');
    const session = db().client.startSession();
    let replayed = false;
    try {
      await session.withTransaction(async () => {
        const task = await load(req, req.params.id, true, { session });
        if (!req.team.owner && task.creatorId !== req.team.actorId) fail('Восстанавливать задачу может её автор или владелец', 403);
        if (!task.deletedAt && task.restoredFromVersion === req.body.expectedVersion) { replayed = true; return; }
        if (!task.deletedAt || !Number.isInteger(req.body.expectedVersion) || task.version !== req.body.expectedVersion) fail('Задача изменилась. Обновите список', 409);
        if (task.parentTaskId) await guardParent(req, task.parentTaskId, session);
        const at = new Date();
        const result = await coll('Tasks').updateOne({ _id: task._id, ...filter(req), version: task.version, deletedAt: task.deletedAt }, {
          $unset: { deletedAt: '', deletedBy: '' },
          $set: { updatedAt: at, restoredAt: at, restoredBy: req.team.actorId, restoredFromVersion: task.version }, $inc: { version: 1 },
        }, { session });
        if (!result.modifiedCount) fail('Задача изменилась. Обновите список', 409);
        await coll('Messages').updateMany({ ...filter(req), taskId: task._id, $or: [
          { deletedWithTask: task._id, taskDeletionVersion: task.version },
          // Earlier task deletions used a shared timestamp without a cascade marker.
          { deletedWithTask: { $exists: false }, deletedAt: task.deletedAt, deletedBy: task.deletedBy },
        ] }, { $unset: { deletedAt: '', deletedBy: '', deletedWithTask: '', taskDeletionVersion: '' }, $set: { updatedAt: at } }, { session });
      });
    } finally { await session.endSession(); }
    const task = (await summaries(req, [req.params.id]))[0];
    if (!replayed) { notify(req); onTasksChanged(req); }
    res.json({ task, calendarTask: calendarTask(task), restored: true, replayed });
  }));
  return { load, summaries };
}
module.exports = { registerTaskRoutes, taskInput, uuid };
