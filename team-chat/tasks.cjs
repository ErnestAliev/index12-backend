"use strict";
const { createHash } = require("node:crypto");
const { fail, text } = require("./domain.cjs");
const { dateOnly } = require("../business/task-domain.cjs");
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
  };
}
function registerTaskRoutes(router, { coll, wrap, notify, User }) {
  const filter = (req) => ({ workspaceId: req.team.workspaceId });
  async function load(req, id = req.params.id) {
    if (!uuid(id)) fail("Задача недоступна", 404);
    const task = await coll("Tasks").findOne({ _id: id, ...filter(req) });
    if (!task) fail("Задача недоступна", 404);
    return task;
  }
  async function summaries(req, ids) {
    const match = { ...filter(req), ...(ids ? { _id: { $in: ids } } : {}) };
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
        { ...filter(req), _id: { $in: ids }, status: "active" },
        { projection: { _id: 1, name: 1 } },
      )
      .toArray();
    const ownerId = "owner:" + req.team.workspace.userId;
    if (ids.includes(ownerId)) {
      const owner = User
        ? await User.findById(req.team.workspace.userId).select("name").lean()
        : null;
      staff.push({
        _id: ownerId,
        name: owner?.name || (req.team.owner ? req.team.name : "Владелец"),
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
      const fingerprint = createHash("sha256")
        .update(JSON.stringify(input))
        .digest("hex");
      let row = await coll("Tasks").findOne({ _id: id });
      if (
        row &&
        (row.workspaceId !== req.team.workspaceId ||
          row.creatorId !== req.team.actorId ||
          row.createHash !== fingerprint)
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
          await coll("Tasks").insertOne(row);
        } catch (e) {
          if (e.code === 11000) fail("Задача уже создана. Обновите чат", 409);
          throw e;
        }
      }
      // The deterministic cover id makes a retry repair an interrupted creation.
      await coll("Messages").updateOne(
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
      res.status(201).json({ task: (await summaries(req, [id]))[0] });
    }),
  );
  router.patch(
    "/tasks/:id",
    wrap(async (req, res) => {
      const old = await load(req);
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
        { _id: old._id, ...filter(req), version: old.version },
        {
          $set: { ...data, updatedAt: new Date(), updatedBy: req.team.actorId },
          $inc: { version: 1 },
        },
      );
      if (!result.modifiedCount)
        fail("Задача изменилась. Откройте её заново", 409);
      notify(req);
      res.json({ task: (await summaries(req, [old._id]))[0] });
    }),
  );
  return { load, summaries };
}
module.exports = { registerTaskRoutes, taskInput, uuid };
