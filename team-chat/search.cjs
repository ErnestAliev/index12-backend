"use strict";
const { fail, text } = require("./domain.cjs");
const escape = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
function registerSearchRoutes(router, { coll, wrap, tasks, decorate }) {
  router.get(
    "/search",
    wrap(async (req, res) => {
      const query = text(req.query.q || "", 160, false);
      const requested =
        req.query.index === undefined ? null : Number(req.query.index);
      if (
        requested !== null &&
        (!Number.isSafeInteger(requested) || requested < 0)
      )
        fail("Некорректное совпадение");
      if (req.query.taskId) await tasks.load(req, req.query.taskId);
      if (!query) return res.json({ total: 0, index: -1, messages: [] });
      const workspaceId = req.team.workspaceId;
      const regex = new RegExp(escape(query), "i");
      const pipeline = [
        {
          $match: {
            workspaceId,
            deletedAt: { $exists: false },
            kind: { $ne: "request" },
            ...(req.query.taskId
              ? {
                  taskId: req.query.taskId,
                  kind: { $nin: ["request", "task"] },
                }
              : {}),
            $or: [{ text: regex }, { "attachments.name": regex }],
          },
        },
        {
          $lookup: {
            from: "teamChatTasks",
            let: { task: "$taskId" },
            pipeline: [
              {
                $match: {
                  workspaceId,
                  deletedAt: { $exists: false },
                  $expr: { $eq: ["$_id", "$$task"] },
                },
              },
              { $project: { _id: 1 } },
            ],
            as: "_searchTask",
          },
        },
        {
          $match: {
            $or: [{ taskId: null }, { "_searchTask.0": { $exists: true } }],
          },
        },
      ];
      const [count] = await coll("Messages")
        .aggregate([...pipeline, { $count: "total" }], { maxTimeMS: 5000 })
        .toArray();
      const total = count?.total || 0;
      if (!total) return res.json({ total: 0, index: -1, messages: [] });
      const index = Math.min(requested ?? total - 1, total - 1);
      const [target] = await coll("Messages")
        .aggregate(
          [
            ...pipeline,
            { $sort: { createdAt: 1, _id: 1 } },
            { $skip: index },
            { $limit: 1 },
            {
              $project: { _searchTask: 0, editHistory: 0, approvalArchive: 0 },
            },
          ],
          { maxTimeMS: 5000 },
        )
        .toArray();
      if (!target) fail("Сообщение изменилось. Повторите поиск", 409);
      const scopeTaskId =
        target.taskId && target.kind !== "task" ? target.taskId : null;
      const filter = {
        workspaceId,
        deletedAt: { $exists: false },
        kind: { $nin: ["request", ...(scopeTaskId ? ["task"] : [])] },
        ...(scopeTaskId
          ? { taskId: scopeTaskId }
          : { $or: [{ taskId: null }, { kind: "task" }] }),
      };
      const before = {
        $or: [
          { createdAt: { $lt: target.createdAt } },
          { createdAt: target.createdAt, _id: { $lt: target._id } },
        ],
      };
      const after = {
        $or: [
          { createdAt: { $gt: target.createdAt } },
          { createdAt: target.createdAt, _id: { $gt: target._id } },
        ],
      };
      const projection = { editHistory: 0, approvalArchive: 0 };
      const [older, newer, summaries] = await Promise.all([
        coll("Messages")
          .find({ ...filter, $and: [before] }, { projection })
          .sort({ createdAt: -1, _id: -1 })
          .limit(12)
          .toArray(),
        coll("Messages")
          .find({ ...filter, $and: [after] }, { projection })
          .sort({ createdAt: 1, _id: 1 })
          .limit(12)
          .toArray(),
        target.taskId ? tasks.summaries(req, [target.taskId]) : [],
      ]);
      const liveIds = await coll("Tasks")
        .find(
          {
            workspaceId,
            deletedAt: { $exists: false },
            _id: {
              $in: [...older, ...newer]
                .filter((m) => m.kind === "task")
                .map((m) => m.taskId),
            },
          },
          { projection: { _id: 1 } },
        )
        .toArray();
      const alive = new Set(liveIds.map((t) => t._id));
      const messages = [...older.reverse(), target, ...newer].filter(
        (m) => m.kind !== "task" || m._id === target._id || alive.has(m.taskId),
      );
      await decorate(req, messages);
      res.json({
        total,
        index,
        messageId: target._id,
        taskId: scopeTaskId,
        messages,
        task: summaries[0] || null,
      });
    }),
  );
}
module.exports = { registerSearchRoutes, escapeSearch: escape };
