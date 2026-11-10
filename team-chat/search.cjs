"use strict";
const { fail, text } = require("./domain.cjs");
const { taskAccess } = require("./mcp-access.cjs");
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
                  kind: { $ne: "request" },
                }
              : {}),
            $or: [
              { text: regex },
              { "attachments.name": regex },
              { kind: "task" },
            ],
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
                  ...taskAccess(req.team),
                  $expr: { $eq: ["$_id", "$$task"] },
                },
              },
              { $project: { _id: 1, description: 1, attachments: 1 } },
            ],
            as: "_searchTask",
          },
        },
        {
          $match: {
            $or: [{ taskId: null }, { "_searchTask.0": { $exists: true } }],
          },
        },
        {
          $match: {
            $or: [
              {
                kind: { $ne: "task" },
                $or: [{ text: regex }, { "attachments.name": regex }],
              },
              {
                kind: "task",
                $or: [
                  ...(req.query.taskId
                    ? []
                    : [{ "_searchTask.description": regex }]),
                  { "_searchTask.attachments.name": regex },
                ],
              },
            ],
          },
        },
      ];
      if (req.query.taskId)
        pipeline.push({
          $set: {
            kind: {
              $cond: [{ $eq: ["$kind", "task"] }, "task-files", "$kind"],
            },
            text: { $cond: [{ $eq: ["$kind", "task"] }, "", "$text"] },
            attachments: {
              $cond: [
                { $eq: ["$kind", "task"] },
                { $arrayElemAt: ["$_searchTask.attachments", 0] },
                "$attachments",
              ],
            },
          },
        });
      else
        pipeline.push(
          // Prefer a match in the current task body to matches in its discussion.
          {
            $set: {
              _searchPriority: {
                $cond: [
                  {
                    $and: [
                      { $ne: [{ $ifNull: ["$taskId", null] }, null] },
                      { $ne: ["$kind", "task"] },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
          },
          { $sort: { _searchPriority: 1, createdAt: 1, _id: 1 } },
          {
            $group: {
              _id: { $ifNull: ["$taskId", "$_id"] },
              _searchDocument: { $first: "$$ROOT" },
              _searchInsideCount: { $sum: "$_searchPriority" },
            },
          },
          {
            $lookup: {
              from: "teamChatMessages",
              let: { task: "$_searchDocument.taskId" },
              pipeline: [
                {
                  $match: {
                    workspaceId,
                    kind: "task",
                    deletedAt: { $exists: false },
                    $expr: { $eq: ["$taskId", "$$task"] },
                  },
                },
                { $limit: 1 },
              ],
              as: "_searchCover",
            },
          },
          {
            $replaceWith: {
              $mergeObjects: [
                {
                  $cond: [
                    { $eq: ["$_searchDocument._searchPriority", 1] },
                    { $arrayElemAt: ["$_searchCover", 0] },
                    "$_searchDocument",
                  ],
                },
                {
                  _searchPriority: "$_searchDocument._searchPriority",
                  _searchInsideCount: "$_searchInsideCount",
                  _searchInside: {
                    $cond: [
                      { $eq: ["$_searchDocument._searchPriority", 1] },
                      {
                        $cond: [
                          {
                            $regexMatch: {
                              input: { $ifNull: ["$_searchDocument.text", ""] },
                              regex,
                            },
                          },
                          "$_searchDocument.text",
                          {
                            $ifNull: [
                              {
                                $arrayElemAt: [
                                  {
                                    $map: {
                                      input: {
                                        $filter: {
                                          input: {
                                            $ifNull: [
                                              "$_searchDocument.attachments",
                                              [],
                                            ],
                                          },
                                          as: "file",
                                          cond: {
                                            $regexMatch: {
                                              input: {
                                                $ifNull: ["$$file.name", ""],
                                              },
                                              regex,
                                            },
                                          },
                                        },
                                      },
                                      as: "file",
                                      in: "$$file.name",
                                    },
                                  },
                                  0,
                                ],
                              },
                              "",
                            ],
                          },
                        ],
                      },
                      "",
                    ],
                  },
                },
              ],
            },
          },
          // A deleted/missing cover must never open an orphaned discussion.
          { $match: { _id: { $exists: true } } },
        );
      const [count] = await coll("Messages")
        .aggregate([...pipeline, { $count: "total" }], { maxTimeMS: 5000 })
        .toArray();
      const total = count?.total || 0;
      if (!total) return res.json({ total: 0, index: -1, messages: [] });
      const index = Math.min(requested ?? 0, total - 1);
      const [target] = await coll("Messages")
        .aggregate(
          [
            ...pipeline,
            {
              $sort: {
                ...(req.query.taskId ? {} : { _searchPriority: 1 }),
                createdAt: 1,
                _id: 1,
              },
            },
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
      const insideCount = target._searchInsideCount || 0;
      const insideText = target._searchInside || "";
      const offset = Math.max(
        0,
        insideText.toLocaleLowerCase().indexOf(query.toLocaleLowerCase()) - 45,
      );
      let taskMatch =
        target.kind === "task" && insideCount
          ? {
              taskId: target.taskId,
              count: insideCount,
              preview: insideText
                ? (offset ? "…" : "") +
                  insideText.slice(offset, offset + 180) +
                  (insideText.length > offset + 180 ? "…" : "")
                : "",
            }
          : null;
      delete target._searchPriority;
      delete target._searchInsideCount;
      delete target._searchInside;
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
      const taskFiles =
        target.kind === "task"
          ? (summaries[0]?.attachments || []).filter((file) =>
              regex.test(file.name || ""),
            )
          : [];
      if (taskFiles.length)
        taskMatch = {
          taskId: target.taskId,
          count: insideCount + taskFiles.length,
          preview: taskFiles
            .map((file) => file.name)
            .join(" / ")
            .slice(0, 180),
        };
      const liveIds = await coll("Tasks")
        .find(
          {
            workspaceId,
            deletedAt: { $exists: false },
            ...taskAccess(req.team),
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
        taskMatch,
      });
    }),
  );
}
module.exports = { registerSearchRoutes, escapeSearch: escape };
