"use strict";
const assert = require("node:assert/strict"),
  { randomUUID } = require("node:crypto");
module.exports = async ({ api, db, employee, scope }) => {
  const marker = "Поиск договора [.*]",
    ids = [randomUUID(), randomUUID()];
  const records = Array.from({ length: 80 }, (_, i) => ({
    _id: randomUUID(),
    workspaceId: scope.workspaceId,
    authorId: employee.member._id,
    authorName: "Employee",
    text: "Запись истории " + i,
    createdAt: new Date(Date.now() - 100000 + i * 1000),
    attachments: [],
  }));
  records[0] = { ...records[0], _id: ids[0], text: marker };
  records[79] = { ...records[79], _id: ids[1], text: marker.toLowerCase() };
  await db.collection("teamChatMessages").insertMany(records);
  await db
    .collection("teamChatMessages")
    .insertOne({
      ...records[0],
      _id: randomUUID(),
      workspaceId: "foreign-workspace",
    });
  await db
    .collection("teamChatMessages")
    .insertOne({ ...records[0], _id: randomUUID(), deletedAt: new Date() });
  const path = "/search?q=" + encodeURIComponent(marker);
  let result = await api(path, undefined, "GET", employee.auth);
  assert.equal(result.status, 200, JSON.stringify(result.data));
  assert.equal(result.data.total, 2);
  assert.equal(result.data.index, 1);
  assert.equal(result.data.messageId, ids[1]);
  result = await api(path + "&index=0", undefined, "GET", employee.auth);
  assert.equal(result.data.messageId, ids[0]);
  assert.ok(result.data.messages.length > 1, "result has neighboring messages");
  const taskId = randomUUID();
  await api(
    "/tasks",
    {
      id: taskId,
      description: "Search task",
      assigneeIds: [employee.member._id],
      timing: "none",
    },
    "POST",
    employee.auth,
  );
  await api(
    "/messages",
    { id: randomUUID(), text: marker, taskId },
    "POST",
    employee.auth,
  );
  result = await api(path, undefined, "GET", employee.auth);
  assert.equal(result.data.total, 3);
  assert.equal(result.data.taskId, taskId);
  assert.equal(result.data.task._id, taskId);
  await db
    .collection("teamChatTasks")
    .updateOne({ _id: taskId }, { $set: { deletedAt: new Date() } });
  result = await api(path, undefined, "GET", employee.auth);
  assert.equal(result.data.total, 2, "deleted task discussions stay hidden");
  assert.equal(
    (await api(path + "&index=-1", undefined, "GET", employee.auth)).status,
    400,
  );
  assert.equal(
    (await api("/search?q=" + "x".repeat(161), undefined, "GET", employee.auth))
      .status,
    400,
  );
  assert.equal(
    (
      await api(
        path + "&taskId=" + randomUUID(),
        undefined,
        "GET",
        employee.auth,
      )
    ).status,
    404,
  );
  result = await api(
    "/search?q=unmatchable-" + randomUUID(),
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(result.data.total, 0);
  await db
    .collection("teamChatMessages")
    .deleteMany({ _id: { $in: records.map((r) => r._id) } });
  console.log(
    "PASS: chat search across older history, literal punctuation, workspace isolation, deleted rows/tasks, context and counts",
  );
};
