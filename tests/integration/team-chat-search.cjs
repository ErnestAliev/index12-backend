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
  await db.collection("teamChatMessages").insertOne({
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
  assert.equal(result.data.index, 0);
  assert.equal(result.data.messageId, ids[0]);
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
  await api(
    "/messages",
    { id: randomUUID(), text: marker + " ещё", taskId },
    "POST",
    employee.auth,
  );
  result = await api(path + "&index=2", undefined, "GET", employee.auth);
  assert.equal(
    result.data.total,
    3,
    "discussion matches are grouped under one task card",
  );
  assert.equal(
    result.data.taskId,
    null,
    "global search never navigates into a task automatically",
  );
  assert.equal(result.data.messageId, taskId);
  assert.equal(result.data.taskMatch.count, 2);
  assert.ok(result.data.taskMatch.preview.includes(marker));
  assert.ok(result.data.messages.every((m) => !m.taskId || m.kind === "task"));
  result = await api(
    path + "&taskId=" + taskId,
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(
    result.data.total,
    2,
    "search within a task returns only its own matching discussion messages",
  );
  assert.equal(result.data.taskId, taskId);
  assert.equal(result.data.task._id, taskId);
  const fileMessageId = randomUUID(),
    discussionFile = "Рабочая таблица ABC.xlsx";
  await db
    .collection("teamChatMessages")
    .insertOne({
      ...records[0],
      _id: fileMessageId,
      taskId,
      text: "Документ для обсуждения",
      attachments: [{ fileId: randomUUID(), name: discussionFile }],
    });
  result = await api(
    "/search?q=" + encodeURIComponent(discussionFile),
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(result.data.total, 1);
  assert.equal(result.data.messageId, taskId);
  assert.equal(result.data.taskId, null);
  assert.equal(result.data.taskMatch.preview, discussionFile);
  result = await api(
    "/search?q=" + encodeURIComponent(discussionFile) + "&taskId=" + taskId,
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(result.data.total, 1);
  assert.equal(result.data.messageId, fileMessageId);
  assert.equal(result.data.taskId, taskId);
  const priorityId = randomUUID();
  await api(
    "/tasks",
    {
      id: priorityId,
      description: marker,
      assigneeIds: [employee.member._id],
      timing: "none",
    },
    "POST",
    employee.auth,
  );
  result = await api(path + "&index=2", undefined, "GET", employee.auth);
  assert.equal(
    result.data.messageId,
    priorityId,
    "task body matches precede discussion-only matches even when newer",
  );
  result = await api(path + "&index=3", undefined, "GET", employee.auth);
  assert.equal(result.data.messageId, taskId);
  await db
    .collection("teamChatTasks")
    .updateOne({ _id: priorityId }, { $set: { deletedAt: new Date() } });
  // A task cover stores empty text; its current task body must still count.
  const taskDescription = "Снять показания счётчиков",
    coverId = taskId;
  await db.collection("teamChatTasks").updateOne(
    { _id: taskId },
    {
      $set: {
        description: taskDescription,
        attachments: [{ fileId: randomUUID(), name: "Отчёт показаний.xlsx" }],
      },
    },
  );
  await db
    .collection("teamChatMessages")
    .updateOne({ _id: coverId }, { $set: { text: "" } });
  for (const q of ["Показани", "показани", "ПОКАЗАНИ"]) {
    result = await api(
      "/search?q=" + encodeURIComponent(q),
      undefined,
      "GET",
      employee.auth,
    );
    assert.equal(
      result.data.total,
      1,
      q + ": task body counts once, without counting its unrelated replies",
    );
    assert.equal(result.data.messageId, coverId);
    assert.equal(result.data.taskId, null);
    assert.equal(result.data.task.description, taskDescription);
  }
  result = await api(
    "/search?q=" + encodeURIComponent("Отчёт показаний.xlsx"),
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(result.data.total, 1);
  assert.equal(result.data.messageId, coverId);
  assert.equal(result.data.taskMatch.preview, "Отчёт показаний.xlsx");
  result = await api(
    "/search?q=" +
      encodeURIComponent("Отчёт показаний.xlsx") +
      "&taskId=" +
      taskId,
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(result.data.total, 1);
  assert.equal(result.data.taskId, taskId);
  assert.equal(
    result.data.messages.find((m) => m._id === coverId).attachments[0].name,
    "Отчёт показаний.xlsx",
  );
  assert.equal(
    result.data.messages.find((m) => m._id === coverId).kind,
    "task-files",
  );
  result = await api(
    "/search?q=" + encodeURIComponent("Снять показания") + "&taskId=" + taskId,
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(
    result.data.total,
    0,
    "thread search does not duplicate task cover as a reply",
  );
  await db.collection("teamChatTasks").updateOne(
    { _id: taskId },
    {
      $set: {
        description: "Актуальное изменённое описание",
        attachments: [],
      },
    },
  );
  await db
    .collection("teamChatMessages")
    .updateOne({ _id: coverId }, { $set: { text: taskDescription } });
  result = await api(
    "/search?q=" + encodeURIComponent("Показани"),
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(
    result.data.total,
    0,
    "old cover text must not override the current task body",
  );
  result = await api(
    "/search?q=" + encodeURIComponent("изменённое описание"),
    undefined,
    "GET",
    employee.auth,
  );
  assert.equal(result.data.total, 1);
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
