"use strict";
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
module.exports = async ({
  api,
  db,
  employee,
  director,
  scope,
  origin,
  cookie,
  inviteToken,
}) => {
  const owner = "owner:" + scope.actorId,
    employeeId = employee.member._id;
  const input = {
    id: randomUUID(),
    description: "Сверить документы в Энергосбыте",
    assigneeIds: [employeeId],
    timing: "none",
  };
  const created = await api("/tasks", input, "POST", employee.auth);
  assert.equal(created.status, 201, JSON.stringify(created));
  assert.equal(created.data.task.creatorId, employeeId);
  assert.equal(created.data.task.dueDate, null);
  assert.equal(
    (await api("/tasks", input, "POST", employee.auth)).status,
    201,
    "retry is idempotent",
  );
  assert.equal(
    await db
      .collection("teamChatMessages")
      .countDocuments({ taskId: input.id, kind: "task" }),
    1,
  );
  assert.equal(
    (
      await api(
        "/tasks",
        { ...input, description: "changed" },
        "POST",
        employee.auth,
      )
    ).status,
    409,
  );
  const malicious = await api(
    "/tasks",
    { ...input, id: randomUUID(), assigneeIds: ["owner:another-workspace"] },
    "POST",
    employee.auth,
  );
  assert.equal(malicious.status, 404);
  const changed = await api(
    "/tasks/" + input.id,
    {
      expectedVersion: 0,
      description: "Сверить документы и забрать акт",
      timing: "deadline",
      dueDate: "2026-11-15",
      dueTime: "18:05",
    },
    "PATCH",
    employee.auth,
  );
  assert.equal(changed.status, 200, JSON.stringify(changed));
  assert.equal(
    changed.data.task.creatorId,
    employeeId,
    "creator cannot be replaced",
  );
  assert.equal(
    (
      await api(
        "/tasks/" + input.id,
        { expectedVersion: 0, status: "done" },
        "PATCH",
        employee.auth,
      )
    ).status,
    409,
    "stale edit fails",
  );
  assert.equal(
    (
      await api(
        "/tasks/" + input.id,
        { expectedVersion: 1, status: "done" },
        "PATCH",
        director.auth,
      )
    ).status,
    403,
    "unrelated participant cannot edit",
  );
  const comment = {
    id: randomUUID(),
    taskId: input.id,
    text: "@Calendar owner Во сколько привезти?",
    mentionIds: [owner],
  };
  assert.equal(
    (await api("/messages", comment, "POST", employee.auth)).status,
    201,
  );
  assert.equal(
    (await api("/messages", comment, "POST", employee.auth)).status,
    200,
    "comment retry is idempotent",
  );
  let row = (await api("/tasks/" + input.id, undefined, "GET")).data.task;
  assert.equal(row.unreadCount, 1);
  assert.equal(row.mentioned, true);
  assert.equal(row.mentionMessageId, comment.id);
  const main = (await api("/state", undefined, "GET")).data;
  assert.ok(
    main.messages.some((m) => m.kind === "task" && m.taskId === input.id),
  );
  assert.ok(
    !main.messages.some((m) => m._id === comment.id),
    "comments are not mixed into the main chat",
  );
  assert.equal(Object.hasOwn(main, "requests"), false);
  const thread = (await api("/messages?taskId=" + input.id, undefined, "GET"))
    .data;
  assert.deepEqual(
    thread.messages.map((m) => m._id),
    [comment.id],
  );
  await api("/messages/read", { ids: [comment.id] });
  row = (await api("/tasks/" + input.id, undefined, "GET")).data.task;
  assert.equal(row.unreadCount, 0);
  assert.equal(row.mentioned, true, "mention remains until a reply");
  const reply = {
    id: randomUUID(),
    taskId: input.id,
    text: "Привозите к 15:00",
  };
  assert.equal((await api("/messages", reply)).status, 201);
  row = (await api("/tasks/" + input.id, undefined, "GET")).data.task;
  assert.equal(row.mentioned, false);
  assert.equal(row.unreadCount, 0, "own messages are not unread");
  assert.equal(
    (await api("/tasks/" + input.id, undefined, "GET", employee.auth)).data.task
      .unreadCount,
    1,
  );
  const topMessage = { id: randomUUID(), text: "Общий чат" };
  await api("/messages", topMessage);
  assert.equal(
    (
      await api("/messages", {
        id: randomUUID(),
        taskId: input.id,
        text: "bad reply",
        replyToId: topMessage.id,
      })
    ).status,
    404,
    "cross-thread replies rejected",
  );
  assert.equal(
    (
      await api("/messages", {
        id: randomUUID(),
        taskId: randomUUID(),
        text: "bad task",
      })
    ).status,
    404,
  );
  assert.equal(
    (await api("/messages/" + input.id, {}, "DELETE")).status,
    409,
    "cover cannot be deleted as a plain message",
  );
  const upload = await fetch(origin + "/api/team-chat/files", {
    method: "POST",
    headers: {
      Cookie: "connect.sid=" + cookie,
      "Content-Type": "application/octet-stream",
      "x-file-name": encodeURIComponent("Документ.pdf"),
    },
    body: Buffer.from("%PDF-1.4\nfixture"),
  });
  assert.equal(upload.status, 201);
  const file = (await upload.json()).file;
  const withFile = await api("/tasks", {
    id: randomUUID(),
    description: "Прочитать договор",
    assigneeIds: [owner, employeeId],
    timing: "deadline",
    dueDate: "2026-11-20",
    fileIds: [file.fileId],
  });
  assert.equal(withFile.status, 201, JSON.stringify(withFile));
  assert.equal(withFile.data.task.attachments[0].name, "Документ.pdf");
  assert.equal(
    (await db.collection("teamChatFiles").findOne({ _id: file.fileId }))
      .expiresAt,
    undefined,
  );
  const stolen = await api(
    "/tasks",
    {
      id: randomUUID(),
      description: "stolen draft file",
      assigneeIds: [employeeId],
      timing: "none",
      fileIds: [file.fileId],
    },
    "POST",
    employee.auth,
  );
  assert.equal(stolen.status, 404, "cannot take another actor upload");
  // Over 50 comments exercises the actual history cursor, not a truncated preview.
  const bulk = Array.from({ length: 61 }, (_, i) => ({
    _id: randomUUID(),
    workspaceId: scope.workspaceId,
    taskId: input.id,
    authorId: employeeId,
    authorName: "Employee",
    text: "История " + i,
    attachments: [],
    createdAt: new Date(Date.now() + i + 1000),
    updatedAt: new Date(),
    version: 0,
    contentVersion: 0,
  }));
  await db.collection("teamChatMessages").insertMany(bulk);
  const page = (await api("/messages?taskId=" + input.id, undefined, "GET"))
    .data;
  assert.equal(page.messages.length, 50);
  assert.equal(page.hasMore, true);
  const older = (
    await api(
      "/messages?taskId=" + input.id + "&before=" + page.messages[0]._id,
      undefined,
      "GET",
    )
  ).data;
  assert.equal(older.messages.length, 13);
  assert.equal(older.hasMore, false);
  assert.equal(
    (await api("/requests", {}, "POST")).status,
    404,
    "removed expense endpoint",
  );
  // Stored legacy requests neither leak into chat nor reserve financial/calendar state.
  const legacyId = randomUUID();
  await db
    .collection("teamChatMessages")
    .insertOne({
      _id: legacyId,
      workspaceId: scope.workspaceId,
      kind: "request",
      createdAt: new Date(Date.now() + 100000),
      attachments: [],
    });
  assert.ok(
    !(await api("/state", undefined, "GET")).data.messages.some(
      (m) => m._id === legacyId,
    ),
  );
  const foreign = randomUUID();
  await db
    .collection("teamChatTasks")
    .insertOne({
      _id: foreign,
      workspaceId: randomUUID(),
      creatorId: employeeId,
      assigneeIds: [employeeId],
      createdAt: new Date(),
    });
  assert.equal(
    (await api("/tasks/" + foreign, undefined, "GET", employee.auth)).status,
    404,
  );
  // Keep browser test credentials in a temporary file only when explicitly running the UI fixture.
  if (process.env.INDEX12_TASK_BROWSER_FIXTURE) {
    require("node:fs").writeFileSync(
      process.env.INDEX12_TASK_BROWSER_FIXTURE,
      JSON.stringify({ origin, cookie, scope, inviteToken, taskId: input.id }),
    );
    console.log("TASK_BROWSER_READY");
    await new Promise((resolve) => process.on("SIGTERM", resolve));
  }
  console.log(
    "PASS: real chat auth, tasks, optimistic edits, comments, mentions, per-actor unread, files, history and workspace isolation",
  );
};
