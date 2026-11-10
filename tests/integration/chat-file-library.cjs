"use strict";
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
module.exports = async ({
  runtime,
  db,
  employeeClient,
  request,
  workspaceId,
  ownerId,
  people,
  headers,
  ownId,
  otherId,
}) => {
  const coll = (name) => db.collection("teamChat" + name),
    ids = Array.from({ length: 7 }, () => randomUUID()),
    projectId = "abcdefabcdefabcdefabcdef";
  const [common, own, privateFile, deleted, orphan, foreign, shared] = ids;
  const bytes = Buffer.from("fixture content only");
  for (const [index, id] of ids.entries())
    await coll("Files").insertOne({
      _id: id,
      workspaceId: id === foreign ? "foreign-workspace" : workspaceId,
      actorId: id === common ? people.manager._id : people.employee._id,
      name: `library-${index}.pdf`,
      mimeType: "application/pdf",
      size: bytes.length,
      data: bytes,
      ...(id !== common ? { createdAt: new Date("2026-11-10T06:00:00Z") } : {}),
    });
  const message = (fileId, taskId = null, extra = {}) => ({
    _id: randomUUID(),
    workspaceId,
    taskId,
    authorId: people.employee._id,
    authorName: "employee",
    text: "Library source " + fileId,
    createdAt: new Date("2026-11-10T07:00:00Z"),
    attachments: [{ fileId }],
    ...extra,
  });
  const commonMessage = message(common),
    sharedMessage = message(shared),
    removed = message(deleted, null, { deletedAt: new Date() });
  await coll("Messages").insertMany([
    commonMessage,
    message(own, ownId),
    message(privateFile, otherId),
    removed,
    message(foreign),
    sharedMessage,
    message(shared, ownId),
  ]);
  await coll("Tasks").updateOne(
    { _id: ownId },
    {
      $set: {
        projectId,
        projectName: "Системный проект",
        attachments: [{ fileId: shared }],
      },
    },
  );
  const call = async (name, args = {}, owner = false) => {
    const result = owner
      ? await runtime.invoke(name, args)
      : await employeeClient.callTool({ name, arguments: args });
    assert.equal(result.isError, undefined, JSON.stringify(result));
    return result.structuredContent || result;
  };
  const employee = await call("index12_chat_files");
  for (const id of [common, own, shared])
    assert.ok(
      employee.files.some((file) => file.fileId === id),
      id,
    );
  for (const id of [privateFile, deleted, orphan, foreign])
    assert.ok(!JSON.stringify(employee).includes(id), id + " must stay hidden");
  assert.ok(!JSON.stringify(employee).includes("fixture content only"));
  assert.ok(!JSON.stringify(employee).includes("password"));
  assert.equal(
    employee.files.find((file) => file.fileId === common).createdAt,
    "2026-11-10T07:00:00.000Z",
    "historical upload inherits first source date",
  );
  assert.equal(
    employee.files.find((file) => file.fileId === shared).sources.length,
    3,
    "deduplicated file keeps each source",
  );
  assert.ok(
    (await call("index12_chat_files", {}, true)).files.some(
      (file) => file.fileId === privateFile,
    ),
  );
  const project = await call("index12_chat_files", { projectId });
  assert.ok(project.files.some((file) => file.fileId === own));
  assert.ok(!project.files.some((file) => file.fileId === common));
  assert.ok(
    (await call("index12_chat_files", { projectId: "__none__" })).files.some(
      (file) => file.fileId === shared,
    ),
  );
  const page1 = await call("index12_chat_files", { limit: 1 }),
    page2 = await call("index12_chat_files", { limit: 1, offset: 1 });
  assert.notEqual(page1.files[0].fileId, page2.files[0].fileId);
  const forbidden = await employeeClient.callTool({
    name: "index12_chat_update_file",
    arguments: { id: common, expectedVersion: 0, tags: ["important"] },
  });
  assert.equal(forbidden.isError, true);
  const saved = await call("index12_chat_update_file", {
    id: own,
    expectedVersion: 0,
    tags: ["important", "review"],
    description: "Описание файла",
  });
  assert.equal(saved.version, 1);
  assert.equal(
    (
      await call("index12_chat_files", {
        tag: "important",
        q: "Описание файла",
      })
    ).files.length,
    1,
  );
  const conflict = await employeeClient.callTool({
    name: "index12_chat_update_file",
    arguments: { id: own, expectedVersion: 0, tags: [] },
  });
  assert.equal(conflict.isError, true);
  const results = await Promise.all([
    call("index12_chat_update_file", {
      id: own,
      expectedVersion: 1,
      tags: ["archive"],
    }).catch(() => null),
    call("index12_chat_update_file", {
      id: own,
      expectedVersion: 1,
      description: "Concurrent",
    }).catch(() => null),
  ]);
  assert.equal(
    results.filter(Boolean).length,
    1,
    "only one concurrent version wins",
  );
  for (const id of [privateFile, foreign, orphan]) {
    const result = await employeeClient.callTool({
      name: "index12_chat_files",
      arguments: { id },
    });
    assert.equal(result.isError, true);
  }
  assert.equal(
    await coll("FileViews").countDocuments({ workspaceId, fileId: common }),
    0,
    "listing and thumbnails do not create views",
  );
  const response = await request(
    "/api/team-chat/files/library/" + common + "/view",
    { method: "POST", headers: { ...headers, "x-test-owner": ownerId } },
  );
  assert.equal(response.status, 200);
  const viewed = (await call("index12_chat_files", { id: common }, true)).file;
  assert.deepEqual(
    viewed.viewedBy.map((view) => view.actorId),
    ["owner:" + ownerId],
  );
  assert.equal(
    (
      await request("/api/team-chat/files/library/" + foreign + "/view", {
        method: "POST",
        headers: { ...headers, "x-test-owner": ownerId },
      })
    ).status,
    404,
  );
  const goneTask = randomUUID(),
    goneFile = randomUUID();
  await coll("Files").insertOne({
    _id: goneFile,
    workspaceId,
    actorId: people.employee._id,
    name: "removed-task.txt",
    data: bytes,
  });
  await coll("Tasks").insertOne({
    _id: goneTask,
    workspaceId,
    deletedAt: new Date(),
    attachments: [{ fileId: goneFile }],
    creatorId: people.employee._id,
  });
  await coll("Messages").insertOne(message(goneFile, goneTask));
  assert.ok(
    !JSON.stringify(await call("index12_chat_files", {}, true)).includes(
      goneFile,
    ),
  );
  console.log(
    "File library: historical sources, deduplication, filters, pagination, workspace/task access, CAS tags and real view receipts passed",
  );
};
