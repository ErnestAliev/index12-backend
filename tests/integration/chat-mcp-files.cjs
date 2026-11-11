"use strict";
const assert = require("node:assert/strict");
const { randomUUID, createHash } = require("node:crypto");
module.exports = async ({
  runtime,
  db,
  employeeClient,
  directorToken,
  managerToken,
  request,
  ownerId,
  workspaceId,
  ownId,
  otherId,
  commonId,
  privateMessageId,
  people,
  headers,
}) => {
  const call = async (name, args = {}, owner = false) => {
    const result = owner
      ? await runtime.invoke(name, args)
      : await employeeClient.callTool({ name, arguments: args });
    assert.equal(result.isError, undefined, JSON.stringify(result));
    return result.structuredContent || result;
  };
  const bad = async (name, args, owner = false) => {
    const result = owner
      ? await runtime.invoke(name, args)
      : await employeeClient.callTool({ name, arguments: args });
    assert.equal(result.isError, true, JSON.stringify(result));
    return result;
  };
  const bytes = Buffer.from("%PDF-1.7\noriginal ESF fixture bytes"),
    id = randomUUID();
  const input = { id, name: "ЭСФ.pdf", dataBase64: bytes.toString("base64") };
  const upload = await call("index12_chat_upload_file", input);
  assert.equal(upload.file.mimeType, "application/pdf");
  assert.equal(
    upload.file.sha256,
    createHash("sha256").update(bytes).digest("hex"),
  );
  assert.equal((await call("index12_chat_upload_file", input)).replayed, true);
  assert.equal(
    await db.collection("teamChatFiles").countDocuments({ _id: id }),
    1,
  );
  assert.deepEqual(
    Buffer.from(
      (await db.collection("teamChatFiles").findOne({ _id: id })).data.buffer,
    ),
    bytes,
  );
  const meta = (await employeeClient.listTools()).tools.find(
    (t) => t.name === "index12_chat_upload_file",
  );
  assert.deepEqual(meta._meta["openai/fileParams"], ["file"]);
  assert.deepEqual(meta.inputSchema.properties.file.required, [
    "download_url",
    "file_id",
  ]);
  assert.ok(
    !(await employeeClient.listTools()).tools.some((t) =>
      t.name.startsWith("index12_codex_"),
    ),
  );
  await bad("index12_chat_upload_file", { ...input, name: "changed.pdf" });
  const secondId = randomUUID();
  await call("index12_chat_upload_file", {
    id: secondId,
    name: "договор.docx",
    dataBase64: Buffer.from("PK\x03\x04document").toString("base64"),
  });
  await call("index12_chat_attach_files", {
    id: ownId,
    expectedVersion: 0,
    fileIds: [id],
  });
  await call("index12_chat_attach_files", {
    id: ownId,
    expectedVersion: 1,
    fileIds: [secondId],
  });
  assert.deepEqual(
    (await call("index12_chat_get_task", { id: ownId })).task.attachments.map(
      (f) => f.fileId,
    ),
    [id, secondId],
  );
  await bad("index12_chat_attach_files", {
    id: ownId,
    expectedVersion: 0,
    fileIds: [id],
  });
  await bad("index12_chat_attach_files", {
    id: otherId,
    expectedVersion: 0,
    fileIds: [id],
  });
  await call("index12_chat_attach_files", {
    id: ownId,
    expectedVersion: 2,
    action: "remove",
    fileIds: [id],
  });
  assert.equal(
    (await call("index12_chat_get_task", { id: ownId })).task.attachments
      .length,
    1,
  );
  assert.equal(
    (await db.collection("teamChatFiles").findOne({ _id: secondId })).expiresAt,
    undefined,
  );
  const messageId = randomUUID();
  const message = await call("index12_chat_send_message", {
    id: messageId,
    fileIds: [id],
    mentionIds: ["owner:" + ownerId],
  });
  assert.equal(message.message.text, "");
  assert.equal(message.message.attachments[0].fileId, id);
  assert.equal(
    (
      await call("index12_chat_send_message", {
        id: messageId,
        fileIds: [id],
        mentionIds: ["owner:" + ownerId],
      })
    ).replayed,
    true,
  );
  await call("index12_chat_edit_message", {
    id: messageId,
    expectedVersion: 0,
    fileIds: [id, secondId],
  });
  assert.equal(
    (await call("index12_chat_get_message", { id: messageId })).message
      .attachments.length,
    2,
  );
  await bad("index12_chat_edit_message", {
    id: messageId,
    expectedVersion: 0,
    text: "stale",
  });
  await bad("index12_chat_edit_message", {
    id: messageId,
    expectedVersion: 1,
    fileIds: [],
  });
  await call("index12_chat_react", { id: commonId, emoji: "👍" });
  assert.ok(
    (
      await call("index12_chat_get_message", { id: commonId })
    ).message.reactions.some((r) => r.emoji === "👍" && r.mine),
  );
  await call("index12_chat_react", { id: commonId, emoji: "❤️" });
  await call("index12_chat_react", { id: commonId, emoji: null });
  await bad("index12_chat_react", { id: privateMessageId, emoji: "👍" });
  await call("index12_chat_mark_read", { ids: [commonId, privateMessageId] });
  assert.equal(
    await db
      .collection("teamChatMessageReads")
      .countDocuments({
        actorId: people.employee._id,
        messageId: privateMessageId,
      }),
    0,
  );
  assert.equal(
    await db
      .collection("teamChatMessageReads")
      .countDocuments({ actorId: people.employee._id, messageId: commonId }),
    1,
  );
  const forwardedId = randomUUID();
  await call("index12_chat_forward_message", {
    id: messageId,
    newId: forwardedId,
    expectedVersion: 1,
    taskId: ownId,
  });
  const forwarded = (
    await call("index12_chat_get_message", { id: forwardedId })
  ).message;
  assert.equal(forwarded.taskId, ownId);
  assert.equal(forwarded.attachments.length, 2);
  const resource = await employeeClient.callTool({
    name: "index12_chat_read_file",
    arguments: { id: forwarded.attachments[0].fileId },
  });
  assert.deepEqual(
    Buffer.from(resource.content[0].resource.blob, "base64"),
    bytes,
  );
  // A task image stays metadata-only until the current user has allowed its inspection.
  const photoId = randomUUID(), photoBytes = Buffer.concat([Buffer.from([137,80,78,71,13,10,26,10]), Buffer.from('synthetic task source')]);
  await call('index12_chat_upload_file', { id: photoId, name: 'invoice.png', mimeType: 'image/png', dataBase64: photoBytes.toString('base64') });
  await call('index12_chat_send_message', { id: randomUUID(), taskId: ownId, text: 'Счёт', fileIds: [photoId] });
  const pendingPhoto = await employeeClient.callTool({ name: 'index12_chat_read_file', arguments: { id: photoId } });
  assert.equal(pendingPhoto.structuredContent.mediaPermissionRequired, true);
  assert.equal(pendingPhoto.structuredContent.file.name, 'invoice.png');
  assert.ok(!pendingPhoto.content.some(part => part.type === 'resource'));
  const permittedPhoto = await employeeClient.callTool({ name: 'index12_chat_read_file', arguments: { id: photoId, mediaPermission: 'user_confirmed' } });
  assert.deepEqual(Buffer.from(permittedPhoto.content[0].resource.blob, 'base64'), photoBytes);

  assert.equal(
    (
      await call("index12_chat_forward_message", {
        id: messageId,
        newId: forwardedId,
        expectedVersion: 1,
        taskId: ownId,
      })
    ).replayed,
    true,
  );
  await bad("index12_chat_delete_message", {
    id: commonId,
    expectedVersion: 0,
  });
  await bad("index12_chat_delete_message", {
    id: messageId,
    expectedVersion: 0,
  });
  await call("index12_chat_delete_message", {
    id: messageId,
    expectedVersion: 1,
  });
  const taskId = randomUUID();
  await call("index12_chat_create_task", {
    id: taskId,
    description: "Restore fixture",
    assigneeIds: [people.employee._id],
    fileIds: [id],
  });
  await call("index12_chat_delete_task", { id: taskId, expectedVersion: 0 });
  assert.ok(
    (await call("index12_chat_tasks", { deleted: true })).tasks.some(
      (t) => t._id === taskId,
    ),
  );
  assert.equal(
    (await call("index12_chat_get_task", { id: taskId, includeDeleted: true }))
      .task.version,
    1,
  );
  await call("index12_chat_restore_task", { id: taskId, expectedVersion: 1 });
  assert.equal(
    (await call("index12_chat_get_task", { id: taskId })).task.version,
    2,
  );
  assert.equal(
    (
      await request("/api/team-chat/files/import", {
        method: "POST",
        token: directorToken,
        body: { ...input, id: randomUUID() },
      })
    ).status,
    403,
  );
  assert.equal(
    (await request("/api/mcp/codex/chats", { token: managerToken, headers }))
      .status,
    403,
  );
  // Owner private chat imports the actual file bytes from work chat; no worker or model is invoked.
  await db
    .collection("desktopWorkers")
    .insertOne({
      _id: ownerId + ":" + workspaceId,
      actorId: ownerId,
      workspaceId,
      seenAt: new Date(0),
    });
  const chatId = randomUUID();
  await call(
    "index12_codex_create_chat",
    { id: chatId, title: "Документы" },
    true,
  );
  assert.equal(
    (
      await call(
        "index12_codex_create_chat",
        { id: chatId, title: "Документы" },
        true,
      )
    ).replayed,
    true,
  );
  const copied = await call(
    "index12_codex_upload_file",
    { chatId, id: randomUUID(), chatFileId: id },
    true,
  );
  const codexResource = await runtime.invoke("index12_codex_read_file", {
    id: copied.file.fileId,
  });
  assert.equal(codexResource.isError, undefined, JSON.stringify(codexResource));
  assert.deepEqual(
    Buffer.from(codexResource.content[0].resource.blob, "base64"),
    bytes,
  );
  const send = {
    chatId,
    requestId: randomUUID(),
    message: "Сведения из 1С. Изучи приложенный документ.",
    fileIds: [copied.file.fileId],
  };
  const queued = await call("index12_codex_send_message", send, true);
  assert.equal(queued.job.status, "queued");
  assert.equal(queued.awaitingConnection, true);
  assert.equal(
    (await call("index12_codex_send_message", send, true)).job.id,
    queued.job.id,
  );
  assert.equal(
    (await call("index12_codex_job", { id: queued.job.id }, true)).job.status,
    "queued",
  );
  assert.equal(
    (await db.collection("desktopJobs").findOne({ _id: queued.job.id }))
      .attachments[0].fileId,
    copied.file.fileId,
  );
  const fromCodex = await call(
    "index12_chat_upload_file",
    { id: randomUUID(), desktopFileId: copied.file.fileId },
    true,
  );
  assert.equal(fromCodex.file.sha256, upload.file.sha256);
  await bad("index12_chat_upload_file", {
    id: randomUUID(),
    desktopFileId: copied.file.fileId,
  });
  await bad("index12_codex_read_file", { id: randomUUID() }, true);
  const large = await call(
    "index12_chat_upload_file",
    {
      id: randomUUID(),
      name: "max.bin",
      dataBase64: Buffer.alloc(10 * 1024 * 1024, 1).toString("base64"),
    },
    true,
  );
  assert.equal(large.file.size, 10 * 1024 * 1024);
  console.log(
    "PASS MCP files/actions: exact ESF bytes, 10MB transport, idempotency, task attachments, file-only messages, edits, reactions, explicit read, forward, delete/restore, private Codex queue and role boundaries",
  );
};
