"use strict";
const { createHash } = require("node:crypto");
const { z } = require("zod");
const {
  safeName,
  imageMime,
  fileBuffer,
  fileMeta,
  FILE_LIMIT,
} = require("../codex/files.cjs");
const { downloadFile } = require("../business/onec/purchase-files.cjs");
const error = (message, status = 400) =>
  Object.assign(new Error(message), { status });
const openaiFile = z
  .object({
    download_url: z.string().url().max(16000),
    file_id: z.string().min(1).max(300),
    mime_type: z.string().max(200).optional(),
    file_name: z.string().max(1000).optional(),
  })
  .strict();
const uploadFields = {
  id: z.string().uuid(),
  file: openaiFile.optional(),
  name: z.string().min(1).max(1000).optional(),
  mimeType: z.string().max(200).optional(),
  dataBase64: z
    .string()
    .min(1)
    .max(Math.ceil(FILE_LIMIT / 3) * 4)
    .optional(),
  desktopFileId: z.string().uuid().optional(),
  chatFileId: z
    .union([z.string().uuid(), z.string().regex(/^forward:[a-f\d]{64}$/)])
    .optional(),
};
const uploadSchema = z
  .object(uploadFields)
  .strict()
  .refine(
    (body) =>
      ["file", "dataBase64", "desktopFileId", "chatFileId"].filter(
        (key) => body[key] !== undefined,
      ).length === 1,
    "Укажите один источник файла",
  );
function importedBytes(data, name, requestedType) {
  if (!Buffer.isBuffer(data) || !data.length || data.length > FILE_LIMIT)
    throw error("Непустой файл — до 10 МБ");
  name = safeName(name);
  if (!name) throw error("Укажите имя файла");
  const mimeType =
    imageMime(data) ||
    (data.subarray(0, 5).toString() === "%PDF-"
      ? "application/pdf"
      : typeof requestedType === "string" &&
          /^[\w.+-]+\/[\w.+-]+$/.test(requestedType) &&
          !requestedType.startsWith("image/")
        ? requestedType
        : "application/octet-stream");
  return {
    name,
    mimeType,
    size: data.length,
    data,
    sha256: createHash("sha256").update(data).digest("hex"),
  };
}
async function accessibleChatFile(db, team, id) {
  const row = await db
    .collection("teamChatFiles")
    .findOne({ _id: id, workspaceId: team.workspaceId });
  if (!row) throw error("Файл недоступен", 404);
  if (row.actorId === team.actorId) return row;
  const { taskAccess } = require("../team-chat/mcp-access.cjs");
  const ids = (
    await db
      .collection("teamChatTasks")
      .find(
        {
          workspaceId: team.workspaceId,
          deletedAt: { $exists: false },
          ...taskAccess(team),
        },
        { projection: { _id: 1 } },
      )
      .toArray()
  ).map((t) => t._id);
  const message = await db
    .collection("teamChatMessages")
    .findOne(
      {
        workspaceId: team.workspaceId,
        deletedAt: { $exists: false },
        "attachments.fileId": id,
        $or: [{ taskId: null }, { taskId: { $in: ids } }],
      },
      { projection: { _id: 1 } },
    );
  const task = message
    ? null
    : await db
        .collection("teamChatTasks")
        .findOne(
          {
            workspaceId: team.workspaceId,
            _id: { $in: ids },
            "attachments.fileId": id,
          },
          { projection: { _id: 1 } },
        );
  if (!message && !task) throw error("Файл недоступен", 404);
  return row;
}
async function importFile(
  body,
  {
    db,
    collection,
    scope,
    team,
    desktopScope,
    chatId,
    download = downloadFile,
  },
) {
  const parsed = uploadSchema.safeParse(body);
  if (!parsed.success) throw error(parsed.error.issues[0].message);
  body = parsed.data;
  // A stable source identity makes retries work even after a temporary download URL expires.
  const sourceKey = createHash("sha256")
    .update(
      JSON.stringify([
        body.file?.file_id ||
          body.desktopFileId ||
          body.chatFileId ||
          body.dataBase64,
        body.name || body.file?.file_name || null,
        body.mimeType || body.file?.mime_type || null,
      ]),
    )
    .digest("hex");
  const old = await collection.findOne({ _id: body.id });
  function replay(row) {
    if (
      !row ||
      Object.entries(scope).some(([key, value]) => row[key] !== value) ||
      row.sourceKey !== sourceKey ||
      (chatId && row.chatId !== chatId)
    )
      throw error("Ключ файла уже использован", 409);
    return { file: { ...fileMeta(row), sha256: row.sha256 }, replayed: true };
  }
  if (old) return replay(old);
  let data,
    name = body.name || body.file?.file_name,
    mimeType = body.mimeType || body.file?.mime_type;
  if (body.file) {
    try {
      data = await download(body.file);
    } catch {
      throw error(
        "Не удалось получить файл по защищённой ссылке. Передайте актуальный файл заново",
      );
    }
  } else if (body.dataBase64 !== undefined) {
    data = Buffer.from(body.dataBase64, "base64");
    if (data.toString("base64") !== body.dataBase64)
      throw error("Некорректное содержимое base64");
  } else {
    let source;
    if (body.desktopFileId) {
      if (!desktopScope)
        throw error("Личные файлы Codex доступны только их владельцу", 403);
      source = await db
        .collection("desktopFiles")
        .findOne({ _id: body.desktopFileId, ...desktopScope });
      if (
        !source ||
        !(await db
          .collection("desktopChats")
          .findOne({ _id: source.chatId, ...desktopScope, archived: false }))
      )
        throw error("Файл Codex недоступен", 404);
    } else {
      if (!team) throw error("Файл чата недоступен", 403);
      source = await accessibleChatFile(db, team, body.chatFileId);
    }
    data = fileBuffer(source);
    name ||= source.name;
    mimeType ||= source.mimeType;
  }
  const row = {
    _id: body.id,
    ...scope,
    ...(chatId ? { chatId } : {}),
    ...importedBytes(data, name || "Файл", mimeType),
    sourceKey,
    createdAt: new Date(),
    expiresAt: new Date(Date.now() + 86400000),
  };
  try {
    await collection.insertOne(row);
  } catch (cause) {
    if (cause.code !== 11000) throw cause;
    return replay(await collection.findOne({ _id: body.id }));
  }
  return { file: { ...fileMeta(row), sha256: row.sha256 }, replayed: false };
}
module.exports = {
  openaiFile,
  uploadFields,
  uploadSchema,
  importedBytes,
  importFile,
  accessibleChatFile,
};
