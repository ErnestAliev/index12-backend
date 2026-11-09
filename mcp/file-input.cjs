"use strict";
const { z } = require("zod");
const { uploadFields } = require("../utils/fileImport.cjs");
const { importedBytes, uploadSchema } = require("../utils/fileImport.cjs");
function createFileInput({
  allowLocalFiles = false,
  resolveContainerFile,
} = {}) {
  const fields = {
    ...uploadFields,
    ...(allowLocalFiles
      ? { localPath: z.string().min(1).max(4000).optional() }
      : {}),
    ...(resolveContainerFile
      ? {
          containerId: z.string().min(1).max(200).optional(),
          containerFileId: z.string().min(1).max(200).optional(),
        }
      : {}),
  };
  async function prepare(args) {
    const { localPath, containerId, containerFileId, ...body } = args;
    const count = [
      localPath,
      containerFileId,
      body.file,
      body.dataBase64,
      body.desktopFileId,
      body.chatFileId,
    ].filter((v) => v !== undefined).length;
    if (count !== 1 || !!containerId !== !!containerFileId)
      throw new Error("Укажите ровно один источник файла");
    if (localPath) {
      if (!allowLocalFiles)
        throw new Error("Локальные файлы недоступны этому подключению");
      const fs = require("node:fs/promises"),
        path = require("node:path");
      if (!path.isAbsolute(localPath))
        throw new Error("Укажите абсолютный путь файла");
      const handle = await fs.open(localPath, "r");
      try {
        const stat = await handle.stat();
        if (
          !stat.isFile() ||
          stat.size > require("../codex/files.cjs").FILE_LIMIT
        )
          throw new Error("Файл — до 10 МБ");
        const buffer = Buffer.allocUnsafe(stat.size + 1);
        let size = 0;
        while (size < buffer.length) {
          const { bytesRead } = await handle.read(
            buffer,
            size,
            buffer.length - size,
          );
          if (!bytesRead) break;
          size += bytesRead;
        }
        if (size !== stat.size)
          throw new Error("Файл изменился во время чтения. Повторите загрузку");
        const file = importedBytes(
          buffer.subarray(0, size),
          body.name || path.basename(localPath),
          body.mimeType,
        );
        Object.assign(body, {
          name: file.name,
          mimeType: file.mimeType,
          dataBase64: file.data.toString("base64"),
        });
      } finally {
        await handle.close();
      }
    } else if (containerFileId)
      Object.assign(
        body,
        await resolveContainerFile({
          containerId,
          containerFileId,
          name: body.name,
          mimeType: body.mimeType,
        }),
      );
    return uploadSchema.parse(body);
  }
  return {
    fields,
    prepare: async (args) => {
      try {
        return await prepare(args);
      } catch (cause) {
        if (cause instanceof z.ZodError) throw cause;
        throw Object.assign(new Error(), {
          detail: {
            code: "VALIDATION_ERROR",
            message: cause.code
              ? "Не удалось прочитать локальный файл"
              : cause.message || "Не удалось подготовить файл",
          },
        });
      }
    },
  };
}
module.exports = { createFileInput };
