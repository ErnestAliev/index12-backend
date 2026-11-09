"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID, createHash } = require("node:crypto");
const { mkdtemp, writeFile, rm } = require("node:fs/promises");
const { tmpdir } = require("node:os");
const path = require("node:path");
const { importFile, importedBytes } = require("../utils/fileImport.cjs");
const { createFileInput } = require("../mcp/file-input.cjs");
const { createMcpRuntime } = require("../mcp/runtime.cjs");
test("file import preserves exact bytes, deduplicates expired URLs, scopes retries and accepts arbitrary documents", async () => {
  const records = new Map(),
    collection = {
      findOne: async (q) => records.get(q._id),
      insertOne: async (row) => records.set(row._id, row),
    };
  const bytes = Buffer.from("%PDF-1.7\noriginal ESF bytes"),
    id = randomUUID(),
    scope = { actorId: "actor", workspaceId: "workspace" };
  const input = {
    id,
    file: {
      file_id: "file-original",
      download_url:
        "https://files.example.invalid/temporary?secret=do-not-store",
      file_name: "ЭСФ.pdf",
    },
  };
  let downloads = 0;
  const options = {
    collection,
    scope,
    download: async () => {
      downloads++;
      return bytes;
    },
  };
  const result = await importFile(input, options);
  assert.equal(result.file.mimeType, "application/pdf");
  assert.equal(
    result.file.sha256,
    createHash("sha256").update(bytes).digest("hex"),
  );
  assert.deepEqual(records.get(id).data, bytes);
  assert.ok(!JSON.stringify(records.get(id)).includes("do-not-store"));
  assert.equal(
    (
      await importFile(
        {
          ...input,
          file: {
            ...input.file,
            download_url: "https://files.example.invalid/expired",
          },
        },
        options,
      )
    ).replayed,
    true,
  );
  assert.equal(downloads, 1);
  await assert.rejects(
    importFile(input, { ...options, scope: { ...scope, actorId: "other" } }),
    { status: 409 },
  );
  await assert.rejects(
    importFile(
      { ...input, file: { ...input.file, file_id: "different" } },
      options,
    ),
    { status: 409 },
  );
  const zip = Buffer.from("PK\x03\x04archive bytes");
  const archived = await importFile(
    {
      id: randomUUID(),
      name: "../архив.zip",
      mimeType: "application/zip",
      dataBase64: zip.toString("base64"),
    },
    options,
  );
  assert.equal(archived.file.mimeType, "application/zip");
  assert.equal(archived.file.name, ".._архив.zip");
  assert.throws(() =>
    importedBytes(Buffer.alloc(10 * 1024 * 1024 + 1), "big.pdf"),
  );
  await assert.rejects(
    importFile(
      { id: randomUUID(), name: "bad", dataBase64: "not base64" },
      options,
    ),
  );
  await assert.rejects(importFile({ ...input, dataBase64: "YQ==" }, options));
});
test("local file transport is only declared for the local runtime and never forwards a server path", async () => {
  assert.equal(createFileInput().fields.localPath, undefined);
  const folder = await mkdtemp(path.join(tmpdir(), "index12-file-input-"));
  try {
    const file = path.join(folder, "original.txt"),
      bytes = Buffer.from("original bytes");
    await writeFile(file, bytes);
    const result = await createFileInput({ allowLocalFiles: true }).prepare({
      id: randomUUID(),
      localPath: file,
    });
    assert.deepEqual(Buffer.from(result.dataBase64, "base64"), bytes);
    assert.equal(result.name, "original.txt");
    assert.equal(result.localPath, undefined);
    await assert.rejects(
      createFileInput().prepare({ id: randomUUID(), localPath: file }),
    );
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("file descriptors declare all OpenAI file fields; staff catalogue excludes personal Codex", async () => {
  const runtime = createMcpRuntime({ config: {}, database: async () => ({}) });
  try {
    for (const name of [
      "index12_chat_upload_file",
      "index12_codex_upload_file",
    ]) {
      const shape = runtime.definitions
        .get(name)
        .schema.shape.file.unwrap().shape;
      assert.deepEqual(Object.keys(shape).sort(), [
        "download_url",
        "file_id",
        "file_name",
        "mime_type",
      ]);
      assert.equal(shape.file_name.isOptional(), true);
      assert.equal(shape.mime_type.isOptional(), true);
      assert.equal(shape.download_url.isOptional(), false);
      assert.equal(shape.file_id.isOptional(), false);
      assert.equal(
        runtime.definitions.get(name).schema.shape.localPath,
        undefined,
      );
    }
    assert.ok(
      !require("../team-chat/mcp-tools.cjs").CHAT_TOOLS.some((name) =>
        name.startsWith("index12_codex_"),
      ),
    );
  } finally {
    await runtime.close();
  }
});
test("partial file workflows report upload separately from confirmed task attachment", () => {
  const { receipt, executionReport } = require("../codex/toolResults.cjs");
  const uploaded = receipt({
    tool: "index12_chat_upload_file",
    result: { structuredContent: { file: { fileId: "file-id" } } },
  });
  assert.match(uploaded.label, /ещё не отправлен/);
  const report = executionReport(
    { code: "VERSION_CONFLICT", message: "Задача изменилась" },
    [uploaded],
  );
  assert.match(report.text, /ещё не отправлен/);
  assert.doesNotMatch(report.text, /прикреплён/);
  const attached = receipt({
    tool: "index12_chat_update_task",
    result: {
      structuredContent: {
        task: {
          _id: "task-id",
          version: 2,
          attachments: [{ fileId: "file-id" }],
        },
      },
    },
  });
  assert.equal(attached.id, "task-id");
  assert.equal(attached.version, 2);
});
