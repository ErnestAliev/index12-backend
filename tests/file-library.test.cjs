"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  fileType,
  libraryQuery,
  filterFiles,
} = require("../team-chat/file-library.cjs");
test("file classification covers Office, PDF, image and media uploads with octet-stream MIME", () => {
  for (const [name, type] of Object.entries({
    "report.xlsx": "spreadsheet",
    "table.MXL": "spreadsheet",
    "page.html": "document",
    "page.htm": "document",
    "page.xhtml": "document",
    "sheet.csv": "spreadsheet",
    "export.tsv": "spreadsheet",
    "legacy.xlsb": "spreadsheet",
    "open.fods": "spreadsheet",
    "letter.docx": "document",
    "notes.md": "document",
    "data.json": "document",
    "scan.PDF": "pdf",
    "photo.heic": "image",
    "movie.mov": "video",
    "note.mp3": "audio",
    "archive.zip": "other",
  }))
    assert.equal(
      fileType({ name, mimeType: "application/octet-stream" }),
      type,
    );
});
test("library rejects malformed filters, periods and unbounded pages", () => {
  for (const query of [
    { limit: 201 },
    { offset: -1 },
    { limit: 1.5 },
    { projectId: "foreign" },
    { type: "unknown" },
    { tag: "custom" },
    { from: "2026-02-30" },
    { from: "2026-11-10", to: "2026-11-09" },
  ])
    assert.throws(() => libraryQuery(query));
  assert.equal(
    libraryQuery({ projectId: "__none__", limit: "20", offset: "80" }).offset,
    80,
  );
});
test("combined filters and date boundaries use Almaty time, with stable pagination order", () => {
  const base = {
    name: "Документ.pdf",
    description: "Счёт",
    type: "pdf",
    size: 30,
    tags: ["review"],
    projects: [{ _id: "a".repeat(24) }],
    sources: [{ projectId: "a".repeat(24), text: "SF" }],
    uploadedBy: { actorId: "employee" },
    viewedBy: [{ actorId: "owner" }],
  };
  const files = [
    { ...base, fileId: "b", createdAt: "2026-11-09T19:00:00Z" },
    { ...base, fileId: "a", createdAt: "2026-11-09T19:00:00Z" },
    { ...base, fileId: "c", createdAt: "2026-11-09T18:59:59Z" },
  ];
  assert.deepEqual(
    filterFiles(
      files,
      libraryQuery({
        from: "2026-11-10",
        to: "2026-11-10",
        q: "счёт",
        projectId: "a".repeat(24),
        uploader: "employee",
        viewer: "owner",
        type: "pdf",
        tag: "review",
      }),
    ).map((f) => f.fileId),
    ["a", "b"],
  );
  assert.equal(
    filterFiles(files, libraryQuery({ projectId: "__none__" })).length,
    0,
  );
});
