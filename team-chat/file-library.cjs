"use strict";
const { fail } = require("./domain.cjs");
const { taskAccess, financialRole } = require("./mcp-access.cjs");
const { validFileId } = require("../codex/files.cjs");
const tags = [
  { id: "important", name: "Важное" },
  { id: "work", name: "В работу" },
  { id: "review", name: "Проверить" },
  { id: "reference", name: "Справочное" },
  { id: "archive", name: "Архив" },
];
const types = {
  image: "Изображения",
  pdf: "PDF",
  spreadsheet: "Таблицы",
  document: "Документы",
  video: "Видео",
  audio: "Аудио",
  other: "Другие",
};
function fileType(file) {
  const mime = file.mimeType || "",
    extension = String(file.name || "")
      .split(".")
      .at(-1)
      .toLowerCase();
  if (
    /^image\//.test(mime) ||
    [
      "png",
      "jpg",
      "jpeg",
      "webp",
      "gif",
      "heic",
      "svg",
      "tif",
      "tiff",
    ].includes(extension)
  )
    return "image";
  if (mime === "application/pdf" || extension === "pdf") return "pdf";
  if (
    /spreadsheet|excel|csv/.test(mime) ||
    ["xls", "xlsx", "xlsm", "xlsb", "csv", "tsv", "ods", "fods", "numbers"].includes(extension)
  )
    return "spreadsheet";
  if (
    /^video\//.test(mime) ||
    ["mp4", "mov", "webm", "avi", "mkv", "m4v"].includes(extension)
  )
    return "video";
  if (
    /^audio\//.test(mime) ||
    ["mp3", "wav", "ogg", "m4a", "flac"].includes(extension)
  )
    return "audio";
  return /word|text|presentation/.test(mime) ||
    ["doc", "docx", "odt", "txt", "rtf", "ppt", "pptx", "md", "json", "xml", "log", "yaml", "yml", "html", "htm"].includes(extension)
    ? "document"
    : "other";
}
function libraryQuery(query) {
  const result = {};
  for (const key of [
    "q",
    "projectId",
    "uploader",
    "viewer",
    "type",
    "tag",
    "from",
    "to",
    "sort",
  ])
    if (query[key]) result[key] = String(query[key]);
  if ((result.q?.length || 0) > 300) fail("Поиск: до 300 символов");
  if (
    result.projectId &&
    result.projectId !== "__none__" &&
    !/^[a-f\d]{24}$/i.test(result.projectId)
  )
    fail("Выберите проект");
  if (result.type && !Object.hasOwn(types, result.type))
    fail("Выберите тип файла");
  if (result.tag && !tags.some((tag) => tag.id === result.tag))
    fail("Выберите метку");
  if (
    result.sort &&
    !["newest", "oldest", "name", "size"].includes(result.sort)
  )
    fail("Выберите сортировку");
  for (const key of ["from", "to"])
    if (result[key])
      require("../business/task-domain.cjs").dateOnly(result[key]);
  for (const key of ["uploader", "viewer"])
    if ((result[key]?.length || 0) > 100) fail("Выберите сотрудника");
  if (result.from && result.to && result.from > result.to)
    fail("Проверьте период");
  result.offset = Number(query.offset || 0);
  result.limit = Number(query.limit || 80);
  if (
    !Number.isSafeInteger(result.offset) ||
    result.offset < 0 ||
    result.offset > 10000000 ||
    !Number.isSafeInteger(result.limit) ||
    result.limit < 1 ||
    result.limit > 200
  )
    fail("Некорректная страница файлов");
  return result;
}
async function libraryFiles(db, team, onlyId) {
  const workspaceId = team.workspaceId,
    live = { workspaceId, deletedAt: { $exists: false } };
  const tasks = await db
    .collection("teamChatTasks")
    .find(
      { ...live, ...taskAccess(team) },
      {
        projection: {
          _id: 1,
          attachments: 1,
          projectId: 1,
          projectName: 1,
          description: 1,
          creatorId: 1,
          creatorName: 1,
          createdAt: 1,
        },
      },
    )
    .toArray();
  const taskMap = new Map(tasks.map((task) => [task._id, task]));
  const messageFilter = {
    ...live,
    "attachments.0": { $exists: true },
    $or: [{ taskId: null }, { taskId: { $in: [...taskMap.keys()] } }],
  };
  if (onlyId) messageFilter["attachments.fileId"] = onlyId;
  const messages = await db
    .collection("teamChatMessages")
    .find(messageFilter, {
      projection: {
        _id: 1,
        taskId: 1,
        attachments: 1,
        text: 1,
        authorId: 1,
        authorName: 1,
        createdAt: 1,
      },
    })
    .toArray();
  const refs = new Map();
  function link(attachments, source) {
    for (const file of attachments || []) {
      if (!file.fileId || (onlyId && onlyId !== file.fileId)) continue;
      if (!refs.has(file.fileId)) refs.set(file.fileId, []);
      const sources = refs.get(file.fileId);
      if (
        !sources.some(
          (item) =>
            item.kind === source.kind && item.messageId === source.messageId,
        )
      )
        sources.push(source);
    }
  }
  for (const task of tasks)
    link(task.attachments, {
      kind: "task",
      messageId: task._id,
      taskId: task._id,
      taskName: task.description,
      text: task.description,
      authorId: task.creatorId,
      authorName: task.creatorName,
      createdAt: task.createdAt,
      projectId: task.projectId || null,
      projectName: task.projectName || "",
    });
  for (const message of messages) {
    const task = taskMap.get(message.taskId);
    link(message.attachments, {
      kind: "message",
      messageId: message._id,
      taskId: task?._id || null,
      taskName: task?.description || "",
      text: message.text || "",
      authorId: message.authorId,
      authorName: message.authorName,
      createdAt: message.createdAt,
      projectId: task?.projectId || null,
      projectName: task?.projectName || "",
    });
  }
  if (!refs.size) return [];
  const ids = [...refs.keys()];
  const [files, metadata, views, members, settings] = await Promise.all([
    db
      .collection("teamChatFiles")
      .find(
        { workspaceId, _id: { $in: ids } },
        {
          projection: {
            _id: 1,
            actorId: 1,
            name: 1,
            mimeType: 1,
            size: 1,
            createdAt: 1,
          },
        },
      )
      .toArray(),
    db
      .collection("teamChatFileMetadata")
      .find({ workspaceId, fileId: { $in: ids } })
      .toArray(),
    db
      .collection("teamChatFileViews")
      .find(
        { workspaceId, fileId: { $in: ids } },
        { projection: { fileId: 1, actorId: 1, name: 1, lastViewedAt: 1 } },
      )
      .toArray(),
    db
      .collection("teamChatMembers")
      .find({ workspaceId }, { projection: { _id: 1, name: 1 } })
      .toArray(),
    db
      .collection("teamChatSettings")
      .findOne({ _id: workspaceId }, { projection: { ownerName: 1 } }),
  ]);
  const names = new Map(members.map((member) => [member._id, member.name]));
  const ownerId = "owner:" + team.workspace.userId;
  if (settings?.ownerName) names.set(ownerId, settings.ownerName);
  if (team.name) names.set(team.actorId, team.name);
  const metaMap = new Map(metadata.map((row) => [row.fileId, row]));
  const viewMap = new Map();
  for (const view of views) {
    if (!viewMap.has(view.fileId)) viewMap.set(view.fileId, []);
    viewMap.get(view.fileId).push(view);
  }
  return files.map((file) => {
    const sources = refs
        .get(file._id)
        .sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt)),
      meta = metaMap.get(file._id);
    const uploader =
      names.get(file.actorId) ||
      sources.find((source) => source.authorId === file.actorId)?.authorName ||
      "Участник";
    return {
      fileId: file._id,
      name: file.name,
      mimeType: file.mimeType,
      size: file.size,
      type: fileType(file),
      uploadedBy: { actorId: file.actorId, name: uploader },
      createdAt: file.createdAt || sources[0].createdAt,
      sources,
      projects: [
        ...new Map(
          sources
            .filter((source) => source.projectId)
            .map((source) => [
              source.projectId,
              { _id: source.projectId, name: source.projectName },
            ]),
        ).values(),
      ],
      tags: meta?.tags || [],
      description: meta?.description || "",
      version: meta?.version || 0,
      canEdit: !!(
        team.owner ||
        file.actorId === team.actorId ||
        financialRole(team.role)
      ),
      viewedBy: (viewMap.get(file._id) || [])
        .map((view) => ({
          actorId: view.actorId,
          name: names.get(view.actorId) || view.name || "Участник",
          lastViewedAt: view.lastViewedAt,
        })),
    };
  });
}
function filterFiles(files, query) {
  const q = query.q?.trim().toLocaleLowerCase("ru-RU");
  return files
    .filter(
      (file) =>
        (!q ||
          [
            file.name,
            file.description,
            ...file.tags.map(
              (id) => tags.find((tag) => tag.id === id)?.name || "",
            ),
            ...file.sources.map((source) => source.text),
          ]
            .join(" ")
            .toLocaleLowerCase("ru-RU")
            .includes(q)) &&
        (!query.projectId ||
          (query.projectId === "__none__"
            ? file.sources.some((source) => !source.projectId)
            : file.projects.some(
                (project) => project._id === query.projectId,
              ))) &&
        (!query.type || file.type === query.type) &&
        (!query.uploader || file.uploadedBy.actorId === query.uploader) &&
        (!query.viewer ||
          file.viewedBy.some((view) => view.actorId === query.viewer)) &&
        (!query.tag || file.tags.includes(query.tag)) &&
        (!query.from ||
          new Date(file.createdAt) >=
            new Date(query.from + "T00:00:00+05:00")) &&
        (!query.to ||
          new Date(file.createdAt) <=
            new Date(query.to + "T23:59:59.999+05:00")),
    )
    .sort(
      (a, b) =>
        (query.sort === "name"
          ? a.name.localeCompare(b.name, "ru")
          : query.sort === "size"
            ? b.size - a.size
            : (query.sort === "oldest" ? 1 : -1) *
              (new Date(a.createdAt) - new Date(b.createdAt))) ||
        a.fileId.localeCompare(b.fileId),
    );
}
function facets(files) {
  const projects = new Map(),
    people = new Map(),
    viewers = new Map();
  for (const file of files) {
    for (const project of file.projects) projects.set(project._id, project);
    people.set(file.uploadedBy.actorId, file.uploadedBy);
    for (const view of file.viewedBy)
      viewers.set(view.actorId, { actorId: view.actorId, name: view.name });
  }
  return {
    projects: [...projects.values()].sort((a, b) =>
      a.name.localeCompare(b.name, "ru"),
    ),
    people: [...people.values()].sort((a, b) =>
      a.name.localeCompare(b.name, "ru"),
    ),
    viewers: [...viewers.values()],
    types: Object.entries(types).map(([id, name]) => ({
      id,
      name,
      count: files.filter((file) => file.type === id).length,
    })),
    tags: tags.map((tag) => ({
      ...tag,
      count: files.filter((file) => file.tags.includes(tag.id)).length,
    })),
  };
}
function registerFileLibrary(router, { db, wrap, notify }) {
  async function fileFor(req) {
    if (!validFileId(req.params.id)) fail("Файл недоступен", 404);
    const file = (await libraryFiles(db(), req.team, req.params.id))[0];
    if (!file) fail("Файл недоступен", 404);
    return file;
  }
  router.get(
    "/files/library",
    wrap(async (req, res) => {
      const query = libraryQuery(req.query),
        files = await libraryFiles(db(), req.team),
        filtered = filterFiles(files, query);
      res.json({
        files: filtered.slice(query.offset, query.offset + query.limit),
        total: filtered.length,
        allTotal: files.length,
        nextOffset:
          query.offset + query.limit < filtered.length
            ? query.offset + query.limit
            : null,
        facets: facets(files),
      });
    }),
  );
  router.get(
    "/files/library/:id",
    wrap(async (req, res) => {
      res.json({ file: await fileFor(req) });
    }),
  );
  router.patch(
    "/files/library/:id",
    wrap(async (req, res) => {
      const file = await fileFor(req);
      if (!file.canEdit)
        fail(
          "Менять метки может загрузивший файл, руководитель или владелец",
          403,
        );
      const expected = req.body.expectedVersion;
      if (!Number.isSafeInteger(expected) || expected !== file.version)
        fail("Метки файла изменены. Обновите коллекцию", 409);
      const changes = {};
      if (Object.hasOwn(req.body, "tags")) {
        if (
          !Array.isArray(req.body.tags) ||
          req.body.tags.some((id) => !tags.some((tag) => tag.id === id))
        )
          fail("Выберите метки из списка");
        changes.tags = [...new Set(req.body.tags)];
      }
      if (Object.hasOwn(req.body, "description")) {
        if (
          typeof req.body.description !== "string" ||
          req.body.description.length > 2000
        )
          fail("Описание: до 2000 символов");
        changes.description = req.body.description.trim();
      }
      if (!Object.keys(changes).length) fail("Нет изменений");
      const collection = db().collection("teamChatFileMetadata"),
        key = req.team.workspaceId + ":" + file.fileId;
      const current = await collection.findOne({ _id: key });
      if ((current?.version || 0) !== expected)
        fail("Метки файла изменены. Обновите коллекцию", 409);
      let metadata;
      if (!current) {
        metadata = {
          _id: key,
          workspaceId: req.team.workspaceId,
          fileId: file.fileId,
          tags: [],
          description: "",
          ...changes,
          version: 1,
          updatedAt: new Date(),
          updatedBy: req.team.actorId,
        };
        try {
          await collection.insertOne(metadata);
        } catch (error) {
          if (error.code === 11000)
            fail("Метки файла изменены. Обновите коллекцию", 409);
          throw error;
        }
      } else {
        metadata = await collection.findOneAndUpdate(
          { _id: key, version: expected },
          {
            $set: {
              ...changes,
              updatedAt: new Date(),
              updatedBy: req.team.actorId,
            },
            $inc: { version: 1 },
          },
          { returnDocument: "after" },
        );
        if (!metadata) fail("Метки файла изменены. Обновите коллекцию", 409);
      }
      notify(req);
      res.json({
        fileId: file.fileId,
        tags: metadata.tags,
        description: metadata.description,
        version: metadata.version,
      });
    }),
  );
  router.post(
    "/files/library/:id/view",
    wrap(async (req, res) => {
      const file = await fileFor(req),
        key = req.team.workspaceId + ":" + file.fileId + ":" + req.team.actorId;
      await db()
        .collection("teamChatFileViews")
        .updateOne(
          { _id: key },
          {
            $set: { lastViewedAt: new Date(), name: req.team.name },
            $setOnInsert: {
              workspaceId: req.team.workspaceId,
              fileId: file.fileId,
              actorId: req.team.actorId,
              firstViewedAt: new Date(),
            },
          },
          { upsert: true },
        );
      res.json({ ok: true });
    }),
  );
}
module.exports = {
  registerFileLibrary,
  libraryFiles,
  libraryQuery,
  fileType,
  filterFiles,
  facets,
  tags,
};
