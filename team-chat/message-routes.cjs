"use strict";
const { createHash } = require("node:crypto");
const { Types } = require("mongoose");
const { text, fail } = require("./domain.cjs");
const { taskAccess } = require('./mcp-access.cjs');
const { fileBuffer, fileMeta, TOTAL_LIMIT } = require("../codex/files.cjs");
const emojis = ["👍", "❤️", "😂", "😮", "😢", "🙏"];
const version = (row) => Number(row.version || 0);
const revisionFilter = (v) =>
  v === 0
    ? { $or: [{ version: 0 }, { version: { $exists: false } }] }
    : { version: v };
function registerMessageRoutes(router, { coll, wrap, notify, Workspace }) {
  async function load(req) {
    const row = await coll("Messages").findOne({
      _id: req.params.id,
      workspaceId: req.team.workspaceId,
      deletedAt: { $exists: false },
    });
    if (!row) fail("Сообщение недоступно", 404);
    if (
      row.taskId &&
      !(await coll("Tasks").findOne(
        {
          _id: row.taskId,
          workspaceId: req.team.workspaceId,
          deletedAt: { $exists: false },
          ...taskAccess(req.team),
        },
        { projection: { _id: 1 } },
      ))
    )
      fail("Задача недоступна", 404);
    return row;
  }
  function expected(req, row) {
    if (
      !Number.isInteger(req.body.expectedVersion) ||
      req.body.expectedVersion !== version(row)
    )
      fail("Сообщение изменилось. Откройте его заново", 409);
  }
  async function update(req, row, changes) {
    const updated = await coll("Messages").findOneAndUpdate(
      {
        _id: row._id,
        workspaceId: row.workspaceId,
        deletedAt: { $exists: false },
        ...revisionFilter(version(row)),
      },
      {
        ...changes,
        $set: { ...changes.$set, updatedAt: new Date() },
        $inc: { version: 1 },
      },
      {
        returnDocument: "after",
        projection: { editHistory: 0, approvalArchive: 0 },
      },
    );
    if (!updated) fail("Сообщение изменилось. Откройте его заново", 409);
    if (changes.$set?.contentVersion !== undefined)
      await coll("Messages").updateMany(
        { workspaceId: row.workspaceId, "replyTo.messageId": row._id },
        { $set: { updatedAt: new Date() } },
      );
    notify(req);
    return updated;
  }
  router.patch(
    "/messages/:id",
    wrap(async (req, res) => {
      const row = await load(req);
      if (["request", "task",'approval_request'].includes(row.kind))
        fail("Откройте карточку для изменения", 409);
      if (!req.team.owner && row.authorId !== req.team.actorId)
        fail("Можно изменить только своё сообщение", 403);
      expected(req, row);
      const content = Object.hasOwn(req.body, 'text') ? text(req.body.text || '', 10000, false) : row.text || '';
      let attachments = row.attachments || [];
      if (Object.hasOwn(req.body, 'fileIds')) {
        const ids = req.body.fileIds;
        if (!Array.isArray(ids) || ids.length > 5 || new Set(ids).size !== ids.length || ids.some(id => typeof id !== 'string' || !/^(?:[a-f\d-]{36}|forward:[a-f\d]{64})$/i.test(id))) fail('Проверьте файлы');
        attachments = [];
        for (const id of ids) {
          const file = await coll('Files').findOne({ _id: id, workspaceId: row.workspaceId });
          if (!file || (file.actorId !== req.team.actorId && !row.attachments?.some(f => f.fileId === id))) fail('Файл недоступен', 404);
          attachments.push(fileMeta(file));
        }
        if (attachments.reduce((size, file) => size + file.size, 0) > TOTAL_LIMIT) fail('Общий размер файлов — до 25 МБ');
      }
      if (!content && !attachments.length) fail("Введите текст сообщения или прикрепите файл");
      if (content === row.text && JSON.stringify(attachments) === JSON.stringify(row.attachments || []))
        return res.json({ messageId: row._id, replayed: true });
      const updated = await update(req, row, {
        $set: {
          text: content,
          attachments,
          contentVersion: Number(row.contentVersion || 0) + 1,
          editedAt: new Date(),
        },
        $unset: { approval: "" },
        $push: {
          editHistory: {
            actorId: req.team.actorId,
            text: row.text,
            attachments: row.attachments || [],
            contentVersion: Number(row.contentVersion || 0),
            approval: row.approval || null,
            at: new Date(),
          },
        },
      });
      if (attachments.length) await coll('Files').updateMany({ workspaceId: row.workspaceId, _id: { $in: attachments.map(f => f.fileId) } }, { $set: { linked: true }, $unset: { expiresAt: '' } });
      res.json({ messageId: updated._id });
    }),
  );
  router.get(
    "/messages/:id",
    wrap(async (req, res) => {
      const row = await load(req);
      await decorate(req, [row]);
      res.json({ message: row });
    }),
  );
  router.post(
    "/messages/read",
    wrap(async (req, res) => {
      const ids = req.body.ids;
      if (
        !Array.isArray(ids) ||
        ids.length > 100 ||
        ids.some((id) => typeof id !== "string" || !/^[a-f\d-]{36}$/i.test(id))
      )
        fail("Проверьте список сообщений");
      // Legacy clients acknowledged background data loads. Only visible-view acknowledgements count.
      const explicitMcp = (req.team.mcp || req.teamMcpOwner) && req.body.explicitMcpRead === true;
      if (req.body.visibilityConfirmed !== true && !explicitMcp) return res.json({ read: 0 });
      const accessible = await coll('Tasks').find({ workspaceId: req.team.workspaceId, deletedAt: { $exists: false }, ...taskAccess(req.team) }, { projection: { _id: 1 } }).toArray();
      const rows = await coll("Messages")
        .find({
          _id: { $in: ids },
          workspaceId: req.team.workspaceId,
          authorId: { $ne: req.team.actorId },
          deletedAt: { $exists: false },
          $or: [{ taskId: null }, { taskId: { $in: accessible.map(t => t._id) } }],
        })
        .toArray();
      if (!rows.length) return res.json({ read: 0 });
      const at = new Date();
      const result = await coll("MessageReads").bulkWrite(
        rows.map((row) => ({
          updateOne: {
            filter: {
              _id: createHash("sha256")
                .update(
                  row.workspaceId + ":" + row._id + ":" + req.team.actorId,
                )
                .digest("hex"),
            },
            update: {
              $setOnInsert: {
                workspaceId: row.workspaceId,
                messageId: row._id,
                actorId: req.team.actorId,
                name: req.team.name,
                readAt: at,
              },
              $set: { visibilityConfirmed: true },
              $min: { visibleReadAt: at },
              $max: { contentVersion: Number(row.contentVersion || 0) },
            },
            upsert: true,
          },
        })),
      );
      // A repeated acknowledgement must not emit another event to every client.
      const firstReads = result.upsertedCount + result.modifiedCount;
      if (firstReads) {
        await coll("Messages").updateMany(
          {
            _id: { $in: rows.map((r) => r._id) },
            workspaceId: req.team.workspaceId,
          },
          { $set: { updatedAt: at } },
        );
        notify(req);
      }
      res.json({ read: rows.length });
    }),
  );
  router.post(
    "/messages/:id/reaction",
    wrap(async (req, res) => {
      const row = await load(req),
        emoji = req.body.emoji;
      if (emoji !== null && !emojis.includes(emoji)) fail("Выберите реакцию");
      const key = createHash("sha256")
        .update(row.workspaceId + ":" + row._id + ":" + req.team.actorId)
        .digest("hex");
      if (emoji === null)
        await coll("MessageReactions").deleteOne({ _id: key });
      else
        await coll("MessageReactions").updateOne(
          { _id: key },
          {
            $set: {
              workspaceId: row.workspaceId,
              messageId: row._id,
              actorId: req.team.actorId,
              emoji,
              updatedAt: new Date(),
            },
          },
          { upsert: true },
        );
      // Reactions are independent of the text/approval revision, so they do not invalidate an open editor.
      await coll("Messages").updateOne(
        { _id: row._id, workspaceId: row.workspaceId },
        { $set: { updatedAt: new Date() } },
      );
      notify(req);
      res.json({ messageId: row._id });
    }),
  );
  router.post(
    "/messages/:id/decision",
    wrap(async (req, res) => {
      let row = await load(req);
      if(row.kind==='approval_request'){
        if(!Number.isInteger(req.body.expectedVersion)||req.body.expectedVersion<0||req.body.expectedVersion>version(row))fail('Сообщение изменилось. Откройте его заново',409);
        const note=text(req.body.note||'',1000,false);
        for(let attempt=0;attempt<4;attempt++){
          const decision=require('./approvals.cjs').decideApproval(row.approvalRequest,req.team.actorId,req.body.action,note);
          if(decision.replayed){await decorate(req,[row]);return res.json({message:row,messageId:row._id,replayed:true});}
          let updated;
          try{updated=await update(req,row,{$set:{approvalRequest:decision.request}});}catch(error){if(error.status===409&&attempt<3){row=await load(req);continue;}throw error;}
          notify(req,{key:`approval-decision:${row._id}:${req.team.actorId}`,title:req.team.name+' · '+(req.body.action==='approve'?'Согласовано':'Отклонено'),body:row.text,taskId:row.taskId,messageId:row._id,onlyActor:row.authorId});
          await decorate(req,[updated]);return res.json({message:updated,messageId:updated._id});
        }
      }
      expected(req, row);
      if (["request", "task"].includes(row.kind))
        fail("Используйте согласование в карточке заявки", 409);
      const state = row.approval?.state || "manager_review";
      if (!["manager_review", "director_review"].includes(state))
        fail("По сообщению уже принято решение", 409);
      const stage = state === "manager_review" ? "manager" : "director";
      if (!req.team.owner && req.team.role !== stage)
        fail("Согласование выполняет другой участник", 403);
      const action = req.body.action;
      if (!["approve", "reject"].includes(action)) fail("Выберите решение");
      const note = text(req.body.note || "", 1000, false);
      if (action === "reject" && !note) fail("Укажите причину отклонения");
      const entry = {
        actorId: req.team.actorId,
        name: req.team.name,
        role: stage,
        action,
        note,
        at: new Date(),
        contentVersion: Number(row.contentVersion || 0),
      };
      const approval = {
        state:
          action === "reject"
            ? "rejected"
            : stage === "manager"
              ? "director_review"
              : "approved",
        contentVersion: Number(row.contentVersion || 0),
        history: [...(row.approval?.history || []), entry],
      };
      const updated = await update(req, row, { $set: { approval } });
      notify(req, {
        key: `approval:${updated._id}:${updated.version}`,
        title: "Chat · Согласование",
        body: row.text || "Решение по сообщению",
      });
      res.json({ messageId: updated._id });
    }),
  );
  router.get(
    "/message-targets",
    wrap(async (req, res) => {
      const targets = [
        { _id: req.team.workspaceId, name: req.team.workspace.name },
      ];
      const actorId = String(req.user?.id || req.user?._id || "");
      if (Types.ObjectId.isValid(actorId)) {
        const rows = await Workspace.find({
          userId: { $in: [actorId, new Types.ObjectId(actorId)] },
        })
          .select("_id name")
          .lean();
        for (const row of rows)
          if (!targets.some((t) => t._id === String(row._id)))
            targets.push({ _id: String(row._id), name: row.name });
      }
      res.json({ targets });
    }),
  );
  router.post(
    "/messages/:id/forward",
    wrap(async (req, res) => {
      const row = await load(req);
      expected(req, row);
      const targetId = text(req.body.targetWorkspaceId, 80),
        messageId = text(req.body.id, 80);
      if (
        !/^[a-f\d-]{36}$/i.test(messageId) ||
        !Types.ObjectId.isValid(targetId)
      )
        fail("Выберите чат");
      const target =
        targetId === req.team.workspaceId
          ? req.team.workspace
          : await Workspace.findById(targetId).lean();
      if (
        !target ||
        ((req.team.mcp || req.teamMcpOwner) && targetId !== req.team.workspaceId) ||
        (targetId !== req.team.workspaceId &&
          String(target.userId) !== String(req.user?.id || req.user?._id || ""))
      )
        fail("Чат недоступен", 403);
      const targetTaskId = req.body.targetTaskId || null;
      if (targetTaskId && (typeof targetTaskId !== 'string' || !/^[a-f\d-]{36}$/i.test(targetTaskId) || !await coll('Tasks').findOne({ _id: targetTaskId, workspaceId: targetId, deletedAt: { $exists: false }, ...taskAccess(req.team) }))) fail('Задача недоступна', 404);
      const actorId =
        targetId === req.team.workspaceId
          ? req.team.actorId
          : "owner:" + target.userId;
      const fingerprint = createHash("sha256")
        .update(
          JSON.stringify([
            row.workspaceId,
            row._id,
            version(row),
            targetId,
            actorId,
            ...(targetTaskId ? [targetTaskId] : []),
          ]),
        )
        .digest("hex");
      const old = await coll("Messages").findOne({ _id: messageId });
      if (old) {
        if (old.forwardKey !== fingerprint || old.deletedAt)
          fail("Ключ пересылки уже использован", 409);
        return res.json({
          forwardedMessageId: old._id,
          targetWorkspaceId: targetId,
          replayed: true,
        });
      }
      const attachments = [];
      for (const source of row.attachments || []) {
        const file = await coll("Files").findOne({
          _id: source.fileId,
          workspaceId: row.workspaceId,
          linked: true,
        });
        if (!file) fail("Вложение больше недоступно", 404);
        const hex = createHash("sha256")
            .update(messageId + ":" + targetId + ":" + actorId + ":" + file._id)
            .digest("hex");
        const fileId = `${hex.slice(0,8)}-${hex.slice(8,12)}-5${hex.slice(13,16)}-8${hex.slice(17,20)}-${hex.slice(20,32)}`;
        await coll("Files").updateOne(
          { _id: fileId },
          {
            $setOnInsert: {
              _id: fileId,
              workspaceId: targetId,
              actorId,
              name: file.name,
              mimeType: file.mimeType,
              size: file.size,
              data: fileBuffer(file),
              linked: true,
            },
          },
          { upsert: true },
        );
        attachments.push({ ...source, fileId });
      }
      const forwarded = {
        _id: messageId,
        workspaceId: targetId,
        ...(targetTaskId ? { taskId: targetTaskId } : {}),
        authorId: actorId,
        authorName: req.team.name,
        text: row.text,
        attachments,
        createdAt: new Date(),
        updatedAt: new Date(),
        version: 0,
        contentVersion: 0,
        forwardedFrom: { authorName: row.authorName, messageId: row._id },
        forwardKey: fingerprint,
      };
      try {
        await coll("Messages").insertOne(forwarded);
      } catch (error) {
        if (error.code !== 11000) throw error;
        const saved = await coll("Messages").findOne({ _id: messageId });
        if (saved?.forwardKey !== fingerprint)
          fail("Ключ пересылки уже использован", 409);
      }
      notify(
        { ...req, team: { ...req.team, workspaceId: targetId } },
        {
          key: "message:" + messageId,
          ...(targetTaskId ? { taskId: targetTaskId } : {}),
          title: "Chat · " + req.team.name,
          body: row.text || "Файл в чате",
        },
      );
      res
        .status(201)
        .json({ forwardedMessageId: messageId, targetWorkspaceId: targetId });
    }),
  );
  async function decorate(req, messages) {
    const ids = [
      ...new Set(messages.filter((m) => !m.deletedAt).map((m) => m._id)),
    ];
    const reactions = ids.length
      ? await coll("MessageReactions")
          .find(
            { workspaceId: req.team.workspaceId, messageId: { $in: ids } },
            { projection: { messageId: 1, actorId: 1, emoji: 1 } },
          )
          .toArray()
      : [];
    const reads = ids.length
      ? await coll("MessageReads")
          .find(
            { workspaceId: req.team.workspaceId, messageId: { $in: ids } },
            { projection: { messageId: 1, actorId: 1, contentVersion: 1, visibilityConfirmed: 1 } },
          )
          .toArray()
      : [];
    const replyIds = [
      ...new Set(messages.map((m) => m.replyTo?.messageId).filter(Boolean)),
    ];
    const sources = replyIds.length
      ? await coll("Messages")
          .find(
            { workspaceId: req.team.workspaceId, _id: { $in: replyIds } },
            { projection: { _id: 1, deletedAt: 1, contentVersion: 1 } },
          )
          .toArray()
      : [];
    const fileIds = messages
      .filter((m) => !m.deletedAt)
      .flatMap((m) => (m.attachments || []).map((f) => f.fileId));
    const files = fileIds.length
      ? await coll("Files")
          .find(
            { workspaceId: req.team.workspaceId, _id: { $in: fileIds } },
            {
              projection: {
                mimeType: 1,
              },
            },
          )
          .toArray()
      : [];
    for (const message of messages) {
      delete message.editHistory;
      delete message.approvalArchive;
      delete message.forwardKey;
      message.version = version(message);
      if (message.replyTo) {
        const source = sources.find((s) => s._id === message.replyTo.messageId);
        message.replyTo =
          !source || source.deletedAt
            ? {
                messageId: message.replyTo.messageId,
                authorName: message.replyTo.authorName,
                text: "",
                fileName: "",
                deleted: true,
              }
            : {
                ...message.replyTo,
                edited:
                  Number(source.contentVersion || 0) !==
                  Number(message.replyTo.contentVersion || 0),
              };
      }
      message.readCount = reads.filter(
        (r) =>
          r.visibilityConfirmed === true &&
          r.messageId === message._id &&
          r.actorId !== message.authorId &&
          r.contentVersion === Number(message.contentVersion || 0),
      ).length;
      message.reactions = emojis
        .map((emoji) => ({
          emoji,
          count: reactions.filter(
            (r) => r.messageId === message._id && r.emoji === emoji,
          ).length,
          mine: reactions.some(
            (r) =>
              r.messageId === message._id &&
              r.emoji === emoji &&
              r.actorId === req.team.actorId,
          ),
        }))
        .filter((r) => r.count);
      message.attachments = (message.attachments || []).map((attachment) => {
        const file = files.find((f) => f._id === attachment.fileId);
        return {
          ...attachment,
          isPdf:
            file?.mimeType === "application/pdf" ||
            /\.pdf$/i.test(attachment.name),
        };
      });
    }
  }
  return { decorate };
}
module.exports = { registerMessageRoutes, emojis };
