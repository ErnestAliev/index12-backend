"use strict";
const { createHash } = require("node:crypto");
const { Types } = require("mongoose");
const { text, fail } = require("./domain.cjs");
const { fileBuffer } = require("../codex/files.cjs");
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
    notify(req);
    return updated;
  }
  router.patch(
    "/messages/:id",
    wrap(async (req, res) => {
      const row = await load(req);
      if (row.authorId !== req.team.actorId)
        fail("Можно изменить только своё сообщение", 403);
      expected(req, row);
      const content = text(req.body.text || "", 10000, false);
      if (!content && !row.attachments?.length) fail("Введите текст сообщения");
      if (content === row.text)
        return res.json({ messageId: row._id, replayed: true });
      const updated = await update(req, row, {
        $set: {
          text: content,
          contentVersion: Number(row.contentVersion || 0) + 1,
          editedAt: new Date(),
        },
        $unset: { approval: "" },
        $push: {
          editHistory: {
            actorId: req.team.actorId,
            text: row.text,
            contentVersion: Number(row.contentVersion || 0),
            approval: row.approval || null,
            at: new Date(),
          },
        },
      });
      res.json({ messageId: updated._id });
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
      const row = await load(req);
      expected(req, row);
      const state = row.approval?.state || "manager_review";
      if (!["manager_review", "director_review"].includes(state))
        fail("По сообщению уже принято решение", 409);
      const stage = state === "manager_review" ? "manager" : "director";
      if (!req.team.owner && req.team.role !== stage)
        fail("Согласование выполняет другой участник", 403);
      const action = req.body.action;
      if (!["approve", "reject"].includes(action)) fail("Выберите решение");
      const entry = {
        actorId: req.team.actorId,
        name: req.team.name,
        role: stage,
        action,
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
        (targetId !== req.team.workspaceId &&
          String(target.userId) !== String(req.user?.id || req.user?._id || ""))
      )
        fail("Чат недоступен", 403);
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
        const fileId =
          "forward:" +
          createHash("sha256")
            .update(messageId + ":" + targetId + ":" + actorId + ":" + file._id)
            .digest("hex");
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
      notify({ ...req, team: { ...req.team, workspaceId: targetId } });
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
                invoiceDismissed: 1,
                invoiceRequestId: 1,
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
          invoiceDismissed: !!file?.invoiceDismissed,
          invoiceRequestId: file?.invoiceRequestId || null,
        };
      });
    }
  }
  return { decorate };
}
module.exports = { registerMessageRoutes, emojis };
