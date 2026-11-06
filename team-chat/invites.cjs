"use strict";
const {
  randomUUID,
  randomBytes,
  createHash,
  createCipheriv,
  createDecipheriv,
} = require("node:crypto");
const { roles, fail } = require("./domain.cjs");
const hash = (s) => createHash("sha256").update(s).digest("hex");
function createInvites({ coll, Workspace, frontendUrl, env = process.env }) {
  function key() {
    const secret =
      env.SESSION_SECRET ||
      env.TEAM_CHAT_VAPID_PRIVATE_KEY ||
      env.CALENDAR_VAPID_PRIVATE_KEY;
    if (!secret) fail("Не настроено защищённое хранение приглашений", 503);
    return createHash("sha256")
      .update("INDEX12 chat invites v1\0" + secret)
      .digest();
  }
  const aad = (row) => Buffer.from(row.workspaceId + ":" + row._id);
  function seal(token, row) {
    const iv = randomBytes(12),
      cipher = createCipheriv("aes-256-gcm", key(), iv);
    cipher.setAAD(aad(row));
    return {
      iv: iv.toString("base64"),
      text: Buffer.concat([
        cipher.update(token, "utf8"),
        cipher.final(),
      ]).toString("base64"),
      tag: cipher.getAuthTag().toString("base64"),
    };
  }
  function open(box, row) {
    const cipher = createDecipheriv(
      "aes-256-gcm",
      key(),
      Buffer.from(box.iv, "base64"),
    );
    cipher.setAAD(aad(row));
    cipher.setAuthTag(Buffer.from(box.tag, "base64"));
    return Buffer.concat([
      cipher.update(Buffer.from(box.text, "base64")),
      cipher.final(),
    ]).toString("utf8");
  }
  function meta(row) {
    let url = null;
    if (row.tokenBox && !row.revokedAt && !row.deletedAt) {
      try {
        url = frontendUrl + "/team/" + open(row.tokenBox, row);
      } catch {
        /* Owner can rotate after a server key change. */
      }
    }
    return {
      _id: row._id,
      role: row.role || "employee",
      version: row.version || 0,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      revokedAt: row.revokedAt || null,
      url,
    };
  }
  async function lookup(token, { allowClosed = false } = {}) {
    if (typeof token !== "string" || !/^[\w-]{20,100}$/.test(token))
      fail("Приглашение недействительно", 404);
    const tokenHash = hash(token);
    const row = await coll("Invites").findOne({
      $or: [
        { _id: tokenHash },
        { tokenHash },
        { previousTokenHashes: tokenHash },
      ],
    });
    if (!row || !(await Workspace.exists({ _id: row.workspaceId })))
      fail("Приглашение недействительно", 404);
    const joinable =
      !row.revokedAt &&
      !row.deletedAt &&
      (row.tokenHash ? row.tokenHash === tokenHash : row._id === tokenHash);
    if (!joinable && !allowClosed)
      fail("Приглашение закрыто. Попросите новую ссылку", 410);
    return { ...row, joinable };
  }
  async function list(workspaceId) {
    return (
      await coll("Invites")
        .find({ workspaceId, deletedAt: { $exists: false } })
        .sort({ createdAt: -1 })
        .toArray()
    ).map(meta);
  }
  async function create(team, role) {
    if (!Object.hasOwn(roles, role)) fail("Выберите роль приглашения");
    const token = randomBytes(32).toString("base64url"),
      row = {
        _id: randomUUID(),
        workspaceId: team.workspaceId,
        tokenHash: hash(token),
        role,
        creatorName: team.name,
        creatorId: team.actorId,
        version: 0,
        revokedAt: null,
        createdAt: new Date(),
      };
    row.tokenBox = seal(token, row);
    await coll("Invites").insertOne(row);
    return meta(row);
  }
  async function change(team, id, body, remove = false) {
    if (
      typeof id !== "string" ||
      id.length > 100 ||
      !Number.isInteger(body.expectedVersion)
    )
      fail("Прочитайте актуальное приглашение");
    const row = await coll("Invites").findOne({
      _id: id,
      workspaceId: team.workspaceId,
      deletedAt: { $exists: false },
    });
    if (!row) fail("Приглашение не найдено", 404);
    if ((row.version || 0) !== body.expectedVersion)
      fail("Приглашение изменилось. Обновите список", 409);
    const changes = remove
      ? { revokedAt: new Date(), deletedAt: new Date(), updatedAt: new Date() }
      : {
          role: body.role || row.role || "employee",
          revokedAt: null,
          updatedAt: new Date(),
        };
    if (!Object.hasOwn(roles, changes.role || row.role || "employee"))
      fail("Выберите роль приглашения");
    if (!remove) {
      const token = randomBytes(32).toString("base64url");
      changes.tokenHash = hash(token);
      changes.tokenBox = seal(token, row);
    }
    const previous = row.tokenHash || row._id;
    const updated = await coll("Invites").findOneAndUpdate(
      {
        _id: id,
        workspaceId: team.workspaceId,
        deletedAt: { $exists: false },
        ...(body.expectedVersion === 0
          ? { $or: [{ version: 0 }, { version: { $exists: false } }] }
          : { version: body.expectedVersion }),
      },
      {
        $set: changes,
        $inc: { version: 1 },
        ...(!remove ? { $addToSet: { previousTokenHashes: previous } } : {}),
      },
      { returnDocument: "after" },
    );
    if (!updated) fail("Приглашение уже изменилось. Обновите список", 409);
    return meta(updated);
  }
  return { lookup, list, create, change, seal, open };
}
module.exports = { createInvites };
