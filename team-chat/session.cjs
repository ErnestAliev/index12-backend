"use strict";
const { randomBytes, createHash } = require("node:crypto");
const COOKIE = "index12_team";
const LIFETIME = 365 * 86400000;
const deviceKind = (req) =>
  /Android|iPhone|iPad|iPod|Mobile/i.test(req.headers["user-agent"] || "") ||
  req.headers["sec-ch-ua-mobile"] === "?1"
    ? "mobile"
    : "desktop";
function createChatSessions({
  coll,
  production = process.env.NODE_ENV === "production",
}) {
  const cookieOptions = {
    httpOnly: true,
    secure: production,
    sameSite: "lax",
    path: "/api/team-chat",
  };
  const tokenFrom = (req) => {
    const value = (req.headers.cookie || "")
      .split(";")
      .map((s) => s.trim())
      .find((s) => s.startsWith(COOKIE + "="))
      ?.slice(COOKIE.length + 1);
    return /^[A-Za-z0-9_-]{43}$/.test(value || "") ? value : null;
  };
  const key = (token) => createHash("sha256").update(token).digest("hex");
  const setCookie = (res, token) =>
    res.cookie(COOKIE, token, { ...cookieOptions, maxAge: LIFETIME });
  async function issue(req, res, memberId, authEpoch = 0) {
    const previous = tokenFrom(req);
    if (previous) await coll("Sessions").deleteOne({ _id: key(previous) });
    const token = randomBytes(32).toString("base64url");
    await coll("Sessions").insertOne({
      _id: key(token),
      memberId,
      authEpoch,
      expiresAt: new Date(Date.now() + LIFETIME),
      renewedAt: new Date(),
      createdAt: new Date(),
      lastSeenAt: new Date(),
      deviceKind: deviceKind(req),
    });
    setCookie(res, token);
  }
  async function memberId(req, res) {
    const token = tokenFrom(req);
    if (!token) return null;
    const session = await coll("Sessions").findOne({
      _id: key(token),
      expiresAt: { $gt: new Date() },
    });
    if (!session) return null;
    const updates = {};
    if (
      !session.deviceKind ||
      !session.lastSeenAt ||
      Date.now() - new Date(session.lastSeenAt).getTime() > 300000
    ) {
      updates.lastSeenAt = new Date();
      updates.deviceKind = session.deviceKind || deviceKind(req);
    }
    if (
      !session.renewedAt ||
      Date.now() - new Date(session.renewedAt).getTime() > 3600000
    ) {
      updates.expiresAt = new Date(Date.now() + LIFETIME);
      updates.renewedAt = new Date();
    }
    if (Object.keys(updates).length) {
      await coll("Sessions").updateOne({ _id: session._id }, { $set: updates });
    }
    req.teamSessionDeviceKind = updates.deviceKind || session.deviceKind;
    req.teamSessionEpoch = session.authEpoch || 0;
    req.teamSessionInstalled = session.installed === true;
    setCookie(res, token);
    return session.memberId;
  }
  async function clear(req, res) {
    const token = tokenFrom(req);
    if (token) await coll("Sessions").deleteOne({ _id: key(token) });
    res.clearCookie(COOKIE, cookieOptions);
  }
  async function confirmInstalled(req) {
    const token = tokenFrom(req);
    if (!token) return false;
    const result = await coll("Sessions").updateOne(
      {
        _id: key(token),
        memberId: req.team.actorId,
        authEpoch: req.team.authEpoch || 0,
        expiresAt: { $gt: new Date() },
      },
      {
        $set: {
          installed: true,
          installedAt: new Date(),
          lastSeenAt: new Date(),
        },
      },
    );
    return result.matchedCount === 1;
  }
  async function observeDevice(req, kind) {
    const token = tokenFrom(req);
    if (
      !token ||
      req.team.owner ||
      !["mobile", "desktop"].includes(kind) ||
      req.teamSessionDeviceKind === kind
    )
      return;
    await coll("Sessions").updateOne(
      {
        _id: key(token),
        memberId: req.team.actorId,
        authEpoch: req.team.authEpoch || 0,
        expiresAt: { $gt: new Date() },
      },
      { $set: { deviceKind: kind, lastSeenAt: new Date() } },
    );
  }
  async function devices(team) {
    if (team.owner) return [];
    const rows = await coll("Sessions")
      .find(
        {
          memberId: team.actorId,
          authEpoch: team.authEpoch || 0,
          expiresAt: { $gt: new Date() },
        },
        { projection: { deviceKind: 1, lastSeenAt: 1, renewedAt: 1 } },
      )
      .toArray();
    const groups = new Map();
    for (const row of rows) {
      if (!["mobile", "desktop"].includes(row.deviceKind)) continue;
      const lastSeenAt = row.lastSeenAt || row.renewedAt;
      const previous = groups.get(row.deviceKind);
      if (!previous || new Date(lastSeenAt) > new Date(previous.lastSeenAt))
        groups.set(row.deviceKind, { kind: row.deviceKind, lastSeenAt });
    }
    return [...groups.values()];
  }
  return { issue, memberId, clear, confirmInstalled, observeDevice, devices };
}
module.exports = { createChatSessions };
