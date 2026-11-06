"use strict";
const { randomBytes, createHash } = require("node:crypto");
const COOKIE = "index12_team";
const LIFETIME = 365 * 86400000;
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
    if (Date.now() - new Date(session.renewedAt).getTime() > 3600000) {
      await coll("Sessions").updateOne(
        { _id: session._id },
        {
          $set: {
            expiresAt: new Date(Date.now() + LIFETIME),
            renewedAt: new Date(),
          },
        },
      );
    }
    req.teamSessionEpoch = session.authEpoch || 0;
    setCookie(res, token);
    return session.memberId;
  }
  async function clear(req, res) {
    const token = tokenFrom(req);
    if (token) await coll("Sessions").deleteOne({ _id: key(token) });
    res.clearCookie(COOKIE, cookieOptions);
  }
  return { issue, memberId, clear };
}
module.exports = { createChatSessions };
