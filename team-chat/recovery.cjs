"use strict";
const { createHash, randomBytes, randomUUID } = require("node:crypto");
const { fail, phone, hashPassword } = require("./domain.cjs");
const hash = (v) => createHash("sha256").update(v).digest("hex");
function createRecovery({ db, coll, invites, Workspace, frontendUrl, notify }) {
  const publicRow = (r) => ({
    _id: r._id,
    memberId: r.memberId,
    name: r.name,
    phone: r.phone,
    status: r.status,
    version: r.version,
    createdAt: r.createdAt,
    expiresAt: r.expiresAt,
  });
  async function transaction(work) {
    const session = db().client.startSession();
    try {
      let value;
      await session.withTransaction(async () => {
        value = await work({ session });
      });
      return value;
    } finally {
      await session.endSession();
    }
  }
  async function secretRow(token, options = {}) {
    if (!/^[\w-]{43}$/.test(token || ""))
      fail("Ссылка восстановления недействительна", 410);
    const row = await coll("Recoveries").findOne(
      {
        secretHash: hash(token),
        status: "approved",
        expiresAt: { $gt: new Date() },
      },
      options,
    );
    if (!row)
      fail(
        "Ссылка восстановления истекла или уже использована. Отправьте новый запрос",
        410,
      );
    return row;
  }
  function registerPublic(router, wrap) {
    router.post(
      "/recovery",
      wrap(async (req, res) => {
        const invite = await invites.lookup(req.body.token, {
            allowClosed: true,
          }),
          number = phone(req.body.phone);
        const attempt = await coll("Attempts").findOneAndUpdate(
          {
            _id: hash(
              "recovery:" + req.ip + ":" + Math.floor(Date.now() / 600000),
            ),
          },
          {
            $inc: { count: 1 },
            $setOnInsert: { expiresAt: new Date(Date.now() + 1200000) },
          },
          { upsert: true, returnDocument: "after" },
        );
        if (attempt.count > 10)
          fail("Слишком много запросов. Повторите через 10 минут", 429);
        const member = await coll("Members").findOne({
          workspaceId: invite.workspaceId,
          phone: number,
          status: { $in: ["active", "pending"] },
        });
        if (member) {
          const old = await coll("Recoveries").findOne({
            memberId: member._id,
            status: { $in: ["pending", "approved"] },
            expiresAt: { $gt: new Date() },
          });
          if (!old) {
            const row = {
              _id: randomUUID(),
              workspaceId: member.workspaceId,
              memberId: member._id,
              name: member.name,
              phone: member.phone,
              status: "pending",
              version: 0,
              createdAt: new Date(),
              expiresAt: new Date(Date.now() + 86400000),
            };
            row.entryBox = invites.seal(req.body.token, row);
            await coll("Recoveries").updateMany(
              {
                memberId: member._id,
                status: "pending",
                expiresAt: { $lte: new Date() },
              },
              { $set: { status: "cancelled" } },
            );
            try {
              await coll("Recoveries").insertOne(row);
            } catch (error) {
              if (error.code === 11000) return res.json({ accepted: true });
              throw error;
            }
            notify(
              {
                team: { workspaceId: member.workspaceId, actorId: member._id },
              },
              {
                key: "recovery:" + row._id,
                onlyActor:
                  "owner:" +
                  (
                    await Workspace.findById(member.workspaceId)
                      .select("userId")
                      .lean()
                  ).userId,
                title: "Chat · Восстановление доступа",
                body: member.name + " просит восстановить пароль",
              },
            );
          }
        }
        // Never disclose whether an anonymous caller guessed an employee's phone number.
        res.json({ accepted: true });
      }),
    );
    router.get(
      "/recovery/:token",
      wrap(async (req, res) => {
        const row = await secretRow(req.params.token);
        if (
          !(await coll("Members").findOne({
            _id: row.memberId,
            workspaceId: row.workspaceId,
            status: { $in: ["active", "pending"] },
          }))
        )
          fail("Доступ сотрудника закрыт", 403);
        res.json({ name: row.name });
      }),
    );
    router.post(
      "/recovery/:token",
      wrap(async (req, res) => {
        await secretRow(req.params.token);
        const attempt = await coll("Attempts").findOneAndUpdate(
          {
            _id: hash(
              "reset:" + req.ip + ":" + Math.floor(Date.now() / 600000),
            ),
          },
          {
            $inc: { count: 1 },
            $setOnInsert: { expiresAt: new Date(Date.now() + 1200000) },
          },
          { upsert: true, returnDocument: "after" },
        );
        if (attempt.count > 10)
          fail("Слишком много попыток. Повторите через 10 минут", 429);
        const password = await hashPassword(req.body.password);
        const changedIdentity = await transaction(async (options) => {
          const row = await secretRow(req.params.token, options);
          const changed = await coll("Members").updateOne(
            {
              _id: row.memberId,
              workspaceId: row.workspaceId,
              status: { $in: ["active", "pending"] },
            },
            {
              $set: { password, passwordChangedAt: new Date() },
              $unset: { passwordBox: "" },
              $inc: { version: 1, authEpoch: 1 },
            },
            options,
          );
          if (!changed.matchedCount) fail("Доступ сотрудника закрыт", 403);
          const consumed = await coll("Recoveries").updateOne(
            {
              _id: row._id,
              secretHash: hash(req.params.token),
              status: "approved",
              expiresAt: { $gt: new Date() },
            },
            {
              $set: { status: "used", usedAt: new Date() },
              $unset: { secretHash: "", entryBox: "" },
            },
            options,
          );
          if (!consumed.modifiedCount) fail("Ссылка уже использована", 410);
          await coll("Sessions").deleteMany(
            { memberId: row.memberId },
            options,
          );
          await coll("PushSubscriptions").deleteMany(
            { workspaceId: row.workspaceId, actorId: row.memberId },
            options,
          );
          await coll("Recoveries").updateMany(
            {
              memberId: row.memberId,
              _id: { $ne: row._id },
              status: { $in: ["pending", "approved"] },
            },
            {
              $set: { status: "cancelled" },
              $unset: { secretHash: "", entryBox: "" },
            },
            options,
          );
          return { workspaceId: row.workspaceId, actorId: row.memberId };
        });
        notify({ team: changedIdentity });
        res.json({ ok: true });
      }),
    );
  }
  function registerOwner(router, { wrap, owner }) {
    router.get(
      "/recoveries",
      wrap(async (req, res) => {
        owner(req);
        const rows = await coll("Recoveries")
          .find({
            workspaceId: req.team.workspaceId,
            status: { $in: ["pending", "approved"] },
            expiresAt: { $gt: new Date() },
          })
          .sort({ createdAt: -1 })
          .toArray();
        res.json({ recoveries: rows.map(publicRow) });
      }),
    );
    router.post(
      "/recoveries/:id/approve",
      wrap(async (req, res) => {
        owner(req);
        if (
          req.body.verified !== true ||
          !Number.isInteger(req.body.expectedVersion)
        )
          fail("Сначала подтвердите личность сотрудника");
        const row = await coll("Recoveries").findOne({
          _id: req.params.id,
          workspaceId: req.team.workspaceId,
          status: { $in: ["pending", "approved"] },
          expiresAt: { $gt: new Date() },
        });
        if (!row) fail("Запрос восстановления недоступен", 404);
        if (
          !(await coll("Members").findOne({
            _id: row.memberId,
            workspaceId: row.workspaceId,
            status: { $in: ["active", "pending"] },
          }))
        )
          fail("Сначала верните сотруднику доступ", 403);
        const token = randomBytes(32).toString("base64url"),
          entry = invites.open(row.entryBox, row);
        const updated = await coll("Recoveries").findOneAndUpdate(
          {
            _id: row._id,
            workspaceId: row.workspaceId,
            version: req.body.expectedVersion,
            status: { $in: ["pending", "approved"] },
          },
          {
            $set: {
              status: "approved",
              secretHash: hash(token),
              expiresAt: new Date(Date.now() + 900000),
              verifiedBy: req.team.actorId,
              verifiedAt: new Date(),
            },
            $inc: { version: 1 },
          },
          { returnDocument: "after" },
        );
        if (!updated) fail("Запрос уже изменился", 409);
        notify(req);
        res.json({
          recovery: publicRow(updated),
          url: frontendUrl + "/team/" + entry + "?reset=" + token,
        });
      }),
    );
    router.delete(
      "/recoveries/:id",
      wrap(async (req, res) => {
        owner(req);
        if (!Number.isInteger(req.body.expectedVersion))
          fail("Прочитайте актуальный запрос");
        const result = await coll("Recoveries").updateOne(
          {
            _id: req.params.id,
            workspaceId: req.team.workspaceId,
            version: req.body.expectedVersion,
          },
          {
            $set: { status: "rejected" },
            $unset: { secretHash: "", entryBox: "" },
            $inc: { version: 1 },
          },
        );
        if (!result.matchedCount) fail("Запрос уже изменился", 409);
        notify(req);
        res.json({ deletedRecoveryId: req.params.id });
      }),
    );
    router.delete(
      "/members/:id",
      wrap(async (req, res) => {
        owner(req);
        if (!Number.isInteger(req.body.expectedVersion))
          fail("Прочитайте актуального сотрудника");
        await transaction(async (options) => {
          const removed = await coll("Members").deleteOne(
            {
              _id: req.params.id,
              workspaceId: req.team.workspaceId,
              version: req.body.expectedVersion,
            },
            options,
          );
          if (!removed.deletedCount)
            fail("Сотрудник уже изменился. Обновите список", 409);
          await coll("Sessions").deleteMany(
            { memberId: req.params.id },
            options,
          );
          await coll("PushSubscriptions").deleteMany(
            { workspaceId: req.team.workspaceId, actorId: req.params.id },
            options,
          );
          await coll("PushJobs").deleteMany(
            { workspaceId: req.team.workspaceId, actorId: req.params.id },
            options,
          );
          await coll("Recoveries").deleteMany(
            { workspaceId: req.team.workspaceId, memberId: req.params.id },
            options,
          );
          await coll("EmployeeInvites").deleteMany(
            { workspaceId: req.team.workspaceId, memberId: req.params.id },
            options,
          );
        });
        notify(req);
        res.json({ deletedMemberId: req.params.id });
      }),
    );
  }
  return { registerPublic, registerOwner };
}
module.exports = { createRecovery };
