"use strict";
const { createHash, randomUUID } = require("node:crypto");
const webpush = require("web-push");
const { subscriptionInput } = require("../business/reminders.cjs");
const { fail } = require("./domain.cjs");
const hash = (s) => createHash("sha256").update(s).digest("hex");
function pushKeys(env) {
  const team =
    env.TEAM_CHAT_VAPID_PUBLIC_KEY || env.TEAM_CHAT_VAPID_PRIVATE_KEY;
  return {
    publicKey: team
      ? env.TEAM_CHAT_VAPID_PUBLIC_KEY
      : env.CALENDAR_VAPID_PUBLIC_KEY,
    privateKey: team
      ? env.TEAM_CHAT_VAPID_PRIVATE_KEY
      : env.CALENDAR_VAPID_PRIVATE_KEY,
  };
}
function createChatPush({
  coll,
  Workspace,
  env = process.env,
  send = webpush.sendNotification,
}) {
  const keys = pushKeys(env),
    configured = !!(keys.publicKey && keys.privateKey);
  let indexes,
    running = false,
    timer;
  const ready = () =>
    (indexes ||= Promise.all([
      coll("PushJobs").createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }),
      coll("PushJobs").createIndex({ done: 1, nextAt: 1 }),
      coll("PushSubscriptions").createIndex({ workspaceId: 1, actorId: 1 }),
    ]).catch((e) => {
      indexes = null;
      throw e;
    }));
  async function permitted(target, pending = false) {
    const workspace = await Workspace.findById(target.workspaceId).lean();
    if (!workspace) return false;
    if (target.actorId === "owner:" + workspace.userId) return true;
    return !!(await coll("Members").findOne({
      _id: target.actorId,
      workspaceId: target.workspaceId,
      status: { $in: pending ? ["pending", "active"] : ["active"] },
    }));
  }
  async function drain() {
    if (!configured || running) return;
    running = true;
    try {
      await ready();
      // Leases allow another process to retry after a restart without a parallel send.
      for (let i = 0; i < 100; i++) {
        const now = new Date(),
          lease = randomUUID();
        const job = await coll("PushJobs").findOneAndUpdate(
          {
            done: false,
            nextAt: { $lte: now },
            expiresAt: { $gt: now },
            $or: [
              { leaseUntil: { $exists: false } },
              { leaseUntil: { $lte: now } },
            ],
          },
          { $set: { lease, leaseUntil: new Date(Date.now() + 60000) } },
          { returnDocument: "after", sort: { nextAt: 1 } },
        );
        if (!job) break;
        const target = await coll("PushSubscriptions").findOne({
          _id: job.subscriptionId,
        });
        try {
          if (
            target &&
            target.actorId === job.actorId &&
            target.workspaceId === job.workspaceId
          ) {
            if (await permitted(target, job.allowPending)) {
              await send(
                target.subscription,
                JSON.stringify({
                  ...job.payload,
                  url: target.url,
                  workspaceId: target.workspaceId,
                }),
                {
                  vapidDetails: { subject: "https://index12.com", ...keys },
                  TTL: 3600,
                  timeout: 10000,
                },
              );
            } else if (!(await permitted(target, true)))
              await coll("PushSubscriptions").deleteOne({ _id: target._id });
          }
          await coll("PushJobs").updateOne(
            { _id: job._id, lease },
            {
              $set: { done: true, sentAt: new Date() },
              $unset: { leaseUntil: "" },
            },
          );
        } catch (e) {
          if ([404, 410].includes(e.statusCode)) {
            await coll("PushSubscriptions").deleteOne({
              _id: job.subscriptionId,
            });
            await coll("PushJobs").updateOne(
              { _id: job._id, lease },
              { $set: { done: true }, $unset: { leaseUntil: "" } },
            );
          } else
            await coll("PushJobs").updateOne(
              { _id: job._id, lease },
              {
                $inc: { attempts: 1 },
                $set: {
                  nextAt: new Date(
                    Date.now() +
                      Math.min(
                        300000,
                        5000 * 2 ** Math.min(job.attempts || 0, 6),
                      ),
                  ),
                },
                $unset: { leaseUntil: "" },
              },
            );
        }
      }
    } finally {
      running = false;
    }
  }
  const kick = () => {
    void drain().catch(() =>
      console.error("[Team chat] Push queue unavailable"),
    );
  };
  function start() {
    if (!configured || timer) return;
    timer = setInterval(kick, 15000);
    timer.unref();
    kick();
  }
  async function enqueue({
    workspaceId,
    actorId,
    key,
    title,
    body,
    onlyActor,
    allowPending = false,
    testId,
    taskId,
    messageId,
  }) {
    if (!configured) return;
    await ready();
    const targets = await coll("PushSubscriptions")
      .find({ workspaceId, actorId: onlyActor || { $ne: actorId } })
      .toArray();
    for (const target of targets) {
      const id = hash(target._id + ":" + key);
      await coll("PushJobs").updateOne(
        { _id: id },
        {
          $setOnInsert: {
            subscriptionId: target._id,
            workspaceId,
            actorId: target.actorId,
            payload: {
              title: String(title || "Chat").slice(0, 100),
              body: String(body || "Новое событие в чате").slice(0, 180),
              tag: "team-" + hash(key).slice(0, 24),
              testId,
              taskId,
              messageId,
            },
            allowPending,
            done: false,
            attempts: 0,
            nextAt: new Date(),
            expiresAt: new Date(Date.now() + 86400000),
          },
        },
        { upsert: true },
      );
    }
    kick();
  }
  function register(router, { wrap, invitation }) {
    router.get(
      "/push/subscriptions/status",
      wrap(async (req, res) => {
        const endpoint = req.query.endpoint;
        if (typeof endpoint !== "string" || endpoint.length > 4096)
          fail("Укажите подписку устройства");
        const row = await coll("PushSubscriptions").findOne({
          _id: hash(endpoint),
          actorId: req.team.actorId,
          workspaceId: req.team.workspaceId,
        });
        res.json({ enabled: configured && !!row });
      }),
    );
    router.get("/push/key", (_req, res) =>
      res.json({ configured, publicKey: configured ? keys.publicKey : null }),
    );
    router.post(
      "/push/subscriptions",
      wrap(async (req, res) => {
        if (!configured)
          fail("Уведомления чата пока не настроены на сервере", 503);
        const token = req.body.token;
        let url;
        if (token === undefined && req.team.owner) {
          url = "/?teamChat=" + req.team.workspaceId;
        } else {
          if (
            typeof token !== "string" ||
            !/^[A-Za-z0-9_-]{20,100}$/.test(token)
          )
            fail("Некорректное приглашение");
          const invite = await invitation(token);
          if (invite.workspaceId !== req.team.workspaceId)
            fail("Приглашение другого чата", 403);
          url = "/team/" + token;
        }
        const subscription = subscriptionInput(req.body.subscription || {});
        // One endpoint represents one device. Rebinding after login prevents another account's notifications.
        const _id = hash(subscription.endpoint);
        await coll("PushSubscriptions").updateOne(
          { _id },
          {
            $set: {
              workspaceId: req.team.workspaceId,
              actorId: req.team.actorId,
              subscription,
              url,
              updatedAt: new Date(),
            },
          },
          { upsert: true },
        );
        start();
        res.json({ enabled: true });
      }),
    );
    router.delete(
      "/push/subscriptions",
      wrap(async (req, res) => {
        const endpoint = req.body.endpoint;
        if (typeof endpoint !== "string" || endpoint.length > 4096)
          fail("Укажите подписку устройства");
        await coll("PushSubscriptions").deleteOne({
          _id: hash(endpoint),
          actorId: req.team.actorId,
          workspaceId: req.team.workspaceId,
        });
        res.json({ enabled: false });
      }),
    );
    router.post(
      "/push/test",
      wrap(async (req, res) => {
        if (!configured)
          fail("Уведомления чата пока не настроены на сервере", 503);
        const target = await coll("PushSubscriptions").findOne({
          _id: hash(String(req.body.endpoint)),
          actorId: req.team.actorId,
          workspaceId: req.team.workspaceId,
        });
        if (!target)
          fail("Сначала включите уведомления на этом устройстве", 404);
        // Rate limit per device. Testing never sends to the other staff devices.
        const claim = await coll("PushSubscriptions").updateOne(
          {
            _id: target._id,
            $or: [
              { testedAt: { $exists: false } },
              { testedAt: { $lt: new Date(Date.now() - 10000) } },
            ],
          },
          { $set: { testedAt: new Date() } },
        );
        if (!claim.modifiedCount)
          fail("Повторите проверку через 10 секунд", 429);
        const testId = randomUUID();
        try {
          await send(
            target.subscription,
            JSON.stringify({
              title: "Chat",
              body: "Уведомления чата работают",
              url: target.url,
              workspaceId: target.workspaceId,
              testId,
              tag: "team-push-test",
            }),
            {
              vapidDetails: { subject: "https://index12.com", ...keys },
              TTL: 60,
              timeout: 10000,
            },
          );
        } catch (error) {
          if ([404, 410].includes(error.statusCode)) {
            await coll("PushSubscriptions").deleteOne({ _id: target._id });
            fail(
              "Подписка устройства устарела. Включите уведомления повторно",
              410,
            );
          }
          fail("Не удалось отправить проверку. Повторите позже", 503);
        }
        res.json({ accepted: true, testId });
      }),
    );
  }
  return { start, enqueue, register, drain, stop: () => clearInterval(timer) };
}
module.exports = { createChatPush, pushKeys };
