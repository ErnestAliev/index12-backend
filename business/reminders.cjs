'use strict';
const webpush = require('web-push');
const { createHash, randomUUID } = require('node:crypto');
const { fail } = require('./providers.cjs');
const { reminderEvent } = require('./task-domain.cjs');
const { workspaceAccess } = require('../utils/operationAccess');
const hash = value => createHash('sha256').update(value).digest('hex');
const configured = env => !!env.CALENDAR_VAPID_PUBLIC_KEY && !!env.CALENDAR_VAPID_PRIVATE_KEY;
function subscriptionInput(input) {
  let url; try { url = new URL(input.endpoint); } catch { fail('Некорректная подписка уведомлений'); }
  const host = url.hostname;
  if (url.protocol !== 'https:' || url.port || url.username || url.password || !(host === 'fcm.googleapis.com' || host === 'updates.push.services.mozilla.com' || host.endsWith('.push.services.mozilla.com') || host === 'web.push.apple.com' || host.endsWith('.notify.windows.com'))) fail('Недопустимый сервер уведомлений');
  if (url.href.length > 4096 || !/^[\w-]{40,200}$/.test(input.keys?.p256dh || '') || !/^[\w-]{16,100}$/.test(input.keys?.auth || '')) fail('Некорректные ключи уведомлений');
  return { endpoint: url.href, keys: { p256dh: input.keys.p256dh, auth: input.keys.auth } };
}
function registerReminderRoutes(router, { coll, wrap, env }) {
  router.get('/reminders/key', (_req, res) => res.json({ configured: configured(env), publicKey: env.CALENDAR_VAPID_PUBLIC_KEY || null }));
  router.post('/reminders/subscriptions', wrap(async (req, res) => {
    if (!configured(env)) fail('На сервере не настроены уведомления календаря', 503);
    const subscription = subscriptionInput(req.body), actorId = String(req.user.id || req.user._id);
    const _id = hash(`${req.businessScope}:${actorId}:${subscription.endpoint}`);
    await coll('PushSubscriptions').updateOne({ _id }, { $set: { scope: req.businessScope, actorId, workspaceId: String(req.cachedWorkspace._id), subscription, updatedAt: new Date() } }, { upsert: true });
    res.json({ enabled: true });
  }));
  router.delete('/reminders/subscriptions', wrap(async (req, res) => {
    if (typeof req.body.endpoint !== 'string') fail('Укажите подписку уведомлений');
    await coll('PushSubscriptions').deleteOne({ _id: hash(`${req.businessScope}:${String(req.user.id || req.user._id)}:${req.body.endpoint}`) });
    res.json({ enabled: false });
  }));
}
async function deliverReminders({ db, Workspace, env = process.env, now = Date.now(), send = webpush.sendNotification }) {
  if (!configured(env)) return;
  const tasks = db.collection('businessTasks'), subscriptions = db.collection('businessPushSubscriptions'), deliveries = db.collection('businessReminderDeliveries');
  for await (const target of subscriptions.find({})) {
    const workspace = await Workspace.findById(target.workspaceId).lean();
    if (!workspace || workspaceAccess(workspace, target.actorId)?.role !== 'admin') { await subscriptions.deleteOne({ _id: target._id }); continue; }
    for await (const task of tasks.find({ scope: target.scope, status: 'open', deletedAt: { $exists: false }, $or: [{ startsAt: { $type: 'string' } }, { dueDate: { $type: 'string' } }] })) {
      const event = reminderEvent(task, now); if (!event) continue;
      const _id = hash(`${target._id}:${task._id}:${task.reminderRevision || 0}:${event.key}`), lease = randomUUID();
      try { await deliveries.updateOne({ _id, sentAt: { $exists: false }, $or: [{ leaseUntil: { $exists: false } }, { leaseUntil: { $lte: new Date(now) } }] }, { $set: { lease, leaseUntil: new Date(now + 300000), expiresAt: new Date(now + 90 * 86400000) } }, { upsert: true }); } catch (error) { if (error.code === 11000) continue; throw error; }
      const latest = await tasks.findOne({ _id: task._id, status: 'open', deletedAt: { $exists: false } });
      if (!latest || (latest.reminderRevision || 0) !== (task.reminderRevision || 0)) continue;
      try {
        await send(target.subscription, JSON.stringify({ title: event.kind === 'overdue' ? 'INDEX12 · Просрочено' : 'INDEX12 · Напоминание', body: task.title, taskId: task._id, workspaceId: target.workspaceId, tag: `index12-task-${task._id}` }), { vapidDetails: { subject: 'https://index12.com', publicKey: env.CALENDAR_VAPID_PUBLIC_KEY, privateKey: env.CALENDAR_VAPID_PRIVATE_KEY }, TTL: 3600, timeout: 10000 });
        await deliveries.updateOne({ _id, lease }, { $set: { sentAt: new Date(now) }, $unset: { leaseUntil: '' } });
      } catch (error) { if ([404, 410].includes(error.statusCode)) { await subscriptions.deleteOne({ _id: target._id }); break; } }
    }
  }
}
function startReminders(options) {
  if (!configured(options.env || process.env)) return () => {};
  let running = false;
  const tick = async () => { if (running) return; running = true; try { await deliverReminders(options); } catch { console.error('[Calendar] Не удалось проверить напоминания'); } finally { running = false; } };
  void options.db.collection('businessReminderDeliveries').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }).catch(() => {});
  const timer = setInterval(tick, 30000); timer.unref(); void tick(); return () => clearInterval(timer);
}
module.exports = { registerReminderRoutes, deliverReminders, startReminders, subscriptionInput };
