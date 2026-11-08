'use strict';
const { randomUUID, randomBytes, createHash } = require('node:crypto');
const { roles, text, fail, hashPassword, verifyPassword } = require('./domain.cjs');
const hash = value => createHash('sha256').update(value).digest('hex');
const loginKey = value => text(value, 100).normalize('NFKC').replace(/\s+/g, ' ').toLocaleLowerCase('ru');
const publicMember = ({ password, passwordBox, ...member }) => member;
function createAccess({ db, coll, invites, sessions, Workspace, frontendUrl, notify }) {
  const passwordContext = row => ({ _id: 'employee-password:' + row._id, workspaceId: row.workspaceId });
  async function ensureIndexes() {
    // Keep uniqueness for legacy phones while allowing new employees without a phone.
    await coll('Members').createIndex({ workspaceId: 1, phone: 1 }, { unique: true, name: 'workspace_phone_present', partialFilterExpression: { phone: { $type: 'string' } } });
    const old = (await coll('Members').listIndexes().toArray()).find(i => i.name === 'workspaceId_1_phone_1' && i.unique && !i.partialFilterExpression);
    if (old) await coll('Members').dropIndex(old.name).catch(error => { if (error.code !== 27) throw error; });
    await coll('Members').createIndex({ workspaceId: 1, loginKey: 1 }, { unique: true, partialFilterExpression: { loginKey: { $type: 'string' } } });
    await coll('EmployeeInvites').createIndex({ tokenHash: 1 }, { unique: true, partialFilterExpression: { tokenHash: { $type: 'string' } } });
    await coll('EmployeeInvites').createIndex({ workspaceId: 1, memberId: 1 });
  }
  async function transaction(work) {
    const session = db().client.startSession();
    try { let result; await session.withTransaction(async () => { result = await work({ session }); }); return result; }
    finally { await session.endSession(); }
  }
  async function throttle(req, workspaceId = '') {
    const id = hash('employee-auth:' + req.ip + ':' + workspaceId + ':' + Math.floor(Date.now() / 600000));
    const attempt = await coll('Attempts').findOneAndUpdate({ _id: id }, { $inc: { count: 1 }, $setOnInsert: { expiresAt: new Date(Date.now() + 1200000) } }, { upsert: true, returnDocument: 'after' });
    if (attempt.count > 20) fail('Слишком много попыток. Попробуйте через 10 минут', 429);
  }
  async function invitation(token, options = {}) {
    if (!/^[A-Za-z0-9_-]{43}$/.test(token || '')) fail('Ссылка недействительна или уже использована', 410);
    const row = await coll('EmployeeInvites').findOne({ tokenHash: hash(token), usedAt: { $exists: false } }, options);
    const member = row && await coll('Members').findOne({ _id: row.memberId, workspaceId: row.workspaceId, status: 'invited' }, options);
    if (!member) fail('Ссылка недействительна, закрыта или уже использована', 410);
    return { row, member };
  }
  function registerPublic(router, wrap) {
    router.get('/access/invitations/:token', wrap(async (req, res) => {
      const { row, member } = await invitation(req.params.token);
      const workspace = await Workspace.findById(row.workspaceId).select('name').lean();
      if (!workspace) fail('Чат недоступен', 404);
      res.json({ name: member.name, role: member.role, roleName: roles[member.role], workspaceId: row.workspaceId, workspaceName: workspace.name });
    }));
    router.post('/access/invitations/:token/claim', wrap(async (req, res) => {
      await throttle(req);
      const initial = await invitation(req.params.token);
      if (loginKey(req.body.name) !== initial.member.loginKey || !(await verifyPassword(req.body.password, initial.member.password))) fail('Проверьте имя и пароль', 401);
      const member = await transaction(async options => {
        const { row, member } = await invitation(req.params.token, options);
        if (member.version !== initial.member.version) fail('Доступ изменился. Повторите вход', 409);
        const used = await coll('EmployeeInvites').updateOne({ _id: row._id, usedAt: { $exists: false } }, { $set: { usedAt: new Date() }, $unset: { tokenHash: '' } }, options);
        if (!used.modifiedCount) fail('Ссылка уже использована', 410);
        const active = await coll('Members').findOneAndUpdate({ _id: member._id, workspaceId: row.workspaceId, status: 'invited', version: member.version }, { $set: { status: 'active', activatedAt: new Date() }, $inc: { version: 1 } }, { ...options, returnDocument: 'after' });
        if (!active) fail('Доступ изменился. Повторите вход', 409);
        return active;
      });
      await sessions.issue(req, res, member._id, member.authEpoch || 0);
      const workspace = await Workspace.findById(member.workspaceId).select('userId').lean();
      notify({ team: { workspaceId: member.workspaceId, actorId: member._id } }, { onlyActor: 'owner:' + workspace.userId, key: 'employee-joined:' + member._id, title: 'Сотрудник в системе', body: member.name + ' зарегистрирован' });
      res.json({ status: 'installation_required', workspaceId: member.workspaceId });
    }));
    router.post('/access/login', wrap(async (req, res) => {
      await throttle(req);
      const entry = await invites.lookup(text(req.body.token, 100), { allowClosed: true });
      const member = await coll('Members').findOne({ workspaceId: entry.workspaceId, loginKey: loginKey(req.body.name) });
      if (!member || !(await verifyPassword(req.body.password, member.password))) fail('Проверьте имя и пароль', 401);
      if (member.status !== 'active') fail(member.status === 'invited' ? 'Первый вход выполняется по одноразовой ссылке' : 'Доступ заблокирован владельцем', 403);
      await sessions.issue(req, res, member._id, member.authEpoch || 0);
      res.json({ status: member.requireInstalledApp ? 'installation_required' : 'active', workspaceId: member.workspaceId });
    }));
  }
  function registerOwner(router, { wrap, owner }) {
    router.post('/access/employees', wrap(async (req, res) => {
      owner(req);
      const name = text(req.body.name, 100), role = req.body.role || 'employee';
      if (!Object.hasOwn(roles, role)) fail('Выберите роль');
      const position = text(req.body.position || '', 160, false);
      const password = await hashPassword(req.body.password), id = randomUUID();
      const member = { _id: id, workspaceId: req.team.workspaceId, name, position, loginKey: loginKey(name), password, role, status: 'invited', requireInstalledApp: true, version: 0, authEpoch: 0, createdAt: new Date(), createdBy: req.team.actorId };
      member.passwordBox = invites.seal(req.body.password, passwordContext(member));
      const entry = await invites.create(req.team, role);
      await coll('Invites').updateOne({ _id: entry._id }, { $set: { namedLogin: true, entryForMemberId: id } });
      const token = randomBytes(32).toString('base64url');
      try {
        await transaction(async options => {
          await coll('Members').insertOne(member, options);
          await coll('EmployeeInvites').insertOne({ _id: randomUUID(), tokenHash: hash(token), memberId: id, workspaceId: req.team.workspaceId, createdAt: new Date() }, options);
        });
      } catch (error) {
        await coll('Invites').deleteOne({ _id: entry._id, entryForMemberId: id });
        if (error.code === 11000) fail('Сотрудник с таким именем уже существует', 409);
        throw error;
      }
      notify(req);
      res.status(201).json({ member: publicMember(member), url: entry.url + '?access=' + token });
    }));
    router.get('/access/employees/:id/password', wrap(async (req, res) => {
      owner(req);
      const member = await coll('Members').findOne({ _id: req.params.id, workspaceId: req.team.workspaceId });
      if (!member) fail('Сотрудник недоступен', 404);
      res.set('Cache-Control', 'no-store');
      let password;
      if (member.passwordBox) {
        try { password = invites.open(member.passwordBox, passwordContext(member)); }
        catch { return res.json({ available: false, version: member.version, message: 'Пароль недоступен для просмотра. Установите новый.' }); }
      }
      if (!password || !(await verifyPassword(password, member.password)))
        return res.json({ available: false, version: member.version, message: 'Ранее выданный пароль не сохранён для просмотра. Можно установить новый.' });
      res.json({ available: true, password, version: member.version });
    }));
    router.patch('/access/employees/:id/password', wrap(async (req, res) => {
      owner(req);
      if (!Number.isInteger(req.body.expectedVersion)) fail('Обновите данные сотрудника');
      const password = await hashPassword(req.body.password);
      const member = await transaction(async options => {
        const row = await coll('Members').findOne({ _id: req.params.id, workspaceId: req.team.workspaceId, version: req.body.expectedVersion }, options);
        if (!row) fail('Сотрудник изменился. Обновите данные', 409);
        const changed = await coll('Members').findOneAndUpdate({ _id: row._id, workspaceId: row.workspaceId, version: row.version }, {
          $set: { password, passwordBox: invites.seal(req.body.password, passwordContext(row)), passwordChangedAt: new Date(), passwordChangedBy: req.team.actorId },
          $inc: { version: 1, authEpoch: 1 },
        }, { ...options, returnDocument: 'after' });
        if (!changed) fail('Сотрудник изменился. Обновите данные', 409);
        await coll('Sessions').deleteMany({ memberId: row._id }, options);
        await coll('PushSubscriptions').deleteMany({ workspaceId: row.workspaceId, actorId: row._id }, options);
        await coll('Recoveries').updateMany({ workspaceId: row.workspaceId, memberId: row._id, status: { $in: ['pending', 'approved'] } }, { $set: { status: 'cancelled' }, $unset: { secretHash: '', entryBox: '' } }, options);
        return changed;
      });
      notify(req);
      res.json({ member: publicMember(member) });
    }));
    router.patch('/access/employees/:id', wrap(async (req, res) => {
      owner(req);
      if (!Number.isInteger(req.body.expectedVersion)) fail('Обновите список сотрудников');
      const role = req.body.role;
      if (!Object.hasOwn(roles, role)) fail('Выберите роль');
      if (Object.hasOwn(req.body, 'blocked') && typeof req.body.blocked !== 'boolean') fail('Проверьте доступ');
      const details = {};
      if (Object.hasOwn(req.body, 'name')) details.name = text(req.body.name, 100);
      if (Object.hasOwn(req.body, 'position')) details.position = text(req.body.position, 160, false);
      const member = await transaction(async options => {
        const row = await coll('Members').findOne({ _id: req.params.id, workspaceId: req.team.workspaceId, version: req.body.expectedVersion }, options);
        if (!row) fail('Сотрудник изменился. Обновите список', 409);
        let status = row.status;
        if (req.body.blocked === true) status = 'revoked';
        if (req.body.blocked === false && ['revoked', 'rejected'].includes(status)) status = row.activatedAt || row.phone ? 'active' : 'invited';
        const changed = await coll('Members').findOneAndUpdate({ _id: row._id, workspaceId: req.team.workspaceId, version: row.version }, { $set: { ...details, ...(details.name && row.loginKey ? { loginKey: loginKey(details.name) } : {}), role, status, reviewedAt: new Date(), reviewedBy: req.team.actorId }, $inc: { version: 1, ...(status !== row.status ? { authEpoch: 1 } : {}) } }, { ...options, returnDocument: 'after' });
        if (!changed) fail('Сотрудник изменился. Обновите список', 409);
        if (status !== row.status) {
          await coll('Sessions').deleteMany({ memberId: row._id }, options);
          await coll('PushSubscriptions').deleteMany({ workspaceId: row.workspaceId, actorId: row._id }, options);
        }
        return changed;
      }).catch(error => {
        if (error.code === 11000) fail('Сотрудник с таким именем уже существует', 409);
        throw error;
      });
      notify(req);
      res.json({ member: publicMember(member) });
    }));
  }
  return { ensureIndexes, registerPublic, registerOwner };
}
module.exports = { createAccess };
