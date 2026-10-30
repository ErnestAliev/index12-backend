// Production HTTP handlers + isolated MongoDB. No production credentials or data.
const assert = require('node:assert/strict');
const { startFixture } = require('./subcategories.cjs');
(async () => {
  const fixture = await startFixture();
  try {
    const { Account, Company, Individual, Event, Category } = fixture.models;
    const userId = fixture.userId;
    const company = await Company.create({ userId, name: 'Компания' });
    const person = await Individual.create({ userId, name: 'Собственник' });
    const source = await Account.create({ userId, name: 'Рабочий счёт', companyId: company._id, initialBalance: 10000 });
    const destination = await Account.create({ userId, name: 'Личная карта в учёте', individualId: person._id, initialBalance: 0 });
    await Category.create({ userId, name: 'Перевод' });
    const date = new Date(); date.setHours(12, 0, 0, 0);
    const input = { amount: 500.25, date: date.toISOString(), fromAccountId: String(source._id), transferPurpose: 'withdrawal', withdrawalRecipient: 'Собственник' };
    const snapshot = async () => (await fixture.request('/snapshot')).data;
    const draw = await fixture.request('/transfers', 'POST', input);
    assert.equal(draw.status, 201, JSON.stringify(draw));
    assert.equal(draw.data.toAccountId, null); assert.equal(draw.data.transferReason, null);
    assert.equal(draw.data.description, 'Вывод средств'); assert.equal(draw.data.fromCompanyId.name, company.name);
    assert.equal((await snapshot()).accountBalances[source._id], -500.25);
    assert.equal((await snapshot()).accountBalances[destination._id], undefined);
    assert.equal((await snapshot()).companyBalances[company._id], -500.25);
    const edited = await fixture.request(`/events/${draw.data._id}`, 'PUT', { amount: 600.50, description: 'Комментарий собственника' });
    assert.equal(edited.status, 200, JSON.stringify(edited));
    assert.equal(edited.data.description, 'Комментарий собственника');
    assert.equal((await snapshot()).accountBalances[source._id], -600.50);
    const converted = await fixture.request(`/events/${draw.data._id}`, 'PUT', { transferPurpose: 'internal', transferReason: null, toAccountId: String(destination._id) });
    assert.equal(converted.status, 200); assert.equal(converted.data.isWithdrawal, false);
    assert.equal((await snapshot()).accountBalances[destination._id], 600.50);
    const back = await fixture.request(`/events/${draw.data._id}`, 'PUT', { transferPurpose: 'withdrawal', withdrawalRecipient: 'Собственник' });
    assert.equal(back.status, 200, JSON.stringify(back));
    assert.equal(back.data.toAccountId, null); assert.equal(back.data.toIndividualId, null);
    assert.equal((await snapshot()).accountBalances[destination._id], undefined);
    for (const patch of [{ withdrawalRecipient: '' }, { fromAccountId: null }, { toAccountId: String(destination._id) }, { amount: 0 }, { transferReason: 'business_growth' }, { transferPurpose: 'internal' }]) {
      const invalid = await fixture.request('/transfers', 'POST', { ...input, ...patch });
      assert.equal(invalid.status, 400, JSON.stringify(invalid));
    }
    assert.equal(await Event.countDocuments({ userId }), 1);
    // Existing business-growth transfers keep their two legs and descriptions.
    const legacy = await Event.create({ userId, date, dateKey:draw.data.dateKey, amount:100, type:'transfer', isTransfer:true, transferPurpose:'personal', transferReason:'business_growth', fromAccountId:source._id, toAccountId:destination._id, description:'Исторический перевод' });
    await fixture.request(`/events/${legacy._id}`, 'PUT', { description: 'Исторический комментарий' });
    const old = await Event.findById(legacy._id);
    assert.equal(old.transferReason, 'business_growth'); assert.equal(old.isWithdrawal, false);
    assert.equal((await snapshot()).accountBalances[destination._id], 100);
    await Event.deleteMany({ userId });
    if (process.argv.includes('--ui')) {
      const { verifyWithdrawalUi } = await import('../../../index12-frontend/tests/browser/withdrawals.mjs');
      await verifyWithdrawalUi({ apiUrl: fixture.apiUrl, sourceId: String(source._id), destinationId: String(destination._id) });
    }
    console.log('PASS withdrawal HTTP: create/edit/conversions, no destination/reason, inferred ownership, snapshots and legacy compatibility');
  } finally { await fixture.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
