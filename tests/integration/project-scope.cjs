// Isolated MongoDB + actual production HTTP handlers; no production credentials.
const assert = require('node:assert/strict');
const { startFixture, verifyDatabase } = require('./subcategories.cjs');

async function verifyProjectScope(fixture, seed) {
  const { models: { Event, Project }, request, userId } = fixture;
  const projects = await Project.create(['Север', 'Юг', 'Запад'].map(name => ({ name, userId })));
  const common = { date: new Date(), accountId: seed.account._id, companyId: seed.company._id, contractorId: seed.contractor._id, categoryId: seed.categories.utilities._id };
  const baseline = (await request('/snapshot')).data.projectBalances.__all_projects__ || 0;
  const shared = await request('/events', 'POST', { ...common, type: 'expense', amount: -1000000, projectScope: 'all', projectId: projects[0]._id, projectIds: projects.map(p => p._id) });
  assert.equal(shared.status, 201, JSON.stringify(shared));
  assert.equal(shared.data.projectId, null);
  assert.equal(shared.data.projectScope, 'all');
  assert.equal(await Event.countDocuments({ parentOpId: shared.data._id }), 0);
  assert.equal((await request('/snapshot')).data.projectBalances.__all_projects__, baseline - 1000000);
  // Legacy editing and unrelated edits preserve the new scope and original amount.
  const edited = await request(`/events/${seed.legacyId}`, 'PUT', { projectScope: 'all' });
  assert.equal(edited.status, 200, JSON.stringify(edited));
  await request(`/events/${seed.legacyId}`, 'PUT', { description: 'Все проекты, без деления' });
  assert.equal((await Event.findById(seed.legacyId)).projectScope, 'all');
  assert.equal((await Event.findById(seed.legacyId)).amount, -120);
  // Reallocate a shared payment: one timeline parent, precise cents, no duplicate total.
  const split = await request(`/events/${shared.data._id}`, 'PUT', { projectScope: 'selected', projectId: projects[0]._id, projectIds: projects.map(p => p._id), amount: -1000000.01 });
  assert.equal(split.status, 200, JSON.stringify(split));
  assert.equal(split.data.isSplitParent, true);
  const children = await Event.find({ parentOpId: shared.data._id });
  assert.equal(children.length, 3);
  assert.equal(Math.round(children.reduce((sum, child) => sum + child.amount, 0) * 100), -100000001);
  assert.ok(children.every(child => child.projectScope === 'selected' && child.cellIndex === split.data.cellIndex));
  const protectedChild = await request(`/events/${children[0]._id}`, 'PUT', { projectScope: 'all' });
  assert.equal(protectedChild.status, 400);
  const childCount = await Event.countDocuments({ parentOpId: shared.data._id });
  assert.equal(childCount, 3);
  const unsplit = await request(`/events/${shared.data._id}`, 'PUT', { projectScope: 'all' });
  assert.equal(unsplit.status, 200, JSON.stringify(unsplit));
  assert.equal(unsplit.data.excludeFromTotals, false);
  assert.equal(unsplit.data.isSplitParent, false);
  assert.equal(unsplit.data.allocationChanges.deletedIds.length, 3);
  assert.equal(await Event.countDocuments({ parentOpId: shared.data._id }), 0);
  const balances = (await request('/snapshot')).data.projectBalances;
  assert.equal(balances.__all_projects__, baseline - 1000000.01 - 120);
  assert.ok(projects.every(project => !balances[String(project._id)]));
  const transfer = await request('/transfers', 'POST', { date: common.date, amount: 800, projectScope: 'all', fromAccountId: seed.account._id, toAccountId: seed.toAccount._id, categoryId: seed.categories.transferCategory._id });
  assert.equal(transfer.status, 201, JSON.stringify(transfer));
  assert.equal(transfer.data.projectScope, 'all');
  assert.equal((await request('/snapshot')).data.projectBalances.__all_projects__, balances.__all_projects__);
  const exported = (await request('/events/all-for-export')).data.find(op => op._id === shared.data._id);
  assert.equal(exported.projectScope, 'all');
  const imported = await request('/import/operations', 'POST', { operations: [{ date: common.date, type: 'expense', amount: -30, project: 'Все проекты', category: 'Маркетинг' }] });
  assert.equal(imported.status, 201, JSON.stringify(imported));
  assert.ok(await Event.findOne({ userId, projectScope: 'all', amount: -30 }));
  assert.equal(await Project.countDocuments({ userId, name: 'Все проекты' }), 0);
  const explicit = await request(`/events/${shared.data._id}`, 'PUT', { projectScope: 'selected', projectId: projects[0]._id, projectIds: [] });
  assert.equal(explicit.data.projectScope, 'selected');
  assert.equal(String(explicit.data.projectId._id), String(projects[0]._id));
  console.log('PASS projects MongoDB/API: one shared payment, old edits, selected ↔ all ↔ exact splits, child protection, no duplicate totals, transfers, export/import');
  return { ...seed, projects: projects.map(p => ({ _id: String(p._id), name: p.name })) };
}

(async () => {
  const fixture = await startFixture();
  try {
    const seed = await verifyProjectScope(fixture, await verifyDatabase(fixture));
    if (process.argv.includes('--browser')) {
      const { verifyProjectScopeUi } = await import('../../../index12-frontend/tests/browser/project-scope.mjs');
      await verifyProjectScopeUi({ apiUrl: fixture.apiUrl, seed });
    }
  } finally { await fixture.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
