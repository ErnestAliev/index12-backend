const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  phone,
  hashPassword,
  verifyPassword,
} = require("../team-chat/domain.cjs");
const { taskInput } = require("../team-chat/tasks.cjs");
const { deadlineFields } = require('../team-chat/deadlines.cjs');
test('private overdue recipient follows the time setter, never guesses from later unrelated edits', () => {
  const task = { timing: 'deadline', dueDate: '2026-11-08', dueTime: '22:00', status: 'open' };
  const created = { ...task, ...deadlineFields(task, null, 'time-setter'), creatorId: 'author', version: 3 };
  assert.equal(created.deadlineAt.toISOString(), '2026-11-08T17:00:00.000Z');
  assert.equal(deadlineFields(task, created, 'text-editor').deadlineActorId, 'time-setter');
  const moved = deadlineFields({ ...task, dueTime: '23:00' }, created, 'new-setter');
  assert.equal(moved.deadlineActorId, 'new-setter');
  assert.equal(moved.deadlineRevision, created.deadlineRevision + 1);
  assert.equal(deadlineFields(task, { ...task, creatorId: 'author', version: 4 }, 'text-editor').deadlineActorId, null);
  assert.equal(deadlineFields(task, { ...task, creatorId: 'author', version: 0 }, 'text-editor').deadlineActorId, 'author');
});
test("chat login normalizes phones and stores password hashes", async () => {
  assert.equal(phone("8 (701) 123-45-67"), "+77011234567");
  assert.throws(() => phone("admin"));
  const p = await hashPassword("fixture-passphrase");
  assert.equal(await verifyPassword("fixture-passphrase", p), true);
  assert.equal(await verifyPassword("wrong", p), false);
  await assert.rejects(hashPassword("short"));
});
test("arbitrary tasks need no price, contractor or deadline", () => {
  const task = taskInput({
    description: "Сверить документы",
    assigneeIds: ["person"],
    timing: "none",
  });
  assert.equal(task.dueDate, null);
  assert.equal(task.status, "open");
  assert.deepEqual(task.assigneeIds, ["person"]);
});
test("deadlines validate actual calendar dates, time, recipients and files", () => {
  const base = {
    description: "Сверить документы",
    assigneeIds: ["person"],
    timing: "deadline",
    dueDate: "2028-02-29",
    dueTime: "18:05",
  };
  assert.equal(taskInput(base).dueTime, "18:05");
  for (const changes of [
    { dueDate: "2026-02-29" },
    { dueDate: null },
    { dueTime: "24:00" },
    { timing: "random" },
    { assigneeIds: [] },
    { assigneeIds: {} },
    { fileIds: ["bad"] },
    { status: "manager_review" },
    { description: "" },
  ])
    assert.throws(() => taskInput({ ...base, ...changes }));
  const cleared = taskInput({ ...base, timing: "none" });
  assert.equal(cleared.dueDate, null);
  assert.equal(cleared.dueTime, null);
});
