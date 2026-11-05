const { test } = require("node:test"),
  assert = require("node:assert/strict");
const {
  phone,
  hashPassword,
  verifyPassword,
  decision,
  money,
  forecast,
} = require("../team-chat/domain.cjs");
const { projectCash } = require("../team-chat/balances.cjs");
test("phone/password identity normalizes KZ numbers, hashes passwords and rejects malformed logins", async () => {
  assert.equal(phone("8 (701) 123-45-67"), "+77011234567");
  assert.equal(phone("7011234567"), "+77011234567");
  assert.throws(() => phone("admin"));
  const p = await hashPassword("fixture-passphrase");
  assert.equal(await verifyPassword("fixture-passphrase", p), true);
  assert.equal(await verifyPassword("wrong-password", p), false);
  assert.equal(JSON.stringify(p).includes("fixture-passphrase"), false);
  await assert.rejects(hashPassword("short"));
});
test("expense workflow enforces both approval steps and rejects skipping or repeating a stage", () => {
  assert.equal(
    decision("manager_review", "manager", "approve"),
    "director_review",
  );
  assert.equal(decision("director_review", "director", "approve"), "ready");
  assert.equal(decision("manager_review", "manager", "reject"), "rejected");
  for (const [state, role] of [
    ["manager_review", "employee"],
    ["manager_review", "director"],
    ["director_review", "manager"],
    ["ready", "manager"],
    ["rejected", "director"],
  ])
    assert.throws(() => decision(state, role, "approve"));
});
test("cash forecast includes unapproved purchases once, excludes rejected/recorded, isolates companies and respects payment dates", () => {
  const accounts = [
    { _id: "a", companyId: "c1", initialBalance: 1000 },
    { _id: "b", companyId: "c2", initialBalance: 9000 },
  ];
  const requests = [
    {
      companyId: "c1",
      amountCents: 120000,
      dueDate: "2026-11-06",
      state: "manager_review",
    },
    {
      companyId: "c1",
      amountCents: 50000,
      dueDate: "2026-11-07",
      state: "rejected",
    },
    {
      companyId: "c1",
      amountCents: 30000,
      dueDate: "2026-11-07",
      state: "recorded",
      operationId: "paid",
    },
  ];
  assert.equal(forecast(requests), 120000);
  const r = projectCash(
    accounts,
    [
      {
        type: "income",
        amount: 500,
        accountId: "a",
        date: "2026-11-07T07:00:00Z",
      },
    ],
    requests,
    "2026-11-05",
  );
  const c = r.companies.find((c) => c.companyId === "c1");
  assert.equal(c.currentCents, 100000);
  assert.equal(c.minimumCents, -20000);
  assert.equal(c.gapDate, "2026-11-06");
  assert.equal(c.projectedCents, 30000);
  assert.equal(
    r.companies.find((c) => c.companyId === "c2").minimumCents,
    900000,
  );
  assert.equal(money("123.45"), 12345);
  assert.throws(() => money("1.001"));
});
test("cash projection handles transfers, withdrawals, split totals and already recorded expenses without counting a request twice", () => {
  const a = [
    { _id: "a", companyId: "c", initialBalance: 1000 },
    { _id: "b", companyId: "c", initialBalance: 0 },
  ];
  const ops = [
    {
      type: "transfer",
      amount: 200,
      fromAccountId: "a",
      toAccountId: "b",
      date: "2026-11-04T07:00:00Z",
    },
    {
      type: "transfer",
      transferPurpose: "withdrawal",
      amount: 100,
      fromAccountId: "a",
      toAccountId: "b",
      date: "2026-11-05T07:00:00Z",
    },
    {
      type: "expense",
      amount: -300,
      accountId: "a",
      date: "2026-11-05T07:00:00Z",
    },
    {
      type: "expense",
      amount: -900,
      excludeFromTotals: true,
      accountId: "a",
      date: "2026-11-05T07:00:00Z",
    },
  ];
  const r = projectCash(
    a,
    ops,
    [
      {
        state: "recorded",
        amountCents: 30000,
        companyId: "c",
        operationId: "id",
        dueDate: "2026-11-05",
      },
    ],
    "2026-11-05",
  );
  assert.equal(r.companies[0].currentCents, 60000);
  assert.equal(r.companies[0].requestedCents, 0);
});

test("an unassigned staff request never borrows an arbitrary managed company cash pool", () => {
  const result = projectCash(
    [
      { _id: "a", companyId: "first", initialBalance: 1000 },
      { _id: "b", companyId: "second", initialBalance: 2000 },
    ],
    [],
    [
      {
        companyId: null,
        amountCents: 50000,
        dueDate: "2026-11-06",
        state: "manager_review",
      },
    ],
    "2026-11-05",
  );
  assert.equal(result.unallocatedCents, 50000);
  assert.deepEqual(
    result.companies.map((c) => c.minimumCents),
    [100000, 200000],
  );
});

test("per-account cash shows pending and approved requests without borrowing from another account or double counting a recorded purchase", () => {
  const requests = [
    {
      state: "manager_review",
      companyId: "c",
      amountCents: 20000,
      dueDate: "2026-11-06",
      placement: { accountId: "a" },
    },
    {
      state: "ready",
      companyId: "c",
      amountCents: 30000,
      dueDate: "2026-11-07",
      placement: { accountId: "a" },
    },
    {
      state: "director_review",
      companyId: "c",
      amountCents: 10000,
      dueDate: "2026-11-07",
      placement: { accountId: "b" },
    },
    {
      state: "recorded",
      companyId: "c",
      amountCents: 10000,
      dueDate: "2026-11-05",
      placement: { accountId: "a" },
      operationId: "saved",
    },
  ];
  const result = projectCash(
    [
      { _id: "a", name: "Bank", companyId: "c", initialBalance: 1000 },
      { _id: "b", name: "Cash", companyId: "c", initialBalance: 2000 },
    ],
    [
      {
        type: "expense",
        amount: -100,
        accountId: "a",
        date: "2026-11-05T07:00:00Z",
      },
      {
        type: "income",
        amount: 200,
        accountId: "a",
        date: "2026-11-06T07:00:00Z",
      },
    ],
    requests,
    "2026-11-05",
  );
  const bank = result.accounts.find((a) => a._id === "a"),
    cash = result.accounts.find((a) => a._id === "b");
  assert.equal(bank.balanceCents, 90000);
  assert.equal(bank.pendingCents, 20000);
  assert.equal(bank.approvedCents, 30000);
  assert.equal(bank.afterRequestsCents, 40000);
  assert.equal(bank.projectedCents, 60000);
  assert.equal(cash.balanceCents, 200000);
  assert.equal(cash.pendingCents, 10000);
  assert.equal(cash.approvedCents, 0);
  assert.equal(cash.afterRequestsCents, 190000);
});
