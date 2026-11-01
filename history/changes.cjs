"use strict";
const { isDeepStrictEqual } = require("node:util");
const clone = (value) =>
  value === undefined ? undefined : JSON.parse(JSON.stringify(value));
const plain = (value) =>
  value && typeof value === "object" && !Array.isArray(value);
const unsafe = (key) =>
  ["__proto__", "constructor", "prototype"].includes(key) ||
  key.includes(".") ||
  key.startsWith("$");
function at(value, path) {
  for (const key of path) {
    if (value == null || !Object.hasOwn(value, key)) return { exists: false };
    value = value[key];
  }
  return { exists: true, value: clone(value) };
}
function put(value, path, field) {
  if (!path.length) return field.exists ? clone(field.value) : null;
  const out = clone(value) || {};
  let node = out;
  for (const key of path.slice(0, -1)) {
    if (!plain(node[key])) node[key] = {};
    node = node[key];
  }
  if (field.exists) node[path.at(-1)] = clone(field.value);
  else delete node[path.at(-1)];
  return out;
}
function diff(before, after, path = []) {
  if (isDeepStrictEqual(before, after)) return [];
  if (plain(before) && plain(after))
    return [...new Set([...Object.keys(before), ...Object.keys(after)])]
      .filter((k) => !unsafe(k))
      .flatMap((k) => diff(before[k], after[k], [...path, k]));
  return [
    {
      path,
      before:
        before === undefined
          ? { exists: false }
          : { exists: true, value: clone(before) },
      after:
        after === undefined
          ? { exists: false }
          : { exists: true, value: clone(after) },
    },
  ];
}
function revert(current, changes) {
  let result = clone(current);
  for (const change of changes) {
    if (!isDeepStrictEqual(at(current, change.path), change.after))
      throw Object.assign(
        new Error(
          "Это поле уже изменено после выбранной записи. Отмена не выполнена.",
        ),
        { statusCode: 409 },
      );
    result = put(result, change.path, change.before);
  }
  return result;
}
const GROUPS = [
  ["date", ["date", "dateKey", "dayOfYear", "cellIndex"]],
  ["amount", ["amount"]],
  ["category", ["categoryId", "categoryIds", "subcategoryId"]],
  ["account", ["accountId", "companyId", "individualId"]],
  [
    "route",
    [
      "fromAccountId",
      "toAccountId",
      "fromCompanyId",
      "toCompanyId",
      "fromIndividualId",
      "toIndividualId",
      "transferPurpose",
      "isTransfer",
      "isWithdrawal",
    ],
  ],
  ["contractor", ["contractorId", "counterpartyIndividualId"]],
  [
    "project",
    [
      "projectId",
      "projectScope",
      "splitMeta",
      "isSplitParent",
      "excludeFromTotals",
    ],
  ],
  ["description", ["description"]],
  ["recipient", ["destination", "withdrawalRecipient"]],
  ["reason", ["transferReason"]],
  ["status", ["isClosed"]],
  ["salary", ["isSalary"]],
  ["offset", ["offsetIncomeId", "relatedEventId"]],
  ["type", ["type"]],
];
const EDIT_KEYS = GROUPS.flatMap(([, keys]) => keys);
const logical = (value, key) =>
  value == null ? (key === "description" ? "" : null) : value;
function operationChanges(before, after) {
  return GROUPS.filter(([, keys]) =>
    keys.some(
      (key) =>
        !isDeepStrictEqual(logical(before[key], key), logical(after[key], key)),
    ),
  ).map(([field, keys]) => ({
    field,
    keys,
    before: Object.fromEntries(keys.map((k) => [k, logical(before[k], k)])),
    after: Object.fromEntries(keys.map((k) => [k, logical(after[k], k)])),
  }));
}
const refs = ["accountId", "fromAccountId", "toAccountId"];
const accountIds = (rows) => [
  ...new Set(
    rows.flatMap((row) =>
      refs
        .map((k) => row?.[k])
        .filter(Boolean)
        .map((v) => String(v._id || v)),
    ),
  ),
];
const operationType = (row) =>
  row.isWithdrawal
    ? "withdrawal"
    : row.type === "transfer" || row.isTransfer
      ? "transfer"
      : row.type;
module.exports = {
  clone,
  at,
  put,
  diff,
  revert,
  GROUPS,
  EDIT_KEYS,
  operationChanges,
  accountIds,
  operationType,
};
