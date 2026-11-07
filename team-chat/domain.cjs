"use strict";
const { randomBytes, scrypt: derive, timingSafeEqual } = require("node:crypto");
const { promisify } = require("node:util");
const scrypt = promisify(derive);
const roles = {
  employee: "Сотрудник",
  manager: "Управляющий",
  director: "Руководитель компании",
};
function fail(message, status = 400) {
  throw Object.assign(new Error(message), { status });
}
function text(value, max, required = true) {
  if (
    typeof value !== "string" ||
    value.length > max ||
    (required && !value.trim())
  )
    fail("Проверьте заполнение полей");
  return value.trim();
}
function phone(value) {
  let p = text(value, 40).replace(/[\s()\-]/g, "");
  if (/^[78]\d{10}$/.test(p)) p = "+7" + p.slice(1);
  else if (/^\d{10}$/.test(p)) p = "+7" + p;
  if (!/^\+[1-9]\d{9,14}$/.test(p))
    fail("Введите номер телефона с кодом страны");
  return p;
}
async function hashPassword(password) {
  if (
    typeof password !== "string" ||
    password.length < 10 ||
    password.length > 128
  )
    fail("Пароль должен содержать от 10 до 128 символов");
  const salt = randomBytes(16).toString("hex");
  return { salt, hash: (await scrypt(password, salt, 64)).toString("hex") };
}
async function verifyPassword(password, stored) {
  if (typeof password !== "string" || password.length > 128 || !stored?.hash)
    return false;
  const actual = await scrypt(password, stored.salt, 64),
    expected = Buffer.from(stored.hash, "hex");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
module.exports = { roles, fail, text, phone, hashPassword, verifyPassword };
