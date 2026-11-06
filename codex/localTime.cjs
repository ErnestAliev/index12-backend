"use strict";
const { dayAt } = require("../business/task-domain.cjs");
function localTimeContext(value = Date.now()) {
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) throw Error("Invalid message time");
  return `Дата сообщения в Asia/Almaty: ${dayAt(time)}. В этом поручении «сегодня» = ${dayAt(time)}, «вчера» = ${dayAt(time - 86400000)}, «завтра» = ${dayAt(time + 86400000)}. Явно указанная пользователем календарная дата имеет приоритет.`;
}
module.exports = { localTimeContext };
