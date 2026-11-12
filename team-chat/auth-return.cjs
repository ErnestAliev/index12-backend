'use strict';
// Only owner chat entry links may survive OAuth; never accept arbitrary redirects.
function chatAuthReturn(value, allowedOrigins) {
  if (typeof value !== 'string' || value.length > 700) return null;
  let url;
  try { url = new URL(value); } catch { return null; }
  if (!allowedOrigins.includes(url.origin) || url.username || url.password || url.hash || !['/', '/mobile'].includes(url.pathname)) return null;
  if (!/^[a-f\d]{24}$/i.test(url.searchParams.get('teamChat') || '')) return null;
  if ([...url.searchParams.keys()].some(key => !['teamChat', 'task', 'message','release'].includes(key))) return null;
  if(url.searchParams.has('release') && !/^[a-f\d]{40}$/i.test(url.searchParams.get('release')))return null;
  const uuid = /^[a-f\d]{8}-(?:[a-f\d]{4}-){3}[a-f\d]{12}$/i;
  for (const key of ['task', 'message']) if (url.searchParams.has(key) && !uuid.test(url.searchParams.get(key))) return null;
  return url.href;
}
module.exports = { chatAuthReturn };
