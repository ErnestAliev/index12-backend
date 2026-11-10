'use strict';
async function containerFileId({ containerId, containerPath, containers, api, signal }) {
  if (!containers.has(containerId)) throw Error('Файл должен принадлежать Python-контейнеру этого запроса');
  const path = String(containerPath || '').replace(/^sandbox:/, '');
  if (!path.startsWith('/mnt/data/') || /[\\\0]/.test(path) || path.split('/').some(part => part === '..' || part === '.')) throw Error('Укажите точный путь файла внутри /mnt/data/');
  const route = `containers/${encodeURIComponent(containerId)}/files`, seen = new Set();
  let after;
  for (let page = 0; page < 20; page++) {
    signal.throwIfAborted();
    const params = new URLSearchParams({ limit: '100', ...(after ? { after } : {}) });
    const listing = await (await api(`${route}?${params}`, {}, signal)).json();
    const matches = (listing.data || []).filter(file => file.container_id === containerId && file.path === path);
    if (matches.length > 1) throw Error('Несколько файлов с одинаковым путём. Укажите containerFileId.');
    if (matches.length === 1 && typeof matches[0].id === 'string') return matches[0].id;
    if (!listing.has_more) break;
    if (!listing.last_id || seen.has(listing.last_id)) throw Error('Не удалось завершить поиск файла контейнера.');
    after = listing.last_id; seen.add(after);
  }
  throw Error('Файл с таким путём не найден в контейнере. Сначала сохраните документ через code_interpreter.');
}
module.exports = { containerFileId };
