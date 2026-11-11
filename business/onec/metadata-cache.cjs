'use strict';
// Per-process publication metadata only. Keys include the connection and credentials.
// Data queries and object versions are never cached here.
class MetadataCache {
  constructor({ ttlMs = 300000, limit = 8, now = Date.now } = {}) {
    Object.assign(this, { ttlMs, limit, now });
    this.entries = new Map();
  }
  load(key, loader, { refresh = false } = {}) {
    const cached = this.entries.get(key);
    if (!refresh && cached && cached.expiresAt > this.now()) return cached.promise;
    const entry = { expiresAt: this.now() + this.ttlMs };
    entry.promise = Promise.resolve().then(loader).then(value => {
      entry.expiresAt = this.now() + this.ttlMs;
      return value;
    }).catch(error => {
      if (this.entries.get(key) === entry) this.entries.delete(key);
      throw error;
    });
    this.entries.delete(key);
    this.entries.set(key, entry);
    while (this.entries.size > this.limit) this.entries.delete(this.entries.keys().next().value);
    return entry.promise;
  }
}
module.exports = { MetadataCache, metadataCache: new MetadataCache() };
