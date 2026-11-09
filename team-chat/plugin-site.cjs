'use strict';
const path = require('node:path');
function registerPluginSite(app) {
  // Dedicated to the employee plugin; the endpoint was previously absent.
  app.get('/.well-known/openai-apps-challenge', (_req, res) => {
    res.type('text/plain').set('Cache-Control', 'no-store').sendFile(path.resolve(__dirname, '../public/team-plugin/openai-apps-challenge.txt'));
  });
  for (const page of ['index', 'privacy', 'terms', 'support']) {
    const route = '/plugins/index12-team' + (page === 'index' ? '' : '/' + page);
    app.get(route, (_req, res) => {
      res.set({ 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'same-origin', 'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'" });
      res.sendFile(path.resolve(__dirname, '../public/team-plugin', page + '.html'));
    });
  }
}
module.exports = { registerPluginSite };
