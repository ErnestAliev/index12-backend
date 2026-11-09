'use strict';
const path = require('node:path');
function registerPluginSite(app) {
  app.get('/plugins/index12-team/review-demo-20261109.mp4', (_req, res) => {
    res.type('video/mp4').set({ 'X-Content-Type-Options': 'nosniff', 'Content-Disposition': 'inline; filename="index12-team-review.mp4"' }).sendFile(path.resolve(__dirname, '../public/team-plugin/review-demo-20261109.mp4'));
  });
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
