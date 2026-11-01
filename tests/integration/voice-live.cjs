'use strict';
// Opt-in paid Realtime API + real Chrome audio + Codex/MCP on disposable data.
// Only OPENAI_API_KEY comes from the caller; production database/config is never read.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { promisify } = require('node:util');
const { execFile } = require('node:child_process');
const mongoose = require('mongoose');
const { startFixture } = require('./subcategories.cjs');
const { createDesktopRouter } = require('../../codex/router.cjs');
const { VoiceService } = require('../../codex/voice.cjs');
const { CodexClient } = require('../../codex/codexClient.cjs');
const { ExecutionStore } = require('../../codex/executionStore.cjs');
const { readMcpAccess } = require('../../codex/mcpPolicy.cjs');
const { chromium } = require('../../../index12-frontend/node_modules/playwright-core');
const execute = promisify(execFile);

(async () => {
  assert.ok(process.env.OPENAI_API_KEY, 'Pass OPENAI_API_KEY for this opt-in live voice test');
  const fixture = await startFixture(), directory = await fs.mkdtemp(path.join(os.tmpdir(), 'index12-voice-live-'));
  let browser, codex, timer, processing, voice;
  const threads = [];
  try {
    const db = mongoose.connection.db, actor = new mongoose.Types.ObjectId(), workspace = new mongoose.Types.ObjectId(), account = new mongoose.Types.ObjectId();
    const scope = { actorId: String(actor), workspaceId: String(workspace) }, chatId = randomUUID(), workerId = randomUUID();
    await db.collection('users').insertOne({ _id: actor, name: 'Synthetic voice admin' });
    await db.collection('workspaces').insertOne({ _id: workspace, userId: String(actor), name: 'Synthetic voice workspace', isDefault: true });
    await db.collection('accounts').insertOne({ _id: account, userId: actor, name: 'Голосовой тест', initialBalance: 12345.67 });
    await db.collection('desktopChats').insertOne({ _id: chatId, ...scope, archived: false, title: 'Synthetic live voice', executionProtocol: 2 });
    const renew = () => db.collection('desktopWorkers').updateOne({ _id: `${actor}:${workspace}` }, { $set: { instanceId: workerId, seenAt: new Date() } }, { upsert: true });
    await renew();
    const configFile = path.join(directory, 'config.json');
    await fs.writeFile(configFile, JSON.stringify({ actorId: String(actor), ownerId: String(actor), workspaceId: String(workspace) }), { mode: 0o600 });
    const env = { PATH: process.env.PATH, INDEX12_MCP_CONFIG: configFile, INDEX12_MCP_DB_URL: 'mongodb://127.0.0.1:27028/subcategory_test' };
    await execute(process.execPath, [path.resolve(__dirname, '../../mcp/configure.cjs'), '--api-url', fixture.apiUrl.replace(/\/api$/, ''), '--confirmation-mode', 'direct'], { env });
    const binding = JSON.parse(await fs.readFile(configFile, 'utf8')), access = await readMcpAccess(binding);
    const executions = new ExecutionStore(db); await executions.ready();
    codex = await new CodexClient({ transport: 'shared' }).connect();
    const invoke = codex.call.bind(codex);
    codex.call = (method, params) => invoke(method, ['thread/start', 'thread/resume', 'thread/fork'].includes(method) ? { ...params, config: { ...params.config,
      'mcp_servers.index12.command': process.execPath, 'mcp_servers.index12.args': [path.resolve(__dirname, '../../mcp/server.cjs')],
      'mcp_servers.index12.env.INDEX12_MCP_CONFIG': configFile, 'mcp_servers.index12.env.INDEX12_MCP_DB_URL': env.INDEX12_MCP_DB_URL,
    } } : params);
    async function consume() {
      await renew();
      if (processing) return;
      const job = await db.collection('desktopJobs').findOneAndUpdate({ ...scope, kind: 'send', status: 'queued', active: true }, { $set: { status: 'running', workerId } }, { returnDocument: 'after' });
      if (!job) return;
      processing = (async () => {
        await executions.open(job, workerId);
        try {
          let chat = await db.collection('desktopChats').findOne({ _id: chatId });
          if (!chat.threadId) {
            const thread = await codex.createThread({ cwd: directory, access, durable: true }); threads.push(thread.id);
            await db.collection('desktopChats').updateOne({ _id: chatId }, { $set: { threadId: thread.id } }); chat = { ...chat, threadId: thread.id };
          }
          const result = await codex.send(chat.threadId, job.message, { access, durable: true,
            onTurnStarted: ({ turnId }) => executions.bind(job._id, workerId, chat.threadId, turnId),
            onToolFailure: detail => executions.stop({ executionId: job._id }, scope, detail),
          });
          await executions.finish(job._id, workerId);
          await db.collection('desktopJobs').updateOne({ _id: job._id }, { $set: { status: 'completed', active: false, result: { ...result, threadId: chat.threadId } } });
        } catch (error) {
          await executions.finish(job._id, workerId, 'failed');
          await db.collection('desktopJobs').updateOne({ _id: job._id }, { $set: { status: 'failed', active: false, error: error.message } });
        }
      })().finally(() => { processing = null; });
    }
    timer = setInterval(() => consume().catch(error => console.error('Synthetic worker:', error.name)), 500);
    voice = new VoiceService({ db: () => db, Workspace: fixture.identityModels.Workspace });
    fixture.app.use('/voice-api', createDesktopRouter({ db: () => db, Workspace: fixture.identityModels.Workspace, voiceService: voice, frontendUrl: fixture.apiUrl.replace(/\/api$/, ''),
      isAuthenticated: (req, _res, next) => { req.user = { id: String(actor), currentWorkspaceId: String(workspace) }; next(); } }));
    fixture.app.get('/voice-client.js', async (_req, res) => res.type('application/javascript').send(await fs.readFile(path.resolve(__dirname, '../../../index12-frontend/src/utils/codexVoice.js'), 'utf8')));
    fixture.app.get('/voice-live', (_req, res) => res.type('html').send(`<!doctype html><button id="start">Start synthetic voice</button><script type="module">
      import { CodexVoiceConnection } from '/voice-client.js';
      window.voiceDebug={states:[],captions:[],events:[],errors:[]};
      const api=async(p,b)=>{const r=await fetch('/voice-api'+p,{...(b?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}:{})});const d=await r.json();if(!r.ok)throw new Error(d.error);return d;};
      const client=new CodexVoiceConnection({api,onState:s=>voiceDebug.states.push(s),onCaption:(r,t)=>voiceDebug.captions.push({r,t}),onError:e=>voiceDebug.errors.push(e.message)});window.voiceClient=client;
      document.querySelector('#start').onclick=async()=>{try{const id=crypto.randomUUID();voiceDebug.id=id;await client.start('${chatId}',id);let seq=0;window.voicePoll=setInterval(async()=>{try{const s=await api('/voice/'+id+'?after='+seq);for(const e of s.events){seq=Math.max(seq,e.seq);voiceDebug.events.push(e);if(e.type==='transcript')client.mute(true);if(e.type==='result')voiceDebug.reply=e.text;}}catch(e){voiceDebug.errors.push(e.message)}},500);}catch(e){voiceDebug.errors.push(e.message)}};
    </script>`));
    const aiff = path.join(directory, 'question.aiff'), wav = path.join(directory, 'question.wav');
    await execute('say', ['-v', 'Milena', '-o', aiff, 'Проверь в Индекс двенадцать, сколько денег на счёте Голосовой тест.']);
    await execute('ffmpeg', ['-y', '-i', aiff, '-af', 'adelay=20000,apad=pad_dur=60', '-ar', '48000', '-ac', '1', wav]);
    browser = await chromium.launch({ executablePath: process.env.SCROLL_TEST_CHROME || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', headless: true,
      args: ['--use-fake-ui-for-media-stream', '--use-fake-device-for-media-stream', `--use-file-for-fake-audio-capture=${wav}`, '--autoplay-policy=no-user-gesture-required'] });
    const page = await browser.newPage();
    await page.goto(fixture.apiUrl.replace(/\/api$/, '') + '/voice-live');
    await page.click('#start');
    try { await page.waitForFunction(() => window.voiceDebug.reply || window.voiceDebug.errors.length, null, { timeout: 120000 }); }
    catch (error) { console.log(JSON.stringify(await page.evaluate(() => ({ debug: window.voiceDebug, connection: voiceClient.peer?.connectionState })))); throw error; }
    const result = await page.evaluate(() => window.voiceDebug);
    assert.deepEqual(result.errors, [], JSON.stringify(result));
    assert.match(result.reply.replace(/\s/g, ''), /12[\s\u202f]?345[,.]67/);
    await page.waitForFunction(() => voiceDebug.states.includes('speaking') && voiceDebug.captions.some(c => c.r.startsWith('assistant') && c.t.length), null, { timeout: 30000 });
    const audio = await page.evaluate(async () => { const stats = await voiceClient.peer.getStats(); return [...stats.values()].filter(s => s.type === 'inbound-rtp' && s.kind === 'audio').map(s => ({ bytes: s.bytesReceived, energy: s.totalAudioEnergy })); });
    assert.ok(audio.some(s => s.bytes > 0), JSON.stringify(audio));
    await page.evaluate(async () => { clearInterval(window.voicePoll); voiceClient.stop(); await fetch('/voice-api/voice/'+voiceDebug.id+'/stop',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); });
    assert.equal(await page.evaluate(() => voiceClient.stream.getTracks().every(track => track.readyState === 'ended')), true);
    assert.equal(await db.collection('events').countDocuments({}), 0, 'voice test only reads the synthetic account');
    assert.equal((await db.collection('accounts').findOne({ _id: account })).initialBalance, 12345.67);
    assert.equal(await db.collection('desktopJobs').countDocuments({ ...scope, kind: 'send' }), 1);
    console.log(JSON.stringify({ liveVoice: true, recognizedOriginalSpeech: result.events.find(e => e.type === 'transcript')?.text, codexReadSyntheticAccount: true, responseAudioReceived: true, microphoneReleased: true, financialWrites: 0 }));
  } finally {
    clearInterval(timer);
    if (voice) for (const session of voice.sessions.values()) await voice.close(session).catch(() => {});
    await browser?.close(); await processing?.catch(() => {});
    if (codex) { for (const id of threads) await codex.call('thread/archive', { threadId: id }).catch(() => {}); codex.close(); }
    await fixture.close(); await fs.rm(directory, { recursive: true, force: true });
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
