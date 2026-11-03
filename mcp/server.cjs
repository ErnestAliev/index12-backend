#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { MongoClient } = require('mongoose').mongo;
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const finance = require('./finance.cjs');
const { createMcpRuntime } = require('./runtime.cjs');
const ROOT = path.resolve(__dirname, '..');
function readConfig() {
  const file=process.env.INDEX12_MCP_CONFIG || path.join(ROOT,'.mcp.local.json');
  if (!fs.existsSync(file)) throw new finance.PublicError('Локальная конфигурация MCP не найдена. См. mcp/README.md.');
  const config=JSON.parse(fs.readFileSync(file,'utf8'));
  let uri=process.env.INDEX12_MCP_DB_URL;
  if (!uri && fs.existsSync(path.join(ROOT,'.env'))) uri=require('dotenv').parse(fs.readFileSync(path.join(ROOT,'.env'))).DB_URL;
  if (!uri) throw new finance.PublicError('Не настроено подключение к базе INDEX12.');
  return { config,uri };
}
let client;
let connection;
async function database() {
  const {config,uri}=readConfig();
  if (!connection) {
    client=new MongoClient(uri,{serverSelectionTimeoutMS:15000,maxPoolSize:2,appName:'index12-mcp',readConcern:{level:'majority'}});
    connection=client.connect().catch(error=>{connection=null; throw error;});
  }
  await connection;
  return client.db();
}
const { server } = createMcpRuntime({ config: () => readConfig().config, database, runtimeEnv: process.env });
async function close() { await server.close(); if (client) await client.close(); }
process.on('SIGINT', () => { close().finally(() => process.exit(0)); });
process.on('SIGTERM', () => { close().finally(() => process.exit(0)); });
server.connect(new StdioServerTransport()).catch(() => { console.error('INDEX12 MCP: ошибка запуска.'); process.exitCode = 1; });
