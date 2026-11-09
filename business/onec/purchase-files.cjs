'use strict';
const https = require('node:https');
const dns = require('node:dns/promises');
const { BlockList, isIP } = require('node:net');
const { safeName, FILE_LIMIT } = require('../../codex/files.cjs');
const denied = new BlockList();
for (const [ip, prefix] of [['0.0.0.0',8],['10.0.0.0',8],['100.64.0.0',10],['127.0.0.0',8],['169.254.0.0',16],['172.16.0.0',12],['192.0.0.0',24],['192.0.2.0',24],['192.168.0.0',16],['198.18.0.0',15],['198.51.100.0',24],['203.0.113.0',24],['224.0.0.0',4],['240.0.0.0',4]]) denied.addSubnet(ip,prefix,'ipv4');
for (const [ip, prefix] of [['::',128],['::1',128],['64:ff9b::',96],['100::',64],['2001::',32],['2001:db8::',32],['2002::',16],['fc00::',7],['fe80::',10],['ff00::',8]]) denied.addSubnet(ip,prefix,'ipv6');
const publicAddress = ip => !!isIP(ip) && !ip.toLowerCase().startsWith('::ffff:') && !denied.check(ip,isIP(ip) === 6 ? 'ipv6' : 'ipv4');
function pdfBytes(data, name) {
  if (!Buffer.isBuffer(data) || !data.length || data.length > FILE_LIMIT || data.toString('ascii',0,5) !== '%PDF-') throw Error('Нужен оригинальный PDF до 10 МБ');
  return { data, name: safeName(name) || 'Счёт.pdf', mimeType: 'application/pdf', size: data.length };
}
// Never send application credentials; pin the resolved public address to avoid
// DNS rebinding. Temporary ChatGPT URLs are not stored in MongoDB or logs.
async function downloadFile(file, { resolve=dns.lookup, request=https.get }={}) {
  const url = new URL(file.download_url);
  if (url.protocol !== 'https:' || url.username || url.password || url.hash || (url.port && url.port !== '443')) throw Error('Нужна защищённая ссылка на файл');
  const addresses = await resolve(url.hostname, { all: true });
  if (!addresses.length || addresses.some(row => !publicAddress(row.address))) throw Error('Адрес файла недоступен');
  const target = addresses[0];
  const data = await new Promise((resolve,reject) => {
    const outgoing = request(url, { lookup: (_host,options,done) => options?.all ? done(null,[target]) : done(null,target.address,target.family), signal: AbortSignal.timeout(20000) }, response => {
      if (response.statusCode !== 200 || Number(response.headers['content-length'] || 0) > FILE_LIMIT) { response.destroy(); reject(Error('Не удалось скачать файл')); return; }
      const chunks=[]; let size=0;
      response.on('data',chunk=>{size+=chunk.length; if(size>FILE_LIMIT) response.destroy(Error('Файл больше 10 МБ')); else chunks.push(chunk);});
      response.on('end',()=>resolve(Buffer.concat(chunks))); response.on('error',reject);
    }); outgoing.on('error',reject);
  });
  return data;
}
async function downloadPdf(file, options) { return pdfBytes(await downloadFile(file, options), file.file_name); }
module.exports={downloadPdf,downloadFile,pdfBytes,publicAddress};
