'use strict';
const LIMIT = 2 * 1024 * 1024;
function validateAttachments(value = []) {
  if (!Array.isArray(value) || value.length > 3) throw new Error('Можно прикрепить до трёх изображений.');
  let total = 0;
  return value.map(file => {
    if (!file || typeof file.name !== 'string' || file.name.length > 255 || typeof file.url !== 'string' || file.url.length > Math.ceil(LIMIT / 3) * 4 + 64) throw new Error('Некорректное изображение.');
    const match = /^data:image\/(png|jpeg|webp|gif);base64,([A-Za-z0-9+/]+={0,2})$/.exec(file.url);
    if (!match) throw new Error('Поддерживаются изображения PNG, JPEG, WebP и GIF.');
    const bytes = Buffer.from(match[2], 'base64'); total += bytes.length;
    const type = match[1];
    const valid = type === 'png' ? bytes.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10]))
      : type === 'jpeg' ? bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255
      : type === 'webp' ? bytes.toString('ascii', 0, 4) === 'RIFF' && bytes.toString('ascii', 8, 12) === 'WEBP'
      : /^GIF8[79]a$/.test(bytes.toString('ascii', 0, 6));
    if (!valid || bytes.toString('base64') !== match[2]) throw new Error('Некорректное содержимое изображения.');
    if (total > LIMIT) throw new Error('Общий размер изображений должен быть не больше 2 МБ.');
    return { name: file.name, url: file.url };
  });
}
module.exports = { validateAttachments };
