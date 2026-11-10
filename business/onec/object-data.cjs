'use strict';
const { fault, GUID, ZERO, cents } = require('./purchase-domain.cjs');
const { dateOnly } = require('../task-domain.cjs');
const referenceKinds = /^(Document|Catalog|BusinessProcess|Task|ChartOfAccounts|ChartOfCharacteristicTypes|ChartOfCalculationTypes|ExchangePlan)_/;
const isReferenceObject = set => referenceKinds.test(set.name) && set.keys.length === 1 && set.keys[0] === 'Ref_Key';
const protectedFields = new Set(['Ref_Key','DataVersion','Posted','DeletionMark','IsPredefined','PredefinedDataName','__proto__','prototype','constructor']);
// Compatibility with previously cached metadata; new metadata resolves every
// GUID target from its published NavigationProperty/Association, not its name.
const legacyTargets = { Организация_Key:'Catalog_Организации',Контрагент_Key:'Catalog_Контрагенты',ВалютаДокумента_Key:'Catalog_Валюты',ДоговорКонтрагента_Key:'Catalog_ДоговорыКонтрагентов',СчетОрганизации_Key:'Catalog_БанковскиеСчета',СчетКонтрагента_Key:'Catalog_БанковскиеСчета',Номенклатура_Key:'Catalog_Номенклатура',СтавкаНДС_Key:'Catalog_СтавкиНДС',Склад_Key:'Catalog_Склады',Автор_Key:'Catalog_Пользователи',Ответственный_Key:'Catalog_Пользователи',СчетУчетаРасчетовСКонтрагентомБУ_Key:'ChartOfAccounts_Типовой',СчетУчетаРасчетовПоАвансам_Key:'ChartOfAccounts_Типовой',СчетУчетаРасчетовСКонтрагентомНУ_Key:'ChartOfAccounts_Налоговый',СтатьяДвиженияДенежныхСредств_Key:'Catalog_СтатьиДвиженияДенежныхСредств' };
async function validateObjectData(client, collection, input, { create = false, depth = 0, cache = new Map() } = {}) {
  if (!input || Array.isArray(input) || typeof input !== 'object' || depth > 2) fault('INVALID_ONEC_FIELD','Передайте объект реквизитов 1С');
  const schema = await client.schema(collection), props = new Map(schema.properties.map(p => [p.name,p]));
  const result = {};
  async function reference(target, id, key) {
    if (!GUID.test(id || '')) fault('INVALID_ONEC_REFERENCE',`Некорректная ссылка ${key}`);
    if (id === ZERO) return;
    if (!target) fault('UNSUPPORTED_ONEC_REFERENCE',`Метаданные 1С не раскрывают тип ссылки ${key}`);
    const targetSchema = await client.schema(target);
    if (!isReferenceObject(targetSchema)) fault('UNSUPPORTED_ONEC_REFERENCE',`Тип ссылки ${key} недоступен для чтения по GUID`);
    const identity = `${target}:${id}`;
    if (!cache.has(identity)) cache.set(identity,(await client.get({collection:target,id})).object);
    const row = cache.get(identity);
    if (row.Ref_Key !== id || row.DeletionMark || row.IsFolder && !/^Parent(?:_Key)?$/.test(key)) fault('ONEC_REFERENCE_UNAVAILABLE',`Ссылка ${key} удалена, является группой или недоступна`);
  }
  for (const [key,value] of Object.entries(input)) {
    if (protectedFields.has(key) || key.includes('@') || !props.has(key) || !create && key === 'Организация_Key') fault('INVALID_ONEC_FIELD',`Поле ${collection}.${key} не разрешено`);
    const property = props.get(key), type = property.type;
    if (key.endsWith('_Type')) {
      const field = key.slice(0,-5);
      if (!Object.hasOwn(input,field) || typeof value !== 'string' || !value.startsWith('StandardODATA.')) fault('UNSUPPORTED_ONEC_REFERENCE',`Для ${key} передайте ссылку и её опубликованный тип вместе`);
      continue;
    }
    if (props.has(`${key}_Type`)) {
      const target = input[`${key}_Type`]?.replace(/^StandardODATA\./,'');
      if (!input[`${key}_Type`]?.startsWith('StandardODATA.')) fault('UNSUPPORTED_ONEC_REFERENCE',`Для ${key} требуется опубликованный ${key}_Type`);
      await reference(target,value,key); result[key] = value; result[`${key}_Type`] = input[`${key}_Type`]; continue;
    }
    if (type.startsWith('Collection(')) {
      if (!Array.isArray(value) || value.length > 100) fault('INVALID_ONEC_ROWS','В табличной части допускается до 100 строк');
      result[key] = [];
      for (const row of value) result[key].push(await validateObjectData(client,`${collection}_${key}`,row,{create,depth:depth+1,cache}));
      continue;
    }
    if (type === 'Edm.Guid') {
      const navigation = (schema.navigation || []).find(n => `${n.name}_Key` === key || n.name === key);
      await reference(navigation?.target || legacyTargets[key],value,key);
    } else if (type === 'Edm.Boolean') {
      if (typeof value !== 'boolean') fault('INVALID_ONEC_FIELD',`Поле ${key} должно быть логическим`);
    } else if (type === 'Edm.DateTime') {
      if (typeof value !== 'string') fault('INVALID_DATE',`Проверьте дату ${key}`);
      dateOnly(value.slice(0,10));
      if (!/^\d{4}-\d\d-\d\d(?:T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d)?$/.test(value)) fault('INVALID_DATE','Дата 1С: YYYY-MM-DD или YYYY-MM-DDTHH:mm:ss, Asia/Almaty');
      result[key] = value.length === 10 ? value+'T00:00:00' : value; continue;
    } else if (/^Edm\.(Double|Decimal|Single|Int64|Int32|Int16|Byte)$/.test(type)) {
      const numeric = typeof value === 'number' || typeof value === 'string' && /^-?\d+(?:\.\d+)?$/.test(value);
      if (!numeric || !Number.isFinite(Number(value)) || Math.abs(Number(value)) > 1e12 || /Int|Byte/.test(type) && !Number.isInteger(Number(value))) fault('INVALID_ONEC_FIELD',`Проверьте число ${key}`);
    } else if (type === 'Edm.String') {
      const limit = Number(property.maxLength) || 10000;
      if (typeof value !== 'string' || value.length > Math.min(limit,10000)) fault('INVALID_ONEC_FIELD',`Проверьте текст ${key}`);
    } else fault('UNSUPPORTED_ONEC_FIELD',`Тип поля ${key} не поддержан для записи`);
    result[key] = value;
  }
  return result;
}
async function validateCustomerInvoice(collection, object, client) {
  if (collection !== 'Document_СчетНаОплатуПокупателю') return;
  for (const key of ['Date','Организация_Key','Контрагент_Key','ДоговорКонтрагента_Key','ВалютаДокумента_Key','УчитыватьНДС','СуммаВключаетНДС','СуммаДокумента']) {
    if (object[key] === undefined || object[key] === '' || object[key] === ZERO) fault('MISSING_ONEC_FIELD',`В счёте покупателю отсутствует ${key}`);
  }
  const rows = [...(object.Товары||[]),...(object.Услуги||[]),...(object.ОС||[])];
  if (!rows.length) fault('MISSING_INVOICE_ROWS','Заполните товары или услуги счёта покупателю');
  let total = 0;
  const rates = new Map();
  for (const row of rows) {
    if (!(Number(row.Количество)>0) || !(Number(row.Цена)>=0) || cents(Number(row.Сумма)) !== Math.round(Number(row.Количество)*Number(row.Цена)*100)) fault('INVALID_LINE_TOTAL','Количество, цена и сумма строки счёта не сходятся');
    if (object.УчитыватьНДС && (!row.СтавкаНДС_Key || row.СтавкаНДС_Key === ZERO || row.СуммаНДС === undefined)) fault('MISSING_VAT','Для строки нужны ставка и сумма НДС');
    if (object.УчитыватьНДС) {
      if (!rates.has(row.СтавкаНДС_Key)) rates.set(row.СтавкаНДС_Key,(await client.get({collection:'Catalog_СтавкиНДС',id:row.СтавкаНДС_Key})).object);
      const rate = rates.get(row.СтавкаНДС_Key);
      const percent = Number(rate.Ставка);
      if (rate.DeletionMark || rate.Ставка == null || rate.Ставка === '' || !Number.isFinite(percent) || percent < 0 || percent > 100) fault('INVALID_VAT','Ставка НДС не подтверждена справочником 1С');
      const expected = object.СуммаВключаетНДС ? Number(row.Сумма)*percent/(100+percent) : Number(row.Сумма)*percent/100;
      if (Math.abs(cents(expected)-cents(Number(row.СуммаНДС))) > 1) fault('INVALID_VAT','Сумма НДС не соответствует выбранной ставке и сумме строки');
    } else if (Number(row.СуммаНДС || 0) !== 0) fault('INVALID_VAT','НДС отключён, но в строке указана сумма НДС');
    total += cents(Number(row.Сумма)) + (object.УчитыватьНДС && !object.СуммаВключаетНДС ? cents(Number(row.СуммаНДС || 0)) : 0);
  }
  if (total <= 0 || total !== cents(Number(object.СуммаДокумента))) fault('INVALID_INVOICE_TOTAL','Сумма строк не совпадает с итогом счёта покупателю');
}
module.exports = { isReferenceObject, validateObjectData, validateCustomerInvoice };
