'use strict';
const { fault, ZERO, cents } = require('./purchase-domain.cjs');
const collection = 'Catalog_СтавкиНДС';
const applicable = rate => rate && !rate.DeletionMark && rate.Ref_Key !== ZERO &&
  !rate.ДляОсвобожденногоОборота && !rate.МестоРеализацииНеРК &&
  rate.Ставка !== undefined && rate.Ставка !== null && rate.Ставка !== '' &&
  Number.isFinite(Number(rate.Ставка)) && Number(rate.Ставка) >= 0 && Number(rate.Ставка) <= 100;
function vatCents(amount, percent, included) {
  const units=BigInt(Math.round(percent*1e6)),denominator=100000000n+(included?units:0n);
  return Number((BigInt(cents(amount))*units+denominator/2n)/denominator);
}
async function normalizeInvoiceVat(input, client) {
  const result = { ...input, lines: input.lines.map(line => ({ ...line })) };
  if (!input.useVat) {
    if (Number(input.vatAmount || 0) !== 0 || Number(input.vatPercent || 0) !== 0 || result.lines.some(line => Number(line.vatAmount || 0) !== 0 || Number(line.vatPercent || 0) !== 0))
      fault('VAT_MODE_CONFLICT','Указаны сумма или ставка НДС, но учёт НДС выключен. Сверьте исходный счёт');
    result.vatIncluded = input.vatIncluded ?? false;
    for (const line of result.lines) line.vatAmount = 0;
    return result;
  }
  // An invoice-level tax amount belongs to the only line. Never distribute a
  // total across multiple lines: they may carry different rates or exemptions.
  if (result.lines.length === 1 && input.vatAmount !== undefined) {
    const line = result.lines[0];
    if (line.vatAmount !== undefined && cents(line.vatAmount) !== cents(input.vatAmount))
      fault('VAT_TOTAL_MISMATCH','НДС строки отличается от НДС исходного счёта');
    line.vatAmount = input.vatAmount;
  }
  const lineTotal = result.lines.reduce((sum,line) => sum + cents(line.amount),0);
  // Same row/header totals mean the tax is already inside the row amounts.
  // Otherwise only an exclusive calculation that matches the total is allowed.
  result.vatIncluded = input.vatIncluded ?? lineTotal === cents(input.amount);
  let ratesPromise;
  async function rates() {
    return ratesPromise ||= (async () => {
      const schema = await client.schema(collection), fields = new Set(schema.properties.map(p => p.name));
      if (!fields.has('Ref_Key') || !fields.has('Ставка')) fault('VAT_SCHEMA_UNAVAILABLE','В схеме 1С не опубликованы ссылка и процент ставки НДС');
      const select = ['Ref_Key','Description','Ставка','DeletionMark','ДляОсвобожденногоОборота','МестоРеализацииНеРК'].filter(name => fields.has(name)).join(',');
      const found=[];
      for (let skip=0;skip<=10000;skip+=200) {
        const page=await client.query({collection,select,orderBy:'Ref_Key',top:200,skip});
        found.push(...page.rows.filter(applicable));
        if (!page.hasMore) return found;
      }
      fault('VAT_RATE_SCAN_INCOMPLETE','Не завершена проверка справочника ставок НДС; записи не выполнялись');
    })();
  }
  const byId = new Map();
  for (const [index,line] of result.lines.entries()) {
    const percent = line.vatPercent ?? input.vatPercent;
    let rate;
    if (line.vatRateRef && line.vatRateRef !== ZERO) {
      if (!byId.has(line.vatRateRef)) byId.set(line.vatRateRef,(await client.get({collection,id:line.vatRateRef})).object);
      rate = byId.get(line.vatRateRef);
      if (!applicable(rate)) fault('INVALID_VAT_RATE',`Строка ${index+1}: ссылка не соответствует действующей ставке НДС`);
      if (percent !== undefined && percent !== Number(rate.Ставка)) fault('VAT_RATE_CONFLICT',`Строка ${index+1}: процент НДС отличается от выбранной ставки 1С`);
    } else {
      if (percent === undefined && line.vatAmount === undefined)
        fault('MISSING_VAT_SOURCE',`Строка ${index+1}: перенесите из счёта процент или сумму НДС. Техническую ссылку 1С сервер найдёт сам; ставку из карточки покупателя не подставляет`);
      const candidates = (await rates()).filter(row => percent !== undefined
        ? Number(row.Ставка) === percent
        : Math.abs(vatCents(line.amount,Number(row.Ставка),result.vatIncluded)-cents(line.vatAmount)) <= 1);
      if (candidates.length !== 1) fault(candidates.length?'VAT_RATE_AMBIGUOUS':'VAT_RATE_NOT_FOUND',
        `Строка ${index+1}: ${candidates.length?'несколько ставок 1С соответствуют НДС счёта':'в справочнике 1С не найдена ставка, соответствующая НДС счёта'}`,409,
        {line:index+1,percent:percent??null,references:candidates.map(row=>row.Ref_Key)});
      rate=candidates[0];
    }
    const expected = vatCents(line.amount,Number(rate.Ставка),result.vatIncluded);
    if (line.vatAmount !== undefined && Math.abs(cents(line.vatAmount)-expected)>1)
      fault('INVALID_VAT',`Строка ${index+1}: НДС из счёта не соответствует сумме и ставке. Исходные цифры не изменялись`);
    line.vatRateRef=rate.Ref_Key;line.vatPercent=Number(rate.Ставка);
    line.vatAmount ??= expected/100;
  }
  const taxTotal = result.lines.reduce((sum,line)=>sum+cents(line.vatAmount),0);
  if (input.vatAmount !== undefined && taxTotal !== cents(input.vatAmount)) fault('VAT_TOTAL_MISMATCH','Сумма НДС строк не совпадает с НДС исходного счёта');
  const total = lineTotal + (result.vatIncluded?0:taxTotal);
  if (total !== cents(input.amount)) fault('INVALID_INVOICE_TOTAL','Суммы строк и НДС не совпадают с итогом счёта. Исходные цифры не изменялись');
  result.vatAmount=taxTotal/100;
  return result;
}
module.exports={normalizeInvoiceVat,vatCents};
