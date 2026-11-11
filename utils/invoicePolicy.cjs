'use strict';
const singleWord = value => typeof value === 'string' && /^[\p{L}\p{N}]+(?:[-‐‑][\p{L}\p{N}]+)*$/u.test(value.trim().normalize('NFKC'));
const idOf = value => value == null ? null : String(value._id || value);
function fail(code, message) { throw Object.assign(new Error(message), { detail: { code, message } }); }
const accountFlag = data => {
  if (Object.hasOwn(data, 'isDefaultForExpenses') && typeof data.isDefaultForExpenses !== 'boolean') throw Error('Признак счёта по умолчанию должен быть логическим значением');
  return { ...data, ...(data.isExcluded === true || data.isCashRegister === true ? { isDefaultForExpenses: false } : {}) };
};
const normalize = value => String(value || '').normalize('NFKC').toLowerCase().replace(/[^a-zа-яё0-9]/gu, '');
const ROUTING = `Сначала определи цель поручения по актуальному тексту задачи, сообщению владельца и ролям сторон. Наличие вложенного счёта само по себе не означает расход. Направления разные: оплатить/оформить полученный счёт поставщика (покупатель — наша компания) или выставить новый счёт клиенту (продавец — наша компания). «Проверить/сверить/обсудить» означает чтение без записей. При выставлении счёта вложение поставщика может быть только источником цен для перевыставления; не превращай его в поручение на оплату. Если направление не установлено или сведения противоречат друг другу, задай один вопрос о направлении, до любых финансовых записей. Счёт между двумя собственными компаниями требует отдельного согласованного межфирменного процесса.
Для счёта покупателю используй опубликованный Document_СчетНаОплатуПокупателю и generic onec_create_object: наша компания — продавец, контрагент — покупатель; проверь дубли, реальные товары/услуги, цены, валюту, налоги и реквизиты. Найди или создай покупателя по БИН/ИИН без дублей, не придумывай договор. Выставление счёта не создаёт расход, исходящее поручение и подтверждённый доход INDEX12. Плановый доход — только по отдельному явному поручению и с установленной датой. Задачи запроса закрывающих у поставщика относятся только к покупке. Верни кратко номер, сумму и кому выставлен счёт.
`;
const INSTRUCTIONS = ROUTING + `Оформление полученного счёта из рабочей задачи: поручение «Оформи счета из задач» запускает всю цепочку, не уточняй, что нужно сделать. Прочитай оригинальную задачу, всю нужную переписку и оригиналы вложений. Если счёта нет — кратко сообщи, финансовые записи не создавай. Проект и назначение бери из задачи; сумму, БИН/ИИН, номер, дату и банковские реквизиты — из счёта. Перед расходом вызови index12_invoice_defaults: явный выбор пользователя важнее, иначе компания-покупатель → отмеченные счета по умолчанию → порядок редактора. Никогда не выбирай по остатку. При отсутствии отметок допустим только однозначный BCCUU подходящей компании. Дата расхода — явно заданная пользователем, иначе срок оплаты из счёта, иначе сегодня Asia/Almaty; дата выставления сама по себе не является сроком оплаты.
В INDEX12 используй prepare_import с namingPolicy=single_word и execute_import: существующего контрагента ищи по БИН/ИИН и имени, при отсутствии создай с реальными реквизитами. Категория и подкатегория обязательны, каждая ровно одно слово: сначала подходящие существующие, иначе создай одну категорию и одну подкатегорию. Например Обслуживание / Дератизация. Не создавай перечень категорий и не переименовывай старые. Описание расхода короткое, с назначением и номером счёта.
Сохрани исходный PDF через upload_purchase_pdf(chatFileId=ID вложения задачи): сервер вернёт реальный sha256 для documentFingerprint. Ключи импорта/покупки/оркестрации привязывай к этому документу и задаче, сохраняй их при повторах; сначала проверь прежний результат, не создавай дубль новым ключом. До финансовых записей проверь 1С, связь своей компании и возможность разрешить все обязательные реквизиты. В 1С через status/schema/query найди контрагента по БИН/ИИН на всех страницах, проверь дубли; отсутствующего создай generic create_object только с подтверждёнными реквизитами. Используй реальные банковские счета, договор, номенклатуру, валюту и ставки из опубликованной базы; не придумывай их и не копируй настройки посторонней компании. Не создавай фиктивный юридический договор, если его нет в источниках.
Для 1С используй prepare_purchase → execute_purchase: непроведённый счёт поставщика с настоящим номером/датой → связанное исходящее поручение → одна обязательная задача о закрывающих с PDF. operationId в плане покупки — ID созданного расхода INDEX12, не UUID изменения 1С. Свяжи исходную рабочую задачу через sourceTaskId. Используй эту задачу о закрывающих, вторую не создавай; исходную рабочую задачу автоматически не закрывай. Платёж в банк не отправляй и документы автоматически не проводи. В direct выполни ясно порученную цепочку без повторных согласований. Вопрос допустим только при отсутствии критичных реквизитов, неоднозначности, конфликте или недоступности; не спрашивай заново назначение сценария. Любая ошибка/partial/uncertain останавливает следующие записи: назови подтверждённое и сверяй прежние ключи. Итог короткий: расход, счёт списания, документы 1С и задача о закрывающих; не называй подготовленное поручение оплатой.`;
function invoiceDefaults(snapshot, { companyId, buyerIdentificationNumber, accountId } = {}) {
  if (snapshot.metadata.role !== 'admin') fail('FORBIDDEN', 'Оформление счетов доступно администратору рабочей области.');
  const companies = [...snapshot.entities.companies.values()];
  let company = companyId ? snapshot.entities.companies.get(companyId) : null;
  if (companyId && !company) fail('COMPANY_NOT_FOUND', 'Компания недоступна в текущей рабочей области.');
  if (buyerIdentificationNumber) {
    const matches = companies.filter(row => row.identificationNumber === buyerIdentificationNumber);
    if (matches.length > 1) fail('COMPANY_AMBIGUOUS', 'Несколько компаний имеют БИН/ИИН покупателя. Нужна сверка карточек.');
    if (!matches.length || company && company._id !== matches[0]._id) fail('BUYER_MISMATCH', 'Компания-покупатель счёта не соответствует выбранной компании INDEX12.');
    company = matches[0];
  }
  const accounts = [...snapshot.entities.accounts.values()];
  const eligible = accounts.filter(row => !row.isExcluded && !row.isCashRegister);
  let selected, basis;
  if (accountId) {
    selected = accounts.find(row => row._id === accountId);
    if (!selected) fail('ACCOUNT_NOT_FOUND', 'Счёт недоступен в текущей рабочей области.');
    if (company && idOf(selected.companyId) !== company._id) fail('BUYER_MISMATCH', 'Счёт списания принадлежит другой компании.');
    basis = 'explicit';
  } else {
    if (!company) fail('BUYER_REQUIRED', 'Сначала определите компанию-покупателя из задачи или счёта; порядок счетов не определяет покупателя.');
    const defaults = eligible.filter(row => row.isDefaultForExpenses);
    const candidates = (company ? defaults.filter(row => idOf(row.companyId) === company._id) : defaults)
      .sort((a, b) => (a.order || 0) - (b.order || 0) || a._id.localeCompare(b._id));
    selected = candidates[0]; basis = 'default_account_order';
    if (!selected && !defaults.length) {
      const legacy = eligible.filter(row => normalize(row.name) === 'bccuu' && (!company || idOf(row.companyId) === company._id));
      if (legacy.length === 1) { selected = legacy[0]; basis = 'bccuu_fallback'; }
    }
    if (!selected) fail('DEFAULT_ACCOUNT_NOT_CONFIGURED', 'Отметьте счёт компании-покупателя в колонке «По умолчанию» редактора счетов.');
  }
  if (!company && selected.companyId) company = snapshot.entities.companies.get(idOf(selected.companyId));
  if (!company) fail('ACCOUNT_COMPANY_REQUIRED', 'Для оформления в 1С счёт списания должен принадлежать связанной компании INDEX12.');
  return { ...snapshot.metadata, account: { id: selected._id, name: selected.name, companyId: company._id, isDefaultForExpenses: !!selected.isDefaultForExpenses },
    company: { id: company._id, name: company.name, identificationNumber: company.identificationNumber || null }, basis,
    namingPolicy: 'single_word', instructions: INSTRUCTIONS, ROUTING };
}
module.exports = { singleWord, accountFlag, invoiceDefaults, INSTRUCTIONS, ROUTING };
