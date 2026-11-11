'use strict';
const { MEDIA_INSTRUCTIONS, INVOICE_EXTRACTION_INSTRUCTIONS } = require('./taskMediaPolicy.cjs');
const singleWord = value => typeof value === 'string' && /^[\p{L}\p{N}]+(?:[-‐‑][\p{L}\p{N}]+)*$/u.test(value.trim().normalize('NFKC'));
const idOf = value => value == null ? null : String(value._id || value);
function fail(code, message) { throw Object.assign(new Error(message), { detail: { code, message } }); }
const accountFlag = data => {
  if (Object.hasOwn(data, 'isDefaultForExpenses') && typeof data.isDefaultForExpenses !== 'boolean') throw Error('Признак счёта по умолчанию должен быть логическим значением');
  return { ...data, ...(data.isExcluded === true || data.isCashRegister === true ? { isDefaultForExpenses: false } : {}) };
};
const normalize = value => String(value || '').normalize('NFKC').toLowerCase().replace(/[^a-zа-яё0-9]/gu, '');
const ROUTING = `Быстрые команды: «Оплатить счёт:» — покупка у поставщика; «Выставить счёт:» — счёт нашему покупателю. Без команды или отдельного финансового поручения владельца задача обычная: прочитать содержание и нужную переписку, разобраться и ответить. PDF сам по себе финансовую цепочку не запускает. «Проверь/сверь» ограничивает работу чтением даже при финансовой команде в задаче. Если направление действительно неоднозначно, один вопрос до записей. Входящий счёт не превращай в исходящий; счета между собственными компаниями требуют отдельного межфирменного сценария.`;
const INSTRUCTIONS = ROUTING + '\n' + MEDIA_INSTRUCTIONS + '\n' + INVOICE_EXTRACTION_INSTRUCTIONS + '\n' + `Два штатных сценария выполняй через index12_process_invoice одним вызовом после разбора исходных данных. Не заменяй этот инструмент множеством generic create/update по отдельным полям и повторными чтениями всех справочников: серверный шаблон сам проверяет актуальные ссылки, дубли и суммы. Code Mode может объединить независимые чтения задачи, документов и карточек; реальные записи последовательны. Возвращай краткий результат, без дампа строк и схем.
Входящий счёт (direction=supplier): прочитай задачу, нужную переписку и документы; проект и назначение из задачи, сумма/номер/дата/БИН/ИИН/банк/НДС из счёта. Передай все строки, companyId покупателя, sourceTaskId и chatFileId оригинала (либо pdfId/desktopFileId), category/subcategory — каждая ровно одно слово, сначала подходящие существующие, иначе одна новая категория и одна подкатегория. Например Обслуживание / Дератизация. Не создавай перечни категорий и не переименовывай старые. Счёт списания: явный выбор, иначе компания → отмеченные счета по умолчанию → порядок редактора; без отметок только однозначный BCCUU нужной компании. Никогда по остатку. Дата расхода: явная, иначе срок оплаты, иначе сегодня Asia/Almaty; дата выставления не является сроком оплаты. Для нескольких банковских счетов своей компании передай проверенный payerIban, иначе используется основной банковский счёт организации 1С.
Сервер до финансовых записей разрешает обязательные реквизиты 1С, затем создаёт/сопоставляет контрагента INDEX12 и расход → контрагента 1С без дублей и предоставленные банковские реквизиты → номенклатуру услуг → счёт поставщика → связанное исходящее платёжное поручение → проведение → синхронизацию официального контрагента → одну задачу закрывающих. Существующую подходящую номенклатуру переиспользует, отсутствующую создаёт с признаком услуги, единицей и подходящей группой; при неопределённой группе общий список. Для ИП: legalForm=ip, ФизЛицо и признак ИП=true; ИИН строкой 12 цифр. Для ТОО: legalForm=llp, ЮрЛицо и признак ИП=false. Тип по длине номера не определяй, ФИО/режим налогообложения не выдумывай. Автозаполнение ТОО через доступный реестр необязательно и не тормозит ручной путь.
Договор не указан или пустой — без вопросов создать/переиспользовать техническую запись «Без договора» именно для контрагента, своей организации и валюты, выбрать её в документах. Не придумывай номер, дату и юридический договор. Указанный реальный договор сохраняй после проверки. КНП отсутствует — 859, явный код сохраняй. Это настройка сценария, не утверждение о законодательных порогах.
НДС входящего счёта только из документа поставщика: useVat, vatPercent и/или vatAmount, vatIncluded если известно. Сервер сопоставит GUID ставки и вычислит недостающие суммы, проверив итог. Для одной строки достаточно общей суммы «в том числе НДС». При «без НДС» не подставляй 16% из нашей карточки и не наследуй ставку из номенклатуры. Общий taxPercent не является НДС. Нечитаемые критичные поля не угадывай: один копируемый черновик с прочитанными значениями и пустыми недостающими полями.
Правило владельца для supplier_payment: платёжное поручение датой фактического создания в Asia/Almaty, а не датой счёта. Сразу Оплачено=true, затем провести в 1С. Повтор сохраняет первоначальный документ и дату. Это явно разрешённая отметка в учёте до ручной отправки в банке. БАНК ОТКЛЮЧЁН: не вызывай банковский коннектор даже для статуса; выгрузку/импорт платёжки и отправку денег выполняет пользователь. bankPaymentSent=false и bankSettlementConfirmed=false. Не объявляй деньги отправленными. Задача закрывающих сразу напоминает запросить АВР, затем получить и подтвердить АВР, при необходимости ЭСФ. Запрос поставщику сам не отправляется; этапы запроса/получения фиксируются по фактам через index12_invoice_followup. Исходную рабочую задачу не закрывай.
Исходящий счёт (direction=customer): покупатель/БИН/ИИН, проект, услуга/товар и сумма из поручения, наша компания — продавец. PDF источника не требуется. Остальные доступные реквизиты возьми из карточек; исходящий НДС из настройки продавца и поручения, не из ставки расхода/поставщика. Передай sourceTaskId: сервер создаёт только счёт покупателю с номенклатурой, синхронизирует карточку и возвращает PDF в обсуждение исходной задачи. PDF формирует штатная печать 1С, printSource=ONEC_NATIVE. До создания счёта сервер проверяет возможность печати; при недоступности сообщает конкретное ограничение без финансовых записей. Готовый PDF кэшируется по версии документа и макета; собственную форму не подставляй вместо штатной. Доход/расход, платёжку, закрывающие поставщика и банковскую оплату выставление счёта не создаёт.
При ясном поручении и прямом режиме выполни без повторных согласований. Ключ idempotencyKey привяжи к документу/задаче; при повторе тот же ключ и параметры. При partial/uncertain останови следующие записи, перечисли только подтверждённое и сверь прежний planToken/operationId; не начинай заново новым ключом. Для ручного пошагового восстановления есть onec_prepare_invoice(workflow=supplier_payment|customer_invoice), execute_invoice, invoice_status, mutation_status и invoice_link_expense. invoice_only — прежний режим только счёта без платёжки. Итог короткий: расход/счёт/платёжка проведена/задача либо счёт покупателю и файл отправлен; никаких лишних согласований назначения сценария.`;
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
