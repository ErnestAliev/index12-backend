'use strict';
// Only fields present in the live OData schema may be read or written.
const scalarFields = [
  ['legalName','НаименованиеПолное','Полное наименование'],
  ['identificationNumber','ИдентификационныйНомер','БИН/ИИН'],
  ['kbe','КБЕ','КБе'], ['registrationDate','ДатаРегистрации','Дата регистрации'],
  ['legalAddress','ЮридическийАдрес','Юридический адрес'], ['postalAddress','ПочтовыйАдрес','Почтовый адрес'],
  ['director','Руководитель','Руководитель'], ['chiefAccountant','ГлавныйБухгалтер','Главный бухгалтер'],
  ['phone','Телефон','Телефон'], ['email','ЭлектроннаяПочта','Электронная почта'], ['website','ВебСайт','Сайт'],
  ['registrationStatus','СтатусРегистрации','Статус регистрации'],
  ['vatRegistrationDate','ДатаПостановкиНаУчетПоНДС','Дата учёта по НДС'],
  ['vatPayer','ПлательщикНДС','Плательщик НДС'], ['vatPercent','СтавкаНДС','Ставка НДС'],
];
const empty = value => value == null || value === '' || typeof value === 'string' && (!value.trim() || /^0001-01-01/.test(value) || value === '00000000-0000-0000-0000-000000000000');
const detectLegalForm = name => {
  const value=String(name||'').trim();
  for(const [prefix,form] of [['ТОО','too'],['ИП','ip'],['АО','ao'],['ТДО','tdo'],['ООО','ooo']])if(new RegExp(`^${prefix}(?:\\s|$)`,'iu').test(value))return form;
};
async function enrichProfile({row,schema,fields,getCatalog,warnings,invoke,collections}) {
  const properties=new Map(schema.properties.map(p=>[p.name,p]));
  const form=detectLegalForm(row.Description)||detectLegalForm(row.НаименованиеПолное);
  if(form)fields.legalForm=form;
  for(const [key,source] of scalarFields) {
    const property=properties.get(source),value=row[source];
    if(!property||empty(value))continue;
    if(property.type==='Edm.String'&&typeof value==='string'&&key!=='vatPercent'&&key!=='vatPayer')fields[key]=value.trim();
    if(property.type==='Edm.Boolean'&&key==='vatPayer'&&typeof value==='boolean')fields[key]=value;
    if(/^Edm\.(Decimal|Double|Int32)$/.test(property.type)&&key==='vatPercent'&&Number.isFinite(Number(value)))fields[key]=Number(value);
  }
  for(const [key,source] of [['director','Руководитель'],['chiefAccountant','ГлавныйБухгалтер']]) {
    const navigation=schema.navigation?.find(n=>n.name===source);
    if(navigation?.target&&row[`${source}_Key`]) {
      const person=await getCatalog(navigation.target,row[`${source}_Key`]);
      if(person&&!person.DeletionMark)fields[key]=person.ФИО||person.НаименованиеПолное||person.Description||fields[key];
    }
    if(!properties.has(source)&&!navigation)warnings.push(`1С не публикует поле «${key==='director'?'Руководитель':'Главный бухгалтер'}»; заполните его в INDEX12`);
  }
  if(!properties.has('ЮридическийАдрес'))warnings.push('Юридический адрес не опубликован в карточке 1С; заполните его в INDEX12');
  const contacts='Catalog_КонтактныеЛица';
  try { if(collections.some(c=>c.name===contacts)) {
    const {schema:contactSchema}=await invoke('onec_schema',{collection:contacts});
    const names=new Set(contactSchema.properties.map(p=>p.name));
    if(['ОбъектВладелец','ОбъектВладелец_Type','Ref_Key'].every(key=>names.has(key))) {
      const found={director:new Set(),chiefAccountant:new Set()};
      for(let skip=0;;) {
        const page=await invoke('onec_query',{collection:contacts,filter:`cast(ОбъектВладелец,'${schema.name}') eq guid'${row.Ref_Key}'${names.has('DeletionMark')?' and DeletionMark eq false':''}`,top:100,skip,orderBy:'Ref_Key asc'});
        if(!Array.isArray(page.rows)||page.hasMore&&!page.rows.length)throw new Error('Неполный список контактных лиц 1С');
        for(const person of page.rows) {
          if(person.ОбъектВладелец!==row.Ref_Key||person.ОбъектВладелец_Type!==`StandardODATA.${schema.name}`)throw new Error('Контактное лицо принадлежит другой организации');
          if(person.DeletionMark)continue;
          const role=person.Роль_Key?await getCatalog('Catalog_РолиКонтактныхЛиц',person.Роль_Key):null;
          const position=String(person.Должность||role?.Description||'').toLocaleLowerCase('ru').replace(/[^а-яё]/gu,'');
          const key=['директор','генеральныйдиректор','руководитель','первыйруководитель'].includes(position)?'director':position==='главныйбухгалтер'?'chiefAccountant':null;
          const name=[person.Фамилия,person.Имя,person.Отчество].filter(Boolean).join(' ')||person.Description;
          if(key&&name)found[key].add(name);
        }
        if(!page.hasMore)break;
        skip+=page.rows.length;if(skip>10000)throw new Error('Слишком много контактных лиц 1С');
      }
      for(const key of ['director','chiefAccountant'])if(!fields[key]&&found[key].size===1) {
        fields[key]=[...found[key]][0];
        const label=key==='director'?'Руководитель':'Главный бухгалтер';
        const index=warnings.indexOf(`1С не публикует поле «${label}»; заполните его в INDEX12`);if(index>=0)warnings.splice(index,1);
      }else if(found[key].size>1)warnings.push(`В контактных лицах 1С несколько записей для ${key==='director'?'руководителя':'главного бухгалтера'}; ФИО не выбрано автоматически`);
    }
  }
  } catch(error) {
    if(!['HTTP_400','HTTP_403','HTTP_404','NOT_PUBLISHED'].includes(error.code))throw error;
    warnings.push('Контактные лица 1С недоступны через публикацию; ФИО заполните в INDEX12');
  }
  // A certificate is not a reliable current VAT-payer flag. Preserve unknown.
  if(!properties.has('ПлательщикНДС'))warnings.push('Признак плательщика НДС не опубликован в 1С; настройка берётся из карточки INDEX12');
}
module.exports={scalarFields,empty,enrichProfile,detectLegalForm};
