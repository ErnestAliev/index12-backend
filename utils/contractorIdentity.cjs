'use strict';
// Typography in a legal name is not a different counterparty. Preserve words,
// legal form and other punctuation; never override a populated conflicting ID.
const contractorName = value => String(value || '').normalize('NFKC').toLocaleLowerCase('ru-RU').replace(/ё/g,'е').replace(/[«»„“”‟"'‘’‚‛`]/gu,'').trim().replace(/\s+/gu,' ');
const taxId = value => String(value || '').replace(/\s/gu,'');
module.exports={contractorName,taxId};
