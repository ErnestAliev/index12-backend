const { isSystemWithdrawal } = require('./transferSemantics');
// Keep the account and its recorded owner together wherever a transfer is shown.
const entityId = value => {
  if (!value) return '';
  if (typeof value.toHexString === 'function') return value.toHexString();
  return typeof value === 'object' ? String(value._id || value.id || '') : String(value);
};
const lookup = (list, value) => {
  const id = entityId(value);
  if (!id) return null;
  return list instanceof Map ? list.get(id) : (list || []).find(item => entityId(item) === id);
};
const entityName = (value, list) => lookup(list, value)?.name || (typeof value === 'object' ? value?.name : '') || '';

function transferLabels(operation = {}, directories = {}) {
  const op = operation || {};
  const { accounts = [], companies = [], individuals = [] } = directories;
  const outside = op.isOutOfSystemTransfer === true || isSystemWithdrawal(op);
  const side = prefix => {
    const isFrom = prefix === 'from';
    const accountRef = op[`${prefix}AccountId`] || op[`${prefix}Account`] || (isFrom ? op.accountId : null);
    const account = lookup(accounts, accountRef) || (typeof accountRef === 'object' ? accountRef : {});
    const accountName = entityName(accountRef, accounts) || op[`${prefix}AccountName`] || op[`${prefix}AccName`] || (isFrom ? op.accountName : '') || '';
    const companyRef = op[`${prefix}CompanyId`] || op[`${prefix}Company`] || (isFrom ? op.companyId : null);
    const individualRef = op[`${prefix}IndividualId`] || op[`${prefix}Individual`] || (isFrom ? op.individualId : null);
    const hasExplicitOwner = !!(entityId(companyRef) || entityId(individualRef));
    const ownerName = entityName(companyRef, companies) || entityName(individualRef, individuals)
      || op[`${prefix}OwnerName`] || op[`${prefix}CompanyName`] || op[`${prefix}IndividualName`]
      || (!hasExplicitOwner ? entityName(account?.companyId, companies) || entityName(account?.individualId, individuals) : '') || '';
    const isOutside = !isFrom && outside;
    const name = accountName || (isOutside ? op.destination || 'Вне системы' : 'Счет не указан');
    const label = `${name}${ownerName ? ` (${ownerName})` : isOutside ? '' : ' (владелец не указан)'}${isOutside && name !== 'Вне системы' ? ' · вне системы' : ''}`;
    return { accountName: name, ownerName, label };
  };
  const from = side('from'), to = side('to');
  return { fromAccName: from.accountName, toAccName: to.accountName,
    fromOwnerName: from.ownerName, toOwnerName: to.ownerName,
    fromLabel: from.label, toLabel: to.label, routeLabel: `${from.label} → ${to.label}` };
}

module.exports = { transferLabels };
