// New withdrawals carry no inferred purpose. Keep legacy withdrawals readable.
function isSystemWithdrawal(op = {}) {
    return op.transferPurpose === 'withdrawal' ||
        (op.transferPurpose === 'personal' && op.transferReason === 'personal_use') ||
        (op.isWithdrawal === true && (op.type === 'transfer' || op.isTransfer === true));
}

async function normalizeTransferData(input, { previous = {}, findAccount }) {
    const effective = { ...previous, ...input };
    const purpose = effective.transferPurpose || 'internal';
    const reason = Object.hasOwn(input, 'transferPurpose') && purpose === 'withdrawal' && !Object.hasOwn(input, 'transferReason')
        ? null : effective.transferReason || null;
    const reject = message => { const error = new Error(message); error.statusCode = 400; throw error; };
    if (!['internal', 'inter_company', 'personal', 'withdrawal'].includes(purpose)) reject('Неизвестный вид перевода.');
    if (purpose === 'personal' ? !['business_growth', 'personal_use'].includes(reason) : reason !== null) reject('Проверьте вид перевода и причину.');
    const withdrawal = isSystemWithdrawal({ transferPurpose: purpose, transferReason: reason });
    if (!effective.fromAccountId) reject('Выберите счёт списания.');
    if (!withdrawal && (!effective.toAccountId || String(effective.fromAccountId) === String(effective.toAccountId))) reject('Выберите другой счёт получателя.');
    if (purpose === 'withdrawal' && ['toAccountId', 'toCompanyId', 'toIndividualId'].some(key => input[key])) reject('Для вывода счёт получателя не нужен.');
    const amount = Math.abs(Number(effective.amount));
    if (!Number.isFinite(amount) || amount <= 0) reject('Введите корректную сумму.');
    const result = { ...input, type: 'transfer', isTransfer: true, isWithdrawal: withdrawal, amount,
        transferPurpose: purpose, transferReason: reason };
    for (const side of ['from', 'to']) {
        const accountId = purpose === 'withdrawal' && side === 'to' ? null : effective[`${side}AccountId`] || null;
        const account = accountId ? await findAccount(accountId) : null;
        if (accountId && !account) reject('Счёт не найден в текущей рабочей области.');
        result[`${side}AccountId`] = accountId;
        result[`${side}CompanyId`] = account?.companyId || null;
        result[`${side}IndividualId`] = account?.individualId || null;
    }
    Object.assign(result, {
        accountId: withdrawal ? result.fromAccountId : null,
        companyId: withdrawal ? result.fromCompanyId : null,
        individualId: withdrawal ? result.fromIndividualId : null,
        destination: withdrawal ? 'Вне системы' : null,
        description: input.description ?? previous.description ?? (withdrawal ? 'Вывод средств' : 'Перевод между счетами'),
    });
    return result;
}

module.exports = { isSystemWithdrawal, normalizeTransferData };
