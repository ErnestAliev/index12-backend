// The application stores a signed Number in Event.amount. Parse at the boundary
// with integer tiyn, and reject values that cannot round-trip in this model.
function parseMoney(value) {
    if (typeof value !== 'string' || !/^\d{1,14}(\.\d{1,2})?$/.test(value)) throw new Error('Укажите положительную сумму строкой, например "1250.50", максимум два знака после точки.');
    const [whole, fraction = ''] = value.split('.');
    const minor = BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0'));
    if (minor <= 0n || minor > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('Сумма должна быть положительной и точно представимой в тиынах.');
    const number = Number(minor) / 100;
    if (number.toFixed(2) !== `${BigInt(whole)}.${fraction.padEnd(2, '0')}` || BigInt(Math.round(number * 100)) !== minor) throw new Error('Сумма превышает точность денежной модели INDEX12.');
    return { minor: String(minor), decimal: number.toFixed(2), number };
}
module.exports = { parseMoney };
