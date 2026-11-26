'use strict';
const {cents}=require('./purchase-domain.cjs');
const ones=['','один','два','три','четыре','пять','шесть','семь','восемь','девять'];
const teens=['десять','одиннадцать','двенадцать','тринадцать','четырнадцать','пятнадцать','шестнадцать','семнадцать','восемнадцать','девятнадцать'];
const tens=['','','двадцать','тридцать','сорок','пятьдесят','шестьдесят','семьдесят','восемьдесят','девяносто'];
const hundreds=['','сто','двести','триста','четыреста','пятьсот','шестьсот','семьсот','восемьсот','девятьсот'];
const groups=[null,['тысяча','тысячи','тысяч'],['миллион','миллиона','миллионов'],['миллиард','миллиарда','миллиардов'],['триллион','триллиона','триллионов']];
const form=(n,forms)=>forms[n%100>=11&&n%100<=14?2:n%10===1?0:n%10>=2&&n%10<=4?1:2];
function triplet(n,feminine=false){
  const words=[hundreds[Math.floor(n/100)]],last=n%100;
  if(last>=10&&last<20)words.push(teens[last-10]);
  else words.push(tens[Math.floor(last/10)],feminine&&n%10===1?'одна':feminine&&n%10===2?'две':ones[n%10]);
  return words.filter(Boolean).join(' ');
}
function amountInWords(amount){
  const value=cents(amount),fraction=String(value%100).padStart(2,'0');
  let whole=Math.floor(value/100),group=0,words=[];
  if(whole===0)words.push('ноль');
  while(whole>0){
    const part=whole%1000;
    if(part)words.unshift(`${triplet(part,group===1)}${group?' '+form(part,groups[group]):''}`);
    whole=Math.floor(whole/1000);group++;
  }
  const result=`${words.join(' ')} теңге ${fraction} тиын`;
  return result[0].toLocaleUpperCase('ru')+result.slice(1);
}
module.exports={amountInWords};
