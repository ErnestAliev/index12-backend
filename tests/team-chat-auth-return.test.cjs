'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {chatAuthReturn}=require('../team-chat/auth-return.cjs');
test('owner OAuth preserves exact chat/task/message on the originating allowed frontend',()=>{
 const origins=['https://index12.com','https://www.index12.com'],query='?teamChat='+'a'.repeat(24)+'&task=11111111-1111-4111-8111-111111111111&message=22222222-2222-4222-8222-222222222222';
 for(const origin of origins)for(const path of ['/','/mobile'])assert.equal(chatAuthReturn(origin+path+query,origins),origin+path+query);
 for(const value of ['https://evil.test/'+query,'https://index12.com.evil.test/'+query,'https://user:pass@index12.com/'+query,'//index12.com/'+query,'https://index12.com/other'+query,'https://index12.com/'+query+'#hash','https://index12.com/'+query+'&redirect=https://evil.test','https://index12.com/?teamChat=bad','https://index12.com/?teamChat='+'a'.repeat(24)+'&task=bad'])assert.equal(chatAuthReturn(value,origins),null,value);
});
