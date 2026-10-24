const {test}=require('node:test');
const assert=require('node:assert/strict');
const {makeImportSchema,normalizeName,canonical,signPlan,unpackPlan,approvalProof}=require('../utils/mcpImports');
const {createSchema}=require('../utils/mcpOperations');

test('Import plans are canonical, signed, immutable and credential-bound',()=>{
  const a={workspaceId:'one',input:{amount:'0.10',name:'Свет'}};
  assert.equal(canonical(a),canonical({input:{name:'Свет',amount:'0.10'},workspaceId:'one'}));
  const token=signPlan(a,'secret');assert.deepEqual(unpackPlan(token,'secret'),a);
  assert.throws(()=>unpackPlan(token,'other'));assert.throws(()=>unpackPlan(token+'x','secret'));
  const changed=Buffer.from(JSON.stringify({...a,workspaceId:'two'})).toString('base64url')+'.'+token.split('.')[1];
  assert.throws(()=>unpackPlan(changed,'secret'));
  assert.notEqual(approvalProof('credential',token),approvalProof('other',token));
  assert.notEqual(approvalProof('credential',token),approvalProof('credential',changed));
});
test('Import names tolerate case/spacing/Unicode but do not conflate distinct records',()=>{
  assert.equal(normalizeName('  СЧЁТ\u00a0  на свет  '),normalizeName('счет на свет'));
  assert.equal(normalizeName('ＡＢＣ'),normalizeName('abc'));
  assert.notEqual(normalizeName('Вода'),normalizeName('Тепло'));
});
test('Import input is bounded, strict and accepts only existing operation fields',()=>{
  const schema=makeImportSchema(createSchema);
  assert.equal(schema.parse({idempotencyKey:'invoice-123',category:{name:'  Коммуналка  ',type:'expense'}}).category.name,'Коммуналка');
  for(const input of [{idempotencyKey:'invoice-123'},{idempotencyKey:'invoice-123',contractor:{name:' ' }},{idempotencyKey:'invoice-123',category:{name:'Свет'},approved:true},{idempotencyKey:'invoice-123',operation:{type:'transfer',amount:'1',date:'2026-10-24',accountId:'a'.repeat(24)}}])assert.equal(schema.safeParse(input).success,false);
});
