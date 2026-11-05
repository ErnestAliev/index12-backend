const { test } = require("node:test"),
  assert = require("node:assert/strict");
const { extractInvoice, matchParty } = require("../team-chat/invoice.cjs");
const result = {
  isInvoice: true,
  title: "Обслуживание",
  amount: "200000.00",
  currency: "KZT",
  dueDate: null,
  invoiceNumber: "13",
  supplierName: "ИП Поставщик",
  supplierBin: "123456789012",
  buyerName: "ТОО Компания",
  buyerBin: "250340021816",
  comment: null,
  categoryId: null,
  subcategoryId: null,
};
test("PDF extraction sends a scoped file with strict schema, no tools or storage and validates the model result", async () => {
  let sent;
  const actual = await extractInvoice({
    bytes: Buffer.from("%PDF-1.4\nfixture"),
    name: "invoice.pdf",
    categories: [{ _id: "category", name: "Обслуживание" }],
    subcategories: [],
    env: { OPENAI_API_KEY: "test-only", OPENAI_CHAT_MODEL: "configured-model" },
    request: async (url, options) => {
      assert.equal(url, "https://api.openai.com/v1/responses");
      sent = JSON.parse(options.body);
      return {
        ok: true,
        json: async () => ({
          status: "completed",
          output: [
            {
              content: [{ type: "output_text", text: JSON.stringify(result) }],
            },
          ],
        }),
      };
    },
  });
  assert.deepEqual(actual, result);
  assert.equal(sent.store, false);
  assert.equal(sent.tools, undefined);
  assert.equal(sent.model, "configured-model");
  assert.equal(sent.text.format.strict, true);
  assert.equal(sent.text.format.schema.additionalProperties, false);
  assert.match(sent.instructions, /недоверенные/);
  assert.match(
    sent.input[0].content[0].file_data,
    /^data:application\/pdf;base64,/,
  );
});
test("ambiguous extraction, refusal and non-PDF input never become invented payment data", async () => {
  await assert.rejects(
    extractInvoice({
      bytes: Buffer.from("not pdf"),
      name: "invoice.pdf",
      categories: [],
      subcategories: [],
      env: { OPENAI_API_KEY: "test" },
    }),
  );
  await assert.rejects(
    extractInvoice({
      bytes: Buffer.from("%PDF-test"),
      name: "i.pdf",
      categories: [],
      subcategories: [],
      env: { OPENAI_API_KEY: "test" },
      request: async () => ({
        ok: true,
        json: async () => ({ status: "incomplete" }),
      }),
    }),
  );
  await assert.rejects(
    extractInvoice({
      bytes: Buffer.from("%PDF-test"),
      name: "i.pdf",
      categories: [],
      subcategories: [],
      env: { OPENAI_API_KEY: "test" },
      request: async () => ({
        ok: true,
        json: async () => ({
          status: "completed",
          output_text: '{"amount":200}',
        }),
      }),
    }),
  );
});
test("party matches are exact and unique; missing identifiers can reuse a unique name without overwriting a conflicting BIN", () => {
  const rows = [
    { _id: "a", name: "ТОО «YBB GROUP»", identificationNumber: null },
    { _id: "b", name: "Different", identificationNumber: "123456789012" },
  ];
  assert.equal(matchParty(rows, "ТОО YBB GROUP", null)._id, "a");
  assert.equal(matchParty(rows, "alias", "123456789012")._id, "b");
  assert.equal(matchParty(rows, "ТОО YBB GROUP", "999999999999")._id, "a");
  assert.equal(
    matchParty(
      [...rows, { _id: "c", name: "ТОО YBB GROUP" }],
      "ТОО YBB GROUP",
      null,
    ),
    null,
  );
  assert.equal(
    matchParty(
      [{ name: "Same", identificationNumber: "111111111111" }],
      "Same",
      "222222222222",
    ),
    null,
  );
});
