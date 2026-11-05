# Internal employee chat

The `/api/team-chat` router provides a workspace-scoped chat alongside INDEX12. Owners use their existing INDEX12 session; invitees use a separate phone/password session at `/team/:token`. A phone session never authenticates the user to financial APIs. Joining requires an owner decision. Chat roles do not modify workspace roles.

The owner starts as `manager`; chat settings allow `employee`, `manager`, and `director`. Only owners issue invites and assign/revoke roles. Phone numbers normalize to E.164; passwords use random salts and scrypt. Authentication attempts are limited in MongoDB. Membership and workspace access are checked on every request. Files are scoped, downloaded as attachments, limited to five files and 25 MB per message, 10 MB per file. Unlinked uploads expire after one day.

Employees create requests with a title, formatted amount, payment date, a selected or newly created workspace contractor, and an optional comment. They do not choose our company or account. Owner identity and the current chat role are shown in the header; only the owner can select their own role. The composer is shared with Codex.

When a PDF is sent, only its sender gets the invoice-processing question. Confirmation calls the server's OpenAI Responses integration using the existing `OPENAI_API_KEY` and `OPENAI_CHAT_MODEL`, with PDF input, a strict extraction schema, no tools, and `store: false`. Supplier and buyer remain distinct. Parties match by a unique BIN or exact normalized name; conflicting BINs are not overwritten. Expense categories and subcategories are suggested only from the workspace catalog. The original PDF remains attached. File ID and content hash prevent duplicate requests. Recognition failures are reported; they do not manufacture financial operations.

Expense requests follow `manager_review → director_review → ready → recorded`, or `rejected`. Decisions require the current role and expected version. The director sees request cards only after manager submission. A separate forecast includes all active requests, including those not yet submitted.

The owner acting as manager can drag a request onto a timeline cell or open its preparation form. Missing details use the existing expense editor: account, company, account owner, categories/subcategory, projects, contractor, date, amount, description and optional comment. Enrichment creates a virtual draft chip, not a financial Event. Drafts reserve cells across GUI and MCP operations under the existing transaction/lock. The manager can save the draft or submit the full snapshot to the director. Submitted details are frozen; recall is required to edit them and reallocates a cell if needed. Cards show the approval chain and decision history. The current first release lets the owner acting as manager convert a fully approved request into a real expense using the existing audited operation handler. A selected account must belong to that request's company. The request and operation commit together, conversion retries return the existing operation, and timeline placement uses a free cell within 32 visible cells.

The Requests tab has a separate cash forecast: all active requests, including unapproved requests, reduce projected cash on their requested payment date. Rejected and converted requests are excluded. Existing scheduled INDEX12 operations are included up to the latest request date. Company cash pools stay separate. Requests without an assigned company are shown as unallocated costs and never charged to an arbitrary company. This forecast is in chat. Prepared virtual request chips appear on the timeline, while the main dashboard's financial totals still use real operations only. Existing entries remain the source of current balances; an expense entry does not prove bank payment.

Messages update through authenticated SSE, with reconnect/focus recovery and a 15-second fallback. SSE carries invalidation only. Owners see an in-app join approval dialog above other windows. This dialog does not claim OS push delivery when INDEX12 is closed. No SMS verification/provider or forgotten-password recovery is included in this initial release.

Read-only chat state excludes password material and returns 50 messages with a compound timestamp/ID history cursor. The most recent 500 request cards are shown with explicit truncation; the forecast always includes all active requests.

Validation:

- `npm test`
- `node tests/integration/timeline-slots.cjs`
- `node tests/integration/team-chat.cjs`
- `node tests/integration/team-chat.cjs --browser` (uses the sibling frontend repository, isolated MongoDB and headless Chrome)
- Frontend: `npm test`, `npm run build`, `node tests/browser/codex-styles.mjs`
