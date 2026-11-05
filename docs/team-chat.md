# Internal employee chat

The `/api/team-chat` router provides a workspace-scoped chat alongside INDEX12. Owners use their existing INDEX12 session; invitees use a separate phone/password session at `/team/:token`. A phone session never authenticates the user to financial APIs. Joining requires an owner decision. Chat roles do not modify workspace roles.

The owner starts as `manager`; chat settings allow `employee`, `manager`, and `director`. Only owners issue invites and assign/revoke roles. Phone numbers normalize to E.164; passwords use random salts and scrypt. Authentication attempts are limited in MongoDB. Membership and workspace access are checked on every request. Files are scoped, downloaded as attachments, limited to five files and 25 MB per message, 10 MB per file. Unlinked uploads expire after one day.

Expense requests follow `manager_review → director_review → ready → recorded`, or `rejected`. Decisions require the current role and expected version. The current first release lets the owner acting as manager convert a fully approved request into a real expense using the existing audited operation handler. A selected account must belong to that request's company. The request and operation commit together, conversion retries return the existing operation, and timeline placement uses a free cell within 32 visible cells.

The Requests tab has a separate cash forecast: all active requests, including unapproved requests, reduce projected cash on their requested payment date. Rejected and converted requests are excluded. Existing scheduled INDEX12 operations are included up to the latest request date. Company cash pools stay separate. This initial forecast is in chat; it does not insert unapproved requests into timeline cells or alter the main dashboard's financial totals. Existing entries remain the source of current balances; an expense entry does not prove bank payment.

Messages update through authenticated SSE, with reconnect/focus recovery and a 15-second fallback. SSE carries invalidation only. Owners see an in-app join approval dialog above other windows. This dialog does not claim OS push delivery when INDEX12 is closed. No SMS verification/provider or forgotten-password recovery is included in this initial release.

Read-only chat state excludes password material and returns 50 messages with a compound timestamp/ID history cursor. The most recent 500 request cards are shown with explicit truncation; the forecast always includes all active requests.

Validation:

- `npm test`
- `node tests/integration/team-chat.cjs`
- `node tests/integration/team-chat.cjs --browser` (uses the sibling frontend repository, isolated MongoDB and headless Chrome)
- Frontend: `npm test`, `npm run build`, `node tests/browser/codex-styles.mjs`
