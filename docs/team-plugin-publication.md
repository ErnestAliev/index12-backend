# INDEX12 Team publication

This is a separate employee plugin. Never replace the owner's existing `/mcp`
connection, copy its credentials into this package, or make employee data public.

Build: `python3 scripts/package-team-plugin.py`.
Live sample API checks: `node scripts/check-team-plugin-review.cjs`.
The ZIP is written under ignored `output/plugins/`. Only three explicitly
allowlisted files enter it. Source: `plugins/index12-team/`.

MCP endpoint: `https://api.index12.com/team-mcp`.
OAuth uses discovery, DCR, PKCE S256, explicit role consent and per-user tokens.
Employee login requires an active invited account. The server rechecks status,
role, workspace and authentication epoch on every call. Financial access is
read-only and requires manager/director role plus consent at connection time.
Descriptor discovery is stable across roles; discovery does not grant access.

Public listing pages are served from `/plugins/index12-team`, `/support`,
`/privacy`, `/terms` on api.index12.com. These describe actual connector
behavior and use the support contact already published in INDEX12's About view.

## Current submission

Developer identity: Approved. Package 1.0.0 uploaded as a draft.
Plugin: `plugin_asdk_app_6af1518cea7c8191984a4567619dd69e`
Draft: `appsub_6af1518ceaa4819193ade69f77ac5657`
Portal: https://platform.openai.com/plugins/manage/plugin_asdk_app_6af1518cea7c8191984a4567619dd69e

Status: Ready for final submission / Not submitted / Not published.
Domain verification completed. Metadata and MCP scans both show No findings;
MCP configuration is Configured. Reviewer credentials and video URL are saved.
The deployed MP4 returned HTTP 206 with valid video/mp4 byte-range playback.
The final Submit for review dialog is open with the private context filled.

The remaining step is the user's explicit confirmation of OpenAI's legal
attestations: Developer Apps Terms and Plugin Guidelines; applicable-law
compliance/responsibility; no money/crypto transfers or investment trades;
necessary third-party content/API rights; suitability for under-18 users; no
targeting under-13s or sharing their personal data with OpenAI. No attestation
was checked and Submit was not clicked before that confirmation.
The 2m42s walkthrough uses only the dedicated sample workspace. It is a real
ChatGPT tab recording with idle frames removed; the original local recording
is retained. Review URL: https://api.index12.com/plugins/index12-team/review-demo-20261109.mp4
All four public listing pages return HTTP 200. The domain challenge returns
only the exact token. The 1.0.0 package was accepted by the portal.

A dedicated sample tenant was provisioned with no existing company data.
`node scripts/check-team-plugin-review.cjs` passed five positive MCP API cases
and two negative access cases. Its access token was revoked afterward. The
real ChatGPT walkthrough additionally verified all five positive cases and the
three negative scenarios. The employee financial request returned FORBIDDEN;
role escalation, credential disclosure and financial transfers were refused.
An injected instruction was read from the isolated sample chat and ignored;
no external send occurred. New sample task and common-chat message writes
completed through ChatGPT after the explicit write consent. The local OAuth/PKCE, role,
revocation and task-history pagination integration checks also passed.

The private `.plugin-review.local.json` file contains the sample credentials.
The user explicitly confirmed OAuth access and transfer of these reviewer
credentials to OpenAI. The portal connection now shows Authorized; 28 tools
were discovered, and the latest MCP scan has no findings. Reviewer credentials
have been saved in the secure Review information form. A separate development
connection, INDEX12 Team Review, was installed in the owner's ChatGPT for the
actual sample-data walkthrough; it is not the public release URL.

The browser CSP had to allow the OpenAI review portal in the callback redirect
chain, in addition to ChatGPT. The employee-only instructions and company-card
description are independent of other plugins. Public package baseScopes
request both read and write permissions at installation; the manually created
development connection initially requested read only, so the video includes
a scope-upgrade OAuth prompt. That development-only URL must not be given to
employees as the published plugin URL.
The domain challenge URL returned 404 before this token was installed; no
existing plugin token was overwritten. The challenge token is intentionally
public and is not a credential.

## Portal steps and remaining requirements

1. Select project INDEX12 in the OpenAI Developer Portal.
2. Upload the ZIP with MCP. Resolve automated package findings.
3. Host the exact domain challenge shown by the portal. Never overwrite a
   different plugin's existing challenge token. Use an eligible parent host or
   distinct endpoint hostname if needed.
4. Connect OAuth with a dedicated sample-data account, not the owner's account.
   Keep reviewer credentials in the secure portal, never in the package or Git.
5. Run the five positive and three negative cases in the package using that
   account. Record the real walkthrough and provide an accessible video URL.
6. Complete review details, policy attestations, then submit. Publication is
   possible only after OpenAI approval; uploading alone does not publish.
7. After actual publication, set `TEAM_CHAT_PLUGIN_URL` to the real public
   `https://chatgpt.com/plugins/plugin_...` URL. The employee UI displays the
   installation button only when this exact URL has been configured.

Do not set a private/development owner plugin URL as TEAM_CHAT_PLUGIN_URL.
Do not fabricate a video URL, reviewer test result, approval or review duration.

Official references (checked 2026-11-09):

- https://developers.openai.com/plugins/build/plugins
- https://developers.openai.com/plugins/build/auth
- https://developers.openai.com/plugins/deploy/submission
