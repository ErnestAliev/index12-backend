# INDEX12 Team publication

This is a separate employee plugin. Never replace the owner's existing `/mcp`
connection, copy its credentials into this package, or make employee data public.

Build: `python3 scripts/package-team-plugin.py`.
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

Status at upload: Not submitted / Not published.
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
