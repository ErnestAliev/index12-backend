# Codex and INDEX12 MCP

The only chat route is `/api/codex`. The site submits text and optional image
attachments to a queue scoped to the authenticated user and current workspace.
A local worker connects to the shared Codex App Server, resumes the selected
thread, and delivers the answer and canonical thread history back to the site.

Financial reads and changes use the existing INDEX12 MCP and its access checks.
The site no longer sends UI snapshots, prebuilt prompts, or conversation requests
to the OpenAI API. The retired AI routes, agent, RAG, history lifecycle, and model
client dependency have been removed. Existing Codex threads keep their identity.

The desktop button below the receipt generator opens a separate window over the
workspace. Its left and top edges resize width and height; the top-left corner
resizes both. On mobile the Codex button opens the same chat in a compact window.
