# Live voice in INDEX12

The waveform button opens a WebRTC call to OpenAI Realtime. The existing microphone button remains dictation into the text composer.

## Server configuration

Set `OPENAI_API_KEY` on the backend. It stays on the server. The optional `OPENAI_REALTIME_MODEL` defaults to `gpt-realtime-2.1`. Realtime API usage is billed separately from Codex/ChatGPT subscriptions. An authenticated INDEX12 user, workspace membership, and an online Codex bridge are required.

## Request path

The browser sends an SDP offer through the authenticated desktop router. The backend creates the provider call and attaches a sideband WebSocket. Only finalized audio transcriptions received directly from the provider become `desktopJobs`. These use the original user transcript, the current chat and workspace, and the existing Codex worker, permission cards, execution journal, and mutation receipts. The browser cannot submit a voice transcript or a model-generated tool command through this API.

Codex results return to the same chat. Realtime reads the result aloud. Automatic Realtime answers are disabled. Interrupting audio does not undo, repeat, or silently cancel an accepted data operation. Closing voice leaves accepted jobs to finish in the chat.

## Lifecycle and privacy

Only session control metadata is persisted in `desktopVoiceSessions`; audio and SDP are not saved there. Recognized text remains in normal chat history. The browser stops microphone tracks on close, failure, scope change, and unmount. A unique active-session index and renewable 30-second lease prevent concurrent calls in one chat. Lost clients expire after 30 seconds; calls end after 20 minutes. Metadata has a one-hour TTL. A subsequent connection can clean up an expired provider call after a backend restart.

## Verification

- Backend: `node --test tests/codex-voice.test.cjs`
- Frontend: `node --test tests/codex-voice.test.mjs` and `node tests/browser/codex-voice.mjs`
- Optional paid end-to-end test: `OPENAI_API_KEY=… node tests/integration/voice-live.cjs`

The live test uses a disposable database, synthetic account, spoken Russian input, real Codex/MCP, and real provider audio. It asserts the account remains unchanged and no financial writes occur. It needs the local Codex bridge dependencies, MongoDB test fixture, Chrome, macOS Russian speech, and ffmpeg.
