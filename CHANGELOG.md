# Changelog

Version history for `sap-ai-core-orchestration.py`. The version is also
in the file header, which OpenWebUI shows in the UI.

## 1.2.0 — 2026-09-29

First public release.

### Added

- Automatic model discovery from the orchestration deployment's
  `modelFilterList`, cached for `MODEL_CACHE_TTL` seconds and warmed at startup.
  A model that is in the AI Core catalog but not in the deployment's list is
  not shown, because SAP would return a 403 for it.
- Streaming and non-streaming responses, with a `STREAM_CHUNK_SIZE` valve.
  SAP generates some model families completely before streaming, so the valve
  only smooths delivery where the model streams for real (Claude).
- OpenWebUI Tools support through a client-side tool loop, capped by
  `TOOL_LOOP_MAX_ITERATIONS`.
- Image and PDF input passed straight through to the model.
- File uploads: text that OpenWebUI extracts from docx, pptx, xlsx, csv and
  similar files is added to the latest user message, capped by
  `UPLOADED_FILE_MAX_CHARS`. SAP's own file block accepts only images and PDF.
- Claude prompt caching for `anthropic--*` models, with breakpoints on the tools,
  the system prompt and the conversation history. GPT and Gemini cache
  automatically and need nothing.
- `reasoning_effort` (`none`, `low`, `medium`, `high`, `xhigh`) and `verbosity`
  passthrough when no tools are attached.
- Token usage saved to the chat message so OpenWebUI shows it after a reload.
  Cached and cache-creation tokens are included, and `input_tokens` counts each
  model family's prompt tokens correctly without double counting.
- Optional token logging behind `ENABLE_TOKEN_LOGGING`: a per-turn log line and,
  with `USAGE_LOG_DB_URL`, a row per turn in a PostgreSQL table
  (`USAGE_LOG_TABLE`). The table is created automatically if it doesn't exist.
- Per-turn timing columns in the usage table: `total_ms`, `ttfb_ms`, `sap_ms`,
  `tool_ms`, `auth_ms` and `discovery_ms`.
- Service key auth from a single `AICORE_SERVICE_KEY` valve. Flat and Cloud
  Foundry (`credentials`-nested) shapes both work, and OAuth tokens are cached
  and refreshed.
- Optional outbound proxy and an SSL verification toggle.
- HTTP connections reused across turns. Usage logging runs in the background so
  a slow database never delays a reply.

### Behaviors that follow SAP's API

- `tool_choice` is dropped, because SAP rejects it.
- `reasoning_effort` and `verbosity` are dropped when tools are attached,
  because SAP rejects the combination.
- `temperature=0` is dropped for gpt-5-family models, which reject it.
- Streams are read without a line-length limit, because SAP can send very
  large single-line events for long conversations.
- The usage chunk includes both `input_tokens`/`output_tokens` and
  `prompt_tokens`/`completion_tokens`, plus an empty `choices` list, so strict
  OpenAI-compatible clients accept it.
- Non-streaming requests never receive the usage or tool-call chunks, which
  OpenWebUI would otherwise print as text.
- Model ids drop SAP's `<vendor>--` prefix and a `:latest` suffix for display.
  The real SAP model name is always what is sent.
- The usage database URL and table name are read from environment variables
  only, not Valves, so an admin-UI user cannot redirect logging.
