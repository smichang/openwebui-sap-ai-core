# SAP AI Core Orchestration Pipe for OpenWebUI

An OpenWebUI **Pipe** that connects chat to the models on your SAP AI Core
**orchestration** deployment. Models are discovered automatically, so nothing
has to be hardcoded.

- **Keeps the SAP AI Core key hidden.** The service key lives only in the
  pipe's admin-only Valves. Users chat through OpenWebUI and never see or
  handle SAP credentials.
- **Works as an OpenAI-compatible LLM.** SAP's orchestration API is wrapped so
  its models appear as normal OpenWebUI models, streaming and tool calls
  included. OpenWebUI's OpenAI-style `/api/chat/completions` endpoint can call
  them too.
- **Records token usage in OpenWebUI's database.** Usage is saved with each
  chat message, and optionally logged to a PostgreSQL table for reporting
  (see *Token logging*).

## Features

- **Automatic model discovery.** Reads the deployment's `modelFilterList`, so
  the model selector always matches what your deployment allows. Results are
  cached (default 5 min) and warmed at startup.
- **Streaming and non-streaming** responses (SSE), with a configurable chunk size.
- **OpenWebUI Tools support.** Attached Tools run in a client-side tool loop
  with a safety cap on iterations.
- **Multimodal input.** Images and PDFs pass straight through to the model.
- **File uploads.** Text that OpenWebUI extracts from docx, pptx, xlsx, csv and
  similar files is added to your latest message, with a per-file size cap.
  SAP's own file block only accepts images and PDF, so this is how the other
  formats reach the model.
- **Claude prompt caching.** `cache_control` breakpoints go on the system prompt,
  the tools and the conversation history for `anthropic--*` models. GPT and
  Gemini cache automatically and need nothing.
- **Reasoning controls.** `reasoning_effort` (`none`, `low`, `medium`, `high`,
  `xhigh`) and `verbosity` pass through when no tools are attached.
- **Token usage.** Totals, including cached and cache-creation tokens, are
  saved to the chat, so OpenWebUI shows them after a reload.
- **Optional token logging.** A valve turns on a per-turn `token usage` log line
  and, if you provide a PostgreSQL URL, inserts a row per turn into a usage
  table. The table is created automatically if it doesn't exist.
- **Simple auth.** Paste the AI Core service key JSON into one valve. The flat
  and Cloud Foundry (`credentials`-nested) shapes both work. OAuth tokens are
  cached and refreshed automatically.
- **Optional outbound proxy** and SSL verification toggle.
- **Reused HTTP connections** across turns for lower latency.

## Requirements

- OpenWebUI with Functions enabled (admin access to add one).
- An SAP AI Core instance with a **RUNNING** deployment whose `scenarioId` is
  `orchestration`.
- An AI Core **service key**: in the BTP cockpit, open the AI Core instance and
  choose *Create Service Key*.
- Python packages `aiohttp` and `requests` (OpenWebUI installs them from the
  file header).

## Installation

1. In OpenWebUI, go to **Admin Panel → Functions → + (New Function)**.
2. Paste the contents of `sap-ai-core-orchestration.py` and save.
3. Enable the function with the toggle.
4. Open the function's **Valves** (gear icon) and paste your service key JSON
   into `AICORE_SERVICE_KEY`. Set `AICORE_RESOURCE_GROUP` if you don't use `default`.
5. Refresh the page. Models appear in the selector with the prefix "SAP Orch -".

## Configuration (Valves)

Each valve can also be set with an environment variable of the same name,
except the proxy valves (see the table).

| Valve | Default | Description |
| --- | --- | --- |
| `AICORE_SERVICE_KEY` | *(empty)* | Full service key JSON. Required. |
| `AICORE_RESOURCE_GROUP` | `default` | Sent as the `AI-Resource-Group` header. |
| `MODEL_CACHE_TTL` | `300` | Seconds to cache discovery. `0` disables caching. |
| `STREAM_CHUNK_SIZE` | `50` | Minimum characters per streamed chunk. `0` uses SAP's default (100). |
| `TOOL_LOOP_MAX_ITERATIONS` | `8` | Maximum tool round-trips per turn. |
| `REQUEST_TIMEOUT` | `300` | Seconds allowed for a single completion call. |
| `VERIFY_SSL` | `true` | Set `false` only for testing. |
| `HTTP_PROXY` / `HTTPS_PROXY` | *(empty)* | Optional proxy. Environment variables are `AICORE_HTTP_PROXY` and `AICORE_HTTPS_PROXY`. |
| `UPLOADED_FILE_MAX_CHARS` | `60000` | Per-file cap on extracted upload text. |
| `ENABLE_TOKEN_LOGGING` | `false` | Master switch for the log line and database rows. |
| `MONTHLY_TOKEN_QUOTA` | `0` | Per-user monthly token limit (UTC month). `0` disables. For example `100000000` is 100 million. See *Monthly token quota*. |
| `MONTHLY_TOKEN_QUOTA_OVERRIDES` | *(empty)* | JSON object of per-user limits, for example `{"alice@example.com": 500000000}`. A limit of `0` means unlimited for that user. |
| `QUOTA_EXEMPT_ADMINS` | `true` | Admins are not limited. |
| `QUOTA_WARN_PERCENT` | `80` | Append a usage notice to the reply once a user passes this percentage of their limit. `0` disables the notice. |

Environment-only settings (not valves, because the URL can contain a password):

| Variable | Default | Description |
| --- | --- | --- |
| `USAGE_LOG_DB_URL` | *(empty)* | PostgreSQL DSN, for example `postgresql://user:pass@host:5432/db`. Empty means log lines only. |
| `USAGE_LOG_TABLE` | `aicore_usage_log` | Table name. Created with `CREATE TABLE IF NOT EXISTS` on the first insert. |

## Choosing which models appear (`modelFilterList`)

The model selector is built from the `modelFilterList` parameter on your
orchestration deployment's configuration. To add or remove models, change that
list in SAP AI Core, not in OpenWebUI.

SAP restricts orchestration models with two parameter bindings, set when the
deployment is created:

| Binding | Meaning |
| --- | --- |
| `modelFilterList` | JSON list of `modelName` and optional `modelVersions`. If `modelVersions` is omitted, all versions of that model are considered. |
| `modelFilterListType` | `allow` (only these models, the default) or `deny` (everything except these). |

**1. Create a configuration** with the bindings, then create an
`orchestration` deployment from it. In **SAP AI Launchpad** use
*ML Operations → Configurations*. Or call the API
(`POST {AI_API_URL}/v2/lm/configurations`, with the `AI-Resource-Group` header):

```json
{
  "name": "orchestration-models",
  "executableId": "orchestration",
  "scenarioId": "orchestration",
  "versionId": "0.0.1",
  "parameterBindings": [
    {
      "key": "modelFilterList",
      "value": "[{\"modelName\": \"anthropic--claude-4.5-haiku\"}, {\"modelName\": \"anthropic--claude-4.6-sonnet\"}, {\"modelName\": \"gpt-5.6-sol\", \"modelVersions\": [\"2026-07-09\", \"latest\"]}]"
    },
    {
      "key": "modelFilterListType",
      "value": "allow"
    }
  ]
}
```

The `value` of `modelFilterList` is a JSON **string**, so the inner quotes are
escaped. The list decoded looks like this:

```json
[
  { "modelName": "anthropic--claude-4.5-haiku" },
  { "modelName": "anthropic--claude-4.6-sonnet" },
  { "modelName": "gpt-5.6-luna", "modelVersions": ["2026-07-09", "latest"] },
  { "modelName": "gpt-5.6-terra", "modelVersions": ["2026-07-09", "latest"] },
  { "modelName": "gpt-5.6-sol", "modelVersions": ["2026-07-09", "latest"] }
]
```

**2. Wait for the deployment to be RUNNING.** The pipe uses the RUNNING
`orchestration` deployment in your resource group.

**3. Refresh OpenWebUI.** The pipe caches discovery for `MODEL_CACHE_TTL`
seconds (default 300). Wait that long, or set it to `0` and reload the page.

How the pipe reads the list:

- Use the exact SAP model name, for example `anthropic--claude-4.6-sonnet`.
- Leave `modelFilterListType` unset or `allow`. The pipe reads only
  `modelFilterList` and treats it as the models to show, so with `deny` it
  would list the models SAP blocks.
- If `modelVersions` includes `latest` the model is used as `latest`. Otherwise
  the first listed version is used. With no `modelVersions`, `latest` is used.
- Wildcards such as `["*"]` are not expanded. List the models you want by name.

If a model you added does not appear, see *No models in the selector* and
*Model list looks outdated* under Troubleshooting.

## Token logging

1. Set `ENABLE_TOKEN_LOGGING` to `true` in the Valves.
2. Optionally set `USAGE_LOG_DB_URL` in OpenWebUI's environment and restart it.
   The `psycopg2-binary` package is installed from the file header.

Each turn writes one row: time (epoch seconds), user id and email, chat id,
model, and input, output, reasoning, cached, cache-creation and total tokens.
`input_tokens` excludes cached tokens.

Timing columns, in milliseconds:

| Column | Meaning |
| --- | --- |
| `total_ms` | Whole turn, including tool calls. |
| `ttfb_ms` | Time until the first text reached the user (`NULL` if none). |
| `sap_ms` | Time spent in SAP AI Core calls, summed over tool-loop rounds. |
| `tool_ms` | Time spent running OpenWebUI Tools. |
| `auth_ms` | Time getting the OAuth token. Near zero when cached. |
| `discovery_ms` | Time locating the deployment. Near zero when cached. |

Inserts run in the background and a database error is only logged, so chat is
never blocked.

Example monthly summary:

```sql
SELECT user_email, model_name, COUNT(*) AS requests, SUM(total_tokens) AS tokens
FROM aicore_usage_log
WHERE created_at >= EXTRACT(EPOCH FROM date_trunc('month', now()))
GROUP BY 1, 2 ORDER BY tokens DESC;
```

## Monthly token quota

Set `MONTHLY_TOKEN_QUOTA` to limit how many tokens each user can use per
calendar month (UTC). Before each turn the pipe adds up the user's usage for the
month from the token log table. A user at or over the limit gets a message with
their usage and the reset date, and no request is sent to SAP AI Core.

- Units are raw tokens. Use plain digits, for example `1000000000` for 1 billion.
- Cache tokens are weighted by billing rate: input and output count 1x, cached
  tokens 0.1x, cache-creation tokens 1.25x. The quota usage is therefore lower
  than the `total_tokens` column when the cache is used.
- It needs `ENABLE_TOKEN_LOGGING` set to `true` and `USAGE_LOG_DB_URL` set, since
  usage is read from the table. Without them, or if the database cannot be
  reached, turns are allowed and an error is logged.
- The check runs once per turn, so one long turn can pass the limit. Totals are
  cached for 60 seconds per worker, so several workers can overshoot slightly.
- Use `MONTHLY_TOKEN_QUOTA_OVERRIDES` for individual limits. Emails must match
  the user's OpenWebUI email exactly.

## Usage tools

**For every user: `sap_ai_core_my_usage`.** Users can ask "how much have I used
this month?". It returns their own requests and tokens by model and by day, and
their quota limit, usage, remaining and percent used. It always uses the
signed-in user's email, so it cannot show anyone else's usage.

**For admins: `sap_ai_core_usage_summary`.** Ask the model, for example, "show token usage for 2026-10" or "who is close to
their quota". It reads the token log table and returns requests and tokens by
user and model, a per-day breakdown (UTC), and each user's `quota_limit`,
`quota_used`, `quota_remaining` and `quota_used_percent`. It needs
`USAGE_LOG_DB_URL`. Only users with usage in the month are listed.

Both tools need `USAGE_LOG_DB_URL`.

## Usage

- **Chat.** Pick any `SAP Orch - <model>` and chat as usual.
- **Reasoning.** Set *Reasoning Effort* in the chat's advanced parameters. It
  is ignored, with a log line, when Tools are active.
- **Tools.** Enable OpenWebUI Tools for the chat or model. The pipe calls them
  and feeds the results back until the model gives a final answer.
- **Files.** Attach a document or image. Images and PDFs go to the model
  directly. Other formats are sent as extracted text, so scanned PDFs with no
  text layer may arrive empty.
- **Versions.** If a deployment lists a specific model version instead of
  `latest`, it shows as `model (version)`.

## Behaviors to know about

These follow SAP's API rules, and the pipe works around them.

- SAP rejects `tool_choice` entirely, so the pipe drops it.
- SAP rejects `reasoning_effort` and `verbosity` together with tools, so the
  pipe drops them when tools are present.
- gpt-5-family models reject `temperature=0`. The pipe drops it and uses the
  model default.
- A model that is in the AI Core catalog but missing from your deployment's
  allow-list is not shown, because SAP would return a 403 for it.
- Tool calls that have no matching OpenWebUI Tool are returned to the caller
  only when streaming. A non-streaming request gets a short notice instead.
- Very large conversations with tools can be slow to start, because SAP echoes
  the templated prompt in its stream.

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| No models in the selector | The service key is missing or invalid, or there is no RUNNING `orchestration` deployment in the resource group. Check the OpenWebUI logs. |
| `service key is not configured or is missing ...` | The pasted JSON lacks `url`, `clientid`, `clientsecret` or `serviceurls.AI_API_URL`. |
| `token request rejected (400/401)` | The service key is stale or revoked. Create a new one in BTP. |
| `Model ... is not allowed` (403) | The model is not in this deployment's allow-list. Update the deployment configuration. |
| Connection timeouts | Set `HTTPS_PROXY` if your network needs one. Raise `REQUEST_TIMEOUT` for long generations. |
| Model list looks outdated | Wait for `MODEL_CACHE_TTL` to expire, or set it to `0` while testing. |
| File contents not seen by the model | OpenWebUI could not extract text from the file (for example a scanned PDF). Use OCR or attach it as an image. |

## Security notes

- The service key is a secret. Only admins can see Valves, but keep the
  function private and never commit the key.
- The pipe logs the user's email and chat id at info level and never logs the
  credentials.
- Leave `VERIFY_SSL` on in production.

## License

MIT
