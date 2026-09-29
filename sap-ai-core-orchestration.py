"""
title: SAP AI Core Orchestration
description: Chat pipe for SAP AI Core's orchestration deployment. Auto-discovers allow-listed models, streams, supports OpenWebUI Tools, images/PDF, file uploads and Claude prompt caching.
author: Smith Chang
version: 1.2.0
license: MIT
requirements: aiohttp, requests, psycopg2-binary
"""

# Talks to a single SAP AI Core scenarioId="orchestration" deployment, which
# proxies to an allow-listed set of models via POST {deploymentUrl}/v2/completion.
#
# The allow-list is per-deployment: a model can exist in the AI Core catalog yet
# 403 because it's absent from THIS deployment's config. The source of truth is
# parameterBindings.modelFilterList (double-encoded JSON string), hence
# _discover_models() rather than a hardcoded list.
#
# model.params is a passthrough dict to the underlying model API. Valid
# reasoning_effort: none|low|medium|high|xhigh. SAP rejects tools together with
# reasoning_effort, rejects tool_choice outright (both dropped), and gpt-5-family
# models reject temperature=0 (also dropped).
#
# tools belong in prompt.tools (NOT model.params); stream is config.stream, a
# sibling of config.modules. The completion is wrapped at response.final_result.
#
# Discovery uses sync requests because pipes() must stay sync; a TTL cache and
# the on_startup() warm-up keep it off the event loop. The chat path is aiohttp
# so streaming never blocks, and reaches the sync helpers via asyncio.to_thread.

from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from pydantic import BaseModel
import aiohttp
import asyncio
import inspect
import json
import logging
import os
import re
import requests
import time


class Pipe:
    """
    SAP AI Core orchestration pipe for OpenWebUI.

    Exposes every model allow-listed on the orchestration deployment as a
    selectable model, and resolves OpenWebUI-attached Tools itself in a
    client-side tool loop.
    """

    class Valves(BaseModel):
        """Configuration for SAP AI Core credentials and behaviour."""

        # Paste the whole AI Core service key JSON here (BTP cockpit -> AI Core
        # instance -> Create Service Key). Flat and Cloud-Foundry/VCAP-style
        # ("credentials" nesting) shapes are both accepted.
        AICORE_SERVICE_KEY: str = os.getenv("AICORE_SERVICE_KEY", "")
        AICORE_RESOURCE_GROUP: str = os.getenv("AICORE_RESOURCE_GROUP", "default")  # Not part of the service key

        MODEL_CACHE_TTL: int = int(os.getenv("MODEL_CACHE_TTL", 300))  # Seconds to cache deployment+model discovery; 0 disables

        # Min chars per SSE chunk (config.stream.chunk_size); SAP default is
        # 100 if omitted. 0 omits the field.
        STREAM_CHUNK_SIZE: int = int(os.getenv("STREAM_CHUNK_SIZE", 50))

        TOOL_LOOP_MAX_ITERATIONS: int = int(os.getenv("TOOL_LOOP_MAX_ITERATIONS", 8))  # Safety cap on tool round-trips per turn
        REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", 300))  # Seconds for a single /v2/completion call
        VERIFY_SSL: bool = os.getenv("VERIFY_SSL", "true").lower() == "true"

        # Optional outbound proxy; leave empty for a direct connection.
        HTTP_PROXY: str = os.getenv("AICORE_HTTP_PROXY", "")
        HTTPS_PROXY: str = os.getenv("AICORE_HTTPS_PROXY", "")

        # Per-file cap on OpenWebUI-extracted upload text, folded into the
        # newest user message. SAP's file block only accepts images/PDF, so
        # this is how other formats (docx, xlsx, ...) reach the model.
        UPLOADED_FILE_MAX_CHARS: int = int(os.getenv("UPLOADED_FILE_MAX_CHARS", 60000))

        # Off by default. When on, each turn's token usage goes to the server
        # log and, if USAGE_LOG_DB_URL is set, to a PostgreSQL table.
        ENABLE_TOKEN_LOGGING: bool = os.getenv("ENABLE_TOKEN_LOGGING", "false").lower() == "true"
        # USAGE_LOG_DB_URL / USAGE_LOG_TABLE are env-only, not valves: the DSN can hold a password.

    REASONING_EFFORTS = {"none", "low", "medium", "high", "xhigh"}

    def __init__(self):
        self.name = "SAP Orch - "
        self.valves = self.Valves()
        self.logger = logging.getLogger(__name__)
        self._usage_log_db_url = os.getenv("USAGE_LOG_DB_URL", "")
        self._usage_log_table = os.getenv("USAGE_LOG_TABLE", "aicore_usage_log")
        self._usage_table_ready = False
        self._background_tasks: set = set()  # strong refs so fire-and-forget tasks aren't garbage-collected
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._deployment_url: Optional[str] = None
        self._configuration_id: Optional[str] = None
        self._model_cache: Optional[List[dict]] = None
        self._model_lookup: Dict[str, Tuple[str, str]] = {}  # short id -> (real SAP name, version)
        self._model_cache_ts: float = 0.0
        self._session: Optional[aiohttp.ClientSession] = None  # reused across turns; see _http_session()

    async def on_startup(self):
        self.logger.info(f"on_startup:{__name__}")
        # Warm the model cache so the first pipes() call doesn't block the
        # event loop. Errors are swallowed by get_models().
        await asyncio.to_thread(self.get_models)

    async def on_shutdown(self):
        self.logger.info(f"on_shutdown:{__name__}")
        self._access_token = None
        self._token_expiry = 0.0
        if self._session and not self._session.closed:
            await self._session.close()

    async def _http_session(self) -> aiohttp.ClientSession:
        """Reused across turns to keep connections alive instead of a new TLS handshake per request."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(trust_env=True)
        return self._session

    # ── Service key / credentials ─────────────────────────────────────────────

    def _service_key(self) -> dict:
        """Parse AICORE_SERVICE_KEY, unwrapping the VCAP-style "credentials" nesting."""
        raw = (self.valves.AICORE_SERVICE_KEY or "").strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"AICORE_SERVICE_KEY is not valid JSON: {e}")
        if not isinstance(data, dict):
            raise ValueError("AICORE_SERVICE_KEY must be a JSON object.")
        nested = data.get("credentials")
        return {**nested, **data} if isinstance(nested, dict) else data

    def _credentials(self) -> Tuple[str, str, str, str]:
        """Resolve (auth_url, client_id, client_secret, api_url) from the pasted service key."""
        sk = self._service_key()
        auth_url = sk.get("url", "")
        client_id = sk.get("clientid", "")
        client_secret = sk.get("clientsecret", "")
        api_url = (sk.get("serviceurls") or {}).get("AI_API_URL", "")

        # Validated here, not in Valves: OpenWebUI instantiates Valves at load
        # time, before an admin has entered anything.
        missing = [
            n for n, v in (
                ("url", auth_url),
                ("clientid", client_id),
                ("clientsecret", client_secret),
                ("serviceurls.AI_API_URL", api_url),
            ) if not v
        ]
        if missing:
            raise ValueError(
                "SAP AI Core service key is not configured or is missing "
                + ", ".join(missing)
                + ". Paste the full service key JSON into the AICORE_SERVICE_KEY valve."
            )
        return auth_url.rstrip("/"), client_id, client_secret, api_url.rstrip("/")

    # ── HTTP plumbing ─────────────────────────────────────────────────────────

    def _proxies(self) -> Optional[dict]:
        p = {}
        if self.valves.HTTP_PROXY:
            p["http"] = self.valves.HTTP_PROXY
        if self.valves.HTTPS_PROXY:
            p["https"] = self.valves.HTTPS_PROXY
        return p or None

    def _aiohttp_proxy(self) -> Optional[str]:
        """aiohttp takes a single proxy per request, unlike requests' scheme map."""
        return self.valves.HTTPS_PROXY or self.valves.HTTP_PROXY or None

    def _get_access_token(self) -> str:
        """OAuth2 client-credentials token, cached until 5 min before expiry."""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        auth_url, client_id, client_secret, _ = self._credentials()
        resp = requests.post(
            f"{auth_url}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
            verify=self.valves.VERIFY_SSL,
            proxies=self._proxies(),
        )
        if resp.status_code in (400, 401):
            # The service key itself is stale or revoked; not fixable client-side.
            raise RuntimeError(
                f"SAP AI Core token request rejected ({resp.status_code}): {resp.text}. "
                "The service key is likely stale or revoked — regenerate it in the BTP cockpit."
            )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600) - 300
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "AI-Resource-Group": self.valves.AICORE_RESOURCE_GROUP,
            "Content-Type": "application/json",
        }

    def _api_get(self, path: str) -> dict:
        _, _, _, api_url = self._credentials()
        resp = requests.get(
            f"{api_url}/v2{path}",
            headers=self._headers(),
            timeout=30,
            verify=self.valves.VERIFY_SSL,
            proxies=self._proxies(),
        )
        resp.raise_for_status()
        return resp.json()

    # ── Discovery ─────────────────────────────────────────────────────────────

    def _discover_orchestration(self) -> Tuple[str, Optional[str]]:
        """Find the RUNNING orchestration deployment; returns (deploymentUrl, configurationId)."""
        if self._deployment_url:
            return self._deployment_url, self._configuration_id

        resources = self._api_get("/lm/deployments?$top=10000&$skip=0").get("resources", [])
        chosen = next(
            (d for d in resources if d.get("scenarioId") == "orchestration" and d.get("status") == "RUNNING"),
            None,
        )
        if not chosen:
            raise RuntimeError(
                "No RUNNING deployment with scenarioId='orchestration' found in resource group "
                f"'{self.valves.AICORE_RESOURCE_GROUP}'."
            )

        deployment_url = chosen.get("deploymentUrl")
        if not deployment_url:
            raise RuntimeError(f"Deployment {chosen.get('id')} has no deploymentUrl.")

        self._deployment_url = deployment_url.rstrip("/")
        self._configuration_id = chosen.get("configurationId")
        return self._deployment_url, self._configuration_id

    def _parse_model_filter(self, raw: Any) -> List[dict]:
        """
        Extract [{name, version}] from a modelFilterList parameter binding.

        The value is a JSON string decoding either to the wrapper object
        {"modelFilterListType": "allow", "modelFilterList": [...]} or straight
        to the entry list. A "deny" list yields nothing rather than being inverted.
        """
        value = raw
        for _ in range(2):  # double-encoded: may need unwrapping twice
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    return []
            else:
                break

        entries = value
        if isinstance(value, dict):
            if str(value.get("modelFilterListType", "allow")).lower() != "allow":
                self.logger.warning("modelFilterList is not an allow-list; no models discovered.")
                return []
            entries = value.get("modelFilterList") or value.get("models") or []
        if not isinstance(entries, list):
            return []

        models = []
        for entry in entries:
            if isinstance(entry, str):
                models.append({"name": entry, "version": "latest"})
                continue
            if not isinstance(entry, dict):
                continue
            name = entry.get("modelName") or entry.get("name")
            if not name:
                continue
            versions = entry.get("modelVersions") or entry.get("versions") or []
            if isinstance(versions, str):
                versions = [versions]
            version = "latest" if "latest" in versions else (versions[0] if versions else "latest")
            models.append({"name": name, "version": version})
        return models

    def _discover_models(self) -> List[dict]:
        """Read the deployment's configuration to get the real, current allow-list."""
        _, configuration_id = self._discover_orchestration()
        if not configuration_id:
            self.logger.warning("Deployment has no configurationId; cannot read modelFilterList.")
            return []

        config = self._api_get(f"/lm/configurations/{configuration_id}")
        for binding in config.get("parameterBindings", []) or []:
            if binding.get("key") == "modelFilterList":
                return self._parse_model_filter(binding.get("value"))
        self.logger.warning("No modelFilterList parameter binding on the orchestration configuration.")
        return []

    def _short_model_name(self, name: str) -> str:
        """Strip a SAP "<vendor>--" prefix (e.g. "anthropic--") for display only; model.name always carries the real name."""
        return re.sub(r"^[a-zA-Z0-9]+--", "", name)

    def get_models(self) -> List[dict]:
        """Selectable models, cached for MODEL_CACHE_TTL. Only successful lookups are cached."""
        if (
            self._model_cache is not None
            and self.valves.MODEL_CACHE_TTL > 0
            and (time.time() - self._model_cache_ts) < self.valves.MODEL_CACHE_TTL
        ):
            return self._model_cache

        try:
            # Re-resolve the deployment too, so a redeployed endpoint doesn't stay cached forever.
            self._deployment_url = None
            self._configuration_id = None
            models = self._discover_models()
        except Exception as e:
            self.logger.error(f"SAP AI Core model discovery failed: {e}")
            return self._model_cache or []

        # Use short names only when unambiguous within this allow-list.
        short_counts: Dict[str, int] = {}
        for m in models:
            s = self._short_model_name(m["name"])
            short_counts[s] = short_counts.get(s, 0) + 1

        result: List[dict] = []
        lookup: Dict[str, Tuple[str, str]] = {}
        for m in models:
            short = self._short_model_name(m["name"])
            base = short if short_counts[short] == 1 else m["name"]
            mid = base if m["version"] == "latest" else f"{base}:{m['version']}"
            result.append({
                "id": mid,
                "name": base if m["version"] == "latest" else f"{base} ({m['version']})",
            })
            lookup[mid] = (m["name"], m["version"])

        self._model_cache = result
        self._model_lookup = lookup
        self._model_cache_ts = time.time()
        return result

    def pipes(self) -> List[dict]:
        return self.get_models()

    def _resolve_model(self, model_param: str) -> Tuple[str, str]:
        """
        Turn OpenWebUI's "<pipe_id>.<our id>" back into the real (SAP model
        name, version). Splitting on "." is unsafe since model names contain
        dots, so match against published ids first, then strip one prefix.
        """
        self.get_models()
        known = set(self._model_lookup.keys())
        candidate = model_param
        if candidate not in known:
            match = next((k for k in known if candidate.endswith(f".{k}")), None)
            candidate = match or (candidate.split(".", 1)[1] if "." in candidate else candidate)

        if candidate in self._model_lookup:
            return self._model_lookup[candidate]

        # Not in the current allow-list (e.g. a since-removed model): best-effort parse.
        name, _, version = candidate.partition(":")
        return name, (version or "latest")

    # ── Message / payload construction ────────────────────────────────────────

    def _inject_uploaded_files(self, messages: List[dict], body: dict) -> List[dict]:
        """
        Fold OpenWebUI's already-extracted upload text (body["metadata"]["files"])
        into the newest user message. SAP's own file block 400s on anything but
        images/PDF, so this is the only way docx/pptx/xlsx/csv reach the model.
        Files with empty extracted content (e.g. scanned PDFs) are skipped.
        """
        files = []
        for item in (body.get("metadata") or {}).get("files") or []:
            if not isinstance(item, dict) or item.get("type") != "file":
                continue
            f = item.get("file") or {}
            content = ((f.get("data") or {}).get("content") or "").strip()
            if not content:
                continue
            filename = f.get("filename") or item.get("name") or "uploaded file"
            cap = self.valves.UPLOADED_FILE_MAX_CHARS
            if len(content) > cap:
                content = content[:cap] + f"\n...[truncated, {len(content) - cap} more characters]"
            files.append(f"### {filename}\n\n{content}")
        if not files or not messages:
            return messages

        out = list(messages)
        for i in range(len(out) - 1, -1, -1):
            if out[i].get("role") != "user":
                continue
            entry = out[i]
            addition = "\n\n---\nAttached file contents:\n\n" + "\n\n---\n\n".join(files)
            content = entry.get("content", "")
            if isinstance(content, list):
                out[i] = {**entry, "content": content + [{"type": "text", "text": addition}]}
            else:
                out[i] = {**entry, "content": f"{content}{addition}"}
            break
        return out

    def _normalize_text(self, content: Any) -> str:
        """Flatten any content shape to a plain string."""
        if isinstance(content, str):
            return content
        if isinstance(content, dict) and "text" in content:
            return content["text"]
        if isinstance(content, list):
            parts = [
                item["text"] if isinstance(item, dict) and "text" in item else item
                for item in content
                if isinstance(item, (str, dict))
            ]
            return "\n".join(p for p in parts if isinstance(p, str) and p)
        return ""

    def _is_anthropic_model(self, model_name: str) -> bool:
        """Only Claude models ("anthropic--" prefix) take cache_control; GPT/Gemini cache automatically."""
        return model_name.lower().startswith("anthropic--")

    def _template_messages(self, messages: List[dict], cache_system: bool = False, cache_history: bool = False) -> List[dict]:
        """
        Build the prompt_templating `template` — OpenAI-shaped messages.
        Multimodal user content passes through untouched so a rejection fails
        loudly instead of silently dropping images.

        cache_system puts an Anthropic cache_control breakpoint on the LAST
        system message (which covers every earlier one). cache_history puts one
        on the last user/assistant message BEFORE the newest, so the growing
        conversation is cached. Never on a "tool" message: SAP 400s when tool
        content becomes an array.
        """
        out = []
        last_system_idx = None
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            if role == "assistant":
                entry: dict = {"role": "assistant", "content": self._normalize_text(content)}
                if msg.get("tool_calls"):
                    entry["tool_calls"] = msg["tool_calls"]
                out.append(entry)
            elif role == "tool":
                if not msg.get("tool_call_id"):
                    continue
                out.append({
                    "role": "tool",
                    "tool_call_id": msg["tool_call_id"],
                    "content": self._normalize_text(content),
                })
            elif role == "user" and isinstance(content, list):
                out.append({"role": "user", "content": content})
            else:
                out.append({"role": role, "content": self._normalize_text(content)})
            if role == "system":
                last_system_idx = len(out) - 1

        if cache_system and last_system_idx is not None:
            out[last_system_idx] = {
                "role": "system",
                "content": [{
                    "type": "text",
                    "text": out[last_system_idx]["content"],
                    "cache_control": {"type": "ephemeral"},
                }],
            }

        if cache_history:
            candidates = [
                i for i, m in enumerate(out)
                if i != len(out) - 1 and m.get("role") in ("user", "assistant")
            ]
            if candidates:
                target = candidates[-1]
                entry = out[target]
                content = entry.get("content")
                if isinstance(content, list):
                    blocks = [dict(b) if isinstance(b, dict) else {"type": "text", "text": str(b)} for b in content]
                    if not blocks:
                        blocks = [{"type": "text", "text": ""}]
                else:
                    blocks = [{"type": "text", "text": content}]
                blocks[-1] = {**blocks[-1], "cache_control": {"type": "ephemeral"}}
                out[target] = {**entry, "content": blocks}

        return out

    def _tool_specs(self, client_tools: dict, body: dict) -> List[dict]:
        """
        Collect OpenAI-schema tool definitions from OpenWebUI's __tools__ and
        from body["tools"]. __tools__ wins on a name collision since this pipe
        can execute those itself.
        """
        specs: Dict[str, dict] = {}
        for tool in body.get("tools") or []:
            if tool.get("type") == "function" and tool.get("function", {}).get("name"):
                specs[tool["function"]["name"]] = tool
        for name, entry in (client_tools or {}).items():
            spec = (entry or {}).get("spec") or {}
            specs[name] = {
                "type": "function",
                "function": {
                    "name": name,
                    "description": spec.get("description", ""),
                    "parameters": spec.get("parameters") or {"type": "object", "properties": {}},
                },
            }
        return list(specs.values())

    def _model_params(self, body: dict, has_tools: bool, model_name: str) -> dict:
        """
        Build model.params. Reasoning controls are omitted when tools are
        present (SAP rejects the combination), and temperature=0 is omitted for
        gpt-5-family models (SAP 400s on it).
        """
        params = {}
        for key in ("temperature", "top_p", "max_tokens", "frequency_penalty", "presence_penalty", "seed", "stop"):
            if body.get(key) is None:
                continue
            if key == "temperature" and body[key] == 0 and model_name.lower().startswith("gpt-5"):
                self.logger.info(f"Dropping temperature=0 for {model_name}: SAP rejects it for gpt-5-family models.")
                continue
            params[key] = body[key]
        if body.get("response_format"):
            params["response_format"] = body["response_format"]

        if has_tools:
            if body.get("reasoning_effort") or body.get("verbosity"):
                self.logger.info("Dropping reasoning_effort/verbosity: SAP rejects them alongside tools.")
            return params

        effort = str(body.get("reasoning_effort") or "").strip().lower()
        if effort:
            if effort in self.REASONING_EFFORTS:
                params["reasoning_effort"] = effort
            else:
                self.logger.warning(
                    f"Ignoring invalid reasoning_effort '{effort}'; valid values: {sorted(self.REASONING_EFFORTS)}."
                )
        verbosity = str(body.get("verbosity") or "").strip().lower()
        if verbosity:
            params["verbosity"] = verbosity
        return params

    def _build_payload(
        self, model_name: str, model_version: str, messages: List[dict], body: dict,
        tool_specs: List[dict], stream: bool,
    ) -> dict:
        cache_claude = self._is_anthropic_model(model_name)
        prompt: dict = {"template": self._template_messages(messages, cache_system=cache_claude, cache_history=cache_claude)}
        if tool_specs:
            if cache_claude:
                # One breakpoint on the last tool covers the whole tool list.
                tool_specs = tool_specs[:-1] + [{**tool_specs[-1], "cache_control": {"type": "ephemeral"}}]
            prompt["tools"] = tool_specs
            # SAP's prompt schema 400s on tool_choice regardless of value.
            if body.get("tool_choice"):
                self.logger.info(f"Dropping tool_choice={body['tool_choice']!r}: SAP's prompt schema rejects it.")

        config: dict = {
            "modules": {
                "prompt_templating": {
                    "prompt": prompt,
                    "model": {
                        "name": model_name,
                        "version": model_version,
                        "params": self._model_params(body, bool(tool_specs), model_name),
                    },
                }
            }
        }
        if stream:
            config["stream"] = {"enabled": True}  # sibling of modules, not inside one
            if self.valves.STREAM_CHUNK_SIZE:
                config["stream"]["chunk_size"] = self.valves.STREAM_CHUNK_SIZE
        return {"config": config}

    # ── Response handling ─────────────────────────────────────────────────────

    def _completion(self, obj: dict) -> dict:
        """Unwrap the orchestration envelope to the OpenAI-shaped completion."""
        if not isinstance(obj, dict):
            return {}
        for key in ("final_result", "orchestration_result"):
            inner = obj.get(key)
            if isinstance(inner, dict):
                return inner
        intermediate = obj.get("intermediate_results")
        if isinstance(intermediate, dict) and isinstance(intermediate.get("llm"), dict):
            return intermediate["llm"]
        return obj

    def _merge_tool_call_deltas(self, acc: Dict[int, dict], deltas: List[dict]) -> None:
        """Accumulate streamed OpenAI tool_call fragments, keyed by their index."""
        for delta in deltas or []:
            idx = delta.get("index", 0)
            call = acc.setdefault(idx, {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
            if delta.get("id"):
                call["id"] = delta["id"]
            fn = delta.get("function") or {}
            if fn.get("name"):
                call["function"]["name"] = fn["name"]
            if fn.get("arguments"):
                call["function"]["arguments"] += fn["arguments"]

    def _record_usage(self, completion: dict, state: dict) -> None:
        """Keep the last non-empty usage block (sent once, at the end)."""
        usage = completion.get("usage")
        if isinstance(usage, dict) and usage:
            state["usage"] = usage

    def _ensure_usage_table(self, cur, sql) -> None:
        """CREATE TABLE IF NOT EXISTS, once per process; created_at is epoch seconds."""
        if self._usage_table_ready:
            return
        table = sql.Identifier(self._usage_log_table)
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "id BIGSERIAL PRIMARY KEY, created_at BIGINT NOT NULL, "
                "user_id TEXT, user_email TEXT, chat_id TEXT, model_id TEXT, "
                "model_name TEXT, model_version TEXT, "
                "input_tokens BIGINT, output_tokens BIGINT, reasoning_tokens BIGINT, "
                "cached_tokens BIGINT, cache_creation_tokens BIGINT, total_tokens BIGINT, "
                "total_ms INTEGER, ttfb_ms INTEGER, sap_ms INTEGER, tool_ms INTEGER, "
                "auth_ms INTEGER, discovery_ms INTEGER)"
            ).format(table)
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (created_at)").format(
                sql.Identifier(f"{self._usage_log_table}_created_at_idx"), table
            )
        )
        self._usage_table_ready = True

    def _log_usage_row(self, **fields) -> None:
        """Best-effort INSERT; a DB failure is logged and never breaks the chat turn."""
        conn = None
        try:
            import psycopg2
            from psycopg2 import sql

            conn = psycopg2.connect(self._usage_log_db_url, connect_timeout=5)
            with conn, conn.cursor() as cur:
                self._ensure_usage_table(cur, sql)
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (created_at, user_id, user_email, chat_id, model_id, "
                        "model_name, model_version, input_tokens, output_tokens, reasoning_tokens, "
                        "cached_tokens, cache_creation_tokens, total_tokens, "
                        "total_ms, ttfb_ms, sap_ms, tool_ms, auth_ms, discovery_ms) "
                        "VALUES (%(created_at)s, %(user_id)s, %(user_email)s, %(chat_id)s, %(model_id)s, "
                        "%(model_name)s, %(model_version)s, %(input_tokens)s, %(output_tokens)s, "
                        "%(reasoning_tokens)s, %(cached_tokens)s, %(cache_creation_tokens)s, %(total_tokens)s, "
                        "%(total_ms)s, %(ttfb_ms)s, %(sap_ms)s, %(tool_ms)s, %(auth_ms)s, %(discovery_ms)s)"
                    ).format(sql.Identifier(self._usage_log_table)),
                    fields,
                )
        except Exception as e:
            self.logger.error(f"usage log insert failed: {e}")
        finally:
            if conn is not None:
                conn.close()

    def _log_usage_background(self, **fields) -> None:
        """Fire-and-forget so a slow database never delays the reply."""
        task = asyncio.create_task(asyncio.to_thread(self._log_usage_row, **fields))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _request(self, session: aiohttp.ClientSession, payload: dict, timing: dict) -> aiohttp.ClientResponse:
        t0 = time.monotonic()
        deployment_url, _ = await asyncio.to_thread(self._discover_orchestration)
        timing["discovery_ms"] = timing.get("discovery_ms", 0) + (time.monotonic() - t0) * 1000
        t1 = time.monotonic()
        headers = await asyncio.to_thread(self._headers)
        # Near zero when the token is cached; a high value means a cold worker or a token refresh.
        timing["auth_ms"] = timing.get("auth_ms", 0) + (time.monotonic() - t1) * 1000
        resp = await session.post(
            f"{deployment_url}/v2/completion",
            headers=headers,
            json=payload,
            ssl=None if self.valves.VERIFY_SSL else False,
            proxy=self._aiohttp_proxy(),
            timeout=aiohttp.ClientTimeout(total=self.valves.REQUEST_TIMEOUT),
        )
        if resp.status != 200:
            body_text = await resp.text()
            resp.release()
            raise RuntimeError(f"SAP AI Core error ({resp.status}): {body_text}")
        return resp

    async def _complete(self, session: aiohttp.ClientSession, payload: dict, state: dict, timing: dict) -> AsyncGenerator:
        """Non-streaming call: yield the whole message, record tool_calls/usage in state."""
        resp = await self._request(session, payload, timing)
        async with resp:
            data = await resp.json(content_type=None)

        completion = self._completion(data)
        self._record_usage(completion, state)
        choices = completion.get("choices") or []
        if not choices:
            raise RuntimeError(f"SAP AI Core returned no choices: {json.dumps(data)[:500]}")

        message = choices[0].get("message") or {}
        if message.get("tool_calls"):
            state["tool_calls"] = message["tool_calls"]
        content = self._normalize_text(message.get("content", ""))
        if content:
            yield content

    async def _iter_sse_lines(self, content: aiohttp.StreamReader) -> AsyncGenerator:
        """
        Split on b"\\n" manually: `async for line in content` has a fixed
        per-line byte cap, and SAP echoes intermediate_results.templating as
        one huge line that grows with conversation size.
        """
        buf = b""
        async for chunk in content.iter_any():
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                yield line
        if buf:
            yield buf

    async def _stream(self, session: aiohttp.ClientSession, payload: dict, state: dict, timing: dict) -> AsyncGenerator:
        """Streaming call: yield text deltas, accumulate tool_calls/usage into state."""
        resp = await self._request(session, payload, timing)
        tool_calls: Dict[int, dict] = {}

        async with resp:
            async for raw in self._iter_sse_lines(resp.content):
                line = raw.decode("utf-8").strip()
                if not line or line.startswith(":"):
                    continue
                data = line[6:].strip() if line.startswith("data:") else line
                if data == "[DONE]":
                    break
                try:
                    chunk = json.loads(data)
                except json.JSONDecodeError:
                    continue

                completion = self._completion(chunk)
                self._record_usage(completion, state)
                if isinstance(chunk, dict) and chunk.get("usage"):
                    self._record_usage(chunk, state)

                choices = completion.get("choices") or []
                if not choices:
                    continue
                choice = choices[0]
                # Streamed chunks carry "delta"; a single terminal chunk may carry "message".
                delta = choice.get("delta") or choice.get("message") or {}

                text = self._normalize_text(delta.get("content") or "")
                if text:
                    yield text
                if delta.get("tool_calls"):
                    self._merge_tool_call_deltas(tool_calls, delta["tool_calls"])

        if tool_calls:
            state["tool_calls"] = [tool_calls[i] for i in sorted(tool_calls)]

    async def _call_client_tool(self, entry: dict, args: dict) -> str:
        """Invoke one OpenWebUI tool callable (sync or async) and stringify the result."""
        fn = (entry or {}).get("callable")
        if fn is None:
            raise RuntimeError("Tool has no callable.")
        result = fn(**args)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, str) else json.dumps(result, default=str)

    # ── Main pipeline ─────────────────────────────────────────────────────────

    async def pipe(
        self,
        body: dict,
        __metadata__: dict = None,
        __task__: str = None,
        __event_emitter__=None,
        __user__: dict = None,
        __tools__: dict = None,
    ) -> AsyncGenerator:
        """Run one chat turn against the orchestration deployment."""
        if __task__ is not None:
            return

        async def emit_status(description: str, done: bool = False):
            if __event_emitter__:
                await __event_emitter__({"type": "status", "data": {"description": description, "done": done}})

        chatid = (__metadata__ or {}).get("chat_id", "unknown")
        user_email = (__user__ or {}).get("email", "unknown")

        messages = body.get("messages") or []
        if not messages:
            yield "Error: No messages provided."
            return

        client_tools = __tools__ or {}
        usage_totals = {
            "input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0,
            "cached_tokens": 0, "cache_creation_tokens": 0,
        }

        try:
            model_name, model_version = await asyncio.to_thread(self._resolve_model, body.get("model", ""))
            if not model_name:
                yield "Error: No model selected."
                return

            tool_specs = self._tool_specs(client_tools, body)
            stream = body.get("stream", True)
            turn_messages = self._inject_uploaded_files(list(messages), body)
            self.logger.info(
                f"pipe invoked | user={user_email} | chat={chatid} | "
                f"model={model_name}:{model_version} | tools={len(tool_specs)}"
            )

            await emit_status(f"Calling {model_name}...")

            turn_start = time.monotonic()
            first_token_at = None
            timing = {"sap_ms": 0.0, "tool_ms": 0.0}

            session = await self._http_session()
            for _ in range(self.valves.TOOL_LOOP_MAX_ITERATIONS):
                payload = self._build_payload(
                    model_name, model_version, turn_messages, body, tool_specs, stream
                )
                state: dict = {}
                iter_start = time.monotonic()
                runner = (
                    self._stream(session, payload, state, timing) if stream
                    else self._complete(session, payload, state, timing)
                )
                async for text in runner:
                    if first_token_at is None:
                        first_token_at = time.monotonic()
                    yield text
                timing["sap_ms"] += (time.monotonic() - iter_start) * 1000

                usage = state.get("usage") or {}
                usage_totals["output_tokens"] += usage.get("completion_tokens", 0)
                usage_totals["reasoning_tokens"] += (usage.get("completion_tokens_details") or {}).get("reasoning_tokens", 0)
                prompt_tokens = usage.get("prompt_tokens", 0)
                details = usage.get("prompt_tokens_details") or {}
                cached = details.get("cached_tokens", 0) or 0
                created = details.get("cache_creation_tokens", 0) or 0
                usage_totals["cached_tokens"] += cached
                usage_totals["cache_creation_tokens"] += created
                if self._is_anthropic_model(model_name):
                    # Anthropic: prompt_tokens EXCLUDES cached/cache-creation tokens.
                    usage_totals["input_tokens"] += prompt_tokens
                else:
                    # OpenAI shape: cached_tokens is a subset of prompt_tokens.
                    usage_totals["input_tokens"] += prompt_tokens - cached

                tool_calls = state.get("tool_calls")
                if not tool_calls:
                    break

                unresolvable = [c for c in tool_calls if c.get("function", {}).get("name") not in client_tools]
                if unresolvable:
                    # Declared via body["tools"] with no callable here — hand back to the caller.
                    self.logger.info(f"Returning {len(unresolvable)} unexecutable tool call(s) to the caller.")
                    if stream:
                        yield {"choices": [{"index": 0, "delta": {"tool_calls": tool_calls}, "finish_reason": "tool_calls"}]}
                    else:
                        # A non-streaming Pipe can't carry structured tool_calls back.
                        fn_names = ", ".join(c.get("function", {}).get("name", "?") for c in tool_calls)
                        yield (
                            f"_(This turn wants to call {fn_names}, but tool-calling through this pipe "
                            "requires stream=true — a non-streaming request can't carry structured "
                            "tool_calls back to the caller.)_"
                        )
                    break

                turn_messages = turn_messages + [{"role": "assistant", "content": "", "tool_calls": tool_calls}]
                for call in tool_calls:
                    fn = call.get("function", {})
                    name = fn.get("name", "")
                    await emit_status(f"⚙️ Calling {name}...")
                    try:
                        args = json.loads(fn.get("arguments") or "{}")
                    except json.JSONDecodeError:
                        args = {}
                    tool_start = time.monotonic()
                    try:
                        result = await self._call_client_tool(client_tools.get(name), args)
                    except Exception as tool_err:
                        self.logger.error(f"Client tool {name} failed | user={user_email} | error={tool_err}")
                        result = f"Error: {tool_err}"
                    timing["tool_ms"] += (time.monotonic() - tool_start) * 1000
                    turn_messages.append({
                        "role": "tool",
                        "tool_call_id": call.get("id", ""),
                        "name": name,
                        "content": result,
                    })
                await emit_status("✅ Tool returned results")
            else:
                self.logger.warning(
                    f"Tool loop hit TOOL_LOOP_MAX_ITERATIONS={self.valves.TOOL_LOOP_MAX_ITERATIONS} | "
                    f"user={user_email} | chat={chatid}"
                )
                yield "\n\n_(Stopped: too many tool calls in a row.)_"

            # Full volume including cache traffic; omitting it would undercount real usage.
            total_tokens = (
                usage_totals["input_tokens"] + usage_totals["output_tokens"]
                + usage_totals["cached_tokens"] + usage_totals["cache_creation_tokens"]
            )
            if self.valves.ENABLE_TOKEN_LOGGING and total_tokens:
                total_ms = (time.monotonic() - turn_start) * 1000
                ttfb_ms = (first_token_at - turn_start) * 1000 if first_token_at else None
                self.logger.info(
                    f"token usage | user={user_email} | chat={chatid} | "
                    f"input={usage_totals['input_tokens']} | output={usage_totals['output_tokens']} | "
                    f"reasoning={usage_totals['reasoning_tokens']} | cached={usage_totals['cached_tokens']} | "
                    f"cache_creation={usage_totals['cache_creation_tokens']} | total={total_tokens} | "
                    f"total_ms={total_ms:.0f} | ttfb_ms={ttfb_ms if ttfb_ms is None else f'{ttfb_ms:.0f}'} | "
                    f"sap_ms={timing['sap_ms']:.0f} | tool_ms={timing['tool_ms']:.0f} | "
                    f"auth_ms={timing.get('auth_ms', 0):.0f} | discovery_ms={timing.get('discovery_ms', 0):.0f}"
                )
                if self._usage_log_db_url:
                    self._log_usage_background(
                        created_at=int(time.time()),
                        user_id=(__user__ or {}).get("id", ""),
                        user_email=user_email,
                        chat_id=chatid,
                        model_id=body.get("model", ""),
                        model_name=model_name,
                        model_version=model_version,
                        input_tokens=usage_totals["input_tokens"],
                        output_tokens=usage_totals["output_tokens"],
                        reasoning_tokens=usage_totals["reasoning_tokens"],
                        cached_tokens=usage_totals["cached_tokens"],
                        cache_creation_tokens=usage_totals["cache_creation_tokens"],
                        total_tokens=total_tokens,
                        total_ms=round(total_ms),
                        ttfb_ms=round(ttfb_ms) if ttfb_ms is not None else None,
                        sap_ms=round(timing["sap_ms"]),
                        tool_ms=round(timing["tool_ms"]),
                        auth_ms=round(timing.get("auth_ms", 0)),
                        discovery_ms=round(timing.get("discovery_ms", 0)),
                    )
            if stream and total_tokens:
                # Yielded (not emitted) so OpenWebUI persists it; streaming only,
                # since a non-streaming Pipe would str() the dict into the reply.
                # "choices": [] is required for strict OpenAI-client validation.
                # Both key sets are emitted since some clients read only prompt_/completion_tokens.
                prompt_total = usage_totals["input_tokens"] + usage_totals["cached_tokens"] + usage_totals["cache_creation_tokens"]
                yield {
                    "choices": [],
                    "usage": {
                        "input_tokens": prompt_total,
                        "output_tokens": usage_totals["output_tokens"],
                        "prompt_tokens": prompt_total,
                        "completion_tokens": usage_totals["output_tokens"],
                        "total_tokens": total_tokens,
                        "cached_tokens": usage_totals["cached_tokens"],
                        "cache_creation_tokens": usage_totals["cache_creation_tokens"],
                    }
                }

            await emit_status("", done=True)

        except Exception as e:
            self.logger.error(f"SAP AI Core request error | user={user_email} | chat={chatid} | error={e}")
            await emit_status("Error", done=True)
            if __event_emitter__:
                await __event_emitter__({"type": "notification", "data": {"type": "error", "content": str(e)}})
            yield f"Error: {str(e)}"
