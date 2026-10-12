"""
title: SAP AI Core Orchestration
description: Chat pipe for SAP AI Core's orchestration deployment. Auto-discovers allow-listed models, streams, supports OpenWebUI Tools, images/PDF, file uploads and Claude prompt caching.
author: Smith Chang
version: 1.4.0
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
import datetime
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

        # Per-user monthly token cap in raw tokens (e.g. 100000000 = 100 million),
        # cache tokens weighted by billing rate, checked before each turn. 0 = off.
        # Reads the usage table, so it needs ENABLE_TOKEN_LOGGING and USAGE_LOG_DB_URL;
        # without them, or if the database is unreachable, turns are allowed.
        MONTHLY_TOKEN_QUOTA: int = int(os.getenv("MONTHLY_TOKEN_QUOTA", 0))
        # JSON object {"email": limit}; a limit of 0 means unlimited for that user.
        MONTHLY_TOKEN_QUOTA_OVERRIDES: str = os.getenv("MONTHLY_TOKEN_QUOTA_OVERRIDES", "")
        QUOTA_EXEMPT_ADMINS: bool = os.getenv("QUOTA_EXEMPT_ADMINS", "true").lower() == "true"
        QUOTA_WARN_PERCENT: int = int(os.getenv("QUOTA_WARN_PERCENT", 80))  # 0 disables the near-limit notice

    REASONING_EFFORTS = {"none", "low", "medium", "high", "xhigh"}
    NATIVE_TOOLS = {"sap_ai_core_usage_summary"}  # pipe-native, admin-only; see pipe()'s tool loop

    def __init__(self):
        self.name = "SAP Orch - "
        self.valves = self.Valves()
        self.logger = logging.getLogger(__name__)
        self._usage_log_db_url = os.getenv("USAGE_LOG_DB_URL", "")
        self._usage_log_table = os.getenv("USAGE_LOG_TABLE", "aicore_usage_log")
        self._usage_table_ready = False
        self._quota_cache: Dict[str, Tuple[str, int, float]] = {}  # email -> (month key, used, fetched at)
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
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (user_email, created_at)").format(
                sql.Identifier(f"{self._usage_log_table}_user_created_idx"), table
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

    QUOTA_CACHE_TTL = 60  # seconds a month-to-date total is reused per user
    # Cache reads bill at ~0.1x and cache writes at ~1.25x, so the quota weights them.
    QUOTA_CACHED_WEIGHT = 0.1
    QUOTA_CACHE_CREATION_WEIGHT = 1.25

    def _quota_tokens(self, input_tokens: int, output_tokens: int, cached_tokens: int, cache_creation_tokens: int) -> int:
        return round(
            input_tokens + output_tokens
            + cached_tokens * self.QUOTA_CACHED_WEIGHT
            + cache_creation_tokens * self.QUOTA_CACHE_CREATION_WEIGHT
        )

    def _quota_limit(self, user_email: str) -> int:
        """Effective monthly limit for a user; 0 means unlimited."""
        raw = self.valves.MONTHLY_TOKEN_QUOTA_OVERRIDES.strip()
        if raw:
            try:
                overrides = json.loads(raw)
                if isinstance(overrides, dict) and user_email in overrides:
                    return max(int(overrides[user_email]), 0)
            except (ValueError, TypeError):
                self.logger.warning("MONTHLY_TOKEN_QUOTA_OVERRIDES is not a valid JSON object; ignoring")
        return max(self.valves.MONTHLY_TOKEN_QUOTA, 0)

    def _month_bounds(self) -> Tuple[str, int, int]:
        now = datetime.datetime.now(datetime.timezone.utc)
        start = datetime.datetime(now.year, now.month, 1, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(now.year + (now.month == 12), (now.month % 12) + 1, 1, tzinfo=datetime.timezone.utc)
        return f"{now.year:04d}-{now.month:02d}", int(start.timestamp()), int(end.timestamp())

    def _month_usage(self, user_email: str) -> Optional[int]:
        """Month-to-date weighted tokens for a user, cached briefly; None if unavailable (fail open)."""
        if not self._usage_log_db_url or not self.valves.ENABLE_TOKEN_LOGGING:
            return None
        month_key, start, end = self._month_bounds()
        cached = self._quota_cache.get(user_email)
        if cached and cached[0] == month_key and time.time() - cached[2] < self.QUOTA_CACHE_TTL:
            return cached[1]
        conn = None
        try:
            import psycopg2
            from psycopg2 import sql

            conn = psycopg2.connect(self._usage_log_db_url, connect_timeout=5)
            with conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT COALESCE(ROUND(SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0) "
                        "+ COALESCE(cached_tokens, 0) * %(cw)s + COALESCE(cache_creation_tokens, 0) * %(ccw)s)), 0) "
                        "FROM {} WHERE user_email = %(user_email)s AND created_at >= %(start)s AND created_at < %(end)s"
                    ).format(sql.Identifier(self._usage_log_table)),
                    {
                        "user_email": user_email, "start": start, "end": end,
                        "cw": self.QUOTA_CACHED_WEIGHT, "ccw": self.QUOTA_CACHE_CREATION_WEIGHT,
                    },
                )
                used = int(cur.fetchone()[0])
        except Exception as e:
            self.logger.error(f"quota lookup failed (allowing turn): {e}")
            return None
        finally:
            if conn is not None:
                conn.close()
        self._quota_cache[user_email] = (month_key, used, time.time())
        return used

    def _add_quota_usage(self, user_email: str, tokens: int) -> None:
        """Bump the cached total so the next turn sees this one before the insert lands."""
        cached = self._quota_cache.get(user_email)
        if cached and cached[0] == self._month_bounds()[0]:
            self._quota_cache[user_email] = (cached[0], cached[1] + tokens, cached[2])

    def _log_usage_background(self, **fields) -> None:
        """Fire-and-forget so a slow database never delays the reply."""
        task = asyncio.create_task(asyncio.to_thread(self._log_usage_row, **fields))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _usage_summary_tool_spec(self) -> dict:
        """Offered to the model only for admin users — see pipe()'s is_admin gate."""
        return {
            "type": "function",
            "function": {
                "name": "sap_ai_core_usage_summary",
                "description": (
                    "Admin-only. Query the SAP AI Core token usage ledger and summarize "
                    "requests and tokens by user and model for a given month, plus a per-day "
                    "breakdown (overall and per user, UTC days), and each user's monthly token quota "
                    "limit, usage, remaining and percent used."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "month": {
                            "type": "string",
                            "description": "Month as YYYY-MM. Defaults to the current month.",
                        },
                        "user_email": {
                            "type": "string",
                            "description": "Restrict to one user's email. Omit for all users.",
                        },
                    },
                },
            },
        }

    def _run_usage_summary(self, month: Optional[str], user_email: Optional[str]) -> str:
        """
        Sync (psycopg2) handler for the native usage-summary tool — run via
        asyncio.to_thread. Returns a JSON string (error or results) since this
        becomes a tool-role message content, not a user-facing reply directly.
        """
        if not self._usage_log_db_url:
            return json.dumps({"error": "Usage logging is not configured (USAGE_LOG_DB_URL is empty)."})

        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            year, mon = (int(p) for p in month.split("-", 1)) if month else (now.year, now.month)
        except (ValueError, AttributeError):
            return json.dumps({"error": f"Invalid month {month!r}; expected YYYY-MM."})
        start = datetime.datetime(year, mon, 1, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(year + (mon == 12), (mon % 12) + 1, 1, tzinfo=datetime.timezone.utc)

        conn = None
        try:
            import psycopg2
            from psycopg2 import sql

            conn = psycopg2.connect(self._usage_log_db_url, connect_timeout=5)
            with conn, conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT user_email, model_name, model_version, COUNT(*), SUM(input_tokens), SUM(output_tokens), "
                        "SUM(cached_tokens), SUM(cache_creation_tokens), SUM(total_tokens) "
                        "FROM {table} WHERE created_at >= %(start)s AND created_at < %(end)s {user_filter} "
                        "GROUP BY user_email, model_name, model_version "
                        "ORDER BY SUM(total_tokens) DESC, user_email, model_name, model_version"
                    ).format(
                        table=sql.Identifier(self._usage_log_table),
                        user_filter=sql.SQL("AND user_email = %(user_email)s") if user_email else sql.SQL(""),
                    ),
                    {"start": int(start.timestamp()), "end": int(end.timestamp()), "user_email": user_email},
                )
                rows = cur.fetchall()
                cur.execute(
                    sql.SQL(
                        "SELECT (to_timestamp(created_at) AT TIME ZONE 'UTC')::date, user_email, COUNT(*), "
                        "SUM(input_tokens), SUM(output_tokens), SUM(cached_tokens), "
                        "SUM(cache_creation_tokens), SUM(total_tokens) "
                        "FROM {table} WHERE created_at >= %(start)s AND created_at < %(end)s {user_filter} "
                        "GROUP BY 1, user_email ORDER BY 1, user_email"
                    ).format(
                        table=sql.Identifier(self._usage_log_table),
                        user_filter=sql.SQL("AND user_email = %(user_email)s") if user_email else sql.SQL(""),
                    ),
                    {"start": int(start.timestamp()), "end": int(end.timestamp()), "user_email": user_email},
                )
                day_rows = cur.fetchall()
        except Exception as e:
            return json.dumps({"error": f"Usage query failed: {e}"})
        finally:
            if conn is not None:
                conn.close()

        user_totals: Dict[str, dict] = {}
        model_totals: Dict[Tuple[str, str], dict] = {}
        for row in rows:
            user_email_value, model_name, model_version, requests_count, input_tokens, output_tokens, cached_tokens, cache_creation_tokens, total_tokens = row
            model = {"model_name": model_name, "model_version": model_version}
            counts = {
                "requests": requests_count or 0,
                "input_tokens": input_tokens or 0,
                "output_tokens": output_tokens or 0,
                "cached_tokens": cached_tokens or 0,
                "cache_creation_tokens": cache_creation_tokens or 0,
                "total_tokens": total_tokens or 0,
            }

            user = user_totals.setdefault(user_email_value, {"user_email": user_email_value, **{key: 0 for key in counts}, "models": []})
            for key, value in counts.items():
                user[key] += value
            user["models"].append({**model, **counts})

            model_key = (model_name, model_version)
            model_total = model_totals.setdefault(model_key, {**model, **{key: 0 for key in counts}})
            for key, value in counts.items():
                model_total[key] += value

        day_totals: Dict[str, dict] = {}
        for day, day_user, requests_count, input_tokens, output_tokens, cached_tokens, cache_creation_tokens, total_tokens in day_rows:
            counts = {
                "requests": requests_count or 0,
                "input_tokens": input_tokens or 0,
                "output_tokens": output_tokens or 0,
                "cached_tokens": cached_tokens or 0,
                "cache_creation_tokens": cache_creation_tokens or 0,
                "total_tokens": total_tokens or 0,
            }
            day_key = day.isoformat()
            entry = day_totals.setdefault(day_key, {"date": day_key, **{key: 0 for key in counts}, "users": []})
            for key, value in counts.items():
                entry[key] += value
            entry["users"].append({"user_email": day_user, **counts})

        for user in user_totals.values():
            limit = self._quota_limit(user["user_email"])
            used = self._quota_tokens(
                user["input_tokens"], user["output_tokens"], user["cached_tokens"], user["cache_creation_tokens"]
            )
            user["quota_limit"] = limit or None  # None = unlimited
            user["quota_used"] = used
            user["quota_remaining"] = max(limit - used, 0) if limit else None
            user["quota_used_percent"] = round(used * 100 / limit, 1) if limit else None

        return json.dumps({
            "month": f"{year:04d}-{mon:02d}",
            "quota_note": "quota_used is billing-weighted (cached x0.1, cache_creation x1.25), so it differs from total_tokens. Admins may be exempt (QUOTA_EXEMPT_ADMINS).",
            "days": sorted(day_totals.values(), key=lambda day: day["date"]),
            "users": sorted(user_totals.values(), key=lambda user: user["total_tokens"], reverse=True),
            "models": sorted(model_totals.values(), key=lambda model: model["total_tokens"], reverse=True),
        })

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
        is_admin = (__user__ or {}).get("role") == "admin"
        usage_totals = {
            "input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0,
            "cached_tokens": 0, "cache_creation_tokens": 0,
        }

        quota_limit = 0
        quota_used = None
        if not (is_admin and self.valves.QUOTA_EXEMPT_ADMINS):
            quota_limit = self._quota_limit(user_email)
        if quota_limit:
            quota_used = await asyncio.to_thread(self._month_usage, user_email)
            if quota_used is not None and quota_used >= quota_limit:
                month_key, _, end = self._month_bounds()
                reset = datetime.datetime.fromtimestamp(end, datetime.timezone.utc).strftime("%Y-%m-%d")
                self.logger.info(
                    f"quota exceeded | user={user_email} | chat={chatid} | used={quota_used} | limit={quota_limit}"
                )
                if __event_emitter__:
                    await __event_emitter__({"type": "notification", "data": {"type": "warning", "content": "Monthly token quota reached."}})
                yield (
                    f"Monthly token quota reached: {quota_used:,} of {quota_limit:,} tokens used in {month_key} (UTC). "
                    f"The quota resets on {reset} 00:00 UTC. Contact an administrator if you need more."
                )
                return

        try:
            model_name, model_version = await asyncio.to_thread(self._resolve_model, body.get("model", ""))
            if not model_name:
                yield "Error: No model selected."
                return

            tool_specs = self._tool_specs(client_tools, body)
            if is_admin:
                tool_specs = tool_specs + [self._usage_summary_tool_spec()]
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

                resolvable_names = set(client_tools.keys()) | (self.NATIVE_TOOLS if is_admin else set())
                unresolvable = [c for c in tool_calls if c.get("function", {}).get("name") not in resolvable_names]
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
                        if name in self.NATIVE_TOOLS:
                            result = await asyncio.to_thread(self._run_usage_summary, args.get("month"), args.get("user_email"))
                        else:
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
            if quota_limit and quota_used is not None and total_tokens:
                turn_quota_tokens = self._quota_tokens(
                    usage_totals["input_tokens"], usage_totals["output_tokens"],
                    usage_totals["cached_tokens"], usage_totals["cache_creation_tokens"],
                )
                self._add_quota_usage(user_email, turn_quota_tokens)
                warn_pct = self.valves.QUOTA_WARN_PERCENT
                new_used = quota_used + turn_quota_tokens
                if warn_pct and new_used * 100 >= quota_limit * warn_pct:
                    yield (
                        f"\n\n_(Token quota: {new_used:,} of {quota_limit:,} used this month "
                        f"({min(new_used * 100 // quota_limit, 100)}%).)_"
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
