"""
Streamable HTTP client for Enterpret Wisdom MCP (https://wisdom-api.enterpret.com/server/mcp).

Uses Bearer token auth (WISDOM_AUTH_TOKEN). Implements MCP lifecycle: initialize,
notifications/initialized, tools/call.

Spec reference: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

DEFAULT_WISDOM_MCP_URL = "https://wisdom-api.enterpret.com/server/mcp"


class WisdomMCPError(RuntimeError):
    pass


def _parse_sse_json_rpc_events(body: str) -> List[dict]:
    """Extract JSON objects from SSE `data:` lines."""
    out: List[dict] = []
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload or payload == "[DONE]":
            continue
        try:
            out.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    return out


def _find_json_rpc_result(messages: List[dict], request_id) -> Optional[dict]:
    for msg in messages:
        if msg.get("id") == request_id and "result" in msg:
            return msg["result"]
        if msg.get("id") == request_id and "error" in msg:
            err = msg["error"]
            raise WisdomMCPError(
                f"MCP error {err.get('code')}: {err.get('message', err)}"
            )
    return None


class WisdomMCPClient:
    def __init__(self, base_url: str, bearer_token: str):
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self._client: Optional[httpx.AsyncClient] = None
        self._session_id: Optional[str] = None
        self._protocol_version = "2025-06-18"
        self._request_id = 0

    async def __aenter__(self) -> "WisdomMCPClient":
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=30.0))
        await self._initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _headers(self) -> Dict[str, str]:
        h = {
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": self._protocol_version,
        }
        if self._session_id:
            h["Mcp-Session-Id"] = self._session_id
        return h

    async def _post_message(self, message: dict) -> Tuple[dict, httpx.Response]:
        assert self._client is not None
        rid = message.get("id")
        resp = await self._client.post(
            self.base_url,
            headers=self._headers(),
            json=message,
        )
        if resp.status_code == 401:
            raise WisdomMCPError(
                "Wisdom MCP returned 401. Use an Auth Token from Enterpret "
                "Settings → Wisdom MCP (not a dashboard session token). "
                "See https://helpcenter.enterpret.com/en/articles/12665166-wisdom-mcp-server"
            )

        ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()

        if ct == "application/json":
            data = resp.json()
            return data, resp

        if "text/event-stream" in ct or resp.text.strip().startswith("event:"):
            text = resp.text
            events = _parse_sse_json_rpc_events(text)
            if rid is not None:
                result_msg = _find_json_rpc_result(events, rid)
                if result_msg is not None:
                    return {"result": result_msg, "_raw_sse": events}, resp
            if resp.status_code >= 400:
                raise WisdomMCPError(
                    f"MCP HTTP {resp.status_code}: {text[:500]}"
                )
            if not events:
                raise WisdomMCPError(
                    f"Empty or unparseable SSE response (HTTP {resp.status_code})"
                )
            return {"result": events[-1].get("result", events[-1]), "_raw_sse": events}, resp

        try:
            data = resp.json()
            return data, resp
        except json.JSONDecodeError as e:
            raise WisdomMCPError(
                f"Unexpected response ({resp.status_code}, {ct}): {resp.text[:300]}"
            ) from e

    async def _initialize(self) -> None:
        init_versions = ("2025-06-18", "2024-11-05")
        last_err: Optional[Exception] = None
        for ver in init_versions:
            self._protocol_version = ver
            req_id = self._next_id()
            body = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "initialize",
                "params": {
                    "protocolVersion": ver,
                    "capabilities": {},
                    "clientInfo": {"name": "figment-agent", "version": "0.1.0"},
                },
            }
            try:
                assert self._client is not None
                resp = await self._client.post(
                    self.base_url,
                    headers=self._headers(),
                    json=body,
                )
            except httpx.RequestError as e:
                last_err = e
                continue

            if resp.status_code == 400 and "MCP-Protocol-Version" in (resp.text or ""):
                last_err = WisdomMCPError(resp.text[:200])
                continue

            sid = resp.headers.get("Mcp-Session-Id") or resp.headers.get("mcp-session-id")
            ct = (resp.headers.get("content-type") or "").lower()

            if resp.status_code == 401:
                raise WisdomMCPError(
                    "Wisdom MCP returned 401. Generate a token under "
                    "Enterpret Settings → Wisdom MCP (Bearer / Auth Token)."
                )

            if "text/event-stream" in ct:
                events = _parse_sse_json_rpc_events(resp.text)
                res = _find_json_rpc_result(events, req_id)
                if res is None:
                    last_err = WisdomMCPError("initialize: no result in SSE")
                    continue
                self._session_id = sid or self._session_id
                await self._send_initialized_notification()
                return

            try:
                data = resp.json()
            except json.JSONDecodeError:
                last_err = WisdomMCPError(f"initialize: non-JSON body: {resp.text[:200]}")
                continue

            if "error" in data:
                last_err = WisdomMCPError(str(data["error"]))
                continue

            if "result" not in data:
                last_err = WisdomMCPError(f"initialize: unexpected {data!r}")
                continue

            self._session_id = sid
            await self._send_initialized_notification()
            return

        raise WisdomMCPError(
            f"Could not initialize Wisdom MCP (tried protocol versions {init_versions}): {last_err}"
        )

    async def _send_initialized_notification(self) -> None:
        note = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }
        assert self._client is not None
        resp = await self._client.post(
            self.base_url,
            headers=self._headers(),
            json=note,
        )
        if resp.status_code not in (200, 202):
            logger.warning(
                "notifications/initialized returned HTTP %s: %s",
                resp.status_code,
                resp.text[:200],
            )

    async def call_tool(self, name: str, arguments: dict) -> dict:
        req_id = self._next_id()
        message = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        data, resp = await self._post_message(message)
        if "error" in data and "result" not in data:
            err = data["error"]
            raise WisdomMCPError(
                f"tools/call error: {err.get('message', err)}"
            )
        result = data.get("result")
        if result is None and "_raw_sse" in data:
            raw = data["_raw_sse"]
            found = _find_json_rpc_result(raw, req_id)
            result = found
        if result is None:
            raise WisdomMCPError(f"tools/call: no result in {data!r}")
        return result if isinstance(result, dict) else {"_value": result}

    async def list_tools(self) -> List[dict]:
        req_id = self._next_id()
        msg = {"jsonrpc": "2.0", "id": req_id, "method": "tools/list", "params": {}}
        raw, _ = await self._post_message(msg)
        if "error" in raw and "result" not in raw:
            raise WisdomMCPError(f"tools/list failed: {raw['error']}")
        tools_result = raw.get("result") or {}
        return tools_result.get("tools") or []


def _wisdom_tool_error_payload(result: dict) -> Optional[dict]:
    """
    Detect Enterpret error payloads. ``isError`` is often on a *wrapper* around
    ``structuredContent``, not on the top-level MCP ``result``, so a plain
    ``result.get("isError")`` misses real failures.
    """
    sc = result.get("structuredContent")
    if not isinstance(sc, dict):
        return None
    inner = sc.get("structuredContent")
    inner = inner if isinstance(inner, dict) else None
    if sc.get("isError"):
        if inner is not None:
            return inner
        return {"error_type": "unknown", "error": "isError wrapper with no inner payload"}
    if inner is not None and inner.get("success") is False:
        return inner
    if inner is None and sc.get("success") is False:
        return sc
    return None


def tool_result_to_text(result: dict) -> str:
    """Flatten MCP CallToolResult text content into a single string."""
    if result.get("isError"):
        parts = result.get("content") or []
        texts = []
        for p in parts:
            if isinstance(p, dict) and p.get("type") == "text":
                texts.append(p.get("text") or "")
        raise WisdomMCPError("Tool error: " + " ".join(texts).strip() or str(result))

    parts = result.get("content") or []
    chunks: List[str] = []
    for p in parts:
        if isinstance(p, dict) and p.get("type") == "text":
            chunks.append(p.get("text") or "")
    if chunks:
        return "\n".join(chunks).strip()
    sc = result.get("structuredContent")
    if sc is not None:
        return json.dumps(sc)
    return ""


def extract_json_array_from_text(text: str) -> Optional[List[dict]]:
    """
    Parse a JSON array of objects from model/tool text (fenced block or raw).
    Returns None if no array found.
    """
    if not text:
        return None

    fence = re.search(r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text, re.IGNORECASE)
    if fence:
        try:
            data = json.loads(fence.group(1))
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
        except json.JSONDecodeError:
            pass

    start = text.find("[")
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            c = text[i]
            if c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    chunk = text[start : i + 1]
                    try:
                        data = json.loads(chunk)
                        if isinstance(data, list) and all(isinstance(x, dict) for x in data):
                            return data
                    except json.JSONDecodeError:
                        break
        start = text.find("[", start + 1)

    return None


def _unwrap_structured_content(payload: dict) -> dict:
    """Wisdom sometimes nests CallToolResult twice (structuredContent.structuredContent)."""
    sc = payload.get("structuredContent")
    if isinstance(sc, dict) and "structuredContent" in sc and isinstance(
        sc.get("structuredContent"), dict
    ):
        inner = sc.get("structuredContent") or {}
        if inner.get("results") is not None or inner.get("success") is not None:
            return inner
    return sc if isinstance(sc, dict) else {}


# Keys Wisdom / KG tools may use for row-like lists (see Enterpret MCP docs).
_STRUCTURED_LIST_KEYS = (
    "records",
    "rows",
    "results",
    "data",
    "items",
    "accounts",
    "matches",
    "entities",
    "nodes",
    "searchResults",
    "search_results",
    "graphResults",
    "graph_results",
    "elements",
    "hits",
)


def _normalize_wisdom_row(item: dict) -> dict:
    """Unwrap common graph shapes: {entity: {...}}, {properties: {...}}, etc."""
    if not isinstance(item, dict):
        return item
    for wrap in ("record", "entity", "node", "item", "object", "row"):
        inner = item.get(wrap)
        if isinstance(inner, dict):
            out = {k: v for k, v in item.items() if k != wrap}
            out.update(inner)
            return out
    props = item.get("properties")
    if isinstance(props, dict):
        out = {k: v for k, v in item.items() if k != "properties"}
        out.update(props)
        return out
    return item


def _dict_list_from_mapping(sc: dict) -> Optional[List[dict]]:
    for key in _STRUCTURED_LIST_KEYS:
        v = sc.get(key)
        if not isinstance(v, list) or not v:
            continue
        dicts = [x for x in v if isinstance(x, dict)]
        if dicts:
            return [_normalize_wisdom_row(d) for d in dicts]
    return None


def records_from_wisdom_tool_result(result: dict) -> List[dict]:
    """
    Best-effort: turn Wisdom tool result into list[dict] for Tier2 normalization.

    - structuredContent (if present)
    - JSON array in text content
    - list of dicts at top-level text JSON
    """
    err_payload = _wisdom_tool_error_payload(result)
    if err_payload is not None:
        et = err_payload.get("error_type") or "?"
        msg = err_payload.get("error") or err_payload.get("message") or ""
        logger.error(
            "Wisdom MCP tool error — error_type=%r message=%r details=%s "
            "(no account rows for this query; Enterpret ServiceError is often upstream/graph)",
            et,
            msg,
            err_payload.get("details"),
        )
        return []

    sc = _unwrap_structured_content(result)
    if not sc and isinstance(result.get("structuredContent"), dict):
        sc = result.get("structuredContent") or {}
    if isinstance(sc, list) and sc:
        dicts = [x for x in sc if isinstance(x, dict)]
        if dicts:
            return [_normalize_wisdom_row(d) for d in dicts]
    if isinstance(sc, dict):
        if sc.get("success") is False:
            err = sc.get("error") or sc.get("message") or sc.get("detail") or ""
            logger.error(
                "Wisdom tool reported success=false (Tier2 may be empty): error=%r keys=%s",
                err,
                list(sc.keys()),
            )
            return []
        found = _dict_list_from_mapping(sc)
        if found:
            return found

    try:
        text = tool_result_to_text(result)
    except WisdomMCPError:
        raise
    parsed = extract_json_array_from_text(text)
    if parsed:
        return [_normalize_wisdom_row(d) for d in parsed]

    if text.strip().startswith("{"):
        try:
            obj = json.loads(text)
            if isinstance(obj, list) and all(isinstance(x, dict) for x in obj):
                return [_normalize_wisdom_row(d) for d in obj]
            if isinstance(obj, dict):
                found = _dict_list_from_mapping(obj)
                if found:
                    return found
        except json.JSONDecodeError:
            pass

    logger.warning(
        "Wisdom tool returned no JSON array of objects; raw text (truncated): %s",
        (text or str(result))[:400],
    )
    return []


async def wisdom_warmup_if_available(client: WisdomMCPClient, tools: List[dict]) -> None:
    """
    Enterpret recommends calling initialize_wisdom at the start of each session.
    Best-effort: failures are logged and ignored so Tier2 can still run.
    """
    by_name = {t.get("name"): t for t in tools if t.get("name")}
    for tool_name in ("initialize_wisdom", "initializeWisdom", "wisdom_initialize"):
        if tool_name not in by_name:
            continue
        tdef = by_name[tool_name]
        schema = tdef.get("inputSchema") or {}
        props = schema.get("properties") or {}
        required = list(schema.get("required") or [])
        args: Dict[str, Any] = {}
        for r in required:
            spec = props.get(r) or {}
            t = spec.get("type")
            if t == "string":
                args[r] = ""
            elif t == "object":
                args[r] = {}
            elif t == "boolean":
                args[r] = False
            elif t == "array":
                args[r] = []
            elif t == "number" or t == "integer":
                args[r] = 0
            else:
                args[r] = None
        try:
            result = await client.call_tool(tool_name, args)
            if result.get("isError"):
                logger.warning("Wisdom %s returned isError=True", tool_name)
            else:
                logger.info("Wisdom session warmed up (%s)", tool_name)
        except WisdomMCPError as e:
            logger.warning("Wisdom %s failed (continuing): %s", tool_name, e)
        return
    logger.debug("Wisdom tools/list has no initialize_wisdom; skipping warmup")


async def run_wisdom_query(
    client: WisdomMCPClient,
    tools: List[dict],
    prompt: str,
    *,
    cypher: Optional[str] = None,
    tool_override: Optional[str] = None,
) -> List[dict]:
    """
    One Wisdom query using an existing MCP session and a cached tools/list payload.
    """
    if cypher:
        tool_name = tool_override or "execute_cypher_query"
        arg_keys = ["cypher", "query", "statement", "cypherQuery"]
        last_err: Optional[Exception] = None
        for key in arg_keys:
            try:
                result = await client.call_tool(tool_name, {key: cypher})
                return records_from_wisdom_tool_result(result)
            except WisdomMCPError as e:
                last_err = e
                continue
        raise last_err or WisdomMCPError("execute_cypher_query failed")

    search_tool = tool_override or "search_knowledge_graph"
    tool_def = next((t for t in tools if t.get("name") == search_tool), None)
    if tool_def is None:
        names = [t.get("name") for t in tools]
        raise WisdomMCPError(
            f"Tool {search_tool!r} not found. Available: {names}"
        )

    input_schema = tool_def.get("inputSchema") or {}
    props = input_schema.get("properties") or {}
    required = list(input_schema.get("required") or [])

    args: Dict[str, Any] = {}
    if required:
        for r in required:
            if r in props and props[r].get("type") == "string":
                args[r] = prompt.strip()
            elif r in props and props[r].get("type") == "object":
                args[r] = {"query": prompt.strip()}
            else:
                args[r] = prompt.strip()
    else:
        keys = pick_string_argument_keys(props)
        if keys:
            args[keys[0]] = prompt.strip()
        else:
            args["query"] = prompt.strip()

    result = await client.call_tool(search_tool, args)
    return records_from_wisdom_tool_result(result)


def pick_string_argument_keys(schema_props: dict) -> List[str]:
    """Guess which JSON Schema property is the main search / query string."""
    if not schema_props:
        return []
    candidates = []
    for key, spec in schema_props.items():
        if not isinstance(spec, dict):
            continue
        if spec.get("type") == "string":
            candidates.append(key)
    priority = [
        "query",
        "searchQuery",
        "search",
        "q",
        "term",
        "text",
        "prompt",
        "question",
        "cypher",
        "statement",
    ]
    ordered = [k for k in priority if k in candidates]
    rest = [k for k in candidates if k not in ordered]
    return ordered + rest


async def wisdom_query_for_prompt(
    base_url: str,
    token: str,
    prompt: str,
    *,
    tool_override: Optional[str] = None,
    cypher: Optional[str] = None,
) -> List[dict]:
    """
    Run Wisdom MCP (opens one session): search_knowledge_graph or execute_cypher_query.
    Prefer execute_wisdom_prompt_jobs + one shared WisdomMCPClient session for multiple prompts.
    """
    async with WisdomMCPClient(base_url, token) as client:
        tools = await client.list_tools()
        return await run_wisdom_query(
            client,
            tools,
            prompt,
            cypher=cypher,
            tool_override=tool_override,
        )
