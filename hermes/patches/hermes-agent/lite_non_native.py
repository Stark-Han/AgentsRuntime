"""Execution policy for the managed Lite image; never installed in Pro/Team.

The policy is fixed by the Lite-only build artifact. Workspace dotenv files
load with override=True upstream, so no environment switch is an authority
for granting native capabilities. Pro/Team images do not install this module.
"""
from functools import wraps
import json
import re

ENABLED = True
POLICY_VERSION = "lite-non-native-v1"
_AGENT_SEAL = object()
NATIVE_TOOLSETS = frozenset({"desktop_ui", "project", "desktop_project", "computer_use"})
# Exact locked toolsets.py desktop_ui + project + computer_use members.
NATIVE_TOOLS = frozenset({
    "read_terminal", "close_terminal", "desktop_preview", "drive_preview",
    "annotate_preview", "read_window_below", "focus_pane", "react_to_message",
    "setup_mcp", "tour", "tip", "desktop_project", "computer_use",
})
RPC_FIELDS = {
    "ping": "",
    "setup.status": "",
    "setup.runtime_check": "provider",
    "session.create": "source cols model provider reasoning_effort fast",
    "session.resume": "session_id source cols defer_history omit_messages lazy",
    "session.activate": "session_id cols omit_messages",
    "session.usage": "session_id",
    "session.close": "session_id",
    "model.options": "session_id explicit_only refresh",
    "config.set": "session_id key value confirm_expensive_model",
    "session.list": "limit",
    "session.status": "session_id",
    "session.history": "session_id",
    "session.interrupt": "session_id",
    "session.events.since": "session_id last_seen",
    "prompt.submit": "session_id text interrupted queued",
    "approval.respond": "session_id request_id choice",
    "approval.pending": "session_id",
    "approval.received": "session_id request_id",
    "clarify.respond": "session_id request_id question_id answer",
}
_SESSION_ID = re.compile(r"[A-Za-z0-9_:-]{1,160}\Z")
_MODEL_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_./:@+-]{0,255}\Z")
_MODEL_SWITCH = re.compile(r"([A-Za-z0-9][A-Za-z0-9_./:@+-]{0,255}) --provider ([A-Za-z0-9][A-Za-z0-9_./:@+-]{0,255}) --session\Z")
REASONING_EFFORTS = frozenset({"none", "minimal", "low", "medium", "high", "xhigh", "max"})


class NonNativeSessionError(RuntimeError):
    def __init__(self):
        super().__init__("incompatible_lite_live_session")


def filter_tool_names(names):
    return set(names) - NATIVE_TOOLS if ENABLED else names


def toolset_filter(fn):
    @wraps(fn)
    def filtered(*args, **kwargs):
        selected = fn(*args, **kwargs)
        if not ENABLED or selected is None:
            return selected
        return type(selected)(name for name in selected if name not in NATIVE_TOOLSETS)
    return filtered


def tool_dispatch(fn):
    @wraps(fn)
    def guarded(function_name, *args, **kwargs):
        if ENABLED and function_name in NATIVE_TOOLS:
            # Do not inspect/reflect arguments or invoke renderer callbacks.
            return json.dumps({"error": "unsupported_native_desktop_tool"})
        return fn(function_name, *args, **kwargs)
    return guarded


def _agent_names(agent):
    definitions = getattr(agent, "tools", None)
    valid = getattr(agent, "valid_tool_names", None)
    if not isinstance(definitions, list) or not isinstance(valid, (set, frozenset)):
        raise NonNativeSessionError()
    try:
        names = {tool["function"]["name"] for tool in definitions}
        if not all(isinstance(name, str) for name in names | set(valid)):
            raise NonNativeSessionError()
    except (KeyError, TypeError):
        raise NonNativeSessionError() from None
    return names | set(valid)


def seal_agent(agent):
    if not ENABLED:
        return agent
    if _agent_names(agent) & NATIVE_TOOLS:
        raise NonNativeSessionError()
    selected = getattr(agent, "enabled_toolsets", None)
    if selected is not None:
        agent.enabled_toolsets = [name for name in selected if name not in NATIVE_TOOLSETS]
    agent._clawmanager_non_native_seal = _AGENT_SEAL
    return agent


def require_compatible_session(session):
    if not ENABLED:
        return
    agent = session.get("agent")
    # Deferred records can only publish through the sealed _make_agent path.
    if agent is None:
        return
    if (getattr(agent, "_clawmanager_non_native_seal", None) is not _AGENT_SEAL
            or _agent_names(agent) & NATIVE_TOOLS
            or set(getattr(agent, "enabled_toolsets", None) or ()) & NATIVE_TOOLSETS):
        raise NonNativeSessionError()


def require_compatible_runtimes(records, session_key):
    if not ENABLED:
        return
    for sid, session in records:
        if (isinstance(session, dict) and not session.get("_finalized")
                and str(session.get("session_key") or sid) == session_key):
            require_compatible_session(session)


def external_transport(transport, *, stdio=False):
    # Only the verified loopback credential can set this ASGI scope bit.
    # A source string / request parameter is never transport authority.
    scope = getattr(getattr(transport, "_ws", None), "scope", {})
    return ENABLED and not (stdio or (isinstance(scope, dict) and scope.get("hermes_lite_internal") is True))


def rpc_denial(method, params, transport, *, stdio=False):
    if not external_transport(transport, stdio=stdio):
        return None
    fields = RPC_FIELDS.get(method)
    if fields is None:
        return (-32601, "unsupported_lite_web_rpc")
    if set(params) - set(fields.split()):
        return (-32602, "unsupported_lite_web_parameters")
    for key, value in params.items():
        if key in {"defer_history", "omit_messages", "lazy", "fast", "explicit_only", "refresh", "confirm_expensive_model", "interrupted", "queued"}:
            valid = isinstance(value, bool)
        elif key in {"limit", "last_seen", "cols"}:
            valid = type(value) is int and 0 <= value <= (100 if key == "limit" else 500 if key == "cols" else 2**63 - 1)
            if key == "cols":
                valid = valid and value >= 20
        else:
            valid = isinstance(value, str) and len(value) <= (262144 if key in {"text", "answer"} else 512)
        if not valid:
            return (-32602, "unsupported_lite_web_parameters")
    if "session_id" in fields and (method != "model.options" or "session_id" in params) and not _SESSION_ID.fullmatch(params.get("session_id", "")):
        return (-32602, "unsupported_lite_web_parameters")
    if params.get("source", "web") != "web" or ("choice" in params and params["choice"] not in {"once", "deny"}):
        return (-32602, "unsupported_lite_web_parameters")
    if method in {"session.create", "session.resume"}:
        params["source"] = "web"
    for key in ("request_id", "question_id"):
        if key in params and not _SESSION_ID.fullmatch(params[key]):
            return (-32602, "unsupported_lite_web_parameters")
    for key in ("model", "provider"):
        if key in params and not _MODEL_NAME.fullmatch(params[key]):
            return (-32602, "unsupported_lite_web_parameters")
    if "reasoning_effort" in params and params["reasoning_effort"] not in REASONING_EFFORTS:
        return (-32602, "unsupported_lite_web_parameters")
    if method == "session.create" and bool(params.get("model")) != bool(params.get("provider")):
        return (-32602, "unsupported_lite_web_parameters")
    if method in {"approval.respond", "approval.received"} and not params.get("request_id"):
        return (-32602, "unsupported_lite_web_parameters")
    if method == "approval.respond" and "choice" not in params:
        return (-32602, "unsupported_lite_web_parameters")
    if method == "model.options":
        params["explicit_only"] = True
    if method == "config.set":
        key, value = params.get("key"), params.get("value", "")
        allowed = ((key == "model" and _MODEL_SWITCH.fullmatch(value))
                   or (key == "reasoning" and value in REASONING_EFFORTS)
                   or (key == "fast" and value in {"fast", "normal"}))
        if not allowed or (key != "model" and "confirm_expensive_model" in params):
            return (-32602, "unsupported_lite_web_parameters")
    return None


def rpc_prepare(method, params, namespace, transport, *, stdio=False):
    """Authorize model identities from the actual configured Runtime inventory."""
    if not external_transport(transport, stdio=stdio):
        return None
    model, provider = params.get("model"), params.get("provider")
    if method == "config.set" and params.get("key") == "model":
        model, provider = _MODEL_SWITCH.fullmatch(params["value"]).groups()
    elif method != "session.create" and method != "setup.runtime_check":
        return None
    if not model and not provider:
        return None
    try:
        from hermes_cli.inventory import build_model_options_payload
        payload = build_model_options_payload(namespace["_model_picker_context"](None), explicit_only=True, include_unconfigured=False, refresh=False)
        configured = set()
        providers = set()
        current_model, current_provider = payload.get("model"), payload.get("provider")
        if isinstance(current_provider, str) and isinstance(current_model, str):
            configured.add((current_provider, current_model))
            providers.add(current_provider)
        for row in payload.get("providers", []):
            slug = row.get("slug")
            if not isinstance(slug, str) or (not row.get("authenticated") and slug != current_provider):
                continue
            providers.add(slug)
            configured.update((slug, name) for name in row.get("models", []) if isinstance(name, str))
        if (method == "setup.runtime_check" and provider in providers) or (provider, model) in configured:
            return None
        return (-32602, "unconfigured_lite_model")
    except Exception:
        return (5033, "model catalogue unavailable")


def session_config(fn):
    """Keep a config request and the close/reaper ownership claim serialized.

    Do not hold _sessions_lock across provider I/O or agent construction. The
    existing resume lock is the lifecycle lock; all close paths claim through
    it. Reject unready agents before invoking upstream's blocking recovery path.
    No missing/closed session can reach config.set's global fallback.
    """
    @wraps(fn)
    def guarded(rid, params):
        ns = fn.__globals__
        transport = ns["current_transport"]()
        if not external_transport(transport, stdio=transport is ns["_stdio_transport"]):
            return fn(rid, params)
        with ns["_session_resume_lock"]:
            with ns["_sessions_lock"]:
                session = ns["_sessions"].get(params.get("session_id"))
                if session is None or session.get("_closing") or session.get("_finalized"):
                    return ns["_err"](rid, 4001, "session not found; resume required")
                require_compatible_session(session)
                if session.get("agent") is None or session.get("resume_hydrating"):
                    return ns["_err"](rid, 5032, "session agent is not ready")
            return fn(rid, params)
    return guarded


def rpc_result(method, response, namespace, transport, *, stdio=False):
    """Keep readiness failures and approval replay on the safe client surface."""
    if not external_transport(transport, stdio=stdio) or not isinstance(response, dict):
        return response
    if method in {"setup.status", "setup.runtime_check", "approval.pending", "approval.received", "config.set", "model.options"} and "error" in response:
        response = dict(response)
        error = response.get("error") or {}
        response["error"] = {"code": error.get("code", -32000), "message": "Runtime request failed"}
        return response
    if method in {"setup.status", "setup.runtime_check", "approval.received"}:
        key = {"setup.status": "provider_configured", "setup.runtime_check": "ok", "approval.received": "acknowledged"}[method]
        result = response.get("result") or {}
        if type(result.get(key)) is not bool:
            return namespace["_err"](response.get("id"), -32000, "Runtime check failed")
        return {**response, "result": {key: result[key]}}
    if method == "approval.pending":
        result = response.get("result") or {}
        approvals = result.get("approvals")
        if not isinstance(approvals, list):
            return namespace["_err"](response.get("id"), -32000, "Runtime approval check failed")
        safe = []
        for approval in approvals:
            projected = namespace["_approval_request_payload"](approval)
            safe.append({key: projected[key] for key in ("request_id", "command", "description", "smart_denied", "choices") if key in projected})
            safe[-1].update(choices=[choice for choice in projected.get("choices", []) if choice in {"once", "deny"}], allow_session=False, allow_permanent=False)
        return {**response, "result": {"approvals": safe}}
    return response
