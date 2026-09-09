import importlib.util
import ast
import asyncio
import io
import json
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
PATCHES = ROOT / "patches/hermes-agent"


def module(name, path, enabled="true"):
    with patch.dict(os.environ, {"CLAWMANAGER_HERMES_DESKTOP_WEB_ENABLED": enabled}):
        spec = importlib.util.spec_from_file_location(name, path)
        loaded = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded)
        return loaded


policy = module("lite_non_native_test", PATCHES / "lite_non_native.py")
patcher = module("apply_lite_non_native_test", PATCHES / "apply_lite_non_native.py")


def agent(names=("terminal", "clarify"), selected=("terminal", "clarify")):
    return SimpleNamespace(tools=[{"type": "function", "function": {"name": name}} for name in names],
                           valid_tool_names=set(names), enabled_toolsets=list(selected))


class NonNativePolicyTests(unittest.TestCase):
    def test_tools_for_all_and_explicit_selection_exclude_native(self):
        offered = policy.NATIVE_TOOLS | {"terminal", "clarify", "read_file", "todo"}
        self.assertEqual({"terminal", "clarify", "read_file", "todo"}, policy.filter_tool_names(offered))
        selected = policy.toolset_filter(lambda: ["terminal", "clarify", "desktop_ui", "project", "computer_use"])
        self.assertEqual(["terminal", "clarify"], selected())
        self.assertEqual(set(), policy.toolset_filter(lambda: {"desktop_ui", "project"})())
        self.assertIsNone(policy.toolset_filter(lambda: None)())

    def test_native_calls_never_reach_execution_or_reflect_arguments(self):
        def forbidden(*args, **kwargs):
            self.fail("native tool reached upstream execution")
        guarded = policy.tool_dispatch(forbidden)
        for name in policy.NATIVE_TOOLS:
            with self.subTest(name=name):
                result = guarded(name, {"password": "private-do-not-reflect"})
                self.assertEqual({"error": "unsupported_native_desktop_tool"}, json.loads(result))
                self.assertNotIn("private", result)

    def test_terminal_and_clarify_keep_real_arguments(self):
        called = []
        guarded = policy.tool_dispatch(lambda name, args: called.append((name, args)) or "ok")
        for name in ("terminal", "clarify"):
            self.assertEqual("ok", guarded(name, {"value": "controlled"}))
        self.assertEqual([("terminal", {"value": "controlled"}), ("clarify", {"value": "controlled"})], called)

    def test_tool_dispatch_accepts_upstream_keyword_invocation(self):
        self.assertEqual({"error": "unsupported_native_desktop_tool"}, json.loads(
            policy.tool_dispatch(lambda **kwargs: self.fail("executed"))(function_name="read_terminal", function_args={})))

    def test_actual_schema_and_dispatch_names_must_both_be_safe(self):
        for kind in ("schema", "dispatch"):
            a = agent()
            if kind == "schema":
                a.tools.append({"function": {"name": "read_terminal"}})
            else:
                a.valid_tool_names.add("read_terminal")
            with self.subTest(kind=kind), self.assertRaisesRegex(policy.NonNativeSessionError, "incompatible_lite_live_session"):
                policy.seal_agent(a)

    def test_metadata_cannot_make_legacy_live_agent_safe(self):
        a = agent(names=("read_terminal",), selected=("desktop_ui",))
        history = [{"role": "user", "content": "preserve this history"}]
        record = {"agent": a, "source": "web", "running": True, "history": history, "transport": object()}
        before = dict(record)
        with self.assertRaises(policy.NonNativeSessionError):
            policy.require_compatible_session(record)
        self.assertEqual(before, record)
        self.assertIs(history, record["history"])
        self.assertEqual({"read_terminal"}, a.valid_tool_names)

    def test_even_empty_legacy_agent_requires_verified_construction(self):
        with self.assertRaises(policy.NonNativeSessionError):
            policy.require_compatible_session({"agent": agent(), "source": "web"})

    def test_safe_warm_reconnect_preserves_agent_and_history(self):
        a = policy.seal_agent(agent())
        record = {"agent": a, "source": "desktop", "running": True, "history": ["history"]}
        policy.require_compatible_session(record)
        self.assertIs(a, record["agent"])
        self.assertEqual("desktop", record["source"])
        self.assertTrue(record["running"])
        self.assertEqual(["history"], record["history"])

    def test_late_registration_invalidates_warm_reuse(self):
        a = policy.seal_agent(agent())
        a.valid_tool_names.add("desktop_preview")
        with self.assertRaises(policy.NonNativeSessionError):
            policy.require_compatible_session({"agent": a})

    def test_deferred_cold_resume_does_not_require_metadata_rewrite(self):
        record = {"agent": None, "source": "desktop", "history": ["preserved"]}
        policy.require_compatible_session(record)
        self.assertEqual("desktop", record["source"])

    def test_concurrent_winners_and_parked_records_are_checked(self):
        unsafe = {"session_key": "stored", "agent": agent(("read_terminal",), ("desktop_ui",)), "running": True}
        good = {"session_key": "other", "agent": policy.seal_agent(agent())}
        errors = []
        start = threading.Barrier(3)
        def resume():
            start.wait()
            try:
                policy.require_compatible_runtimes([("old", unsafe), ("safe", good)], "stored")
            except policy.NonNativeSessionError:
                errors.append(True)
        threads = [threading.Thread(target=resume) for _ in range(2)]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(2, len(errors))
        self.assertTrue(unsafe["running"])
        self.assertNotIn("_finalized", unsafe)

    def test_process_policy_does_not_change_with_later_environment(self):
        with patch.dict(os.environ, {"CLAWMANAGER_HERMES_DESKTOP_WEB_ENABLED": "false"}):
            self.assertEqual(set(), policy.filter_tool_names({"read_terminal"}))

    def test_lite_artifact_policy_cannot_be_disabled_before_import(self):
        for value in ("false", "", "0", "off"):
            with self.subTest(value=value):
                frozen = module("lite_non_native_fixed", PATCHES / "lite_non_native.py", value)
                self.assertTrue(frozen.ENABLED)
                self.assertEqual(set(), frozen.filter_tool_names({"read_terminal"}))
                with self.assertRaises(frozen.NonNativeSessionError):
                    frozen.seal_agent(agent(("read_terminal",), ("desktop_ui",)))
                self.assertEqual({"error": "unsupported_native_desktop_tool"}, json.loads(
                    frozen.tool_dispatch(lambda *args: self.fail("executed"))("read_terminal", {})))
                self.assertEqual((-32601, "unsupported_lite_web_rpc"), frozen.rpc_denial("shell.exec", {}, None))


class RPCPolicyTests(unittest.TestCase):
    def test_exact_core_requests_are_supported(self):
        params = {
            "ping": {}, "session.create": {"source": "web"}, "session.list": {"limit": 100},
            "setup.status": {}, "setup.runtime_check": {}, "model.options": {"explicit_only": True},
            "config.set": {"session_id": "session-1", "key": "model", "value": "m --provider managed --session"},
            "approval.pending": {"session_id": "session-1"}, "approval.received": {"session_id": "session-1", "request_id": "approval-1"},
            "session.resume": {"session_id": "session-1", "source": "web", "lazy": True, "defer_history": True, "omit_messages": True},
            "session.events.since": {"session_id": "session-1", "last_seen": 0},
            "prompt.submit": {"session_id": "session-1", "text": "local stub"},
            "approval.respond": {"session_id": "session-1", "request_id": "approval-1", "choice": "once"},
            "clarify.respond": {"session_id": "session-1", "request_id": "clarify-1", "answer": "yes"},
        }
        for method in ("session.status", "session.history", "session.interrupt", "session.activate", "session.usage", "session.close"):
            params[method] = {"session_id": "session-1"}
        self.assertEqual(set(policy.RPC_FIELDS), set(params))
        for method, fields in params.items():
            with self.subTest(method=method):
                self.assertIsNone(policy.rpc_denial(method, fields, None))

    def test_no_native_arbitrary_shell_files_config_or_profile_rpc(self):
        for method in ("shell.exec", "cli.exec", "fs.read", "browser.manage", "desktop.respond", "profiles.configure", "profiles.list", "session.active_list", "tools.call"):
            with self.subTest(method=method):
                self.assertEqual((-32601, "unsupported_lite_web_rpc"), policy.rpc_denial(method, {}, None))
        for fields in ({"cwd": "/"}, {"profile": "other"}, {"source": "desktop"}, {"source": "internal"}, {"messages": []}, {"model": "other"}):
            self.assertEqual((-32602, "unsupported_lite_web_parameters"), policy.rpc_denial("session.create", fields, None))

    def test_rpc_types_and_bounds(self):
        for method, fields in [
            ("session.resume", {"session_id": "../other"}),
            ("session.resume", {"session_id": "safe", "lazy": "true"}),
            ("session.list", {"limit": True}), ("session.list", {"limit": 101}),
            ("approval.respond", {"session_id": "safe", "choice": "always"}),
            ("session.events.since", {"session_id": "safe", "last_seen": -1}),
        ]:
            with self.subTest(method=method, fields=fields):
                self.assertEqual((-32602, "unsupported_lite_web_parameters"), policy.rpc_denial(method, fields, None))

    def test_only_verified_transport_scope_allows_classic_internal_rpc(self):
        trusted = SimpleNamespace(_ws=SimpleNamespace(scope={"hermes_lite_internal": True}))
        untrusted = SimpleNamespace(auth_identity={"source": "internal"}, _ws=SimpleNamespace(scope={}))
        self.assertIsNone(policy.rpc_denial("cli.exec", {}, trusted))
        self.assertIsNone(policy.rpc_denial("cli.exec", {}, None, stdio=True))
        self.assertEqual((-32601, "unsupported_lite_web_rpc"), policy.rpc_denial("cli.exec", {}, untrusted))

    def test_desktop_create_and_live_options_use_exact_scope_and_types(self):
        self.assertIsNone(policy.rpc_denial("session.create", {"source": "web", "cols": 96, "model": "m", "provider": "managed", "reasoning_effort": "high", "fast": False}, None))
        self.assertIsNone(policy.rpc_denial("prompt.submit", {"session_id": "s", "text": "next", "queued": True, "interrupted": False}, None))
        for key, values in {"reasoning": policy.REASONING_EFFORTS, "fast": {"fast", "normal"}}.items():
            for value in values:
                self.assertIsNone(policy.rpc_denial("config.set", {"session_id": "s", "key": key, "value": value}, None))
        for fields in [
            {"key": "reasoning", "value": "high"},
            {"session_id": "s", "key": "reasoning", "value": "show"},
            {"session_id": "s", "key": "reasoning", "value": "high", "scope": "global"},
            {"session_id": "s", "key": "fast", "value": "toggle"},
            {"session_id": "s", "key": "fast", "value": True},
            {"session_id": "s", "key": "model", "value": "m --provider managed --global"},
            {"session_id": "s", "key": "env", "value": "KEY=x"},
        ]:
            self.assertEqual((-32602, "unsupported_lite_web_parameters"), policy.rpc_denial("config.set", fields, None), fields)
        for fields in ({"cols": True}, {"cols": 501}, {"model": "m"}, {"model": "m", "provider": "p", "fast": "false"}, {"reasoning_effort": "show"}):
            self.assertIsNotNone(policy.rpc_denial("session.create", fields, None), fields)

    def test_catalogue_requires_a_configured_pair_and_failure_does_not_allow_writes(self):
        payload = {"provider": "managed", "model": "default", "providers": [{"slug": "managed", "authenticated": True, "models": ["m"]}, {"slug": "external", "authenticated": False, "models": ["evil"]}]}
        inventory = SimpleNamespace(build_model_options_payload=lambda *args, **kwargs: payload)
        namespace = {"_model_picker_context": lambda _: object()}
        with patch.dict("sys.modules", {"hermes_cli": SimpleNamespace(), "hermes_cli.inventory": inventory}):
            for fields in ({"model": "m", "provider": "managed"}, {"model": "default", "provider": "managed"}):
                self.assertIsNone(policy.rpc_prepare("session.create", fields, namespace, None))
            for fields in ({"model": "evil", "provider": "external"}, {"model": "new", "provider": "managed"}):
                self.assertEqual((-32602, "unconfigured_lite_model"), policy.rpc_prepare("session.create", fields, namespace, None))
            self.assertIsNone(policy.rpc_prepare("config.set", {"key": "model", "value": "m --provider managed --session"}, namespace, None))
            self.assertIsNone(policy.rpc_prepare("setup.runtime_check", {"provider": "managed"}, namespace, None))
        self.assertIsNotNone(policy.rpc_prepare("session.create", {"model": "m", "provider": "managed"}, namespace, None))

    def test_config_scope_serializes_close_without_holding_registry_lock(self):
        entered, release, closed = threading.Event(), threading.Event(), threading.Event()
        live = {"agent": policy.seal_agent(agent())}
        namespace = {"current_transport": lambda: None, "_stdio_transport": object(), "_session_resume_lock": threading.Lock(), "_sessions_lock": threading.RLock(), "_sessions": {"s": live}, "_err": lambda rid, code, message: {"error": {"code": code, "message": message}}, "entered": entered, "release": release}
        exec('def change(rid, params):\n    entered.set()\n    if not release.wait(2): raise RuntimeError("blocked")\n    _sessions[params["session_id"]]["changed"] = True\n    return {"result": True}', namespace)
        change = policy.session_config(namespace["change"])
        result = []
        worker = threading.Thread(target=lambda: result.append(change(1, {"session_id": "s"})))
        worker.start()
        self.assertTrue(entered.wait(2))
        def close():
            with namespace["_session_resume_lock"]:
                with namespace["_sessions_lock"]:
                    namespace["_sessions"].pop("s")
                closed.set()
        closer = threading.Thread(target=close)
        closer.start()
        self.assertTrue(namespace["_sessions_lock"].acquire(timeout=1))
        namespace["_sessions_lock"].release()
        self.assertFalse(closed.is_set())
        release.set()
        worker.join(2)
        closer.join(2)
        self.assertFalse(worker.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertEqual([{"result": True}], result)
        self.assertTrue(live["changed"])
        self.assertEqual(4001, change(2, {"session_id": "s"})["error"]["code"])
        namespace["_sessions"]["s"] = {"agent": None}
        self.assertEqual(5032, change(3, {"session_id": "s"})["error"]["code"])

    def test_readiness_and_pending_approval_use_safe_projection(self):
        ns = {"_err": lambda rid, code, message: {"id": rid, "error": {"code": code, "message": message}}, "_approval_request_payload": lambda _: {"request_id": "r", "command": "safe command", "choices": ["once", "session", "always", "deny"], "private": "secret"}}
        response = policy.rpc_result("setup.runtime_check", {"id": 1, "result": {"ok": False, "error": "private-token", "source": "credential-file"}}, ns, None)
        self.assertEqual({"id": 1, "result": {"ok": False}}, response)
        response = policy.rpc_result("approval.pending", {"id": 2, "result": {"approvals": [{"command": "unredacted secret"}]}}, ns, None)
        self.assertNotIn("secret", json.dumps(response))
        self.assertEqual(["once", "deny"], response["result"]["approvals"][0]["choices"])


class HydrationLifecycleTests(unittest.TestCase):
    def test_actual_hydration_failure_cannot_remove_configured_session(self):
        # Use the actual built/installed handler, not a rewritten lookalike.
        # Host integration runs point this at the exact patched upstream tree.
        source_root = Path(os.environ.get("HERMES_LITE_TEST_PATCHED_ROOT", "/opt/hermes-agent"))
        source_file = source_root / "tui_gateway/server.py"
        if not source_file.is_file():
            self.skipTest("requires installed candidate or HERMES_LITE_TEST_PATCHED_ROOT")
        tree = ast.parse(source_file.read_text(encoding="utf-8"))
        hydration = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                         and node.name == "_schedule_resume_hydration")
        ready, entered, failed, popped = (threading.Event() for _ in range(4))
        threads = []

        class Sessions(dict):
            def pop(self, *args):
                value = super().pop(*args)
                popped.set()
                return value

        live = {"agent": None, "resume_hydrating": True, "history_lock": threading.Lock(),
                "resume_history_ready": threading.Event(), "agent_ready": threading.Event()}
        sessions = Sessions(s=live)

        def start_thread(*args, **kwargs):
            thread = threading.Thread(*args, **kwargs)
            threads.append(thread)
            return thread

        def late_failure(*_args):
            # resume_history_ready has awakened the independent builder. Its
            # sealed agent may finish before this late hydration failure.
            self.assertTrue(live["resume_history_ready"].is_set())
            live["agent"] = policy.seal_agent(agent())
            ready.set()
            if not entered.wait(2):
                raise AssertionError("config handler did not start")
            raise RuntimeError("synthetic late hydration failure")

        def emit(kind, _sid, _payload=None):
            if kind == "error":
                failed.set()

        namespace = {"threading": SimpleNamespace(Thread=start_thread), "_sessions": sessions,
                     "_sessions_lock": threading.RLock(), "_session_resume_lock": threading.Lock(),
                     "current_transport": lambda: None, "_stdio_transport": object(),
                     "_err": lambda rid, code, message: {"error": {"code": code}},
                     "sanitize_replay_history": lambda value: value, "_todo_state_from_history": lambda _: None,
                     "_start_agent_build": lambda *_: None, "_maybe_schedule_auto_continue": late_failure,
                     "_emit": emit, "entered": entered, "failed": failed, "popped": popped}
        exec(compile(ast.Module(body=[hydration], type_ignores=[]), str(source_file), "exec"), namespace)
        exec('def config(rid, params):\n'
             '    entered.set()\n'
             '    if not failed.wait(2): raise AssertionError("hydration did not fail")\n'
             '    popped.wait(0.5)\n'
             '    return {"scope": "session" if _sessions.get(params["session_id"]) else "global"}', namespace)
        database = SimpleNamespace(reopen_session=lambda _: None,
                                   get_resume_conversations=lambda _: ([], []),
                                   get_ancestor_display_prefix=lambda _: [])
        namespace["_schedule_resume_hydration"]("s", "stored", database)
        try:
            self.assertTrue(ready.wait(2))
            result = policy.session_config(namespace["config"])(1, {"session_id": "s"})
            self.assertEqual({"scope": "session"}, result)
        finally:
            entered.set()
            for thread in threads:
                thread.join(2)
                self.assertFalse(thread.is_alive(), "hydration deadlocked with config")
        self.assertTrue(popped.is_set(), "failed hydration did not release its record after config")


class PatchIntegrityTests(unittest.TestCase):
    def test_sha_mismatch_is_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for relative in patcher.EXPECTED:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("original untouched")
            with self.assertRaisesRegex(ValueError, "exact locked upstream"):
                patcher.apply(root)
            for relative in patcher.EXPECTED:
                self.assertEqual("original untouched", (root / relative).read_text())

    def test_missing_and_duplicate_anchors_fail_closed(self):
        for source in ("missing", "anchor anchor"):
            with self.assertRaisesRegex(ValueError, "anchor changed"):
                patcher.replace_once(source, "anchor", "replacement")


class DeploymentEnvironmentTests(unittest.TestCase):
    def test_workspace_cannot_change_or_create_managed_keys(self):
        initial = {"HOME": "/fixture/home", "HERMES_HOME": "/fixture/home/.hermes", "PORT": "20001",
                   "CLAWMANAGER_CONTROL_UI_ORIGIN": "https://internal.example",
                   "CLAWMANAGER_TRUSTED_PROXY_CIDRS": "10.0.0.0/24",
                   "HERMES_DASHBOARD_BASIC_AUTH_PASSWORD": "initial-fixture-only"}
        with patch.dict(os.environ, initial, clear=True):
            guard = module("lite_environment_test", PATCHES / "lite_environment.py")
            fixture = "\n".join(key + "=workspace-override" for key in initial)
            fixture += "\nCLAWMANAGER_FORGED=bad\nPYTHONPATH=/unsafe\nFIXTURE_ROOT=/user\nFIXTURE_NOTE=${FIXTURE_ROOT}/child\n"
            self.assertTrue(guard.load_dotenv(stream=io.StringIO(fixture), override=True))
            for key, value in initial.items():
                self.assertEqual(value, os.environ[key])
            self.assertNotIn("CLAWMANAGER_FORGED", os.environ)
            self.assertNotIn("PYTHONPATH", os.environ)
            self.assertEqual("/user/child", os.environ["FIXTURE_NOTE"])
            guard.load_dotenv(stream=io.StringIO("FIXTURE_NOTE=new\n"), override=False)
            self.assertEqual("/user/child", os.environ["FIXTURE_NOTE"])

    def test_reload_and_error_preserve_initial_managed_values(self):
        with patch.dict(os.environ, {"HOME": "/fixture/home", "CLAWMANAGER_INSTANCE_ID": "123"}, clear=True):
            guard = module("lite_environment_reload_test", PATCHES / "lite_environment.py")
            @guard.preserve_environment
            def reload():
                os.environ["HOME"] = "/changed"
                os.environ["CLAWMANAGER_FORGED"] = "bad"
                os.environ["USER_OPTION"] = "retained"
                raise ValueError("controlled failure")
            with self.assertRaises(ValueError):
                reload()
            self.assertEqual("/fixture/home", os.environ["HOME"])
            self.assertNotIn("CLAWMANAGER_FORGED", os.environ)
            self.assertEqual("retained", os.environ["USER_OPTION"])


class PublicProtocolTests(unittest.TestCase):
    def test_accept_only_selects_offered_public_protocol(self):
        boundary_module = module("lite_boundary_protocol_test", PATCHES / "lite_gateway_boundary.py")
        for path in ("/api/ws", "/api/events", "/api/pty"):
            for offered, selected in ((b"hermes-gateway-v1, hermes-gateway-ticket.fixture-only", "hermes-gateway-v1"),
                                      (b"hermes-gateway-ticket.fixture-only", None), (b"", None)):
                with self.subTest(path=path, offered=offered):
                    sent = []
                    async def application(scope, receive, send):
                        await send({"type": "websocket.accept", "subprotocol": "hermes-gateway-ticket.fixture-only"})
                    async def send(event):
                        sent.append(event)
                    with patch.dict(os.environ, {"CLAWMANAGER_CONTROL_UI_ORIGIN": "https://internal.example", "CLAWMANAGER_TRUSTED_PROXY_CIDRS": "10.0.0.0/24"}):
                        boundary = boundary_module.LiteGatewayBoundary(application)
                    scope = {"type": "websocket", "path": path, "client": ("10.0.0.5", 12345),
                             "headers": [(b"origin", b"https://internal.example"), (b"sec-websocket-protocol", offered)]}
                    asyncio.run(boundary(scope, None, send))
                    self.assertEqual([{"type": "websocket.accept", "subprotocol": selected}], sent)
                    self.assertNotIn("fixture-only", json.dumps(sent))


if __name__ == "__main__":
    unittest.main()
