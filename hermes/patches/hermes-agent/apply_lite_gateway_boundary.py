#!/usr/bin/env python3
"""Apply only to the reviewed v2026.8.31 server, before packaging Lite."""
import hashlib
from pathlib import Path
import shutil
import sys

EXPECTED = "c56b2f4605f0858704463a4f8ed8f3e44b56bba0f53b0e1839c33a537b4b16f4"


def apply(root):
    server = root / "hermes_cli/web_server.py"
    if hashlib.sha256(server.read_bytes()).hexdigest() != EXPECTED:
        raise ValueError("Lite boundary patch requires the exact locked upstream server")
    source = server.read_text(encoding="utf-8")
    replacements = {
        "def start_server(\n": "# AgentsRuntime Lite: preserve socket peer and enforce managed Origin.\n"
                              "from hermes_cli.lite_gateway_boundary import install_lite_boundary\n"
                              "install_lite_boundary(app)\n\n\ndef start_server(\n",
        "proxy_headers=bool(app.state.auth_required),": "proxy_headers=False,  # Lite boundary verifies the original socket peer\n"
                                                     "        access_log=False,  # Never log request queries or authentication material",
        'internal = ws.query_params.get("internal", "")':
            'from hermes_cli.lite_gateway_boundary import internal_credential\n'
            '        internal = internal_credential(ws.scope.get("headers", []))',
        'qs = urllib.parse.urlencode({"internal": internal_ws_credential()})':
            'qs = ""  # Lite TUI authenticates through Sec-WebSocket-Protocol',
        'qs = urllib.parse.urlencode(\n            {"internal": internal_ws_credential(), "channel": channel}\n        )':
            'qs = urllib.parse.urlencode({"channel": channel})',
        'argv, cwd = _make_tui_argv(PROJECT_ROOT / "ui-tui", tui_dev=False)':
            'argv, cwd = ["/usr/local/bin/node", "--expose-gc", str(PROJECT_ROOT / "hermes_cli/tui_dist/entry.js")], PROJECT_ROOT / "hermes_cli/tui_dist"',
        'env["HERMES_TUI_DASHBOARD"] = "1"':
            'env["HERMES_TUI_DASHBOARD"] = "1"\n'
            '    from hermes_cli.dashboard_auth.ws_tickets import internal_ws_credential\n'
            '    env["HERMES_LITE_INTERNAL_WS_CREDENTIAL"] = internal_ws_credential()',
    }
    for before, after in replacements.items():
        if source.count(before) != 1:
            raise ValueError("Lite boundary patch anchor changed")
        source = source.replace(before, after)
    server.write_text(source, encoding="utf-8", newline="\n")
    shutil.copyfile(Path(__file__).with_name("lite_gateway_boundary.py"), root / "hermes_cli/lite_gateway_boundary.py")
    client = root / "ui-tui/src/gatewayClient.ts"
    if hashlib.sha256(client.read_bytes()).hexdigest() != "86a0259975e1515d46d6e50939c2756dfbda135d585a5e03cd12c0763f4d4d2c":
        raise ValueError("Lite TUI patch requires the exact locked upstream client")
    client_source = client.read_text(encoding="utf-8")
    for before in ['new WebSocketCtor(this.sidecarUrl)', 'new WebSocketCtor(attachUrl)']:
        if client_source.count(before) != 1:
            raise ValueError("Lite TUI patch anchor changed")
        client_source = client_source.replace(before, before[:-1] + ', liteInternalProtocols())')
    client_source += '\n// AgentsRuntime: server-only credential, never placed in URLs or renderer state.\n'
    client_source += 'function liteInternalProtocols(): string[] {\n'
    client_source += '  const token = process.env.HERMES_LITE_INTERNAL_WS_CREDENTIAL\n'
    client_source += '  if (!token) throw new Error("missing_internal_websocket_credential")\n'
    client_source += '  return ["hermes-gateway-v1", "hermes-internal." + token]\n}\n'
    client.write_text(client_source, encoding="utf-8", newline="\n")
    provider = root / "hermes_cli/runtime_provider.py"
    if hashlib.sha256(provider.read_bytes()).hexdigest() != "955054a4ebac704dc7f8182dbe7431e03716d59bc80044c05da2bf8533590372":
        raise ValueError("Lite provider patch requires the exact locked upstream resolver")
    provider_source = provider.read_text(encoding="utf-8")
    before = '    pool_key = get_custom_provider_pool_key(base_url, provider_name=provider_name)'
    if provider_source.count(before) != 1:
        raise ValueError("Lite provider patch anchor changed")
    after = (
        '    # AgentsRuntime: managed instance credentials outrank saved endpoint pools.\n'
        '    managed_name = str(provider_name or "").strip().lower()\n'
        '    if (os.environ.get("CLAWMANAGER_HERMES_DESKTOP_WEB_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}\n'
        '            and (managed_name == "clawmanager" or managed_name.startswith("clawmanager-"))):\n'
        '        return None\n' + before
    )
    provider.write_text(provider_source.replace(before, after), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    apply(Path(sys.argv[1]))
