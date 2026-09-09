"""Lite-only network and Origin boundary, installed outside upstream auth.

Uvicorn proxy rewriting MUST remain disabled: ``scope.client`` must be the
socket peer, never an attacker-controlled X-Forwarded-For value.
"""
import ipaddress
import logging
import os
from urllib.parse import parse_qsl, urlsplit

_LOG = logging.getLogger("hermes.lite_boundary")
_NATIVE_ROUTES = ("/api/console", "/api/ssh", "/api/desktop", "/api/cloud", "/api/hermes/update")
INTERNAL_PROTOCOL_PREFIX = "hermes-internal."


def internal_credential(headers):
    values = [item.strip()[len(INTERNAL_PROTOCOL_PREFIX):]
              for key, value in headers if key.lower() == b"sec-websocket-protocol"
              for item in value.decode("latin1").split(",")
              if item.strip().startswith(INTERNAL_PROTOCOL_PREFIX)]
    return values[0] if len(values) == 1 else ""


def verified_internal_peer(scope):
    try:
        if not ipaddress.ip_address(scope["client"][0]).is_loopback:
            return False
        credential = internal_credential(scope.get("headers", []))
        if not credential:
            return False
        from hermes_cli.dashboard_auth.ws_tickets import TicketInvalid, consume_internal_credential
        try:
            consume_internal_credential(credential)
        except TicketInvalid:
            return False
        return True
    except (ValueError, KeyError, TypeError, ImportError):
        return False


def canonical_origin(raw):
    try:
        value = urlsplit(raw)
        if (value.scheme not in {"http", "https"} or not value.hostname or "*" in value.hostname
                or value.username is not None or value.password is not None or value.path not in {"", "/"}
                or value.query or value.fragment or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in raw)):
            return None
        port = value.port
        return (value.scheme, value.hostname.lower(), port if port is not None else (443 if value.scheme == "https" else 80))
    except ValueError:
        return None


class LiteGatewayBoundary:
    def __init__(self, app):
        self.app = app
        self.origin_raw = os.environ.get("CLAWMANAGER_CONTROL_UI_ORIGIN", "")
        self.origin = canonical_origin(self.origin_raw)
        raw = os.environ.get("CLAWMANAGER_TRUSTED_PROXY_CIDRS", "")
        self.proxies = []
        try:
            self.proxies = [ipaddress.ip_network(item.strip(), strict=False)
                            for item in raw.split(",") if item.strip()]
        except ValueError:
            raise RuntimeError("invalid_trusted_proxy_config") from None
        if not self.origin or not self.proxies or any(network.prefixlen == 0 for network in self.proxies):
            raise RuntimeError("missing_lite_network_boundary")

    def denial(self, scope):
        headers = scope.get("headers", [])
        origins = [value.decode("latin1") for name, value in headers if name.lower() == b"origin"]
        forwarded = any(name.lower().startswith(b"x-forwarded-") or name.lower() == b"forwarded"
                        for name, _ in headers)
        try:
            peer = ipaddress.ip_address(scope["client"][0])
        except (ValueError, KeyError, TypeError):
            return "untrusted_gateway_peer"
        trusted = any(peer in network for network in self.proxies)
        if not trusted and (not peer.is_loopback or forwarded):
            return "untrusted_gateway_peer"
        # A server-spawned TUI cannot set an Origin with the standard Node
        # WebSocket API. Only a real loopback peer presenting the process-local
        # credential can be normalized to the deployment's internal Origin.
        if scope["type"] == "websocket" and internal_credential(headers):
            if not verified_internal_peer(scope) or forwarded:
                return "invalid_internal_websocket_peer"
            if not origins:
                headers = list(headers) + [(b"origin", self.origin_raw.encode())]
                scope["headers"] = headers
                origins = [self.origin_raw]
            scope["hermes_lite_internal"] = True
        if len(origins) > 1 or (origins and canonical_origin(origins[0]) != self.origin):
            return "websocket_origin_rejected" if scope["type"] == "websocket" else "origin_rejected"
        if not origins and (scope["type"] == "websocket" or scope.get("method") not in {"GET", "HEAD"}):
            return "origin_required"
        path = scope.get("path", "")
        if any(path == route or path.startswith(route + "/") for route in _NATIVE_ROUTES):
            return "unsupported_native_desktop_api"
        query = parse_qsl(scope.get("query_string", b"").decode("latin1"), keep_blank_values=True)
        if any(name in {"token", "ticket", "internal", "password", "access_token"} for name, _ in query):
            return "credential_query_rejected"
        if path == "/api/pty":
            for name, value in query:
                if name not in {"channel", "resume", "fresh", "attach", "profile"} or (name == "profile" and value not in {"", "current"}):
                    return "unsupported_chat_parameters"
        return None

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            return await self.app(scope, receive, send)
        reason = self.denial(scope)
        if reason:
            # Fixed categories only: never reflect cookies, URLs or headers.
            _LOG.warning("hermes_lite_request_denied reason=%s", reason)
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1008, "reason": reason})
            else:
                await send({"type": "http.response.start", "status": 403,
                            "headers": [(b"content-type", b"application/json"), (b"cache-control", b"no-store")]})
                await send({"type": "http.response.body", "body": ('{"error":"' + reason + '"}').encode()})
            return
        async def safe_send(event):
            if event["type"] == "websocket.accept":
                offered = [part.strip() for key, value in scope.get("headers", [])
                           if key.lower() == b"sec-websocket-protocol"
                           for part in value.decode("latin1").split(",")]
                # Select only the public protocol, never a credential-bearing
                # value; events and PTY otherwise omit protocol selection.
                event = dict(event, subprotocol="hermes-gateway-v1" if "hermes-gateway-v1" in offered else None)
            await send(event)
        await self.app(scope, receive, safe_send)


def install_lite_boundary(app):
    app.add_middleware(LiteGatewayBoundary)
