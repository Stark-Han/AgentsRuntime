"""Expose only the already running Team agent to its Redis adapter.

No generic busy-input policy, gateway-control flag or interruption changes.
The adapter checks current business identity before calling native steer().
"""
import re
import sys
from pathlib import Path

MARKER = "clawmanager-team-context-reader-v1"


def patch(source: str) -> str:
    if MARKER in source:
        return source
    anchor = "    async def _handle_active_session_busy_message(self, event: MessageEvent, session_key: str) -> bool:"
    if source.count(anchor) != 1:
        raise RuntimeError("Unsupported Hermes active-session boundary")
    method = '''    # clawmanager-team-context-reader-v1
    def _clawmanager_team_active_agent(self, source):
        if getattr(source.platform, "value", source.platform) != "redis_team":
            return None
        key = self._session_key_for_source(source)
        if hasattr(self, "_peek_session_state"):
            state = self._peek_session_state(key)
            return state.turn.agent if state and state.turn else None
        return self._running_agents.get(key)

'''
    source = source.replace(anchor, method + anchor)
    expression = r"(?m)^( +)adapter\.set_busy_session_handler\(self\._handle_active_session_busy_message\)$"
    def register(match):
        indent = match.group(1)
        return match.group(0) + "\n" + indent + 'if hasattr(adapter, "set_clawmanager_team_agent_reader"):\n' + indent + "    adapter.set_clawmanager_team_agent_reader(self._clawmanager_team_active_agent)"
    source, count = re.subn(expression, register, source)
    if count != 2:
        raise RuntimeError(f"Unsupported Hermes adapter registration boundary: {count}")
    return source


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.write_text(patch(target.read_text(encoding="utf-8")), encoding="utf-8")
