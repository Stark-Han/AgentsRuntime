"""Keep Lite process identity and deployment boundaries out of workspace dotenv.

Imported at the beginning of the pinned env loader, before any workspace load.
Only the Lite image installs this module. Unknown user keys keep the upstream
dotenv semantics, including interpolation and override precedence.
"""
from functools import wraps
import os
import threading

_PREFIXES = ("CLAWMANAGER_", "HERMES_DASHBOARD_BASIC_AUTH_", "HERMES_MANAGED_",
             "PYTHON", "LD_", "XDG_")
_KEYS = frozenset({
    "HOME", "HERMES_HOME", "HERMES_PROFILE", "HERMES_MANAGED", "HOST", "PORT", "PATH",
    "HERMES_ACCEPT_HOOKS", "NODE_OPTIONS", "NODE_PATH", "BASH_ENV", "ENV",
    "OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_API_BASE", "CUSTOM_BASE_URL", "OPENAI_MODEL",
})


def protected(key):
    return key in _KEYS or key.startswith(_PREFIXES)


_INITIAL = {key: value for key, value in os.environ.items() if protected(key)}
_LOCK = threading.RLock()


def _restore():
    for key in list(os.environ):
        if protected(key) and key not in _INITIAL:
            os.environ.pop(key, None)
    os.environ.update(_INITIAL)


def preserve_environment(fn):
    @wraps(fn)
    def guarded(*args, **kwargs):
        with _LOCK:
            _restore()
            try:
                return fn(*args, **kwargs)
            finally:
                # Secret-provider/config reload paths run inside this loader.
                _restore()
    return guarded


def load_dotenv(*, dotenv_path=None, stream=None, override=False, encoding="utf-8"):
    # Use the same pinned parser, but filter before assignment: restoring only
    # after load_dotenv would expose transient attacker values to other threads.
    from dotenv.main import DotEnv
    with _LOCK:
        _restore()
        values = DotEnv(dotenv_path=dotenv_path, stream=stream, verbose=False,
                        encoding=encoding, interpolate=True, override=override).dict()
        for key, value in values.items():
            if protected(key) or value is None or (not override and key in os.environ):
                continue
            os.environ[key] = value
        return bool(values)
