"""Regression: native custom-profile default must not break opaque CM routes."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace, ModuleType
import sys
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / "patches/hermes-agent/lite_non_native.py"
spec = importlib.util.spec_from_file_location("managed_reasoning_policy", path)
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class ManagedReasoningDefaultTests(unittest.TestCase):
    def test_only_implicit_custom_default_of_verified_lite_agent_is_omitted(self):
        for sealed, mode, profile, omit in [
            (True, "chat_completions", "custom", True),
            (False, "chat_completions", "custom", False),
            (True, "codex_responses", "custom", False),
            (True, "chat_completions", "deepseek", False),
            (True, "chat_completions", None, False),
        ]:
            with self.subTest(sealed=sealed, mode=mode, profile=profile):
                provider_module = ModuleType("providers")
                provider_module.get_provider_profile = lambda _: SimpleNamespace(name=profile) if profile else None
                agent = SimpleNamespace(provider="custom:clawmanager", api_mode=mode)
                if sealed:
                    agent._clawmanager_non_native_seal = policy._AGENT_SEAL
                original = {"enabled": True, "effort": "medium"}
                with patch.dict(sys.modules, {"providers": provider_module}):
                    result = policy.managed_reasoning_default(lambda _: original)(agent)
                self.assertIs(None if omit else original, result)

    def test_profile_failure_preserves_upstream_fallback(self):
        provider_module = ModuleType("providers")
        def fail(_):
            raise ValueError("not loaded")
        provider_module.get_provider_profile = fail
        agent = SimpleNamespace(provider="unknown", api_mode="chat_completions",
                                _clawmanager_non_native_seal=policy._AGENT_SEAL)
        with patch.dict(sys.modules, {"providers": provider_module}):
            self.assertEqual("fallback", policy.managed_reasoning_default(lambda _: "fallback")(agent))


if __name__ == "__main__":
    unittest.main()
