import os

import pytest

from agents.prioritizer import prioritizer_llm_requested


@pytest.mark.parametrize(
    "value,expected_llm",
    [
        (None, True),
        ("llm", True),
        ("deterministic", False),
        ("heuristic", False),
        ("off", False),
        ("false", False),
        ("0", False),
        ("no", False),
    ],
)
def test_prioritizer_llm_requested(monkeypatch, value, expected_llm):
    if value is None:
        monkeypatch.delenv("E100_PRIORITIZER_MODE", raising=False)
    else:
        monkeypatch.setenv("E100_PRIORITIZER_MODE", value)
    assert prioritizer_llm_requested() is expected_llm
