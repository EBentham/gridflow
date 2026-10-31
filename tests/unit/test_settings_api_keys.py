"""API keys reach the source configs by one route: each source's ``api_key_env``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gridflow.config.settings import load_settings

if TYPE_CHECKING:
    import pytest


def test_api_key_env_is_the_only_key_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both GIE sources take ``GIE_API_KEY``; a stale prefixed variable is ignored."""
    monkeypatch.setenv("GIE_API_KEY", "gie-key")
    monkeypatch.setenv("GRIDFLOW_GIE_API_KEY", "stale-prefixed-key")

    config = load_settings()

    assert config.get_source_config("gie_agsi").api_key == "gie-key"
    assert config.get_source_config("gie_alsi").api_key == "gie-key"
