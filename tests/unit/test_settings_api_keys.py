"""API keys reach the source configs by one route: each source's ``api_key_env``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gridflow.config.settings import PipelineSettings, load_settings

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_api_key_env_is_the_only_key_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both GIE sources take ``GIE_API_KEY``; a stale prefixed variable is ignored."""
    prefixed = f"{PipelineSettings.model_config['env_prefix']}GIE_API_KEY"
    monkeypatch.setenv("GIE_API_KEY", "gie-key")
    monkeypatch.setenv(prefixed, "stale-prefixed-key")

    config = load_settings()

    assert config.get_source_config("gie_agsi").api_key == "gie-key"
    assert config.get_source_config("gie_alsi").api_key == "gie-key"


def test_dotenv_is_found_from_the_current_folder_upwards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``.env`` in the folder you run from, or above it, gives keys to an installed gridflow."""
    (tmp_path / ".env").write_text("GIE_API_KEY=from-dotenv\n", encoding="utf-8")
    nested = tmp_path / "project" / "notebooks"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    # setenv first so teardown removes the value load_dotenv writes straight into os.environ.
    monkeypatch.setenv("GIE_API_KEY", "placeholder")
    monkeypatch.delenv("GIE_API_KEY")

    config = load_settings()

    assert config.get_source_config("gie_agsi").api_key == "from-dotenv"
