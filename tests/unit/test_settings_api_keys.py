"""CH2-02 / CH-COR-04: the GIE key must resolve via the PipelineSettings field.

Today the GIE key only reaches the source configs through the
``get_source_config`` ``os.environ`` fallback (the un-prefixed ``GIE_API_KEY``).
Elexon / ENTSO-E / ENTSO-G each have a dedicated ``*_api_key`` field plus a
``key_map`` entry, so the prefixed ``GRIDFLOW_*_API_KEY`` env var flows uniformly
into the source config. GIE was missing that field + mapping.

These tests set only the *prefixed* field var (``GRIDFLOW_GIE_API_KEY``) and
explicitly clear the un-prefixed fallback (``GIE_API_KEY``), so the key can only
arrive via the new field + key_map path — not the os.environ fallback.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from gridflow.config.settings import PipelineSettings, load_settings

if TYPE_CHECKING:
    import pytest


def test_gie_api_key_field_reads_prefixed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """``GRIDFLOW_GIE_API_KEY`` must populate the ``gie_api_key`` field.

    Mirrors how ``GRIDFLOW_ELEXON_API_KEY`` populates ``elexon_api_key``.
    """
    # Force the un-prefixed fallback empty rather than deleting it: load_dotenv
    # (override=False) would otherwise re-populate a *deleted* GIE_API_KEY from
    # the on-disk .env. An explicit "" is "present" so .env can't overwrite it.
    monkeypatch.setenv("GIE_API_KEY", "")
    monkeypatch.setenv("GRIDFLOW_GIE_API_KEY", "prefixed-gie-key")

    settings = PipelineSettings()
    assert settings.gie_api_key == "prefixed-gie-key"


def test_gie_key_map_wires_both_gie_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """The field-resolved GIE key must reach BOTH gie_agsi and gie_alsi.

    RED before the fix: no ``gie_api_key`` field / ``key_map`` entry existed, so
    with the un-prefixed ``GIE_API_KEY`` fallback cleared, both source configs
    resolved to an empty key.
    """
    # Empty (not deleted) un-prefixed fallback so the only path to the key is
    # the field + key_map. get_source_config falls back to os.environ[GIE_API_KEY];
    # an explicit "" both neutralises that fallback and survives load_dotenv
    # (override=False won't overwrite a present var with the on-disk .env value).
    monkeypatch.setenv("GIE_API_KEY", "")
    monkeypatch.setenv("GRIDFLOW_GIE_API_KEY", "field-gie-key")

    config = load_settings()

    assert config.pipeline.gie_api_key == "field-gie-key"
    assert config.get_source_config("gie_agsi").api_key == "field-gie-key"
    assert config.get_source_config("gie_alsi").api_key == "field-gie-key"
