"""scripts/seed_canonical_schema.py, which seeds docs/CANONICAL_SCHEMA.yaml."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

from gridflow.config.settings import DatasetConfig, SourceConfig

if TYPE_CHECKING:
    from types import ModuleType

    import pytest


def _load_script() -> ModuleType:
    script_path = Path(__file__).parents[2] / "scripts" / "seed_canonical_schema.py"
    spec = importlib.util.spec_from_file_location("gridflow_seed_canonical_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_cadence_map_is_read_through_load_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """The script walked sources.yaml with yaml.safe_load, so datasets the loader
    fills (the portal's, from its registry) were stamped cadence "unknown"."""
    mod = _load_script()
    source = SourceConfig(
        base_url="https://example.test",
        datasets={"hourly": DatasetConfig(schedule="hourly"), "plain": DatasetConfig()},
    )
    monkeypatch.setattr(
        mod, "load_settings", lambda: SimpleNamespace(sources={"fake": source}), raising=False
    )

    assert mod._load_cadence_map() == {("fake", "hourly"): "hourly", ("fake", "plain"): "daily"}
