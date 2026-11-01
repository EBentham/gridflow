"""Shared test fixtures for gridflow tests."""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from dotenv import load_dotenv

from gridflow.config import settings as settings_module
from gridflow.config.settings import (
    DatasetConfig,
    GridflowConfig,
    PipelineSettings,
    QualityConfig,
    SourceConfig,
)
from gridflow.connectors import base as connector_base
from gridflow.connectors.base import RawResponse

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env", override=False)
_PATH_ENV_VARS = ("GRIDFLOW_DATA_DIR", "GRIDFLOW_DUCKDB_PATH", "GRIDFLOW_LOG_DIR")


@pytest.fixture(autouse=True)
def _clear_gridflow_path_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear data-root overrides so local machine settings cannot leak into tests."""
    for env_var in _PATH_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)

    original_load_dotenv = settings_module.load_dotenv

    def load_dotenv_without_path_env(*args: object, **kwargs: object) -> bool:
        protected = {env_var for env_var in _PATH_ENV_VARS if env_var in os.environ}
        loaded = original_load_dotenv(*args, **kwargs)
        for env_var in _PATH_ENV_VARS:
            if env_var not in protected:
                monkeypatch.delenv(env_var, raising=False)
        return loaded

    monkeypatch.setattr(settings_module, "load_dotenv", load_dotenv_without_path_env)


@pytest.fixture(autouse=True)
def _fresh_connector_send_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connector pacing is shared process-wide, so no test inherits another's slots."""
    monkeypatch.setattr(connector_base, "_NEXT_SEND_AT", {})


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Keep live API tests opt-in even when local credentials are present."""
    markexpr = config.option.markexpr or ""
    if "live" in markexpr and "not live" not in markexpr:
        return

    skip_live = pytest.mark.skip(reason="select live tests explicitly with -m live")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


@pytest.fixture
def stub_neso_resolver(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """Stub the NESO connector's name resolver with one globally-routable answer.

    Under D-39 §1a every send validates its target, and validation performs a
    real host lookup. respx mocks HTTP, **not** name resolution, so without this
    a mocked NESO fetch would resolve ``api.neso.energy`` for real — a live
    network call in the default suite.

    **Non-autouse, and defined here rather than in a test module, deliberately.**
    It lives in the repo-root conftest so ``tests/integration/`` can reach it
    too; it is inert by default so the other five sources' suites are
    unaffected. Every NESO test module opts in with a module-level
    ``pytestmark = pytest.mark.usefixtures("stub_neso_resolver")``; the live
    module simply does not.

    Returns:
        The ``(host, port)`` pairs the stub was asked to resolve, so a test can
        prove the helper was actually consulted — a refactor that bypasses it
        would otherwise silently reintroduce real DNS while the suite stayed
        green.
    """
    import ipaddress
    from typing import Any

    calls: list[tuple[str, int]] = []

    async def _stub_resolver(host: str, port: int) -> list[Any]:
        calls.append((host, port))
        return [ipaddress.ip_address("93.184.216.34")]

    monkeypatch.setattr(
        "gridflow.connectors.neso_data_portal.client._resolve_host_addresses",
        _stub_resolver,
    )
    return calls


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory structure."""
    for layer in ["bronze", "silver", "gold"]:
        (tmp_path / layer).mkdir()
    return tmp_path


_SHORT_PARENT_NAME = "gft"
_STALE_SECONDS = 24 * 3600


@pytest.fixture(scope="session")
def short_base() -> Iterator[Path]:
    """This session's directory for short data roots, under ``<drive>:/gft``.

    A family key appears twice in an engine output path, so the silver and
    completion names pass Windows' 260-character MAX_PATH below pytest's long
    ``tmp_path``; on Windows the parent sits at the drive root, elsewhere under
    the system temp. The session removes its directory at the end, and the
    parent too once no other session uses it. A session killed before its
    teardown leaves its directory behind, so a directory untouched for a day is
    removed when the next session starts.
    """
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    parent = Path(drive + os.sep if drive else tempfile.gettempdir()) / _SHORT_PARENT_NAME
    parent.mkdir(exist_ok=True)
    for stale in parent.iterdir():
        if stale.stat().st_mtime < time.time() - _STALE_SECONDS:
            shutil.rmtree(stale, ignore_errors=True)
    base = Path(tempfile.mkdtemp(prefix="", dir=parent))
    yield base
    shutil.rmtree(base)
    with contextlib.suppress(OSError):
        parent.rmdir()


@pytest.fixture
def short_root(short_base: Path) -> Iterator[Path]:
    """A fresh short data root for one test, removed at teardown.

    The root name is eight characters, so the longest engine path stays under
    MAX_PATH. Removal errors are not ignored: a root that cannot be removed
    fails the test instead of being left on the drive.
    """
    root = Path(tempfile.mkdtemp(prefix="", dir=short_base))
    yield root
    shutil.rmtree(root)


@dataclass(frozen=True)
class IsolatedPaths:
    """The data, catalogue and log locations :func:`isolated_env` points gridflow at."""

    data_dir: Path
    duckdb_path: Path
    log_dir: Path


@pytest.fixture
def isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> IsolatedPaths:
    """Point every ``GRIDFLOW_*`` path under ``tmp_path``, with the gold views off.

    The gold SQL views read silver relations an empty data dir lacks, so in
    strict mode they would fail ``init_catalogue``.
    """
    paths = IsolatedPaths(
        data_dir=tmp_path / "data",
        duckdb_path=tmp_path / "catalogue" / "gridflow.duckdb",
        log_dir=tmp_path / "logs",
    )
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(paths.data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(paths.duckdb_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(paths.log_dir))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return paths


def _run_fresh(code: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return result.stdout


@pytest.fixture
def run_fresh() -> Callable[..., str]:
    """``run_fresh(code)``: run ``code`` in a new interpreter.

    The interpreter starts at the project root with nothing imported, which is
    what a cold-import or bootstrap check needs. A non-zero exit fails the
    test with both streams; the return value is the child's stdout.
    """
    return _run_fresh


@pytest.fixture
def sample_config(tmp_data_dir: Path) -> GridflowConfig:
    """Create a sample GridflowConfig for testing."""
    return GridflowConfig(
        pipeline=PipelineSettings(
            data_dir=tmp_data_dir,
            log_dir=tmp_data_dir / "logs",
            duckdb_path=tmp_data_dir / "test.duckdb",
            default_lookback_hours=24,
            log_level="DEBUG",
        ),
        quality=QualityConfig(),
        sources={
            "elexon": SourceConfig(
                base_url="https://data.elexon.co.uk/bmrs/api/v1",
                api_key="test-key",
                api_key_header="x-api-key",
                rate_limit_per_second=100,
                timeout=5,
                datasets={
                    "system_prices": DatasetConfig(
                        endpoint="/balancing/settlement/system-prices",
                        schedule="hourly",
                        max_query_days=1,
                    ),
                },
            ),
        },
    )


@pytest.fixture
def sample_elexon_response_data() -> dict:
    """Sample Elexon system prices API response."""
    return {
        "data": [
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 45.50,
                "systemBuyPrice": 55.00,
                "netImbalanceVolume": -120.5,
                "settlementRunType": "SF",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 2,
                "systemSellPrice": 46.75,
                "systemBuyPrice": 56.25,
                "netImbalanceVolume": 80.3,
                "settlementRunType": "SF",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 3,
                "systemSellPrice": 48.00,
                "systemBuyPrice": 58.50,
                "netImbalanceVolume": -45.0,
                "settlementRunType": "SF",
            },
        ]
    }


@pytest.fixture
def sample_raw_response(sample_elexon_response_data: dict) -> RawResponse:
    """Create a sample RawResponse for testing."""
    body = json.dumps(sample_elexon_response_data).encode()
    return RawResponse(
        body=body,
        content_type="application/json",
        source="elexon",
        dataset="system_prices",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        request_url="https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices",
        request_params={"settlementDate": "2024-01-15"},
        api_version="v1",
        page=1,
        total_pages=1,
        http_status=200,
    )
