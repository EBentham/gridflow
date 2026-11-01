"""The sample store: a tiny data root built from committed fixtures.

Bronze from ``tests/_bronze_fixtures.py`` is transformed through the pipeline
runner, as ``gridflow transform`` does, and the catalogue is registered over
it. It lives in pytest's temp folder for the session and is never committed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _bronze_fixtures import (
    FIXTURE_DATE,
    seed_gie_fixture,
    write_carbon_intensity_bronze,
    write_entsoe_fixture_bronze,
    write_entsog_flows_bronze,
    write_fuelhh_bronze,
    write_openmeteo_historical_bronze,
    write_system_prices_bronze,
)

from gridflow.config.settings import load_settings
from gridflow.pipeline import runner
from gridflow.silver.gie.agsi import GasStorageTransformer
from gridflow.silver.gie.alsi import LNGTerminalTransformer

if TYPE_CHECKING:
    from pathlib import Path

# The datasets the sample store holds, per source.
SAMPLE_DATASETS = {
    "elexon": ["fuelhh", "system_prices"],
    "entsoe": ["day_ahead_prices"],
    "neso": ["carbon_intensity"],
    "open_meteo": ["historical_demand"],
    "entsog": ["physical_flows"],
    "gie_agsi": ["storage"],
    "gie_alsi": ["lng"],
}


def _write_bronze(root: Path) -> None:
    write_fuelhh_bronze(root)
    write_system_prices_bronze(root)
    write_entsoe_fixture_bronze(root, "day_ahead_prices", "day_ahead_prices_gb.xml")
    write_carbon_intensity_bronze(root)
    write_openmeteo_historical_bronze(root, FIXTURE_DATE)
    write_entsog_flows_bronze(root)
    seed_gie_fixture(GasStorageTransformer(root), FIXTURE_DATE, "agsi_gb_response.json")
    seed_gie_fixture(LNGTerminalTransformer(root), FIXTURE_DATE, "alsi_gb_response.json")


@pytest.fixture(scope="session")
def sample_store(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A data root holding the fixture datasets in silver, with its catalogue."""
    root = tmp_path_factory.mktemp("sample_store")
    _write_bronze(root)
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("GRIDFLOW_DATA_DIR", str(root))
        mp.setenv("GRIDFLOW_DUCKDB_PATH", str(root / "gridflow.duckdb"))
        mp.setenv("GRIDFLOW_LOG_DIR", str(root / "logs"))
        # Register as `gridflow` does outside pytest: a gold view whose inputs the
        # store lacks (the GB benchmark reads MID) is skipped with a warning, as on
        # any partial store, rather than raising. GRIDFLOW_ENV is pinned empty, not
        # unset, so a `.env` read by load_settings cannot turn strict mode back on.
        mp.delenv("PYTEST_CURRENT_TEST", raising=False)
        mp.setenv("GRIDFLOW_ENV", "")
        settings = load_settings()
        runner.import_transformers()
        day = FIXTURE_DATE.isoformat()
        start, end = runner.resolve_dates(day, day, None, 0)
        with runner.build_context(settings) as ctx:  # init_catalogue
            for source, datasets in SAMPLE_DATASETS.items():
                for result in runner.run_transform(ctx, source, datasets, start, end):
                    assert result.status != "failed", result
        runner.refresh_views(settings)
    return root
