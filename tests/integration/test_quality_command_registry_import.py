"""Regression: `quality()` must import transformers itself (F-16 follow-up).

Every other pipeline command (`ingest`/`transform`/`pipeline`/`backfill`,
`cli.py:288`, `:371`, `:516`) calls ``runner.import_transformers()`` before
touching the silver registry. ``quality()`` did not, so in a genuinely fresh
process the registry is empty, ``_entity_key_for`` always falls back to the
legacy ``(settlement_date, settlement_period)`` pair, and F-16's fix
(``cli.py:_entity_key_for``, keying the duplicate check on the dataset's real
entity grain) becomes a no-op in production.

Pytest's collection-time imports populate the process-global
``gridflow.silver.registry._REGISTRY`` ambiently, which conceals this bug for
any in-process test (including one that clears ``_REGISTRY`` by hand --
Python caches already-imported modules, so re-running
``import_transformers()`` in the SAME process would not re-execute the
transformer modules' top-level ``register_transformer()`` calls and would
give a false negative). This test drives the CLI in a genuinely separate
subprocess instead, so it cannot ride on any ambient import.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from typing import TYPE_CHECKING

import duckdb
import polars as pl

from gridflow.storage.parquet import write_parquet
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path


def _write_fuelhh_fixture(data_dir: Path) -> None:
    """20 distinct fuel types at the same (settlement_date, settlement_period).

    Shaped exactly like ``test_quality_duplicate_keys.py``'s false-positive
    fixture: real (non-duplicate) rows under fuelhh's actual entity key
    (``settlement_date``, ``settlement_period``, ``fuel_type``), but reported
    as one giant duplicate group under the legacy 2-column fallback.
    """
    df = pl.DataFrame(
        {
            "settlement_date": [date(2026, 7, 11)] * 20,
            "settlement_period": [1] * 20,
            "fuel_type": [f"FUEL_{i}" for i in range(20)],
            "generation_mw": [float(i) for i in range(20)],
        }
    )
    paths = PathBuilder(data_dir)
    path = paths.silver_file("elexon", "fuelhh", date(2026, 7, 11))
    write_parquet(df, path)


def test_quality_command_resolves_the_real_entity_key_in_a_fresh_process(
    tmp_path: Path,
) -> None:
    """RED against the pre-fix `quality()`: without `import_transformers()`,
    the registry is empty, `_entity_key_for` falls back to the legacy pair,
    and this fixture is misreported as one duplicate-key group (metric 1.0,
    failed) instead of 0 genuine duplicates."""
    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    _write_fuelhh_fixture(data_dir)

    # Inherit the full parent environment (Windows subprocess creation needs
    # more than PATH/SYSTEMROOT -- a minimal env dict was observed to make
    # some Windows API write a literal "%SystemDrive%" cache dir into cwd)
    # and only override the GRIDFLOW_* settings this test cares about.
    env = dict(os.environ)
    env["GRIDFLOW_DATA_DIR"] = str(data_dir)
    env["GRIDFLOW_DUCKDB_PATH"] = str(db_path)
    env["GRIDFLOW_LOG_DIR"] = str(tmp_path / "logs")
    env["ELEXON_API_KEY"] = "test-key"

    result = subprocess.run(
        [sys.executable, "-c", "from gridflow.cli import app; app()", "quality", "--all"],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )

    assert result.returncode == 0, (
        f"quality command failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT metric, passed, detail FROM quality_reports "
            "WHERE source = 'elexon' AND dataset = 'fuelhh' AND check_name = 'duplicates'"
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == 1, f"expected exactly one duplicate_check row, got {rows}"
    metric, passed, details = rows[0]
    assert passed is True, (
        "duplicate check failed on genuinely distinct fuel_type rows -- "
        f"quality() resolved the legacy (settlement_date, settlement_period) fallback "
        f"instead of fuelhh's real entity key (registry was not populated): {details}"
    )
    assert metric == 0.0
