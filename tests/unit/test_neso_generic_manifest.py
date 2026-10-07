"""Generated NESO families in the silver schema manifest (ADR-034 P-12; T-B9-1..3).

A generated family exports its frozen record's columns, its recipe's designated
date column and SQL type, and its ``_latest`` relation name. Across the whole
manifest every designated date name keeps one SQL type: the invariant
``gridflow_models``' ``_load()`` loop relies on (E16), replicated here without
importing ``gridflow_models``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from _neso_generic_support import install_generated
from _neso_registry_support import column, epoch, family, package, record, resource, sp_columns

from gridflow.silver.neso_data_portal.casting import record_columns
from gridflow.silver.schema_manifest import DESIGNATED_DATE_COLS, get_silver_schema_manifest

SOURCE = "neso_data_portal"
PKG = "dddddddd-0000-4000-8000-000000000000"


def _instant_record() -> dict[str, Any]:
    columns = [
        column("Time", "time", "datetime", format="%Y-%m-%dT%H:%M", zone="UTC", nullable=False),
        column("Unit", "unit", nullable=False),
        column("Value", "value", "float64"),
    ]
    return record(
        epochs=[epoch(columns)],
        temporal={"kind": "utc_instant", "column": "time"},
        entity_key=("time", "unit"),
    )


def _forecast_date_record() -> dict[str, Any]:
    columns = [
        column("ForecastDate", "forecast_date", "date", format="%Y-%m-%d", nullable=False),
        column("Unit", "unit", nullable=False),
        column("Value", "value", "float64"),
    ]
    return record(
        epochs=[epoch(columns)],
        temporal={"kind": "date_sp1", "date_column": "forecast_date"},
        entity_key=("forecast_date", "unit"),
    )


FAMILIES: dict[str, dict[str, Any]] = {
    "gen_sp": record(epochs=[epoch(sp_columns())]),
    "gen_inst": _instant_record(),
    "gen_fd_one": _forecast_date_record(),
    "gen_fd_two": _forecast_date_record(),
}
EXPECTED_DATE = {
    "gen_sp": ("settlement_date", "DATE"),
    "gen_inst": ("timestamp_utc", "TIMESTAMPTZ"),
    "gen_fd_one": ("forecast_date", "DATE"),
    "gen_fd_two": ("forecast_date", "DATE"),
}


@pytest.fixture
def generated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    entries = [family(key, record=rec) for key, rec in FAMILIES.items()]
    resources = [
        resource(f"dddddddd-0000-4000-8000-00000000000{index}", key.title(), key)
        for index, key in enumerate(FAMILIES, start=1)
    ]
    _registry, generated_set = install_generated(
        monkeypatch, tmp_path / "reg", [package("pkg-gen", PKG, entries, resources)]
    )
    return generated_set


def test_t_b9_1_a_generated_family_exports_its_frozen_record(generated: Any) -> None:
    """Detects a generated family exported with no columns, the wrong date
    column, or the base view instead of its ``_latest`` relation."""
    entries = {
        entry.dataset: entry
        for entry in get_silver_schema_manifest(include_serving_aliases=False)
        if entry.source == SOURCE
    }
    for key, (date_col, sql_type) in EXPECTED_DATE.items():
        entry = entries[key]
        assert entry.columns == record_columns(generated.transformers[key].RECORD)
        assert entry.columns_source == "frozen_record"
        assert (entry.designated_date_col, entry.date_col_sql_type) == (date_col, sql_type)
        assert entry.partition_columns == ("year", "month")
        assert entry.relation_name == f"silver_{SOURCE}_{key}_latest"
        assert entry.qualified_view == f"silver_{SOURCE}_{key}"


def test_t_b9_2_no_generated_key_in_designated_date_cols(generated: Any) -> None:
    assert not {(SOURCE, key) for key in generated.transformers} & set(DESIGNATED_DATE_COLS)


def test_t_b9_3_every_designated_date_name_keeps_one_sql_type(generated: Any) -> None:
    """The models ``_load()`` loop (E16): one SQL type per designated date name,
    across the real registry plus the generated families."""
    seen: dict[str, str] = {}
    entries = get_silver_schema_manifest(include_serving_aliases=True)
    assert {entry.dataset for entry in entries if entry.source == SOURCE} >= set(FAMILIES)
    for entry in entries:
        sql_type = seen.setdefault(entry.designated_date_col, entry.date_col_sql_type)
        assert sql_type == entry.date_col_sql_type, (
            f"{entry.relation_name}: {entry.designated_date_col} is {entry.date_col_sql_type} "
            f"here and {sql_type} elsewhere"
        )


def test_t_b9_3_the_registry_import_stays_off_the_silver_layer() -> None:
    """V-13 reads ``silver.date_columns``; importing the registry must not pull
    ``silver.base`` or the manifest (P-12's relocation)."""
    script = (
        "import sys\n"
        "import gridflow.connectors.neso_data_portal.registry as r\n"
        "r.load_registry()\n"
        "print(sorted(m for m in ('gridflow.silver.base', 'gridflow.silver.schema_manifest') "
        "if m in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"
