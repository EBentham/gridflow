"""Integrity matrix over the live catalogue, one case per registered (source, dataset).

Each case reads the bronze and silver files and the run log under
``GRIDFLOW_DATA_DIR``, so the cases are marked ``slow`` (out of the default
gate) and skip when that variable is unset or the catalogue is absent. Run them
with ``uv run pytest tests/integration/test_pipeline_backfill_integrity.py``;
the matrix lands in ``.pytest_artifacts/integrity-matrix.md``. The two
``test_skip_rationale_logic_*`` tests are not gated and always run.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from gridflow.pipeline.runner import import_transformers
from gridflow.silver.registry import list_transformers

if TYPE_CHECKING:
    from collections.abc import Iterator

# The registry fills only on import, and must be full before list_transformers()
# parametrises the tests.
import_transformers()

# Read at import: the root conftest clears the GRIDFLOW_* paths before every test.
_DATA_DIR = os.environ.get("GRIDFLOW_DATA_DIR")

# ---------------------------------------------------------------------------
# Pinned known-empty / skip-with-rationale allowlist. Only entries where the
# harness verdict is SKIP belong here; reference data expected to PASS is not
# listed.
# ---------------------------------------------------------------------------
_PINNED_KNOWN_EMPTY: dict[tuple[str, str], str] = {
    # Yearly cadence — 7-day window doesn't trigger a publication
    ("entsoe", "installed_capacity_units"): "yearly cadence",
    ("entsoe", "load_forecast_yearly"): "yearly cadence",
    ("entsoe", "forecast_margin"): "yearly cadence",
    # Monthly cadence — May publication typically not yet out by 2026-05-08
    ("entsoe", "load_forecast_monthly"): "monthly cadence",
    ("entsoe", "balancing_financial_expenses_income"): "monthly cadence",
    ("entsog", "tariffs"): "monthly cadence",
    ("entsog", "tariff_simulations"): "monthly cadence",
    # Event-driven — empty for a quiet week is normal
    ("entsog", "urgent_market_messages"): "event-driven, empty for quiet week",
    ("elexon", "remit"): "event-driven, empty for quiet week",
}


# ---------------------------------------------------------------------------
# Verdict helpers
# ---------------------------------------------------------------------------


def _api_call_succeeded(data_dir: Path, source: str, dataset: str) -> bool:
    """Return True iff the most-recent ingest pipeline_run for this pair was successful."""
    import duckdb  # noqa: PLC0415

    db = data_dir / "gridflow.duckdb"
    if not db.exists():
        return False
    con = duckdb.connect(str(db), read_only=True)
    try:
        row = con.execute(
            "SELECT status, error_message FROM pipeline_runs "
            "WHERE source=? AND dataset=? AND operation='ingest' "
            "ORDER BY started_at DESC LIMIT 1",
            [source, dataset],
        ).fetchone()
        if row is None:
            return False
        return row[0] == "success" and row[1] is None
    finally:
        con.close()


def _bronze_is_api_empty(source: str, bronze_dir: Path) -> bool:
    """Return True iff every bronze file in ``bronze_dir`` contains an empty API response.

    Used to distinguish "API returned nothing" (→ SKIP) from "transformer bug on
    real data" (→ FAIL) when bronze exists but silver is absent.
    """
    if source == "entsoe":
        return _entsoe_bronze_all_empty(bronze_dir)
    if source == "entsog":
        return _entsog_bronze_all_empty(bronze_dir)
    return False


def _entsoe_bronze_all_empty(bronze_dir: Path) -> bool:
    """True iff every XML bronze file is an Acknowledgement_MarketDocument
    or has zero TimeSeries."""
    import xml.etree.ElementTree as ET  # noqa: PLC0415, N817

    xml_files = [f for f in bronze_dir.rglob("raw_*.xml") if not f.name.endswith(".meta.json")]
    if not xml_files:
        return False
    for xml_file in xml_files:
        try:
            root = ET.parse(xml_file).getroot()
            local_name = root.tag.split("}")[-1] if "}" in root.tag else root.tag
            if local_name == "Acknowledgement_MarketDocument":
                continue
            ts_count = sum(
                1
                for child in root
                if (child.tag.split("}")[-1] if "}" in child.tag else child.tag) == "TimeSeries"
            )
            if ts_count == 0:
                continue
            return False  # document has real TimeSeries data
        except ET.ParseError:
            return False  # malformed XML — treat as potentially real data
    return True


def _entsog_bronze_all_empty(bronze_dir: Path) -> bool:
    """True iff every JSON bronze file has no non-empty top-level data array.

    Handles two ENTSOG empty-response formats:
    - ``{"meta": {...}, "<key>": []}`` — standard empty response
    - ``{"message": "No result found"}`` — error-style empty response
    """
    import json as _json  # noqa: PLC0415

    json_files = [f for f in bronze_dir.rglob("raw_*.json") if not f.name.endswith(".meta.json")]
    if not json_files:
        return False
    for json_file in json_files:
        try:
            payload = _json.loads(json_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False  # unreadable — treat as potentially real data
        if not isinstance(payload, dict):
            return False
        # If any top-level list value is non-empty, this file has real data.
        if any(isinstance(v, list) and len(v) > 0 for v in payload.values()):
            return False
    return True


def _decide(
    source: str,
    dataset: str,
    data_dir: Path,
    bronze_files: int,
    silver_files: int,
    silver_rows: int,
    has_event_time: bool,
    has_available_at: bool,
    et_tz: str | None,
    aa_tz: str | None,
    pinned_known_empty: dict[tuple[str, str], str],
) -> tuple[str, str]:
    """Map per-dataset facts to (verdict, last_error).

    Verdict codes: PASS / FAIL / SKIP.
    FAIL last_error values: BRONZE_MISSING, SILVER_EMPTY_DESPITE_BRONZE,
    SILVER_ZERO_ROWS, BITEMPORAL_EVENT_TIME, BITEMPORAL_AVAILABLE_AT.
    SKIP last_error values: CADENCE_NOT_IN_WINDOW:<reason>, API_EMPTY_FOR_WINDOW.
    """
    # A pin excuses an empty window, never the silver that does exist.
    if (source, dataset) in pinned_known_empty and silver_rows == 0:
        return "SKIP", f"CADENCE_NOT_IN_WINDOW: {pinned_known_empty[(source, dataset)]}"
    if bronze_files == 0 and silver_files == 0:
        if _api_call_succeeded(data_dir, source, dataset):
            return "SKIP", "API_EMPTY_FOR_WINDOW"
        return "FAIL", "BRONZE_MISSING"
    if bronze_files > 0 and silver_files == 0:
        bronze_dir = data_dir / "bronze" / source / dataset
        if _api_call_succeeded(data_dir, source, dataset) and _bronze_is_api_empty(
            source, bronze_dir
        ):
            return "SKIP", "API_EMPTY_FOR_WINDOW"
        return "FAIL", "SILVER_EMPTY_DESPITE_BRONZE"
    if silver_rows == 0:
        return "FAIL", "SILVER_ZERO_ROWS"
    if not has_event_time or et_tz != "UTC":
        return "FAIL", "BITEMPORAL_EVENT_TIME"
    if not has_available_at or aa_tz != "UTC":
        return "FAIL", "BITEMPORAL_AVAILABLE_AT"
    return "PASS", ""


def _compute_row(
    source: str,
    dataset: str,
    data_dir: Path,
    pinned_known_empty: dict[tuple[str, str], str],
) -> dict[str, object]:
    """Compute one integrity matrix row for a (source, dataset) pair."""
    bronze_dir = data_dir / "bronze" / source / dataset
    silver_dir = data_dir / "silver" / source / dataset

    bronze_files = (
        sum(1 for _ in bronze_dir.rglob("raw_*.json"))
        + sum(1 for _ in bronze_dir.rglob("raw_*.xml"))
        if bronze_dir.exists()
        else 0
    )
    silver_files = sum(1 for _ in silver_dir.rglob("*.parquet")) if silver_dir.exists() else 0

    has_event_time = has_available_at = False
    silver_rows = 0
    et_tz: str | None = None
    aa_tz: str | None = None

    if silver_files > 0:
        sample = next(silver_dir.rglob("*.parquet"))
        schema = pl.scan_parquet(sample).collect_schema()
        silver_rows = int(
            pl.scan_parquet(str(silver_dir / "**" / "*.parquet")).select(pl.len()).collect().item()
        )
        has_event_time = "event_time" in schema.names()
        has_available_at = "available_at" in schema.names()
        if has_event_time:
            dt = schema["event_time"]
            et_tz = dt.time_zone if isinstance(dt, pl.Datetime) else None
        if has_available_at:
            dt = schema["available_at"]
            aa_tz = dt.time_zone if isinstance(dt, pl.Datetime) else None

    verdict, last_error = _decide(
        source,
        dataset,
        data_dir,
        bronze_files,
        silver_files,
        silver_rows,
        has_event_time,
        has_available_at,
        et_tz,
        aa_tz,
        pinned_known_empty,
    )
    return {
        "source": source,
        "dataset": dataset,
        "bronze_files": bronze_files,
        "silver_files": silver_files,
        "silver_rows": silver_rows,
        "has_event_time": has_event_time,
        "has_available_at": has_available_at,
        "last_error": last_error,
        "verdict": verdict,
    }


# ---------------------------------------------------------------------------
# SKIP-rationale unit tests (not gated, always run)
# ---------------------------------------------------------------------------


def test_skip_rationale_logic_pinned_known_empty(tmp_path: Path) -> None:
    """Pinned-known-empty rows must produce SKIP with CADENCE_NOT_IN_WINDOW prefix."""
    verdict, err = _decide(
        "entsoe",
        "load_forecast_yearly",
        tmp_path,
        bronze_files=0,
        silver_files=0,
        silver_rows=0,
        has_event_time=False,
        has_available_at=False,
        et_tz=None,
        aa_tz=None,
        pinned_known_empty=_PINNED_KNOWN_EMPTY,
    )
    assert verdict == "SKIP"
    assert err.startswith("CADENCE_NOT_IN_WINDOW:")
    assert "yearly cadence" in err


def test_a_pinned_dataset_with_silver_is_still_validated(tmp_path: Path) -> None:
    """The pin excuses an empty window only: silver that exists is checked, so a
    pinned dataset whose rows lack the event-time column still fails."""
    verdict, err = _decide(
        "entsoe",
        "load_forecast_yearly",
        tmp_path,
        bronze_files=1,
        silver_files=1,
        silver_rows=10,
        has_event_time=False,
        has_available_at=True,
        et_tz=None,
        aa_tz="UTC",
        pinned_known_empty=_PINNED_KNOWN_EMPTY,
    )
    assert (verdict, err) == ("FAIL", "BITEMPORAL_EVENT_TIME")


def test_skip_rationale_logic_generic_no_data_is_fail(tmp_path: Path) -> None:
    """Generic 'no data' WITHOUT pipeline_runs success → FAIL, not SKIP.

    tmp_path has no gridflow.duckdb, so _api_call_succeeded returns False
    without raising — the intended FAIL path is preserved.
    """
    verdict, err = _decide(
        "elexon",
        "system_prices",
        tmp_path,  # not in _PINNED_KNOWN_EMPTY
        bronze_files=0,
        silver_files=0,
        silver_rows=0,
        has_event_time=False,
        has_available_at=False,
        et_tz=None,
        aa_tz=None,
        pinned_known_empty=_PINNED_KNOWN_EMPTY,
    )
    assert verdict == "FAIL"
    assert err in ("BRONZE_MISSING", "NO_PIPELINE_RUN_RECORDED")


def test_every_pinned_entry_is_a_registered_dataset() -> None:
    """A pin for a deleted transformer never matches a case and only hides drift."""
    assert set(_PINNED_KNOWN_EMPTY) - set(list_transformers()) == set()


# ---------------------------------------------------------------------------
# Parametrised integrity harness (gated on GRIDFLOW_DATA_DIR + gridflow.duckdb)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def matrix_rows(request: pytest.FixtureRequest) -> Iterator[list[dict[str, object]]]:
    """Collect one row per case and write the matrix once the module's cases finish."""
    rows: list[dict[str, object]] = []
    yield rows
    if not rows:
        return
    matrix_path = request.config.rootpath / ".pytest_artifacts" / "integrity-matrix.md"
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = matrix_path.with_suffix(".md.tmp")
    tmp.write_text(_render_matrix_md(rows), encoding="utf-8")
    os.replace(tmp, matrix_path)


def _render_matrix_md(rows: list[dict[str, object]]) -> str:
    """Render the integrity matrix rows as markdown."""
    pass_count = sum(1 for r in rows if r["verdict"] == "PASS")
    fail_count = sum(1 for r in rows if r["verdict"] == "FAIL")
    skip_count = sum(1 for r in rows if r["verdict"] == "SKIP")
    rows_sorted = sorted(rows, key=lambda r: (r["source"], r["dataset"]))

    lines: list[str] = [
        "# Silver integrity matrix",
        "",
        f"**Generated:** {datetime.now(UTC).isoformat()}",
        f"**Total rows:** {len(rows)}",
        f"**PASS / FAIL / SKIP:** {pass_count} / {fail_count} / {skip_count}",
        "",
        "| source | dataset | bronze_files | silver_files | silver_rows "
        "| has_event_time | has_available_at | last_error | verdict |",
        "|--------|---------|-------------:|-------------:|------------:"
        "|:--------------:|:----------------:|------------|:-------:|",
    ]
    for r in rows_sorted:
        et = "✓" if r["has_event_time"] else "—"
        aa = "✓" if r["has_available_at"] else "—"
        lines.append(
            f"| {r['source']} | {r['dataset']} | {r['bronze_files']} | {r['silver_files']} | "
            f"{r['silver_rows']} | {et} | {aa} | {r['last_error']} | {r['verdict']} |"
        )
    return "\n".join(lines) + "\n"


@pytest.mark.slow
@pytest.mark.parametrize("source,dataset", sorted(list_transformers()))
def test_integrity_one_dataset(
    source: str, dataset: str, matrix_rows: list[dict[str, object]]
) -> None:
    if not _DATA_DIR:
        pytest.skip("GRIDFLOW_DATA_DIR not set")
    data_dir = Path(_DATA_DIR)
    db = data_dir / "gridflow.duckdb"
    if not db.exists():
        pytest.skip(f"gridflow.duckdb absent at {db}")

    row = _compute_row(source, dataset, data_dir, _PINNED_KNOWN_EMPTY)
    matrix_rows.append(row)

    if row["verdict"] == "FAIL":
        pytest.fail(f"{source}/{dataset}: {row['last_error']}")
