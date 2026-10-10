"""The held gold view ``gold_gb_wind_forecast_vs_outturn`` (v0.22-G, ADR-041).

The world: five captures of the national day-ahead wind forecast on the autumn
long day 2025-10-26 (50 periods), targets SP1, SP2, SP49 and SP50:

- O (original, written 08:30, CKAN last_modified 08:25) and C (correction,
  12:00 / 11:55) are complete;
- X (12:30) loses its completion record (an orphan output, FM-1);
- Y (12:40) has its completion ``row_count`` rewritten to 5 (FM-2);
- E (12:50) has its completion rewritten to ``valid_empty`` / 0 while its four
  silver rows remain (FM-16).

The metered output has SP1, SP2 and SP49 in resource R1 and a conflicting SP49
in resource R2; SP50 has no outturn. Captures use the real registry's identities
and vendor headers and run through the real generated transformers; the
catalogue is the real ``refresh_views``. The view is held, so each test executes
its SQL itself (``register_held``) after the catalogue exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import pytest
from _gold_support import (
    SOURCE,
    both_as_of,
    capture,
    catalogue,
    query,
    register_held,
    run_family,
    short_root,
    utc,
    view_columns,
)

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.gold.contracts import GOLD_VIEW_CONTRACTS, contract_for, sql_path
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, _resolve_selection, latest_select_sql
from gridflow.silver.neso_data_portal.completion import (
    completion_path,
    read_completion,
    record_completion,
)
from gridflow.silver.schema_manifest import select_list_columns

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from datetime import datetime
    from pathlib import Path

RELATION = "gold_gb_wind_forecast_vs_outturn"
FORECAST = "da_wind_forecast_day_ahead"
METERED = "metered_wind_output_monthly"
F_HEADER = ("Datetime_GMT", "Date", "Settlement_period", "Capacity", "Incentive_forecast")
M_HEADER = (
    "Sett_Date",
    "Sett_Period",
    "Scottish Wind Output",
    "England/Wales Wind Output",
    "Total",
)
TARGET_DAY = "2025-10-26"
TARGETS = (
    (1, "2025-10-25T23:00:00"),
    (2, "2025-10-25T23:30:00"),
    (49, "2025-10-26T23:00:00"),
    (50, "2025-10-26T23:30:00"),
)
CAPTURE_DAY = date(2025, 10, 25)
METERED_DAY = date(2025, 11, 30)
# capture -> (written, last_modified, first Incentive_forecast value)
FORECASTS: dict[str, tuple[datetime, datetime, int]] = {
    "O": (utc(2025, 10, 25, 8, 30), utc(2025, 10, 25, 8, 25), 100),
    "C": (utc(2025, 10, 25, 12, 0), utc(2025, 10, 25, 11, 55), 200),
    "X": (utc(2025, 10, 25, 12, 30), utc(2025, 10, 25, 12, 25), 300),
    "Y": (utc(2025, 10, 25, 12, 40), utc(2025, 10, 25, 12, 35), 400),
    "E": (utc(2025, 10, 25, 12, 50), utc(2025, 10, 25, 12, 45), 500),
}
R1_ROWS = (
    ("1", "4000", "6000", "10000"),
    ("2", "4100", "6100", "10200"),
    ("49", "4200", "6200", "10400"),
)
R2_ROWS = (("49", "4300", "6300", "10600"),)
OUTTURN_VALUES = ("outturn_total_mw", "outturn_scottish_mw", "outturn_england_wales_mw")


@dataclass(frozen=True)
class WindWorld:
    """One built world: its data root, catalogue, capture ids and clocks."""

    data: Path
    db: Path
    ids: dict[str, str]
    available: dict[str, datetime]
    r1: str
    r1_available_at: datetime


def _metered_resource(index: int) -> str:
    package, _family = registry_module.load_registry().families[METERED]
    return [r for r in package.resources if r.family == METERED][index].id


def _build(data: Path, seed: Callable[[Path], None], *, tie: bool = False) -> WindWorld:
    ids: dict[str, str] = {}
    for name, (written, lm, base) in FORECASTS.items():
        if tie and name == "C":
            lm = FORECASTS["O"][1]
        rows = [
            [instant, TARGET_DAY, str(sp), "15000", str(base + i)]
            for i, (sp, instant) in enumerate(TARGETS)
        ]
        ids[name] = capture(data, FORECAST, 0, F_HEADER, rows, written, lm)
    r1 = capture(
        data,
        METERED,
        0,
        M_HEADER,
        [[TARGET_DAY, *row] for row in R1_ROWS],
        utc(2025, 11, 30, 9, 0),
        utc(2025, 11, 30, 8, 0),
    )
    capture(
        data,
        METERED,
        1,
        M_HEADER,
        [[TARGET_DAY, *row] for row in R2_ROWS],
        utc(2025, 11, 30, 9, 10),
        utc(2025, 11, 30, 8, 5),
    )
    run_family(data, FORECAST, CAPTURE_DAY)
    run_family(data, METERED, METERED_DAY)

    available: dict[str, datetime] = {}
    for name, cid in ids.items():
        row = read_completion(data, FORECAST, cid)
        assert row is not None and row["outcome"] == "populated", name
        available[name] = row["available_at"]
    completion_path(data, FORECAST, ids["X"]).unlink()
    miscount = read_completion(data, FORECAST, ids["Y"])
    assert miscount is not None
    miscount["row_count"] = 5
    record_completion(data, miscount)
    mislabelled = read_completion(data, FORECAST, ids["E"])
    assert mislabelled is not None
    mislabelled["outcome"] = "valid_empty"
    mislabelled["row_count"] = 0
    record_completion(data, mislabelled)
    r1_completion = read_completion(data, METERED, r1)
    assert r1_completion is not None

    db = catalogue(data, seed)
    return WindWorld(data, db, ids, available, _metered_resource(0), r1_completion["available_at"])


@pytest.fixture
def world(seed_silver: Callable[..., None]) -> Iterator[WindWorld]:
    """The G-2 world, built once per test; the held view is NOT registered yet."""
    with short_root() as data:
        yield _build(data, seed_silver)


@pytest.fixture
def tie_world(seed_silver: Callable[..., None]) -> Iterator[WindWorld]:
    """The G-2 world with O and C sharing one CKAN last_modified (FM-9)."""
    with short_root() as data:
        yield _build(data, seed_silver, tie=True)


CONTRACT = contract_for(RELATION)


def _view(db: Path) -> list[dict[str, object]]:
    return query(
        db, f'SELECT * FROM "{RELATION}" ORDER BY bronze_capture_id, timestamp_utc'
    ).to_dicts()


class TestWindForecastVsOutturn:
    """T-G2-1 ... T-G2-7."""

    def test_t_g2_1_original_then_correction_by_as_of(self, world: WindWorld) -> None:
        """T-G2-1 (I-3): detects a correction leaking into an as-of before it was
        available, or an incomplete capture (X, Y, E, all later) winning because
        the complete-capture filter is missing; SQL and Polars must agree."""
        register_held(world.db, CONTRACT)
        early = both_as_of(world.db, CONTRACT, utc(2025, 10, 25, 9, 0))
        assert set(early["bronze_capture_id"]) == {world.ids["O"]}
        assert sorted(early["incentive_forecast_mw"]) == [100.0, 101.0, 102.0, 103.0]
        late = both_as_of(world.db, CONTRACT, utc(2025, 10, 25, 13, 0))
        assert set(late["bronze_capture_id"]) == {world.ids["C"]}
        assert sorted(late["incentive_forecast_mw"]) == [200.0, 201.0, 202.0, 203.0]
        latest = both_as_of(world.db, CONTRACT, utc(2025, 10, 26, 23, 59))
        assert set(latest["bronze_capture_id"]) == {world.ids["C"]}

    def test_t_g2_2_outturn_conflicts_absences_and_row_counts(self, world: WindWorld) -> None:
        """T-G2-2 (I-1, I-2, FM-4/5/10): detects a duplicated forecast row, an
        invented precedence between conflicting outturn resources, an outturn on
        a target without one, or a collapsed vintage, on the 50-period day."""
        register_held(world.db, CONTRACT)
        rows = _view(world.db)
        silver = query(
            world.db,
            f'SELECT bronze_capture_id, COUNT(*) AS n FROM "silver_{SOURCE}_{FORECAST}" '
            "GROUP BY bronze_capture_id",
        )
        silver_counts = dict(zip(silver["bronze_capture_id"], silver["n"], strict=True))
        for name in ("O", "C"):
            cid = world.ids[name]
            assert sum(1 for row in rows if row["bronze_capture_id"] == cid) == silver_counts[cid]
            assert silver_counts[cid] == 4
        assert len(rows) == 8
        by_sp: dict[int, list[dict[str, object]]] = {}
        for row in rows:
            assert row["settlement_date"] == date(2025, 10, 26)
            by_sp.setdefault(int(str(row["settlement_period"])), []).append(row)
        assert sorted(by_sp) == [1, 2, 49, 50]
        for row in by_sp[1]:
            assert row["outturn_rows"] == 1
            assert row["outturn_total_mw"] == 10000.0
            assert row["outturn_scottish_mw"] == 4000.0
            assert row["outturn_england_wales_mw"] == 6000.0
            assert row["outturn_resource_id"] == world.r1
            assert row["outturn_available_at"] == world.r1_available_at
        for row in by_sp[49]:
            assert row["outturn_rows"] == 2
            for column in (*OUTTURN_VALUES, "outturn_resource_id", "outturn_available_at"):
                assert row[column] is None, column
        for row in by_sp[50]:
            assert row["outturn_rows"] is None
            for column in (*OUTTURN_VALUES, "outturn_resource_id", "outturn_available_at"):
                assert row[column] is None, column
        assert {row["timestamp_utc"] for row in by_sp[49]} == {utc(2025, 10, 26, 23, 0)}

    def test_t_g2_3_only_complete_captures_enter(self, world: WindWorld) -> None:
        """T-G2-3 (P-3, FM-1/2/3/16): detects an orphan output, a miscounted
        completion, or a ``valid_empty`` completion beside silver rows letting a
        capture into the view."""
        register_held(world.db, CONTRACT)
        ids = {row["bronze_capture_id"] for row in _view(world.db)}
        assert ids == {world.ids["O"], world.ids["C"]}

    def test_t_g2_4_equal_vintages_break_on_capture_written_at(self, tie_world: WindWorld) -> None:
        """T-G2-4 (FM-9): detects a scan-order winner when two captures share one
        ``available_at``; the later ``capture_written_at`` (C) must win."""
        register_held(tie_world.db, CONTRACT)
        assert tie_world.available["O"] == tie_world.available["C"]
        late = both_as_of(tie_world.db, CONTRACT, utc(2025, 10, 25, 13, 0))
        assert set(late["bronze_capture_id"]) == {tie_world.ids["C"]}
        assert late.height == 4

    def test_t_g2_5_complete_capture_parity_with_the_silver_selector(
        self, world: WindWorld
    ) -> None:
        """T-G2-5 (P-3): detects the gold complete-capture filter disagreeing with
        the silver whole-capture selector on the populated arm."""
        register_held(world.db, CONTRACT)
        in_view = {row["bronze_capture_id"] for row in _view(world.db)}
        base = f"silver_{SOURCE}_{FORECAST}"
        spec = LATEST_VIEW_SPECS[(SOURCE, FORECAST)]
        select = latest_select_sql(base, spec, set(view_columns(world.db, base)), as_of_param=True)
        assert select is not None
        verdicts: dict[str, bool] = {}
        for name in ("O", "C", "X", "Y"):
            chosen = query(world.db, select, {"as_of": world.available[name].isoformat()})
            in_silver = world.ids[name] in set(chosen["bronze_capture_id"])
            assert (world.ids[name] in in_view) == in_silver, name
            verdicts[name] = in_silver
        assert verdicts == {"O": True, "C": True, "X": False, "Y": False}

    def test_t_g2_6_the_real_catalogue_does_not_register_it(self, world: WindWorld) -> None:
        """T-G2-6 (I-4, FM-6): detects a held view registered by the default
        ``refresh_views`` (its SQL inside the globbed directory)."""
        views = set(
            query(world.db, "SELECT table_name FROM information_schema.views")["table_name"]
        )
        assert {
            "gold_gb_day_ahead_benchmark",
            "gold_uk_imbalance_context",
            "gold_eu_gas_storage",
        } <= views
        for contract in GOLD_VIEW_CONTRACTS:
            assert contract.relation_name not in views

    def test_t_g2_7_comments_projection_and_static_sql(self, world: WindWorld) -> None:
        """T-G2-7 (I-2, I-5): detects a missing ex-post or fail-closed comment, a
        manifest projection that differs from the registered columns, a wrong
        date type, a spec that cannot resolve on the view, or an as-of parameter
        inside the view text."""
        register_held(world.db, CONTRACT)
        sql = sql_path(CONTRACT).read_text(encoding="utf-8")
        described = query(
            world.db,
            "SELECT column_name, data_type, comment FROM duckdb_columns() "
            f"WHERE table_name = '{RELATION}' ORDER BY column_index",
        )
        comments = dict(zip(described["column_name"], described["comment"], strict=True))
        types = dict(zip(described["column_name"], described["data_type"], strict=True))
        for column in OUTTURN_VALUES:
            assert str(comments[column]).startswith("EX-POST:"), column
        assert "fail-closed" in str(comments["outturn_available_at"])
        assert "select_latest_vintage" in str(comments["available_at"])
        columns = view_columns(world.db, RELATION)
        assert list(select_list_columns(sql, origin=RELATION)) == columns
        assert list(described["column_name"]) == columns
        assert CONTRACT.designated_date_col in columns
        assert types[CONTRACT.designated_date_col] == CONTRACT.date_col_sql_type
        assert _resolve_selection(CONTRACT.point_in_time, set(columns)) is not None
        assert "$as_of" not in sql
        assert sql.count("CREATE OR REPLACE VIEW") == 1
