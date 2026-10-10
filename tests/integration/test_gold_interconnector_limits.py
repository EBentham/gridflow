"""The held gold view ``gold_gb_interconnector_limits`` (v0.22-G, ADR-041).

The world: one capture per current link family on 2026-10-08, in the real
registry's identities and vendor headers. The six dumps are ``datastore``
captures without a CKAN ``last_modified`` (``available_at`` = capture time);
BritNed is an upload whose flows stay vendor text. ElecLink also carries one
archive-epoch row (no start instant, no issue time) in its second resource, and
``ifa_itl`` an original (written 08:30, uploaded 08:20, 1000 MW) and a
correction (12:00, 11:50, 900 MW) of one target, plus a target only the
original states. The catalogue is the real ``refresh_views``; the view is held,
so each test executes its SQL itself (``register_held``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

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
from gridflow.connectors.neso_data_portal.eligibility import CLOCK_LABELS
from gridflow.gold.contracts import contract_for, sql_path
from gridflow.silver.schema_manifest import select_list_columns

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

RELATION = "gold_gb_interconnector_limits"
CONTRACT = contract_for(RELATION)
DAY = date(2026, 10, 8)
DUMP_HEADER = (
    "Data Upload Time GMT",
    "Auction Type",
    "Operational Period Start Date and Time GMT",
    "Flow in MW To GB",
    "Reason For Restriction To GB",
    "Flow in MW From GB",
    "Reason For Restriction From GB",
)
ARCHIVE_HEADER = (
    "Operational Date",
    "Auction Type",
    "Version",
    "Hourly Time Period (GMT)",
    "Flow (MW) To GB",
    "Flow (MW) From GB",
    "Reason For Reduction",
)
BRITNED_HEADER = (
    "Operational Date (YYYY-MM-DD) & Time GMT/BST (HH:MM - HH:MM)",
    "Flow (MW) To GB",
    "Flow (MW) From GB",
    "Reason For Restriction",
)
TARGET = "2026-10-09T00:00:00"
OTHER_TARGET = "2026-10-09T01:00:00"
# family -> (to GB, from GB) of its single dump row
DUMPS: dict[str, tuple[int, int]] = {
    "eleclink": (1001, 951),
    "ifa2_ifa_itl": (1003, 953),
    "nemolink_ntc": (1004, 954),
    "nsl": (1005, 955),
    "viking_link_ntc": (1006, 956),
}
FAMILIES = (
    "eleclink",
    "ifa_itl",
    "ifa2_ifa_itl",
    "nemolink_ntc",
    "nsl",
    "viking_link_ntc",
    "brit_ned",
)
VALUE_COLUMNS = (
    "resource_id",
    "auction_type",
    "operational_period_start_gmt",
    "operational_date",
    "hourly_time_period",
    "operational_date_and_hour",
    "flow_to_gb_mw",
    "flow_from_gb_mw",
    "flow_to_gb_mw_raw",
    "flow_from_gb_mw_raw",
    "reason_for_restriction_to_gb",
    "reason_for_restriction_from_gb",
    "reason_for_restriction",
    "issue_time",
)


@dataclass(frozen=True)
class IcWorld:
    """One built world: its data root, catalogue and expected rows per family."""

    data: Path
    db: Path
    expected: dict[str, list[dict[str, Any]]]
    ifa_original: str
    ifa_correction: str


def _resource(family: str, index: int) -> str:
    package, _family = registry_module.load_registry().families[family]
    return [r for r in package.resources if r.family == family][index].id


def _dump_row(to_gb: int, from_gb: int, tag: str, start: str, upload: str) -> list[str]:
    return [upload, "DayAhead", start, str(to_gb), f"to {tag}", str(from_gb), f"from {tag}"]


def _dump_expected(
    resource: str | None, to_gb: int, from_gb: int, tag: str, start: str, upload: str
) -> dict[str, Any]:
    return {
        "resource_id": resource,
        "auction_type": "DayAhead",
        "operational_period_start_gmt": utc(*_parts(start)),
        "operational_date": None,
        "hourly_time_period": None,
        "operational_date_and_hour": None,
        "flow_to_gb_mw": float(to_gb),
        "flow_from_gb_mw": float(from_gb),
        "flow_to_gb_mw_raw": None,
        "flow_from_gb_mw_raw": None,
        "reason_for_restriction_to_gb": f"to {tag}",
        "reason_for_restriction_from_gb": f"from {tag}",
        "reason_for_restriction": None,
        "issue_time": utc(*_parts(upload)),
    }


def _parts(text: str) -> tuple[int, int, int, int, int]:
    day, clock = text.split("T")
    year, month, mday = (int(part) for part in day.split("-"))
    hour, minute, _second = (int(part) for part in clock.split(":"))
    return year, month, mday, hour, minute


_WITH_RESOURCE = {"eleclink", "nemolink_ntc", "nsl"}


def _build(data: Path, seed: Callable[[Path], None]) -> IcWorld:
    expected: dict[str, list[dict[str, Any]]] = {family: [] for family in FAMILIES}
    upload = "2026-10-08T07:00:00"
    for index, (family, (to_gb, from_gb)) in enumerate(DUMPS.items()):
        tag = family
        capture(
            data,
            family,
            0,
            DUMP_HEADER,
            [_dump_row(to_gb, from_gb, tag, TARGET, upload)],
            utc(2026, 10, 8, 9, index),
            None,
        )
        resource = _resource(family, 0) if family in _WITH_RESOURCE else None
        expected[family].append(_dump_expected(resource, to_gb, from_gb, tag, TARGET, upload))

    capture(
        data,
        "eleclink",
        1,
        ARCHIVE_HEADER,
        [["2019-01-01", "Day Ahead", "1", "00:00 - 01:00", "800", "700", "archive reduction"]],
        utc(2026, 10, 8, 9, 30),
        utc(2026, 10, 8, 6, 0),
    )
    expected["eleclink"].append(
        {
            "resource_id": _resource("eleclink", 1),
            "auction_type": "Day Ahead",
            "operational_period_start_gmt": None,
            "operational_date": "2019-01-01",
            "hourly_time_period": "00:00 - 01:00",
            "operational_date_and_hour": None,
            "flow_to_gb_mw": 800.0,
            "flow_from_gb_mw": 700.0,
            "flow_to_gb_mw_raw": None,
            "flow_from_gb_mw_raw": None,
            "reason_for_restriction_to_gb": None,
            "reason_for_restriction_from_gb": None,
            "reason_for_restriction": "archive reduction",
            "issue_time": None,
        }
    )

    original = capture(
        data,
        "ifa_itl",
        0,
        DUMP_HEADER,
        [
            _dump_row(1000, 950, "ifa original", TARGET, "2026-10-08T08:20:00"),
            _dump_row(1100, 1050, "ifa original", OTHER_TARGET, "2026-10-08T08:20:00"),
        ],
        utc(2026, 10, 8, 8, 30),
        None,
    )
    correction = capture(
        data,
        "ifa_itl",
        0,
        DUMP_HEADER,
        [_dump_row(900, 850, "ifa correction", TARGET, "2026-10-08T11:50:00")],
        utc(2026, 10, 8, 12, 0),
        None,
    )
    expected["ifa_itl"] += [
        _dump_expected(None, 1000, 950, "ifa original", TARGET, "2026-10-08T08:20:00"),
        _dump_expected(None, 1100, 1050, "ifa original", OTHER_TARGET, "2026-10-08T08:20:00"),
        _dump_expected(None, 900, 850, "ifa correction", TARGET, "2026-10-08T11:50:00"),
    ]

    capture(
        data,
        "brit_ned",
        0,
        BRITNED_HEADER,
        [["2026-10-08 00:00 - 01:00", "1016", "990", "britned reason"]],
        utc(2026, 10, 8, 9, 40),
        utc(2026, 10, 8, 7, 0),
    )
    expected["brit_ned"].append(
        {
            "resource_id": _resource("brit_ned", 0),
            "auction_type": None,
            "operational_period_start_gmt": None,
            "operational_date": None,
            "hourly_time_period": None,
            "operational_date_and_hour": "2026-10-08 00:00 - 01:00",
            "flow_to_gb_mw": None,
            "flow_from_gb_mw": None,
            "flow_to_gb_mw_raw": "1016",
            "flow_from_gb_mw_raw": "990",
            "reason_for_restriction_to_gb": None,
            "reason_for_restriction_from_gb": None,
            "reason_for_restriction": "britned reason",
            "issue_time": None,
        }
    )
    for family in FAMILIES:
        run_family(data, family, DAY)
    db = catalogue(data, seed)
    return IcWorld(data, db, expected, original, correction)


@pytest.fixture
def world(seed_silver: Callable[..., None]) -> Iterator[IcWorld]:
    """The G-3 world, built once per test; the held view is NOT registered yet."""
    with short_root() as data:
        yield _build(data, seed_silver)


def _view(db: Path) -> list[dict[str, Any]]:
    return query(db, f'SELECT * FROM "{RELATION}"').to_dicts()


def _key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(row[column]) for column in VALUE_COLUMNS)


def _package_slug(family: str) -> str:
    package, _family = registry_module.load_registry().families[family]
    return package.package


class TestInterconnectorLimits:
    """T-G3-1 ... T-G3-6."""

    def test_t_g3_1_one_row_per_silver_row_and_no_capture_time_target(self, world: IcWorld) -> None:
        """T-G3-1 (I-1, FM-11): detects a dropped or duplicated vintage row, the
        capture-time ``timestamp_utc`` projected as if it were a target, or a
        wrong link label."""
        register_held(world.db, CONTRACT)
        rows = _view(world.db)
        assert "timestamp_utc" not in view_columns(world.db, RELATION)
        for family in FAMILIES:
            silver = query(world.db, f'SELECT COUNT(*) AS n FROM "silver_{SOURCE}_{family}"')
            mine = [row for row in rows if row["family"] == family]
            assert len(mine) == silver["n"][0] == len(world.expected[family]), family
            assert {row["link"] for row in mine} == {_package_slug(family)}, family

    def test_t_g3_2_every_value_lands_in_its_named_column(self, world: IcWorld) -> None:
        """T-G3-2 (FM-12): detects a positional ``UNION ALL`` misalignment: a flow,
        raw flow, reason, label or start instant in the wrong column, or a
        cross-type column that is not NULL."""
        register_held(world.db, CONTRACT)
        rows = _view(world.db)
        for family in FAMILIES:
            got = sorted(
                (
                    {column: row[column] for column in VALUE_COLUMNS}
                    for row in rows
                    if row["family"] == family
                ),
                key=_key,
            )
            assert got == sorted(world.expected[family], key=_key), family

    def test_t_g3_3_original_then_correction_by_as_of(self, world: IcWorld) -> None:
        """T-G3-3 (I-3): detects a correction leaking into an as-of before it was
        captured, or the correction not replacing its target afterwards; SQL and
        Polars must agree."""
        register_held(world.db, CONTRACT)
        target = utc(2026, 10, 9, 0, 0)

        other = utc(2026, 10, 9, 1, 0)

        def ifa(as_of_hour: int) -> dict[Any, tuple[Any, Any]]:
            rows = both_as_of(world.db, CONTRACT, utc(2026, 10, 8, as_of_hour, 0))
            mine = rows.filter(rows["family"] == "ifa_itl")
            return {
                row["operational_period_start_gmt"]: (
                    row["flow_to_gb_mw"],
                    row["bronze_capture_id"],
                )
                for row in mine.to_dicts()
            }

        original, correction = world.ifa_original, world.ifa_correction
        assert ifa(9) == {target: (1000.0, original), other: (1100.0, original)}
        # C-4: per-target selection keeps the target the correction omits.
        assert ifa(13) == {target: (900.0, correction), other: (1100.0, original)}

    def test_t_g3_4_null_key_parts_survive_as_their_own_keys(self, world: IcWorld) -> None:
        """T-G3-4 (FM-8): detects NULL key parts (BritNed labels, the ElecLink
        archive row) collapsing into another row's key or vanishing."""
        register_held(world.db, CONTRACT)
        chosen = both_as_of(world.db, CONTRACT, utc(2026, 10, 8, 13, 0))
        brit = chosen.filter(chosen["family"] == "brit_ned")
        assert brit["operational_date_and_hour"].to_list() == ["2026-10-08 00:00 - 01:00"]
        eleclink = chosen.filter(chosen["family"] == "eleclink")
        assert eleclink.height == 2
        archive = eleclink.filter(eleclink["operational_period_start_gmt"].is_null())
        assert archive["operational_date"].to_list() == ["2019-01-01"]
        assert archive["issue_time"].to_list() == [None]

    def test_t_g3_5_projection_types_and_labelled_clocks(self, world: IcWorld) -> None:
        """T-G3-5 (I-5, I-6): detects a manifest projection that differs from the
        registered columns, a wrong target type, an unlabelled or mislabelled
        clock, or a flow comment that reads as a signed flow."""
        register_held(world.db, CONTRACT)
        sql = sql_path(CONTRACT).read_text(encoding="utf-8")
        columns = view_columns(world.db, RELATION)
        assert list(select_list_columns(sql, origin=RELATION)) == columns
        described = query(
            world.db,
            "SELECT column_name, data_type, comment FROM duckdb_columns() "
            f"WHERE table_name = '{RELATION}' ORDER BY column_index",
        )
        types = dict(zip(described["column_name"], described["data_type"], strict=True))
        comments = dict(zip(described["column_name"], described["comment"], strict=True))
        assert types[CONTRACT.designated_date_col] == "TIMESTAMP WITH TIME ZONE"
        loaded = registry_module.load_registry()
        rows = _view(world.db)
        for family in FAMILIES:
            record = loaded.families[family][1].record
            assert record is not None
            labels = {row["available_at_basis"] for row in rows if row["family"] == family}
            assert labels == {CLOCK_LABELS[record.vintage]}, family
        assert "available_at = gridflow capture time" in str(comments["available_at"])
        for column in ("flow_to_gb_mw", "flow_from_gb_mw"):
            assert "not a signed flow" in str(comments[column]), column
        for column in ("flow_to_gb_mw_raw", "flow_from_gb_mw_raw"):
            assert "uncast" in str(comments[column]), column
        assert "$as_of" not in sql

    def test_t_g3_6_the_real_catalogue_does_not_register_it(self, world: IcWorld) -> None:
        """T-G3-6 (I-4): detects the held view registered by the default
        ``refresh_views``."""
        views = set(
            query(world.db, "SELECT table_name FROM information_schema.views")["table_name"]
        )
        assert "gold_eu_gas_storage" in views
        assert RELATION not in views
