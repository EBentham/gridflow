"""Per-resource revision selection for multi-resource families (v0.22-DEM-1H, ADR-039).

A ``whole_capture`` record with ``latest_partition = "resource_id"`` keeps the newest
complete capture **per resource** rather than per family (H1), bounded by
``available_at <= as_of`` before selection (B3). Every selection runs through both
renderers (the DuckDB ``SELECT`` and Polars ``select_latest_vintage``) and, for the
randomised rounds, a plain-Python oracle. T-H1 proves every family that existed
before this unit is byte-unchanged against the base golden (H4).
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
import textwrap
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest
from _neso_dem1h_pin import PIN_PATH, dump, generated_pin
from _neso_generic_support import install_generated, write_capture
from _neso_registry_support import (
    column,
    epoch,
    family,
    package,
    record,
    resource,
    write_registry,
)

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import RegistryError, SchemaRecord
from gridflow.connectors.neso_data_portal.registry.record import RESERVED, ColumnSpec
from gridflow.silver.latest_views import (
    LATEST_VIEW_SPECS,
    LatestViewSpec,
    latest_select_sql,
    select_latest_vintage,
)
from gridflow.silver.neso_data_portal.casting import (
    DuplicateEntityKeyError,
    UnmappedResourceFormatError,
)
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    scan_completions,
)
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile
from gridflow.storage.duckdb import init_catalogue

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE = "neso_data_portal"
DEM2_ADDED = frozenset(
    {
        "national_demand_bmus",
        "school_holiday_percentages",
        "transmission_losses_main",
        "transmission_losses_financial_year",
    }
)
"""The four families v0.22-K-DEM-2 records after the golden was written."""
REWORDED_HOLDS = {
    "national_forecast_7d_historic_day_ahead": (
        "TODO: FORECAST_TIMESTAMP has no vendor definition and its zone is undocumented "
        "(values carry Z); whether it is the immutable issue instant"
    ),
}
"""The one hold question reworded since the golden (K-DEM-2, RULINGS 534): the golden's text
claimed a measurement K-DEM-1-FACTS made only for the 1-day, 2-day and 2-14-day archives."""
TS = pl.Datetime("us", "UTC")
TIE = ("capture_written_at", "bronze_capture_id")

PARTITIONED = LatestViewSpec(
    key_columns=(),
    mode="whole_capture",
    completion_relation="completion",
    completion_family="fam",
    completion_partition="resource_id",
    tiebreak_columns=TIE,
)
FAMILY_SCOPE = LatestViewSpec(
    key_columns=(),
    mode="whole_capture",
    completion_relation="completion",
    completion_family="fam",
    tiebreak_columns=TIE,
)


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# T-H1: byte-unchanged (I-1)
# --------------------------------------------------------------------------- #


class TestByteUnchanged:
    def test_t_h1_every_pre_existing_family_matches_the_base_golden(self) -> None:
        """Detects any change to an existing family's ``_latest`` SQL (either as-of mode),
        record dump, output columns or DEM-1 engine output against the golden written on
        the untouched base (master ``73fde80``), and any generated family other than
        ``historic_demand`` and K-DEM-2's four demand-reference records appearing.
        """
        golden = json.loads(PIN_PATH.read_text(encoding="utf-8"))
        for key, question in REWORDED_HOLDS.items():
            assert golden["records"][key]["eligibility"]["question"] != question, key
            golden["records"][key]["eligibility"]["question"] = question
        current = json.loads(dump(generated_pin()))
        for section in ("sql", "records", "columns", "engine"):
            assert set(golden[section]) <= set(current[section]), section
            for key, value in golden[section].items():
                assert current[section][key] == value, (section, key)
            added = {"historic_demand", *DEM2_ADDED} if section != "engine" else set()
            assert set(current[section]) - set(golden[section]) == added, section


# --------------------------------------------------------------------------- #
# Renderer level (T-H2, T-H3, T-H4)
# --------------------------------------------------------------------------- #

Capture = dict[str, Any]
"""``id``, ``resource``, ``available``, ``written``, ``outcome`` (``populated`` /
``valid_empty``), ``rows`` (written output rows), and optionally ``row_count`` (the
completion's count, when it disagrees) or ``completion=False`` (no completion)."""


def _cap(
    capture_id: str,
    resource_id: str,
    available: datetime,
    *,
    written: datetime | None = None,
    outcome: str = "populated",
    rows: int = 1,
    **extra: Any,
) -> Capture:
    return {
        "id": capture_id,
        "resource": resource_id,
        "available": available,
        "written": written if written is not None else available,
        "outcome": outcome,
        "rows": 0 if outcome == "valid_empty" else rows,
        **extra,
    }


def _frames(captures: list[Capture]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The base rows and the completion records of ``captures``."""
    rows: list[dict[str, Any]] = []
    completions: list[dict[str, Any]] = []
    for cap in captures:
        for index in range(cap["rows"]):
            rows.append(
                {
                    "settlement_date": cap.get("date", date(2026, 10, 7)),
                    "settlement_period": index + 1,
                    "resource_id": cap["resource"],
                    "value": float(index),
                    "available_at": cap["available"],
                    "capture_written_at": cap["written"],
                    "bronze_capture_id": cap["id"],
                }
            )
        if cap.get("completion", True):
            completions.append(
                {
                    "family": "fam",
                    "bronze_capture_id": cap["id"],
                    "resource_id": cap["resource"],
                    "outcome": cap["outcome"],
                    "row_count": cap.get("row_count", cap["rows"]),
                    "available_at": cap["available"],
                    "capture_written_at": cap["written"],
                }
            )
    base = pl.DataFrame(
        rows,
        schema={
            "settlement_date": pl.Date,
            "settlement_period": pl.Int64,
            "resource_id": pl.Utf8,
            "value": pl.Float64,
            "available_at": TS,
            "capture_written_at": TS,
            "bronze_capture_id": pl.Utf8,
        },
    )
    ledger = pl.DataFrame(
        completions,
        schema={
            "family": pl.Utf8,
            "bronze_capture_id": pl.Utf8,
            "resource_id": pl.Utf8,
            "outcome": pl.Utf8,
            "row_count": pl.Int64,
            "available_at": TS,
            "capture_written_at": TS,
        },
    )
    return base, ledger


def _utc(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col(name).dt.convert_time_zone("UTC").cast(TS)
        for name, dtype in frame.schema.items()
        if isinstance(dtype, pl.Datetime)
    )


def _sql(
    base: pl.DataFrame, ledger: pl.DataFrame, spec: LatestViewSpec, as_of: datetime | None
) -> pl.DataFrame:
    con = duckdb.connect(":memory:")
    try:
        con.register("base", base.to_arrow())
        con.register("completion", ledger.to_arrow())
        select = latest_select_sql("base", spec, set(base.columns), as_of_param=as_of is not None)
        assert select is not None
        if as_of is None:
            out = con.execute(select).pl()
        else:
            out = con.execute(select, {"as_of": as_of.isoformat()}).pl()
    finally:
        con.close()
    return _utc(out)


def _polars(
    base: pl.DataFrame, ledger: pl.DataFrame, spec: LatestViewSpec, as_of: datetime | None
) -> pl.DataFrame:
    return select_latest_vintage(base.lazy(), spec, as_of, completions=ledger.lazy()).collect()


def _both(
    base: pl.DataFrame,
    ledger: pl.DataFrame,
    spec: LatestViewSpec,
    as_of: datetime | None = None,
) -> pl.DataFrame:
    """Both renderers; asserts they return the same rows (iv); returns them."""
    sql = _sql(base, ledger, spec, as_of)
    pol = _polars(base, ledger, spec, as_of)
    order = sorted(base.columns)
    assert sql.select(order).sort(order).to_dicts() == pol.select(order).sort(order).to_dicts()
    return pol


def _oracle(
    captures: list[Capture], as_of: datetime | None, *, partitioned: bool = True
) -> list[str]:
    """Plain Python: each resource's newest eligible completion with ``available_at <= as_of``.

    Eligible is B's predicate: a valid-empty completion with ``row_count == 0``, or a
    populated one whose output holds exactly ``row_count`` rows. Returns the winning
    captures' row-level capture ids (a multiset, one entry per output row).
    """
    eligible = []
    for cap in captures:
        if not cap.get("completion", True):
            continue
        count = cap.get("row_count", cap["rows"])
        ok = (cap["outcome"] == "valid_empty" and count == 0) or (
            cap["outcome"] == "populated" and cap["rows"] == count
        )
        if ok and (as_of is None or cap["available"] <= as_of):
            eligible.append(cap)
    groups: dict[str, list[Capture]] = {}
    for cap in eligible:
        groups.setdefault(cap["resource"] if partitioned else "", []).append(cap)
    out: list[str] = []
    for members in groups.values():
        winner = max(members, key=lambda c: (c["available"], c["written"], c["id"]))
        out.extend([winner["id"]] * winner["rows"])
    return sorted(out)


def _assert_selection(captures: list[Capture], as_of: datetime | None, expected: list[str]) -> None:
    """(i) winners, (ii) completions and (iii) rows bounded by as_of, (iv) SQL = Polars,
    (v) every resource with an oracle winner present (no vacuous pass)."""
    base, ledger = _frames(captures)
    selected = _both(base, ledger, PARTITIONED, as_of)
    ids = sorted(selected["bronze_capture_id"].to_list())
    assert ids == expected  # (i)
    assert ids == _oracle(captures, as_of)  # (v): nothing the oracle selects is lost
    if as_of is not None:
        chosen = ledger.filter(pl.col("bronze_capture_id").is_in(ids))
        assert (chosen["available_at"] <= as_of).all()  # (ii)
        assert (selected["available_at"] <= as_of).all()  # (iii)


def _row1() -> list[Capture]:
    return [
        _cap("r1a", "R1", _t(8, 30)),
        _cap("r2a", "R2", _t(8, 30)),
        _cap("r1b", "R1", _t(12)),
        _cap("r2b", "R2", _t(12)),
    ]


def _row2() -> list[Capture]:
    return [_cap("r1a", "R1", _t(8, 30)), _cap("r2a", "R2", _t(8, 30)), _cap("r1b", "R1", _t(12))]


def _row3() -> list[Capture]:
    return [_cap("r1a", "R1", _t(8)), _cap("r2a", "R2", _t(10))]


def _row4() -> list[Capture]:
    return [
        _cap("r1a", "R1", _t(8), rows=2),
        _cap("r1b", "R1", _t(10), outcome="valid_empty"),
        _cap("r1c", "R1", _t(12)),
        _cap("r2a", "R2", _t(8)),
    ]


def _row6(variant: str) -> list[Capture]:
    broken: dict[str, Any] = {"row_count": 5} if variant == "count" else {"completion": False}
    return [
        _cap("r1a", "R1", _t(8)),
        _cap("r1b", "R1", _t(12), **broken),
        _cap("r2a", "R2", _t(8)),
    ]


class TestLeakageMatrix:
    """T-H2: H1 and B3 per resource, both renderers."""

    @pytest.mark.parametrize(
        ("name", "captures", "as_of", "expected"),
        [
            ("1 both corrected, before", _row1(), _t(9), ["r1a", "r2a"]),
            ("1 both corrected, latest", _row1(), None, ["r1b", "r2b"]),
            ("2 one corrected, latest", _row2(), None, ["r1b", "r2a"]),
            ("2 one corrected, before", _row2(), _t(9), ["r1a", "r2a"]),
            ("3 first capture after the bound", _row3(), _t(9), ["r1a"]),
            ("3 first capture, latest", _row3(), None, ["r1a", "r2a"]),
            ("4 empty between, before it", _row4(), _t(9), ["r1a", "r1a", "r2a"]),
            ("4 empty between, during it", _row4(), _t(11), ["r2a"]),
            ("4 empty between, latest", _row4(), None, ["r1c", "r2a"]),
            ("6 count mismatch", _row6("count"), None, ["r1a", "r2a"]),
            ("6 no completion", _row6("missing"), None, ["r1a", "r2a"]),
        ],
        ids=lambda value: value if isinstance(value, str) else "",
    )
    def test_each_resource_keeps_its_own_newest_complete_capture(
        self, name: str, captures: list[Capture], as_of: datetime | None, expected: list[str]
    ) -> None:
        """Detects a per-family winner (one resource survives), a bound applied after
        ranking (an earlier eligible resource vanishes), a newer incomplete or
        completion-less capture winning, and a valid-empty capture blanking another
        resource."""
        _assert_selection(captures, as_of, expected)

    def test_row5_ties_resolve_within_each_resource_regardless_of_order(self) -> None:
        """Detects a tie resolved by scan order or by another resource's captures:
        equal ``available_at``, the later ``capture_written_at`` wins."""
        captures = [
            _cap("r1a", "R1", _t(8), written=_t(8)),
            _cap("r1b", "R1", _t(8), written=_t(9)),
            _cap("r2a", "R2", _t(8)),
        ]
        _assert_selection(captures, None, ["r1b", "r2a"])
        for seed in range(6):
            shuffled = list(captures)
            random.Random(seed).shuffle(shuffled)
            _assert_selection(shuffled, None, ["r1b", "r2a"])
        _assert_selection([c for c in captures if c["id"] != "r2a"], None, ["r1b"])

    def test_row7_available_at_equal_to_as_of_is_included_and_later_is_not(self) -> None:
        """Detects a strict ``<`` bound (r1a dropped) or a bound that admits a capture
        one microsecond late (r2a leaked)."""
        as_of = _t(9)
        captures = [
            _cap("r1a", "R1", as_of),
            _cap("r2a", "R2", as_of + timedelta(microseconds=1)),
        ]
        _assert_selection(captures, as_of, ["r1a"])
        _assert_selection(captures, None, ["r1a", "r2a"])

    def test_row8_one_pair_in_two_resources_is_served_by_both(self) -> None:
        """Detects a collapse by the settlement pair across resources (the hard rule:
        never dedup on (date, period) alone); overlap is reconcile's to report."""
        captures = [_cap("r1a", "R1", _t(8)), _cap("r2a", "R2", _t(9))]
        base, ledger = _frames(captures)
        selected = _both(base, ledger, PARTITIONED)
        assert sorted(selected["bronze_capture_id"].to_list()) == ["r1a", "r2a"]
        pairs = selected.select("settlement_date", "settlement_period").unique()
        assert pairs.height == 1


class TestRandomisedParity:
    def test_t_h3_sql_polars_and_oracle_agree(self) -> None:
        """Detects a bound applied after ranking, a lost resource, a tie broken
        differently, or any drift between the two renderers, over random states with
        deliberate ties, incomplete outputs and missing completions."""
        rng = random.Random(20261009)
        for _round in range(40):
            captures: list[Capture] = []
            for r in range(rng.randint(1, 4)):
                for c in range(rng.randint(1, 5)):
                    available = _t(6) + timedelta(minutes=30 * rng.randint(0, 12))
                    outcome = rng.choice(["populated", "populated", "valid_empty"])
                    cap = _cap(
                        f"r{r}{chr(97 + c)}",
                        f"R{r}",
                        available,
                        written=available + timedelta(minutes=rng.randint(0, 2)),
                        outcome=outcome,
                        rows=rng.randint(1, 3),
                    )
                    roll = rng.random()
                    if outcome == "populated" and roll < 0.15:
                        cap["row_count"] = cap["rows"] + 1
                    elif roll < 0.25:
                        cap["completion"] = False
                    captures.append(cap)
            as_of = rng.choice([None, _t(6) + timedelta(minutes=30 * rng.randint(0, 12))])
            base, ledger = _frames(captures)
            selected = _both(base, ledger, PARTITIONED, as_of)
            assert sorted(selected["bronze_capture_id"].to_list()) == _oracle(captures, as_of)


class TestFamilyScopeUnchanged:
    def test_t_h4_without_a_partition_one_winner_spans_every_resource(self) -> None:
        """Detects the partition leaking into a spec that does not set it: the base
        ``LIMIT 1`` keeps exactly one capture for the whole family."""
        captures = _row2()
        base, ledger = _frames(captures)
        assert FAMILY_SCOPE.completion_partition is None
        latest = _both(base, ledger, FAMILY_SCOPE)
        assert latest["bronze_capture_id"].to_list() == ["r1b"]
        assert latest["bronze_capture_id"].to_list() == _oracle(captures, None, partitioned=False)
        before = _both(base, ledger, FAMILY_SCOPE, _t(9))
        assert before["bronze_capture_id"].to_list() == ["r2a"]
        sql = latest_select_sql("base", FAMILY_SCOPE, set(base.columns), as_of_param=False)
        assert sql is not None and sql.endswith("LIMIT 1)") and "QUALIFY" not in sql

    def test_a_partition_on_key_latest_is_refused(self) -> None:
        """Detects a partition accepted where it has no meaning (``key_latest``)."""
        with pytest.raises(ValueError, match="whole_capture"):
            LatestViewSpec(key_columns=("a",), completion_partition="resource_id")


# --------------------------------------------------------------------------- #
# Engine + catalogue (T-H5)
# --------------------------------------------------------------------------- #

PKG = "eeeeeeee-0000-4000-8000-000000000000"
KEY = "multi"
DAY = date(2026, 10, 7)
RESOURCES = {
    "A": ("eeeeeeee-0000-4000-8000-00000000000a", "Multi A", "a.csv"),
    "B": ("eeeeeeee-0000-4000-8000-00000000000b", "Multi B", "b.csv"),
    "C": ("eeeeeeee-0000-4000-8000-00000000000c", "Multi C", "c.csv"),
}
HEADER = b"SettlementDate,SettlementPeriod,Value\n"
PARTITION_KEY = ("resource_id", "settlement_date", "settlement_period")


def partitioned_record(**overrides: Any) -> dict[str, Any]:
    """A resource-partitioned sp_pair record (date, period, value)."""
    columns = [
        column("SettlementDate", "settlement_date", "date", nullable=False),
        column("SettlementPeriod", "settlement_period", "int64", nullable=False),
        column("Value", "value", "float64"),
    ]
    document = record(epochs=[epoch(columns)], entity_key=PARTITION_KEY, latest="whole_capture")
    document["latest_partition"] = "resource_id"
    document.update(overrides)
    return document


def install_multi(
    monkeypatch: pytest.MonkeyPatch,
    data: Path,
    rec: dict[str, Any] | None = None,
    resources: dict[str, tuple[str, str, str]] | None = None,
) -> Any:
    """Install one package whose one family ``multi`` holds every resource of ``resources``."""
    entries = [
        resource(rid, name, KEY) for rid, name, _filename in (resources or RESOURCES).values()
    ]
    fam = family(KEY, record=rec or partitioned_record(), empty_allowed=True)
    document = package("pkg-multi", PKG, [fam], entries)
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    return generated


def capture_multi(
    data: Path,
    letter: str,
    body: bytes,
    written: datetime,
    *,
    resources: dict[str, tuple[str, str, str]] | None = None,
    **kwargs: Any,
) -> str:
    """Write one capture of resource ``letter``; returns its capture id."""
    rid, name, filename = (resources or RESOURCES)[letter]
    path, _sidecar = write_capture(
        data,
        KEY,
        package_slug="pkg-multi",
        package_id=PKG,
        resource_id=rid,
        resource_name=name,
        resource_filename=kwargs.pop("resource_filename", filename),
        body=body,
        written_at=written,
        ckan_last_modified=written.replace(tzinfo=None).isoformat(),
        partition=DAY,
        **kwargs,
    )
    return capture_id_for(path, data)


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return tmp_path_factory.mktemp("m")


def _query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()


def _ids(frame: pl.DataFrame) -> list[str]:
    return sorted(frame["bronze_capture_id"].to_list())


def both_as_of(db: Path, data: Path, key: str, as_of: datetime | None) -> list[str]:
    """The catalogue's ``_latest`` (or P-3's parameterised select) = Polars; capture ids."""
    view = f"silver_{SOURCE}_{key}"
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    if as_of is None:
        sql = _ids(_query(db, f'SELECT * FROM "{view}_latest"'))
    else:
        columns = set(_query(db, f'SELECT * FROM "{view}" LIMIT 0').columns)
        select = latest_select_sql(view, spec, columns, as_of_param=True)
        assert select is not None
        sql = _ids(_query(db, select, {"as_of": as_of.isoformat()}))
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    polars: list[str] = []
    if files:
        lf = pl.scan_parquet(files, hive_partitioning=False)
        polars = _ids(
            select_latest_vintage(lf, spec, as_of, completions=scan_completions(data)).collect()
        )
    assert sql == polars, (key, as_of)
    return sql


class TestEngineAndCatalogue:
    """T-H5: three resources through ``run()``, the catalogue and Polars."""

    def test_t_h5_each_resource_is_served_its_newest_complete_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the per-family ``LIMIT 1`` (one resource surviving), a valid-empty
        capture of one resource blanking the others, an as-of read leaking a later
        capture, an unstamped or mis-stamped ``resource_id``, and the parameter reaching
        a registered view."""
        generated = install_multi(monkeypatch, data)
        assert generated.specs[(SOURCE, KEY)].completion_partition == "resource_id"
        two = HEADER + b"2026-10-07,1,1.0\n2026-10-07,2,2.0\n"
        a1 = capture_multi(data, "A", two, _t(8))
        a2 = capture_multi(data, "A", HEADER + b"2026-10-07,1,9.0\n", _t(12))
        b1 = capture_multi(data, "B", two, _t(8))
        capture_multi(data, "B", HEADER, _t(10), empty_capture=True)
        c1 = capture_multi(data, "C", HEADER + b"2026-10-07,3,3.0\n", _t(11))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)

        assert both_as_of(db, data, KEY, None) == sorted([a2, c1])
        assert both_as_of(db, data, KEY, _t(9)) == sorted([a1, a1, b1, b1])
        assert both_as_of(db, data, KEY, _t(10, 30)) == [a1, a1]

        silver = pl.concat(
            pl.read_parquet(path, hive_partitioning=False)
            for path in sorted((data / "silver" / SOURCE / KEY).rglob("*.parquet"))
        )
        assert silver.schema["resource_id"] == pl.Utf8
        by_capture = {letter: RESOURCES[letter][0] for letter in RESOURCES}
        for capture_id, letter in ((a1, "A"), (a2, "A"), (b1, "B"), (c1, "C")):
            completion = read_completion(data, KEY, capture_id)
            assert completion is not None
            assert completion["resource_id"] == by_capture[letter] != ""
            stamped = silver.filter(pl.col("bronze_capture_id") == capture_id)["resource_id"]
            assert stamped.len() > 0
            assert stamped.unique().to_list() == [by_capture[letter]]

        views = _query(
            db,
            "SELECT sql FROM duckdb_views() WHERE view_name = $name",
            {"name": f"silver_{SOURCE}_{KEY}_latest"},
        )
        assert views.height == 1
        text = views["sql"][0]
        assert "$as_of" not in text and "QUALIFY" in text and "PARTITION BY" in text

    def test_a_pair_repeated_within_one_resource_fails_loudly(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the per-resource grain going unchecked (C-2): a body repeating a
        settlement pair fails with ``DuplicateEntityKeyError`` and writes nothing."""
        generated = install_multi(monkeypatch, data)
        body = HEADER + b"2026-10-07,1,1.0\n2026-10-07,1,2.0\n"
        capture_id = capture_multi(data, "A", body, _t(8))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[KEY](data).run(DAY, run_id="r")
        assert DuplicateEntityKeyError.__name__ in str(info.value)
        assert read_completion(data, KEY, capture_id) is None
        assert not list(data.rglob("silver/**/*.parquet"))


# --------------------------------------------------------------------------- #
# Record rules (T-H6)
# --------------------------------------------------------------------------- #


def _load(tmp_path: Path, rec: dict[str, Any]) -> Any:
    document = package(
        "pkg-multi",
        PKG,
        [family(KEY, record=rec)],
        [resource(rid, name, KEY) for rid, name, _f in RESOURCES.values()],
    )
    return registry_module.load_registry(write_registry(tmp_path / "registry", [document]))


def _refused(tmp_path: Path, rule: str, rec: dict[str, Any]) -> str:
    with pytest.raises(RegistryError) as info:
        _load(tmp_path, rec)
    message = str(info.value)
    assert f"{rule}:" in message, message
    return message


class TestRecordRules:
    def test_a_partitioned_record_loads(self, tmp_path: Path) -> None:
        """The positive control every negative below breaks one rule of."""
        loaded = _load(tmp_path, partitioned_record())
        entry = loaded.families[KEY][1].record
        assert entry is not None and entry.latest_partition == "resource_id"

    def test_t_h6_a_partition_needs_whole_capture(self, tmp_path: Path) -> None:
        """Detects a partition accepted on ``key_latest``, where it would be ignored."""
        message = _refused(tmp_path, "V-17", partitioned_record(latest="key_latest"))
        assert "latest_partition needs whole_capture" in message

    @pytest.mark.parametrize(
        "entity_key",
        [["settlement_date", "settlement_period", "value"], ["resource_id"]],
        ids=["no-resource-id", "only-resource-id"],
    )
    def test_t_h6_b_the_key_holds_resource_id_and_a_grain(
        self, tmp_path: Path, entity_key: list[str]
    ) -> None:
        """Detects a partitioned key that does not name the resource, or names only it."""
        rec = partitioned_record(entity_key=entity_key)
        if entity_key == ["resource_id"]:
            rec["temporal"] = {"kind": "none"}
        message = _refused(tmp_path, "V-17", rec)
        assert "resource_id and the per-resource grain" in message

    def test_t_h6_c_resource_id_in_a_key_needs_the_field(self, tmp_path: Path) -> None:
        """Detects ``resource_id`` admitted to an entity key without the opt-in."""
        rec = partitioned_record()
        del rec["latest_partition"]
        _refused(tmp_path, "V-4", rec)

    def test_t_h6_d_a_vendor_column_named_resource_id_is_reserved(self) -> None:
        """Detects the engine's stamp colliding with a vendor column: ``resource_id``
        is reserved and the parametrised V-2 test collects a case for it."""
        assert "resource_id" in RESERVED
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "--collect-only",
                "-q",
                "-p",
                "no:cacheprovider",
                "tests/unit/test_neso_record.py",
                "-k",
                "test_v2_reserved_name",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
            check=False,
        )
        assert "test_v2_reserved_name[resource_id]" in result.stdout, result.stdout

    def test_t_h6_e_every_committed_record_loads_and_only_resource_partitioned_families_partition(
        self,
    ) -> None:
        """Detects a committed record broken by V-4/V-17 or the reserved name, and any
        family other than ``historic_demand`` and ``school_holiday_percentages`` opting
        into the partition (in a fresh interpreter, so nothing collection imported can
        mask it)."""
        code = textwrap.dedent(
            """
            from gridflow.connectors.neso_data_portal.registry import load_registry
            families = load_registry().families
            recorded = [k for k, (_p, f) in families.items() if f.record is not None]
            partitioned = sorted(
                k for k, (_p, f) in families.items()
                if f.record is not None and f.record.latest_partition is not None
            )
            assert partitioned == ["historic_demand", "school_holiday_percentages"], partitioned
            print("OK", len(recorded), partitioned)
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        assert "OK" in result.stdout, result.stdout

    def test_t_h6_f_the_default_dump_carries_no_new_field(self) -> None:
        """Detects a non-``None`` default on the new field, which would change every
        record's ``exclude_none`` dump and stale every COVERED grant (E14)."""
        plain = SchemaRecord.model_validate(record())
        assert "latest_partition" not in plain.model_dump(mode="json", exclude_none=True)


# --------------------------------------------------------------------------- #
# Per-filename date formats (T-H7, T-H8)
# --------------------------------------------------------------------------- #

MAPPED = [["a.csv", "%Y-%m-%d"], ["b.csv", "%d/%m/%Y"]]


def _date_spec(**fields: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {"source": "D", "name": "d", "dtype": "date", "nullable": False}
    spec.update(fields)
    return spec


class TestFormatMapShape:
    @pytest.mark.parametrize(
        ("fields", "fragment"),
        [
            ({"format": "%Y-%m-%d", "formats_by_filename": MAPPED}, "exactly one"),
            ({}, "needs a format"),
            ({"dtype": "datetime", "zone": "UTC", "formats_by_filename": MAPPED}, "date"),
            ({"dtype": "string", "formats_by_filename": MAPPED}, "date"),
            ({"dtype": "int64", "formats_by_filename": MAPPED}, "date"),
            ({"formats_by_filename": []}, "non-empty"),
            ({"formats_by_filename": [["", "%Y-%m-%d"]]}, "non-empty"),
            ({"formats_by_filename": [["a.csv", ""]]}, "non-empty"),
            ({"formats_by_filename": [["a.csv", "%Y"], ["a.csv", "%d"]]}, "repeats"),
        ],
        ids=[
            "both",
            "neither",
            "datetime",
            "string",
            "int64",
            "empty-map",
            "empty-filename",
            "empty-format",
            "duplicate-filename",
        ],
    )
    def test_t_h7_a_malformed_map_is_refused_naming_the_column(
        self, fields: dict[str, Any], fragment: str
    ) -> None:
        """Detects an ambiguous or silently ignored per-file format: both or neither of
        ``format``/map, a map outside ``date``, an empty map or entry, a repeated file."""
        with pytest.raises(ValueError) as info:
            ColumnSpec.model_validate(_date_spec(**fields))
        message = str(info.value)
        assert "'d'" in message, message
        assert fragment in message, message

    def test_t_h7_a_valid_map_loads_and_a_scalar_column_dumps_no_map(self) -> None:
        """The positive control, and E14: a scalar column's dump has no map key."""
        spec = ColumnSpec.model_validate(_date_spec(formats_by_filename=MAPPED))
        assert spec.formats_by_filename == (("a.csv", "%Y-%m-%d"), ("b.csv", "%d/%m/%Y"))
        plain = ColumnSpec.model_validate(_date_spec(format="%Y-%m-%d"))
        assert "formats_by_filename" not in plain.model_dump(mode="json", exclude_none=True)
        assert "formats_by_filename" not in json.dumps(
            SchemaRecord.model_validate(record()).model_dump(mode="json", exclude_none=True)
        )


def mapped_record() -> dict[str, Any]:
    """``partitioned_record`` with the settlement date mapped per filename."""
    rec = partitioned_record()
    date_column = rec["epochs"][0]["columns"][0]
    del date_column["format"]
    date_column["formats_by_filename"] = MAPPED
    return rec


def _failure_names(info: pytest.ExceptionInfo[NesoCaptureFailedError]) -> list[str]:
    return [cls for _capture, cls, _message in info.value.failures]


class TestFormatResolution:
    """T-H8: the capture's ``resource_filename`` picks the format; no fallback."""

    def test_t_h8_a_each_mapped_file_parses_with_its_own_format(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects one format applied to every file, or a lookup by anything other than
        the exact filename: both files type the same date."""
        generated = install_multi(monkeypatch, data, mapped_record())
        capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n", _t(8))
        capture_multi(data, "B", HEADER + b"07/10/2026,2,2.0\n", _t(8))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        silver = pl.concat(
            pl.read_parquet(path, hive_partitioning=False)
            for path in sorted((data / "silver" / SOURCE / KEY).rglob("*.parquet"))
        )
        assert silver["settlement_date"].unique().to_list() == [date(2026, 10, 7)]
        assert sorted(silver["settlement_period"].to_list()) == [1, 2]

    @pytest.mark.parametrize("empty", [False, True], ids=["populated", "valid-empty"])
    def test_t_h8_bc_an_unmapped_filename_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, empty: bool
    ) -> None:
        """Detects a fallback format (or a skipped check on the header-only path) for a
        file the record does not name: the capture fails loudly, writes no output and no
        completion, and reconcile reports it ``failed``."""
        generated = install_multi(monkeypatch, data, mapped_record())
        body = HEADER if empty else HEADER + b"2026-10-07,1,1.0\n"
        extra = {"empty_capture": True} if empty else {}
        capture_id = capture_multi(data, "C", body, _t(8), **extra)
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[KEY](data).run(DAY, run_id="r")
        assert _failure_names(info) == [UnmappedResourceFormatError.__name__]
        message = info.value.failures[0][2]
        assert "c.csv" in message and "settlement_date" in message and "a.csv" in message
        assert read_completion(data, KEY, capture_id) is None
        assert not list(data.rglob("silver/**/*.parquet"))
        report = reconcile(data, registry_module.load_registry(), [KEY], DAY)
        assert [(gap.category, gap.capture_id) for gap in report.gaps] == [("failed", capture_id)]

    def test_t_h8_d_a_mapped_format_that_does_not_fit_fails_strictly(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a lenient parse under a mapped format (D-41): ``b.csv`` is mapped
        ``%d/%m/%Y`` and carries ISO dates, so the capture fails and writes nothing."""
        generated = install_multi(monkeypatch, data, mapped_record())
        capture_id = capture_multi(data, "B", HEADER + b"2026-10-07,1,1.0\n", _t(8))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[KEY](data).run(DAY, run_id="r")
        assert len(info.value.failures) == 1
        assert _failure_names(info)[0] != UnmappedResourceFormatError.__name__
        assert read_completion(data, KEY, capture_id) is None


# --------------------------------------------------------------------------- #
# Overlap (T-H9)
# --------------------------------------------------------------------------- #


def _overlap_lines(report: Any) -> list[str]:
    return [gap.line() for gap in report.gaps if gap.category == "overlap"]


class TestOverlap:
    def test_t_h9_a_one_pair_in_two_resources_is_served_and_reported(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects an invented precedence (one resource's row dropped) or an overlap
        that passes silently: both rows are served by both renderers, reconcile names
        both captures as non-drainable ``overlap`` gaps, and the drain leaves them."""
        generated = install_multi(monkeypatch, data)
        a = capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n2026-10-07,2,2.0\n", _t(8))
        b = capture_multi(data, "B", HEADER + b"2026-10-07,2,5.0\n", _t(9))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        assert both_as_of(db, data, KEY, None) == sorted([a, a, b])

        loaded = registry_module.load_registry()
        report = reconcile(data, loaded, [KEY], DAY)
        overlaps = [gap for gap in report.gaps if gap.category == "overlap"]
        assert sorted(gap.capture_id for gap in overlaps) == sorted([a, b])
        assert all(not gap.drainable for gap in overlaps)
        assert len(report.gaps) == 2
        by_capture = {gap.capture_id: gap.detail for gap in overlaps}
        rid_a, rid_b = RESOURCES["A"][0], RESOURCES["B"][0]
        assert by_capture[a] == (
            f"1 key(s) also served by resource(s) ['{rid_b}']; "
            "first: settlement_date=2026-10-07, settlement_period=2"
        )
        assert rid_a in by_capture[b]
        assert "SUMMARY overlap 2" in report.lines()
        assert all(gap.partition_date == DAY for gap in overlaps)

        refreshed: list[bool] = []
        after = drain(data, loaded, [KEY], DAY, lambda: refreshed.append(True))
        assert after.drained == () and refreshed == []
        assert _overlap_lines(after) == _overlap_lines(report)

    def test_t_h9_b_a_recreated_uuid_with_the_same_name_overlaps(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a recreated resource (new UUID, the registered name, ADR-033 P-10)
        silently shadowing or being shadowed by the old one (FM-7): both are served and
        both captures are reported ``overlap``."""
        generated = install_multi(monkeypatch, data)
        old = capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n", _t(8))
        recreated = {"A": ("eeeeeeee-0000-4000-8000-0000000000ff", RESOURCES["A"][1], "a.csv")}
        new = capture_multi(data, "A", HEADER + b"2026-10-07,1,2.0\n", _t(9), resources=recreated)
        generated.transformers[KEY](data).run(DAY, run_id="r")
        new_completion = read_completion(data, KEY, new)
        assert new_completion is not None
        assert new_completion["resource_id"] == recreated["A"][0]
        report = reconcile(data, registry_module.load_registry(), [KEY], DAY)
        assert sorted(gap.capture_id for gap in report.gaps) == sorted([old, new])
        assert {gap.category for gap in report.gaps} == {"overlap"}

    def test_t_h9_c_disjoint_resources_and_unpartitioned_families_report_none(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a false overlap on disjoint resources, and the check running on a
        family that did not opt in (where one key across captures is ordinary)."""
        generated = install_multi(monkeypatch, data)
        capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n", _t(8))
        capture_multi(data, "B", HEADER + b"2026-10-07,2,2.0\n", _t(8))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        report = reconcile(data, registry_module.load_registry(), [KEY], DAY)
        assert report.clean
        assert "SUMMARY overlap 0" in report.lines()

    def test_t_h9_c_a_family_without_the_partition_is_never_checked(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the overlap check reading a family that did not opt in."""
        plain = partitioned_record(entity_key=["settlement_date", "settlement_period", "value"])
        del plain["latest_partition"]
        generated = install_multi(monkeypatch, data, plain)
        capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n", _t(8))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        calls: list[str] = []
        monkeypatch.setattr(
            "gridflow.silver.neso_data_portal.reconcile._overlaps",
            lambda key, *args: calls.append(key) or [],
        )
        assert reconcile(data, registry_module.load_registry(), [KEY], DAY).clean
        assert calls == []

    def test_t_h9_d_an_unreadable_output_is_a_failed_check_not_a_pass(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a check that cannot read an output passing silently, or aborting
        reconcile before the drainable gap is reported (FM-10): a truncated Parquet
        yields one ``overlap check failed`` gap beside the invalid-output gap."""
        generated = install_multi(monkeypatch, data)
        capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n", _t(8))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        (output,) = sorted((data / "silver" / SOURCE / KEY).rglob("*.parquet"))
        output.write_bytes(output.read_bytes()[:40])
        report = reconcile(data, registry_module.load_registry(), [KEY], DAY)
        overlaps = [gap for gap in report.gaps if gap.category == "overlap"]
        assert len(overlaps) == 1
        assert overlaps[0].capture_id == "-" and not overlaps[0].drainable
        assert overlaps[0].detail.startswith("overlap check failed: ")
        assert any(gap.drainable for gap in report.gaps if gap.category != "overlap")
