"""Revision selection: tie-break, as-of and whole-capture (ADR-034 P-10; B3, B4).

Every selection is run through BOTH renderers — the DuckDB ``SELECT`` from
``latest_select_sql`` (bound with an ISO ``$as_of`` string, C-7) and the
Polars ``select_latest_vintage`` — and the two results must agree, so a
divergence between the catalogue and Polars readers is caught where it starts.
"""

from __future__ import annotations

import random
import shutil
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from _neso_generic_support import install_generated, write_capture
from _neso_registry_support import column, epoch, family, package, record, resource, sp_columns

from gridflow.silver.latest_views import (
    _SETTLEMENT_RUN_RANK,
    LATEST_VIEW_SPECS,
    LatestViewSpec,
    latest_select_sql,
    latest_view_sql,
    select_latest_vintage,
)
from gridflow.silver.neso_data_portal.completion import (
    COMPLETION_RELATION,
    completion_path,
    scan_completions,
)
from gridflow.storage.duckdb import init_catalogue, refresh_views

if TYPE_CHECKING:
    from pathlib import Path

TS = pl.Datetime("us", "UTC")
TIE = ("capture_written_at", "bronze_capture_id")
KEY_SPEC = LatestViewSpec(
    key_columns=("settlement_date", "settlement_period", "unit"),
    order_columns=("issue_time", "available_at"),
    tiebreak_columns=TIE,
)
WHOLE_SPEC = LatestViewSpec(
    key_columns=(),
    mode="whole_capture",
    completion_relation="completion",
    completion_family="fam",
    tiebreak_columns=TIE,
)


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


def _sql(
    frame: pl.DataFrame,
    spec: LatestViewSpec,
    as_of: datetime | None = None,
    completions: pl.DataFrame | None = None,
) -> pl.DataFrame:
    con = duckdb.connect(":memory:")
    try:
        con.register("base", frame.to_arrow())
        if completions is not None:
            con.register("completion", completions.to_arrow())
        select = latest_select_sql("base", spec, set(frame.columns), as_of_param=as_of is not None)
        assert select is not None
        params = {"as_of": as_of.isoformat()} if as_of is not None else None
        out = con.execute(select, params).pl() if params else con.execute(select).pl()
    finally:
        con.close()
    return _utc(out)


def _polars(
    frame: pl.DataFrame,
    spec: LatestViewSpec,
    as_of: datetime | None = None,
    completions: pl.DataFrame | None = None,
) -> pl.DataFrame:
    lazy = completions.lazy() if completions is not None else None
    return select_latest_vintage(frame.lazy(), spec, as_of, completions=lazy).collect()


def _utc(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col(name).dt.convert_time_zone("UTC").cast(TS)
        for name, dtype in frame.schema.items()
        if isinstance(dtype, pl.Datetime)
    )


def _both(frame: pl.DataFrame, spec: LatestViewSpec, **kwargs: Any) -> list[str]:
    """Run both renderers; assert they agree; return the winning capture ids."""
    sql = _sql(frame, spec, **kwargs)
    pol = _polars(frame, spec, **kwargs)
    sort = sorted(frame.columns)
    assert sql.select(sort).sort(sort).to_dicts() == pol.select(sort).sort(sort).to_dicts()
    return sorted(pol["bronze_capture_id"].to_list())


def _rows(*rows: dict[str, Any]) -> pl.DataFrame:
    schema = {
        "settlement_date": pl.Date,
        "settlement_period": pl.Int64,
        "unit": pl.Utf8,
        "value": pl.Float64,
        "issue_time": TS,
        "published_at": TS,
        "available_at": TS,
        "capture_written_at": TS,
        "bronze_capture_id": pl.Utf8,
    }
    base = {
        "settlement_date": date(2026, 10, 7),
        "settlement_period": 1,
        "unit": "U",
        "value": 0.0,
        "issue_time": None,
        "published_at": None,
    }
    return pl.DataFrame([{**base, **row} for row in rows], schema=schema)


class TestAsOfBeforeSelection:
    def test_t_b3_2_a_later_issue_not_yet_available_never_wins(self) -> None:
        """Detects as-of applied after selection: the 12:00 row has the later
        issue time, so selecting first would pick it and the as-of filter would
        then drop the key entirely, losing the 08:00 row that WAS available."""
        frame = _rows(
            {
                "issue_time": _t(7),
                "available_at": _t(8),
                "capture_written_at": _t(8),
                "bronze_capture_id": "c1",
            },
            {
                "issue_time": _t(9),
                "available_at": _t(12),
                "capture_written_at": _t(12),
                "bronze_capture_id": "c2",
            },
        )
        assert _both(frame, KEY_SPEC, as_of=_t(9)) == ["c1"]
        assert _both(frame, KEY_SPEC) == ["c2"]

    @pytest.mark.parametrize(
        ("spec", "dropped"),
        [
            (KEY_SPEC, "capture_written_at"),
            (KEY_SPEC, "available_at"),
            (KEY_SPEC, "unit"),
            (WHOLE_SPEC, "bronze_capture_id"),
        ],
    )
    def test_a_skipped_selection_never_returns_rows_past_as_of(
        self, spec: LatestViewSpec, dropped: str
    ) -> None:
        """Detects the skip reaction leaking through an as-of read: a skipped
        selection returned the unbounded frame, so the 13:00 row came back for
        an as-of of 12:00 (REVIEW-DIFF-1 correctness #1). Without ``as_of`` the
        skip reaction (frame unchanged) is kept."""
        frame = _rows(
            {"available_at": _t(8), "capture_written_at": _t(8), "bronze_capture_id": "c1"},
            {"available_at": _t(13), "capture_written_at": _t(13), "bronze_capture_id": "c2"},
        ).drop(dropped)
        completions = _completions(
            {
                "bronze_capture_id": "c1",
                "outcome": "populated",
                "row_count": 1,
                "available_at": _t(8),
                "capture_written_at": _t(8),
            }
        ).lazy()
        with pytest.raises(ValueError, match="as-of bound cannot be applied"):
            select_latest_vintage(frame.lazy(), spec, _t(12), completions=completions).collect()
        if dropped != "available_at":  # still a selection (by issue_time) without as_of
            unbounded = select_latest_vintage(frame.lazy(), spec, completions=completions)
            assert unbounded.collect().height == 2


class TestTieBreak:
    def test_t_b3_3_equal_available_at_resolves_by_capture_then_id(self) -> None:
        """Detects a tie resolved by scan order: shuffled input, one winner."""
        rows = [
            {"available_at": _t(8), "capture_written_at": _t(9), "bronze_capture_id": "c1"},
            {"available_at": _t(8), "capture_written_at": _t(10), "bronze_capture_id": "c0"},
            {"available_at": _t(8), "capture_written_at": _t(10), "bronze_capture_id": "c2"},
        ]
        for seed in range(6):
            random.Random(seed).shuffle(rows)
            assert _both(_rows(*rows), KEY_SPEC) == ["c2"]

    def test_t_b3_4_equal_published_at_with_different_bytes(self) -> None:
        """C-3 (accepted residual): a later capture repeating an earlier
        ``published_at`` inherits its availability; the tie-break still orders
        the two deterministically (by capture time, then id). The as-of
        exposure is named in ADR-034, not fixed here."""
        frame = _rows(
            {
                "published_at": _t(8),
                "available_at": _t(8),
                "capture_written_at": _t(8, 30),
                "bronze_capture_id": "c1",
                "value": 1.0,
            },
            {
                "published_at": _t(8),
                "available_at": _t(8),
                "capture_written_at": _t(12),
                "bronze_capture_id": "c2",
                "value": 2.0,
            },
        )
        assert _both(frame, KEY_SPEC) == ["c2"]
        assert _both(frame, KEY_SPEC, as_of=_t(9)) == ["c2"]

    def test_a_missing_tiebreak_column_skips_fail_closed(self) -> None:
        """Detects a silently dropped tie-break (the winner would follow scan order)."""
        frame = _rows(
            {"available_at": _t(8), "capture_written_at": _t(9), "bronze_capture_id": "c1"}
        ).drop("capture_written_at")
        assert latest_view_sql("base", "base_latest", KEY_SPEC, set(frame.columns)) is None

    def test_run_type_rank_precedes_the_tiebreak(self) -> None:
        """T-B1-9's selection half: available_at, then the run rank, then the
        run_type column itself for unmapped runs."""
        spec = LatestViewSpec(
            key_columns=("settlement_date", "settlement_period"),
            order_columns=("available_at",),
            rank_column="run_type",
            rank_map=_SETTLEMENT_RUN_RANK,
            tiebreak_columns=(*TIE, "run_type"),
        )
        rows = _rows(
            {"available_at": _t(8), "capture_written_at": _t(8), "bronze_capture_id": "c1"},
            {"available_at": _t(8), "capture_written_at": _t(8), "bronze_capture_id": "c1"},
        ).with_columns(pl.Series("run_type", ["R1", "SF"]), pl.Series("value", [1.0, 2.0]))
        assert _sql(rows, spec)["run_type"].to_list() == ["R1"]
        assert _polars(rows, spec)["run_type"].to_list() == ["R1"]
        unmapped = rows.with_columns(pl.Series("run_type", ["ZZ", "YY"]))
        assert _sql(unmapped, spec)["run_type"].to_list() == ["ZZ"]
        assert _polars(unmapped, spec)["run_type"].to_list() == ["ZZ"]


def _completions(*rows: dict[str, Any]) -> pl.DataFrame:
    schema = {
        "family": pl.Utf8,
        "bronze_capture_id": pl.Utf8,
        "outcome": pl.Utf8,
        "row_count": pl.Int64,
        "available_at": TS,
        "capture_written_at": TS,
    }
    return pl.DataFrame([{"family": "fam", **row} for row in rows], schema=schema)


class TestWholeCapture:
    """E4's shape: populated(2) at 08:00, valid-empty at 10:00, populated(1) at 12:00."""

    @staticmethod
    def _state() -> tuple[pl.DataFrame, pl.DataFrame]:
        base = _rows(
            {
                "unit": "A",
                "available_at": _t(8),
                "capture_written_at": _t(8),
                "bronze_capture_id": "c1",
            },
            {
                "unit": "B",
                "available_at": _t(8),
                "capture_written_at": _t(8),
                "bronze_capture_id": "c1",
            },
            {
                "unit": "A",
                "available_at": _t(12),
                "capture_written_at": _t(12),
                "bronze_capture_id": "c3",
            },
        )
        completions = _completions(
            {
                "bronze_capture_id": "c1",
                "outcome": "populated",
                "row_count": 2,
                "available_at": _t(8),
                "capture_written_at": _t(8),
            },
            {
                "bronze_capture_id": "c2",
                "outcome": "valid_empty",
                "row_count": 0,
                "available_at": _t(10),
                "capture_written_at": _t(10),
            },
            {
                "bronze_capture_id": "c3",
                "outcome": "populated",
                "row_count": 1,
                "available_at": _t(12),
                "capture_written_at": _t(12),
            },
        )
        return base, completions

    @pytest.mark.parametrize(
        ("as_of", "expected"),
        [(_t(9), ["c1", "c1"]), (_t(11), []), (_t(13), ["c3"]), (None, ["c3"])],
    )
    def test_newest_complete_capture_as_of(
        self, as_of: datetime | None, expected: list[str]
    ) -> None:
        base, completions = self._state()
        assert _both(base, WHOLE_SPEC, as_of=as_of, completions=completions) == expected

    def test_an_output_whose_count_disagrees_is_not_eligible(self) -> None:
        """c3's rows gone (or partial): c2, the newest still-complete capture, wins."""
        base, completions = self._state()
        assert (
            _both(
                base.filter(pl.col("bronze_capture_id") != "c3"),
                WHOLE_SPEC,
                completions=completions,
            )
            == []
        )

    def test_an_output_without_a_completion_is_never_eligible(self) -> None:
        base, completions = self._state()
        orphan = _rows(
            {
                "unit": "Z",
                "available_at": _t(14),
                "capture_written_at": _t(14),
                "bronze_capture_id": "c4",
            }
        )
        assert _both(pl.concat([base, orphan]), WHOLE_SPEC, completions=completions) == ["c3"]

    def test_polars_without_completions_raises(self) -> None:
        base, _completions_frame = self._state()
        with pytest.raises(ValueError, match="completion records"):
            select_latest_vintage(base.lazy(), WHOLE_SPEC)


class TestRendererParity:
    def test_t_b3_5_randomized_frames_agree_incl_nulls(self) -> None:
        """Detects any divergence between the SQL and Polars renderers, with
        null ``issue_time``/``published_at`` sorting last on both."""
        rng = random.Random(20261007)
        for _ in range(25):
            rows = []
            for i in range(rng.randint(1, 30)):
                available = _t(6) + timedelta(minutes=rng.randint(0, 600))
                rows.append(
                    {
                        "settlement_period": rng.randint(1, 3),
                        "unit": rng.choice("AB"),
                        "issue_time": rng.choice([None, _t(5), _t(7)]),
                        "published_at": rng.choice([None, available]),
                        "available_at": available,
                        "capture_written_at": available + timedelta(minutes=rng.randint(0, 5)),
                        "bronze_capture_id": f"c{i:03d}",
                    }
                )
            frame = _rows(*rows)
            _both(frame, KEY_SPEC)
            _both(frame, KEY_SPEC, as_of=_t(11))

    def test_no_registered_view_text_carries_the_parameter(self) -> None:
        sql = latest_view_sql("base", "base_latest", KEY_SPEC, set(_rows().columns))
        assert sql is not None and "$as_of" not in sql
        whole = latest_view_sql("base", "base_latest", WHOLE_SPEC, set(_rows().columns))
        assert whole is not None and "$as_of" not in whole


# --------------------------------------------------------------------------- #
# Over the engine's real outputs and the catalogue (P-6 + P-10 + P-11)
# --------------------------------------------------------------------------- #

SOURCE = "neso_data_portal"
PKG = "dddddddd-0000-4000-8000-000000000000"
DAY = date(2026, 10, 7)
SP_HEADER = b"SettlementDate,SettlementPeriod,Unit,Value\n"
ISSUE_KEY = ("settlement_date", "settlement_period", "unit", "issue_time")
ISSUE_HEADER = b"SettlementDate,SettlementPeriod,Unit,Value,Issued\n"


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return tmp_path_factory.mktemp("s")


def _issue_epoch() -> dict[str, Any]:
    issued = column(
        "Issued", "issued", "datetime", format="%Y-%m-%dT%H:%M", zone="UTC", nullable=False
    )
    return epoch([*sp_columns(), issued], issue={"kind": "data_column", "column": "issued"})


def _install(
    monkeypatch: pytest.MonkeyPatch, data: Path, families: dict[str, dict[str, Any]]
) -> Any:
    """Install one package holding one resource per ``key -> family kwargs``."""
    entries = []
    resources = []
    for index, (key, kwargs) in enumerate(families.items(), start=1):
        entries.append(family(key, **kwargs))
        resources.append(resource(f"dddddddd-0000-4000-8000-00000000000{index}", key.title(), key))
    document = package("pkg-gen", PKG, entries, resources)
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    return generated


def _capture(
    data: Path,
    key: str,
    index: int,
    body: bytes,
    written: datetime,
    *,
    lm: datetime | None = None,
    **kwargs: Any,
) -> str:
    stamp = lm if lm is not None else written
    path, _sidecar = write_capture(
        data,
        key,
        package_slug="pkg-gen",
        package_id=PKG,
        resource_id=f"dddddddd-0000-4000-8000-00000000000{index}",
        resource_name=key.title(),
        body=body,
        written_at=written,
        ckan_last_modified=kwargs.pop("ckan_last_modified", stamp.replace(tzinfo=None).isoformat()),
        partition=DAY,
        **kwargs,
    )
    return path.relative_to(data).as_posix()


def _query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()


def _ids(frame: pl.DataFrame) -> list[str]:
    return sorted(frame["bronze_capture_id"].to_list())


def _sql_as_of(db: Path, key: str, as_of: datetime | None) -> list[str]:
    """``_latest`` when ``as_of`` is None, else P-10's parameterised select."""
    view = f"silver_{SOURCE}_{key}"
    if as_of is None:
        return _ids(_query(db, f'SELECT * FROM "{view}_latest"'))
    columns = set(_query(db, f'SELECT * FROM "{view}" LIMIT 0').columns)
    select = latest_select_sql(view, LATEST_VIEW_SPECS[(SOURCE, key)], columns, as_of_param=True)
    assert select is not None
    return _ids(_query(db, select, {"as_of": as_of.isoformat()}))


def _polars_as_of(data: Path, key: str, as_of: datetime | None) -> list[str]:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    if not files:
        return []
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    completions = scan_completions(data) if spec.mode == "whole_capture" else None
    lf = pl.scan_parquet(files, hive_partitioning=False)
    return _ids(select_latest_vintage(lf, spec, as_of, completions=completions).collect())


def _both_as_of(db: Path, data: Path, key: str, as_of: datetime | None) -> list[str]:
    sql = _sql_as_of(db, key, as_of)
    assert sql == _polars_as_of(data, key, as_of), (key, as_of)
    return sql


class TestCorrectionLeakage:
    """T-B3-1: an original (08:30) and its correction (12:00) of one issue."""

    FAMILIES: dict[str, dict[str, Any]] = {  # noqa: RUF012
        "gen_pub": {"record": record(epochs=[_issue_epoch()], entity_key=ISSUE_KEY)},
        "gen_fb": {
            "record": record(
                epochs=[_issue_epoch()], entity_key=ISSUE_KEY, vintage="capture_fallback"
            )
        },
        "gen_none": {"record": record()},
    }

    def test_as_of_before_the_correction_never_sees_it(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the correction leaking into an as-of before it existed, for a
        non-null ``published_at``, a null one (``capture_fallback``), and an
        issue-``none`` family, in the catalogue and in Polars alike."""
        generated = _install(monkeypatch, data, self.FAMILIES)
        originals: dict[str, str] = {}
        corrections: dict[str, str] = {}
        for index, key in enumerate(self.FAMILIES, start=1):
            issue_row = b",2026-10-07T06:00" if key != "gen_none" else b""
            header = ISSUE_HEADER if key != "gen_none" else SP_HEADER
            fallback = key == "gen_fb"
            originals[key] = _capture(
                data,
                key,
                index,
                header + b"2026-10-07,1,A,1.5" + issue_row + b"\n",
                _t(8, 30),
                lm=_t(8, 25),
                **({"ckan_last_modified": ""} if fallback else {}),
            )
            corrections[key] = _capture(
                data,
                key,
                index,
                header + b"2026-10-07,1,A,9.5" + issue_row + b"\n",
                _t(12),
                lm=_t(11, 55),
                **({"ckan_last_modified": ""} if fallback else {}),
            )
            generated.transformers[key](data).run(DAY, run_id="r")
        published = scan_completions(data).collect()
        assert published.filter(pl.col("family") == "gen_fb")["published_at"].null_count() == 2
        assert published.filter(pl.col("family") == "gen_pub")["published_at"].null_count() == 0
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        for key in self.FAMILIES:
            assert _both_as_of(db, data, key, _t(9)) == [originals[key]]
            assert _both_as_of(db, data, key, None) == [corrections[key]]


class TestNoRunTypeColumn:
    def test_t_b3_6_a_record_without_a_run_type_registers_its_latest(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a ``None`` reaching the tie-break or the catalogue for a
        record with no ``run_type_column``."""
        generated = _install(monkeypatch, data, {"gen": {"record": record()}})
        spec = generated.specs[(SOURCE, "gen")]
        assert None not in spec.tiebreak_columns and spec.rank_column is None
        _capture(data, "gen", 1, SP_HEADER + b"2026-10-07,1,A,1\n2026-10-07,2,A,2\n", _t(8))
        _capture(data, "gen", 1, SP_HEADER + b"2026-10-07,1,A,3\n", _t(9))
        generated.transformers["gen"](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        latest = _query(db, f'SELECT * FROM "silver_{SOURCE}_gen_latest"')
        assert latest.height == 2
        files = sorted((data / "silver" / SOURCE / "gen").rglob("*.parquet"))
        polars = select_latest_vintage(pl.scan_parquet(files, hive_partitioning=False), spec)
        frame = polars.collect().sort("settlement_period")
        assert frame["value"].to_list() == [3.0, 2.0]
        assert frame.select("settlement_period", "unit").is_unique().all()


class TestIssueOnlyEntityKey:
    def test_an_entity_key_of_only_issue_time_registers_one_latest_row(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects ``PARTITION BY`` rendered with no expression: an entity key of
        only ``issue_time`` leaves an empty selection key, and the catalogue
        raised a ``ParserException`` creating its ``_latest`` (REVIEW-DIFF-1
        correctness #3). Both renderers return the newest issue's one row, and
        the as-of read the one available then."""
        issued = column(
            "Issued", "issued", "datetime", format="%Y-%m-%dT%H:%M", zone="UTC", nullable=False
        )
        issue_epoch = epoch(
            [issued, column("Value", "value", "float64")],
            issue={"kind": "data_column", "column": "issued"},
        )
        rec = record(epochs=[issue_epoch], temporal={"kind": "none"}, entity_key=("issue_time",))
        generated = _install(monkeypatch, data, {"gen_issue": {"record": rec}})
        assert generated.specs[(SOURCE, "gen_issue")].key_columns == ()
        header = b"Issued,Value\n"
        first = _capture(data, "gen_issue", 1, header + b"2026-10-07T06:00,1.5\n", _t(8))
        second = _capture(data, "gen_issue", 1, header + b"2026-10-07T07:00,2.5\n", _t(12))
        generated.transformers["gen_issue"](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        assert _both_as_of(db, data, "gen_issue", None) == [second]
        assert _both_as_of(db, data, "gen_issue", _t(9)) == [first]


def _whole_family(empty_allowed: bool = True) -> dict[str, Any]:
    return {"record": record(latest="whole_capture"), "empty_allowed": empty_allowed}


class TestEmptyCaptureAsOf:
    def test_t_b4_4_populated_empty_populated_as_of(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B4-2's state: as-of 09:00 -> capture 1, 11:00 -> none, latest -> 3."""
        generated = _install(monkeypatch, data, {"whole": _whole_family()})
        body = SP_HEADER + b"2026-10-07,1,A,1\n2026-10-07,2,A,2\n"
        first = _capture(data, "whole", 1, body, _t(8))
        _capture(data, "whole", 1, SP_HEADER, _t(10), empty_capture=True)
        third = _capture(data, "whole", 1, body, _t(12))
        generated.transformers["whole"](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        assert _both_as_of(db, data, "whole", _t(9)) == [first, first]
        assert _both_as_of(db, data, "whole", _t(11)) == []
        assert _both_as_of(db, data, "whole", None) == [third, third]


def _relations(db: Path) -> set[str]:
    return set(
        _query(db, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")[
            "table_name"
        ].to_list()
    )


def _described(db: Path, relation: str) -> list[tuple[str, str]]:
    frame = _query(
        db,
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = $name ORDER BY ordinal_position",
        {"name": relation},
    )
    return list(zip(frame["column_name"].to_list(), frame["data_type"].to_list(), strict=True))


class TestRelationExistence:
    """T-B4-6: I-1's state matrix under pytest strict mode.

    In every state ``init_catalogue`` raises nothing; the base view, ``_latest``
    and the completion relation of both families exist; and each ``_latest``
    equals Polars ``select_latest_vintage`` over the same state.
    """

    BODY = SP_HEADER + b"2026-10-07,1,A,1\n2026-10-07,2,A,2\n"
    FAMILIES: dict[str, dict[str, Any]] = {  # noqa: RUF012
        "keyed": {"record": record(), "empty_allowed": True},
        "whole": _whole_family(),
    }

    def _check(self, data: Path, *, refresh: bool = False) -> dict[str, list[str]]:
        db = data / "cat.duckdb"
        (refresh_views if refresh else init_catalogue)(db, data)
        relations = _relations(db)
        assert COMPLETION_RELATION in relations
        out = {}
        for key in self.FAMILIES:
            base = f"silver_{SOURCE}_{key}"
            assert {base, f"{base}_latest"} <= relations
            out[key] = _both_as_of(db, data, key, None)
        return out

    def _run(self, generated: Any, data: Path) -> None:
        for key in self.FAMILIES:
            generated.transformers[key](data).run(DAY, run_id="r")

    def _wipe_silver(self, data: Path) -> None:
        shutil.rmtree(data / "silver")

    def test_a_fresh_catalogue(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _install(monkeypatch, data, self.FAMILIES)
        assert self._check(data) == {"keyed": [], "whole": []}

    def test_b_empty_first_then_populated(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        generated = _install(monkeypatch, data, self.FAMILIES)
        for index, key in enumerate(self.FAMILIES, start=1):
            _capture(data, key, index, SP_HEADER, _t(8), empty_capture=True)
        self._run(generated, data)
        assert not (data / "silver").exists()
        assert self._check(data) == {"keyed": [], "whole": []}
        populated = {
            key: _capture(data, key, index, self.BODY, _t(9))
            for index, key in enumerate(self.FAMILIES, start=1)
        }
        self._run(generated, data)
        after = self._check(data, refresh=True)
        assert after == {key: [capture, capture] for key, capture in populated.items()}

    def test_c_populated_then_empty_then_silver_wiped(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """REVIEW-PLAN-2's mixed ledger: the valid-empty capture still wins."""
        generated = _install(monkeypatch, data, self.FAMILIES)
        for index, key in enumerate(self.FAMILIES, start=1):
            _capture(data, key, index, self.BODY, _t(8))
            _capture(data, key, index, SP_HEADER, _t(10), empty_capture=True)
        self._run(generated, data)
        self._wipe_silver(data)
        assert self._check(data) == {"keyed": [], "whole": []}

    def test_d_populated_only_then_silver_wiped(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Zero rows; reconcile's half of (d) lives in ``test_neso_reconcile``."""
        generated = _install(monkeypatch, data, self.FAMILIES)
        for index, key in enumerate(self.FAMILIES, start=1):
            _capture(data, key, index, self.BODY, _t(8))
        self._run(generated, data)
        self._wipe_silver(data)
        assert self._check(data) == {"keyed": [], "whole": []}

    def test_e_an_output_without_a_completion_beside_an_older_complete_one(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        generated = _install(monkeypatch, data, self.FAMILIES)
        older: dict[str, str] = {}
        newer: dict[str, str] = {}
        for index, key in enumerate(self.FAMILIES, start=1):
            older[key] = _capture(data, key, index, self.BODY, _t(8))
            newer[key] = _capture(data, key, index, self.BODY.replace(b",1\n", b",7\n"), _t(9))
        self._run(generated, data)
        for key in self.FAMILIES:
            completion_path(data, key, newer[key]).unlink()
        got = self._check(data)
        assert got["whole"] == [older["whole"], older["whole"]]
        # key_latest reads rows, not the ledger: the newer rows still win per key.
        assert got["keyed"] == [newer["keyed"], newer["keyed"]]

    def test_schema_parity_typed_empty_equals_glob_backed(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a typed-empty relation whose ordered (name, type) list differs
        from the glob view's after one real output, incl. a null-typed
        ``source_run_id`` (P-6); likewise the completion relation."""
        generated = _install(monkeypatch, data, self.FAMILIES)
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        relations = [f"silver_{SOURCE}_{key}" for key in self.FAMILIES] + [COMPLETION_RELATION]
        empty = {relation: _described(db, relation) for relation in relations}
        for index, key in enumerate(self.FAMILIES, start=1):
            _capture(data, key, index, self.BODY, _t(8))
        self._run(generated, data)
        refresh_views(db, data)
        for relation in relations:
            assert _query(db, f'SELECT count(*) AS n FROM "{relation}"')["n"][0] > 0
            assert _described(db, relation) == empty[relation], relation


class TestQualityCli:
    def test_quality_reads_a_whole_capture_family_through_its_completions(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects ``gridflow quality`` crashing on (or reading every vintage of)
        a whole-capture family: it must see only the newest complete capture."""
        from typer.testing import CliRunner

        from gridflow.cli import app

        generated = _install(monkeypatch, data, {"whole": _whole_family()})
        _capture(data, "whole", 1, SP_HEADER + b"2026-10-07,1,A,1\n2026-10-07,2,A,2\n", _t(8))
        _capture(data, "whole", 1, SP_HEADER + b"2026-10-07,1,A,5\n", _t(12))
        generated.transformers["whole"](data).run(DAY, run_id="r")
        db = data / "q.duckdb"
        monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data))
        monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db))
        monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(data / "logs"))
        result = CliRunner().invoke(app, ["quality", "--source", SOURCE])
        assert result.exit_code == 0, result.output
        report = _query(
            db,
            "SELECT metric FROM quality_reports WHERE dataset = 'whole' "
            "AND check_name = 'row_count'",
        )
        assert report["metric"].to_list() == [1.0]
