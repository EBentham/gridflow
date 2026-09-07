"""Runner/status regressions for partition-ownership accounting."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl

from gridflow.pipeline import runner
from gridflow.silver.base import BaseSilverTransformer, _PublicationWindowPlan
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.partition_window import IntervalSemantics, RequestWindow

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pytest


DESTINATION = date(2024, 1, 15)
PREDECESSOR = DESTINATION - timedelta(days=1)


def _row(owner: date, period: int) -> dict[str, object]:
    return {
        "settlementDate": owner.isoformat(),
        "settlementPeriod": period,
        "dataProvider": "P",
        "price": float(period),
        "volume": 1.0,
    }


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    factory: Callable[[Path], BaseSilverTransformer],
    start: date = DESTINATION,
    end: date = DESTINATION,
    dataset: str = "mid",
) -> tuple[runner.DatasetResult, tuple[str, int]]:
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import get_connection, init_catalogue

    data_dir = tmp_path / "data"
    db_path = tmp_path / "catalogue" / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda _con: None)
    monkeypatch.setattr(
        "gridflow.silver.registry.get_transformer",
        lambda _source, _dataset, root: factory(root),
    )
    settings = load_settings()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        result = runner.run_transform(
            runner.PipelineContext(con=con, settings=settings),
            "elexon",
            [dataset],
            datetime.combine(start, datetime.min.time(), tzinfo=UTC),
            datetime.combine(end, datetime.min.time(), tzinfo=UTC),
        )[0]
        persisted = con.execute(
            "SELECT status, rows_skipped FROM pipeline_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    finally:
        con.close()
    assert persisted is not None
    return result, (str(persisted[0]), int(persisted[1]))


def _transformer(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    inputs: dict[date, pl.DataFrame],
) -> MIDTransformer:
    transformer = MIDTransformer(root)
    monkeypatch.setattr(
        transformer,
        "read_bronze",
        lambda source_date: inputs.get(source_date, pl.DataFrame()),
    )
    monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)
    return transformer


def test_p_t12_p_t14_routine_trim_stays_success_with_visible_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = {
        PREDECESSOR: pl.DataFrame([_row(PREDECESSOR, 1), _row(DESTINATION, 1)]),
        DESTINATION: pl.DataFrame([_row(DESTINATION, 2)]),
    }
    result, persisted = _run(
        tmp_path,
        monkeypatch,
        lambda root: _transformer(root, monkeypatch, inputs),
    )
    assert result.status == "success"
    assert result.rows_partition_trimmed == 1
    assert result.rows_partition_trim_unrecoverable == 0
    assert result.rows_skipped == 0
    assert persisted == ("success", 0)


def test_p_t14_unsafe_trim_warns_without_entering_rows_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = {
        PREDECESSOR: pl.DataFrame([_row(PREDECESSOR - timedelta(days=1), 1), _row(DESTINATION, 1)])
    }
    result, persisted = _run(
        tmp_path,
        monkeypatch,
        lambda root: _transformer(root, monkeypatch, inputs),
    )
    assert result.status == "completed_with_warnings"
    assert result.rows_partition_trimmed == 1
    assert result.rows_partition_trim_unrecoverable == 1
    assert result.rows_skipped == 0
    assert persisted == ("completed_with_warnings", 0)


def test_p_t16b_unresolved_windows_are_source_evaluation_occurrences(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = {
        PREDECESSOR: pl.DataFrame([_row(PREDECESSOR, 1), _row(DESTINATION, 1)]),
    }

    def factory(root: Path) -> MIDTransformer:
        transformer = MIDTransformer(root)
        monkeypatch.setattr(
            transformer,
            "read_bronze",
            lambda source_date: inputs.get(source_date, pl.DataFrame()),
        )

        def unresolved(_source_date: date) -> None:
            transformer.last_partition_filter_unresolved_count += 1
            return None

        monkeypatch.setattr(transformer, "_source_window_plan", unresolved)
        return transformer

    result, persisted = _run(
        tmp_path,
        monkeypatch,
        factory,
        start=PREDECESSOR,
        end=DESTINATION,
    )
    assert result.partition_windows_unresolved == 2
    assert result.status == "completed_with_warnings"
    assert persisted[0] == "completed_with_warnings"


def test_p_t02_runner_destination_list_is_unchanged_for_bst_and_gmt_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[date] = []
    successor = DESTINATION + timedelta(days=1)
    inputs = {
        PREDECESSOR: pl.DataFrame([_row(DESTINATION, 1)]),
        DESTINATION: pl.DataFrame(
            [
                {
                    "settlementDate": DESTINATION.isoformat(),
                    "settlementPeriod": 48,
                    "startTime": "2024-01-15T23:30:00Z",
                    "publishTime": "2024-01-16T00:00:00Z",
                    "fuelType": "CCGT",
                    "generation": 1.0,
                }
            ]
        ),
    }

    def mid_factory(root: Path) -> MIDTransformer:
        transformer = _transformer(root, monkeypatch, inputs)
        original = transformer.run

        def observed(target_date: date, **kwargs: object) -> int:
            calls.append(target_date)
            return original(target_date, **kwargs)

        monkeypatch.setattr(transformer, "run", observed)
        return transformer

    _run(tmp_path / "bst", monkeypatch, mid_factory)
    assert calls == [DESTINATION]

    fuel_inputs = {DESTINATION: inputs[DESTINATION]}

    def fuel_factory(root: Path) -> FuelHHTransformer:
        transformer = FuelHHTransformer(root)
        monkeypatch.setattr(
            transformer,
            "read_bronze",
            lambda source_date: fuel_inputs.get(source_date, pl.DataFrame()),
        )
        monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)
        original = transformer.run

        def observed(target_date: date, **kwargs: object) -> int:
            calls.append(target_date)
            return original(target_date, **kwargs)

        monkeypatch.setattr(transformer, "run", observed)
        return transformer

    calls.clear()
    first, _ = _run(tmp_path / "gmt-first", monkeypatch, fuel_factory, dataset="fuelhh")
    assert calls == [DESTINATION]
    assert first.rows_out == 1

    fuel_inputs[successor] = inputs[DESTINATION]
    calls.clear()
    healed, _ = _run(tmp_path / "gmt-healed", monkeypatch, fuel_factory, dataset="fuelhh")
    assert calls == [DESTINATION]
    assert healed.rows_out == 1


def test_p_t15_prior_accounting_survives_a_later_raising_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = PREDECESSOR
    first_source = first - timedelta(days=1)

    def factory(root: Path) -> MIDTransformer:
        transformer = MIDTransformer(root)

        def read(source_date: date) -> pl.DataFrame:
            if source_date == DESTINATION:
                raise RuntimeError("durability probe")
            if source_date == first_source:
                return pl.DataFrame([_row(first_source, 1), _row(first, 1)])
            return pl.DataFrame([_row(first, 2)])

        monkeypatch.setattr(transformer, "read_bronze", read)
        monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)
        return transformer

    result, persisted = _run(tmp_path, monkeypatch, factory, start=first, end=DESTINATION)
    assert result.status == "failed"
    assert result.rows_partition_trimmed == 1
    assert "durability probe" in (result.error or "")
    assert persisted[0] == "failed"


def test_p_t13_all_dropped_neighbour_does_not_fail_healthy_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = {
        PREDECESSOR: pl.DataFrame([_row(PREDECESSOR, 1)]),
        DESTINATION: pl.DataFrame([_row(DESTINATION, 1)]),
    }

    def factory(root: Path) -> MIDTransformer:
        transformer = _transformer(root, monkeypatch, inputs)

        def source_window(source_date: date) -> _PublicationWindowPlan:
            window_start = (
                datetime.combine(DESTINATION, datetime.min.time(), tzinfo=UTC)
                if source_date == PREDECESSOR
                else datetime.combine(source_date, datetime.min.time(), tzinfo=UTC)
            )
            return _PublicationWindowPlan(
                column="timestamp_utc",
                window=RequestWindow(
                    start=window_start,
                    end=window_start + timedelta(days=1),
                    param_names=("periodStart", "periodEnd"),
                ),
                from_param="periodStart",
                to_param="periodEnd",
                interval_semantics=IntervalSemantics.HALF_OPEN,
            )

        monkeypatch.setattr(transformer, "_source_window_plan", source_window)
        return transformer

    result, persisted = _run(tmp_path, monkeypatch, factory)

    assert result.status == "success"
    assert result.rows_out == 1
    assert persisted == ("success", 0)


def test_p_t18_cli_clauses_are_conditional(capsys: pytest.CaptureFixture[str]) -> None:
    from gridflow.cli import _echo_transform_results

    clean = runner.DatasetResult("elexon", "mid", "transform", "success", rows_out=1)
    _echo_transform_results("elexon", [clean])
    assert capsys.readouterr().out == "  elexon/mid: 1 rows transformed\n"

    counted = runner.DatasetResult(
        "elexon",
        "mid",
        "transform",
        "success",
        rows_out=1,
        rows_partition_trimmed=2,
    )
    _echo_transform_results("elexon", [counted])
    assert "2 routine covering-set trim" in capsys.readouterr().out
