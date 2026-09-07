"""Runner/status regressions for partition-ownership accounting."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl

from gridflow.pipeline import runner
from gridflow.silver.elexon.mid import MIDTransformer

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
    factory: Callable[[Path], MIDTransformer],
    start: date = DESTINATION,
    end: date = DESTINATION,
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
            ["mid"],
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


def test_p_t10_p_t16b_unresolved_windows_are_source_evaluation_occurrences(
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
    assert "2 partition-trimmed" in capsys.readouterr().out
