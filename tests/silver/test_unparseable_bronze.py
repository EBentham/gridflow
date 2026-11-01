"""A bronze body that cannot be parsed refuses its date instead of shrinking silver.

Each Elexon and ENTSO-E reader used to skip an unparseable body with a warning
and write the remaining bodies over the partition, so the run reported
``success`` with fewer rows, or left a stale file in place. Now the date is
refused: nothing is written for it and the old file stays, the rest of the
range is still transformed, and the dataset ends ``failed`` naming the file and
``UNPARSEABLE_BODY``. These tests go through the real ``run_transform``,
because the point is the status, not a counter.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    import pytest

    from gridflow.pipeline.runner import DatasetResult

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
DAY = date(2024, 1, 15)
NEXT_DAY = date(2024, 1, 16)


def _partition(data_dir: Path, source: str, dataset: str, day: date) -> Path:
    path = data_dir / "bronze" / source / dataset / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _agws_records(day: date, periods: range) -> list[dict[str, Any]]:
    return [
        {
            "dataset": "AGWS",
            "documentId": f"NGET-EMFIP-AGWS-{period}",
            "documentRevisionNumber": 1,
            "publishTime": f"{day.isoformat()}T12:00:00Z",
            "businessType": "Wind generation",
            "psrType": psr_type,
            "quantity": 100.0 + period,
            "settlementDate": day.isoformat(),
            "settlementPeriod": period,
        }
        for period in periods
        for psr_type in ("Solar", "Wind Offshore", "Wind Onshore")
    ]


def _seed_agws(data_dir: Path, day: date) -> tuple[Path, Path]:
    """Two bodies of 72 rows each: one AGWS day split across two captures."""
    partition = _partition(data_dir, "elexon", "agws", day)
    first = partition / f"raw_{day:%Y%m%d}T120000Z_aaaaaaaa.json"
    second = partition / f"raw_{day:%Y%m%d}T130000Z_bbbbbbbb.json"
    first.write_text(json.dumps({"data": _agws_records(day, range(1, 25))}))
    second.write_text(json.dumps({"data": _agws_records(day, range(25, 49))}))
    return first, second


def _truncate(path: Path) -> None:
    body = path.read_bytes()
    path.write_bytes(body[: len(body) // 2])


def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(tmp_path / "catalogue" / "gridflow.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return data_dir


def _transform(source: str, dataset: str, first: date, last: date | None = None) -> DatasetResult:
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    settings = load_settings()
    pipeline_runner.import_transformers()
    db_path = settings.pipeline.duckdb_path
    init_catalogue(db_path, settings.pipeline.data_dir)
    con = get_connection(db_path)
    last = last or first
    try:
        results = pipeline_runner.run_transform(
            pipeline_runner.PipelineContext(con=con, settings=settings),
            source,
            [dataset],
            datetime(first.year, first.month, first.day, tzinfo=UTC),
            datetime(last.year, last.month, last.day, tzinfo=UTC),
        )
    finally:
        con.close()
    assert len(results) == 1
    return results[0]


def _silver(data_dir: Path, source: str, dataset: str, day: date) -> Path:
    from gridflow.silver.registry import get_transformer

    return get_transformer(source, dataset, data_dir)._silver_destination(day)


def test_elexon_corrupt_body_refuses_the_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = _env(tmp_path, monkeypatch)
    _, second = _seed_agws(data_dir, DAY)
    assert _transform("elexon", "agws", DAY).status == "success"
    assert pl.read_parquet(_silver(data_dir, "elexon", "agws", DAY)).height == 144

    _truncate(second)
    result = _transform("elexon", "agws", DAY)

    assert result.status == "failed"
    assert result.bronze_unvouched == 1
    assert "UNPARSEABLE_BODY=1" in (result.error or "")
    assert second.name in (result.error or "")
    assert pl.read_parquet(_silver(data_dir, "elexon", "agws", DAY)).height == 144


def test_elexon_error_envelope_refuses_the_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dict with no ``data`` list is an error reply, not empty data."""
    data_dir = _env(tmp_path, monkeypatch)
    _, second = _seed_agws(data_dir, DAY)
    empty = second.with_name(f"raw_{DAY:%Y%m%d}T140000Z_cccccccc.json")
    empty.write_text(json.dumps({"data": []}))
    assert _transform("elexon", "agws", DAY).status == "success"

    second.write_text(json.dumps({"error": "upstream failed"}))
    result = _transform("elexon", "agws", DAY)

    assert result.status == "failed"
    assert result.bronze_unvouched == 1
    assert "UNPARSEABLE_BODY=1" in (result.error or "")
    assert second.name in (result.error or "")
    assert empty.name not in (result.error or "")
    assert pl.read_parquet(_silver(data_dir, "elexon", "agws", DAY)).height == 144


def test_refused_date_does_not_stop_the_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = _env(tmp_path, monkeypatch)
    _, second = _seed_agws(data_dir, DAY)
    _truncate(second)
    _seed_agws(data_dir, NEXT_DAY)

    result = _transform("elexon", "agws", DAY, NEXT_DAY)

    assert result.status == "failed"
    assert result.bronze_unvouched == 1
    assert not _silver(data_dir, "elexon", "agws", DAY).exists()
    assert pl.read_parquet(_silver(data_dir, "elexon", "agws", NEXT_DAY)).height == 144


def test_refused_date_detail_survives_a_later_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash on a later date must not hide which file refused an earlier one."""
    from gridflow.silver.elexon.agws import AGWSTransformer

    data_dir = _env(tmp_path, monkeypatch)
    _, second = _seed_agws(data_dir, DAY)
    _truncate(second)
    _seed_agws(data_dir, NEXT_DAY)

    def crash(self: AGWSTransformer, raw_df: pl.DataFrame) -> pl.DataFrame:
        raise RuntimeError("second date failed")

    monkeypatch.setattr(AGWSTransformer, "transform", crash)
    result = _transform("elexon", "agws", DAY, NEXT_DAY)

    assert result.status == "failed"
    assert "second date failed" in (result.error or "")
    assert "UNPARSEABLE_BODY=1" in (result.error or "")
    assert second.name in (result.error or "")


def test_bmunits_corrupt_newest_body_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The newest capture is corrupt: keep the file, never fall back to an older one."""
    data_dir = _env(tmp_path, monkeypatch)
    older = _partition(data_dir, "elexon", "bmunits_reference", DAY) / "raw_20240115T000000Z_a.json"
    older.write_bytes((FIXTURES / "elexon" / "bmunits_response.json").read_bytes())
    assert _transform("elexon", "bmunits_reference", DAY).status == "success"
    reference = data_dir / "silver" / "elexon" / "bmunits_reference" / "bmunits_reference.parquet"
    before = reference.read_bytes()

    newest = _partition(data_dir, "elexon", "bmunits_reference", NEXT_DAY)
    (newest / "raw_20240116T000000Z_b.json").write_text('{"data": [{"bmUnit": "T_')
    result = _transform("elexon", "bmunits_reference", NEXT_DAY)

    assert result.status == "failed"
    assert "UNPARSEABLE_BODY=1" in (result.error or "")
    assert reference.read_bytes() == before
