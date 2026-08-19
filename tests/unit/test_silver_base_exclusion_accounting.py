"""D-41 / D-42: every exclusion, at every granularity, reaches the run status.

``BaseSilverTransformer``'s ``VINTAGE_PER_BRONZE_FILE`` branch had no
file-level accounting at all -- both of its skips dropped a whole vintage in
silence -- and ``_process_frame`` returned ``None`` on an empty ``transform()``
output with nothing to distinguish "the transformer declined rows and counted
them" from "the frame emptied for a reason nothing counted". Both states
reached ``run_transform`` as plain ``success`` with zero rows.

``elexon/system_prices`` is the ONLY other ``VINTAGE_PER_BRONZE_FILE``
transformer in the repo (grep-verified), so it is where the behaviour change
lands and it is proven here deliberately rather than discovered later:

- a skipped bronze body      -> ``completed_with_warnings`` (was silent ``success``)
- every candidate skipped    -> ``failed``            (was ``success``, zero rows)
- transform() empty, uncounted -> ``completed_with_warnings``, or ``failed`` when
  no body for the date produced a frame (was ``success``, zero rows)

and the boundary that must NOT move: a body whose rows were ALL row-excluded is
a CONSUMED file, not an excluded one, so it stays on the warning rung.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl
import pytest

from gridflow.silver.base import BaseSilverTransformer, BronzeVouchReason
from gridflow.silver.elexon.system_prices import SystemPriceTransformer

if TYPE_CHECKING:
    from pathlib import Path

TARGET_DATE = date(2024, 1, 15)
STAMP = datetime(2024, 1, 15, 8, tzinfo=UTC)
EXCLUDED_PERIOD = 2
"""The settlement period the row-level fixtures below declare invalid."""


# --------------------------------------------------------------------------- #
# Bronze fixtures
# --------------------------------------------------------------------------- #


def _price_row(period: int, sell_price: float = 44.0) -> dict[str, Any]:
    return {
        "settlementDate": TARGET_DATE.isoformat(),
        "settlementPeriod": period,
        "systemSellPrice": sell_price,
        "systemBuyPrice": 55.0,
        "netImbalanceVolume": -120.5,
    }


def _column_starved_row(period: int) -> dict[str, Any]:
    """A non-empty record missing required columns.

    ``SystemPriceTransformer.transform`` returns an EMPTY frame for this shape
    with only a ``logger.error`` -- the measured, real defect D-42 exists for,
    not a hypothetical one.
    """
    return {"settlementDate": TARGET_DATE.isoformat(), "settlementPeriod": period}


def _partition(data_dir: Path) -> Path:
    path = data_dir / "bronze" / "elexon" / "system_prices" / "2024" / "01" / "15"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_body(
    partition: Path,
    name: str,
    rows: list[dict[str, Any]],
    *,
    sidecar: bool = True,
    stamp: datetime = STAMP,
) -> Path:
    """Write one bronze body, with or without the sidecar that vouches for it."""
    body = partition / f"raw_{name}.json"
    body.write_text(json.dumps({"data": rows}))
    if sidecar:
        body.with_suffix(".meta.json").write_text(json.dumps({"written_at": stamp.isoformat()}))
    return body


# --------------------------------------------------------------------------- #
# Transformer fixtures -- subclasses of the REAL transformer, never a stub that
# re-implements the behaviour under test.
# --------------------------------------------------------------------------- #


class _AllRowsExcluded(SystemPriceTransformer):
    """Declines every row and COUNTS them: an accounted empty frame."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Exclude the whole frame, accumulating the count with ``+=``."""
        self.last_excluded_row_count += raw_df.height
        return pl.DataFrame()


class _ExcludesFlaggedRows(SystemPriceTransformer):
    """Declines only rows at :data:`EXCLUDED_PERIOD`, counting each one."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Drop the flagged rows, count them, then normalise the survivors."""
        kept = raw_df.filter(pl.col("settlementPeriod") != EXCLUDED_PERIOD)
        self.last_excluded_row_count += raw_df.height - kept.height
        return super().transform(kept)


class _AccountedOrUnaccounted(SystemPriceTransformer):
    """Accounted empty for a flagged body; the REAL uncounted empty otherwise."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Count-and-decline a flagged frame; else delegate to the real path."""
        if raw_df.height and raw_df.get_column("settlementPeriod")[0] == EXCLUDED_PERIOD:
            self.last_excluded_row_count += raw_df.height
            return pl.DataFrame()
        return super().transform(raw_df)


class _RaisesOnBoom(SystemPriceTransformer):
    """Raises while READING any body whose name carries ``boom``."""

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Simulate a mid-loop read failure (X-10) on the flagged body."""
        if "boom" in raw_path.name:
            raise RuntimeError("simulated bronze read failure")
        return super().read_bronze_file(raw_path)


class _ExcludesRowsThenRaises(_RaisesOnBoom):
    """Row-level accounting on the first body, then a raise on the second."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Drop the flagged rows, count them, then normalise the survivors."""
        kept = raw_df.filter(pl.col("settlementPeriod") != EXCLUDED_PERIOD)
        self.last_excluded_row_count += raw_df.height - kept.height
        return super().transform(kept)


class _UnaccountedThenRaises(_RaisesOnBoom):
    """An UNCOUNTED empty frame on the first body, then a raise on the second."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Return empty and count NOTHING -- exactly what D-42 must detect."""
        return pl.DataFrame()


class _PlainBranchEmptyTransform(BaseSilverTransformer):
    """A NON-``VINTAGE_PER_BRONZE_FILE`` transformer with an uncounted empty frame.

    The gate's control. ``_process_frame`` has three call sites, so an ungated
    D-42 increment would fire here too and flip this transformer -- and every
    other transformer in the repo -- off ``success``.
    """

    source = "test_gate"
    dataset = "plain_empty"
    schema_cls = None
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ("value",)

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """A non-empty raw frame, so the empty output comes from transform()."""
        return pl.DataFrame({"value": [1, 2, 3]})

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Return empty for an uncounted reason, exactly as master's do."""
        return pl.DataFrame()


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #


def _run_transform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    seed: Any,
    *,
    source: str = "elexon",
    dataset: str = "system_prices",
    transformer_cls: type[BaseSilverTransformer] | None = None,
    captured: list[BaseSilverTransformer] | None = None,
) -> Any:
    """Drive the REAL ``pipeline.runner.run_transform`` over a seeded bronze tree.

    A unit assertion on the transformer's counters cannot prove the reported
    STATUS, and the status is the whole claim of D-41 and D-42.

    Args:
        tmp_path: Per-test temporary root.
        monkeypatch: Fixture used to isolate every gridflow path env var.
        seed: Callable receiving the data dir; writes the bronze tree.
        source: Source name passed to ``run_transform``.
        dataset: Dataset name passed to ``run_transform``.
        transformer_cls: When set, ``get_transformer`` is stubbed to return an
            instance of it, so a fixture subclass never has to be entered into
            the process-wide registry.
        captured: When given, every constructed fixture instance is appended to
            it, so a test can assert on the producer as well as the consumer.

    Returns:
        The single ``DatasetResult`` for the requested dataset.
    """
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    data_dir = tmp_path / "data"
    db_path = tmp_path / "catalogue" / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    if transformer_cls is not None:

        def _factory(_source: str, _dataset: str, root: Path) -> BaseSilverTransformer:
            instance = transformer_cls(root)
            if captured is not None:
                captured.append(instance)
            return instance

        monkeypatch.setattr("gridflow.silver.registry.get_transformer", _factory)
    seed(data_dir)

    settings = load_settings()
    pipeline_runner.import_transformers()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        ctx = pipeline_runner.PipelineContext(con=con, settings=settings)
        results = pipeline_runner.run_transform(
            ctx,
            source,
            [dataset],
            datetime(2024, 1, 15, tzinfo=UTC),
            datetime(2024, 1, 15, tzinfo=UTC),
        )
    finally:
        con.close()
    assert len(results) == 1
    return results[0]


# --------------------------------------------------------------------------- #
# D-41: file-level accounting on the VINTAGE_PER_BRONZE_FILE branch
# --------------------------------------------------------------------------- #


def test_a_skipped_body_warns_instead_of_vanishing_under_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 1. One good body, one unsidecarred body.

    Master reported plain ``success``: a whole vintage disappeared with only a
    WARNING nobody reads. The good body's rows must still be written -- this is
    accounting, not a new refusal.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_good", [_price_row(1)])
        _write_body(partition, "b_orphan", [_price_row(3)], sidecar=False)

    result = _run_transform(tmp_path, monkeypatch, seed)

    assert result.status == "completed_with_warnings"
    assert result.bronze_unvouched == 1
    assert result.rows_out == 1, "the vouched body's rows must still reach silver"
    assert result.rows_skipped == 0, "an excluded FILE is never summed into a ROW counter"


def test_every_candidate_skipped_is_a_failure_not_a_zero_row_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 2. Bronze demonstrably exists and no row could be read from it.

    Only a ``failed`` status stops a stale pre-existing Parquet passing as
    current, which is the argument the sibling rungs already make.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_orphan", [_price_row(1)], sidecar=False)
        _write_body(partition, "b_orphan", [_price_row(2)], sidecar=False)

    result = _run_transform(tmp_path, monkeypatch, seed)

    assert result.status == "failed"
    assert result.bronze_unvouched == 2
    assert result.rows_out == 0


def test_all_rows_excluded_is_a_consumed_file_and_must_not_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 3, the D-40/D-41 boundary nothing else pins.

    One fully-vouched body whose provenance is fine and whose rows are ALL
    row-excluded. It was READ and CONSUMED, and its rows were accounted by
    D-40, so it is not a file-level exclusion. A "was any frame processed"
    total-exclusion predicate gets this wrong, and the failure rung outranks
    the warning rung, so the wrong answer would win silently.
    """
    captured: list[BaseSilverTransformer] = []

    def seed(data_dir: Path) -> None:
        _write_body(_partition(data_dir), "a_good", [_price_row(1), _price_row(2)])

    result = _run_transform(
        tmp_path, monkeypatch, seed, transformer_cls=_AllRowsExcluded, captured=captured
    )

    assert result.status == "completed_with_warnings", (
        "an all-rows-excluded file is CONSUMED, not excluded -- this must not fail"
    )
    assert result.rows_invalid == 2
    assert result.bronze_unvouched == 0
    assert captured[0].last_unaccounted_empty_frames == 0, (
        "the counter moved, so the empty frame was ACCOUNTED, not unaccounted"
    )


def test_unusable_provenance_is_its_own_reason_not_a_borrowed_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D-41's new enum member, asserted on the ASSOCIATION.

    The sidecar vouched and the timestamp key was present and fine; what failed
    was one level further in. ``NO_TIMESTAMP_KEY`` would be a false record.
    """
    data_dir = tmp_path / "data"
    partition = _partition(data_dir)
    body = _write_body(partition, "a_empty", [])
    transformer = SystemPriceTransformer(data_dir)

    assert transformer.run(TARGET_DATE, run_id="provenance") == 0
    assert transformer.last_unvouched_bronze == frozenset(
        {(body, BronzeVouchReason.UNUSABLE_PROVENANCE)}
    )
    assert transformer.last_unvouched_total_exclusion is True


# --------------------------------------------------------------------------- #
# D-42: frame-level accounting, classified by the base class
# --------------------------------------------------------------------------- #


def test_a_missing_required_column_no_longer_reports_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Case 4, the blocker. The REAL transformer's REAL early return.

    ``system_prices`` returns an empty frame on a missing required column with
    only a ``logger.error``. With a good sibling body the date still produces a
    frame, so the correct outcome is the warning rung -- and the frame count is
    NOT folded into the row counters, because a frame is not a row.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_good", [_price_row(1)])
        _write_body(partition, "b_starved", [_column_starved_row(2)])

    with caplog.at_level(logging.WARNING):
        result = _run_transform(tmp_path, monkeypatch, seed)

    assert result.status == "completed_with_warnings", (
        "master reported `success` with zero rows for the starved body"
    )
    assert result.rows_out == 1
    assert (result.rows_skipped, result.rows_invalid, result.bronze_unvouched) == (0, 0, 0), (
        "a FRAME count must never inflate a ROW count"
    )
    assert "unaccounted_empty_frames=1" in caplog.text


def test_every_body_starved_of_a_required_column_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 5. Bronze was read, no frame was produced, nothing counted it."""

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_starved", [_column_starved_row(1)])
        _write_body(partition, "b_starved", [_column_starved_row(2)])

    result = _run_transform(tmp_path, monkeypatch, seed)

    assert result.status == "failed"
    assert result.rows_out == 0
    assert (result.rows_skipped, result.rows_invalid) == (0, 0), (
        "the frame count must not be smuggled into the row counters"
    )
    assert result.error is not None
    assert "unaccounted_empty_frames=2" in result.error


def test_the_base_class_discriminates_accounted_from_unaccounted_empties(
    tmp_path: Path,
) -> None:
    """Case 6, side by side in one run.

    One body whose rows are all counted-and-declined and one body missing a
    required column. The base class must CLASSIFY -- an implementation that
    merely counted empty frames would report 2 here.
    """
    data_dir = tmp_path / "data"
    partition = _partition(data_dir)
    _write_body(partition, "a_excluded", [_price_row(EXCLUDED_PERIOD)])
    _write_body(partition, "b_starved", [_column_starved_row(1)])

    transformer = _AccountedOrUnaccounted(data_dir)
    assert transformer.run(TARGET_DATE, run_id="discriminate") == 0

    assert transformer.last_excluded_row_count == 1, "the accounted body's row was counted"
    assert transformer.last_unaccounted_empty_frames == 1, (
        "counting empty frames instead of classifying them would report 2"
    )
    assert transformer.last_total_unaccounted_exclusion is True
    assert transformer.last_unvouched_bronze == frozenset(), (
        "both bodies were READ and CONSUMED -- neither is a file-level exclusion"
    )


def test_the_gate_keeps_the_increment_off_every_other_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 8, the gate. ``_process_frame`` has three call sites.

    Without the ``VINTAGE_PER_BRONZE_FILE`` gate this increment silently
    reaches all six sources and flips their runs off ``success``. This is the
    assertion that keeps the "single place outside its own source" claim true.
    """
    captured: list[BaseSilverTransformer] = []

    def seed(data_dir: Path) -> None:
        (data_dir / "bronze" / "test_gate" / "plain_empty").mkdir(parents=True, exist_ok=True)

    result = _run_transform(
        tmp_path,
        monkeypatch,
        seed,
        source="test_gate",
        dataset="plain_empty",
        transformer_cls=_PlainBranchEmptyTransform,
        captured=captured,
    )

    assert captured[0].last_unaccounted_empty_frames == 0
    assert (result.status, result.rows_out) == ("success", 0), (
        "a non-vintage transformer's reported status must be unchanged from master"
    )


# --------------------------------------------------------------------------- #
# X-10: the publication rule -- producer, then the consumer's three folds
# --------------------------------------------------------------------------- #


def test_accounting_recorded_before_a_raise_survives_the_raise(tmp_path: Path) -> None:
    """Case 7, the PRODUCER half.

    A publication after the loop would discard the exclusions recorded for
    files 1..N-1 at exactly the moment the record matters most: the run fails
    while reporting ``bronze_unvouched == 0``.
    """
    data_dir = tmp_path / "data"
    partition = _partition(data_dir)
    orphan = _write_body(partition, "a_orphan", [_price_row(1)], sidecar=False)
    _write_body(partition, "b_boom", [_price_row(2)])

    transformer = _RaisesOnBoom(data_dir)
    with pytest.raises(RuntimeError, match="simulated bronze read failure"):
        transformer.run(TARGET_DATE, run_id="publication")

    assert transformer.last_unvouched_bronze == frozenset(
        {(orphan, BronzeVouchReason.NO_SIDECAR)}
    ), "the exclusion recorded before the raise was discarded"


def test_the_failed_path_still_reports_a_file_level_exclusion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 9a, the CONSUMER half at FILE level.

    ``status == "failed"`` is guaranteed by the second body raising, so it
    proves nothing on its own. The exact ``bronze_unvouched`` is the assertion.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_orphan", [_price_row(1)], sidecar=False)
        _write_body(partition, "b_boom", [_price_row(2)])

    result = _run_transform(tmp_path, monkeypatch, seed, transformer_cls=_RaisesOnBoom)

    assert result.status == "failed"
    assert result.bronze_unvouched == 1


def test_the_failed_path_still_reports_a_row_level_exclusion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 9b, the CONSUMER half at ROW level.

    The failing date never reached the in-loop accumulation, so without the
    handler's fold this exclusion is counted and then dropped.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_rows", [_price_row(1), _price_row(EXCLUDED_PERIOD)])
        _write_body(partition, "b_boom", [_price_row(3)])

    result = _run_transform(tmp_path, monkeypatch, seed, transformer_cls=_ExcludesRowsThenRaises)

    assert result.status == "failed"
    assert result.rows_invalid == 1, "the excluded row vanished from the failed result"
    assert result.rows_skipped == 1


def test_the_failed_path_still_reports_an_unaccounted_empty_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 9c, the CONSUMER half at FRAME level.

    The frame count has no ``DatasetResult`` field, so a fold nothing renders
    is exactly as invisible as no fold at all. The token, not prose, is the
    assertion.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir)
        _write_body(partition, "a_empty_frame", [_price_row(1)])
        _write_body(partition, "b_boom", [_price_row(2)])

    result = _run_transform(tmp_path, monkeypatch, seed, transformer_cls=_UnaccountedThenRaises)

    assert result.status == "failed"
    assert result.error is not None
    assert "unaccounted_empty_frames=1" in result.error
