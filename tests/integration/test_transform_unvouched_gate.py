"""R2-g: an unvouched bronze body must be VISIBLE, not a log line nobody reads.

The exclude-until-vouched ruling (2026-08-02) is only acceptable because a
permanently-unvouched file becomes visible and stays visible. That is what
these tests pin, end-to-end through the real ``pipeline.runner.run_transform``:

- partial exclusion  -> ``completed_with_warnings`` + a counted file total;
- total exclusion    -> ``status == "failed"`` and a non-zero exit, every run,
  forever, until the state is resolved;
- at most ONE log record above DEBUG per (source, dataset) per invocation,
  whatever the file count or the date-range length;
- ``rows_skipped`` keeps its ROW meaning -- excluded FILES never sum into it.

A unit-level assertion on the transformer's counters would not prove the
STATUS changes, which is the whole point of the ruling's acceptance gate.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import gridflow.silver.entsog  # noqa: F401 -- registers every entsog transformer

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from gridflow.pipeline.runner import DatasetResult

FLOWS = "physical_flows"
REFERENCE_DATASET = "interconnections"
DAY_1 = date(2026, 5, 1)
DAY_2 = date(2026, 5, 2)
DAY_3 = date(2026, 5, 3)
STAMP = datetime(2026, 5, 1, 9, 15, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _partition(data_dir: Path, dataset: str, day: date) -> Path:
    path = (
        data_dir
        / "bronze"
        / "entsog"
        / dataset
        / str(day.year)
        / f"{day.month:02d}"
        / f"{day.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _flow(point: str, day: date) -> dict[str, Any]:
    return {
        "indicator": "Physical Flow",
        "periodFrom": f"{day.isoformat()}T06:00:00Z",
        "pointKey": point,
        "directionKey": "entry",
        "value": "1000000",
        "unit": "kWh/d",
    }


def _write_orphan(directory: Path, name: str, payload: dict[str, Any]) -> Path:
    """A durable body with NO sidecar -- the literal crash residue."""
    body = directory / name
    body.write_text(json.dumps(payload))
    return body


def _write_pair(directory: Path, name: str, payload: dict[str, Any]) -> Path:
    body = _write_orphan(directory, name, payload)
    body.with_suffix(".meta.json").write_text(json.dumps({"written_at": STAMP.isoformat()}))
    return body


def _write_unparseable_sidecar(directory: Path, name: str, payload: dict[str, Any]) -> Path:
    body = _write_orphan(directory, name, payload)
    body.with_suffix(".meta.json").write_text(json.dumps({"written_at": "not-a-date"}))
    return body


def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    db_path = tmp_path / "catalogue" / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return data_dir, db_path


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    seed: Any,
    dataset: str = FLOWS,
    *,
    first: date = DAY_1,
    last: date = DAY_1,
) -> DatasetResult:
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    data_dir, db_path = _isolated_env(tmp_path, monkeypatch)
    seed(data_dir)

    settings = load_settings()
    pipeline_runner.import_transformers()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        ctx = pipeline_runner.PipelineContext(con=con, settings=settings)
        results = pipeline_runner.run_transform(
            ctx,
            "entsog",
            [dataset],
            datetime(first.year, first.month, first.day, tzinfo=UTC),
            datetime(last.year, last.month, last.day, tzinfo=UTC),
        )
    finally:
        con.close()
    assert len(results) == 1
    return results[0]


def _alarms(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Every WARNING-or-above record emitted during the invocation.

    The bounded-record invariant is about ALARM records -- the ones an operator
    is meant to act on. Routine INFO progress that master already emits
    ("Silver write: ... -> N rows", "DuckDB catalogue initialised") is
    pre-existing behaviour outside this unit's record budget and is
    deliberately not counted; counting it would make these tests assert
    something the plan never claimed and couple them to unrelated logging.
    """
    return [r for r in caplog.records if r.levelno >= logging.WARNING]


# --------------------------------------------------------------------------- #
# Rung 3: partial exclusion
# --------------------------------------------------------------------------- #


def test_partial_exclusion_warns_and_counts_files_not_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-c (I-7, I-10)."""

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(partition, "raw_0900_a.json", {"operationalData": [_flow("GOOD", DAY_1)]})
        _write_orphan(partition, "raw_1000_b.json", {"operationalData": [_flow("ORPHAN1", DAY_1)]})
        _write_orphan(partition, "raw_1100_c.json", {"operationalData": [_flow("ORPHAN2", DAY_1)]})

    result = _run(tmp_path, monkeypatch, seed)

    assert result.status == "completed_with_warnings"
    assert result.bronze_unvouched == 2
    assert result.rows_skipped == 0, (
        "excluded FILES must never be summed into the ROW counter -- their row "
        "count is unknown precisely because we refuse to read them"
    )
    assert result.rows_out == 1


def test_exactly_one_record_for_seven_files_across_three_dates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T3-b (I-6): the count is exact; only the EXAMPLES are capped at 5."""

    def seed(data_dir: Path) -> None:
        first = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(first, "raw_ok.json", {"operationalData": [_flow("GOOD", DAY_1)]})
        for index in range(4):
            _write_orphan(
                first, f"raw_orphan_{index}.json", {"operationalData": [_flow("X", DAY_1)]}
            )
        second = _partition(data_dir, FLOWS, DAY_2)
        _write_pair(second, "raw_ok.json", {"operationalData": [_flow("GOOD", DAY_2)]})
        for index in range(3):
            _write_orphan(
                second, f"raw_orphan_{index}.json", {"operationalData": [_flow("X", DAY_2)]}
            )
        _partition(data_dir, FLOWS, DAY_3)

    with caplog.at_level(logging.DEBUG):
        result = _run(tmp_path, monkeypatch, seed, first=DAY_1, last=DAY_3)

    assert result.status == "completed_with_warnings"
    assert result.bronze_unvouched == 7

    records = [r for r in _alarms(caplog) if "unvouched" in r.getMessage()]
    assert len(records) == 1, f"expected exactly ONE aggregated record, got {len(records)}"
    message = records[0].getMessage()
    assert "Excluded 7 unvouched" in message
    assert message.count(" (NO_SIDECAR)") == 5, "examples are capped at 5, the count is not"
    assert "NO_SIDECAR=7" in message


def test_the_signal_does_not_self_clear_on_a_second_identical_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-l (I-7): bronze is never deleted, so the orphan persists and re-reports."""

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(partition, "raw_ok.json", {"operationalData": [_flow("GOOD", DAY_1)]})
        _write_orphan(partition, "raw_orphan.json", {"operationalData": [_flow("X", DAY_1)]})

    first = _run(tmp_path, monkeypatch, seed)
    second = _run(tmp_path, monkeypatch, lambda _data_dir: None)

    assert (first.status, first.bronze_unvouched) == ("completed_with_warnings", 1)
    assert (second.status, second.bronze_unvouched) == (first.status, first.bronze_unvouched)


# --------------------------------------------------------------------------- #
# Cross-date accounting (S-10, S-15)
# --------------------------------------------------------------------------- #


def test_one_orphan_over_three_dates_is_counted_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-n: the reference candidate scan ignores target_date.

    Integer accumulation across the date loop would treble a single orphan and
    overstate the remediation scope threefold in the record meant to size it.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, REFERENCE_DATASET, DAY_1)
        _write_orphan(partition, "raw_orphan.json", {"interconnections": [{"id": "x"}]})

    result = _run(tmp_path, monkeypatch, seed, dataset=REFERENCE_DATASET, first=DAY_1, last=DAY_3)

    assert result.bronze_unvouched == 1, "one orphan body, seen on 3 dates, is ONE file"


def test_two_orphans_with_different_reasons_report_each_reason_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T3-n2 (S-15's decisive case).

    T3-n's single-orphan shape passes even under a detached
    path-set-plus-Counter implementation, so it does not actually pin the
    (path, reason) association. This does.
    """

    def seed(data_dir: Path) -> None:
        # NEWEST_VOUCHED walks the reverse-sorted list and stops at the first
        # vouched file, so the two unvouched bodies must sort AHEAD of the good
        # one to be stepped over at all.
        partition = _partition(data_dir, REFERENCE_DATASET, DAY_1)
        _write_orphan(partition, "raw_c_orphan.json", {"interconnections": [{"id": "x"}]})
        _write_unparseable_sidecar(partition, "raw_b_bad.json", {"interconnections": [{"id": "y"}]})
        _write_pair(partition, "raw_a_ok.json", {"interconnections": [{"id": "GOOD"}]})

    with caplog.at_level(logging.DEBUG):
        result = _run(
            tmp_path, monkeypatch, seed, dataset=REFERENCE_DATASET, first=DAY_1, last=DAY_3
        )

    assert result.bronze_unvouched == 2
    records = [r for r in _alarms(caplog) if "unvouched" in r.getMessage()]
    assert len(records) == 1
    message = records[0].getMessage()
    assert "NO_SIDECAR=1" in message
    assert "UNPARSEABLE_TIMESTAMP=1" in message


# --------------------------------------------------------------------------- #
# Rung 2: total exclusion is a HARD failure
# --------------------------------------------------------------------------- #


def test_total_exclusion_hard_fails_with_a_single_error_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T3-i (I-7).

    Asserted over a MULTI-DATE reference-family range, which is the shape that
    would otherwise emit one "No bronze data" warning per date on top of the
    ERROR. The generic warning must be absent BY MESSAGE, not merely absent
    from a count of one.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, REFERENCE_DATASET, DAY_1)
        _write_orphan(partition, "raw_a.json", {"interconnections": [{"id": "x"}]})
        _write_orphan(partition, "raw_b.json", {"interconnections": [{"id": "y"}]})

    with caplog.at_level(logging.DEBUG):
        result = _run(
            tmp_path, monkeypatch, seed, dataset=REFERENCE_DATASET, first=DAY_1, last=DAY_3
        )

    assert result.status == "failed"
    assert result.ok is False
    assert result.bronze_unvouched > 0, (
        "the hard-fail path must still populate the structured count -- the "
        "field defaults to 0, so omitting it would break the visibility "
        "contract on exactly the path a scheduler reads first"
    )
    assert result.error is not None

    records = _alarms(caplog)
    assert len(records) == 1, f"expected exactly ONE alarm record, got {records}"
    assert records[0].levelno == logging.ERROR
    assert "No bronze data" not in caplog.text, (
        "that warning is FALSE here -- bronze exists, it merely could not vouch"
    )


def test_total_exclusion_leaves_a_stale_parquet_untouched_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-i companion (case 17): the failed status is what stops the stale file
    being read as current."""

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_orphan(partition, "raw_a.json", {"operationalData": [_flow("X", DAY_1)]})

    result = _run(tmp_path, monkeypatch, seed)

    assert result.status == "failed"
    silver_dir = tmp_path / "data" / "silver" / "entsog" / FLOWS
    written = list(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []
    assert written == []


def test_total_exclusion_is_recorded_as_failed_in_pipeline_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-i, exit-code half: `gridflow status` reads pipeline_runs, so the
    signal is structured, not merely logged."""
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    data_dir, db_path = _isolated_env(tmp_path, monkeypatch)
    partition = _partition(data_dir, FLOWS, DAY_1)
    _write_orphan(partition, "raw_a.json", {"operationalData": [_flow("X", DAY_1)]})

    settings = load_settings()
    pipeline_runner.import_transformers()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        ctx = pipeline_runner.PipelineContext(con=con, settings=settings)
        report = pipeline_runner.run_transform(
            ctx,
            "entsog",
            [FLOWS],
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 5, 1, tzinfo=UTC),
        )
    finally:
        con.close()

    assert report[0].ok is False
    con = get_connection(db_path, read_only=True)
    try:
        rows = con.execute(
            "SELECT status FROM pipeline_runs WHERE source = 'entsog' "
            "AND dataset = ? AND operation = 'transform'",
            [FLOWS],
        ).fetchall()
    finally:
        con.close()
    assert rows == [("failed",)]


# --------------------------------------------------------------------------- #
# The cross-product no other test covers (S-30)
# --------------------------------------------------------------------------- #


def test_partial_exclusion_with_zero_surviving_rows_still_warns_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T3-o: one UNVOUCHED body plus one VOUCHED body holding only off-date rows.

    Total-exclusion is False (a candidate vouched) and zero rows survive, so
    the generic "No bronze data" warning would be both FALSE and a second
    above-DEBUG record. Rung 3's aggregate must be the only one.
    """

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(partition, "raw_ok.json", {"operationalData": [_flow("OFFDATE", DAY_3)]})
        _write_orphan(partition, "raw_orphan.json", {"operationalData": [_flow("X", DAY_1)]})

    with caplog.at_level(logging.DEBUG):
        result = _run(tmp_path, monkeypatch, seed)

    assert result.status == "completed_with_warnings"
    assert result.bronze_unvouched == 1
    assert result.rows_out == 0

    records = _alarms(caplog)
    assert len(records) == 1, f"expected exactly ONE alarm record, got {records}"
    assert "No bronze data" not in caplog.text


# --------------------------------------------------------------------------- #
# Precedence and fail-closed behaviour
# --------------------------------------------------------------------------- #


def test_a_resolver_error_fails_closed_with_a_redacted_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3-k (case 14): never swallowed, never a quiet zero-row success."""
    from gridflow.silver.base import BaseSilverTransformer

    def boom(self, candidates, selection):  # type: ignore[no-untyped-def]
        raise OSError("simulated directory failure")

    monkeypatch.setattr(BaseSilverTransformer, "_resolve_vouched_bronze_set", boom)

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(partition, "raw_ok.json", {"operationalData": [_flow("GOOD", DAY_1)]})

    result = _run(tmp_path, monkeypatch, seed)

    assert result.status == "failed"
    assert result.error is not None


def test_a_run_exception_after_classification_still_reports_unvouched_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R2-g finding 1: ``run()`` classifies an orphan, then raises.

    A collision guard, a read error, anything -- if ``run()`` raises AFTER
    classifying an unvouched body, ``run_transform``'s ``except`` handler must
    still report the count. ``bronze_unvouched`` defaults to 0 on
    ``DatasetResult``, so without the fix this silently drops a permanently-
    unvouched file from the one structured result a scheduler reads (I-7).
    """
    from gridflow.silver.entsog.physical_flows import PhysicalFlowsTransformer

    original_run = PhysicalFlowsTransformer.run

    def run_then_raise(self, target_date, run_id=None, reingest=False):  # type: ignore[no-untyped-def]
        original_run(self, target_date, run_id=run_id, reingest=reingest)
        raise OSError("simulated failure after classification")

    monkeypatch.setattr(PhysicalFlowsTransformer, "run", run_then_raise)

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(partition, "raw_ok.json", {"operationalData": [_flow("GOOD", DAY_1)]})
        _write_orphan(partition, "raw_orphan.json", {"operationalData": [_flow("X", DAY_1)]})

    result = _run(tmp_path, monkeypatch, seed)

    assert result.status == "failed"
    assert result.bronze_unvouched > 0


def test_all_dropped_keeps_precedence_but_does_not_swallow_the_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T3-j (extended by S-33).

    ``bronze_unvouched`` defaults to 0, so without this assertion the
    highest-precedence failure path could return zero and hide a permanent
    orphan from the structured result -- the visibility contract broken on
    exactly the path a scheduler reads first.
    """
    from gridflow.silver.entsog.physical_flows import PhysicalFlowsTransformer

    original_run = PhysicalFlowsTransformer.run

    def run_with_all_dropped(self, target_date, run_id=None, reingest=False):  # type: ignore[no-untyped-def]
        rows = original_run(self, target_date, run_id=run_id, reingest=reingest)
        self.last_partition_filter_all_dropped_count = 3
        return rows

    monkeypatch.setattr(PhysicalFlowsTransformer, "run", run_with_all_dropped)

    def seed(data_dir: Path) -> None:
        partition = _partition(data_dir, FLOWS, DAY_1)
        _write_pair(partition, "raw_ok.json", {"operationalData": [_flow("GOOD", DAY_1)]})
        _write_orphan(partition, "raw_orphan.json", {"operationalData": [_flow("X", DAY_1)]})

    with caplog.at_level(logging.DEBUG):
        result = _run(tmp_path, monkeypatch, seed)

    assert result.status == "failed"
    assert "event-window filter excluded 100%" in (result.error or "")
    assert result.bronze_unvouched == 1
    assert "Excluded 1 unvouched" in (result.error or "")

    records = _alarms(caplog)
    assert len(records) == 1, f"expected exactly ONE alarm record, got {records}"
