"""R2-g Task 2: ENTSO-G reads and stamps from ONE vouched bronze set.

The prior attempt at this change failed four review passes, each on a distinct
instance of one defect class: the read path and the vintage path were two
independently-coded filesystem walks that could disagree. These tests pin the
structural fix -- one scan, one sidecar read per examined candidate, one value
threaded through both derivations -- and each of the four historical failure
modes.

FIXTURE PRECONDITION (load-bearing): no fixture record here carries a
``published_at``, so ADR-025's row-wise
``available_at = coalesce(published_at, ingest_stamp)`` falls through to the
ingest-stamp arm, which is the only arm this unit changes. A fixture carrying
a vendor ``published_at`` would correctly assert that value instead -- master's
behaviour, untouched here.
"""

from __future__ import annotations

import ast
import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

import gridflow.silver.entsog  # noqa: F401 -- registers the generic entsog family
from gridflow.silver.base import _BRONZE_VINTAGE_COLUMN
from gridflow.silver.entsog.physical_flows import PhysicalFlowsTransformer
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from gridflow.silver.entsog.generic import GenericEntsogJsonTransformer

TARGET = date(2026, 5, 1)
STAMP_A = datetime(2026, 5, 1, 9, 15, tzinfo=UTC)
STAMP_B = datetime(2026, 5, 1, 10, 30, tzinfo=UTC)

# `nominations` is a non-reference, date-windowed generic dataset;
# `interconnections` is a reference dataset (whole-tree, newest-only).
GENERIC_DATASET = "nominations"
REFERENCE_DATASET = "interconnections"


# --------------------------------------------------------------------------- #
# Fixture helpers
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


def _write_body(directory: Path, name: str, payload: dict[str, Any] | str) -> Path:
    body = directory / name
    body.write_text(payload if isinstance(payload, str) else json.dumps(payload))
    return body


def _write_sidecar(body: Path, stamp: datetime) -> None:
    body.with_suffix(".meta.json").write_text(json.dumps({"written_at": stamp.isoformat()}))


def _flow(point: str, day: date, hour: int = 6, value: str = "1000000") -> dict[str, Any]:
    return {
        "indicator": "Physical Flow",
        "periodFrom": f"{day.isoformat()}T{hour:02d}:00:00Z",
        "pointKey": point,
        "directionKey": "entry",
        "operatorKey": "OP-1",
        "value": value,
        "unit": "kWh/d",
    }


def _nomination(record_id: str, day: date, hour: int = 6) -> dict[str, Any]:
    return {
        "id": record_id,
        "periodFrom": f"{day.isoformat()}T{hour:02d}:00:00Z",
        "pointKey": "ITP-00001",
        "value": 42.0,
    }


def _flows_transformer(data_dir: Path) -> PhysicalFlowsTransformer:
    return PhysicalFlowsTransformer(data_dir)


def _generic(data_dir: Path, dataset: str = GENERIC_DATASET) -> GenericEntsogJsonTransformer:
    transformer = get_transformer("entsog", dataset, data_dir)
    assert isinstance(transformer.LOCKSTEP_BRONZE_READ, bool)
    return transformer  # type: ignore[return-value]


def _silver(data_dir: Path, dataset: str, day: date) -> pl.DataFrame:
    path = (
        data_dir
        / "silver"
        / "entsog"
        / dataset
        / f"year={day.year}"
        / f"month={day.month:02d}"
        / f"{dataset}_{day.strftime('%Y%m%d')}.parquet"
    )
    return pl.read_parquet(path)


# --------------------------------------------------------------------------- #
# T2-a: one scan, one sidecar read per EXAMINED candidate (I-1)
# --------------------------------------------------------------------------- #


class TestSingleScanLockstep:
    def test_candidates_scanned_once_and_each_sidecar_read_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T2-a."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        for name, stamp in (("raw_0900_a.json", STAMP_A), ("raw_1000_b.json", STAMP_B)):
            body = _write_body(partition, name, {"operationalData": [_flow(name, TARGET)]})
            _write_sidecar(body, stamp)

        transformer = _flows_transformer(tmp_path)
        scans: list[date] = []
        sidecar_reads: list[Path] = []

        original_candidates = transformer._bronze_candidates
        original_read = type(transformer)._read_sidecar_timestamp

        def spy_candidates(target_date: date) -> list[Path]:
            scans.append(target_date)
            return original_candidates(target_date)

        def spy_sidecar(meta_path: Path):  # type: ignore[no-untyped-def]
            sidecar_reads.append(meta_path)
            return original_read(meta_path)

        monkeypatch.setattr(transformer, "_bronze_candidates", spy_candidates)
        monkeypatch.setattr(transformer, "_read_sidecar_timestamp", spy_sidecar)

        transformer.run(TARGET, reingest=True)

        assert scans == [TARGET], "the candidate scan must happen exactly once per run()"
        assert len(sidecar_reads) == 2
        assert len(set(sidecar_reads)) == 2, "each sidecar is read exactly once"

    def test_newest_vouched_never_probes_behind_the_selected_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T2-a, trailing-candidate half.

        A resolver that eagerly probed the whole list would satisfy every
        other assertion here while counting irrelevant OLDER orphans into a
        false non-success result.
        """
        newest = _partition(tmp_path, REFERENCE_DATASET, date(2026, 5, 2))
        oldest = _partition(tmp_path, REFERENCE_DATASET, date(2026, 4, 1))
        body = _write_body(newest, "raw_new.json", {"interconnections": [{"id": "x"}]})
        _write_sidecar(body, STAMP_B)
        _write_body(oldest, "raw_old.json", {"interconnections": [{"id": "y"}]})

        transformer = _generic(tmp_path, REFERENCE_DATASET)
        reads: list[Path] = []
        original_read = type(transformer)._read_sidecar_timestamp

        def spy_sidecar(meta_path: Path):  # type: ignore[no-untyped-def]
            reads.append(meta_path)
            return original_read(meta_path)

        monkeypatch.setattr(transformer, "_read_sidecar_timestamp", spy_sidecar)
        transformer.run(TARGET, reingest=True)

        assert reads == [body.with_suffix(".meta.json")]
        assert transformer.last_unvouched_bronze == frozenset()

    def test_a_file_landing_after_the_scan_is_in_neither_derivation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T2-h: the TOCTOU residual is consistent, on BOTH axes."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        early = _write_body(
            partition, "raw_0900_a.json", {"operationalData": [_flow("EARLY", TARGET)]}
        )
        _write_sidecar(early, STAMP_A)

        transformer = _flows_transformer(tmp_path)
        original = transformer._bronze_candidates

        def racing_candidates(target_date: date) -> list[Path]:
            paths = original(target_date)
            late = _write_body(
                partition, "raw_1000_b.json", {"operationalData": [_flow("LATE", TARGET)]}
            )
            _write_sidecar(late, STAMP_B)
            return paths

        monkeypatch.setattr(transformer, "_bronze_candidates", racing_candidates)
        assert transformer.run(TARGET, reingest=True) == 1

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert written["point_key"].to_list() == ["EARLY"]
        assert set(written["available_at"].to_list()) == {STAMP_A}


# --------------------------------------------------------------------------- #
# Exclusion on BOTH axes: rows AND stamp (I-2, I-4)
# --------------------------------------------------------------------------- #


class TestUnvouchedExclusion:
    def test_unvouched_file_contributes_neither_rows_nor_stamp(self, tmp_path: Path) -> None:
        """T2-b / T2-i: the two-axis assertion the prior fix passed on one axis only."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        vouched = _write_body(
            partition, "raw_0900_a.json", {"operationalData": [_flow("VOUCHED", TARGET)]}
        )
        _write_sidecar(vouched, STAMP_A)
        _write_body(partition, "raw_1000_b.json", {"operationalData": [_flow("ORPHAN", TARGET)]})

        transformer = _flows_transformer(tmp_path)
        assert transformer.run(TARGET, reingest=True) == 1

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert "ORPHAN" not in written["point_key"].to_list()
        assert set(written["available_at"].to_list()) == {STAMP_A}
        assert len(transformer.last_unvouched_bronze) == 1
        assert transformer.last_unvouched_total_exclusion is False

    def test_stamp_is_never_borrowed_from_an_excluded_sibling(self, tmp_path: Path) -> None:
        """T2-c: unvouched NEWEST + vouched older -> the OLDER file's own stamp."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        older = _write_body(
            partition, "raw_0900_a.json", {"operationalData": [_flow("OLDER", TARGET)]}
        )
        _write_sidecar(older, STAMP_A)
        _write_body(partition, "raw_1000_b.json", {"operationalData": [_flow("NEWER", TARGET)]})

        transformer = _flows_transformer(tmp_path)
        transformer.run(TARGET, reingest=True)

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert written["point_key"].to_list() == ["OLDER"]
        assert set(written["available_at"].to_list()) == {STAMP_A}

    def test_reference_dataset_selects_the_next_newest_vouched_path(self, tmp_path: Path) -> None:
        """T2-d: assert the SELECTED PATH, so candidate-order drift fails loudly."""
        newest = _partition(tmp_path, REFERENCE_DATASET, date(2026, 5, 3))
        middle = _partition(tmp_path, REFERENCE_DATASET, date(2026, 5, 2))
        _write_body(newest, "raw_new.json", {"interconnections": [{"id": "NEWEST"}]})
        chosen = _write_body(middle, "raw_mid.json", {"interconnections": [{"id": "MIDDLE"}]})
        _write_sidecar(chosen, STAMP_A)

        transformer = _generic(tmp_path, REFERENCE_DATASET)
        transformer.run(TARGET, reingest=True)

        written = pl.read_parquet(
            tmp_path / "silver" / "entsog" / REFERENCE_DATASET / f"{REFERENCE_DATASET}.parquet"
        )
        assert written["id"].to_list() == ["MIDDLE"]
        assert set(written["available_at"].to_list()) == {STAMP_A}
        assert {p for p, _ in transformer.last_unvouched_bronze} == {newest / "raw_new.json"}

    def test_total_exclusion_yields_no_rows_and_no_now_stamp(self, tmp_path: Path) -> None:
        """T2-g (I-5, cases 16 and 22)."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_body(partition, "raw_1000_b.json", {"operationalData": [_flow("B", TARGET)]})

        transformer = _flows_transformer(tmp_path)
        assert transformer.run(TARGET, reingest=True) == 0
        assert transformer.last_unvouched_total_exclusion is True
        assert len(transformer.last_unvouched_bronze) == 2

        silver_dir = tmp_path / "silver" / "entsog" / "physical_flows"
        assert list(silver_dir.rglob("*.parquet")) == [] if silver_dir.exists() else True


# --------------------------------------------------------------------------- #
# Per-row attribution: the case no frame-level scalar survives (I-4)
# --------------------------------------------------------------------------- #


class TestPerRowVintageAttribution:
    def test_each_row_carries_its_own_files_stamp(self, tmp_path: Path) -> None:
        """TV-3: A's rows carry 09:15 and B's carry 10:30, per row.

        `max` over both would over-stamp A (hiding its rows from a
        point-in-time query at 10:00); `min` would leak B's rows to an
        `as_of` before they existed. Every rejected attempt fails this case.
        """
        partition = _partition(tmp_path, "physical_flows", TARGET)
        a = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(a, STAMP_A)
        b = _write_body(
            partition, "raw_1000_b.json", {"operationalData": [_flow("B", TARGET, hour=7)]}
        )
        _write_sidecar(b, STAMP_B)

        _flows_transformer(tmp_path).run(TARGET, reingest=True)

        written = _silver(tmp_path, "physical_flows", TARGET)
        by_point = dict(zip(written["point_key"], written["available_at"], strict=True))
        assert by_point == {"A": STAMP_A, "B": STAMP_B}

    def test_a_zero_contribution_file_does_not_drag_the_vintage_forward(
        self, tmp_path: Path
    ) -> None:
        """T2-o / TV-1: B vouches at 10:30 but contributes nothing."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        a = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(a, STAMP_A)
        # B's rows are all off-date -- vouched, read, filtered to nothing.
        b = _write_body(
            partition,
            "raw_1000_b.json",
            {"operationalData": [_flow("B", date(2026, 4, 30))]},
        )
        _write_sidecar(b, STAMP_B)

        _flows_transformer(tmp_path).run(TARGET, reingest=True)

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert written["point_key"].to_list() == ["A"]
        assert set(written["available_at"].to_list()) == {STAMP_A}

    def test_a_malformed_body_vouches_but_contributes_no_stamp(self, tmp_path: Path) -> None:
        """T2-o, second shape: the reader's own JSONDecodeError handler skips B."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        a = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(a, STAMP_A)
        b = _write_body(partition, "raw_1000_b.json", "{not json")
        _write_sidecar(b, STAMP_B)

        _flows_transformer(tmp_path).run(TARGET, reingest=True)

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert set(written["available_at"].to_list()) == {STAMP_A}

    def test_rows_dropped_inside_transform_take_their_stamp_with_them(self, tmp_path: Path) -> None:
        """TV-2: B's rows pass the read filter and are dropped by transform()."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        a = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(a, STAMP_A)
        bad_unit = _flow("B", TARGET, hour=7)
        bad_unit["unit"] = "mystery-unit"
        b = _write_body(partition, "raw_1000_b.json", {"operationalData": [bad_unit]})
        _write_sidecar(b, STAMP_B)

        _flows_transformer(tmp_path).run(TARGET, reingest=True)

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert written["point_key"].to_list() == ["A"]
        assert set(written["available_at"].to_list()) == {STAMP_A}

    def test_cross_file_duplicates_still_collapse_to_the_later_stamp(self, tmp_path: Path) -> None:
        """TV-5: fails loudly the moment the dedup exclusion is lost.

        The record deliberately carries NO ``id``, so ``transform()`` takes the
        ALL-COLUMNS dedup branch -- the only branch the exclusion affects. With
        an ``id`` present the explicit ``["id"]`` key is used and this test
        passes even with the exclusion removed, i.e. pins nothing (caught by a
        mutation check while writing it).
        """
        partition = _partition(tmp_path, GENERIC_DATASET, TARGET)
        record: dict[str, Any] = {
            "periodFrom": f"{TARGET.isoformat()}T06:00:00Z",
            "pointKey": "ITP-00001",
            "value": 42.0,
        }
        assert "id" not in record, "TV-5 must exercise the all-columns dedup subset"
        a = _write_body(partition, "raw_0900_a.json", {"nominations": [record]})
        _write_sidecar(a, STAMP_A)
        b = _write_body(partition, "raw_1000_b.json", {"nominations": [dict(record)]})
        _write_sidecar(b, STAMP_B)

        assert _generic(tmp_path).run(TARGET, reingest=True) == 1

        written = _silver(tmp_path, GENERIC_DATASET, TARGET)
        assert written.height == 1
        assert written["available_at"].to_list() == [STAMP_B]

    def test_the_transient_carrier_never_reaches_silver(self, tmp_path: Path) -> None:
        """TV-4: assert on the file read back from disk, not the in-memory frame."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        body = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(body, STAMP_A)

        transformer = _flows_transformer(tmp_path)
        transformer.write_silver_csv = True
        transformer.run(TARGET, reingest=True)

        written = _silver(tmp_path, "physical_flows", TARGET)
        assert _BRONZE_VINTAGE_COLUMN not in written.columns
        csv_path = tmp_path / "silver" / "entsog" / "physical_flows" / "physical_flows_20260501.csv"
        assert _BRONZE_VINTAGE_COLUMN not in csv_path.read_text().splitlines()[0]

    @pytest.mark.parametrize(
        "vendor_field",
        ["gf_bronze_vintage", "gfBronzeVintage", "gf-bronze-vintage"],
    )
    def test_a_vendor_field_normalising_onto_the_carrier_fails_loud(
        self, tmp_path: Path, vendor_field: str
    ) -> None:
        """TV-8: a guard that passes the literal and fails camelCase is the realistic bug."""
        partition = _partition(tmp_path, GENERIC_DATASET, TARGET)
        record = _nomination("x", TARGET)
        record[vendor_field] = "2020-01-01T00:00:00Z"
        body = _write_body(partition, "raw_0900_a.json", {"nominations": [record]})
        _write_sidecar(body, STAMP_A)

        with pytest.raises(ValueError, match=_BRONZE_VINTAGE_COLUMN):
            _generic(tmp_path).run(TARGET, reingest=True)

    def test_a_desynced_reader_raises_before_stamping(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TV-9: a reader returning rows for an UNVOUCHED path fails loud.

        Rows and stamps are generated from the same ``pairs`` structure by
        parallel comprehensions, so a pure length mismatch is structurally
        impossible; the length guard is belt-and-braces. The one REACHABLE
        desync is a reader returning records for a path the resolver never
        vouched -- which must raise rather than mis-stamp those rows with a
        sibling's timestamp.
        """
        partition = _partition(tmp_path, "physical_flows", TARGET)
        body = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(body, STAMP_A)
        smuggled = partition / "raw_1000_never_vouched.json"

        transformer = _flows_transformer(tmp_path)

        def desynced(paths, target_date):  # type: ignore[no-untyped-def]
            return ((smuggled, [_flow("SMUGGLED", target_date)]),)

        monkeypatch.setattr(transformer, "_read_bronze_records", desynced)

        with pytest.raises(ValueError, match="not in the vouched read set"):
            transformer.run(TARGET, reingest=True)


# --------------------------------------------------------------------------- #
# Idempotence + run-mode symmetry (I-3, D-1)
# --------------------------------------------------------------------------- #


class TestIdempotenceAndRunMode:
    def test_two_reingests_with_the_clock_advanced_agree(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T2-e / TV-10.

        Compares PROVENANCE-relevant columns with a fixed run_id: `ingested_at`
        and `source_run_id` are wall-clock stamped on every run, so asserting
        byte-identical Parquet would be permanently red. Freezing the clock
        wholesale would neuter the very independence this proves.
        """
        partition = _partition(tmp_path, "physical_flows", TARGET)
        body = _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]})
        _write_sidecar(body, STAMP_A)

        transformer = _flows_transformer(tmp_path)
        compared = ["point_key", "flow_gwh_per_day", "available_at", "event_time"]

        transformer.run(TARGET, run_id="fixed", reingest=True)
        first = _silver(tmp_path, "physical_flows", TARGET).select(compared)

        real_now = datetime.now
        monkeypatch.setattr(
            "gridflow.silver.entsog.physical_flows.datetime",
            type(
                "Clock",
                (),
                {
                    "now": staticmethod(
                        lambda tz=None: real_now(tz) + __import__("datetime").timedelta(days=3)
                    )
                },
            ),
        )
        transformer.run(TARGET, run_id="fixed", reingest=True)
        second = _silver(tmp_path, "physical_flows", TARGET).select(compared)

        assert first.equals(second)

    def test_the_read_set_is_identical_across_run_modes(self, tmp_path: Path) -> None:
        """T2-f (D-1): reingest chooses only the CLOCK, never which rows are read."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        vouched = _write_body(
            partition, "raw_0900_a.json", {"operationalData": [_flow("VOUCHED", TARGET)]}
        )
        _write_sidecar(vouched, STAMP_A)
        _write_body(partition, "raw_1000_b.json", {"operationalData": [_flow("ORPHAN", TARGET)]})

        transformer = _flows_transformer(tmp_path)
        transformer.run(TARGET, reingest=True)
        reingest_rows = _silver(tmp_path, "physical_flows", TARGET)["point_key"].to_list()

        transformer.run(TARGET, reingest=False)
        live = _silver(tmp_path, "physical_flows", TARGET)

        assert live["point_key"].to_list() == reingest_rows == ["VOUCHED"]
        # A live run stamps one wall-clock instant, so the column is constant.
        assert len(set(live["available_at"].to_list())) == 1
        assert set(live["available_at"].to_list()) != {STAMP_A}


# --------------------------------------------------------------------------- #
# Falling through to master's common tail (cases 15, 18)
# --------------------------------------------------------------------------- #


class TestCommonTailPreserved:
    def test_empty_partition_still_emits_masters_no_bronze_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T2-k, first shape: no candidates at all."""
        transformer = _flows_transformer(tmp_path)
        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert transformer.run(TARGET, reingest=True) == 0

        assert transformer.last_unvouched_total_exclusion is False
        assert "No bronze data" in caplog.text

    def test_vouched_bronze_with_zero_surviving_rows_also_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T2-k / T2-q, second shape: master sets saw_bronze only on a non-empty frame.

        Asserting the warning is SUPPRESSED here would pin the silent
        zero-row path instead of forbidding it.
        """
        partition = _partition(tmp_path, "physical_flows", TARGET)
        body = _write_body(
            partition,
            "raw_0900_a.json",
            {"operationalData": [_flow("OFFDATE", date(2026, 4, 30))]},
        )
        _write_sidecar(body, STAMP_A)

        transformer = _flows_transformer(tmp_path)
        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert transformer.run(TARGET, reingest=True) == 0

        assert transformer.last_unvouched_total_exclusion is False, (
            "a post-filter empty frame is not a vouching failure"
        )
        assert "No bronze data" in caplog.text

    def test_a_malformed_only_partition_does_not_raise_or_flag_exclusion(
        self, tmp_path: Path
    ) -> None:
        """T2-q, malformed-body shape, for both run modes."""
        partition = _partition(tmp_path, GENERIC_DATASET, TARGET)
        body = _write_body(partition, "raw_0900_a.json", "{not json")
        _write_sidecar(body, STAMP_A)

        transformer = _generic(tmp_path)
        assert transformer.run(TARGET, reingest=True) == 0
        assert transformer.last_unvouched_total_exclusion is False
        assert transformer.run(TARGET, reingest=False) == 0
        assert transformer.last_unvouched_total_exclusion is False

    def test_date_window_filter_emptying_the_frame_is_not_a_vouching_failure(
        self, tmp_path: Path
    ) -> None:
        """T2-l (case 18), generic family."""
        partition = _partition(tmp_path, GENERIC_DATASET, TARGET)
        body = _write_body(
            partition,
            "raw_0900_a.json",
            {"nominations": [_nomination("off", date(2026, 4, 20))]},
        )
        _write_sidecar(body, STAMP_A)

        transformer = _generic(tmp_path)
        assert transformer.run(TARGET, reingest=True) == 0
        assert transformer.last_unvouched_total_exclusion is False


# --------------------------------------------------------------------------- #
# The row-level date filter must survive the relocation (S-1, S-17)
# --------------------------------------------------------------------------- #


class TestRowLevelDateFilterSurvives:
    def test_physical_flows_excludes_off_date_rows_on_the_lockstep_path(
        self, tmp_path: Path
    ) -> None:
        """T2-n: a lockstep branch bypassing read_bronze() would write off-date rows."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        body = _write_body(
            partition,
            "raw_0900_a.json",
            {
                "operationalData": [
                    _flow("ONDATE", TARGET),
                    _flow("OFFDATE", date(2026, 4, 30)),
                ]
            },
        )
        _write_sidecar(body, STAMP_A)

        assert _flows_transformer(tmp_path).run(TARGET, reingest=True) == 1
        written = _silver(tmp_path, "physical_flows", TARGET)
        assert written["point_key"].to_list() == ["ONDATE"]

    def test_generic_family_excludes_off_date_rows_on_the_lockstep_path(
        self, tmp_path: Path
    ) -> None:
        """T2-n, generic half."""
        partition = _partition(tmp_path, GENERIC_DATASET, TARGET)
        body = _write_body(
            partition,
            "raw_0900_a.json",
            {
                "nominations": [
                    _nomination("on", TARGET),
                    _nomination("off", date(2026, 4, 20)),
                ]
            },
        )
        _write_sidecar(body, STAMP_A)

        assert _generic(tmp_path).run(TARGET, reingest=True) == 1
        assert _silver(tmp_path, GENERIC_DATASET, TARGET)["id"].to_list() == ["on"]

    def test_undated_records_across_three_files_emit_exactly_one_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T2-r: the per-file relocation must not become per-file spam."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        for index in range(3):
            body = _write_body(
                partition,
                f"raw_{index}.json",
                {"operationalData": [{"indicator": "Physical Flow", "pointKey": f"P{index}"}]},
            )
            _write_sidecar(body, STAMP_A)

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.entsog.datetime"):
            _flows_transformer(tmp_path).run(TARGET, reingest=True)

        undated = [r for r in caplog.records if "no parseable date" in r.getMessage()]
        assert len(undated) == 1, f"expected ONE aggregated warning, got {len(undated)}"
        assert "3 record(s)" in undated[0].getMessage()

    def test_the_filter_wrapper_still_emits_its_single_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T2-r companion: existing callers of the logging wrapper are unchanged."""
        from gridflow.silver.entsog.datetime import filter_records_to_target_date

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.entsog.datetime"):
            kept = filter_records_to_target_date(
                [{"pointKey": "A"}, {"pointKey": "B"}],
                TARGET,
                ("periodFrom",),
                source="entsog",
                dataset="physical_flows",
            )

        assert len(kept) == 2
        undated = [r for r in caplog.records if "no parseable date" in r.getMessage()]
        assert len(undated) == 1
        assert "2 record(s)" in undated[0].getMessage()


# --------------------------------------------------------------------------- #
# read_bronze()'s own contract (T2-p) and the candidate-level sidecar filter
# --------------------------------------------------------------------------- #


class TestReadBronzeContract:
    def test_reference_read_bronze_returns_the_newest_body_only(self, tmp_path: Path) -> None:
        """T2-p, first axis: forwarding the untruncated list would merge every snapshot."""
        newest = _partition(tmp_path, REFERENCE_DATASET, date(2026, 5, 3))
        older = _partition(tmp_path, REFERENCE_DATASET, date(2026, 5, 2))
        _write_body(newest, "raw_a.json", {"interconnections": [{"id": "NEWEST"}]})
        _write_body(older, "raw_a.json", {"interconnections": [{"id": "OLDER"}]})

        frame = _generic(tmp_path, REFERENCE_DATASET).read_bronze(TARGET)

        assert frame["id"].to_list() == ["NEWEST"]

    def test_read_bronze_does_not_vouch(self, tmp_path: Path) -> None:
        """T2-p, second axis (S-25).

        Vouching belongs only to the lockstep branch, the only path that can
        COUNT and REPORT an exclusion. A read_bronze that quietly excluded
        would produce missing rows under a `success` status.
        """
        partition = _partition(tmp_path, "physical_flows", TARGET)
        _write_body(partition, "raw_0900_a.json", {"operationalData": [_flow("ORPHAN", TARGET)]})

        frame = _flows_transformer(tmp_path).read_bronze(TARGET)

        assert frame["pointKey"].to_list() == ["ORPHAN"]

    def test_sidecars_are_never_candidates(self, tmp_path: Path) -> None:
        """T2-s (G-2), BOTH families.

        `raw_*.json` matches sidecars too. If the exclusion were lost, every
        sidecar would become an unvouched candidate and the hard-fail rung
        would fire on every healthy run for every entsog dataset.
        """
        flows_partition = _partition(tmp_path, "physical_flows", TARGET)
        body = _write_body(
            flows_partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]}
        )
        _write_sidecar(body, STAMP_A)
        assert _flows_transformer(tmp_path)._bronze_candidates(TARGET) == [body]

        generic_partition = _partition(tmp_path, GENERIC_DATASET, TARGET)
        generic_body = _write_body(
            generic_partition, "raw_0900_a.json", {"nominations": [_nomination("x", TARGET)]}
        )
        _write_sidecar(generic_body, STAMP_A)
        assert _generic(tmp_path)._bronze_candidates(TARGET) == [generic_body]

        reference = _generic(tmp_path, REFERENCE_DATASET)
        ref_partition = _partition(tmp_path, REFERENCE_DATASET, TARGET)
        ref_body = _write_body(ref_partition, "raw_r.json", {"interconnections": [{"id": "x"}]})
        _write_sidecar(ref_body, STAMP_A)
        assert reference._bronze_candidates(TARGET) == [ref_body]


# --------------------------------------------------------------------------- #
# Emission discipline and the I-5 AST pin
# --------------------------------------------------------------------------- #


class TestEmissionAndClockDiscipline:
    def test_run_emits_nothing_above_debug_about_vouching(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T2-j: aggregation belongs to run_transform, not to run()."""
        partition = _partition(tmp_path, "physical_flows", TARGET)
        vouched = _write_body(
            partition, "raw_0900_a.json", {"operationalData": [_flow("A", TARGET)]}
        )
        _write_sidecar(vouched, STAMP_A)
        _write_body(partition, "raw_1000_b.json", {"operationalData": [_flow("B", TARGET)]})

        with caplog.at_level(logging.DEBUG, logger="gridflow.silver.base"):
            _flows_transformer(tmp_path).run(TARGET, reingest=True)

        above_debug = [
            r
            for r in caplog.records
            if r.levelno > logging.DEBUG and ("vouch" in r.getMessage().lower())
        ]
        assert above_debug == []

    def test_the_reingest_stamp_arm_contains_no_clock_call(self) -> None:
        """T2-m (I-5), AST pin.

        `live_now` is computed only under `not reingest`, which is what makes
        this expressible without the self-contradiction that killed the
        original whole-file grep gate. Asserts the locator found its target
        BEFORE asserting anything about it -- a matcher that silently matched
        nothing would be worse than no test.
        """
        import gridflow.silver.base as base_module

        tree = ast.parse(Path(base_module.__file__).read_text())
        run_fn = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "run"
        )
        conditionals = [
            node
            for node in ast.walk(run_fn)
            if isinstance(node, ast.IfExp)
            and any(
                isinstance(sub, ast.Subscript)
                and isinstance(sub.value, ast.Name)
                and sub.value.id == "stamp_by_path"
                for sub in ast.walk(node)
            )
        ]
        assert len(conditionals) == 1, (
            "could not locate the lockstep stamp expression -- this pin matched "
            "nothing and must be repaired, not deleted"
        )
        reingest_arm = conditionals[0].body
        clock_calls = [
            node
            for node in ast.walk(reingest_arm)
            if isinstance(node, ast.Attribute) and node.attr == "now"
        ]
        assert clock_calls == [], (
            "the reingest arm must be textually free of any clock call: a now() "
            "vintage on a reingest run is a fabricated vintage"
        )
