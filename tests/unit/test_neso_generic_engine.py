"""The NESO generic engine end to end over synthetic families (ADR-034 P-6, P-7).

Every test installs a tmp registry with one or two recorded families through
P-15's seam (``install_generated``) and writes synthetic captures in unit A's
sidecar shape under a short tmp data root; nothing touches ``C:/gridflow-data``.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest
from _neso_generic_support import (
    assert_same_output,
    frame_from,
    install_generated,
    snapshot,
    write_capture,
)
from _neso_registry_support import (
    column,
    epoch,
    family,
    ingest_context,
    package,
    record,
    resource,
    sp_columns,
)

from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal import completion as completion_module
from gridflow.silver.neso_data_portal import readers
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    is_valid,
    read_completion,
    read_failure,
    scan_completions,
)
from gridflow.silver.neso_data_portal.generic import OutputCollisionError
from gridflow.silver.neso_data_portal.readers import ChildTable

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

PKG = "dddddddd-0000-4000-8000-000000000000"
R1 = "dddddddd-0000-4000-8000-000000000001"
R2 = "dddddddd-0000-4000-8000-000000000002"
DAY = date(2026, 10, 7)
HEADER = b"SettlementDate,SettlementPeriod,Unit,Value\n"
BODY = HEADER + b"2026-10-07,1,A,1.5\n2026-10-07,2,A,2.5\n"


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A short data root: output names are long, Windows paths are not."""
    return tmp_path_factory.mktemp("g")


def _install(
    monkeypatch: pytest.MonkeyPatch,
    data: Path,
    rec: dict[str, Any],
    *,
    empty_allowed: bool = False,
    extra_families: list[dict[str, Any]] | None = None,
    resources: list[dict[str, Any]] | None = None,
) -> Any:
    families = [family("gen", record=rec, empty_allowed=empty_allowed), *(extra_families or [])]
    document = package("pkg-gen", PKG, families, resources or [resource(R1, "Series", "gen")])
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    return generated


def _capture(
    data: Path,
    body: bytes,
    written: datetime,
    *,
    key: str = "gen",
    resource_id: str = R1,
    name: str = "Series",
    lm: str | None = None,
    **kwargs: Any,
) -> str:
    path, _sidecar = write_capture(
        data,
        key,
        package_slug="pkg-gen",
        package_id=PKG,
        resource_id=resource_id,
        resource_name=name,
        body=body,
        written_at=written,
        ckan_last_modified=lm if lm is not None else written.replace(tzinfo=None).isoformat(),
        partition=DAY,
        **kwargs,
    )
    return path.relative_to(data).as_posix()


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


def _outputs(data: Path, key: str = "gen") -> list[Path]:
    return sorted((data / "silver" / "neso_data_portal" / key).rglob("[!.]*.parquet"))


def _ledger(data: Path, key: str = "gen") -> pl.DataFrame:
    return scan_completions(data, key).collect().sort("capture_written_at")


class TestOneCaptureOneOutput:
    def test_a_capture_writes_one_output_and_one_completion(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the engine writing anything but one output + one record per pair."""
        generated = _install(monkeypatch, data, record())
        capture_id = _capture(data, BODY, _t(12))
        rows = generated.transformers["gen"](data).run(DAY, run_id="r1")
        assert rows == 2
        (output,) = _outputs(data)
        frame = pl.read_parquet(output, hive_partitioning=False)
        assert frame["bronze_capture_id"].unique().to_list() == [capture_id]
        assert frame["dataset_version"].unique().to_list() == ["1+e1"]
        assert frame["available_at"].to_list() == [_t(12)] * 2  # ckan_last_modified, as UTC
        ledger = read_completion(data, "gen", capture_id)
        assert ledger is not None
        assert ledger["outcome"] == "populated" and ledger["row_count"] == 2
        assert ledger["output_path"] == output.relative_to(data).as_posix()

    def test_t_b4_5_a_repeat_is_a_no_op(self, data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """T-B4-5 / B6: skip-if-valid never rewrites a valid output (mtimes unchanged)."""
        generated = _install(monkeypatch, data, record())
        _capture(data, BODY, _t(12))
        transformer = generated.transformers["gen"](data)
        transformer.run(DAY, run_id="r1")
        before = snapshot(data, "gen")
        mtimes = {p: p.stat().st_mtime_ns for p in data.rglob("*.parquet")}
        assert transformer.run(DAY, run_id="r2") == 0
        assert {p: p.stat().st_mtime_ns for p in data.rglob("*.parquet")} == mtimes
        assert_same_output(
            before, snapshot(data, "gen"), ("settlement_date", "settlement_period", "unit")
        )

    def test_a_record_version_bump_re_transforms(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-9: older completions become invalid and the capture is rewritten."""
        generated = _install(monkeypatch, data, record())
        capture_id = _capture(data, BODY, _t(12))
        generated.transformers["gen"](data).run(DAY, run_id="r1")
        bumped = _install(monkeypatch, data / "b", record(version="2"))
        assert bumped.transformers["gen"](data).run(DAY, run_id="r2") == 2
        ledger = read_completion(data, "gen", capture_id)
        assert ledger is not None and ledger["record_version"] == "2"
        frame = pl.read_parquet(_outputs(data)[0], hive_partitioning=False)
        assert frame["dataset_version"].unique().to_list() == ["2+e1"]


class TestRunIdentity:
    def test_t_b7_2_one_adhoc_run_id_across_captures_typed_varchar(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B7-2: a direct run() resolves ONE non-null run id for every capture."""
        generated = _install(monkeypatch, data, record())
        _capture(data, BODY, _t(12))
        _capture(data, BODY.replace(b"1.5", b"9.5"), _t(13))
        generated.transformers["gen"](data).run(DAY)
        ids: set[str] = set()
        for output in _outputs(data):
            frame = pl.read_parquet(output, hive_partitioning=False)
            assert frame.schema["source_run_id"] == pl.Utf8
            ids |= set(frame["source_run_id"].to_list())
        assert len(ids) == 1 and next(iter(ids)).startswith("adhoc-")
        glob = (data / "silver" / "neso_data_portal" / "gen" / "**" / "*.parquet").as_posix()
        con = duckdb.connect(":memory:")
        try:
            described = dict(
                (row[0], row[1])
                for row in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=true, "
                    "union_by_name=true)"
                ).fetchall()
            )
        finally:
            con.close()
        assert described["source_run_id"] == "VARCHAR"


class TestCaptureFailures:
    def test_t_b1_7_a_bad_value_fails_only_its_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B1-7: an undeclared bad value fails its capture (no output, no
        completion, a failure record), the dataset reports failed, and the
        date's other capture is still written."""
        _install(monkeypatch, data, record())
        bad = _capture(data, HEADER + b"2026-10-07,1,A,oops\n", _t(12))
        good = _capture(data, BODY, _t(13))
        with ingest_context(data, monkeypatch) as ctx:
            (result,) = pipeline_runner.run_transform(
                ctx, "neso_data_portal", ["gen"], _t(0), _t(0)
            )
        assert result.status == "failed"
        assert bad in (result.error or "")
        assert read_completion(data, "gen", bad) is None
        failure = read_failure(data, "gen", bad)
        assert failure is not None and failure["bronze_capture_id"] == bad
        assert read_completion(data, "gen", good) is not None
        assert len(_outputs(data)) == 1

    def test_t_b1_8_exclusions_move_the_counter_and_write_the_rest(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B1-8 (engine half): four excluded rows count into
        ``last_excluded_row_count`` and the ledger, the valid rows are written."""
        cols = [*sp_columns()[:3], column("Value", "value", "float64", nullable=False, max=100)]
        generated = _install(monkeypatch, data, record(epochs=[epoch(cols)]))
        body = HEADER + (
            b"2026-10-07,1,OK,1\n,2,NODATE,1\n2026-10-07,,NOPERIOD,1\n"
            b"2026-10-07,3,NOVALUE,\n2026-10-07,4,BIG,500\n"
        )
        capture_id = _capture(data, body, _t(12))
        transformer = generated.transformers["gen"](data)
        assert transformer.run(DAY, run_id="r") == 1
        assert transformer.last_excluded_row_count == 4
        ledger = read_completion(data, "gen", capture_id)
        assert ledger is not None and ledger["rows_excluded"] == 4

    def test_t_b1_10_a_duplicate_key_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        generated = _install(monkeypatch, data, record())
        _capture(data, HEADER + b"2026-10-07,1,A,1\n2026-10-07,1,A,2\n", _t(12))
        with pytest.raises(NesoCaptureFailedError, match="DuplicateEntityKeyError"):
            generated.transformers["gen"](data).run(DAY, run_id="r")
        assert _outputs(data) == []

    def test_t_b2_3_datastore_sidecar_under_ckan_last_modified_fails(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B2-3: decision 9 at the capture (V-8 checks the registry side)."""
        generated = _install(monkeypatch, data, record())
        _capture(data, BODY, _t(12), url_type="datastore")
        with pytest.raises(NesoCaptureFailedError, match="datastore"):
            generated.transformers["gen"](data).run(DAY, run_id="r")

    def test_t_b2_3_capture_fallback_uses_the_capture_instant(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B2-3: ``capture_fallback`` -> ``available_at = capture_written_at``,
        ``published_at`` null; a dump without ``last_modified`` is read."""
        generated = _install(monkeypatch, data, record(vintage="capture_fallback"))
        _capture(data, BODY, _t(12), lm="")
        generated.transformers["gen"](data).run(DAY, run_id="r")
        frame = pl.read_parquet(_outputs(data)[0], hive_partitioning=False)
        assert frame["published_at"].to_list() == [None, None]
        assert frame["available_at"].to_list() == [_t(12), _t(12)]


class TestEmptyCaptures:
    """T-B4-2 / T-B4-3: the valid-empty rule."""

    def test_t_b4_2_populated_empty_populated_gives_three_records(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        generated = _install(monkeypatch, data, record(), empty_allowed=True)
        first = _capture(data, BODY, _t(8))
        second = _capture(data, HEADER, _t(10), empty_capture=True)
        third = _capture(data, BODY, _t(12))
        assert first != third
        generated.transformers["gen"](data).run(DAY, run_id="r")
        ledger = _ledger(data)
        assert ledger["bronze_capture_id"].to_list() == [first, second, third]
        assert ledger["outcome"].to_list() == ["populated", "valid_empty", "populated"]
        assert ledger["output_path"].to_list()[1] is None
        assert len(_outputs(data)) == 2

    @pytest.mark.parametrize(
        ("label", "body", "marker", "allowed", "match"),
        [
            ("no marker", HEADER, False, True, "without the empty_capture marker"),
            ("legacy sidecar", HEADER, None, True, "without the empty_capture marker"),
            ("family forbids", HEADER, True, False, "does not allow empty"),
            ("marker on rows", BODY, True, True, "marker is set on a body with rows"),
            ("all excluded", HEADER + b",1,A,1\n", True, True, "every row of capture"),
        ],
    )
    def test_t_b4_3_only_a_marked_header_only_body_is_valid_empty(
        self,
        data: Path,
        monkeypatch: pytest.MonkeyPatch,
        label: str,
        body: bytes,
        marker: bool | None,
        allowed: bool,
        match: str,
    ) -> None:
        """Detects a fabricated empty: every other shape fails and is never
        recorded valid-empty (an all-excluded marked body fails with its tally)."""
        generated = _install(monkeypatch, data, record(), empty_allowed=allowed)
        capture_id = _capture(data, body, _t(12), empty_capture=marker)
        with pytest.raises(NesoCaptureFailedError, match=match):
            generated.transformers["gen"](data).run(DAY, run_id="r")
        assert read_completion(data, "gen", capture_id) is None


class TestSiblingRead:
    def test_t_b1_11_a_sibling_capture_is_transformed_by_its_owner(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B1-11: bronze under S with resource SILVER(F) is F's; S's other
        resources are ignored; the capture id stays under ``S/``."""
        resources = [
            resource(R1, "Series", "gen"),
            resource(R2, "Box", "gen_box", disposition={"kind": "SILVER", "key": "gen"}),
            resource(
                "dddddddd-0000-4000-8000-000000000003",
                "Notes",
                "gen_box",
                fmt="PDF",
            ),
        ]
        generated = _install(
            monkeypatch,
            data,
            record(siblings=("gen_box",)),
            extra_families=[family("gen_box")],
            resources=resources,
        )
        sibling = _capture(data, BODY, _t(12), key="gen_box", resource_id=R2, name="Box")
        write_capture(
            data,
            "gen_box",
            package_slug="pkg-gen",
            package_id=PKG,
            resource_id="dddddddd-0000-4000-8000-000000000003",
            resource_name="Notes",
            body=b"%PDF-1.4",
            written_at=_t(12, 5),
            partition=DAY,
            ckan_format="PDF",
            extension="pdf",
        )
        assert generated.transformers["gen"](data).run(DAY, run_id="r") == 2
        (output,) = _outputs(data)
        frame = pl.read_parquet(output, hive_partitioning=False)
        assert frame["bronze_capture_id"].unique().to_list() == [sibling]
        assert sibling.startswith("bronze/neso_data_portal/gen_box/")


def _stub_reader(tables: dict[str, list[ChildTable]]) -> Any:
    def _read(path: Path, rec: Any, children: tuple[str, ...]) -> Iterator[ChildTable]:
        for table in tables[path.name]:
            if table.child_id in children:
                if table.frame.width == 0:
                    raise ValueError(f"child {table.child_id} is unreadable")
                yield table

    return _read


ROW_HEADER = ["RowId", "Value"]


def _row_record(siblings: tuple[str, ...] = ()) -> dict[str, Any]:
    return record(
        reader="zip_member",
        epochs=[
            epoch([column("RowId", "row_id", "int64", nullable=False), column("Value", "value")])
        ],
        temporal={"kind": "none"},
        entity_key=("row_id",),
        siblings=siblings,
    )


class TestContainers:
    """T-B5-1..3: container accounting through a stub reader (X plugs in later)."""

    def _box(
        self,
        monkeypatch: pytest.MonkeyPatch,
        data: Path,
        children: list[tuple[str, str]],
        tables: list[ChildTable],
    ) -> tuple[Any, str]:
        inventory = [
            {"child": child, "disposition": {"kind": "SILVER", "key": key}}
            for child, key in children
        ]
        # The container belongs to a record-free family both owners list as a
        # sibling (V-10 single owner + V-11).
        resources = [resource(R1, "Box", "gen_box", fmt="ZIP", children=inventory)]
        families = [
            family("gen_two", record=_row_record(("gen_box",))),
            family("gen_box", kind="files"),
        ]
        generated = _install(
            monkeypatch,
            data,
            _row_record(("gen_box",)),
            extra_families=families,
            resources=resources,
        )
        capture_id = _capture(
            data,
            b"PK-not-really",
            _t(12),
            key="gen_box",
            name="Box",
            ckan_format="ZIP",
            extension="zip",
        )
        name = capture_id.rsplit("/", 1)[1]
        monkeypatch.setitem(readers.READERS, "zip_member", _stub_reader({name: tables}))
        return generated, capture_id

    def test_t_b5_1_two_children_of_one_family(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        tables = [
            ChildTable("a.csv", tuple(ROW_HEADER), frame_from(ROW_HEADER, [["1", "x"]])),
            ChildTable("b.csv", tuple(ROW_HEADER), frame_from(ROW_HEADER, [["2", "y"]])),
        ]
        generated, capture_id = self._box(
            monkeypatch, data, [("a.csv", "gen"), ("b.csv", "gen")], tables
        )
        generated.transformers["gen"](data).run(DAY, run_id="r")
        (output,) = _outputs(data)
        frame = pl.read_parquet(output, hive_partitioning=False).sort("row_id")
        assert frame["child_id"].to_list() == ["a.csv", "b.csv"]
        ledger = read_completion(data, "gen", capture_id)
        assert ledger is not None and sorted(ledger["children"]) == ["a.csv", "b.csv"]

    def test_t_b5_2_and_3_children_of_two_families_complete_independently(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B5-2, then T-B5-3: one failing child of F1 leaves F1 with a failure
        record and no output, while F2 completes."""
        tables = [
            ChildTable("a.csv", tuple(ROW_HEADER), frame_from(ROW_HEADER, [["1", "x"]])),
            ChildTable("b.csv", tuple(ROW_HEADER), frame_from(ROW_HEADER, [["2", "y"]])),
        ]
        generated, capture_id = self._box(
            monkeypatch, data, [("a.csv", "gen"), ("b.csv", "gen_two")], tables
        )
        generated.transformers["gen"](data).run(DAY, run_id="r")
        generated.transformers["gen_two"](data).run(DAY, run_id="r")
        assert len(_outputs(data, "gen")) == 1 and len(_outputs(data, "gen_two")) == 1
        assert read_completion(data, "gen_two", capture_id) is not None

        broken = [
            ChildTable("a.csv", (), pl.DataFrame()),
            ChildTable("b.csv", tuple(ROW_HEADER), frame_from(ROW_HEADER, [["2", "y"]])),
        ]
        fresh = data / "fresh"
        fresh.mkdir()
        generated, capture_id = self._box(
            monkeypatch, fresh, [("a.csv", "gen"), ("b.csv", "gen_two")], broken
        )
        with pytest.raises(NesoCaptureFailedError):
            generated.transformers["gen"](fresh).run(DAY, run_id="r")
        generated.transformers["gen_two"](fresh).run(DAY, run_id="r")
        assert _outputs(fresh, "gen") == []
        assert read_completion(fresh, "gen", capture_id) is None
        assert read_failure(fresh, "gen", capture_id) is not None
        assert read_completion(fresh, "gen_two", capture_id) is not None

    def test_a_reader_missing_a_requested_child_fails_the_pair(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        tables = [ChildTable("a.csv", tuple(ROW_HEADER), frame_from(ROW_HEADER, [["1", "x"]]))]
        generated, _capture_id = self._box(
            monkeypatch, data, [("a.csv", "gen"), ("b.csv", "gen")], tables
        )
        with pytest.raises(NesoCaptureFailedError, match="ContainerInventoryError"):
            generated.transformers["gen"](data).run(DAY, run_id="r")

    def test_t_b1_14_engine_counts_two_exclusions(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B1-14 (engine half): I-2 across two epochs moves the counter by 2."""
        epoch_a = [
            column("RowId", "row_id", "int64", nullable=False),
            column("Value", "value", "float64", max=100),
        ]
        epoch_b = [
            column("RowId", "row_id", "int64", nullable=False),
            column("Value", "value", "float64", nullable=False, max=50),
            column("Extra", "extra", nullable=False),
        ]
        rec = record(
            reader="zip_member",
            epochs=[epoch(epoch_a), epoch(epoch_b)],
            temporal={"kind": "none"},
            entity_key=("row_id",),
        )
        inventory = [
            {"child": c, "disposition": {"kind": "SILVER", "key": "gen"}} for c in ("a", "b")
        ]
        generated = _install(
            monkeypatch,
            data,
            rec,
            resources=[resource(R1, "Box", "gen", fmt="ZIP", children=inventory)],
        )
        capture_id = _capture(data, b"PK", _t(12), name="Box", ckan_format="ZIP", extension="zip")
        tables = [
            ChildTable(
                "a", ("RowId", "Value"), frame_from(["RowId", "Value"], [["1", None], ["2", "75"]])
            ),
            ChildTable(
                "b",
                ("RowId", "Value", "Extra"),
                frame_from(
                    ["RowId", "Value", "Extra"],
                    [["3", "40", "x"], ["4", "60", "y"], ["5", "40", None]],
                ),
            ),
        ]
        monkeypatch.setitem(
            readers.READERS, "zip_member", _stub_reader({capture_id.rsplit("/", 1)[1]: tables})
        )
        transformer = generated.transformers["gen"](data)
        assert transformer.run(DAY, run_id="r") == 3
        assert transformer.last_excluded_row_count == 2

    def test_an_unregistered_container_reader_fails_loudly(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Until unit X registers it, a zip_member record fails each capture."""
        inventory = [{"child": "a", "disposition": {"kind": "SILVER", "key": "gen"}}]
        generated = _install(
            monkeypatch,
            data,
            _row_record(),
            resources=[resource(R1, "Box", "gen", fmt="ZIP", children=inventory)],
        )
        monkeypatch.delitem(readers.READERS, "zip_member", raising=False)
        _capture(data, b"PK", _t(12), name="Box", ckan_format="ZIP", extension="zip")
        with pytest.raises(NesoCaptureFailedError, match="ReaderUnavailableError"):
            generated.transformers["gen"](data).run(DAY, run_id="r")


class TestLedgerWriteOrder:
    def test_t_b5_4_a_planted_foreign_file_is_never_replaced(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B5-4: P-6's collision guard raises before ``write_parquet``."""
        generated = _install(monkeypatch, data, record())
        capture_id = _capture(data, BODY, _t(12))
        transformer = generated.transformers["gen"](data)
        path = transformer.output_path(DAY, capture_id, _t(12))
        path.parent.mkdir(parents=True)
        pl.DataFrame({"bronze_capture_id": ["someone/else"]}).write_parquet(path)
        with pytest.raises(NesoCaptureFailedError, match="OutputCollisionError"):
            transformer.run(DAY, run_id="r")
        assert pl.read_parquet(path)["bronze_capture_id"].to_list() == ["someone/else"]
        assert (
            OutputCollisionError.__name__
            in (read_failure(data, "gen", capture_id) or {})["error_class"]
        )

    def test_t_b5_5_a_silver_wipe_invalidates_populated_not_valid_empty(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B5-5 (predicate half): after a silver-only wipe the populated pair
        fails ``is_valid`` and the valid-empty pair (ADR-029) stays valid."""
        generated = _install(monkeypatch, data, record(), empty_allowed=True)
        populated = _capture(data, BODY, _t(8))
        empty = _capture(data, HEADER, _t(10), empty_capture=True)
        cls = generated.transformers["gen"]
        cls(data).run(DAY, run_id="r")
        for output in _outputs(data):
            output.unlink()
        versions = cls.versions()
        assert not is_valid(read_completion(data, "gen", populated) or {}, data, versions)
        assert is_valid(read_completion(data, "gen", empty) or {}, data, versions)

    @pytest.mark.parametrize("fail_at", [1, 2])
    def test_t_b5_6_write_order_faults(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, fail_at: int
    ) -> None:
        """T-B5-6: output fails -> nothing visible + a failure record;
        completion fails -> the output exists with no record (FM-2)."""
        generated = _install(monkeypatch, data, record())
        capture_id = _capture(data, BODY, _t(12))
        from gridflow.silver.neso_data_portal import generic

        calls = {"n": 0}
        real = generic.write_parquet
        real_completion = completion_module.write_parquet

        def _counting(frame: pl.DataFrame, path: Path, *args: Any) -> Path:
            calls["n"] += 1
            if calls["n"] == fail_at:
                raise OSError("disk full")
            return real(frame, path)

        monkeypatch.setattr(generic, "write_parquet", _counting)
        monkeypatch.setattr(completion_module, "write_parquet", _counting)
        with pytest.raises(NesoCaptureFailedError, match="disk full"):
            generated.transformers["gen"](data).run(DAY, run_id="r")
        assert read_completion(data, "gen", capture_id) is None
        assert read_failure(data, "gen", capture_id) is not None
        assert len(_outputs(data)) == (0 if fail_at == 1 else 1)
        assert not any(".tmp_" in p.name for p in data.rglob("*"))
        monkeypatch.setattr(completion_module, "write_parquet", real_completion)

    def test_t_b5_6_unlink_failure_leaves_the_completion_valid(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B5-6 / FM-4: a crash after the completion but before the failure
        record's unlink leaves a VALID completion beside a stale failure record;
        the next run skips the pair (a valid completion wins)."""
        from pathlib import Path as _Path

        from gridflow.silver.neso_data_portal import generic

        generated = _install(monkeypatch, data, record())
        capture_id = _capture(data, BODY, _t(12))
        cls = generated.transformers["gen"]

        def _boom(*args: Any, **kwargs: Any) -> pl.DataFrame:
            raise RuntimeError("first attempt fails")

        with monkeypatch.context() as patch:
            patch.setattr(generic, "finish_capture", _boom)
            with pytest.raises(NesoCaptureFailedError):
                cls(data).run(DAY, run_id="r1")
        failure = completion_module.failure_path(data, "gen", capture_id)
        assert failure.is_file()

        real_unlink = _Path.unlink

        def _refuse(self: _Path, missing_ok: bool = False) -> None:
            if self == failure:
                raise OSError("unlink refused")
            real_unlink(self, missing_ok=missing_ok)

        with monkeypatch.context() as patch:
            patch.setattr(_Path, "unlink", _refuse)
            with pytest.raises(NesoCaptureFailedError, match="unlink refused"):
                cls(data).run(DAY, run_id="r2")
        ledger = read_completion(data, "gen", capture_id)
        assert ledger is not None and is_valid(ledger, data, cls.versions())
        assert failure.is_file()
        assert cls(data).run(DAY, run_id="r3") == 0


@pytest.fixture(autouse=True)
def _quiet(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR, logger="gridflow.connectors.neso_data_portal.captures")


class TestRunTypeKey:
    def test_t_b1_9_two_runs_of_one_pair_are_both_stored(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B1-9 (storage half): never dedup on (date, period) without run_type;
        the selection half is in ``test_neso_latest_selection.py``."""
        cols = [*sp_columns()[:2], column("Run", "run_type", nullable=False)]
        rec = record(
            epochs=[epoch(cols)],
            entity_key=("settlement_date", "settlement_period", "run_type"),
            run_type_column="run_type",
        )
        generated = _install(monkeypatch, data, rec)
        _capture(
            data,
            b"SettlementDate,SettlementPeriod,Run\n2026-10-07,1,SF\n2026-10-07,1,R1\n",
            _t(12),
        )
        assert generated.transformers["gen"](data).run(DAY, run_id="r") == 2
        spec = generated.specs[("neso_data_portal", "gen")]
        assert spec.rank_column == "run_type"
        assert spec.tiebreak_columns == ("capture_written_at", "bronze_capture_id", "run_type")
