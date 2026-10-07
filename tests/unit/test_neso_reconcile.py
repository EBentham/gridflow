"""Reconcile and drain over synthetic NESO families (ADR-034 P-14; T-B6, T-B7-1).

Every test runs on a short tmp data root with a tmp registry installed through
P-15's seam; the CLI is driven in process through ``main`` with
``GRIDFLOW_DATA_DIR`` / ``GRIDFLOW_DUCKDB_PATH`` pointed at that root, so no
test reads or writes ``C:/gridflow-data``.
"""

from __future__ import annotations

import shutil
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import duckdb
import pytest
from _neso_generic_support import assert_same_output, install_generated, snapshot, write_capture
from _neso_registry_support import family, package, record, resource

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.reconcile import main
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    completion_path,
    completion_row,
    read_completion,
    read_failure,
    record_completion,
)
from gridflow.silver.neso_data_portal.daily_wind_availability import (
    DailyWindAvailabilityTransformer,
)
from gridflow.silver.neso_data_portal.reconcile import drain, inventory_sha256, reconcile
from gridflow.storage.duckdb import init_catalogue, refresh_views

if TYPE_CHECKING:
    from pathlib import Path

SOURCE = "neso_data_portal"
PKG = "dddddddd-0000-4000-8000-000000000000"
DAY = date(2026, 10, 7)
D1 = date(2026, 10, 5)
D2 = date(2026, 10, 6)
CUTOFF = "2026-10-31"
SP = b"SettlementDate,SettlementPeriod,Unit,Value\n"
BODY = SP + b"2026-10-07,1,A,1.5\n2026-10-07,2,A,2.5\n"
KEY = ("settlement_date", "settlement_period", "unit")


def _rid(index: int) -> str:
    return f"dddddddd-0000-4000-8000-{index:012d}"


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A short data root the settings (and so the CLI) point at."""
    root = tmp_path_factory.mktemp("r")
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(root))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(root / "cat.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(root / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    pipeline_runner.import_transformers()
    return root


def _install(
    monkeypatch: pytest.MonkeyPatch,
    data: Path,
    families: dict[str, dict[str, Any]],
    *,
    extra: list[dict[str, Any]] | None = None,
    where: str = "_registry",
) -> Any:
    entries = [family(key, **kwargs) for key, kwargs in families.items()]
    resources = [resource(_rid(i), key.title(), key) for i, key in enumerate(families, start=1)]
    document = package("pkg-gen", PKG, entries, [*resources, *(extra or [])])
    return install_generated(monkeypatch, data / where, [document])


def _capture(
    data: Path,
    key: str,
    index: int,
    body: bytes,
    written: datetime,
    *,
    day: date = DAY,
    name: str | None = None,
    **kwargs: Any,
) -> str:
    path, _sidecar = write_capture(
        data,
        key,
        package_slug="pkg-gen",
        package_id=PKG,
        resource_id=_rid(index),
        resource_name=name if name is not None else key.title(),
        body=body,
        written_at=written,
        ckan_last_modified=written.replace(tzinfo=None).isoformat(),
        partition=day,
        **kwargs,
    )
    return path.relative_to(data).as_posix()


def _t(hour: int, minute: int = 0, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


def _cli(capsys: pytest.CaptureFixture[str], *args: str) -> tuple[int, list[str]]:
    code = main(list(args))
    return code, capsys.readouterr().out.splitlines()


def _gaps(lines: list[str]) -> list[str]:
    return [line for line in lines if line.startswith("GAP ")]


def _state(data: Path) -> dict[str, Any]:
    """Every ledger file's bytes and every output's mtime (a no-op detector)."""
    ledger = {
        p.relative_to(data).as_posix(): p.read_bytes() for p in (data / "state").rglob("*.parquet")
    }
    outputs = {
        p.relative_to(data).as_posix(): p.stat().st_mtime_ns
        for p in (data / "silver").rglob("*.parquet")
    }
    return {"ledger": ledger, "outputs": outputs}


def _generic(generated: Any, key: str = "fam_one") -> Any:
    return generated.transformers[key]


def _query(db: Path, sql: str) -> list[tuple[Any, ...]]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


class TestCli:
    def test_a_clean_family_exits_0_with_a_summary(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _registry, generated = _install(monkeypatch, data, {"fam_one": {"record": record()}})
        _capture(data, "fam_one", 1, BODY, _t(8))
        _generic(generated)(data).run(DAY, run_id="r")
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF)
        assert code == 0 and _gaps(lines) == []
        assert "SUMMARY families=1 skipped=0 gaps=0" in lines

    @pytest.mark.parametrize(
        "args",
        [
            ("--cutoff", CUTOFF),
            ("fam_one", "--all", "--cutoff", CUTOFF),
            ("nope", "--cutoff", CUTOFF),
            ("fam_one", "--cutoff", "07/10/2026"),
            ("fam_one",),
        ],
    )
    def test_usage_errors_exit_2(
        self,
        data: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        args: tuple[str, ...],
    ) -> None:
        _install(monkeypatch, data, {"fam_one": {"record": record()}})
        code, _lines = _cli(capsys, *args)
        assert code == 2

    def test_ingest_only_families_are_skipped_never_gaps(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _install(monkeypatch, data, {"fam_one": {"record": record()}, "fam_raw": {}})
        _capture(data, "fam_raw", 2, BODY, _t(8))
        code, lines = _cli(capsys, "--all", "--cutoff", CUTOFF)
        assert code == 0 and _gaps(lines) == []
        assert "SUMMARY skipped fam_raw (ingest-only: no frozen schema record)" in lines

    def test_t_b6_1_a_missing_and_an_orphaned_completion_each_report(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Detects a capture with no completion, or a completion with no
        capture, passing reconcile silently."""
        _registry, generated = _install(monkeypatch, data, {"fam_one": {"record": record()}})
        missing = _capture(data, "fam_one", 1, BODY, _t(8))
        ghost = "bronze/neso_data_portal/fam_one/2026/10/07/raw_ghost.csv"
        record_completion(
            data,
            completion_row(
                family="fam_one",
                capture_id=ghost,
                source_key="fam_one",
                partition_date=DAY,
                resource_id=_rid(1),
                body_sha256="0" * 64,
                capture_written_at=_t(7),
                published_at=_t(7),
                outcome="valid_empty",
                row_count=0,
                rows_excluded=0,
                output_path=None,
                children=[],
                versions=_generic(generated).versions(),
            ),
        )
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF)
        assert code == 1
        assert f"GAP missing fam_one 2026-10-07 {missing} no completion" in lines
        assert (
            f"GAP orphaned fam_one 2026-10-07 {ghost} a: completion without an expected capture"
            in (lines)
        )
        assert "SUMMARY missing 1" in lines and "SUMMARY orphaned 1" in lines

    def test_the_cutoff_is_inclusive_and_bounds_the_scan(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _install(monkeypatch, data, {"fam_one": {"record": record()}})
        _capture(data, "fam_one", 1, BODY, _t(8))
        assert _cli(capsys, "fam_one", "--cutoff", "2026-10-06")[0] == 0
        assert _cli(capsys, "fam_one", "--cutoff", "2026-10-07")[0] == 1


class TestDrain:
    def test_t_b6_2_a_repeat_drain_changes_nothing(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _install(monkeypatch, data, {"fam_one": {"record": record()}})
        _capture(data, "fam_one", 1, BODY, _t(8))
        _capture(data, "fam_one", 1, BODY.replace(b"1.5", b"3.5"), _t(9))
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 0 and _gaps(lines) == []
        assert "SUMMARY drained fam_one 2026-10-07 2" in lines
        before = _state(data)
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 0
        assert not [line for line in lines if line.startswith("SUMMARY drained")]
        assert _state(data) == before

    def test_t_b6_4_unusable_sidecars_and_orphaned_a_are_never_drained(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _registry, generated = _install(monkeypatch, data, {"fam_one": {"record": record()}})
        broken = _capture(data, "fam_one", 1, BODY, _t(8))
        sidecar = data / f"{broken}.meta.json"
        sidecar = sidecar.with_name(sidecar.name.replace(".csv.meta.json", ".meta.json"))
        assert sidecar.is_file(), sidecar
        sidecar.write_text("not json", encoding="utf-8")
        ghost = "bronze/neso_data_portal/fam_one/2026/10/07/raw_ghost.csv"
        record_completion(
            data,
            completion_row(
                family="fam_one",
                capture_id=ghost,
                source_key="fam_one",
                partition_date=DAY,
                resource_id=_rid(1),
                body_sha256="0" * 64,
                capture_written_at=_t(7),
                published_at=_t(7),
                outcome="valid_empty",
                row_count=0,
                rows_excluded=0,
                output_path=None,
                children=[],
                versions=_generic(generated).versions(),
            ),
        )
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 1
        assert not [line for line in lines if line.startswith("SUMMARY drained")]
        unusable = [
            line for line in _gaps(lines) if line.startswith("GAP failed fam_one 2026-10-07")
        ]
        assert len(unusable) == 1 and "unusable sidecar: sidecar unreadable" in unusable[0]
        assert any(line.startswith(f"GAP orphaned fam_one 2026-10-07 {ghost} a:") for line in lines)
        assert list((data / "silver").rglob("*.parquet")) == []

    def test_a_duplicated_capture_without_a_completion_is_never_drained(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Detects the drain dispatching a ``duplicated`` capture through its
        other gaps: with no completion it is also ``missing`` and orphaned (b),
        and the drain re-transformed and recorded it (REVIEW-DIFF-1 tests #1).
        P-14: the drain never touches ``duplicated``."""
        _registry, generated = _install(monkeypatch, data, {"fam_one": {"record": record()}})
        capture = _capture(data, "fam_one", 1, BODY, _t(8))
        _generic(generated)(data).run(DAY, run_id="r")
        (output,) = (data / "silver").rglob("*.parquet")
        shutil.copyfile(output, output.with_name(f"copy_{output.name}"))
        completion_path(data, "fam_one", capture).unlink()
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF)
        assert code == 1
        assert sorted(line.split()[1] for line in _gaps(lines)) == [
            "duplicated",
            "missing",
            "orphaned",
        ]
        before = _state(data)
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 1
        assert not [line for line in lines if line.startswith("SUMMARY drained")]
        assert _state(data) == before
        assert read_completion(data, "fam_one", capture) is None

    def test_t_b5_5_a_silver_wipe_reports_populated_not_valid_empty(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """T-B5-5's reconcile half and T-B4-6 (d): ADR-029:47-51's mixed ledger."""
        _registry, generated = _install(
            monkeypatch, data, {"fam_one": {"record": record(), "empty_allowed": True}}
        )
        populated = _capture(data, "fam_one", 1, BODY, _t(8))
        _capture(data, "fam_one", 1, SP, _t(10), empty_capture=True)
        _generic(generated)(data).run(DAY, run_id="r")
        shutil.rmtree(data / "silver")
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF)
        assert code == 1
        assert _gaps(lines) == [
            f"GAP missing_or_invalid_output fam_one 2026-10-07 {populated} "
            "completion fails the validity predicate (populated)"
        ]

    def test_t_b6_5_the_drain_restores_wiped_outputs_and_clears_orphaned_b(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _registry, generated = _install(
            monkeypatch, data, {"fam_one": {"record": record(), "empty_allowed": True}}
        )
        wiped = _capture(data, "fam_one", 1, BODY, _t(8))
        unrecorded = _capture(data, "fam_one", 1, BODY.replace(b"1.5", b"4.5"), _t(9))
        _capture(data, "fam_one", 1, SP, _t(10), empty_capture=True)
        _generic(generated)(data).run(DAY, run_id="r")
        before = snapshot(data, "fam_one")
        ledger = read_completion(data, "fam_one", wiped)
        assert ledger is not None
        (data / ledger["output_path"]).unlink()
        completion_path(data, "fam_one", unrecorded).unlink()
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF)
        assert code == 1
        categories = sorted(line.split()[1] for line in _gaps(lines))
        assert categories == ["missing", "missing_or_invalid_output", "orphaned"]
        assert any(
            line.startswith(f"GAP orphaned fam_one 2026-10-07 {unrecorded} b:") for line in lines
        )
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 0 and _gaps(lines) == []
        assert_same_output(before, snapshot(data, "fam_one"), KEY)

    def test_t_b6_6_a_state_wipe_then_drain_restores_whole_capture_latest(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """With the ledger gone, every pair is reported and whole-capture
        ``_latest`` serves nothing; the drain re-records populated pairs
        (equal rows) and valid-empty pairs, and ``_latest`` serves again."""
        _registry, generated = _install(
            monkeypatch,
            data,
            {"fam_one": {"record": record(latest="whole_capture"), "empty_allowed": True}},
        )
        _capture(data, "fam_one", 1, BODY, _t(8))
        _capture(data, "fam_one", 1, SP, _t(10), empty_capture=True)
        newest = _capture(data, "fam_one", 1, BODY.replace(b"1.5", b"6.5"), _t(12))
        _generic(generated)(data).run(DAY, run_id="r")
        before = snapshot(data, "fam_one")
        db = data / "cat.duckdb"
        latest = f'SELECT bronze_capture_id FROM "silver_{SOURCE}_fam_one_latest"'
        init_catalogue(db, data)
        assert [row[0] for row in _query(db, latest)] == [newest, newest]
        shutil.rmtree(data / "state")
        refresh_views(db, data)
        assert _query(db, latest) == []
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF)
        assert code == 1
        categories = sorted(line.split()[1] for line in _gaps(lines))
        assert categories == ["missing"] * 3 + ["orphaned"] * 2
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 0 and _gaps(lines) == []
        assert [row[0] for row in _query(db, latest)] == [newest, newest]
        assert_same_output(before, snapshot(data, "fam_one"), KEY)

    def test_t_b6_7_a_failed_replay_leaves_a_sibling_family_untouched(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Then a record-version bump makes the drain rewrite every capture."""
        families = {"fam_one": {"record": record()}, "fam_two": {"record": record()}}
        _registry, generated = _install(monkeypatch, data, families)
        bad = _capture(data, "fam_one", 1, SP + b"2026-10-07,1,A,oops\n", _t(8))
        _capture(data, "fam_two", 2, BODY, _t(8))
        with pytest.raises(NesoCaptureFailedError):
            _generic(generated, "fam_one")(data).run(DAY, run_id="r")
        _generic(generated, "fam_two")(data).run(DAY, run_id="r")
        before = _state(data)
        code, lines = _cli(capsys, "--all", "--cutoff", CUTOFF, "--drain")
        assert code == 1
        assert [line.split()[1:5] for line in _gaps(lines)] == [
            ["failed", "fam_one", "2026-10-07", bad]
        ]
        assert read_failure(data, "fam_one", bad) is not None
        fam_two = {k: v for k, v in _state(data)["outputs"].items() if "/fam_two/" in k}
        assert fam_two == {k: v for k, v in before["outputs"].items() if "/fam_two/" in k}
        assert {k: v for k, v in _state(data)["ledger"].items() if "/fam_two/" in k} == {
            k: v for k, v in before["ledger"].items() if "/fam_two/" in k
        }

        bumped = {"fam_one": {"record": record()}, "fam_two": {"record": record(version="2")}}
        _install(monkeypatch, data, bumped, where="_registry2")
        code, lines = _cli(capsys, "fam_two", "--cutoff", CUTOFF)
        assert code == 1 and [line.split()[1] for line in _gaps(lines)] == [
            "missing_or_invalid_output"
        ]
        code, lines = _cli(capsys, "fam_two", "--cutoff", CUTOFF, "--drain")
        assert code == 0
        after = {k: v for k, v in _state(data)["outputs"].items() if "/fam_two/" in k}
        assert after.keys() == fam_two.keys() and after != fam_two

    def test_t_b6_9_a_drain_after_an_empty_catalogue_serves_its_rows(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """FM-18: a catalogue built before the family's first populated capture
        holds a typed-empty base; the drain's one refresh makes it glob-backed."""
        _install(monkeypatch, data, {"fam_one": {"record": record()}})
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        assert _query(db, f'SELECT count(*) FROM "silver_{SOURCE}_fam_one"') == [(0,)]
        _capture(data, "fam_one", 1, BODY, _t(8))
        code, _lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 0
        assert _query(db, f'SELECT count(*) FROM "silver_{SOURCE}_fam_one"') == [(2,)]
        (definition,) = _query(
            db,
            "SELECT view_definition FROM information_schema.views "
            f"WHERE table_name = 'silver_{SOURCE}_fam_one'",
        )
        assert "read_parquet" in definition[0]

    def test_t_b6_10_a_failing_group_never_stops_the_next(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """FM-15: d1 holds a bad and a recoverable capture, d2 a recoverable
        capture of a family with no prior output."""
        families = {"fam_one": {"record": record()}, "fam_two": {"record": record()}}
        _install(monkeypatch, data, families)
        bad = _capture(data, "fam_one", 1, SP + b"2026-10-05,1,A,oops\n", _t(8, day=D1), day=D1)
        good = _capture(data, "fam_one", 1, SP + b"2026-10-05,1,A,1\n", _t(9, day=D1), day=D1)
        late = _capture(data, "fam_two", 2, SP + b"2026-10-06,1,A,1\n", _t(9, day=D2), day=D2)
        code, lines = _cli(capsys, "--all", "--cutoff", CUTOFF, "--drain")
        assert code == 1
        assert [line.split()[1:5] for line in _gaps(lines)] == [
            ["failed", "fam_one", "2026-10-05", bad]
        ]
        assert read_completion(data, "fam_one", good) is not None
        assert read_completion(data, "fam_two", late) is not None
        db = data / "cat.duckdb"
        assert _query(db, f'SELECT bronze_capture_id FROM "silver_{SOURCE}_fam_two"') == [(late,)]


class TestCoveredEvidence:
    def test_t_b6_8_a_newer_covered_capture_is_stale_and_survives_the_drain(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        covered_rid = _rid(9)
        covered = _capture(data, "fam_one", 9, BODY, _t(8), name="Covered")
        covering = _capture(data, "fam_one", 1, BODY, _t(8))
        grant = resource(
            covered_rid,
            "Covered",
            "fam_one",
            disposition={
                "kind": "COVERED",
                "by": _rid(1),
                "evidence": {
                    "covered_capture": covered,
                    "covering_capture": covering,
                    "covered_record_version": "1",
                    "covering_record_version": "1",
                    "inventory_sha256": "",
                },
            },
        )
        registry, generated = _install(
            monkeypatch, data, {"fam_one": {"record": record()}}, extra=[grant], where="_reg_a"
        )
        digest = inventory_sha256(registry.resources[covered_rid][1])
        grant["disposition"]["evidence"]["inventory_sha256"] = digest
        _registry, generated = _install(
            monkeypatch, data, {"fam_one": {"record": record()}}, extra=[grant], where="_reg_b"
        )
        _generic(generated)(data).run(DAY, run_id="r")
        assert _cli(capsys, "fam_one", "--cutoff", CUTOFF)[0] == 0

        newer = _capture(data, "fam_one", 9, BODY.replace(b"1.5", b"8.5"), _t(9), name="Covered")
        code, lines = _cli(capsys, "fam_one", "--cutoff", CUTOFF, "--drain")
        assert code == 1
        (stale,) = _gaps(lines)
        assert stale.startswith(
            f"GAP stale_covered fam_one 2026-10-07 {newer} resource {covered_rid}"
        )
        assert "covered resource has a newer capture" in stale


class TestLateDrainEquality:
    def test_t_b7_1_a_late_drain_equals_the_on_time_transform(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """B7: only ``source_run_id`` may differ between on-time and late."""
        registry, generated = _install(monkeypatch, data, {"fam_one": {"record": record()}})
        on_time = data / "on_time"
        late = data / "late"
        _capture(on_time, "fam_one", 1, BODY, _t(8))
        shutil.copytree(on_time / "bronze", late / "bronze")
        _generic(generated)(on_time).run(DAY, run_id="on-time")
        report = drain(late, registry, ["fam_one"], date(2026, 10, 31), lambda: None)
        assert report.clean and report.drained == (("fam_one", DAY, 1),)
        assert_same_output(snapshot(on_time, "fam_one"), snapshot(late, "fam_one"), KEY)


class TestBespoke:
    DWA = "daily_wind_availability"

    def test_a_bespoke_capture_without_its_hook_is_drained_by_adoption(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The legacy keys are in scope: an output the hook never recorded is
        ``missing`` and the drain adopts it (mtime unchanged)."""
        from pathlib import Path as _Path

        fixture = (
            _Path(__file__).resolve().parents[1]
            / "fixtures"
            / "neso_data_portal"
            / "daily_wind_availability.csv"
        )
        day = date(2026, 8, 16)
        path, _sidecar = write_capture(
            data,
            self.DWA,
            package_slug="daily-wind-availability",
            package_id="3758a0ed-6c96-4e36-88d0-107f5020ddf3",
            resource_id="7aa508eb-36f5-4298-820f-2fa6745ae2e7",
            resource_name="Daily Wind Availability",
            body=fixture.read_bytes(),
            written_at=datetime(2026, 8, 16, 18, 25, tzinfo=UTC),
            ckan_last_modified="2026-08-16T18:25:00",
            partition=day,
        )
        capture_id = path.relative_to(data).as_posix()
        DailyWindAvailabilityTransformer(data).run(day, run_id="r")
        (output,) = (data / "silver" / SOURCE / self.DWA).rglob("*.parquet")
        mtime = output.stat().st_mtime_ns
        registry = registry_module.load_registry()
        cutoff = date(2026, 8, 31)
        report = reconcile(data, registry, [self.DWA], cutoff)
        assert [(g.category, g.capture_id) for g in report.gaps] == [("missing", capture_id)]
        after = drain(data, registry, [self.DWA], cutoff, lambda: None)
        assert after.clean
        assert output.stat().st_mtime_ns == mtime
        ledger = read_completion(data, self.DWA, capture_id)
        assert ledger is not None and ledger["engine_version"] == "bespoke"

    def test_a_bespoke_stamp_collision_is_reported_and_never_drained(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the drain adopting a collided bespoke pair through its
        ``missing`` gaps: P-8 records neither capture of a shared output path,
        and the drain must not record them either (REVIEW-DIFF-1 tests #1)."""
        from pathlib import Path as _Path

        fixture = (
            _Path(__file__).resolve().parents[1]
            / "fixtures"
            / "neso_data_portal"
            / "daily_wind_availability.csv"
        )
        day = date(2026, 8, 16)
        ids = []
        for rid, body in (
            ("7aa508eb-36f5-4298-820f-2fa6745ae2e7", fixture.read_bytes()),
            (
                "7aa508eb-36f5-4298-820f-2fa6745ae2e8",
                fixture.read_bytes().replace(b"120.5", b"121.5"),
            ),
        ):
            path, _sidecar = write_capture(
                data,
                self.DWA,
                package_slug="daily-wind-availability",
                package_id="3758a0ed-6c96-4e36-88d0-107f5020ddf3",
                resource_id=rid,
                resource_name="Daily Wind Availability",
                body=body,
                written_at=datetime(2026, 8, 16, 18, 25, tzinfo=UTC),
                ckan_last_modified="2026-08-16T18:25:00",
                partition=day,
            )
            ids.append(path.relative_to(data).as_posix())
        DailyWindAvailabilityTransformer(data).run(day, run_id="r")
        registry = registry_module.load_registry()
        cutoff = date(2026, 8, 31)
        report = reconcile(data, registry, [self.DWA], cutoff)
        categories = sorted((g.category, g.capture_id) for g in report.gaps)
        assert categories == sorted(
            [*(("duplicated", i) for i in ids), *(("missing", i) for i in ids)]
        )
        assert not [g for g in report.gaps if g.drainable]
        after = drain(data, registry, [self.DWA], cutoff, lambda: None)
        assert after.drained == ()
        assert all(read_completion(data, self.DWA, i) is None for i in ids)
