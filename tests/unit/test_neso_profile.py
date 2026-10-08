"""The offline NESO bronze profiler and its record proposals (v0.22-E P-1..P-7).

T-P1..T-P10 over synthetic bronze in ``tmp_path``: a tmp registry installed
through unit A's seam, captures in A's sidecar shape, a snapshot directory
that passes ``verify_snapshot``, a checksummed field-info run and a batch map.
T-B1 pins the committed batch map against the registry.
"""

from __future__ import annotations

import json
import socket
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from _neso_generic_support import write_capture
from _neso_profile_support import SNAPSHOT_ID, field_doc, write_field_info, write_snapshot
from _neso_registry_support import family, install_registry, package, resource, write_registry

from gridflow.connectors.neso_data_portal import profile
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import SchemaRecord

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BATCHES = PROJECT_ROOT / "docs" / "neso_data_portal" / "batches.json"
BOM = b"\xef\xbb\xbf"

PKG_A, PKG_C, PKG_F, PKG_X = "pkg-a", "pkg-c", "pkg-files", "pkg-unmapped"
IDS = {
    name: f"00000000-0000-4000-8000-{n:012d}"
    for n, name in enumerate(
        ["pa", "pc", "pf", "px", "ra", "rb", "rc", "rf", "rx", "rm", "rt", "rs", "rk", "rd", "rr"]
    )
}
# family key -> (package slug, resource key, resource name, format)
FAMILIES: dict[str, tuple[str, str, str, str]] = {
    "fam_a": (PKG_A, "ra", "Series A", "CSV"),
    "fam_b": (PKG_A, "rb", "Series B", "CSV"),
    "fam_m": (PKG_A, "rm", "Measured", "CSV"),
    "fam_t": (PKG_A, "rt", "Typed", "CSV"),
    "fam_sp": (PKG_A, "rs", "Periods", "CSV"),
    "fam_k": (PKG_A, "rk", "Keys", "CSV"),
    "fam_dup": (PKG_A, "rd", "Duplicates", "CSV"),
    "fam_c": (PKG_C, "rc", "Series C", "CSV"),
    "fam_xlsx": (PKG_C, "rx", "Workbook", "XLSX"),
    "fam_files": (PKG_F, "rf", "Guide", "CSV"),
    "fam_r": (PKG_X, "rr", "Unmapped", "CSV"),
}
PACKAGE_IDS = {PKG_A: IDS["pa"], PKG_C: IDS["pc"], PKG_F: IDS["pf"], PKG_X: IDS["px"]}


def _registry_documents() -> list[dict[str, Any]]:
    documents = []
    for slug, package_id in PACKAGE_IDS.items():
        keys = [k for k, (pkg, *_rest) in FAMILIES.items() if pkg == slug]
        families = [family(k, kind="files" if k == "fam_files" else "tabular") for k in keys]
        resources = []
        for key in keys:
            _pkg, rid, name, fmt = FAMILIES[key]
            disposition = (
                {"kind": "DOC"}
                if key == "fam_files"
                else {"kind": "HOLD", "reason": "Excel", "unit": "X"}
                if fmt == "XLSX"
                else None
            )
            resources.append(resource(IDS[rid], name, key, fmt=fmt, disposition=disposition))
        documents.append(package(slug, package_id, families, resources))
    return documents


def _t(day: int, hour: int = 9) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


class World:
    """One synthetic profiler world: registry, bronze, snapshot, field-info, batches."""

    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.root = root
        self.data = root / "data"
        self.registry_dir = write_registry(root / "registry", _registry_documents())
        self.registry_bytes = {p.name: p.read_bytes() for p in self.registry_dir.iterdir()}
        install_registry(monkeypatch, self.registry_dir)
        snapshot_packages = [
            {
                "name": slug,
                "id": package_id,
                "title": slug.title(),
                "organization": {"title": "Org"},
                "license_title": "NESO Open Data Licence",
                "extras": [{"key": "Update Frequency", "value": "Daily"}],
                "resources": [],
            }
            for slug, package_id in PACKAGE_IDS.items()
        ]
        self.snapshot = write_snapshot(root / "snapshots", snapshot_packages)
        self.field_info = write_field_info(
            root / "field-info",
            {
                "fam_t": field_doc(
                    "fam_t",
                    [
                        ("Effective", "date", "yyyy-mm-dd"),
                        ("Month", "text", ""),
                        ("Num", "numeric", "MW"),
                        ("Bad", "int4", "MW"),
                        ("Stamp", "timestamp", "GMT/BST"),
                    ],
                )
            },
        )
        self.batches = root / "batches.json"
        self.batches.write_text(
            json.dumps({PKG_A: "B1", PKG_C: "B2", PKG_F: "B2"}), encoding="utf-8"
        )

    def capture(self, key: str, body: bytes, written_at: datetime, **kwargs: Any) -> Path:
        slug, rid, name, fmt = FAMILIES[key]
        path, _sidecar = write_capture(
            self.data,
            key,
            package_slug=slug,
            package_id=PACKAGE_IDS[slug],
            resource_id=IDS[rid],
            resource_name=name,
            body=body,
            written_at=written_at,
            ckan_format=fmt,
            extension=fmt.lower(),
            **kwargs,
        )
        return path

    def run(self, out: Path, *extra: str) -> int:
        return profile.main(
            [
                "--snapshot",
                str(self.snapshot),
                "--field-info",
                str(self.field_info),
                "--batches",
                str(self.batches),
                "--out",
                str(out),
                "--report",
                str(out.parent / f"{out.name}.md"),
                "--data-dir",
                str(self.data),
                *extra,
            ]
        )


def _family(out: Path, key: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((out / "families" / f"{key}.json").read_bytes())
    return document


def _summary(out: Path) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((out / "summary.json").read_bytes())
    return document


def _column(document: dict[str, Any], vendor: str, epoch: int = 0) -> dict[str, Any]:
    return next(
        c
        for c in document["proposal"]["record_draft"]["epochs"][epoch]["columns"]
        if c["source"] == vendor
    )


def _todos(document: dict[str, Any], kind: str) -> list[dict[str, str]]:
    return [t for t in document["proposal"]["todos"] if t["kind"] == kind]


MEASURED = BOM + b"Id,Value,Note\r\n1,10,a\r\n2,, \r\n3,30,c\n,,\r\n"
CP1252 = b"Id,Value,Note\r\n4,40,\xa3x\r\n"
RAGGED = b"Id,Value,Note\r\n5,50,e\r\n6,60,f,extra\r\n"


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    """Every family of the synthetic world, captured."""
    w = World(tmp_path, monkeypatch)
    w.capture("fam_m", MEASURED, _t(1))
    w.capture("fam_m", CP1252, _t(2))
    w.capture("fam_m", RAGGED, _t(3))
    w.capture("fam_a", b"A,B\n1,2\n", _t(1))
    w.capture("fam_a", b"A,B,C\n1,2,3\n", _t(2))
    w.capture("fam_b", b"A,B,C\n4,5,6\n", _t(1))
    w.capture("fam_c", b"A,B,C\n7,8,9\n", _t(1))
    w.capture(
        "fam_t",
        b"Effective,Ambig,Iso,Month,Stamp,Num,Bad\n"
        b"31/10/2034,01/02/2024,2024-01-01,Oct-26,2024-01-01T00:30:00Z,1,1.5\n"
        b"01/02/2024,03/04/2025,2024-01-02,Nov-26,2024-01-01T01:00:00Z,2,2\n",
        _t(1),
    )
    rows = [f"2024-01-01,{sp},{sp}" for sp in range(1, 49)]
    rows += ["2024-01-01,4,104", "2024-01-01,5,105"]
    rows += [f"2024-01-02,{sp},{200 + sp}" for sp in range(1, 49)] + ["2024-01-02,51,999"]
    w.capture(
        "fam_sp",
        ("SettlementDate,SettlementPeriod,Value\n" + "\n".join(rows) + "\n").encode(),
        _t(1),
    )
    w.capture("fam_k", b"K,L,V\na,1,x\nb,2,y\nc,3,z\nd,4,w\na,5,v\n", _t(1))
    w.capture("fam_dup", b"A,B\n1,2\n1,2\n3,4\n", _t(1))
    w.capture("fam_xlsx", b"PK\x03\x04 not a csv", _t(1))
    w.capture("fam_files", b"A\n1\n", _t(1))
    w.capture("fam_r", b"A\n1\n", _t(1))
    return w


@pytest.fixture
def out(world: World, tmp_path: Path) -> Path:
    """One run of the profiler over the world, with ``--sample-rows 3``."""
    directory = tmp_path / "out"
    assert world.run(directory, "--sample-rows", "3") == 0
    return directory


class TestMeasurement:
    def test_t_p1_bytes_rows_nulls_blanks_and_encodings(self, out: Path) -> None:
        """T-P1: detects a miscounted row, blank, null, line ending, BOM or encoding.

        Also detects a ragged body aborting the run instead of being recorded.
        """
        document = _family(out, "fam_m")
        first, second, third = document["captures"]
        assert first["bom"] is True and first["utf8"] is True
        assert (first["crlf"], first["lf"], first["cr"]) == (4, 1, 0)
        assert first["rows"] == 4 and first["all_blank_rows"] == 1
        columns = document["epochs"][0]["columns"]
        assert columns["Id"]["null_count"] == 1
        assert columns["Value"]["null_count"] == 2
        assert columns["Note"]["null_count"] == 1
        assert columns["Note"]["blank_count"] == 1
        assert second["utf8"] is False and second["cp1252_clean"] is True
        encoding = _todos(document, "encoding")
        assert len(encoding) == 1 and encoding[0]["consequence"] == "blocked"
        assert document["proposal"]["record_draft"]["encoding"] == "TODO: " + encoding[0]["id"]
        assert third["parse_error"] and third["rows"] is None
        assert "parse_error" in document["flags"] and "not_utf8" in document["flags"]
        assert _todos(document, "parse")[0]["consequence"] == "blocked"


class TestEpochsAndSiblings:
    def test_t_p2_epochs_ordered_and_siblings_flagged_never_merged(
        self, world: World, out: Path
    ) -> None:
        """T-P2: detects epochs out of capture order, a missed or cross-package sibling,
        and any renamed key or registry edit."""
        a = _family(out, "fam_a")
        assert [e["header"] for e in a["epochs"]] == [["A", "B"], ["A", "B", "C"]]
        assert "multi_epoch" in a["flags"]
        assert _todos(a, "multi_epoch")[0]["consequence"] == "none"
        assert "sibling_candidate:fam_b" in a["flags"]
        assert "sibling_candidate:fam_a" in _family(out, "fam_b")["flags"]
        assert not [f for f in _family(out, "fam_c")["flags"] if f.startswith("sibling")]
        on_disk = {p.stem for p in (out / "families").iterdir()}
        measured = {"fam_a", "fam_b", "fam_c", "fam_m", "fam_t", "fam_sp", "fam_k", "fam_dup"}
        assert on_disk == measured | {"fam_r"}
        assert all(_family(out, key)["key"] == key for key in on_disk)
        after = {p.name: p.read_bytes() for p in world.registry_dir.iterdir()}
        assert after == world.registry_bytes


class TestFormats:
    def test_t_p3_dtype_draft_rule(self, out: Path) -> None:
        """T-P3: detects each P-3 dtype-rule branch drafting the wrong dtype or format."""
        document = _family(out, "fam_t")
        assert _column(document, "Effective") == {
            "source": "Effective",
            "name": "effective",
            "nullable": True,
            "dtype": "date",
            "format": "%d/%m/%Y",
        }
        ambiguous = _column(document, "Ambig")
        order = _todos(document, "date_order")
        assert ambiguous["format"] == "TODO: " + order[0]["id"]
        assert order[0]["consequence"] == "blocked"
        assert _column(document, "Iso")["format"] == "%Y-%m-%d"
        month = _column(document, "Month")
        assert (month["dtype"], month["format"], month["name"]) == ("date", "%b-%y", "month_vendor")
        assert "text_declared_date_shaped:Month" in document["proposal"]["flags"]
        assert _column(document, "Stamp")["dtype"] == "string"
        time = _todos(document, "time_semantics")
        assert [t["field"] for t in time] == ["column 'Stamp'"]
        assert time[0]["consequence"] == "held"
        assert _column(document, "Num")["dtype"] == "float64"
        conflict = _todos(document, "type_conflict")
        assert [t["field"] for t in conflict] == ["column 'Bad'.dtype"]
        assert conflict[0]["consequence"] == "blocked"
        assert _column(document, "Bad")["dtype"] == "TODO: " + conflict[0]["id"]
        eligibility = document["proposal"]["record_draft"]["eligibility"]
        assert eligibility["status"] == "held" and eligibility["unit"] == "B1"


class TestSettlementPeriods:
    def test_t_p4_duplicated_pairs_out_of_range_and_days(self, out: Path) -> None:
        """T-P4: detects (date, SP) duplicates, out-of-range periods or day sizes miscounted,
        and any temporal recipe drafted from SP evidence."""
        document = _family(out, "fam_sp")
        (periods,) = document["epochs"][0]["settlement_periods"]
        assert periods["duplicated_pairs"] == 2
        assert periods["out_of_range"] == 1
        assert periods["rows_per_date"] == {"49": 1, "50": 1}
        assert periods["dates_with_repeated_sp"] == 1
        assert (periods["min"], periods["max"]) == (1, 51)
        draft = document["proposal"]["record_draft"]
        temporal = _todos(document, "temporal")
        assert draft["temporal"] == "TODO: " + temporal[0]["id"]
        assert temporal[0]["consequence"] == "none"
        assert "SettlementPeriod" in temporal[0]["question"]


class TestKeys:
    def test_t_p5_sample_unique_key_is_measured_on_the_full_body(self, out: Path) -> None:
        """T-P5: detects a key unique only in the sample being drafted as the entity key."""
        document = _family(out, "fam_k")
        candidates = {",".join(c["columns"]): c for c in document["keys"]["candidates"]}
        assert candidates["K"]["max_duplicates"] == 1
        assert candidates["L"]["max_duplicates"] == 0
        assert document["proposal"]["record_draft"]["entity_key"] == ["l"]
        assert document["proposal"]["record_draft"]["latest"] == "whole_capture"
        assert document["keys"]["full_row_max_duplicates"] == 0
        assert "sampled" in document["flags"]

    def test_t_p5_full_row_duplicates_are_exact(self, out: Path) -> None:
        """T-P5: detects an inexact full-row duplicate count or a key drafted without one."""
        document = _family(out, "fam_dup")
        assert document["keys"]["full_row_max_duplicates"] == 1
        assert document["keys"]["candidates"] == []
        assert document["proposal"]["record_draft"]["entity_key"] == ["a", "b"]


def _resolve(value: Any, key: str | None = None) -> Any:
    """Settle every TODO marker the way a human would, to prove they are the only blocker."""
    if isinstance(value, dict):
        resolved = {k: _resolve(v, k) for k, v in value.items()}
        if resolved.get("dtype") == "string":
            resolved.pop("format", None)
        return resolved
    if isinstance(value, list):
        return [_resolve(v) for v in value]
    if isinstance(value, str) and value.startswith("TODO:"):
        return {
            "temporal": {"kind": "none"},
            "encoding": "utf-8",
            "format": "%d/%m/%Y",
            "dtype": "string",
        }[key or ""]
    return value


class TestDeterminismAndRefusals:
    def test_t_p6_two_runs_are_byte_identical_with_posix_ids(
        self, world: World, out: Path, tmp_path: Path
    ) -> None:
        """T-P6 (I-2): detects nondeterminism (order, clock, host) or a Windows path."""
        again = tmp_path / "again"
        assert world.run(again, "--sample-rows", "3") == 0
        first = {
            p.relative_to(out).as_posix(): p.read_bytes() for p in out.rglob("*") if p.is_file()
        }
        second = {
            p.relative_to(again).as_posix(): p.read_bytes() for p in again.rglob("*") if p.is_file()
        }
        assert first == second
        assert (tmp_path / "out.md").read_bytes() == (tmp_path / "again.md").read_bytes()
        for data in first.values():
            assert b"\\\\" not in data and str(tmp_path).encode() not in data

    def test_t_p6_todo_kinds_and_consequences_come_from_the_table(self, out: Path) -> None:
        """T-P6: detects a TODO kind or consequence outside P-6's table."""
        for path in (out / "families").iterdir():
            for todo in json.loads(path.read_bytes())["proposal"]["todos"]:
                assert profile.TODO_CONSEQUENCES[todo["kind"]] == todo["consequence"]

    def test_t_p6_drafts_are_invalid_until_every_todo_is_settled(self, out: Path) -> None:
        """T-P6: detects a draft that validates as a frozen record while carrying a TODO.

        Every emitted draft must fail ``SchemaRecord`` (its ``temporal`` is always a
        TODO); the same draft with each TODO settled must pass, so the markers are the
        only thing standing between a proposal and a record.
        """
        for path in sorted((out / "families").iterdir()):
            draft = json.loads(path.read_bytes())["proposal"]["record_draft"]
            assert isinstance(draft["temporal"], str) and draft["temporal"].startswith("TODO:")
            with pytest.raises(ValueError):
                SchemaRecord.model_validate(draft)
            SchemaRecord.model_validate(_resolve(draft))

    def test_t_p6_a_non_empty_out_is_refused(self, world: World, tmp_path: Path) -> None:
        """T-P6: detects a run writing into a directory that already holds files."""
        busy = tmp_path / "busy"
        busy.mkdir()
        (busy / "keep.txt").write_bytes(b"x")
        assert world.run(busy) == 2
        assert [p.name for p in busy.iterdir()] == ["keep.txt"]
        assert not (tmp_path / "busy.md").exists()

    def test_t_p6_an_unregistered_bronze_dir_is_refused(self, world: World, tmp_path: Path) -> None:
        """T-P6 (FM-3): detects profiling bronze under a key the registry does not declare."""
        stray = world.data / "bronze" / "neso_data_portal" / "not_a_key"
        stray.mkdir(parents=True)
        assert world.run(tmp_path / "o") == 1
        assert not (tmp_path / "o").exists()

    def test_t_p6_unlisted_field_info_is_refused(self, world: World, tmp_path: Path) -> None:
        """T-P6: detects field-info evidence not covered by its own checksums."""
        (world.field_info / "extra.json").write_text('{"fields": []}', encoding="utf-8")
        assert world.run(tmp_path / "o") == 1
        assert not (tmp_path / "o").exists()

    def test_t_p6_a_kill_before_summary_leaves_no_summary(
        self, world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-P6 (FM-1): detects ``summary.json`` written before the family files."""
        real = profile.replace_atomically

        def dying(path: Path, data: bytes) -> None:
            if path.name == "summary.json":
                raise OSError("killed")
            real(path, data)

        monkeypatch.setattr(profile, "replace_atomically", dying)
        target = tmp_path / "o"
        with pytest.raises(OSError, match="killed"):
            world.run(target)
        assert (target / "families" / "fam_a.json").is_file()
        assert not (target / "summary.json").exists()
        assert not (tmp_path / "o.md").exists()


class TestBounds:
    def test_t_p7_no_body_is_read_whole(
        self, world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-P7 (I-1): detects a body read into Python whole or an unbounded read_csv."""
        bronze = (world.data / "bronze").resolve()
        real_bytes, real_text, real_read_csv = Path.read_bytes, Path.read_text, pl.read_csv

        def is_body(path: Path) -> bool:
            resolved = path.resolve()
            return resolved.is_relative_to(bronze) and not resolved.name.endswith(".meta.json")

        def guarded_bytes(self: Path) -> bytes:
            assert not is_body(self), f"read_bytes on body {self}"
            return real_bytes(self)

        def guarded_text(self: Path, *args: Any, **kwargs: Any) -> str:
            assert not is_body(self), f"read_text on body {self}"
            return real_text(self, *args, **kwargs)

        def bounded_read_csv(*args: Any, **kwargs: Any) -> pl.DataFrame:
            assert kwargs.get("n_rows") is not None, "read_csv without n_rows"
            return real_read_csv(*args, **kwargs)

        monkeypatch.setattr(Path, "read_bytes", guarded_bytes)
        monkeypatch.setattr(Path, "read_text", guarded_text)
        monkeypatch.setattr(pl, "read_csv", bounded_read_csv)
        assert world.run(tmp_path / "o") == 0

    def test_t_p8_no_network(
        self, world: World, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-P8: detects any connection attempt during a full run.

        The package initialiser imports the HTTP client (its registration side
        effect), so the proof is behavioural: every socket connect raises.
        """

        def refuse(*_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("the profiler opened a network connection")

        monkeypatch.setattr(socket.socket, "connect", refuse)
        monkeypatch.setattr(socket.socket, "connect_ex", refuse)
        assert world.run(tmp_path / "o") == 0


class TestCounts:
    def test_t_p9_measured_families_and_batches(self, out: Path) -> None:
        """T-P9: detects files families or non-CSV-only families counted as measured,
        and an unmapped package not landing in UNASSIGNED."""
        summary = _summary(out)
        measured = set(summary["families"])
        assert "fam_files" not in measured and "fam_xlsx" not in measured
        assert summary["totals"]["measured_families"] == len(measured) == 9
        assert summary["families"]["fam_r"]["batch"] == profile.UNASSIGNED
        assert "unassigned_batch" in summary["families"]["fam_r"]["flags"]
        b1 = summary["batches"]["B1"]
        assert b1["families_with_csv"] == 7
        assert b1["distinct_headers"] == sum(
            row["epochs"] for row in summary["families"].values() if row["batch"] == "B1"
        )
        assert summary["batches"]["B2"]["packages"] == 2

    @pytest.mark.parametrize(("epochs", "over"), [(15, False), (16, True)])
    def test_t_p9_over_15_flips_at_16(self, world: World, epochs: int, over: bool) -> None:
        """T-P9: detects the batch-sizing flag off by one."""
        documents = {
            "fam_a": {
                "package": PKG_A,
                "batch": "B1",
                "captures": [],
                "other_captures": 0,
                "epochs": [{"header": [str(i)]} for i in range(epochs)],
                "flags": [],
                "proposal": None,
            }
        }
        registry = registry_module.load_registry()
        summary = profile.build_summary(documents, registry, {PKG_A: "B1"}, {}, [])
        assert summary["batches"]["B1"]["distinct_headers"] == epochs
        assert summary["batches"]["B1"]["over_15"] is over

    def test_t_p10_the_report_renders_from_summary_alone(self, out: Path, tmp_path: Path) -> None:
        """T-P10: detects the report reading anything but ``summary.json``."""
        import shutil

        shutil.rmtree(out / "families")
        rendered = profile.render_report(_summary(out))
        assert rendered.encode("utf-8") == (tmp_path / "out.md").read_bytes()
        assert "- Measured family count: **9**" in rendered
        assert "| B1 |" in rendered and "| `fam_r` |" in rendered


def test_snapshot_id_is_recorded(out: Path) -> None:
    """Detects the run's inputs missing from the summary (I-2: inputs, not host values)."""
    assert _summary(out)["inputs"] == {
        "snapshot_id": SNAPSHOT_ID,
        "field_info": "20261008T114228Z",
        "sample_rows": 3,
    }
