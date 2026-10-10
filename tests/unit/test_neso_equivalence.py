"""The COVERED harness: proof inputs, fingerprint, scope and reconcile (ADR-037 P-13, P-14).

Every X3 row of the unit X test matrix. Each test builds a synthetic package
(a covered and a covering family with one frozen record each), writes captures
in unit A's sidecar shape under a short tmp data root, proves, installs the
returned disposition into a tmp registry through P-15's seam and reconciles.
"""

from __future__ import annotations

import copy
import dataclasses
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from _container_support import forbid_zipfile_reads, zip_bytes  # noqa: F401 - a fixture
from _neso_generic_support import install_generated, write_capture
from _neso_registry_support import epoch, family, package, record, resource, sp_columns

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import RegistryError, dump_json
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal import equivalence
from gridflow.silver.neso_data_portal.equivalence import (
    COMPONENTS,
    ProofInputError,
    ProofResult,
    Site,
    comparison_components,
    gather_inputs,
    prove,
)
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry import Registry
    from gridflow.silver.neso_data_portal.reconcile import Gap

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

PKG = "ccccccc0-0000-4000-8000-000000000000"
SLUG = "pkg-eq"
COVERED = "ccccccc0-0000-4000-8000-000000000001"
COVERING = "ccccccc0-0000-4000-8000-000000000002"
COVERING_2 = "ccccccc0-0000-4000-8000-000000000003"
BOX = "ccccccc0-0000-4000-8000-000000000004"
SIDE = "ccccccc0-0000-4000-8000-000000000005"
DAY = date(2026, 10, 7)
D1 = date(2026, 10, 5)
CUTOFF = date(2026, 10, 31)
HEADER = b"SettlementDate,SettlementPeriod,Unit,Value\n"
ROWS = b"2026-10-07,1,A,1.5\n2026-10-07,2,A,2.5\n"
BODY = HEADER + ROWS
COVERED_LM = "2026-10-07T09:00:00.000001"
COVERING_LM = "2026-10-05T08:00:00.000001"
ISSUE = {"kind": "filename_token", "pattern": r"^f_(\d{8})\.csv$", "format": "%Y%m%d"}

CASES: dict[str, str] = {
    "a": "covered_scope",
    "b": "covering_scope",
    "c": "covered_record",
    "d": "covering_record",
    "e": "covered_scope",
    "f": "covered_scope",
    "g": "harness",
    "h": "covering_leg",
}
"""T-X3-5: the one component each single change must void."""


@pytest.fixture
def data() -> Iterator[Path]:
    """A short data root (output names are long; Windows paths are not)."""
    pipeline_runner.import_transformers()
    with tempfile.TemporaryDirectory(prefix="q", ignore_cleanup_errors=True) as root:
        yield Path(root)


def _t(hour: int, minute: int = 0, day: date = DAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


@dataclass
class World:
    """One synthetic package and the captures written into ``data``."""

    data: Path
    monkeypatch: pytest.MonkeyPatch
    covered_record: dict[str, Any] = field(default_factory=record)
    covering_record: dict[str, Any] = field(default_factory=record)
    covered: dict[str, Any] = field(default_factory=dict)
    extra_resources: list[dict[str, Any]] = field(default_factory=list)
    extra_families: list[dict[str, Any]] = field(default_factory=list)
    installs: int = 0

    def __post_init__(self) -> None:
        if not self.covered:
            self.covered = resource(COVERED, "Covered", "eq_old")

    def doc(self) -> dict[str, Any]:
        families = [
            family("eq_old", record=self.covered_record),
            family("eq_new", record=self.covering_record),
            *self.extra_families,
        ]
        resources = [
            self.covered,
            resource(COVERING, "Covering", "eq_new"),
            resource(COVERING_2, "Covering Two", "eq_new"),
            *self.extra_resources,
        ]
        return package(SLUG, PKG, families, resources)

    def install(self) -> Registry:
        self.installs += 1
        registry, _generated = install_generated(
            self.monkeypatch, self.data / f"_r{self.installs}", [self.doc()]
        )
        return registry

    def capture(
        self,
        key: str,
        resource_id: str,
        name: str,
        body: bytes,
        written: datetime,
        lm: str,
        *,
        day: date = DAY,
        filename: str = "file.csv",
        fmt: str = "CSV",
        extension: str = "csv",
    ) -> str:
        path, _sidecar = write_capture(
            self.data,
            key,
            package_slug=SLUG,
            package_id=PKG,
            resource_id=resource_id,
            resource_name=name,
            body=body,
            written_at=written,
            ckan_last_modified=lm,
            resource_filename=filename,
            partition=day,
            ckan_format=fmt,
            extension=extension,
        )
        return path.relative_to(self.data).as_posix()

    def covered_capture(self, body: bytes = BODY, **kwargs: Any) -> str:
        kwargs.setdefault("written", _t(9))
        kwargs.setdefault("lm", COVERED_LM)
        return self.capture("eq_old", COVERED, "Covered", body, **kwargs)

    def covering_capture(
        self, body: bytes = BODY, resource_id: str = COVERING, **kwargs: Any
    ) -> str:
        kwargs.setdefault("written", _t(8, day=D1))
        kwargs.setdefault("lm", COVERING_LM)
        kwargs.setdefault("day", D1)
        name = "Covering" if resource_id == COVERING else "Covering Two"
        return self.capture("eq_new", resource_id, name, body, **kwargs)

    def prove(
        self, site: Site | None = None, by: str = COVERING, key: str = "eq_old"
    ) -> ProofResult:
        registry = self.install()
        inputs = gather_inputs(registry, self.data, site or Site(COVERED, None), by, key, CUTOFF)
        return prove(inputs)

    def grant(self, result: ProofResult, child: str | None = None) -> None:
        assert result.granted, result.refusals
        assert result.disposition is not None
        if child is None:
            self.covered["disposition"] = copy.deepcopy(result.disposition)
        else:
            for entry in self.covered["children"]:
                if entry["child"] == child:
                    entry["disposition"] = copy.deepcopy(result.disposition)

    def stale(self, key: str = "eq_old", cutoff: date = CUTOFF) -> list[Gap]:
        registry = self.install()
        report = reconcile(self.data, registry, [key], cutoff)
        return [gap for gap in report.gaps if gap.category == "stale_covered"]


def _changed(gap: Gap) -> list[str]:
    marker = "components changed: "
    assert marker in gap.detail, gap.detail
    return gap.detail.split(marker, 1)[1].split(";", 1)[0].split(", ")


@pytest.fixture
def world(data: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    return World(data, monkeypatch)


def _granted_world(world: World) -> World:
    world.covered_capture()
    world.covering_capture()
    world.grant(world.prove())
    assert world.stale() == []
    return world


class TestRefusals:
    """T-X3-1..3, T-X3-7: pairs that must never grant."""

    def test_x3_1_same_header_different_rows(self, world: World) -> None:
        world.covered_capture()
        world.covering_capture(BODY.replace(b",A,", b",B,"))
        result = world.prove()
        assert not result.granted
        (refusal,) = result.refusals
        assert "i" in refusal["against"][0]["legs"]

    def test_x3_2_tr141_vintage_shape(self, world: World) -> None:
        world.covered_capture(lm="2021-09-16T13:08:27.615831")
        world.covering_capture(lm="2026-05-29T12:28:29.477953")
        result = world.prove()
        assert not result.granted
        legs = result.refusals[0]["against"][0]["legs"]
        assert set(legs) == {"iii"}

    def test_x3_3a_filename_token_issue_differs(self, data: Path, monkeypatch: Any) -> None:
        rec = record(
            epochs=[epoch(sp_columns(), issue=ISSUE)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
        )
        world = World(data, monkeypatch, covered_record=rec, covering_record=copy.deepcopy(rec))
        world.covered_capture(filename="f_20261001.csv")
        world.covering_capture(filename="f_20261002.csv")
        result = world.prove()
        assert not result.granted
        assert set(result.refusals[0]["against"][0]["legs"]) == {"i"}

    def test_x3_3b_an_edition_column_differs(self, data: Path, monkeypatch: Any) -> None:
        # ``edition`` is engine-reserved since ADR-042, so the vendor column takes another name.
        edition = {
            "source": "Edition",
            "name": "edition_label",
            "dtype": "string",
            "nullable": True,
        }
        cols = [*sp_columns(), edition]
        rec = record(epochs=[epoch(cols)])
        world = World(data, monkeypatch, covered_record=rec, covering_record=copy.deepcopy(rec))
        head = HEADER.replace(b"Value\n", b"Value,Edition\n")
        world.covered_capture(head + b"2026-10-07,1,A,1.5,first\n")
        world.covering_capture(head + b"2026-10-07,1,A,1.5,second\n")
        result = world.prove()
        assert not result.granted
        assert set(result.refusals[0]["against"][0]["legs"]) == {"i"}

    def test_x3_7_a_wider_covering_never_grants(self, world: World) -> None:
        world.covered_capture(HEADER + b"2026-10-07,1,A,1.5\n")
        world.covering_capture(BODY)
        result = world.prove()
        assert not result.granted
        assert result.refusals[0]["against"][0]["legs"]["i"]["anti_join"] == [0, 1]

    def test_x3_7_duplicate_rows_in_one_capture_fail_clean_typing(self, world: World) -> None:
        world.covered_capture(HEADER + b"2026-10-07,1,A,1.5\n2026-10-07,1,A,1.5\n")
        world.covering_capture(HEADER + b"2026-10-07,1,A,1.5\n")
        result = world.prove()
        assert not result.granted
        assert "DuplicateEntityKeyError" in result.refusals[0]["legs"]["ii"]

    @pytest.mark.parametrize("side", ["covered", "covering"])
    def test_x3_7_an_excluded_row_in_either_scope(self, world: World, side: str) -> None:
        bad = BODY + b"2026-10-07,3,,9.5\n"
        world.covered_capture(bad if side == "covered" else BODY)
        world.covering_capture(bad if side == "covering" else BODY)
        result = world.prove()
        assert not result.granted
        assert "excluded 1 row(s)" in result.refusals[0]["legs"]["ii"]


class TestGrant:
    """T-X3-4, T-X3-6: a proved pair grants, installs clean and survives a recapture."""

    def test_x3_4_granted_pair_installs_clean(self, world: World) -> None:
        world.covered_capture()
        world.covering_capture()
        result = world.prove()
        assert result.granted
        assert result.disposition is not None
        assert result.disposition["key"] == "eq_old" and result.disposition["by"] == COVERING
        assert set(result.components) == set(COMPONENTS)
        world.grant(result)
        assert world.stale() == []

    def test_x3_6_identical_recapture_keeps_the_grant(self, world: World) -> None:
        _granted_world(world)
        world.covered_capture(written=_t(15))
        assert world.stale() == []


class TestEachComponentVoids:
    """T-X3-5a-h: every proof-input field, changed alone, voids the grant by name."""

    def test_meta_every_component_has_a_voiding_case(self) -> None:
        assert set(CASES.values()) | {"covered_leg", "covered_children"} == set(COMPONENTS)

    @staticmethod
    def _assert_voids(world: World, letter: str, **stale: Any) -> None:
        (gap,) = world.stale(**stale)
        assert _changed(gap) == [CASES[letter]], gap.detail
        registry = world.install()
        after = drain(world.data, registry, ["eq_old"], stale.get("cutoff", CUTOFF), lambda: None)
        assert any(g.category == "stale_covered" for g in after.gaps)

    def test_a_new_covered_capture(self, world: World) -> None:
        _granted_world(world)
        world.covered_capture(BODY.replace(b"1.5", b"7.5"), written=_t(12))
        self._assert_voids(world, "a")

    def test_a_reconcile_cutoff_before_the_proof(self, world: World) -> None:
        _granted_world(world)
        (gap,) = world.stale(cutoff=date(2026, 10, 6))
        assert _changed(gap) == ["covered_scope"]
        assert "no capture of covered" in gap.detail

    def test_b_a_new_covering_capture(self, world: World) -> None:
        _granted_world(world)
        world.covering_capture(BODY.replace(b"1.5", b"7.5"), written=_t(12, day=D1))
        self._assert_voids(world, "b")

    def test_c_covered_record_version(self, world: World) -> None:
        _granted_world(world)
        world.covered_record = record(version="2")
        self._assert_voids(world, "c")

    def test_d_covering_record_version(self, world: World) -> None:
        _granted_world(world)
        world.covering_record = record(version="2")
        self._assert_voids(world, "d")

    def test_e_same_bytes_new_resource_filename(self, data: Path, monkeypatch: Any) -> None:
        rec = record(
            epochs=[epoch(sp_columns(), issue=ISSUE)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
        )
        world = World(data, monkeypatch, covered_record=rec, covering_record=copy.deepcopy(rec))
        world.covered_capture(filename="f_20261001.csv")
        world.covering_capture(filename="f_20261001.csv")
        world.grant(world.prove())
        assert world.stale() == []
        world.covered_capture(filename="f_20261003.csv", written=_t(13))
        self._assert_voids(world, "e")

    def test_f_same_bytes_new_last_modified(self, world: World) -> None:
        _granted_world(world)
        world.covered_capture(lm="2026-10-07T10:00:00.000001", written=_t(13))
        self._assert_voids(world, "f")

    def test_g_harness_version(self, world: World, monkeypatch: pytest.MonkeyPatch) -> None:
        _granted_world(world)
        monkeypatch.setattr(equivalence, "HARNESS_VERSION", "2")
        self._assert_voids(world, "g")

    def test_h_by_moved_to_an_equal_valued_resource(self, world: World) -> None:
        world.covering_capture(resource_id=COVERING_2)
        _granted_world(world)
        world.covered["disposition"]["by"] = COVERING_2
        self._assert_voids(world, "h")


class TestCoveringGone:
    """T-X3-8: a grant never outlives its cover."""

    def test_by_removed_from_the_registry_is_refused_at_load(self, world: World) -> None:
        _granted_world(world)
        world.covered["disposition"]["by"] = "ccccccc0-0000-4000-8000-0000000000ff"
        with pytest.raises(RegistryError, match="V-12:"):
            world.install()

    def test_by_never_captured_is_stale(self, world: World) -> None:
        _granted_world(world)
        shutil.rmtree(world.data / "bronze" / "neso_data_portal" / "eq_new")
        (gap,) = world.stale()
        assert "no capture of covering" in gap.detail


def _box_world(
    world: World, bodies: dict[str, bytes], extra: dict[str, str] | None = None
) -> World:
    """A ZIP resource (family ``eq_files``) whose CSV members feed ``eq_box``."""
    spec = {"member_pattern": r"[a-z]\.csv", "inner": "csv"}
    world.extra_families = [
        family(
            "eq_box", record=record(reader="zip_member", zip_member=spec, siblings=("eq_files",))
        ),
        family("eq_files", kind="files"),
    ]
    children = [
        {"child": name, "disposition": {"kind": "SILVER", "key": "eq_box"}} for name in bodies
    ]
    children.extend({"child": name, "disposition": {"kind": "DOC"}} for name in (extra or {}))
    world.covered = resource(
        BOX,
        "Box",
        "eq_files",
        fmt="ZIP",
        disposition={"kind": "HOLD", "reason": "box", "unit": "T"},
        children=children,
    )
    body = zip_bytes([*bodies.items(), *((n, b.encode()) for n, b in (extra or {}).items())])
    world.capture("eq_files", BOX, "Box", body, _t(9), COVERED_LM, fmt="ZIP", extension="zip")
    return world


class TestInstallDoesNotSelfInvalidate:
    """T-X3-9 (I-PROOF): installing, or editing a sibling, changes no component."""

    def test_resource_level(self, world: World) -> None:
        world.covered_capture()
        world.covering_capture()
        result = world.prove()
        world.grant(result)
        registry = world.install()
        assert registry.resources[COVERED][1].disposition.kind == "COVERED"
        assert reconcile(world.data, registry, ["eq_old"], CUTOFF).gaps == ()

    def test_child_level_with_a_second_grant_and_a_sibling_edit(self, world: World) -> None:
        _box_world(world, {"a.csv": BODY, "b.csv": BODY}, {"n.txt": "notes"})
        world.covering_capture()
        first = world.prove(Site(BOX, "a.csv"), key="eq_box")
        world.grant(first, "a.csv")
        assert world.stale("eq_box") == []
        second = world.prove(Site(BOX, "b.csv"), key="eq_box")
        world.grant(second, "b.csv")
        for entry in world.covered["children"]:
            if entry["child"] == "n.txt":
                entry["disposition"] = {"kind": "HOLD", "reason": "edited", "unit": "T"}
        assert world.stale("eq_box") == []

    def test_dump_json_round_trip_and_evidence_only_edits(self, world: World) -> None:
        world.covered_capture()
        world.covering_capture()
        result = world.prove()
        world.grant(result)
        directory = world.data / "_copy"
        directory.mkdir()
        (directory / f"{SLUG}.json").write_text(dump_json(world.doc()), encoding="utf-8")
        (directory / "_frozen_keys.json").write_text("[]\n", encoding="utf-8")
        (directory / "_adjudications.json").write_text("[]\n", encoding="utf-8")
        loaded = registry_module.load_registry(directory)
        inputs = gather_inputs(
            world.install(), world.data, Site(COVERED, None), COVERING, "eq_old", CUTOFF
        )
        assert loaded.resources[COVERED][1].disposition.kind == "COVERED"
        assert comparison_components(inputs) == result.components


class TestEvidenceBindsItsChild:
    """T-X3-11: evidence copied or swapped between sites is stale by ``covered_leg``."""

    def test_a_copied_onto_a_different_child(self, world: World) -> None:
        other = BODY.replace(b"1.5", b"4.5")
        _box_world(world, {"a.csv": BODY, "b.csv": other})
        world.covering_capture()
        result = world.prove(Site(BOX, "a.csv"), key="eq_box")
        world.grant(result, "a.csv")
        world.grant(result, "b.csv")
        (gap,) = world.stale("eq_box")
        assert "child b.csv" in gap.detail and _changed(gap) == ["covered_leg"]

    def test_b_valid_evidences_swapped(self, world: World) -> None:
        _box_world(world, {"a.csv": BODY, "b.csv": BODY})
        world.covering_capture()
        for_a = world.prove(Site(BOX, "a.csv"), key="eq_box")
        for_b = world.prove(Site(BOX, "b.csv"), key="eq_box")
        world.grant(for_b, "a.csv")
        world.grant(for_a, "b.csv")
        gaps = world.stale("eq_box")
        assert len(gaps) == 2
        assert all(_changed(gap) == ["covered_leg"] for gap in gaps)


class TestInventoryChange:
    """T-X3-12: a registry inventory edit voids by ``covered_children``; mismatched seams refuse."""

    def test_a_removing_a_sibling_child(self, world: World) -> None:
        _box_world(world, {"a.csv": BODY}, {"s.txt": "doc"})
        world.covering_capture()
        world.grant(world.prove(Site(BOX, "a.csv"), key="eq_box"), "a.csv")
        assert world.stale("eq_box") == []
        world.covered["children"] = [c for c in world.covered["children"] if c["child"] != "s.txt"]
        (gap,) = world.stale("eq_box")
        assert _changed(gap) == ["covered_children"]
        refused = world.prove(Site(BOX, "a.csv"), key="eq_box")
        assert not refused.granted
        assert "ContainerInventoryError" in refused.refusals[0]["legs"]["ii"]

    def test_b_proof_and_seam_registries_disagree(
        self, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _box_world(world, {"a.csv": BODY}, {"s.txt": "doc"})
        world.covering_capture()
        proof_registry = world.install()
        world.covered["children"] = [c for c in world.covered["children"] if c["child"] != "s.txt"]
        world.install()

        def _no_read(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("a body was read")

        monkeypatch.setattr(equivalence, "read_children", _no_read)
        with pytest.raises(ProofInputError, match="children"):
            gather_inputs(
                proof_registry, world.data, Site(BOX, "a.csv"), COVERING, "eq_box", CUTOFF
            )


class TestWholeExemptionIsProved:
    """T-X3-13 (I-SCOPE): every covered capture, wherever it sits, must be matched."""

    TEN = HEADER + b"2026-10-07,1,A,10\n"
    TWENTY = HEADER + b"2026-10-07,1,A,20\n"

    def _two_covered(self, world: World) -> None:
        world.covered_capture(self.TEN, written=_t(9), lm="2026-10-07T09:00:00.000001")
        world.covered_capture(self.TWENTY, written=_t(11), lm="2026-10-07T11:00:00.000001")

    def test_a_a_historical_capture_unmatched(self, world: World) -> None:
        self._two_covered(world)
        world.covering_capture(
            self.TWENTY, written=_t(10), lm="2026-10-07T10:00:00.000001", day=DAY
        )
        newest_only = world.prove()
        assert not newest_only.granted
        assert len(newest_only.refusals) == 1
        assert newest_only.refusals[0]["body_sha256"] != ""
        registry = world.install()
        inputs = gather_inputs(
            registry, world.data, Site(COVERED, None), COVERING, "eq_old", CUTOFF
        )
        c2 = max(inputs.covered_scope, key=lambda item: item.capture.written_at)
        live = dataclasses.replace(inputs, covered_scope=(c2,))
        assert prove(live).granted  # the newest pair alone passes every leg
        c1 = min(inputs.covered_scope, key=lambda item: item.capture.written_at)
        assert newest_only.refusals[0]["capture_id"].endswith(c1.capture.body.name)

    def test_b_every_covered_capture_matched(self, world: World) -> None:
        self._two_covered(world)
        world.covering_capture(
            self.TEN, written=_t(8, 30), lm="2026-10-07T08:30:00.000001", day=DAY
        )
        world.covering_capture(
            self.TWENTY, written=_t(10), lm="2026-10-07T10:00:00.000001", day=DAY
        )
        result = world.prove()
        assert result.granted, result.refusals
        registry = world.install()
        inputs = gather_inputs(
            registry, world.data, Site(COVERED, None), COVERING, "eq_old", CUTOFF
        )
        assert len(inputs.covered_scope) == 2

    def test_c_a_capture_under_a_sibling_directory_is_in_scope(self, world: World) -> None:
        world.extra_families = [family("eq_side", name_regex="Covered.*")]
        world.extra_resources = [resource(SIDE, "Covered Side", "eq_side")]
        self._two_covered(world)
        world.covering_capture(
            self.TEN, written=_t(8, 30), lm="2026-10-07T08:30:00.000001", day=DAY
        )
        world.covering_capture(
            self.TWENTY, written=_t(10), lm="2026-10-07T10:00:00.000001", day=DAY
        )
        thirty = HEADER + b"2026-10-07,1,A,30\n"
        world.capture("eq_side", COVERED, "Covered", thirty, _t(12), "2026-10-07T12:00:00.000001")
        result = world.prove()
        assert not result.granted
        assert "eq_side" in result.refusals[0]["capture_id"]
