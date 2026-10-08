"""Capture index and the usable rule (ADR-033 P-10, X-1..X-4; REVIEW-PLAN-3 M1).

Every negative is fed from a copy of one valid capture and changes exactly one
thing, so each clause of the usable rule is shown to be load-bearing; the
unmodified copy is the positive control. Per REVIEW-PLAN-3 M1 the registry
identity clause checks package and family selector, not UUID membership, so a
recreated-UUID capture is a positive control too.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
from _neso_registry_support import (
    edit_sidecar,
    family,
    install_registry,
    package,
    resource,
    write_capture,
    write_registry,
)

from gridflow.connectors.neso_data_portal import captures
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.captures import (
    RegistryFreezeError,
    assert_bronze_dirs_registered,
    newest_by_resource,
    scan_dataset,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.registry import Registry

PKG_A = "aaaaaaaa-0000-4000-8000-000000000000"
PKG_B = "bbbbbbbb-0000-4000-8000-000000000000"
R1 = "aaaaaaaa-0000-4000-8000-000000000001"
R2 = "aaaaaaaa-0000-4000-8000-000000000002"
R3 = "bbbbbbbb-0000-4000-8000-000000000003"
RECREATED = "cccccccc-0000-4000-8000-00000000000c"


@pytest.fixture
def registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Registry:
    directory = write_registry(
        tmp_path / "registry",
        [
            package(
                "pkg-alpha",
                PKG_A,
                [family("alpha_series"), family("alpha_extra")],
                [
                    resource(R1, "Alpha Series", "alpha_series"),
                    resource(R2, "Alpha Extra", "alpha_extra"),
                ],
            ),
            package(
                "pkg-beta", PKG_B, [family("beta_series")], [resource(R3, "Beta", "beta_series")]
            ),
        ],
    )
    return install_registry(monkeypatch, directory)


def _alpha_capture(bronze: Path, key: str = "alpha_series", **kwargs: Any) -> tuple[Path, Path]:
    defaults: dict[str, Any] = {
        "package_slug": "pkg-alpha",
        "package_id": PKG_A,
        "resource_id": R1,
        "resource_name": "Alpha Series",
    }
    defaults.update(kwargs)
    return write_capture(bronze / key, **defaults)


def _set_params(key: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def _mutate(meta: dict[str, Any]) -> None:
        meta["request_params"][key] = value

    return _mutate


def _drop_param(key: str) -> Callable[[dict[str, Any]], None]:
    def _mutate(meta: dict[str, Any]) -> None:
        del meta["request_params"][key]

    return _mutate


class TestUsableRule:
    """X-1: one negative per clause of the usable rule, each from a valid copy."""

    def test_positive_control_the_unmodified_capture_is_usable(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        body, sidecar = _alpha_capture(tmp_path / "bronze")
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert [c.sidecar for c in scan.captures] == [sidecar]
        assert scan.captures[0].body == body
        assert scan.unusable == ()

    def test_recreated_uuid_with_the_selected_name_is_usable(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        """M1 positive control: NESO recreated the resource under a new UUID."""
        assert RECREATED not in registry.resources
        _alpha_capture(tmp_path / "bronze", resource_id=RECREATED)
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert [c.resource_id for c in scan.captures] == [RECREATED]
        assert scan.unusable == ()

    @pytest.mark.parametrize(
        ("label", "mutate_meta", "expected"),
        [
            (
                "naive written_at",
                lambda meta: meta.update(written_at="2026-10-07T12:00:00"),
                "written_at",
            ),
            ("resource_name removed", _drop_param("resource_name"), "provenance_for"),
            ("ckan_last_modified empty", _set_params("ckan_last_modified", ""), "provenance_for"),
            (
                "ckan_last_modified unparseable",
                _set_params("ckan_last_modified", "yesterday"),
                "provenance_for",
            ),
            (
                "name the family's selector rejects",
                _set_params("resource_name", "Alpha Series (renamed)"),
                "does not select",
            ),
            ("package differs from the registry's", _set_params("package", "pkg-beta"), "package"),
            ("ckan_format missing", _drop_param("ckan_format"), "ckan_format"),
        ],
    )
    def test_sidecar_negatives(
        self,
        tmp_path: Path,
        registry: Registry,
        label: str,
        mutate_meta: Callable[[dict[str, Any]], None],
        expected: str,
    ) -> None:
        _body, sidecar = _alpha_capture(tmp_path / "bronze")
        edit_sidecar(sidecar, mutate_meta)
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.captures == (), label
        assert len(scan.unusable) == 1, label
        assert expected in scan.unusable[0].reason, (label, scan.unusable[0].reason)

    def test_body_size_mismatch(self, tmp_path: Path, registry: Registry) -> None:
        body, _sidecar = _alpha_capture(tmp_path / "bronze")
        body.write_bytes(body.read_bytes() + b"3,4\n")
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.captures == ()
        assert "sidecar records" in scan.unusable[0].reason

    def test_missing_body(self, tmp_path: Path, registry: Registry) -> None:
        body, _sidecar = _alpha_capture(tmp_path / "bronze")
        body.unlink()
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.captures == ()
        assert "found 0" in scan.unusable[0].reason
        assert scan.unusable[0].resource_id == R1

    def test_capture_under_another_familys_directory(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        _alpha_capture(tmp_path / "bronze", key="alpha_extra")
        scan = scan_dataset(tmp_path / "bronze" / "alpha_extra", registry)
        assert scan.captures == ()
        assert "does not select" in scan.unusable[0].reason

    def test_capture_under_an_unregistered_directory(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        _alpha_capture(tmp_path / "bronze", key="not_a_key")
        scan = scan_dataset(tmp_path / "bronze" / "not_a_key", registry)
        assert scan.captures == ()
        assert "not a registry family" in scan.unusable[0].reason


class TestLegacySidecar:
    """X-2: master's E8 sidecar shape (no additive keys) is usable."""

    def test_legacy_shape_against_the_real_registry(self, tmp_path: Path) -> None:
        registry = registry_module.load_registry()
        _body, sidecar = write_capture(
            tmp_path / "historic_generation_mix",
            package_slug="historic-generation-mix",
            package_id="88313ae5-94e4-4ddc-a790-593554d8c6b9",
            resource_id="f93d1835-75bc-43e5-84ad-12472b180a98",
            resource_name="Historic GB Generation Mix",
            ckan_last_modified="2026-09-26T18:19:36.497453",
            legacy_name=True,
            additive=False,
        )
        scan = scan_dataset(tmp_path / "historic_generation_mix", registry)
        assert [c.sidecar for c in scan.captures] == [sidecar]
        assert scan.captures[0].ckan_last_modified == "2026-09-26T18:19:36.497453"


class TestNewest:
    """X-3 and X-4: the newest *usable* capture, deterministically."""

    def test_corrupt_newer_sidecar_is_unusable_and_the_older_capture_wins(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        t0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
        _old_body, old_sidecar = _alpha_capture(tmp_path / "bronze", fetched_at=t0)
        _new_body, new_sidecar = _alpha_capture(
            tmp_path / "bronze", fetched_at=t0 + timedelta(hours=1), body=b"A,B\n9,9\n"
        )
        new_sidecar.write_text("{not json", encoding="utf-8")
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert [u.sidecar for u in scan.unusable] == [new_sidecar]
        assert newest_by_resource(scan.captures)[R1].sidecar == old_sidecar

    def test_newest_is_by_written_at(self, tmp_path: Path, registry: Registry) -> None:
        t0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
        _alpha_capture(tmp_path / "bronze", fetched_at=t0 + timedelta(hours=1), written_at=t0)
        _b, later = _alpha_capture(
            tmp_path / "bronze", fetched_at=t0, written_at=t0 + timedelta(hours=2), body=b"x\n1\n"
        )
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert newest_by_resource(scan.captures)[R1].sidecar == later

    def test_tie_on_written_at_resolves_by_path(self, tmp_path: Path, registry: Registry) -> None:
        t0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
        first, _ = _alpha_capture(tmp_path / "bronze", written_at=t0, body=b"A\n1\n")
        second, _ = _alpha_capture(tmp_path / "bronze", written_at=t0, body=b"A\n2\n")
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        expected = max(first, second, key=str)
        assert newest_by_resource(scan.captures)[R1].body == expected
        assert newest_by_resource(reversed(scan.captures))[R1].body == expected


class TestOrphansAndTemps:
    def test_body_without_sidecar_is_an_orphan_and_temps_are_reported(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        body, sidecar = _alpha_capture(tmp_path / "bronze")
        sidecar.unlink()
        temp = body.parent / f".tmp_{body.name}.0123abcd"
        temp.write_bytes(b"partial")
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.captures == () and scan.unusable == ()
        assert scan.orphans == (body,)
        assert scan.temps == (temp,)


class TestRuntimeFreezePin:
    """P-4's runtime pin reads bronze directory names only."""

    def test_unregistered_directory_raises(self, tmp_path: Path, registry: Registry) -> None:
        bronze = captures.bronze_source_dir(tmp_path)
        (bronze / "alpha_series").mkdir(parents=True)
        assert_bronze_dirs_registered(tmp_path, registry)
        (bronze / "renamed_key").mkdir()
        with pytest.raises(RegistryFreezeError, match="renamed_key"):
            assert_bronze_dirs_registered(tmp_path, registry)

    def test_no_bronze_at_all_is_fine(self, tmp_path: Path, registry: Registry) -> None:
        assert_bronze_dirs_registered(tmp_path, registry)


class TestSilverScanOptions:
    """T-B8-4: B's keyword-only scan options; A's tests above stay unchanged."""

    def test_require_provenance_false_admits_a_capture_without_last_modified(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        """Detects silver losing sight of a dump capture (no ``last_modified``)."""
        _alpha_capture(tmp_path / "bronze", ckan_last_modified="")
        dataset = tmp_path / "bronze" / "alpha_series"
        assert scan_dataset(dataset, registry).captures == ()
        relaxed = scan_dataset(dataset, registry, require_provenance=False)
        assert len(relaxed.captures) == 1
        assert relaxed.captures[0].ckan_last_modified is None

    def test_require_provenance_false_still_needs_the_identity_keys(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        _body, sidecar = _alpha_capture(tmp_path / "bronze")
        edit_sidecar(sidecar, _drop_param("resource_filename"))
        scan = scan_dataset(
            tmp_path / "bronze" / "alpha_series", registry, require_provenance=False
        )
        assert scan.captures == ()
        assert "resource_filename" in scan.unusable[0].reason

    def test_partition_restricts_the_walk_to_one_date_directory(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        _alpha_capture(tmp_path / "bronze", partition="2026/10/07")
        _alpha_capture(
            tmp_path / "bronze",
            partition="2026/10/08",
            fetched_at=datetime(2026, 10, 8, 12, tzinfo=UTC),
        )
        dataset = tmp_path / "bronze" / "alpha_series"
        assert len(scan_dataset(dataset, registry).captures) == 2
        from datetime import date

        only = scan_dataset(dataset, registry, partition=date(2026, 10, 8))
        assert [c.body.parent.name for c in only.captures] == ["08"]
        assert scan_dataset(dataset, registry, partition=date(2026, 10, 9)).captures == ()


def _as_dump(meta: dict[str, Any]) -> None:
    """Reshape a test sidecar as the dump leg writes one: no CKAN file stamp."""
    meta["request_params"]["url_type"] = "datastore"
    meta["request_params"]["ckan_last_modified"] = ""
    meta["request_params"]["resource_filename"] = meta["request_params"]["resource_id"]


class TestDatastoreUsableRule:
    """T-D3-2 (ADR-035 P-6): a dump never needs ``last_modified`` to be usable."""

    def test_d3_2_strict_scan_admits_a_null_last_modified_dump(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        """Detects a dump capture hidden from coverage and the dedup basis (red: E6, strict 0)."""
        _body, sidecar = _alpha_capture(tmp_path / "bronze")
        edit_sidecar(sidecar, _as_dump)
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.unusable == ()
        (capture,) = scan.captures
        assert capture.url_type == "datastore"
        assert capture.ckan_last_modified is None

    def test_d3_2_upload_with_unparseable_last_modified_stays_unusable(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        """Detects the widening leaking into uploads (A's strict rule must hold)."""
        _body, sidecar = _alpha_capture(tmp_path / "bronze")
        edit_sidecar(sidecar, _set_params("ckan_last_modified", "not-a-time"))
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.captures == ()
        assert "provenance_for" in scan.unusable[0].reason

    def test_d3_2_dump_missing_resource_name_is_unusable(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        """Detects the identity form being skipped for a dump."""
        _body, sidecar = _alpha_capture(tmp_path / "bronze")
        edit_sidecar(sidecar, _as_dump)
        edit_sidecar(sidecar, _drop_param("resource_name"))
        scan = scan_dataset(tmp_path / "bronze" / "alpha_series", registry)
        assert scan.captures == ()
        assert "resource_name" in scan.unusable[0].reason

    def test_d3_2_upload_capture_carries_its_url_type(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        """Detects ``Capture.url_type`` not being populated from the sidecar."""
        _alpha_capture(tmp_path / "bronze")
        (capture,) = scan_dataset(tmp_path / "bronze" / "alpha_series", registry).captures
        assert capture.url_type == "upload"
