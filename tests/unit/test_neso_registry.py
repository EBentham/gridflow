"""NESO registry tests (ADR-033, PLAN R-1..R-10).

Registry contents and import wiring are asserted **out of process**: pytest
collection has already imported the connector and the transformers, so an
in-process check could pass on state something else populated. Each assertion
block runs in a fresh ``sys.executable`` interpreter (the
``test_neso_data_portal_registration.py`` idiom), and each positive proof is
paired with a negative control that shows the same check failing on a
deliberately broken registry.

The snapshot of record lives under the git-ignored ``.planning`` tree, so the
tests read the committed, trimmed fixture derived from it.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_DIR = PROJECT_ROOT / "src" / "gridflow" / "connectors" / "neso_data_portal" / "registry"
FIXTURE = (
    PROJECT_ROOT
    / "tests"
    / "fixtures"
    / "neso_data_portal"
    / "catalog_snapshot_20261006T195819Z_trimmed.json"
)

# E1 (PLAN §2): the snapshot of record's tallies.
FORMAT_TALLY = {
    "CSV": 1244,
    "XLSX": 50,
    "PDF": 29,
    "XLSM": 23,
    "ZIP": 19,
    "PNG": 7,
    "GEOJSON": 5,
    "DOC": 3,
    "TXT": 2,
    "GPKG": 2,
    "PPT": 1,
}
URL_TYPE_TALLY = {"upload": 1233, "datastore": 152}
# P-2: CSV 1244 = SILVER 1205 + 39 CSV-declared ZIP bodies held for X-R;
# XLSX 50 + XLSM 23 + ZIP 19 + 39 = HOLD 131; PDF/PNG/DOC/PPT/TXT = DOC 42.
DISPOSITION_TALLY = {"SILVER": 1205, "HOLD": 131, "DOC": 42, "GIS": 7}


def _run(code: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run ``code`` in a fresh interpreter with the project on its path."""
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code), *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )


def _assert_ok(result: subprocess.CompletedProcess[str]) -> str:
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "OK" in result.stdout, result.stdout
    return result.stdout


def _copy_registry(tmp_path: Path) -> Path:
    target = tmp_path / "registry"
    target.mkdir()
    for item in REGISTRY_DIR.glob("*.json"):
        shutil.copyfile(item, target / item.name)
    return target


def _write_package(directory: Path, document: dict[str, object]) -> None:
    (directory / f"{document['package']}.json").write_text(
        json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _synthetic_package(**overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "package": "synthetic-package",
        "package_id": "00000000-0000-4000-8000-000000000000",
        "group": "synthetic",
        "archetype": "SER",
        "refresh": "daily",
        "eligibility": {"status": "eligible"},
        "families": [
            {
                "key": "synthetic_family",
                "kind": "tabular",
                "legacy": False,
                "archetype": "SER",
                "refresh": "daily",
                "empty_allowed": False,
                "max_download_bytes": 1024,
                "name_regex": None,
                "transformer": None,
            }
        ],
        "resources": [
            {
                "id": "00000000-0000-4000-8000-000000000001",
                "name": "Synthetic Resource",
                "format": "CSV",
                "url_type": "upload",
                "family": "synthetic_family",
                "disposition": {"kind": "SILVER", "key": "synthetic_family"},
            }
        ],
    }
    document.update(overrides)
    return document


class TestRegistryLoads:
    """R-1: the package data loads, ids are unique, ledgers are not packages."""

    def test_package_data_loads_and_ledgers_are_not_parsed_as_packages(self) -> None:
        _assert_ok(
            _run(
                """
                from gridflow.connectors.neso_data_portal.registry import (
                    load_adjudications, load_frozen_keys, load_registry,
                )
                registry = load_registry()
                assert registry.root is None
                assert len(registry.packages) == 131, len(registry.packages)
                assert len(registry.resources) == 1385, len(registry.resources)
                slugs = {p.package for p in registry.packages}
                assert not any(s.startswith('_') for s in slugs), slugs
                assert load_frozen_keys(), 'the ledger must carry the legacy keys'
                assert load_adjudications() == ()
                print('OK')
                """
            )
        )

    def test_a_resource_id_declared_twice_is_refused(self, tmp_path: Path) -> None:
        directory = tmp_path / "registry"
        directory.mkdir()
        _write_package(directory, _synthetic_package())
        twin = _synthetic_package(package="synthetic-twin")
        twin["families"] = [dict(f, key="synthetic_twin") for f in twin["families"]]  # type: ignore[attr-defined]
        twin["resources"] = [
            dict(
                r, family="synthetic_twin", disposition={"kind": "SILVER", "key": "synthetic_twin"}
            )
            for r in twin["resources"]  # type: ignore[attr-defined]
        ]
        _write_package(directory, twin)
        result = _run(
            """
            import sys
            from pathlib import Path
            from gridflow.connectors.neso_data_portal.registry import RegistryError, load_registry
            try:
                load_registry(Path(sys.argv[1]))
            except RegistryError as exc:
                assert 'also declared' in str(exc), exc
                print('OK')
            else:
                raise AssertionError('a duplicated resource id loaded')
            """,
            str(directory),
        )
        _assert_ok(result)

    def test_an_underscore_ledger_would_fail_if_parsed_as_a_package(self, tmp_path: Path) -> None:
        """Negative control for the skip: the same list renamed without ``_`` fails."""
        directory = tmp_path / "registry"
        directory.mkdir()
        _write_package(directory, _synthetic_package())
        (directory / "frozen_keys.json").write_text("[]\n", encoding="utf-8")
        result = _run(
            """
            import sys
            from pathlib import Path
            from gridflow.connectors.neso_data_portal.registry import RegistryError, load_registry
            try:
                load_registry(Path(sys.argv[1]))
            except RegistryError:
                print('OK')
            else:
                raise AssertionError('a non-underscore list file was not parsed as a package')
            """,
            str(directory),
        )
        _assert_ok(result)


class TestDispositionCoverage:
    """R-2: 1,385/1,385 resources, exactly one disposition each, E1 tallies."""

    def test_fixture_ids_equal_registry_ids_with_e1_tallies(self) -> None:
        _assert_ok(
            _run(
                f"""
                import collections, json, sys
                from gridflow.connectors.neso_data_portal.registry import load_registry
                fixture = json.loads(open(sys.argv[1], encoding='utf-8').read())
                registry = load_registry()
                fixture_ids = {{
                    r['id'] for p in fixture['packages'] for r in p['resources']
                }}
                assert len(fixture_ids) == 1385, len(fixture_ids)
                assert fixture_ids == set(registry.resources), (
                    sorted(fixture_ids ^ set(registry.resources))[:5]
                )
                formats = collections.Counter(
                    r.format for _p, r in registry.resources.values()
                )
                assert dict(formats) == {FORMAT_TALLY!r}, formats
                url_types = collections.Counter(
                    r.url_type for _p, r in registry.resources.values()
                )
                assert dict(url_types) == {URL_TYPE_TALLY!r}, url_types
                kinds = collections.Counter(
                    r.disposition.kind for _p, r in registry.resources.values()
                )
                assert dict(kinds) == {DISPOSITION_TALLY!r}, kinds
                for package in fixture['packages']:
                    for resource in package['resources']:
                        entry_package, entry = registry.resources[resource['id']]
                        assert entry_package.package == package['name']
                        assert entry_package.package_id == package['id']
                        assert entry.name == resource['name']
                        assert entry.format == resource['format'].upper()
                        assert entry.url_type == resource['url_type']
                print('OK')
                """,
                str(FIXTURE),
            )
        )


class TestKeys:
    """R-4: keys valid, unique, and disjoint from every registered silver dataset."""

    _CHECK = """
        import sys
        from pathlib import Path
        from gridflow.connectors.neso_data_portal.registry import (
            KEY_PATTERN, key_collisions, load_registry,
        )
        from gridflow.pipeline.runner import import_transformers
        from gridflow.silver.registry import list_transformers
        import_transformers()
        registry = load_registry(Path(sys.argv[1]) if len(sys.argv) > 1 else None)
        bad = [k for k in registry.families if not KEY_PATTERN.fullmatch(k)]
        assert not bad, bad
        problems = key_collisions(registry, list_transformers())
        if problems:
            print('COLLIDES', problems)
        else:
            print('OK')
        """

    def test_every_key_is_valid_and_disjoint(self) -> None:
        _assert_ok(_run(self._CHECK))

    def test_negative_control_a_key_named_after_another_sources_dataset(
        self, tmp_path: Path
    ) -> None:
        directory = tmp_path / "registry"
        directory.mkdir()
        package = _synthetic_package()
        package["families"] = [dict(f, key="system_prices") for f in package["families"]]  # type: ignore[attr-defined]
        package["resources"] = [
            dict(r, family="system_prices", disposition={"kind": "SILVER", "key": "system_prices"})
            for r in package["resources"]  # type: ignore[attr-defined]
        ]
        _write_package(directory, package)
        result = _run(self._CHECK, str(directory))
        assert result.returncode == 0, result.stderr
        assert "COLLIDES" in result.stdout and "system_prices" in result.stdout, result.stdout


class TestOwnerFacts:
    """R-5: the owner-ruled facts A1 requires the registry to carry."""

    def test_owner_facts(self) -> None:
        _assert_ok(
            _run(
                """
                from gridflow.connectors.neso_data_portal.registry import load_registry
                registry = load_registry()
                packages = {p.package: p for p in registry.packages}

                nordpool = packages['day-ahead-power-exchange-prices-nordpool']
                assert nordpool.eligibility.status == 'eligible', nordpool.eligibility
                n2ex = [r for r in nordpool.resources if r.name == ' N2EX GB Day-Ahead Price']
                assert len(n2ex) == 1, [r.name for r in nordpool.resources]
                assert n2ex[0].disposition.kind == 'SILVER', n2ex[0].disposition
                assert (' N2EX GB Day-Ahead Price', n2ex[0].format) in registry.family_names(
                    n2ex[0].family
                )

                for slug in ('levelised-cost-of-green-hydrogen',
                             'gis-boundaries-for-gb-dno-license-areas'):
                    assert packages[slug].eligibility.status == 'held', slug
                    assert packages[slug].eligibility.unit == 'N', slug
                held = sorted(p.package for p in registry.packages
                              if p.eligibility.status == 'held')
                assert held == ['gis-boundaries-for-gb-dno-license-areas',
                                'levelised-cost-of-green-hydrogen'], held

                embedded = packages['embedded-wind-and-solar-forecasts']
                archive = [r for r in embedded.resources
                           if r.family == 'embedded_wind_solar_forecast_archive']
                assert len(archive) == 8, [r.name for r in archive]
                assert sorted(r.url_type for r in archive).count('upload') == 7
                assert sorted(r.url_type for r in archive).count('datastore') == 1
                live = [r for r in embedded.resources
                        if r.family == 'embedded_wind_solar_forecast']
                assert [r.name for r in live] == ['Embedded Solar and Wind Forecast'], live
                _p, family = registry.families['embedded_wind_solar_forecast_archive']
                assert family.transformer is None and not family.legacy

                for _p, resource in registry.resources.values():
                    if resource.format in {'XLSX', 'XLSM', 'ZIP'}:
                        assert resource.disposition.kind == 'HOLD', resource
                        assert resource.disposition.unit == 'X-R', resource
                csv_zip = [
                    r for _p, r in registry.resources.values()
                    if r.format == 'CSV' and r.disposition.kind == 'HOLD'
                ]
                assert len(csv_zip) == 39, len(csv_zip)
                assert {registry.resources[r.id][0].package for r in csv_zip} == {
                    'system-frequency-data'
                }
                assert all(r.disposition.unit == 'X-R' for r in csv_zip)
                print('OK')
                """
            )
        )


class TestFreezeLedger:
    """R-6: every ``_frozen_keys.json`` row exists with the same package (CI pin)."""

    _CHECK = """
        import sys
        from pathlib import Path
        from gridflow.connectors.neso_data_portal.registry import (
            frozen_key_violations, load_frozen_keys, load_registry,
        )
        path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
        frozen = load_frozen_keys(path)
        keys = sorted(row.key for row in frozen)
        assert {'daily_wind_availability', 'embedded_wind_solar_forecast',
                'historic_generation_mix'} <= set(keys), keys
        problems = frozen_key_violations(load_registry(path), frozen)
        print('VIOLATIONS' if problems else 'OK', problems)
        """

    def test_the_committed_ledger_is_honoured(self) -> None:
        _assert_ok(_run(self._CHECK))

    def test_negative_control_a_removed_frozen_key_fails(self, tmp_path: Path) -> None:
        directory = _copy_registry(tmp_path)
        (directory / "historic-generation-mix.json").unlink()
        result = _run(self._CHECK, str(directory))
        assert result.returncode == 0, result.stderr
        assert "VIOLATIONS" in result.stdout, result.stdout
        assert "historic_generation_mix" in result.stdout, result.stdout


class TestValidation:
    """R-7: load-time validation rejects each malformed shape."""

    _LOAD = """
        import sys
        from pathlib import Path
        from gridflow.connectors.neso_data_portal.registry import RegistryError, load_registry
        try:
            load_registry(Path(sys.argv[1]))
        except RegistryError as exc:
            print('REFUSED', exc)
        else:
            print('OK')
        """

    def _load(self, tmp_path: Path, document: dict[str, object]) -> str:
        directory = tmp_path / "registry"
        directory.mkdir()
        _write_package(directory, document)
        result = _run(self._LOAD, str(directory))
        assert result.returncode == 0, result.stderr
        return result.stdout

    def test_positive_control_the_synthetic_package_loads(self, tmp_path: Path) -> None:
        assert self._load(tmp_path, _synthetic_package()).startswith("OK")

    def test_silver_key_differing_from_its_family(self, tmp_path: Path) -> None:
        package = _synthetic_package()
        package["resources"][0]["disposition"] = {"kind": "SILVER", "key": "other_key"}  # type: ignore[index]
        assert "REFUSED" in self._load(tmp_path, package)

    def test_unknown_family(self, tmp_path: Path) -> None:
        package = _synthetic_package()
        package["resources"][0]["family"] = "not_declared"  # type: ignore[index]
        assert "REFUSED" in self._load(tmp_path, package)

    def test_duplicate_name_and_format(self, tmp_path: Path) -> None:
        package = _synthetic_package()
        twin = dict(package["resources"][0], id="00000000-0000-4000-8000-000000000002")  # type: ignore[index]
        package["resources"] = [package["resources"][0], twin]  # type: ignore[index]
        assert "REFUSED" in self._load(tmp_path, package)

    def test_same_name_in_another_format_is_allowed(self, tmp_path: Path) -> None:
        """The ORPS shape (E5): one name, two formats, two resources."""
        package = _synthetic_package()
        package["families"].append(  # type: ignore[attr-defined]
            dict(package["families"][0], key="synthetic_files", kind="files")  # type: ignore[index]
        )
        pdf = dict(
            package["resources"][0],  # type: ignore[index]
            id="00000000-0000-4000-8000-000000000002",
            format="PDF",
            family="synthetic_files",
            disposition={"kind": "DOC"},
        )
        package["resources"] = [package["resources"][0], pdf]  # type: ignore[index]
        assert self._load(tmp_path, package).startswith("OK")

    def test_bad_regex(self, tmp_path: Path) -> None:
        package = _synthetic_package()
        package["families"][0]["name_regex"] = "([unclosed"  # type: ignore[index]
        assert "REFUSED" in self._load(tmp_path, package)

    def test_invalid_key(self, tmp_path: Path) -> None:
        package = _synthetic_package()
        package["families"][0]["key"] = "Bad-Key"  # type: ignore[index]
        package["resources"][0]["family"] = "Bad-Key"  # type: ignore[index]
        package["resources"][0]["disposition"] = {"kind": "SILVER", "key": "Bad-Key"}  # type: ignore[index]
        assert "REFUSED" in self._load(tmp_path, package)


class TestVerbatimNames:
    """R-8: U+2013 and whitespace-edged names survive byte-exact (C-4)."""

    def test_en_dash_and_whitespace_names_are_byte_exact(self) -> None:
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        dashed: list[tuple[str, str]] = []
        edged: list[tuple[str, str]] = []
        for package in fixture["packages"]:
            for resource in package["resources"]:
                name = resource["name"]
                assert "�" not in name, name
                if "–" in name:
                    dashed.append((package["name"], name))
                if name != name.strip():
                    edged.append((package["name"], name))
        assert len(dashed) == 54, len(dashed)
        assert len(edged) == 71, len(edged)
        for slug, name in dashed + edged:
            text = (REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8")
            encoded = json.dumps(name, ensure_ascii=False)
            assert f'"name": {encoded}' in text, (slug, name)


@pytest.mark.parametrize("name", [" N2EX GB Day-Ahead Price"])
def test_leading_space_name_round_trips_through_the_loader(name: str) -> None:
    """The loader hands back the stored name unstripped (P-6 compares as stored)."""
    result = _run(
        """
        import sys
        from gridflow.connectors.neso_data_portal.registry import load_registry
        names = {r.name for _p, r in load_registry().resources.values()}
        assert sys.argv[1] in names, repr(sys.argv[1])
        assert sys.argv[1].strip() not in names
        print('OK')
        """,
        name,
    )
    _assert_ok(result)
