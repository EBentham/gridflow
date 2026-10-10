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

import builtins
import errno
import io
import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

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
# P-2, then unit X's inventories (ADR-037 P-8): the 131 X-R holds became SILVER 44
# (4 CMP381/395 workbooks, 1 phase-2 ResultSummary ZIP, 39 frequency ZIPs), HOLD 73
# (data containers awaiting their batch's record), DOC 1 (Building Heat Model) and
# GIS 13 (shapefile/GeoJSON/GPKG archives).
DISPOSITION_TALLY = {"SILVER": 1248, "HOLD": 74, "DOC": 43, "GIS": 20}


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

    def test_t_kc1_a_recorded_family_registered_under_its_own_source_does_not_collide(
        self, tmp_path: Path
    ) -> None:
        """T-KC1: a generic transformer under ``neso_data_portal`` is not a collision.

        Detects ``key_collisions`` reporting a recorded (non-legacy) family
        whose generated transformer registers under ``neso_data_portal`` by
        design (ADR-034 P-15): on master every non-legacy key collided with
        any registration, its own source included.
        """
        directory = tmp_path / "registry"
        directory.mkdir()
        _write_package(directory, _synthetic_package())
        result = _run(
            """
            import sys
            from pathlib import Path
            from gridflow.connectors.neso_data_portal.registry import (
                key_collisions, load_registry,
            )
            registry = load_registry(Path(sys.argv[1]))
            own = key_collisions(registry, [('neso_data_portal', 'synthetic_family')])
            foreign = key_collisions(registry, [('elexon', 'synthetic_family')])
            assert not own, own
            assert foreign and 'elexon' in foreign[0], foreign
            print('OK')
            """,
            str(directory),
        )
        _assert_ok(result)


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

                # Unit X (ADR-037 P-8) dispositioned every X-R hold and committed
                # the containers' child inventories; no X-R unit survives.
                for _p, resource in registry.resources.values():
                    if resource.format in {'XLSX', 'XLSM', 'ZIP'}:
                        assert resource.disposition.kind in {'SILVER', 'HOLD', 'DOC', 'GIS'}
                    for d in (resource.disposition,
                              *(c.disposition for c in resource.children)):
                        assert getattr(d, 'unit', None) != 'X-R', resource
                csv_zip = [
                    r for _p, r in registry.resources.values()
                    if r.format == 'CSV' and r.children
                ]
                assert len(csv_zip) == 39, len(csv_zip)
                assert {registry.resources[r.id][0].package for r in csv_zip} == {
                    'system-frequency-data'
                }
                assert all(
                    r.disposition.kind == 'SILVER' and r.disposition.key == 'system_frequency'
                    and len(r.children) == 1
                    for r in csv_zip
                )
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

    def test_t_f1_the_ledger_freezes_every_swept_key(self) -> None:
        """T-F1: the 301 swept keys plus the legacy three, sorted, packages honoured.

        Detects a ledger that does not cover the S sweep's bronze (3 rows on
        master), a repeated or unsorted key, or a row whose package disagrees
        with the registry's owner of that key.
        """
        result = _run(
            """
            import json
            from gridflow.connectors.neso_data_portal.registry import (
                load_frozen_keys, load_registry,
            )
            frozen = load_frozen_keys()
            keys = [row.key for row in frozen]
            assert len(keys) == 304, len(keys)
            assert len(set(keys)) == len(keys), 'repeated key'
            assert keys == sorted(keys), 'not sorted by key'
            registry = load_registry()
            wrong = [
                row.key for row in frozen
                if registry.families[row.key][0].package != row.package
            ]
            assert not wrong, wrong
            assert {'daily_wind_availability', 'embedded_wind_solar_forecast',
                    'historic_generation_mix'} <= set(keys)
            print('OK')
            """
        )
        _assert_ok(result)

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


class TestGeneratedDatasets:
    """R-3: the generated legacy ``DATASETS`` equal master's three literals."""

    def test_generated_datasets_equal_masters_literals(self) -> None:
        _assert_ok(
            _run(
                """
                from gridflow.connectors.neso_data_portal.endpoints import DATASETS, CkanDataset
                MIB = 1024 * 1024
                HGM = (
                    "DATETIME", "GAS", "COAL", "NUCLEAR", "WIND", "WIND_EMB", "HYDRO",
                    "IMPORTS", "BIOMASS", "OTHER", "SOLAR", "STORAGE", "GENERATION",
                    "CARBON_INTENSITY", "LOW_CARBON", "ZERO_CARBON", "RENEWABLE", "FOSSIL",
                    "GAS_perc", "COAL_perc", "NUCLEAR_perc", "WIND_perc", "WIND_EMB_perc",
                    "HYDRO_perc", "IMPORTS_perc", "BIOMASS_perc", "OTHER_perc",
                    "SOLAR_perc", "STORAGE_perc", "GENERATION_perc", "LOW_CARBON_perc",
                    "ZERO_CARBON_perc", "RENEWABLE_perc", "FOSSIL_perc",
                )
                master = {
                    "daily_wind_availability": CkanDataset(
                        package="daily-wind-availability",
                        resource_name="Daily Wind Availability",
                        expected_format="CSV",
                        expected_columns=("BMU_ID", "Date", "MW"),
                        max_download_bytes=8 * MIB,
                    ),
                    "historic_generation_mix": CkanDataset(
                        package="historic-generation-mix",
                        resource_name="Historic GB Generation Mix",
                        expected_format="CSV",
                        expected_columns=HGM,
                        max_download_bytes=256 * MIB,
                    ),
                    "embedded_wind_solar_forecast": CkanDataset(
                        package="embedded-wind-and-solar-forecasts",
                        resource_name="Embedded Solar and Wind Forecast",
                        expected_format="CSV",
                        expected_columns=(
                            "DATE_GMT", "TIME_GMT", "SETTLEMENT_DATE", "SETTLEMENT_PERIOD",
                            "EMBEDDED_WIND_FORECAST", "EMBEDDED_WIND_CAPACITY",
                            "EMBEDDED_SOLAR_FORECAST", "EMBEDDED_SOLAR_CAPACITY",
                        ),
                        max_download_bytes=8 * MIB,
                    ),
                }
                assert len(HGM) == 34
                assert list(DATASETS) == list(master), list(DATASETS)
                for key, expected in master.items():
                    assert DATASETS[key] == expected, (key, DATASETS[key])
                print('OK')
                """
            )
        )

    def test_negative_control_a_legacy_family_with_two_members_is_refused(self) -> None:
        _assert_ok(
            _run(
                """
                import dataclasses
                from gridflow.connectors.neso_data_portal import endpoints
                families = dict(endpoints.FAMILIES)
                dwa = families["daily_wind_availability"]
                families["daily_wind_availability"] = dataclasses.replace(
                    dwa, names=dwa.names | {("Daily Wind Availability 2", "CSV")}
                )
                try:
                    endpoints.build_datasets(families)
                except RuntimeError:
                    print('OK')
                else:
                    raise AssertionError('a two-member legacy family generated a CkanDataset')
                """
            )
        )


# Master's three entries, byte-for-byte (P-3): the generated block opens with them.
_LEGACY_YAML_LINES = [
    "      daily_wind_availability:",
    '        endpoint: "/api/3/action/package_show"',
    '        schedule: "daily"',
    "        max_query_days: 1",
    "      historic_generation_mix:",
    '        endpoint: "/api/3/action/package_show"',
    '        schedule: "daily"',
    "        max_query_days: 1",
    "      embedded_wind_solar_forecast:",
    '        endpoint: "/api/3/action/package_show"',
    '        schedule: "daily"',
    "        max_query_days: 1",
]
_GENERATED_SUFFIX = (
    ': {endpoint: "/api/3/action/package_show", schedule: "daily", max_query_days: 1}'
)


def _yaml_check(*extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "gridflow.connectors.neso_data_portal.registry",
            "yaml",
            "--check",
            *extra,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


class TestAgreement:
    """R-9: sources.yaml, FAMILIES and list_datasets() agree with the registry."""

    _SOURCES = PROJECT_ROOT / "config" / "sources.yaml"

    def test_yaml_check_is_clean_on_the_committed_file(self) -> None:
        result = _yaml_check()
        assert result.returncode == 0, result.stdout + result.stderr

    def test_negative_control_a_dropped_generated_line_is_drift(self, tmp_path: Path) -> None:
        text = self._SOURCES.read_bytes().decode("utf-8")
        dropped = "      aahedc_tariffs" + _GENERATED_SUFFIX
        assert dropped in text
        copy = tmp_path / "sources.yaml"
        copy.write_bytes(text.replace(dropped, "", 1).encode("utf-8"))
        result = _yaml_check("--path", str(copy))
        assert result.returncode == 1, result.stdout + result.stderr

    def _drifted_copy(self, tmp_path: Path) -> tuple[Path, bytes]:
        text = self._SOURCES.read_bytes().decode("utf-8")
        dropped = "      aahedc_tariffs" + _GENERATED_SUFFIX
        assert dropped in text
        copy = tmp_path / "sources.yaml"
        drifted = text.replace(dropped, "", 1).encode("utf-8")
        copy.write_bytes(drifted)
        return copy, drifted

    def test_write_regenerates_a_drifted_copy_and_leaves_no_temp(self, tmp_path: Path) -> None:
        from gridflow.connectors.neso_data_portal.registry.__main__ import main

        copy, _drifted = self._drifted_copy(tmp_path)
        assert main(["yaml", "--write", "--path", str(copy)]) == 0
        assert copy.read_bytes() == self._SOURCES.read_bytes()
        assert [p.name for p in tmp_path.iterdir()] == ["sources.yaml"]

    def test_a_failed_write_leaves_the_original_intact(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A disk-full write must not truncate the file that configures every source."""
        from gridflow.connectors.neso_data_portal.registry.__main__ import main

        copy, drifted = self._drifted_copy(tmp_path)
        real_open = io.open

        class _DiskFull:
            """Writes a short prefix, then fails as a full disk would."""

            def __init__(self, handle: Any) -> None:
                self._handle = handle

            def __enter__(self) -> _DiskFull:
                return self

            def __exit__(self, *exc: object) -> None:
                self._handle.close()

            def close(self) -> None:
                self._handle.close()

            def write(self, data: bytes) -> int:
                self._handle.write(data[:64])
                self._handle.flush()
                raise OSError(errno.ENOSPC, "No space left on device")

        def failing_open(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
            handle = real_open(file, mode, *args, **kwargs)
            if (
                "w" in mode
                and isinstance(file, (str, os.PathLike))
                and Path(file).resolve().parent == tmp_path.resolve()
            ):
                return _DiskFull(handle)
            return handle

        monkeypatch.setattr(io, "open", failing_open)
        monkeypatch.setattr(builtins, "open", failing_open)

        with pytest.raises(OSError, match="No space left"):
            main(["yaml", "--write", "--path", str(copy)])

        monkeypatch.undo()
        assert copy.read_bytes() == drifted, (
            f"sources.yaml truncated to {len(copy.read_bytes())} of {len(drifted)} bytes"
        )
        assert [p.name for p in tmp_path.iterdir()] == ["sources.yaml"]

    def test_keys_agree_and_legacy_lines_are_byte_identical(self) -> None:
        lines = self._SOURCES.read_bytes().decode("utf-8").replace("\r\n", "\n").split("\n")
        begin = lines.index("      # >>> generated: neso_data_portal datasets")
        end = lines.index("      # <<< generated")
        # Only the two marker lines are new around master's entries.
        assert lines[begin - 1] == "    datasets:", lines[begin - 1]
        assert lines[begin + 1 : begin + 13] == _LEGACY_YAML_LINES
        generated = lines[begin + 13 : end]
        assert all(line.endswith(_GENERATED_SUFFIX) for line in generated)
        assert generated == sorted(generated)
        assert all(not line.strip() for line in lines[end + 1 :]), lines[end + 1 :]
        _assert_ok(
            _run(
                """
                from gridflow.config.settings import load_settings
                from gridflow.connectors.neso_data_portal.endpoints import FAMILIES
                from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
                config = load_settings().get_source_config('neso_data_portal')
                configured = set(config.datasets)
                assert len(FAMILIES) == 316, len(FAMILIES)
                assert configured == set(FAMILIES), sorted(configured ^ set(FAMILIES))[:5]
                listed = NesoDataPortalConnector(config).list_datasets()
                assert listed == list(FAMILIES)
                print('OK')
                """
            )
        )


class TestRegistrations:
    """R-10: bespoke families are exactly the registered transformers; data ships."""

    def test_registered_transformers_equal_bespoke_families(self) -> None:
        _assert_ok(
            _run(
                """
                from gridflow.connectors.neso_data_portal.registry import load_registry
                from gridflow.pipeline.runner import import_transformers
                from gridflow.silver.registry import list_transformers
                import_transformers()
                registered = {d for _s, d in list_transformers('neso_data_portal')}
                families = load_registry().families
                bespoke = {k for k, (_p, f) in families.items() if f.transformer == 'bespoke'}
                recorded = {k for k, (_p, f) in families.items() if f.record is not None}
                assert registered == bespoke | recorded, (registered, bespoke, recorded)
                assert bespoke == {'daily_wind_availability', 'historic_generation_mix',
                                   'embedded_wind_solar_forecast'}
                print('OK')
                """
            )
        )

    def test_package_data_is_reachable_through_importlib_resources(self) -> None:
        _assert_ok(
            _run(
                """
                from importlib.resources import files
                root = files('gridflow.connectors.neso_data_portal.registry')
                names = {item.name for item in root.iterdir() if item.name.endswith('.json')}
                assert '_frozen_keys.json' in names and '_adjudications.json' in names
                assert '_reconcile_adjudications.json' in names
                assert len([n for n in names if not n.startswith('_')]) == 131, len(names)
                print('OK')
                """
            )
        )


# --------------------------------------------------------------------------- #
# The reconcile adjudication ledger (v0.22-GEN-2H, ADR-040 P-1)
# --------------------------------------------------------------------------- #

_CAPTURE = (
    "bronze/neso_data_portal/fam_one/2026/10/07/raw_20261007T080000000000Z_{rid}_a5666ada.csv"
)
_RID_A = "eeeeeeee-0000-4000-8000-00000000000a"
_RID_B = "eeeeeeee-0000-4000-8000-00000000000b"


def _entry(**overrides: Any) -> dict[str, Any]:
    """A valid ``overlap`` entry over two captures; ``overrides`` break one rule."""
    entry: dict[str, Any] = {
        "family": "fam_one",
        "category": "overlap",
        "captures": [_CAPTURE.format(rid=_RID_A), _CAPTURE.format(rid=_RID_B)],
        "reason": "two archives publish one key",
        "question": "Which archive is authoritative?",
        "evidence": "FACTS g3",
        "ruling": "547",
    }
    entry.update(overrides)
    return entry


def _failed(**overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "category": "failed",
        "cause": "DuplicateEntityKeyError",
        "captures": [_CAPTURE.format(rid=_RID_A)],
    }
    fields.update(overrides)
    return _entry(**fields)


_LOAD_LEDGER = """
    import sys
    from pathlib import Path
    from gridflow.connectors.neso_data_portal.registry import (
        RegistryError, load_reconcile_adjudications,
    )
    try:
        entries = load_reconcile_adjudications(Path(sys.argv[1]))
    except RegistryError as exc:
        print('REFUSED', exc)
    else:
        print('OK', len(entries))
    """

_NON_ADJUDICABLE = [
    "missing",
    "orphaned",
    "missing_or_invalid_output",
    "stale_covered",
    "duplicated",
    "stale_adjudication",
]
_MALFORMED: dict[str, list[dict[str, Any]]] = {
    **{f"category-{c}": [_entry(category=c)] for c in _NON_ADJUDICABLE},
    "failed-without-cause": [_entry(category="failed", captures=[_CAPTURE.format(rid=_RID_A)])],
    "overlap-with-cause": [_entry(cause="DuplicateEntityKeyError")],
    "other-cause": [_failed(cause="ValueError")],
    "compute-error-cause": [_failed(cause="ComputeError")],
    "exception-cause": [_failed(cause="Exception")],
    "overlap-one-capture": [_entry(captures=[_CAPTURE.format(rid=_RID_A)])],
    "no-capture": [_failed(captures=[])],
    "repeated-capture": [_entry(captures=[_CAPTURE.format(rid=_RID_A)] * 2)],
    "wildcard-capture": [_failed(captures=["*"])],
    "dash-capture": [_failed(captures=["-"])],
    "directory-capture": [_failed(captures=["bronze/neso_data_portal/fam_one/2026/10/07/"])],
    "glob-capture": [_failed(captures=["bronze/neso_data_portal/fam_one/2026/10/07/raw_*.csv"])],
    "bad-date": [_failed(captures=[_CAPTURE.format(rid=_RID_A).replace("10/07", "02/30")])],
    "bad-family": [_failed(family="Fam")],
    "empty-reason": [_failed(reason="   ")],
    "multi-line-reason": [_failed(reason="one\ntwo")],
    "tab-question": [_failed(question="one\ttwo")],
    "empty-evidence": [_failed(evidence="")],
    "non-numeric-ruling": [_failed(ruling="R547")],
    "extra-field": [_failed(scope="family")],
    "duplicate-entry": [_failed(), _failed(reason="a second entry for the same capture")],
}


class TestReconcileLedger:
    """P-1: ``_reconcile_adjudications.json`` loads only narrow, explicit entries (H1, H5)."""

    def _load(self, tmp_path: Path, entries: Any) -> str:
        directory = tmp_path / "registry"
        directory.mkdir(exist_ok=True)
        (directory / "_reconcile_adjudications.json").write_text(
            json.dumps(entries, indent=2) + "\n", encoding="utf-8"
        )
        result = _run(_LOAD_LEDGER, str(directory))
        assert result.returncode == 0, result.stderr
        return result.stdout

    def test_valid_entries_load(self, tmp_path: Path) -> None:
        """The positive control every malformed case below breaks one rule of; both stamp
        forms (seconds and microseconds, E15) are accepted, and so is the invalid-encoding
        cause (``UnicodeDecodeError``, ADR-040 §Amendment 1)."""
        seconds = _CAPTURE.format(rid=_RID_B).replace("T080000000000Z", "T080000Z")
        entries = [
            _entry(),
            _failed(),
            _failed(captures=[seconds]),
            _failed(cause="UnicodeDecodeError", captures=[_CAPTURE.format(rid=_RID_B)]),
        ]
        assert self._load(tmp_path, entries).startswith("OK 4"), entries

    @pytest.mark.parametrize("case", sorted(_MALFORMED))
    def test_a4_non_adjudicable_entries_fail_validation(self, tmp_path: Path, case: str) -> None:
        """A4: detects a ledger that adjudicates a gridflow-owned category, a wildcard or
        directory scope, the overlap check's own ``-`` gap, a cause outside the allowlist (a
        generic ``ComputeError`` or ``Exception`` names no vendor fault), an entry a receipt
        line cannot render, or two entries covering one gap."""
        out = self._load(tmp_path, _MALFORMED[case])
        assert out.startswith("REFUSED"), (case, out)
        assert "_reconcile_adjudications.json" in out, out

    def test_t_reg_1_the_package_ledger_loads_and_matches_the_registry(self) -> None:
        """T-REG-1: detects a committed ledger that does not load, or names a family,
        directory or resource the package registry does not back."""
        _assert_ok(
            _run(
                """
                from gridflow.connectors.neso_data_portal.registry import (
                    load_reconcile_adjudications, load_registry,
                    reconcile_adjudication_problems,
                )
                entries = load_reconcile_adjudications()
                problems = reconcile_adjudication_problems(load_registry(), entries)
                assert problems == [], problems
                print('OK')
                """
            )
        )

    def test_t_reg_2_a_missing_ledger_is_an_error_not_empty(self, tmp_path: Path) -> None:
        """T-REG-2: detects a deleted ledger silently reading as no entries."""
        directory = tmp_path / "registry"
        directory.mkdir()
        result = _run(_LOAD_LEDGER, str(directory))
        assert result.stdout.startswith("REFUSED"), result.stdout
        assert "_reconcile_adjudications.json" in result.stdout

    def test_t_reg_3_entries_the_registry_does_not_back_are_problems(self, tmp_path: Path) -> None:
        """T-REG-3: detects an entry on a family without a record, a capture filed under a
        directory that is neither the family nor a sibling, or a resource of another
        package."""
        from _neso_registry_support import family, package, record, resource, write_registry

        foreign = "ffffffff-0000-4000-8000-00000000000f"
        documents = [
            package(
                "pkg-one",
                "dddddddd-0000-4000-8000-000000000000",
                [family("fam_one", record=record()), family("fam_raw")],
                [
                    resource(_RID_A, "One A", "fam_one"),
                    resource(_RID_B, "One B", "fam_one"),
                    resource("eeeeeeee-0000-4000-8000-00000000000c", "Raw", "fam_raw"),
                ],
            ),
            package(
                "pkg-two",
                "dddddddd-0000-4000-8000-000000000001",
                [family("fam_two", record=record())],
                [resource(foreign, "Two", "fam_two")],
            ),
        ]
        good = _entry()
        entries = [
            good,
            _failed(family="fam_raw", captures=[_CAPTURE.format(rid=_RID_A)]),
            _failed(captures=[_CAPTURE.format(rid=_RID_A).replace("fam_one", "fam_two")]),
            _failed(captures=[_CAPTURE.format(rid=foreign)]),
        ]
        directory = write_registry(
            tmp_path / "registry", documents, reconcile_adjudications=entries
        )
        result = _run(
            """
            import sys
            from pathlib import Path
            from gridflow.connectors.neso_data_portal.registry import (
                load_reconcile_adjudications, load_registry,
                reconcile_adjudication_problems,
            )
            path = Path(sys.argv[1])
            for problem in reconcile_adjudication_problems(
                load_registry(path), load_reconcile_adjudications(path)
            ):
                print('PROBLEM', problem)
            print('DONE')
            """,
            str(directory),
        )
        assert result.returncode == 0, result.stderr
        problems = [line for line in result.stdout.splitlines() if line.startswith("PROBLEM")]
        assert len(problems) == 3, result.stdout
        assert "fam_raw" in problems[0] and "no record" in problems[0], problems
        assert "fam_two" in problems[1] and "directory" in problems[1], problems
        assert foreign in problems[2] and "package" in problems[2], problems
