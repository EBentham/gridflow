"""Container registry rules, the committed inventories and sibling-fed ingest (ADR-037).

Rows T-X2-9 (V-15, V-15b, V-16, P-10) and T-X2-11 (P-8's inventories) of the
unit X test matrix. Assertions over the committed registry run in a fresh
interpreter, as unit A's registry tests do.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
from _container_support import forbid_zipfile_reads  # noqa: F401 - a fixture
from _neso_registry_support import (
    family,
    install_registry,
    package,
    record,
    resource,
    write_registry,
)

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
from gridflow.connectors.neso_data_portal.registry import RegistryError, load_registry

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads", "stub_neso_resolver")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PKG = "eeeeeeee-0000-4000-8000-000000000000"
BASE_URL = "https://api.neso.energy"
XLSX = {"header_row": 1, "columns": "A:D"}


def _run(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=180,
        check=False,
    )


def _assert_ok(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "OK" in result.stdout, result.stdout


def _child(name: str, key: str | None = None) -> dict[str, Any]:
    disposition = {"kind": "SILVER", "key": key} if key else {"kind": "DOC"}
    return {"child": name, "disposition": disposition}


def _load(
    tmp_path: Path,
    rec: dict[str, Any],
    resources: list[dict[str, Any]],
) -> None:
    families = [
        family("box_table", record=rec),
        family("box_files", kind="files", archetype="FILE"),
    ]
    load_registry(write_registry(tmp_path / "reg", [package("pkg-box", PKG, families, resources)]))


def _xlsx_record() -> dict[str, Any]:
    return record(reader="xlsx", xlsx=XLSX, siblings=("box_files",))


def _member_record(inner: str = "csv") -> dict[str, Any]:
    spec = {"member_pattern": r"data/[a-z]+\.(csv|xlsx)", "inner": inner}
    extra = {"xlsx": XLSX} if inner == "xlsx" else {}
    return record(reader="zip_member", zip_member=spec, siblings=("box_files",), **extra)


def _container(
    children: list[dict[str, Any]], own: dict[str, Any] | None = None, *, fmt: str = "XLSX"
) -> dict[str, Any]:
    return resource(
        "eeeeeeee-0000-4000-8000-000000000001",
        "Box",
        "box_files",
        fmt=fmt,
        disposition=own or {"kind": "SILVER", "key": "box_table"},
        children=children,
    )


class TestV15:
    """T-X2-9: a SILVER target's reader must fit the body's shape (V-15, V-15b)."""

    def test_controls_load(self, tmp_path: Path) -> None:
        _load(
            tmp_path / "a", _xlsx_record(), [_container([_child("S1", "box_table"), _child("N")])]
        )
        members = [_child("data/a.csv", "box_table"), _child("readme.txt")]
        _load(tmp_path / "b", _member_record(), [_container(members, fmt="ZIP")])
        sheets = [_child("data/a.xlsx::S", "box_table")]
        _load(tmp_path / "c", _member_record("xlsx"), [_container(sheets, fmt="ZIP")])
        held = {"kind": "HOLD", "reason": "r", "unit": "U"}
        _load(tmp_path / "d", record(), [_container([_child("S1")], held)])

    def _refused(self, tmp_path: Path, rule: str, rec: dict[str, Any], res: dict[str, Any]) -> str:
        with pytest.raises(RegistryError) as info:
            _load(tmp_path, rec, [res])
        message = str(info.value)
        assert f"{rule}:" in message, message
        return message

    def test_csv_reader_on_a_child(self, tmp_path: Path) -> None:
        rec = record(siblings=("box_files",))
        res = _container([_child("S1", "box_table")], {"kind": "HOLD", "reason": "r", "unit": "U"})
        assert "csv reader" in self._refused(tmp_path, "V-15", rec, res)

    def test_container_reader_on_a_childless_resource(self, tmp_path: Path) -> None:
        res = resource(
            "eeeeeeee-0000-4000-8000-000000000001",
            "Box",
            "box_files",
            fmt="XLSX",
            disposition={"kind": "SILVER", "key": "box_table"},
        )
        assert "childless" in self._refused(tmp_path, "V-15", _xlsx_record(), res)

    def test_member_outside_the_pattern(self, tmp_path: Path) -> None:
        res = _container([_child("other/a.csv", "box_table")], fmt="ZIP")
        assert "does not match" in self._refused(tmp_path, "V-15", _member_record(), res)

    def test_sheet_child_under_inner_csv(self, tmp_path: Path) -> None:
        res = _container([_child("data/a.xlsx::S", "box_table")], fmt="ZIP")
        self._refused(tmp_path, "V-15", _member_record(), res)

    def test_member_child_under_inner_xlsx(self, tmp_path: Path) -> None:
        res = _container([_child("data/a.xlsx", "box_table")], fmt="ZIP")
        self._refused(tmp_path, "V-15", _member_record("xlsx"), res)

    def test_xlsx_child_naming_a_member(self, tmp_path: Path) -> None:
        res = _container([_child("m.xlsx::S", "box_table")])
        assert "names a sheet" in self._refused(tmp_path, "V-15", _xlsx_record(), res)

    def test_resource_level_silver_differs_from_a_child_target(self, tmp_path: Path) -> None:
        families = [
            family("box_table", record=_xlsx_record()),
            family("box_other", record=_xlsx_record()),
            family("box_files", kind="files", archetype="FILE"),
        ]
        res = _container([_child("S1", "box_table"), _child("S2", "box_other")])
        with pytest.raises(RegistryError, match="V-15b:"):
            load_registry(
                write_registry(tmp_path / "reg", [package("pkg-box", PKG, families, [res])])
            )


class TestCommittedRegistry:
    """T-X2-9 (V-16) and T-X2-11 over the committed registry."""

    def test_v16_every_recorded_sibling_fed_family_lists_a_sibling(self) -> None:
        _assert_ok(
            _run(
                """
                from gridflow.connectors.neso_data_portal.registry import load_registry
                registry = load_registry()
                owners = {resource.family for _p, resource in registry.resources.values()}
                fed = sorted(
                    key for key, (_p, family) in registry.families.items()
                    if key not in owners
                )
                assert fed == ['current_bsuos_cap_adjustments',
                               'embedded_forecast_archive_dump',
                               'embedded_forecast_archive_upload',
                               'ffr_phase2_result_summary_archive'], fed
                for key in fed:
                    record = registry.families[key][1].record
                    if record is not None:
                        assert record.siblings, key
                print('OK')
                """
            )
        )

    def test_x2_11_inventories_and_dispositions(self) -> None:
        _assert_ok(
            _run(
                """
                import re
                from collections import Counter
                from gridflow.connectors.neso_data_portal.registry import (
                    KEY_PATTERN, HoldDisposition, load_registry,
                )
                registry = load_registry()
                resources = [r for _p, r in registry.resources.values()]
                kinds = Counter(r.disposition.kind for r in resources)
                assert dict(kinds) == {'SILVER': 1248, 'HOLD': 74, 'DOC': 43, 'GIS': 20}, kinds
                with_children = [r for r in resources if r.children]
                assert len(with_children) == 4 + 1 + 39 + 73 + 1, len(with_children)
                allowed = {'system_frequency', 'thermal_constraint_costs'}
                for r in resources:
                    for d in (r.disposition, *(c.disposition for c in r.children)):
                        if isinstance(d, HoldDisposition):
                            assert d.unit != 'X-R', r.id
                            for key in re.findall(r'proposed family (\\S+?):', d.reason):
                                assert KEY_PATTERN.fullmatch(key), key
                                assert key not in registry.families or key in allowed, key
                cmp = [r for r in resources if r.disposition.kind == 'SILVER'
                       and r.disposition.key == 'current_bsuos_cap_adjustments']
                assert len(cmp) == 4
                for r in cmp:
                    assert [c.disposition.kind for c in r.children] == ['SILVER', 'DOC'], r.id
                print('OK')
                """
            )
        )


def _sibling_fed_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rec = record(reader="xlsx", xlsx=XLSX, siblings=("box_files",))
    families = [family("box_table", record=rec), family("box_files", kind="files")]
    res = _container([_child("S1", "box_table")])
    install_registry(
        monkeypatch, write_registry(tmp_path / "reg", [package("pkg-box", PKG, families, [res])])
    )


@pytest.fixture(autouse=True)
def _release_pacers() -> Iterator[None]:
    yield
    pacer_module.reset_shared_pacers()


class TestSiblingFedIngest:
    """T-X2-9 (P-10): a family with no own resources fetches nothing and succeeds."""

    @staticmethod
    def _window() -> tuple[datetime, datetime]:
        end = datetime.now(UTC) - timedelta(minutes=1)
        return end - timedelta(hours=1), end

    def test_iter_members_makes_no_request(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _sibling_fed_registry(tmp_path, monkeypatch)
        start, end = self._window()
        config = SourceConfig(
            base_url=BASE_URL,
            rate_limit_per_second=1000,
            timeout=30,
            datasets={"box_table": DatasetConfig(endpoint="/api/3/action/package_show")},
        )

        async def _run() -> list[Any]:
            connector = NesoDataPortalConnector(config)
            connector.bind_data_dir(tmp_path / "d")
            async with connector:
                return [event async for event in connector.iter_members("box_table", start, end)]

        with respx.mock(assert_all_called=False) as router:
            router.route(url__regex=r".*").mock(return_value=httpx.Response(500))
            with caplog.at_level("INFO"):
                events = asyncio.run(_run())
            assert events == []
            assert router.calls.call_count == 0
        assert "box_table: sibling-fed family, no own resources; nothing to fetch" in caplog.text

    def test_runner_records_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from _neso_registry_support import ingest_context

        from gridflow.pipeline import runner as pipeline_runner

        # Generation runs at import: import it under the real registry first, so the
        # tmp registry below never leaks into the process-wide silver registry.
        pipeline_runner.import_transformers()
        _sibling_fed_registry(tmp_path, monkeypatch)
        start, end = self._window()
        with (
            respx.mock(assert_all_called=False) as router,
            ingest_context(tmp_path / "d", monkeypatch) as ctx,
        ):
            router.route(url__regex=r".*").mock(return_value=httpx.Response(500))
            (result,) = pipeline_runner.run_ingest(
                ctx, "neso_data_portal", ["box_table"], start, end
            )
            assert router.calls.call_count == 0
        assert result.status == "success", result
