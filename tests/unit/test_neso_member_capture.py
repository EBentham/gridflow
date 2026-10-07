"""Member capture in the NESO connector (ADR-033 P-5..P-8; M-1..M-9, N-1).

The tests consume ``iter_members`` and publish each capture with
``BronzeWriter.publish_capture`` exactly as the runner's member branch does,
against a synthetic registry installed through P-1's seam (M-8 and M-9 use the
real registry's legacy families).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
from _neso_registry_support import (
    edit_sidecar,
    family,
    install_registry,
    package,
    resource,
    write_registry,
)

from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.base import MemberCaptureConnector, MemberEvent
from gridflow.connectors.neso_data_portal import client as client_module
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.connectors.neso_data_portal.client import (
    _VALIDATED_MARKER,
    NesoDataPortalConnector,
    NesoEmptyResourceError,
    NesoResourceSelectionError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

BASE_URL = "https://api.neso.energy"
FILE_HOST = "https://files.neso-mock.example"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal"
PKG = "aaaaaaaa-0000-4000-8000-000000000000"
EN_DASH_NAME = "Impact on BSUoS due to COVID-19 low demands – Excluding new services"
FFFD_NAME = "Alpha � Series"
ORPS_NAME = "Reactive Default Payment Rate - Oct - 2026"


def _rid(n: int) -> str:
    return f"aaaaaaaa-0000-4000-8000-{n:012d}"


LM = "2026-10-06T10:00:00.000001"
CSV_BODY = b"DATE,VALUE\n2026-10-06,1\n"


def _resource_url(rid: str, filename: str) -> str:
    return f"{BASE_URL}/dataset/{PKG}/resource/{rid}/download/{filename}"


def _live(
    n: int,
    name: str,
    fmt: str = "CSV",
    filename: str = "file.csv",
    *,
    url_type: str = "upload",
    last_modified: str = LM,
) -> dict[str, Any]:
    rid = _rid(n)
    return {
        "id": rid,
        "name": name,
        "format": fmt,
        "url_type": url_type,
        "last_modified": last_modified,
        "url": _resource_url(rid, filename)
        if url_type == "upload"
        else f"{BASE_URL}/datastore/dump/{rid}",
    }


def _payload(resources: list[dict[str, Any]], slug: str = "pkg-alpha") -> dict[str, Any]:
    return {"success": True, "result": {"id": PKG, "name": slug, "resources": resources}}


@pytest.fixture(autouse=True)
def _release_pacers() -> Iterator[None]:
    yield
    pacer_module.reset_shared_pacers()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


@pytest.fixture
def data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("m")


def _registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_registry(
        tmp_path / "registry",
        [
            package(
                "pkg-alpha",
                PKG,
                [
                    family("alpha_series"),
                    family("alpha_notes"),
                    family("alpha_register", archetype="REG", empty_allowed=True),
                    family("alpha_files", kind="files"),
                ],
                [
                    resource(_rid(1), FFFD_NAME, "alpha_series"),
                    resource(_rid(2), EN_DASH_NAME, "alpha_series"),
                    resource(_rid(3), ORPS_NAME, "alpha_notes"),
                    resource(_rid(4), ORPS_NAME, "alpha_files", fmt="PDF"),
                    resource(_rid(5), "Alpha Register", "alpha_register"),
                    resource(_rid(6), "Alpha Workbook", "alpha_files", fmt="XLSX"),
                    resource(
                        _rid(7),
                        "Alpha Map",
                        "alpha_files",
                        fmt="GEOJSON",
                        disposition={"kind": "GIS"},
                    ),
                    resource(
                        _rid(8),
                        "Alpha Zipped",
                        "alpha_series",
                        disposition={"kind": "HOLD", "reason": "zip", "unit": "X-R"},
                    ),
                    resource(_rid(9), "Alpha Macro", "alpha_files", fmt="XLSX"),
                    resource(_rid(10), "Alpha Live Dump", "alpha_notes", url_type="datastore"),
                ],
            )
        ],
    )
    install_registry(monkeypatch, tmp_path / "registry")


def _config(*datasets: str) -> SourceConfig:
    return SourceConfig(
        base_url=BASE_URL,
        rate_limit_per_second=1000,
        timeout=30,
        datasets={d: DatasetConfig(endpoint="/api/3/action/package_show") for d in datasets},
    )


def _window() -> tuple[datetime, datetime]:
    end = datetime.now(UTC) - timedelta(minutes=1)
    return end - timedelta(hours=1), end


def _wire(
    router: respx.MockRouter,
    payload: dict[str, Any],
    bodies: dict[str, bytes],
    *,
    hop: bool = False,
) -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/api/3/action/package_show" in url:
            return httpx.Response(200, json=payload)
        if url.startswith(FILE_HOST):
            return httpx.Response(200, content=bodies[url.removeprefix(FILE_HOST + "/")])
        for item in payload["result"]["resources"]:
            if url == item["url"]:
                if hop:
                    return httpx.Response(302, headers={"location": f"{FILE_HOST}/{item['id']}"})
                return httpx.Response(200, content=bodies[item["id"]])
        return httpx.Response(404)

    router.route(url__regex=r".*").mock(side_effect=_handler)


def _consume(
    data_dir: Path, dataset: str, config: SourceConfig | None = None
) -> tuple[list[MemberEvent], list[Path]]:
    start, end = _window()
    events: list[MemberEvent] = []
    published: list[Path] = []
    writer = BronzeWriter(data_dir)

    async def _run() -> None:
        connector = NesoDataPortalConnector(config or _config(dataset))
        connector.bind_data_dir(data_dir)
        async with connector:
            async for event in connector.iter_members(dataset, start, end):
                if event.outcome == "captured":
                    assert event.response is not None and event.extension is not None
                    published.append(
                        writer.publish_capture(event.response, extension=event.extension)
                    )
                events.append(event)

    asyncio.run(_run())
    return events, published


def _meta(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.with_suffix(".meta.json").read_text(encoding="utf-8"))
    return loaded


def _present(events: list[MemberEvent]) -> list[str]:
    """Outcomes other than ``absent`` (the synthetic families list unserved members)."""
    return [e.outcome for e in events if e.outcome != "absent"]


def _file_leg_calls(router: respx.MockRouter) -> list[str]:
    return [
        str(call.request.url)
        for call in router.calls
        if "/api/3/action/" not in str(call.request.url)
    ]


class TestContract:
    def test_the_connector_satisfies_the_member_capture_protocol(self) -> None:
        connector = NesoDataPortalConnector(_config("daily_wind_availability"))
        assert isinstance(connector, MemberCaptureConnector)


class TestTwoFamilies:
    """M-1: each family fetches every member once; one sidecar per member."""

    def test_two_families_one_package(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        live = [
            _live(1, FFFD_NAME, filename="a1.csv"),
            _live(2, EN_DASH_NAME, filename="a2.csv"),
            _live(3, ORPS_NAME, filename="orps.csv"),
        ]
        bodies = {_rid(n): CSV_BODY + str(n).encode() + b",2\n" for n in (1, 2, 3)}
        _wire(router, _payload(live), bodies)

        series, series_paths = _consume(data_dir, "alpha_series")
        notes, notes_paths = _consume(data_dir, "alpha_notes")

        assert [e.outcome for e in series] == ["absent", "captured", "captured"]
        assert [e.outcome for e in notes] == ["absent", "captured"]
        downloads = _file_leg_calls(router)
        assert sorted(downloads) == sorted(item["url"] for item in live)
        for path, item in zip([*series_paths, *notes_paths], live, strict=True):
            meta = _meta(path)
            params = meta["request_params"]
            assert params["resource_id"] == item["id"]
            assert params["resource_name"] == item["name"]
            assert params["package"] == "pkg-alpha"
            assert params["package_id"] == PKG
            assert params["ckan_last_modified"] == item["last_modified"]
            assert params["ckan_format"] == "CSV"
            assert params["resource_filename"] == item["url"].rsplit("/", 1)[-1]
            assert params["body_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
            assert params["capture_family"] == path.parts[-5]
            assert item["id"] in path.name
        sidecars = sorted(data_dir.rglob("*.meta.json"))
        assert len(sidecars) == 3


class TestSelection:
    """M-2: exact (name, format) selection, absent, ambiguous, unassigned."""

    def test_fffd_en_dash_and_the_orps_duplicate_by_format(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        live = [
            _live(1, FFFD_NAME),
            _live(2, EN_DASH_NAME),
            _live(3, ORPS_NAME),
            _live(4, ORPS_NAME, "PDF", "orps.pdf"),
        ]
        bodies = {_rid(n): CSV_BODY for n in (1, 2, 3)}
        bodies[_rid(4)] = b"%PDF-1.7 synthetic"
        _wire(router, _payload(live), bodies)

        series, _ = _consume(data_dir, "alpha_series")
        assert {e.resource_id for e in series if e.outcome == "captured"} == {_rid(1), _rid(2)}
        notes, _ = _consume(data_dir, "alpha_notes")
        assert [e.resource_id for e in notes if e.outcome == "captured"] == [_rid(3)]
        assert all(_rid(4) not in url for url in _file_leg_calls(router)[:3])

    def test_a_listed_member_missing_live_is_absent_and_warned(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(router, _payload([_live(1, FFFD_NAME)]), {_rid(1): CSV_BODY})
        with caplog.at_level(logging.WARNING):
            events, _ = _consume(data_dir, "alpha_series")
        absent = [e for e in events if e.outcome == "absent"]
        assert {e.resource_id for e in absent} == {_rid(2), _rid(8)}
        assert any(EN_DASH_NAME in r.getMessage() for r in caplog.records)

    def test_zero_members_is_a_selection_error(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(router, _payload([_live(3, ORPS_NAME)]), {})
        with pytest.raises(NesoResourceSelectionError, match="no live resource"):
            _consume(data_dir, "alpha_series")

    def test_two_live_matches_for_one_listed_member_is_an_error(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        twin = _live(11, FFFD_NAME, filename="twin.csv")
        _wire(router, _payload([_live(1, FFFD_NAME), twin]), {})
        with pytest.raises(NesoResourceSelectionError, match="more than one"):
            _consume(data_dir, "alpha_series")
        assert _file_leg_calls(router) == []

    def test_whitespace_and_case_are_not_normalised(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(
            router,
            _payload([_live(1, " " + FFFD_NAME), _live(2, EN_DASH_NAME.lower())]),
            {},
        )
        with pytest.raises(NesoResourceSelectionError, match="no live resource"):
            _consume(data_dir, "alpha_series")

    def test_an_unassigned_live_resource_warns_once(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        monkeypatch.setattr(client_module, "_WARNED_UNASSIGNED", set())
        stranger = _live(99, "A Brand New Table", filename="new.csv")
        _wire(router, _payload([_live(1, FFFD_NAME), stranger]), {_rid(1): CSV_BODY})
        with caplog.at_level(logging.WARNING):
            _consume(data_dir, "alpha_series")
            _consume(data_dir, "alpha_series")
        unassigned = [r for r in caplog.records if "unassigned" in r.getMessage()]
        assert len(unassigned) == 1, [r.getMessage() for r in unassigned]
        assert _rid(99) in unassigned[0].getMessage()
        assert all(_rid(99) not in url for url in _file_leg_calls(router))


class TestUnchangedSkip:
    """M-3 (A5): the skip key is the newest usable capture's last_modified."""

    def test_second_run_makes_no_file_leg_request(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(
            router,
            _payload([_live(1, FFFD_NAME), _live(2, EN_DASH_NAME)]),
            {_rid(1): CSV_BODY, _rid(2): CSV_BODY},
        )
        first, _ = _consume(data_dir, "alpha_series")
        assert _present(first) == ["captured", "captured"]
        seen = len(router.calls)

        second, published = _consume(data_dir, "alpha_series")
        assert _present(second) == ["unchanged", "unchanged"]
        assert published == []
        later = [str(c.request.url) for c in router.calls][seen:]
        assert len(later) == 1 and "package_show" in later[0], later

    def test_two_captures_only_the_newest_is_compared(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        older, newer = "2026-10-01T00:00:00.000001", "2026-10-05T00:00:00.000001"
        _wire(router, _payload([_live(1, FFFD_NAME, last_modified=older)]), {_rid(1): b"A\n1\n"})
        _consume(data_dir, "alpha_series")
        _wire(router, _payload([_live(1, FFFD_NAME, last_modified=newer)]), {_rid(1): b"A\n2\n"})
        assert _present(_consume(data_dir, "alpha_series")[0]) == ["captured"]

        # Live last_modified equals the OLDER capture's: fetched (only the newest counts).
        _wire(router, _payload([_live(1, FFFD_NAME, last_modified=older)]), {_rid(1): b"A\n3\n"})
        assert _present(_consume(data_dir, "alpha_series")[0]) == ["captured"]
        # Live equals the newest capture's: skipped.
        assert _present(_consume(data_dir, "alpha_series")[0]) == ["unchanged"]

    def test_never_captured_is_fetched(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(router, _payload([_live(1, FFFD_NAME)]), {_rid(1): CSV_BODY})
        assert _present(_consume(data_dir, "alpha_series")[0]) == ["captured"]

    def test_negative_control_an_unusable_newest_capture_is_never_a_skip_basis(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(router, _payload([_live(1, FFFD_NAME)]), {_rid(1): CSV_BODY})
        _events, (path,) = _consume(data_dir, "alpha_series")
        edit_sidecar(
            path.with_suffix(".meta.json"), lambda meta: meta["request_params"].pop("resource_name")
        )
        # Different bytes so the refetch cannot collide with the first capture's
        # name inside the same second (a real rerun is seconds apart).
        _wire(router, _payload([_live(1, FFFD_NAME)]), {_rid(1): CSV_BODY + b"2026-10-07,2\n"})
        seen = len(router.calls)
        events, _ = _consume(data_dir, "alpha_series")
        assert _present(events) == ["captured"]
        later = [str(c.request.url) for c in router.calls][seen:]
        assert [url for url in later if "/api/3/action/" not in url] == [_live(1, FFFD_NAME)["url"]]


class TestAdmission:
    """M-4 (A7): admission by signature; extension from filename or format."""

    def test_extensions_by_signature_and_filename(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        geojson = json.dumps({"type": "FeatureCollection", "features": []}).encode()
        pk = b"PK\x03\x04" + b"\x00" * 64
        live = [
            _live(6, "Alpha Workbook", "XLSX", "book.xlsx"),
            _live(7, "Alpha Map", "GEOJSON", "map.geojson"),
            _live(9, "Alpha Macro", "XLSX", "macro.xlsm"),
            _live(4, ORPS_NAME, "PDF", "orps.pdf"),
        ]
        bodies = {_rid(6): pk, _rid(7): geojson, _rid(9): pk, _rid(4): b"<html>blocked</html>"}
        _wire(router, _payload(live), bodies)
        events, paths = _consume(data_dir, "alpha_files")
        by_id = {e.resource_id: e for e in events}
        assert by_id[_rid(6)].extension == "xlsx"
        assert by_id[_rid(7)].extension == "geojson"
        assert by_id[_rid(9)].extension == "xlsm"
        assert by_id[_rid(4)].outcome == "failed"
        assert "NesoUnexpectedBodyError" in by_id[_rid(4)].detail
        assert sorted(p.suffix for p in paths) == [".geojson", ".xlsm", ".xlsx"]
        assert _meta(paths[0])["content_type"] != "text/csv"

    def test_csv_declared_zip_body_lands_as_zip(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        live = [_live(8, "Alpha Zipped", "CSV", "freq.zip"), _live(1, FFFD_NAME)]
        _wire(router, _payload(live), {_rid(8): b"PK\x03\x04zipdata", _rid(1): CSV_BODY})
        events, paths = _consume(data_dir, "alpha_series")
        assert {e.resource_id: e.extension for e in events if e.outcome == "captured"} == {
            _rid(8): "zip",
            _rid(1): "csv",
        }
        assert {p.suffix for p in paths} == {".zip", ".csv"}

    @pytest.mark.parametrize(
        ("body", "fmt"),
        [
            (b'{"success": false}', "CSV"),
            (b"\xef\xbb\xbf  <!DOCTYPE html>", "CSV"),
            (b"%PDF-1.4", "CSV"),
            (b"PK\x03\x04", "PDF"),
        ],
    )
    def test_refused_bodies(self, body: bytes, fmt: str) -> None:
        with pytest.raises(client_module.NesoUnexpectedBodyError):
            client_module._admit_member_body(
                body, declared_format=fmt, filename="x.csv", empty_allowed=True, label="t"
            )

    def test_zero_byte_body_is_refused_for_every_format(self) -> None:
        for fmt in ("CSV", "PDF", "XLSX", "TXT"):
            with pytest.raises(NesoEmptyResourceError):
                client_module._admit_member_body(
                    b"", declared_format=fmt, filename="x", empty_allowed=True, label="t"
                )

    def test_encoding_is_not_checked(self) -> None:
        cp1252 = "Name,Value\nCafé,1\n".encode("cp1252")
        assert client_module._admit_member_body(
            cp1252, declared_format="CSV", filename="x.csv", empty_allowed=False, label="t"
        ) == ("csv", False)


class TestEmptyCapture:
    """M-5 (A6): header-only bodies."""

    def test_header_only_in_an_empty_allowed_family_is_captured_and_marked(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(
            router,
            _payload([_live(5, "Alpha Register")]),
            {_rid(5): b'\xef\xbb\xbf"A","B"\r\n,,\r\n'},
        )
        events, (path,) = _consume(data_dir, "alpha_register")
        assert [e.outcome for e in events] == ["captured"]
        assert _meta(path)["request_params"]["empty_capture"] is True
        assert events[0].response is not None and events[0].response.record_count is None

    def test_header_only_in_a_non_empty_family_fails(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        _wire(router, _payload([_live(1, FFFD_NAME)]), {_rid(1): b"A,B\n"})
        events, paths = _consume(data_dir, "alpha_series")
        assert _present(events) == ["failed"]
        assert "NesoEmptyResourceError" in events[-1].detail
        assert paths == []

    def test_a_data_row_is_not_empty(self) -> None:
        assert client_module._admit_member_body(
            b"A,B\n1,\n", declared_format="CSV", filename="x.csv", empty_allowed=False, label="t"
        ) == ("csv", False)

    def test_legacy_header_only_still_raises_through_fetch(self, router: respx.MockRouter) -> None:
        payload = json.loads((FIXTURES / "package_show_daily_wind_availability.json").read_text())
        url = payload["result"]["resources"][0]["url"]
        router.get(url__startswith=f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json=payload)
        )
        router.get(url__startswith=url).mock(
            return_value=httpx.Response(200, content=b"BMU_ID,Date,MW\n")
        )
        start, end = _window()

        async def _run() -> None:
            async with NesoDataPortalConnector(_config("daily_wind_availability")) as connector:
                await connector.fetch("daily_wind_availability", start, end)

        with pytest.raises(NesoEmptyResourceError):
            asyncio.run(_run())

    def test_legacy_header_only_through_the_member_path_fails_with_the_same_error(
        self, router: respx.MockRouter, data_dir: Path
    ) -> None:
        payload = json.loads((FIXTURES / "package_show_daily_wind_availability.json").read_text())
        url = payload["result"]["resources"][0]["url"]
        router.get(url__startswith=f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json=payload)
        )
        router.get(url__startswith=url).mock(
            return_value=httpx.Response(200, content=b"BMU_ID,Date,MW\n")
        )
        events, paths = _consume(data_dir, "daily_wind_availability")
        assert [e.outcome for e in events] == ["failed"]
        assert events[0].detail.startswith("NesoEmptyResourceError")
        assert paths == []


class TestFailuresAndDeferral:
    """M-6 (continue past a failed member) and M-7 (datastore deferred)."""

    def test_a_failed_member_does_not_stop_the_family(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        live = [_live(1, FFFD_NAME), _live(2, EN_DASH_NAME)]

        def _handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "package_show" in url:
                return httpx.Response(200, json=_payload(live))
            if _rid(1) in url:
                return httpx.Response(404)
            return httpx.Response(200, content=CSV_BODY)

        router.route(url__regex=r".*").mock(side_effect=_handler)
        events, paths = _consume(data_dir, "alpha_series")
        assert [(e.resource_id, e.outcome) for e in events if e.outcome != "absent"] == [
            (_rid(1), "failed"),
            (_rid(2), "captured"),
        ]
        assert "HTTP 404" in events[1].detail
        assert "X-Amz" not in events[1].detail
        assert len(paths) == 1

    def test_datastore_member_is_deferred_with_no_request(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        live = [_live(10, "Alpha Live Dump", url_type="datastore")]
        _wire(router, _payload(live), {})
        events, paths = _consume(data_dir, "alpha_notes")
        assert [(e.resource_id, e.outcome) for e in events] == [
            (_rid(3), "absent"),
            (_rid(10), "deferred"),
        ]
        assert paths == []
        assert _file_leg_calls(router) == []


_LEGACY = [
    (
        "daily_wind_availability",
        "package_show_daily_wind_availability.json",
        "daily_wind_availability.csv",
    ),
    (
        "historic_generation_mix",
        "package_show_historic_generation_mix.json",
        "historic_generation_mix.csv",
    ),
    (
        "embedded_wind_solar_forecast",
        "package_show_embedded_wind_and_solar_forecasts.json",
        "embedded_forecast.csv",
    ),
]


class TestLegacyByteEquivalence:
    """M-8 (A10): the member path reproduces master's fetch() for the legacy keys."""

    @pytest.mark.parametrize(("dataset", "payload_file", "body_file"), _LEGACY)
    def test_member_path_equals_fetch(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path_factory: pytest.TempPathFactory,
        dataset: str,
        payload_file: str,
        body_file: str,
    ) -> None:
        payload = json.loads((FIXTURES / payload_file).read_text(encoding="utf-8"))
        selected = next(
            r for r in payload["result"]["resources"] if r["name"] == _legacy_name(dataset)
        )
        body = (FIXTURES / body_file).read_bytes()
        presigned = f"{FILE_HOST}/{dataset}.csv?X-Amz-Signature=abc"

        def _handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "package_show" in url:
                return httpx.Response(200, json=payload)
            if url == selected["url"]:
                return httpx.Response(302, headers={"location": presigned})
            if url.startswith(FILE_HOST):
                return httpx.Response(200, content=body)
            return httpx.Response(404)

        router.route(url__regex=r".*").mock(side_effect=_handler)
        start, end = _window()

        async def _fetch() -> Any:
            async with NesoDataPortalConnector(_config(dataset)) as connector:
                return await connector.fetch(dataset, start, end)

        (legacy_response,) = asyncio.run(_fetch())
        legacy_dir = tmp_path_factory.mktemp("legacy")
        legacy_path = BronzeWriter(legacy_dir).write(legacy_response)
        legacy_requests = [(c.request.method, str(c.request.url)) for c in router.calls]
        seen = len(router.calls)

        events, (member_path,) = _consume(data_dir, dataset)
        member_requests = [(c.request.method, str(c.request.url)) for c in router.calls][seen:]

        assert [e.outcome for e in events] == ["captured"]
        assert member_requests == legacy_requests
        assert member_path.read_bytes() == legacy_path.read_bytes() == body
        legacy_meta, member_meta = _meta(legacy_path), _meta(member_path)
        assert list(member_meta) == list(legacy_meta)
        for key in (
            "source",
            "dataset",
            "data_date",
            "request_url",
            "api_version",
            "http_status",
            "content_type",
            "body_sha256",
            "body_size_bytes",
            "page",
            "total_pages",
        ):
            assert member_meta[key] == legacy_meta[key], key
        d12 = set(legacy_meta["request_params"])
        assert d12 == {
            "package",
            "package_id",
            "resource_id",
            "resource_name",
            "resource_filename",
            "ckan_last_modified",
            "ckan_format",
            "body_sha256",
        }
        for key in d12:
            assert member_meta["request_params"][key] == legacy_meta["request_params"][key], key
        assert set(member_meta["request_params"]) - d12 == {
            "capture_family",
            "url_type",
            "empty_capture",
            "declared_content_length",
        }
        rid = selected["id"]
        sha8 = hashlib.sha256(body).hexdigest()[:8]
        assert re.fullmatch(rf"raw_\d{{8}}T\d{{6}}Z_{sha8}\.csv", legacy_path.name)
        assert re.fullmatch(rf"raw_\d{{8}}T\d{{6}}Z_{rid}_{sha8}\.csv", member_path.name)
        stamp = re.compile(r"^raw_\d{8}T\d{6}Z_")
        assert stamp.sub("", member_path.name).replace(f"{rid}_", "", 1) == stamp.sub(
            "", legacy_path.name
        )
        assert member_path.parent.relative_to(data_dir) == legacy_path.parent.relative_to(
            legacy_dir
        )


def _legacy_name(dataset: str) -> str:
    from gridflow.connectors.neso_data_portal.endpoints import DATASETS

    return DATASETS[dataset].resource_name


class TestFetchGuard:
    """M-9: fetch() serves the legacy keys only."""

    def test_fetch_refuses_a_non_legacy_key(self) -> None:
        start, end = _window()

        async def _run() -> None:
            async with NesoDataPortalConnector(_config("nordpool_da_prices")) as connector:
                await connector.fetch("nordpool_da_prices", start, end)

        with pytest.raises(ValueError, match="iter_members"):
            asyncio.run(_run())

    def test_fetch_still_serves_a_legacy_key(self, router: respx.MockRouter) -> None:
        payload = json.loads((FIXTURES / "package_show_daily_wind_availability.json").read_text())
        url = payload["result"]["resources"][0]["url"]
        router.get(url__startswith=f"{BASE_URL}/api/3/action/package_show").mock(
            return_value=httpx.Response(200, json=payload)
        )
        router.get(url__startswith=url).mock(
            return_value=httpx.Response(
                200, content=(FIXTURES / "daily_wind_availability.csv").read_bytes()
            )
        )
        start, end = _window()

        async def _run() -> list[Any]:
            async with NesoDataPortalConnector(_config("daily_wind_availability")) as connector:
                return await connector.fetch("daily_wind_availability", start, end)

        assert len(asyncio.run(_run())) == 1


class TestTransportMarker:
    """N-1 (A10, D-39): every member-path request carries a fresh, consumed token."""

    def test_member_path_requests_are_all_attested(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _registry(tmp_path, monkeypatch)
        live = [
            _live(6, "Alpha Workbook", "XLSX", "book.xlsx"),
            _live(7, "Alpha Map", "GEOJSON", "map.geojson"),
        ]
        bodies = {
            _rid(6): b"PK\x03\x04" + b"\x00" * 32,
            _rid(7): json.dumps({"type": "Feature"}).encode(),
        }
        _wire(router, _payload(live), bodies, hop=True)
        start, end = _window()
        sink: list[NesoDataPortalConnector] = []

        async def _run() -> None:
            connector = NesoDataPortalConnector(_config("alpha_files"))
            connector.bind_data_dir(data_dir)
            sink.append(connector)
            async with connector:
                async for _event in connector.iter_members("alpha_files", start, end):
                    pass

        asyncio.run(_run())
        issued = set(sink[0]._issued_send_tokens)
        kinds: dict[str, int] = {}
        for call in router.calls:
            request = call.request
            token = request.extensions.get(_VALIDATED_MARKER)
            assert token in issued, f"unattested request reached the transport: {request.url}"
            issued.remove(token)
            url = str(request.url)
            kind = (
                "package_show"
                if "package_show" in url
                else "redirect_hop"
                if url.startswith(FILE_HOST)
                else "resource_url"
            )
            kinds[kind] = kinds.get(kind, 0) + 1
        assert issued == set()
        assert kinds == {"package_show": 1, "resource_url": 2, "redirect_hop": 2}, kinds
