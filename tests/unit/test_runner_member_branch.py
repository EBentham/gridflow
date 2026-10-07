"""``run_ingest``'s member branch (ADR-033 P-5, P-10 accounting; U-1..U-7, M-6).

Driven through the real ``run_ingest`` against a tmp DuckDB catalogue and data
dir, with HTTP mocked by respx and a synthetic registry installed through
P-1's seam.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
from _neso_registry_support import (
    family,
    ingest_context,
    install_registry,
    package,
    resource,
    write_registry,
)

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.neso_data_portal import coverage
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.observability import read_watermark
from gridflow.pipeline import runner as pipeline_runner
from gridflow.pipeline.runner import DatasetResult

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

SOURCE = "neso_data_portal"
BASE_URL = "https://api.neso.energy"
PKG = "aaaaaaaa-0000-4000-8000-000000000000"
LM = "2026-10-06T10:00:00.000001"
CSV_BODY = b"DATE,VALUE\n2026-10-06,1\n"


def _rid(n: int) -> str:
    return f"aaaaaaaa-0000-4000-8000-{n:012d}"


def _live(n: int, name: str, *, url_type: str = "upload", lm: str = LM) -> dict[str, Any]:
    rid = _rid(n)
    url = (
        f"{BASE_URL}/dataset/{PKG}/resource/{rid}/download/f{n}.csv"
        if url_type == "upload"
        else f"{BASE_URL}/datastore/dump/{rid}"
    )
    return {
        "id": rid,
        "name": name,
        "format": "CSV",
        "url_type": url_type,
        "last_modified": lm,
        "url": url,
    }


def _registry_packages(
    *, with_beta: bool = True, with_register: bool = False
) -> list[dict[str, Any]]:
    families = [family("alpha_series"), family("alpha_dump")]
    resources = [
        resource(_rid(1), "Alpha One", "alpha_series"),
        resource(_rid(2), "Alpha Two", "alpha_series"),
        resource(_rid(3), "Alpha Dump", "alpha_dump", url_type="datastore"),
    ]
    if with_beta:
        families.append(family("alpha_beta"))
        resources.append(resource(_rid(4), "Alpha Beta", "alpha_beta"))
    if with_register:
        families.append(family("alpha_register", archetype="REG", empty_allowed=True))
        resources.append(resource(_rid(5), "Alpha Register", "alpha_register"))
    return [package("pkg-alpha", PKG, families, resources)]


LEGACY_LEDGER = [
    {"key": "daily_wind_availability", "package": "daily-wind-availability"},
    {"key": "embedded_wind_solar_forecast", "package": "embedded-wind-and-solar-forecasts"},
    {"key": "historic_generation_mix", "package": "historic-generation-mix"},
]


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
    return tmp_path_factory.mktemp("u")


@pytest.fixture
def registry_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = write_registry(tmp_path / "registry", _registry_packages(), frozen=LEGACY_LEDGER)
    install_registry(monkeypatch, directory)
    return directory


def _wire(router: respx.MockRouter, live: list[dict[str, Any]], bodies: dict[str, Any]) -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "package_show" in url:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "result": {"id": PKG, "name": "pkg-alpha", "resources": live},
                },
            )
        for item in live:
            if url == item["url"]:
                body = bodies.get(item["id"])
                if body is None:
                    return httpx.Response(404)
                return httpx.Response(200, content=body)
        return httpx.Response(404)

    router.route(url__regex=r".*").mock(side_effect=_handler)


def _ingest(
    data_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    datasets: list[str],
    *,
    end: datetime | None = None,
    incremental: bool = False,
) -> tuple[list[DatasetResult], dict[str, Any]]:
    end_dt = end or (datetime.now(UTC) - timedelta(minutes=2))
    with ingest_context(data_dir, monkeypatch) as ctx:
        fast = ctx.settings.sources[SOURCE].model_copy(update={"rate_limit_per_second": 1000})
        ctx.settings.sources[SOURCE] = fast
        results = pipeline_runner.run_ingest(
            ctx, SOURCE, datasets, end_dt - timedelta(hours=1), end_dt, incremental=incremental
        )
        marks = {ds: read_watermark(ctx.con, SOURCE, ds) for ds in datasets}
    return results, marks


class TestUntouchedPath:
    """U-1: a non-member connector still goes through fetch() + write()."""

    def test_fake_connector_uses_fetch_and_write(
        self, data_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        response = RawResponse(
            body=b'{"data": [1]}',
            content_type="application/json",
            source="elexon",
            dataset="fuelhh",
            fetched_at=datetime(2026, 10, 7, 9, 0, 0, tzinfo=UTC),
            data_date=date(2026, 10, 7),
        )
        calls: list[str] = []

        class _Fake:
            last_skipped_units = 0

            def __init__(self, config: Any) -> None:
                self.config = config

            async def __aenter__(self) -> _Fake:
                return self

            async def __aexit__(self, *exc: Any) -> None:
                return None

            async def fetch(
                self, dataset: str, start: datetime, end: datetime
            ) -> list[RawResponse]:
                calls.append("fetch")
                return [response]

        written: list[Path] = []
        real_write = BronzeWriter.write

        def _spy_write(self: BronzeWriter, resp: RawResponse) -> Path:
            path = real_write(self, resp)
            written.append(path)
            return path

        def _no_publish(self: BronzeWriter, *args: Any, **kwargs: Any) -> Path:
            raise AssertionError("publish_capture reached from the legacy branch")

        monkeypatch.setattr(BronzeWriter, "write", _spy_write)
        monkeypatch.setattr(BronzeWriter, "publish_capture", _no_publish)
        monkeypatch.setattr(
            "gridflow.connectors.registry.get_connector", lambda source, config: _Fake(config)
        )
        monkeypatch.setenv("ELEXON_API_KEY", "test-key")
        end = datetime.now(UTC) - timedelta(minutes=2)
        with ingest_context(data_dir, monkeypatch) as ctx:
            results = pipeline_runner.run_ingest(
                ctx, "elexon", ["fuelhh"], end - timedelta(hours=1), end
            )
        assert [r.status for r in results] == ["success"]
        assert results[0].members_unchanged == 0
        assert calls == ["fetch"]
        sha8 = hashlib.sha256(response.body).hexdigest()[:8]
        assert written == [
            data_dir
            / "bronze"
            / "elexon"
            / "fuelhh"
            / "2026"
            / "10"
            / "07"
            / f"raw_20261007T090000Z_{sha8}.json"
        ]
        meta = json.loads(written[0].with_suffix(".meta.json").read_text(encoding="utf-8"))
        assert list(meta) == [
            "source",
            "dataset",
            "fetched_at",
            "written_at",
            "data_date",
            "request_url",
            "request_params",
            "api_version",
            "http_status",
            "content_type",
            "body_sha256",
            "body_size_bytes",
            "page",
            "total_pages",
        ]


class TestAccounting:
    """U-2, U-3, U-4, M-6: outcomes, status and the frontier."""

    def test_u4_captured_and_clean_advances_the_watermark(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        live = [_live(1, "Alpha One"), _live(2, "Alpha Two")]
        _wire(router, live, {_rid(1): CSV_BODY, _rid(2): CSV_BODY + b"x,2\n"})
        end = datetime.now(UTC) - timedelta(minutes=2)
        (result,), marks = _ingest(data_dir, monkeypatch, ["alpha_series"], end=end)
        assert (result.status, result.rows_in, result.rows_out, result.rows_skipped) == (
            "success",
            2,
            2,
            0,
        )
        assert marks["alpha_series"].value == end.replace(microsecond=end.microsecond)
        assert len(list(data_dir.rglob("raw_*.meta.json"))) == 2

    @pytest.mark.parametrize("incremental", [False, True])
    def test_u2_all_unchanged_is_success_with_no_advance(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        incremental: bool,
    ) -> None:
        live = [_live(1, "Alpha One"), _live(2, "Alpha Two")]
        _wire(router, live, {_rid(1): CSV_BODY, _rid(2): CSV_BODY + b"x,2\n"})
        first_end = datetime.now(UTC) - timedelta(minutes=10)
        _results, marks = _ingest(data_dir, monkeypatch, ["alpha_series"], end=first_end)
        first_mark = marks["alpha_series"].value
        assert first_mark is not None

        with caplog.at_level(logging.INFO):
            (result,), marks = _ingest(
                data_dir, monkeypatch, ["alpha_series"], incremental=incremental
            )
        assert result.status == "success"
        assert (result.rows_in, result.rows_out, result.members_unchanged) == (0, 0, 2)
        assert marks["alpha_series"].value == first_mark, "an all-unchanged run held the frontier"
        assert (
            "neso_data_portal/alpha_series: all 2 member(s) unchanged since their newest "
            "capture; nothing captured; frontier unchanged"
        ) in [r.getMessage() for r in caplog.records]

    def test_u3_partial_capture_with_a_failed_member(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire(router, [_live(1, "Alpha One"), _live(2, "Alpha Two")], {_rid(2): CSV_BODY})
        (result,), marks = _ingest(data_dir, monkeypatch, ["alpha_series"])
        assert (result.status, result.rows_in, result.rows_skipped) == (
            "completed_with_warnings",
            1,
            1,
        )
        assert marks["alpha_series"].value is None

    def test_u3_all_deferred_warns_without_raising(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire(router, [_live(3, "Alpha Dump", url_type="datastore")], {})
        (result,), marks = _ingest(data_dir, monkeypatch, ["alpha_dump"])
        assert (result.status, result.rows_in, result.rows_skipped) == (
            "completed_with_warnings",
            0,
            1,
        )
        assert result.error is None
        assert marks["alpha_dump"].value is None
        assert [
            str(c.request.url) for c in router.calls if "package_show" not in str(c.request.url)
        ] == []

    def test_m6_every_member_failed_fails_the_dataset(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire(router, [_live(1, "Alpha One"), _live(2, "Alpha Two")], {})
        (result,), marks = _ingest(data_dir, monkeypatch, ["alpha_series"])
        assert result.status == "failed"
        assert result.error is not None and "all 2 attempted member(s) failed" in result.error
        assert marks["alpha_series"].value is None


class TestEmptyCaptureAccounting:
    """RULINGS 480: header-only reaches the runner as a capture only where empty is allowed.

    A register that becomes empty is real state (ROADMAP row 28): its header-only
    capture is evidence and the frontier advances. Every other family refuses
    the body before the runner sees a capture (A6).
    """

    @pytest.fixture
    def register_registry(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        directory = write_registry(
            tmp_path / "registry",
            _registry_packages(with_register=True),
            frozen=LEGACY_LEDGER,
        )
        install_registry(monkeypatch, directory)
        return directory

    @pytest.mark.parametrize("body", [b"DATE,VALUE\n", b'"DA\nTE",VALUE\r\n,\r\n'])
    def test_header_only_in_a_non_empty_family_never_reaches_the_runner_as_a_capture(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        register_registry: Path,
        monkeypatch: pytest.MonkeyPatch,
        body: bytes,
    ) -> None:
        _wire(router, [_live(1, "Alpha One")], {_rid(1): body})
        captured: list[RawResponse] = []
        real_publish = BronzeWriter.publish_capture

        def _spy(self: BronzeWriter, response: RawResponse, *, extension: str) -> Path:
            captured.append(response)
            return real_publish(self, response, extension=extension)

        monkeypatch.setattr(BronzeWriter, "publish_capture", _spy)
        (result,), marks = _ingest(data_dir, monkeypatch, ["alpha_series"])
        assert captured == [], "a header-only body reached the runner as a capture"
        assert (result.status, result.rows_in) == ("failed", 0)
        assert result.error is not None
        assert marks["alpha_series"].value is None
        assert list(data_dir.rglob("raw_*")) == []

    def test_header_only_register_is_evidence_and_advances_the_frontier(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        register_registry: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire(router, [_live(5, "Alpha Register")], {_rid(5): b'"REG\nID",NAME\r\n'})
        end = datetime.now(UTC) - timedelta(minutes=2)
        (result,), marks = _ingest(data_dir, monkeypatch, ["alpha_register"], end=end)
        assert (result.status, result.rows_in, result.rows_out) == ("success", 1, 1)
        assert marks["alpha_register"].value == end
        (sidecar,) = data_dir.rglob("raw_*.meta.json")
        meta = json.loads(sidecar.read_text(encoding="utf-8"))
        assert meta["request_params"]["empty_capture"] is True


class TestEcho:
    """U-5: the unchanged clause appears only when non-zero."""

    def test_echo_lines(self, capsys: pytest.CaptureFixture[str]) -> None:
        from gridflow.cli import _echo_ingest_results

        _echo_ingest_results(
            "elexon",
            [
                DatasetResult("elexon", "fuelhh", "ingest", "success", rows_in=3, rows_out=3),
                DatasetResult(
                    "elexon",
                    "freq",
                    "ingest",
                    "completed_with_warnings",
                    rows_in=2,
                    rows_out=2,
                    rows_skipped=1,
                ),
            ],
        )
        _echo_ingest_results(
            SOURCE,
            [
                DatasetResult(SOURCE, "a", "ingest", "success", members_unchanged=4),
                DatasetResult(
                    SOURCE,
                    "b",
                    "ingest",
                    "completed_with_warnings",
                    rows_in=1,
                    rows_out=1,
                    rows_skipped=2,
                    members_unchanged=3,
                ),
            ],
        )
        assert capsys.readouterr().out.splitlines() == [
            "  elexon/fuelhh: 3 responses ingested",
            "  elexon/freq: 2 responses ingested, 1 unit(s) skipped (completed_with_warnings)",
            "  neso_data_portal/a: 0 responses ingested, 4 unchanged since newest capture",
            "  neso_data_portal/b: 1 responses ingested, 3 unchanged since newest capture, "
            "2 unit(s) skipped (completed_with_warnings)",
        ]


class TestCollision:
    """U-6: a publication collision fails the member and leaves the artifact intact."""

    def test_collision_is_a_failed_member(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire(
            router,
            [_live(1, "Alpha One"), _live(2, "Alpha Two")],
            {_rid(1): CSV_BODY, _rid(2): CSV_BODY},
        )
        real_publish = BronzeWriter.publish_capture
        occupied: list[Path] = []

        def _occupy_then_publish(
            self: BronzeWriter, response: RawResponse, *, extension: str
        ) -> Path:
            if not occupied:
                ts = response.fetched_at.strftime("%Y%m%dT%H%M%SZ")
                sha8 = hashlib.sha256(response.body).hexdigest()[:8]
                rid = response.request_params["resource_id"]
                target = (
                    self._paths.bronze_date_dir(
                        response.source, response.dataset, response.data_date
                    )
                    / f"raw_{ts}_{rid}_{sha8}.{extension}"
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"EXISTING ARTIFACT")
                occupied.append(target)
            return real_publish(self, response, extension=extension)

        monkeypatch.setattr(BronzeWriter, "publish_capture", _occupy_then_publish)
        (result,), _marks = _ingest(data_dir, monkeypatch, ["alpha_series"])
        assert (result.status, result.rows_in, result.rows_skipped) == (
            "completed_with_warnings",
            1,
            1,
        )
        assert occupied[0].read_bytes() == b"EXISTING ARTIFACT"
        assert not occupied[0].with_suffix(".meta.json").exists()


class TestSweepAcrossUnfrozenKeys:
    """U-7: new keys never block a sweep; coverage reports them unfrozen."""

    def test_sweep_binds_and_captures_then_coverage_flags_unfrozen(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        live = [_live(1, "Alpha One"), _live(2, "Alpha Two"), _live(4, "Alpha Beta")]
        _wire(router, live, {_rid(n): CSV_BODY + str(n).encode() + b",1\n" for n in (1, 2, 4)})
        results, _marks = _ingest(data_dir, monkeypatch, ["alpha_series", "alpha_beta"])
        assert [r.status for r in results] == ["success", "success"]

        snapshot = tmp_path / "snapshot.json"
        snapshot.write_text(
            json.dumps(
                {
                    "packages": [
                        {
                            "name": "pkg-alpha",
                            "resources": [{"id": i["id"], "name": i["name"]} for i in live],
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        capsys.readouterr()
        assert coverage.main(["--snapshot", str(snapshot), "--data-dir", str(data_dir)]) == 1
        out = capsys.readouterr().out
        assert "unfrozen: alpha_beta" in out and "unfrozen: alpha_series" in out
        assert "captured 3" in out and "missing 0" in out

    def test_negative_control_a_removed_key_with_bronze_refuses_before_any_send(
        self,
        router: respx.MockRouter,
        data_dir: Path,
        tmp_path: Path,
        registry_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        live = [_live(1, "Alpha One"), _live(2, "Alpha Two"), _live(4, "Alpha Beta")]
        _wire(router, live, {_rid(n): CSV_BODY + str(n).encode() + b",1\n" for n in (1, 2, 4)})
        _ingest(data_dir, monkeypatch, ["alpha_series", "alpha_beta"])
        pacer_module.reset_shared_pacers()

        reduced = tmp_path / "reduced"
        shutil.copytree(registry_dir, reduced)
        write_registry(reduced, _registry_packages(with_beta=False), frozen=LEGACY_LEDGER)
        install_registry(monkeypatch, reduced)
        seen = len(router.calls)
        (result,), _marks = _ingest(data_dir, monkeypatch, ["alpha_series"])
        assert result.status == "failed"
        assert result.error is not None and "alpha_beta" in result.error
        assert "RegistryFreezeError" in result.error or "frozen" in result.error
        assert len(router.calls) == seen, "a send escaped the freeze pin"
