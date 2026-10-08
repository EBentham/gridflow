"""Run-scoped NESO pacing (ADR-033 P-12, A8; Q-1..Q-7).

Every pacing assertion stamps the send instant inside the transport handler
(the respx side effect), so it measures sends, not ``acquire`` returns. The
fake-clock tests drive :class:`RunPacer` with an injected ``monotonic`` and
``sleep``; Q-3 and Q-6 run real processes against a real OS lock.
"""

from __future__ import annotations

import asyncio
import builtins
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
from _neso_registry_support import ingest_context
from tenacity import wait_none

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.neso_data_portal import catalog_snapshot
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
from gridflow.connectors.neso_data_portal.pacer import (
    DATASTORE_INTERVAL_S,
    Lane,
    NesoPacerBusyError,
    RunPacer,
    shared_pacer,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

BASE_URL = "https://api.neso.energy"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal"
PROC = Path(__file__).resolve().parent / "_neso_pacer_proc.py"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _release_pacers() -> Iterator[None]:
    """Bound pacers hold an OS lock for the process; release them per test."""
    yield
    pacer_module.reset_shared_pacers()


class FakeClock:
    """A monotonic clock and an async sleep that advances it."""

    def __init__(self, start: float = 0.0, oversleep: float = 0.0) -> None:
        self.now = start
        self.oversleep_once = oversleep

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds + self.oversleep_once
        self.oversleep_once = 0.0
        await asyncio.sleep(0)


def _config(
    rate: int = 1000, datasets: tuple[str, ...] = ("daily_wind_availability",)
) -> SourceConfig:
    return SourceConfig(
        base_url=BASE_URL,
        rate_limit_per_second=rate,
        timeout=30,
        datasets={name: DatasetConfig(endpoint="/api/3/action/package_show") for name in datasets},
    )


def _package_show_handler(stamps: list[float], clock: FakeClock) -> Any:
    def _handler(request: httpx.Request) -> httpx.Response:
        stamps.append(clock.now)
        return httpx.Response(200, json={"success": True, "result": {"id": "p", "name": "x"}})

    return _handler


def _gaps(stamps: list[float]) -> list[float]:
    return [round(b - a, 9) for a, b in zip(stamps, stamps[1:], strict=False)]


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


@pytest.fixture
def no_retry_backoff() -> Iterator[None]:
    retrying = NesoDataPortalConnector._send.retry  # type: ignore[attr-defined]
    original = retrying.wait
    retrying.wait = wait_none()
    try:
        yield
    finally:
        retrying.wait = original


def _legacy_payload(name: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    return payload


class TestDatasetHandoff:
    """Q-1: pacing survives dataset change, connector recreation and handoff."""

    def test_two_run_ingest_datasets_stay_paced_across_the_handoff(
        self,
        router: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        from gridflow.connectors import registry as connector_registry
        from gridflow.pipeline import runner as pipeline_runner

        clock = FakeClock()
        fake = RunPacer(1.0, monotonic=clock.monotonic, sleep=clock.sleep)

        def _shared(config: SourceConfig, state_dir: Path | None = None) -> RunPacer:
            if state_dir is not None:
                fake.bind(state_dir)
            return fake

        monkeypatch.setattr(pacer_module, "shared_pacer", _shared)
        real_get_connector = connector_registry.get_connector
        built: list[object] = []

        def _get_connector(source: str, config: SourceConfig) -> Any:
            clock.now += 0.3  # the handoff between datasets takes 0.3 s
            connector = real_get_connector(source, config)
            built.append(connector)
            return connector

        monkeypatch.setattr(connector_registry, "get_connector", _get_connector)

        dwa = _legacy_payload("package_show_daily_wind_availability.json")
        hgm = _legacy_payload("package_show_historic_generation_mix.json")
        bodies = {
            dwa["result"]["resources"][0]["url"]: (FIXTURES / "daily_wind_availability.csv"),
            hgm["result"]["resources"][0]["url"]: (FIXTURES / "historic_generation_mix.csv"),
        }
        stamps: list[float] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            stamps.append(clock.now)
            url = str(request.url)
            if "package_show" in url:
                return httpx.Response(200, json=dwa if "daily-wind" in url else hgm)
            for prefix, body in bodies.items():
                if url.startswith(prefix):
                    return httpx.Response(200, content=body.read_bytes())
            return httpx.Response(404)

        router.route(url__regex=r".*").mock(side_effect=_handler)
        data_dir = tmp_path_factory.mktemp("q1")
        end = datetime.now(UTC) - timedelta(minutes=1)
        with ingest_context(data_dir, monkeypatch) as ctx:
            results = pipeline_runner.run_ingest(
                ctx,
                "neso_data_portal",
                ["daily_wind_availability", "historic_generation_mix"],
                end - timedelta(hours=1),
                end,
            )
        assert [r.status for r in results] == ["success", "success"], results
        assert len(built) == 2 and built[0] is not built[1]
        assert len(stamps) == 4, stamps
        assert all(gap >= 1.0 for gap in _gaps(stamps)), _gaps(stamps)


class TestRetries:
    """Q-2: each retry attempt is admitted by the shared pacer."""

    def test_500_then_200_each_attempt_paced(
        self, router: respx.MockRouter, no_retry_backoff: None
    ) -> None:
        clock = FakeClock()
        pacer = RunPacer(1.0, monotonic=clock.monotonic, sleep=clock.sleep)
        stamps: list[float] = []
        responses = iter(
            [
                httpx.Response(500, content=b"boom"),
                httpx.Response(200, json={"success": True, "result": {"id": "p"}}),
            ]
        )

        def _handler(request: httpx.Request) -> httpx.Response:
            stamps.append(clock.now)
            return next(responses)

        router.route(url__regex=r".*").mock(side_effect=_handler)

        async def _run() -> None:
            async with NesoDataPortalConnector(_config(), pacer=pacer) as connector:
                await connector._package_show("daily-wind-availability")

        asyncio.run(_run())
        assert stamps == [0.0, 1.0], stamps

    def test_connectors_share_one_pacer_by_default(self) -> None:
        """Red on master: each instance paced only itself (E14)."""
        first = NesoDataPortalConnector(_config(rate=1))
        second = NesoDataPortalConnector(_config(rate=1))
        assert first._pacer is second._pacer


def _spawn(role: str, data_dir: Path, **env: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, str(PROC), role, str(data_dir)],
        cwd=PROJECT_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, **env},
    )


def _read_tag(proc: subprocess.Popen[str], tag: str) -> str:
    assert proc.stdout is not None
    while True:
        line = proc.stdout.readline()
        if not line:
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise AssertionError(f"process ended before {tag}: {stderr[-2000:]}")
        name, _, value = line.strip().partition(" ")
        if name == tag:
            return value


def _run_b(data_dir: Path, **env: str) -> dict[str, float]:
    b = _spawn("B", data_dir, **env)
    try:
        _read_tag(b, "READY")
        assert b.stdin is not None
        b.stdin.write("go\n")
        b.stdin.flush()
        lock = float(_read_tag(b, "LOCK"))
        admit = float(_read_tag(b, "ADMIT"))
        send = float(_read_tag(b, "SEND"))
        b.wait(timeout=60)
    finally:
        if b.poll() is None:
            b.kill()
    return {"lock": lock, "admit": admit, "send": send}


class TestCrossProcess:
    """Q-3 (I-P(b)) and Q-6 (busy lock), real processes and a real OS lock."""

    def test_next_process_first_send_follows_a_killed_process_last_send(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        data_dir = tmp_path_factory.mktemp("q3")
        b = _spawn("B", data_dir)
        a = _spawn("A", data_dir)
        try:
            _read_tag(b, "READY")
            a_sends = [float(_read_tag(a, "SEND")) for _ in range(3)]
            a.terminate()  # dies mid-send, holding the lock; no interpreter shutdown
            a.wait(timeout=30)
            assert b.stdin is not None
            b.stdin.write("go\n")
            b.stdin.flush()
            lock = float(_read_tag(b, "LOCK"))
            admit = float(_read_tag(b, "ADMIT"))
            b_send = float(_read_tag(b, "SEND"))
            b.wait(timeout=60)
        finally:
            for proc in (a, b):
                if proc.poll() is None:
                    proc.kill()
        assert all(gap >= 1.0 - 0.05 for gap in _gaps(a_sends)), a_sends
        assert b_send >= a_sends[-1] + 1.0 - 0.05, (b_send - a_sends[-1], a_sends)
        assert admit - lock >= 1.0, admit - lock

    def test_negative_control_without_the_bind_anchor_b_admits_at_once(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        instants = _run_b(tmp_path_factory.mktemp("q3n"), NESO_PACER_NO_ANCHOR="1")
        # Admission follows the lock by B's own enter overhead only (building
        # the TLS context loads the CA bundle, ~0.2 s here), far under the 1 s
        # the anchor enforces, so the positive assertion detects a missing anchor.
        assert instants["admit"] - instants["lock"] < 0.5, instants

    def test_second_process_while_the_lock_is_held_is_refused_before_any_send(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        data_dir = tmp_path_factory.mktemp("q6")
        holder = NesoDataPortalConnector(_config(rate=1))
        holder.bind_data_dir(data_dir)
        busy = _spawn("busy", data_dir)
        out, err = busy.communicate(timeout=60)
        assert busy.returncode == 0, err
        assert "BUSY NesoPacerBusyError" in out, out
        assert "CALLS 0" in out, out

    def test_in_process_second_handle_is_refused_too(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        data_dir = tmp_path_factory.mktemp("q6b")
        state_dir = data_dir / "state" / "neso_data_portal"
        holder = RunPacer(1.0)  # kept referenced: collecting it would drop the lock
        holder.bind(state_dir)
        with pytest.raises(NesoPacerBusyError):
            RunPacer(1.0).bind(state_dir)
        holder.close()
        RunPacer(1.0).bind(state_dir)  # released: a new holder may bind


class TestBindAnchor:
    """Q-4: the bind anchor, and no I/O at admission."""

    def test_bind_anchors_both_lanes_once(self, tmp_path: Path) -> None:
        clock = FakeClock(start=0.3)
        pacer = RunPacer(1.0, 30.0, monotonic=clock.monotonic, sleep=clock.sleep)
        pacer.bind(tmp_path / "state")
        admitted: list[tuple[str, float]] = []

        async def _go(lane: Lane) -> None:
            await pacer.acquire(lane)
            admitted.append((lane.value, clock.now))

        async def _run() -> None:
            await _go(Lane.CKAN)
            await _go(Lane.CKAN)
            await _go(Lane.CKAN)
            await _go(Lane.DATASTORE)

        asyncio.run(_run())
        assert admitted == [
            ("ckan", 1.3),
            ("ckan", 2.3),
            ("ckan", 3.3),
            ("datastore", 30.3),
        ], admitted

        clock.now = 35.0
        pacer.bind(tmp_path / "state")  # same directory: a no-op, moves neither lane
        asyncio.run(_go(Lane.CKAN))
        assert admitted[-1] == ("ckan", 35.0)
        pacer.close()

    def test_rebinding_to_another_directory_is_refused(self, tmp_path: Path) -> None:
        pacer = RunPacer(1.0)
        pacer.bind(tmp_path / "one")
        with pytest.raises(RuntimeError, match="already bound"):
            pacer.bind(tmp_path / "two")
        pacer.close()

    def test_admission_does_no_file_io(
        self, router: respx.MockRouter, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = FakeClock()
        pacer = RunPacer(1.0, monotonic=clock.monotonic, sleep=clock.sleep)
        pacer.bind(tmp_path / "state")
        stamps: list[float] = []
        router.route(url__regex=r".*").mock(side_effect=_package_show_handler(stamps, clock))

        def _refuse(*args: object, **kwargs: object) -> None:
            raise OSError("file I/O during admission")

        async def _run() -> None:
            async with NesoDataPortalConnector(_config(), pacer=pacer) as connector:
                monkeypatch.setattr(builtins, "open", _refuse)
                monkeypatch.setattr(os, "replace", _refuse)
                monkeypatch.setattr(os, "link", _refuse)
                await connector._package_show("a")
                await connector._package_show("b")

        asyncio.run(_run())
        monkeypatch.undo()
        assert stamps == [1.0, 2.0], stamps
        pacer.close()

    def test_non_positive_rate_is_refused(self) -> None:
        with pytest.raises(ValueError, match="must be > 0"):
            shared_pacer(_config(rate=0))
        with pytest.raises(ValueError, match="must be > 0"):
            shared_pacer(_config(rate=-1))


class TestEntryPointsBind:
    """Q-5 (snapshot half): the catalogue snapshot tool binds under the data dir."""

    def test_default_connector_session_is_bound(
        self, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from gridflow.config.settings import load_settings

        data_dir = tmp_path_factory.mktemp("q5")
        monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
        assert load_settings().pipeline.data_dir == data_dir
        session = catalog_snapshot._default_connector_session()
        assert isinstance(session, NesoDataPortalConnector)
        state_dir = session._pacer.state_dir
        assert state_dir == data_dir / "state" / "neso_data_portal"
        assert (state_dir / pacer_module.LOCK_FILENAME).exists()


class TestDatastoreLane:
    """Q-6 (lane half): the datastore lane is 30 s, independent of CKAN."""

    def test_datastore_interval(self) -> None:
        clock = FakeClock()
        pacer = RunPacer(1.0, monotonic=clock.monotonic, sleep=clock.sleep)
        assert pacer.interval(Lane.DATASTORE) == DATASTORE_INTERVAL_S == 30.0
        admitted: list[float] = []

        async def _run() -> None:
            for _ in range(2):
                await pacer.acquire(Lane.DATASTORE)
                admitted.append(clock.now)
            await pacer.acquire(Lane.CKAN)
            admitted.append(clock.now)

        asyncio.run(_run())
        assert admitted == [0.0, 30.0, 30.0], admitted


class TestInProcess:
    """Q-7: I-P(a) on an unbound pacer, measured at the transport."""

    def _connector(self, clock: FakeClock) -> NesoDataPortalConnector:
        pacer = RunPacer(1.0, 30.0, monotonic=clock.monotonic, sleep=clock.sleep)
        return NesoDataPortalConnector(_config(), pacer=pacer)

    def test_late_wake(self, router: respx.MockRouter) -> None:
        clock = FakeClock(oversleep=0.5)
        stamps: list[float] = []
        router.route(url__regex=r".*").mock(side_effect=_package_show_handler(stamps, clock))

        async def _run() -> None:
            async with self._connector(clock) as connector:
                for _ in range(3):
                    await connector._package_show("x")

        asyncio.run(_run())
        assert stamps == [0.0, 1.5, 2.5], stamps

    def test_slow_send(self, router: respx.MockRouter) -> None:
        clock = FakeClock()
        stamps: list[float] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            stamps.append(clock.now)
            if len(stamps) == 1:
                clock.now += 1.5  # the first request takes 1.5 s
            return httpx.Response(200, json={"success": True, "result": {"id": "p"}})

        router.route(url__regex=r".*").mock(side_effect=_handler)

        async def _run() -> None:
            async with self._connector(clock) as connector:
                for _ in range(3):
                    await connector._package_show("x")

        asyncio.run(_run())
        assert all(gap >= 1.0 for gap in _gaps(stamps)), stamps

    def test_two_coroutines_racing_on_one_pacer(self, router: respx.MockRouter) -> None:
        clock = FakeClock()
        stamps: list[float] = []
        router.route(url__regex=r".*").mock(side_effect=_package_show_handler(stamps, clock))

        async def _run() -> None:
            async with self._connector(clock) as connector:
                await asyncio.gather(
                    connector._package_show("a"),
                    connector._package_show("b"),
                    connector._package_show("c"),
                )

        asyncio.run(_run())
        assert len(stamps) == 3
        assert all(gap >= 1.0 for gap in _gaps(sorted(stamps))), stamps


class TestRunIngestBinds:
    """Q-5 (run_ingest half): the ingest entry point binds under the data dir."""

    def test_run_ingest_binds_before_the_first_send(
        self,
        router: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        from gridflow.pipeline import runner as pipeline_runner

        bound: list[tuple[Path, Path | None]] = []
        real_bind = NesoDataPortalConnector.bind_data_dir

        def _spy(self: NesoDataPortalConnector, data_dir: Path) -> None:
            real_bind(self, data_dir)
            bound.append((Path(data_dir), self._pacer.state_dir))

        monkeypatch.setattr(NesoDataPortalConnector, "bind_data_dir", _spy)
        payload = _legacy_payload("package_show_daily_wind_availability.json")
        url = payload["result"]["resources"][0]["url"]
        sends: list[int] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            sends.append(len(bound))
            if "package_show" in str(request.url):
                return httpx.Response(200, json=payload)
            if str(request.url).startswith(url):
                return httpx.Response(
                    200, content=(FIXTURES / "daily_wind_availability.csv").read_bytes()
                )
            return httpx.Response(404)

        router.route(url__regex=r".*").mock(side_effect=_handler)
        data_dir = tmp_path_factory.mktemp("q5i")
        end = datetime.now(UTC) - timedelta(minutes=1)
        with ingest_context(data_dir, monkeypatch) as ctx:
            source = ctx.settings.sources["neso_data_portal"]
            ctx.settings.sources["neso_data_portal"] = source.model_copy(
                update={"rate_limit_per_second": 1000}
            )
            (result,) = pipeline_runner.run_ingest(
                ctx, "neso_data_portal", ["daily_wind_availability"], end - timedelta(hours=1), end
            )
        assert result.status == "success", result
        assert bound == [(data_dir, data_dir / "state" / "neso_data_portal")]
        assert sends and all(count == 1 for count in sends), "a send preceded the bind"


_DUMP_RID = "aaaaaaaa-0000-4000-8000-000000000010"


async def _dump_send(connector: NesoDataPortalConnector) -> None:
    """One datastore-lane send through the primitive, as the dump leg makes it."""
    from gridflow.connectors.neso_data_portal.client import SafeUrl
    from gridflow.connectors.neso_data_portal.endpoints import build_dump_path

    assert connector._client is not None
    request = connector._client.build_request("GET", build_dump_path(_DUMP_RID))
    response = await connector._send(
        request, SafeUrl.verified(request.url), stream=True, lane=Lane.DATASTORE
    )
    await response.aclose()


class TestDatastoreLanePlumbing:
    """T-D2-1 / T-D2-2 (ADR-035 P-1): the lane reaches the pacer; CKAN call shapes are frozen."""

    def test_d2_1_datastore_sends_are_30s_apart_and_ckan_is_not_delayed(
        self, router: respx.MockRouter
    ) -> None:
        """Detects a datastore send admitted on the CKAN lane (red on master: no ``lane``)."""
        clock = FakeClock()
        pacer = RunPacer(1.0, 30.0, monotonic=clock.monotonic, sleep=clock.sleep)
        stamps: list[tuple[str, float]] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            kind = "dump" if "/datastore/dump/" in str(request.url) else "ckan"
            stamps.append((kind, clock.now))
            if kind == "dump":
                return httpx.Response(200, content=b"A,B\n1,2\n")
            return httpx.Response(200, json={"success": True, "result": {"id": "p"}})

        router.route(url__regex=r".*").mock(side_effect=_handler)

        async def _run() -> None:
            async with NesoDataPortalConnector(_config(), pacer=pacer) as connector:
                await _dump_send(connector)
                await connector._package_show("x")
                await _dump_send(connector)

        asyncio.run(_run())
        assert stamps == [("dump", 0.0), ("ckan", 0.0), ("dump", 30.0)], stamps

    def test_d2_2_ckan_sequence_still_paces_at_one_second(self, router: respx.MockRouter) -> None:
        """Detects the CKAN path moving onto another lane or interval (I-1)."""
        clock = FakeClock()
        pacer = RunPacer(1.0, 30.0, monotonic=clock.monotonic, sleep=clock.sleep)
        stamps: list[float] = []
        router.route(url__regex=r".*").mock(side_effect=_package_show_handler(stamps, clock))

        async def _run() -> None:
            async with NesoDataPortalConnector(_config(), pacer=pacer) as connector:
                for _ in range(3):
                    await connector._package_show("x")

        asyncio.run(_run())
        assert stamps == [0.0, 1.0, 2.0], stamps
