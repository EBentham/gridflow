"""T-28 (D-35): the ``SNAPSHOT_ONLY`` capability and the generic backfill refusal.

A snapshot-only source cannot be backfilled: every resource is a whole-file
republication with no server-side date filter, so a backfill would re-download
the same bytes once per chunk and retain one identical vintage each time —
hundreds of 62 MB requests at a portal that publishes 1 req/s guidance and
reserves the right to IP-block (T-NDP-07).

**Why a capability rather than a window check.** D-34's recency screen is a
*proxy* for "this is a backfill", and the proxy leaks:
``--start 2026-08-14 --end 2026-08-16 --chunk-days 1`` yields chunk ends 45.5 h
and 21.5 h stale — both inside tolerance, both admitted, two duplicate
downloads and a reported success. No value of that constant fixes it, because a
recent backfill window is genuinely indistinguishable from a live one by
recency. The capability check is decided by *what the source is*, so it holds
for every window shape and every chunk size. Both leaking cases are pinned
below.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
import respx
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.connectors.base import BaseConnector
from gridflow.connectors.elexon.client import ElexonConnector
from gridflow.connectors.entsoe.client import EntsoeConnector
from gridflow.connectors.entsog.client import EntsogConnector
from gridflow.connectors.gie.client import AgsiConnector, AlsiConnector, GieConnector
from gridflow.connectors.neso.carbon_intensity import CarbonIntensityConnector
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
from gridflow.connectors.openmeteo.client import OpenMeteoConnector
from gridflow.pipeline import runner
from gridflow.pipeline.runner import BackfillUnsupportedError, assert_backfillable

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.config.settings import GridflowConfig

cli = CliRunner()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    """A global router with a catch-all, so a stray request is RECORDED.

    An unrouted-request error would also fail the test, but it would fail it as
    a mock-configuration problem someone later "fixes" by adding a route. A
    recorded request fails it as what it is.
    """
    with respx.mock(assert_all_called=False) as mock_router:
        mock_router.route(url__regex=r".*").mock(
            return_value=__import__("httpx").Response(200, json={})
        )
        yield mock_router


class TestCapabilityDeclaration:
    """The ClassVar defaults to ``False``, so no existing source is affected."""

    def test_base_connector_declares_snapshot_only_false(self) -> None:
        assert BaseConnector.SNAPSHOT_ONLY is False

    @pytest.mark.parametrize(
        "connector_cls",
        [
            ElexonConnector,
            EntsoeConnector,
            EntsogConnector,
            GieConnector,
            AgsiConnector,
            AlsiConnector,
            CarbonIntensityConnector,
            OpenMeteoConnector,
        ],
    )
    def test_every_existing_connector_inherits_false(
        self, connector_cls: type[BaseConnector]
    ) -> None:
        """Enumerated EXPLICITLY, never parameterised over a discovered list.

        A discovered list that silently became empty would make this test pass
        while proving nothing — and the failure it is guarding against is
        backfill being silently disabled for a real source.
        """
        assert connector_cls.SNAPSHOT_ONLY is False, connector_cls.__name__

    def test_the_neso_data_portal_connector_declares_true(self) -> None:
        assert NesoDataPortalConnector.SNAPSHOT_ONLY is True


class TestAssertBackfillable:
    """The generic helper: no source name appears in the CLI."""

    def test_a_normal_source_passes(self) -> None:
        assert assert_backfillable("elexon") is None

    def test_a_snapshot_only_source_is_refused_naming_the_source(self) -> None:
        with pytest.raises(BackfillUnsupportedError) as excinfo:
            assert_backfillable("neso_data_portal")

        assert "neso_data_portal" in str(excinfo.value)


class TestFreshInterpreter:
    """D-37: the helper's own bootstrap is what makes this work at all.

    In a fresh CLI process the connector registry is **empty** until
    ``cli.py``'s ``import_connectors()`` runs, which is *after*
    ``_resolve_datasets`` and would be after a guard placed "first". A helper
    that resolves a class from an empty registry cannot refuse anything — it
    would fail to find NESO *and* fail to find the five legitimate sources.
    ``run_backfill`` never bootstraps at all, so a programmatic caller has the
    same empty registry.
    """

    def _run(self, script: str) -> Any:
        import subprocess
        import sys
        from pathlib import Path

        return subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_snapshot_only_is_refused_in_a_fresh_interpreter(self) -> None:
        result = self._run(
            "from gridflow.pipeline.runner import assert_backfillable, "
            "BackfillUnsupportedError\n"
            "try:\n"
            "    assert_backfillable('neso_data_portal')\n"
            "except BackfillUnsupportedError:\n"
            "    print('REFUSED')\n"
        )

        assert result.returncode == 0, result.stderr
        assert "REFUSED" in result.stdout

    def test_a_normal_source_still_resolves_in_a_fresh_interpreter(self) -> None:
        result = self._run(
            "from gridflow.pipeline.runner import assert_backfillable\n"
            "assert_backfillable('elexon')\n"
            "print('ALLOWED')\n"
        )

        assert result.returncode == 0, result.stderr
        assert "ALLOWED" in result.stdout

    def test_negative_control_without_the_self_bootstrap_nothing_resolves(self) -> None:
        """With the helper's own ``import_connectors()`` stubbed out, the
        registry is empty and the lookup FAILS.

        Without this control both tests above could pass on a registry
        something else happened to populate, and the self-bootstrap — the whole
        point of the design — would be untested.
        """
        result = self._run(
            "from gridflow.pipeline import runner\n"
            "runner.import_connectors = lambda: None\n"
            "try:\n"
            "    runner.assert_backfillable('neso_data_portal')\n"
            "except runner.BackfillUnsupportedError:\n"
            "    print('REFUSED_ANYWAY')\n"
            "except ValueError as exc:\n"
            "    print('UNRESOLVED')\n"
        )

        assert result.returncode == 0, result.stderr
        assert "UNRESOLVED" in result.stdout, (
            "the connector resolved without the helper's own bootstrap — the "
            "fresh-interpreter proofs above are therefore vacuous"
        )


class TestCliRefusal:
    """Both entry points, every window shape, zero requests."""

    def test_a_historical_backfill_is_refused_with_zero_requests(
        self, router: respx.MockRouter
    ) -> None:
        result = cli.invoke(
            app,
            [
                "backfill",
                "neso_data_portal",
                "daily_wind_availability",
                "--start",
                "2020-01-01",
                "--end",
                "2020-12-31",
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, BackfillUnsupportedError)
        assert "neso_data_portal" in str(result.exception)
        assert len(router.calls) == 0

    def test_the_recent_window_case_sol_falsified_is_refused(
        self, router: respx.MockRouter
    ) -> None:
        """Chunk ends 45.5 h and 21.5 h stale — both inside D-34's tolerance,
        both admitted by the recency proxy, two duplicate downloads."""
        result = cli.invoke(
            app,
            [
                "backfill",
                "neso_data_portal",
                "daily_wind_availability",
                "--start",
                "2026-08-14",
                "--end",
                "2026-08-16",
                "--chunk-days",
                "1",
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, BackfillUnsupportedError)
        assert len(router.calls) == 0

    def test_the_single_large_chunk_case_is_refused(self, router: respx.MockRouter) -> None:
        """One chunk whose end lands near now leaks the recency proxy the same
        way a many-chunk backfill does."""
        today = datetime.now(UTC).date().isoformat()
        result = cli.invoke(
            app,
            [
                "backfill",
                "neso_data_portal",
                "daily_wind_availability",
                "--start",
                "2020-01-01",
                "--end",
                today,
                "--chunk-days",
                "3650",
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, BackfillUnsupportedError)
        assert len(router.calls) == 0


class TestRunnerEntryPoint:
    """The second entry point is covered, not just the CLI."""

    def test_run_backfill_refuses_a_snapshot_only_source(
        self, sample_config: GridflowConfig, router: respx.MockRouter
    ) -> None:
        with pytest.raises(BackfillUnsupportedError):
            runner.run_backfill(
                sample_config,
                "neso_data_portal",
                ["daily_wind_availability"],
                datetime(2020, 1, 1, tzinfo=UTC),
                datetime(2020, 12, 31, tzinfo=UTC),
            )

        assert len(router.calls) == 0

    def test_run_backfill_still_reaches_the_chunk_loop_for_a_normal_source(
        self, sample_config: GridflowConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The guard must not have become a blanket refusal.

        Asserted on the chunk loop actually running, not merely on the absence
        of an exception: a guard that raised for every source would also be
        caught here, but so would one that returned early.
        """
        ingest_calls: list[tuple[str, list[str]]] = []

        @contextlib.contextmanager
        def _fake_context(_settings: object) -> Iterator[object]:
            yield object()

        monkeypatch.setattr(runner, "build_context", _fake_context)
        monkeypatch.setattr(
            runner,
            "run_ingest",
            lambda ctx, source, datasets, *a, **k: ingest_calls.append((source, datasets)) or [],
        )
        monkeypatch.setattr(runner, "run_transform", lambda *a, **k: [])
        monkeypatch.setattr(runner, "refresh_views", lambda *a, **k: None)

        start = datetime(2024, 1, 1, tzinfo=UTC)
        runner.run_backfill(
            sample_config,
            "elexon",
            ["system_prices"],
            start,
            start + timedelta(days=2),
        )

        assert ingest_calls == [("elexon", ["system_prices"])] * 2, (
            "the chunk loop did not run for a normal source"
        )
