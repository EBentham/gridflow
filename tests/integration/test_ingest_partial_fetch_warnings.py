"""CH2-01 / CH-COR-01: partial ingest is never silently recorded as 'success'.

A connector that tolerates per-unit failures (GIE country, NESO window) must
surface a partial fetch so the run is recorded as ``completed_with_warnings``
with ``rows_skipped >= 1`` — not ``success`` (audit C3-9/C2-9). And a fetch in
which *every* attempted unit failed must raise so the run is ``failed`` (C2-6).

The ALSI ``lng`` dataset routes through ``GieConnector.fetch`` ->
``_fetch_legacy_country_dataset`` -> ``_fetch_country`` (because
``source_name != "gie_agsi"``), so it exercises the per-country tolerate-but-
tally path. Modelled on ``test_gie_alsi_legacy_pagination.py`` (respx,
``AlsiConnector``, ``_alsi_config()``).
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb
import httpx
import pytest
import respx
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.gie.client import AlsiConnector
from gridflow.connectors.gie.endpoints import ALSI_COUNTRIES

if TYPE_CHECKING:
    from pathlib import Path

ALSI_BASE_URL = "https://alsi.gie.eu"
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)

runner = CliRunner()


def _alsi_config() -> SourceConfig:
    return (
        load_settings()
        .get_source_config("gie_alsi")
        .model_copy(update={"api_key": "test-key", "rate_limit_per_second": 1000, "timeout": 5})
    )


def _alsi_lng_body(*, country: str) -> bytes:
    """A valid single-page (``last_page=1``) ALSI lng envelope for one country."""
    return json.dumps(
        {
            "last_page": 1,
            "total": 1,
            "gas_day": "2024-01-15",
            "dataset": "lng",
            "data": [{"name": country, "lngInventory": "1.0"}],
        }
    ).encode()


@pytest.fixture(autouse=True)
def _instant_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralise tenacity's exponential backoff so the 5-attempt retry on a
    persistently-500 country runs instantly.

    Patching ``asyncio.sleep`` (which tenacity awaits by module-level name) is
    the reliable, decorator-agnostic route and survives ``asyncio.run`` in the
    CLI path. See ``test_openmeteo.py::TestOpenMeteoLocationRetry._instant_retry``.
    """

    async def _no_sleep(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _no_sleep)


def _one_country_fails_handler(failing_country: str):
    """respx side_effect: ``failing_country`` always 500s, every other is valid."""

    def handler(request: httpx.Request) -> httpx.Response:
        country = request.url.params.get("country", "")
        if country == failing_country:
            return httpx.Response(500, text="persistent upstream error")
        return httpx.Response(
            200,
            content=_alsi_lng_body(country=country),
            headers={"content-type": "application/json"},
        )

    return handler


# ---------------------------------------------------------------------------
# Connector-unit: one country fails (retries exhausted) -> tally, others served
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_partial_fetch_tallies_skipped_country() -> None:
    """One ALSI country 500s on every attempt; the rest return valid last_page=1.

    The fetch must surface the partial result via ``last_skipped_units == 1``
    while still returning the surviving ``N-1`` countries' responses.

    RED before CH2-01: ``last_skipped_units`` does not exist (AttributeError).
    """
    failing = ALSI_COUNTRIES[0]
    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(
        side_effect=_one_country_fails_handler(failing)
    )

    async with AlsiConnector(_alsi_config()) as connector:
        responses = await connector.fetch("lng", START, END)

    # The 7 surviving countries are each served one single-page response.
    assert len(responses) == len(ALSI_COUNTRIES) - 1 == 7
    assert connector.last_skipped_units == 1
    assert all(r.source == "gie_alsi" for r in responses)
    assert failing not in {json.loads(r.body)["data"][0]["name"] for r in responses}


@respx.mock
@pytest.mark.asyncio
async def test_counter_resets_between_fetches_on_same_connector() -> None:
    """A reused connector never inherits a prior call's skipped count.

    First fetch skips one country (==1); a second clean fetch on the SAME
    instance must reset to 0 (the reset-at-top-of-``fetch()`` contract).
    """
    failing = ALSI_COUNTRIES[0]
    route = respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*"))

    async with AlsiConnector(_alsi_config()) as connector:
        route.mock(side_effect=_one_country_fails_handler(failing))
        await connector.fetch("lng", START, END)
        assert connector.last_skipped_units == 1

        # Now every country succeeds; the stale count must be cleared, not carried.
        def _all_ok(request: httpx.Request) -> httpx.Response:
            country = request.url.params.get("country", "")
            return httpx.Response(
                200,
                content=_alsi_lng_body(country=country),
                headers={"content-type": "application/json"},
            )

        route.mock(side_effect=_all_ok)
        responses = await connector.fetch("lng", START, END)

    assert len(responses) == len(ALSI_COUNTRIES)
    assert connector.last_skipped_units == 0


# ---------------------------------------------------------------------------
# All-fail: every country 500s -> fetch() RAISES (run recorded as 'failed')
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_all_countries_fail_raises() -> None:
    """When every attempted country 500s, ``fetch()`` re-raises (raise-on-all-fail).

    Mirrors ``test_openmeteo.py::test_fetch_surfaces_location_failure_after_retries``.
    RED before CH2-01: the swallow returned an empty list and raised nothing.
    """
    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(
        return_value=httpx.Response(500, text="persistent upstream error")
    )

    async with AlsiConnector(_alsi_config()) as connector:
        with pytest.raises(httpx.HTTPStatusError):
            await connector.fetch("lng", START, END)


# ---------------------------------------------------------------------------
# CLI integration: the C3-9/C2-9 guarantee in the pipeline_runs row
# ---------------------------------------------------------------------------


def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    # gie_alsi resolves its key from GIE_API_KEY (api_key_env); respx intercepts
    # the HTTP, so any non-empty value lets the connector construct.
    monkeypatch.setenv("GIE_API_KEY", "test-key")
    # Gold SQL views reference silver tables absent from test tmpdirs; init_catalogue
    # registers them, so stub it out (mirrors test_cli_transform_refresh.py:39).
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return db_path


@respx.mock
@pytest.mark.integration
def test_ingest_partial_fetch_records_completed_with_warnings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``gridflow ingest gie_alsi lng`` with one failing country records the
    pipeline_runs row as ``completed_with_warnings`` with ``rows_skipped >= 1``.

    RED before CH2-01: ingest called ``tracker.complete(...)`` unconditionally,
    so the row was ``success`` with ``rows_skipped == 0``.
    """
    db_path = _isolated_env(tmp_path, monkeypatch)
    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(
        side_effect=_one_country_fails_handler(ALSI_COUNTRIES[0])
    )

    result = runner.invoke(
        app,
        ["ingest", "gie_alsi", "lng", "--start", "2024-01-15", "--end", "2024-01-15"],
    )
    assert result.exit_code == 0, result.output

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            """
            SELECT status, rows_skipped FROM pipeline_runs
            WHERE source = 'gie_alsi' AND dataset = 'lng' AND operation = 'ingest'
            ORDER BY started_at DESC LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    assert row is not None, "no pipeline_runs row recorded for the ingest"
    status, rows_skipped = row
    assert status == "completed_with_warnings", f"status was {status!r}, output:\n{result.output}"
    assert rows_skipped >= 1, f"rows_skipped was {rows_skipped}"
