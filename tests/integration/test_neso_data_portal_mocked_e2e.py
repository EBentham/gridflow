"""Mocked connector -> bronze -> silver E2E for the NESO Data Portal (T-15).

**Why a full-path test and not two half-tests.** The connector suite proves the
fetch, the transformer suite proves the transform, and both can be green while
the composition yields zero rows: a ``expected_columns`` mismatch between the
connector's ``DATASETS`` entry and the transformer's, a ``BRONZE_BODY_GLOB``
that does not match the extension the writer chose, or a bronze partition the
transform leg never looks at, are each invisible to either half alone. So this
module drives the **real** connector, the **real** ``BronzeWriter`` and the
**real** registered transformer, and mocks only the network.

``pytestmark`` is not optional (D-39 §1a): every send validates its target by
resolving the host, and respx mocks HTTP but **not** name resolution. Without
the ``stub_neso_resolver`` opt-in this module would resolve ``api.neso.energy``
for real — a live network call in the default suite. T-27 and T-19 add to this
module and inherit the declaration; T-24's live module deliberately does not
have it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest
import respx

import gridflow.silver.neso_data_portal  # noqa: F401 -- registers the transformers
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import load_settings
from gridflow.connectors import base as connectors_base
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.config.settings import SourceConfig

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

SOURCE = "neso_data_portal"
DATASET = "daily_wind_availability"

BASE_URL = "https://api.neso.energy"
PACKAGE_SHOW_URL = f"{BASE_URL}/api/3/action/package_show"
FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal"

# The same hand-authored body the unit suite uses (its provenance is disclosed
# in `tests/unit/test_neso_data_portal.py`'s T-09 section and in the T-09 commit
# message). Reused rather than re-invented so the row count this module asserts
# is the SAME number the transformer suite pins.
DAILY_WIND_CSV = (FIXTURE_DIR / "daily_wind_availability.csv").read_bytes()
EXPECTED_ROWS = 6

# The real presigned shape from `_probe/sample_historic-generation-mix.headers`.
FILE_HOST = "https://83025b28472d6aa2bf5ae59f3724aa78.eu.r2.cloudflarestorage.com"
PRESIGNED_URL = (
    f"{FILE_HOST}/dx-national-grid/national-grid/resources/x/windavailability.csv"
    "?response-content-disposition=attachment%3B%20filename%3Dwindavailability.csv"
    "&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=604800"
    "&X-Amz-SignedHeaders=host&X-Amz-Date=20260816T185401Z"
    "&X-Amz-Signature=b4f0da8dbcf4e5c46e06a16556fcc90257a632b4684f5d6d4c4d0da7565bceef"
)

CKAN_LAST_MODIFIED = "2026-08-16T18:20:11.953941"
PUBLISHED_AT = datetime(2026, 8, 16, 18, 20, 11, 953941, tzinfo=UTC)


def _package_show_payload() -> dict[str, Any]:
    """The trimmed Stage-A ``package_show`` capture for this package."""
    payload: dict[str, Any] = json.loads(
        (FIXTURE_DIR / "package_show_daily_wind_availability.json").read_text(encoding="utf-8")
    )
    return payload


def _source_config() -> SourceConfig:
    """The REAL config from ``config/sources.yaml``, paced for a test.

    Built from the shipped YAML rather than hand-assembled: a base URL or a
    dataset key that drifted out of the config would otherwise be invisible
    here, which is half of what a composition test exists to catch. Only the
    pacing is overridden — ``rate_limit_per_second: 1`` is a real 1.0 s minimum
    interval per send (D-07), and this module makes two sends per fetch.
    """
    return (
        load_settings()
        .get_source_config(SOURCE)
        .model_copy(update={"rate_limit_per_second": 1000, "timeout": 5})
    )


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    """A GLOBAL respx router, never ``base_url``-scoped.

    A scoped router only patches traffic matching its base URL, so the
    cross-host file leg would escape it entirely.
    """
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


def _wire_all_three_legs(router: respx.MockRouter) -> None:
    """Route the ``package_show`` JSON, the 302 redirector, and the CSV body."""
    router.get(url__startswith=PACKAGE_SHOW_URL).mock(
        return_value=httpx.Response(200, json=_package_show_payload())
    )
    resource_url = _package_show_payload()["result"]["resources"][0]["url"]
    router.get(url__startswith=resource_url).mock(
        return_value=httpx.Response(
            302,
            headers=[("location", PRESIGNED_URL)],
            content=b"<html>redirecting</html>",
        )
    )
    router.get(url__startswith=FILE_HOST).mock(
        return_value=httpx.Response(200, content=DAILY_WIND_CSV)
    )


def _window() -> tuple[datetime, datetime]:
    """A window D-34 admits: ends now, so neither future nor stale."""
    end = datetime.now(UTC)
    return end - timedelta(hours=24), end


async def _fetch_one(config: SourceConfig, start: datetime, end: datetime) -> Any:
    async with NesoDataPortalConnector(config) as connector:
        responses = await connector.fetch(DATASET, start, end)
    assert len(responses) == 1, "D-16: the window is not a selector; one fetch, one response"
    return responses[0]


def _partition_parts(bronze_path: Path) -> tuple[str, str, str]:
    return (bronze_path.parts[-4], bronze_path.parts[-3], bronze_path.parts[-2])


def _expected_partition(day: Any) -> tuple[str, str, str]:
    return (str(day.year), f"{day.month:02d}", f"{day.day:02d}")


def _silver_dir(data_dir: Path, day: Any) -> Path:
    return data_dir / "silver" / SOURCE / DATASET / f"year={day.year}" / f"month={day.month:02d}"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_writes_bronze_and_the_real_transformer_reads_it(
    router: respx.MockRouter,
    tmp_data_dir: Path,
) -> None:
    """The whole path, with the row count asserted as a NUMBER.

    "No error" is not the assertion that matters here: a composition break
    (a header contract mismatch, a glob that misses the extension the writer
    chose, a partition the transform leg never visits) shows up as a silent
    zero-row run, which "no error" passes.
    """
    _wire_all_three_legs(router)
    start, end = _window()

    response = await _fetch_one(_source_config(), start, end)
    bronze_path = BronzeWriter(tmp_data_dir).write(response)

    # D-13: the partition is the resolved window END, which is by construction
    # the last date `run_transform` iterates for that window.
    assert response.data_date == end.date()
    assert bronze_path.name.startswith("raw_")
    assert bronze_path.suffix == ".csv", (
        "D-10: content_type is stamped from CKAN's format, not the presigned "
        "host's application/octet-stream — a .bin body is invisible to the "
        "transformer's raw_*.csv glob"
    )
    assert _partition_parts(bronze_path) == _expected_partition(end.date())
    assert bronze_path.with_suffix(".meta.json").exists()

    transformer = get_transformer(SOURCE, DATASET, tmp_data_dir)
    rows_written = transformer.run(end.date(), run_id="t15")

    assert rows_written == EXPECTED_ROWS

    written = sorted(_silver_dir(tmp_data_dir, end.date()).glob("*.parquet"))
    assert len(written) == 1
    assert written[0].name.startswith(f"{DATASET}_{end.date():%Y%m%d}_run"), (
        "APPEND_ONLY must suffix the silver filename with the vintage scalar"
    )

    frame = pl.read_parquet(written[0])
    assert frame.height == EXPECTED_ROWS
    for column in ("event_time", "available_at", "source_run_id", "dataset_version"):
        assert column in frame.columns
    assert frame.schema["event_time"] == pl.Datetime("us", "UTC")
    assert frame.schema["available_at"] == pl.Datetime("us", "UTC")
    assert frame["source_run_id"].unique().to_list() == ["t15"]
    assert frame["dataset_version"].unique().to_list() == ["1.0.0"]

    # D-22: NESO's publication instant is the vintage axis, not our capture
    # instant. Asserted row-wise, and against the CKAN value the mocked
    # payload actually declared rather than against whatever landed.
    assert frame["published_at"].unique().to_list() == [PUBLISHED_AT]
    assert frame["available_at"].to_list() == frame["published_at"].to_list()


@respx.mock
@pytest.mark.asyncio
async def test_re_running_the_transform_is_idempotent(
    router: respx.MockRouter,
    tmp_data_dir: Path,
) -> None:
    """D-21: the run suffix comes from the bronze sidecar, so it is stable.

    On the plain ``read_bronze`` branch the scalar would be ``datetime.now(UTC)``
    and every re-transform would mint a NEW Parquet file, quietly duplicating
    every row in the base view. ``VINTAGE_PER_BRONZE_FILE`` is what makes the
    second run replace the first at a byte-identical path.
    """
    _wire_all_three_legs(router)
    start, end = _window()

    response = await _fetch_one(_source_config(), start, end)
    BronzeWriter(tmp_data_dir).write(response)

    transformer = get_transformer(SOURCE, DATASET, tmp_data_dir)
    assert transformer.run(end.date(), run_id="t15-first") == EXPECTED_ROWS
    first = sorted(_silver_dir(tmp_data_dir, end.date()).glob("*.parquet"))

    assert transformer.run(end.date(), run_id="t15-second") == EXPECTED_ROWS
    second = sorted(_silver_dir(tmp_data_dir, end.date()).glob("*.parquet"))

    assert len(second) == 1, f"a re-transform minted a second silver file: {second}"
    assert first == second, "the silver path moved between two runs over the same bronze"


@respx.mock
@pytest.mark.asyncio
async def test_a_download_crossing_utc_midnight_still_lands_where_transform_looks(
    router: respx.MockRouter,
    tmp_data_dir: Path,
) -> None:
    """FM-13, the hazard that changed D-13 in revision 2.

    ``fetched_at`` is stamped at ``RawResponse`` construction — i.e. AFTER the
    download — so a ``fetched_at``-derived partition would put a
    ``--last 24h`` run started at 23:58 onto day N+1 while the transform leg,
    working from the window resolved at 23:58, only ever looks at day N. Ingest
    reports success, transform finds nothing, and nothing anywhere reports a
    problem.

    The clock is advanced ONLY in ``connectors.base``, where ``fetched_at`` is
    stamped. ``client.py`` holds its own ``datetime`` reference, so D-34's
    window admission still runs against the real clock and the window this test
    resolves stays admissible — which is the point: the two clocks genuinely
    disagree, exactly as they would on a slow 62 MB download.
    """
    _wire_all_three_legs(router)
    start, end = _window()

    # Deterministically past the next UTC midnight relative to `end`, whatever
    # time of day the suite happens to run.
    after_midnight = datetime.combine(end.date() + timedelta(days=1), time(0, 1), tzinfo=UTC)
    assert after_midnight.date() != end.date(), "precondition: the clock crossed midnight"

    class _AdvancedClock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # noqa: ARG003 - signature parity
            return after_midnight

    # Its OWN MonkeyPatch, not the shared `monkeypatch` fixture. The
    # `stub_neso_resolver` opt-in this module declares uses that same fixture
    # instance, so an `undo()` here would silently un-stub the resolver and put
    # real DNS back in the default suite — the exact thing D-39 §1a's testing
    # rule exists to keep out.
    with pytest.MonkeyPatch.context() as clock:
        clock.setattr(connectors_base, "datetime", _AdvancedClock)
        response = await _fetch_one(_source_config(), start, end)

    assert response.fetched_at == after_midnight, (
        "precondition: the patched clock did not reach the RawResponse stamp, so "
        "this test would pass without exercising the hazard at all"
    )
    assert response.fetched_at.date() != end.date()

    bronze_path = BronzeWriter(tmp_data_dir).write(response)

    assert _partition_parts(bronze_path) == _expected_partition(end.date()), (
        "bronze followed the wall clock instead of the resolved window end"
    )

    transformer = get_transformer(SOURCE, DATASET, tmp_data_dir)
    assert transformer.run(end.date(), run_id="t15-midnight") == EXPECTED_ROWS, (
        "the transform leg, working from the same window, did not find the capture"
    )
    assert transformer.run(response.fetched_at.date(), run_id="t15-midnight-wrong-day") == 0, (
        "the capture was ALSO readable from the wall-clock day, so the partition "
        "assertion above does not actually discriminate"
    )
