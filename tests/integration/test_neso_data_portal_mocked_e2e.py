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

import csv
import io
import json
from datetime import UTC, date, datetime, time, timedelta
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
from gridflow.connectors.neso_data_portal.endpoints import DATASETS
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.registry import get_transformer
from gridflow.storage.parquet import scan_parquet_dir
from gridflow.utils.time import settlement_period_to_utc

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


# --------------------------------------------------------------------------- #
# T-27 -- the same full-path proof for the two B3a datasets
#
# Not a transformer-only test, for the reason this module's docstring gives:
# the pieces can each pass while the COMPOSITION yields zero rows. Both of the
# datasets below have a wider surface for that than `daily_wind_availability`
# did -- a 34-column header contract that has to agree between the connector's
# `DATASETS` entry and the transformer's, and (for the embedded forecast) an
# `issue_time` that travels from a CKAN resource FILENAME, through the bronze
# sidecar, into a silver column that is part of the entity key. Neither half
# alone can see either of those break.
# --------------------------------------------------------------------------- #

HGM_DATASET = "historic_generation_mix"
EMB_DATASET = "embedded_wind_solar_forecast"

# Reused rather than re-invented, so the row counts asserted here are the SAME
# numbers the transformer suite pins. Both fixtures' provenance (range-capture
# truncation; hand-constructed DST days) is disclosed in
# `tests/unit/test_neso_data_portal.py`'s T-16 and T-17 sections.
HGM_CSV = (FIXTURE_DIR / "historic_generation_mix.csv").read_bytes()
EMB_CSV = (FIXTURE_DIR / "embedded_forecast.csv").read_bytes()
HGM_EXPECTED_ROWS = 9
EMB_EXPECTED_ROWS = 20

# The embedded forecast's issue instant, carried by the vendor's own filename
# (`202608161825_embedded_forecast.csv`) in the package_show fixture. Asserted
# END TO END: nothing else in the pipeline knows this value.
EMB_ISSUE_TIME = datetime(2026, 8, 16, 18, 25, tzinfo=UTC)

_B3A_DATASETS: dict[str, tuple[str, bytes, int]] = {
    HGM_DATASET: ("package_show_historic_generation_mix.json", HGM_CSV, HGM_EXPECTED_ROWS),
    EMB_DATASET: (
        "package_show_embedded_wind_and_solar_forecasts.json",
        EMB_CSV,
        EMB_EXPECTED_ROWS,
    ),
}


@pytest.fixture
def short_data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """`tmp_data_dir`, but rooted somewhere deliberately SHORT.

    Windows MAX_PATH, a harness limit rather than a product one.
    `embedded_wind_solar_forecast`'s silver filename is 74 characters and
    `storage.parquet.write_parquet` appends a 21-character `.tmp_<16 hex>`
    before `os.replace`, so under the standard `tmp_path` fixture -- whose
    directory embeds the test's own name -- the temporary path crosses 260 and
    the rename fails with WinError 3. The real data root (`C:\\gridflow-data`)
    is ~90 characters clear. Same reasoning, and same fix, as
    `tests/unit/test_neso_data_portal.py::short_tmp_path`.
    """
    root = tmp_path_factory.mktemp("e")
    for layer in ("bronze", "silver", "gold"):
        (root / layer).mkdir()
    return root


def _package_show_for(dataset: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(
        (FIXTURE_DIR / _B3A_DATASETS[dataset][0]).read_text(encoding="utf-8")
    )
    return payload


def _selected_resource(dataset: str) -> dict[str, Any]:
    """The resource the connector's own D-04 exact-name rule will pick."""
    wanted = DATASETS[dataset].resource_name
    matches = [
        resource
        for resource in _package_show_for(dataset)["result"]["resources"]
        if resource.get("name") == wanted
    ]
    assert len(matches) == 1, f"fixture does not carry exactly one {wanted!r} resource"
    resource: dict[str, Any] = matches[0]
    return resource


def _presigned_for(dataset: str) -> str:
    """A presigned URL of the real R2 shape, named for this resource's file."""
    filename = str(_selected_resource(dataset)["url"]).rsplit("/", 1)[-1]
    return (
        f"{FILE_HOST}/dx-national-grid/national-grid/resources/x/{filename}"
        f"?response-content-disposition=attachment%3B%20filename%3D{filename}"
        "&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=604800"
        "&X-Amz-SignedHeaders=host&X-Amz-Date=20260816T185401Z"
        "&X-Amz-Signature=b4f0da8dbcf4e5c46e06a16556fcc90257a632b4684f5d6d4c4d0da7565bceef"
    )


def _wire_dataset_legs(router: respx.MockRouter, dataset: str) -> None:
    """Route the `package_show` JSON, the 302 redirector, and the CSV body."""
    body = _B3A_DATASETS[dataset][1]
    router.get(url__startswith=PACKAGE_SHOW_URL).mock(
        return_value=httpx.Response(200, json=_package_show_for(dataset))
    )
    router.get(url__startswith=str(_selected_resource(dataset)["url"])).mock(
        return_value=httpx.Response(
            302,
            headers=[("location", _presigned_for(dataset))],
            content=b"<html>redirecting</html>",
        )
    )
    router.get(url__startswith=FILE_HOST).mock(return_value=httpx.Response(200, content=body))


async def _fetch_dataset(config: SourceConfig, dataset: str, start: datetime, end: datetime) -> Any:
    async with NesoDataPortalConnector(config) as connector:
        responses = await connector.fetch(dataset, start, end)
    assert len(responses) == 1, "D-16: the window is not a selector; one fetch, one response"
    return responses[0]


def _silver_files(data_dir: Path, dataset: str, day: Any) -> list[Path]:
    silver_dir = (
        data_dir / "silver" / SOURCE / dataset / f"year={day.year}" / f"month={day.month:02d}"
    )
    return sorted(silver_dir.glob("*.parquet"))


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", [HGM_DATASET, EMB_DATASET])
async def test_each_b3a_dataset_transforms_end_to_end(
    router: respx.MockRouter,
    short_data_dir: Path,
    dataset: str,
) -> None:
    """The whole path per dataset, with the row count asserted as a LITERAL.

    A composition break shows up as a silent zero-row run, which "no error"
    passes. The expected count is stated rather than derived from the fixture at
    runtime, so a transformer that dropped every second row would still fail
    here.
    """
    expected_rows = _B3A_DATASETS[dataset][2]
    _wire_dataset_legs(router, dataset)
    start, end = _window()

    response = await _fetch_dataset(_source_config(), dataset, start, end)
    bronze_path = BronzeWriter(short_data_dir).write(response)

    assert response.data_date == end.date()
    assert bronze_path.name.startswith("raw_")
    assert bronze_path.suffix == ".csv", (
        "D-10: content_type is stamped from CKAN's format, not the presigned host's "
        "application/octet-stream — a .bin body is invisible to the raw_*.csv glob"
    )
    assert _partition_parts(bronze_path) == _expected_partition(end.date())
    assert bronze_path.with_suffix(".meta.json").exists()

    transformer = get_transformer(SOURCE, dataset, short_data_dir)
    rows_written = transformer.run(end.date(), run_id="t27")

    assert rows_written == expected_rows
    assert transformer.last_excluded_row_count == 0, (
        "neither fixture carries an out-of-calendar settlement period, so a "
        "non-zero count here means the D-27 filter is rejecting valid rows"
    )

    written = _silver_files(short_data_dir, dataset, end.date())
    assert len(written) == 1
    assert written[0].name.startswith(f"{dataset}_{end.date():%Y%m%d}_run"), (
        "APPEND_ONLY must suffix the silver filename with the vintage scalar"
    )

    frame = pl.read_parquet(written[0])
    assert frame.height == expected_rows
    assert frame.schema["event_time"] == pl.Datetime("us", "UTC")
    assert frame["event_time"].null_count() == 0
    assert frame.schema["available_at"] == pl.Datetime("us", "UTC")
    assert frame["source_run_id"].unique().to_list() == ["t27"]

    # D-22: NESO's publication instant is the vintage axis, taken from the CKAN
    # payload this test actually served rather than from whatever landed.
    published = datetime.fromisoformat(str(_selected_resource(dataset)["last_modified"])).replace(
        tzinfo=UTC
    )
    assert frame["published_at"].unique().to_list() == [published]
    assert frame["available_at"].to_list() == frame["published_at"].to_list()


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", [HGM_DATASET, EMB_DATASET])
async def test_re_running_each_b3a_transform_is_idempotent(
    router: respx.MockRouter,
    short_data_dir: Path,
    dataset: str,
) -> None:
    """D-21: the run suffix comes from the bronze sidecar, so it is stable.

    On the plain `read_bronze` branch the scalar would be `datetime.now(UTC)`
    and every re-transform would mint a NEW Parquet file, quietly duplicating
    every row in the base view -- which for `historic_generation_mix` means a
    second full copy of 2009-present.
    """
    expected_rows = _B3A_DATASETS[dataset][2]
    _wire_dataset_legs(router, dataset)
    start, end = _window()

    response = await _fetch_dataset(_source_config(), dataset, start, end)
    BronzeWriter(short_data_dir).write(response)

    transformer = get_transformer(SOURCE, dataset, short_data_dir)
    assert transformer.run(end.date(), run_id="t27-first") == expected_rows
    first = _silver_files(short_data_dir, dataset, end.date())

    assert transformer.run(end.date(), run_id="t27-second") == expected_rows
    second = _silver_files(short_data_dir, dataset, end.date())

    assert len(second) == 1, f"a re-transform minted a second silver file: {second}"
    assert first == second, "the silver path moved between two runs over the same bronze"


@respx.mock
@pytest.mark.asyncio
async def test_the_embedded_forecasts_issue_time_survives_the_whole_path(
    router: respx.MockRouter,
    short_data_dir: Path,
) -> None:
    """Filename -> sidecar -> silver column, with nothing else able to supply it.

    `issue_time` exists nowhere in the CSV body: it is a 12-digit token in the
    CKAN resource's filename (D-15/D-23), written into the bronze sidecar by the
    connector and parsed back out by the transformer. Every link is in a
    different module, so only a full-path test can see the chain break -- and a
    break would not be loud, it would be a run declined for "no issue token" or,
    worse, a vintage stamped from the wrong clock.
    """
    _wire_dataset_legs(router, EMB_DATASET)
    start, end = _window()

    response = await _fetch_dataset(_source_config(), EMB_DATASET, start, end)
    bronze_path = BronzeWriter(short_data_dir).write(response)

    sidecar = json.loads(bronze_path.with_suffix(".meta.json").read_text(encoding="utf-8"))
    assert sidecar["request_params"]["resource_filename"] == "202608161825_embedded_forecast.csv"

    transformer = get_transformer(SOURCE, EMB_DATASET, short_data_dir)
    assert transformer.run(end.date(), run_id="t27-issue") == EMB_EXPECTED_ROWS

    frame = pl.read_parquet(_silver_files(short_data_dir, EMB_DATASET, end.date())[0])

    assert frame.schema["issue_time"] == pl.Datetime("us", "UTC")
    assert frame["issue_time"].unique().to_list() == [EMB_ISSUE_TIME]
    assert "timestamp_utc" not in frame.columns, (
        "D-26: emitting one would take event_time off the DST-fold-safe branch"
    )
    assert frame["event_time"].to_list() == [
        settlement_period_to_utc(row["settlement_date"], row["settlement_period"])
        for row in frame.iter_rows(named=True)
    ]


# --------------------------------------------------------------------------- #
# T-18 -- cross-source validation, both checks offline
#
# PHASE.md requires the settlement-convention claim be cross-VALIDATED, not
# merely asserted internally. A transformer that derived `event_time` from its
# own parallel arithmetic would satisfy every test above this line: they all
# compare this source against itself. The only way to prove it shares the
# repo's ONE convention is to compare it against a different source's
# transformer over the same settlement pairs, on the days where a parallel
# convention would diverge -- the 46- and 50-period DST days.
# --------------------------------------------------------------------------- #

EMB_SPRING_CSV = (FIXTURE_DIR / "embedded_forecast_dst_spring.csv").read_bytes()
EMB_AUTUMN_CSV = (FIXTURE_DIR / "embedded_forecast_dst_autumn.csv").read_bytes()

SPRING_DAY = date(2026, 3, 29)
AUTUMN_DAY = date(2026, 10, 25)


def _wire_dataset_legs_with_body(router: respx.MockRouter, dataset: str, body: bytes) -> None:
    """`_wire_dataset_legs`, but serving a caller-supplied CSV body.

    A separate function rather than a keyword on the T-27 helper: that helper
    is what the row-count tests above depend on, and this unit adds tests
    without touching the assertions already in place.
    """
    router.get(url__startswith=PACKAGE_SHOW_URL).mock(
        return_value=httpx.Response(200, json=_package_show_for(dataset))
    )
    router.get(url__startswith=str(_selected_resource(dataset)["url"])).mock(
        return_value=httpx.Response(
            302,
            headers=[("location", _presigned_for(dataset))],
            content=b"<html>redirecting</html>",
        )
    )
    router.get(url__startswith=FILE_HOST).mock(return_value=httpx.Response(200, content=body))


def _fuelhh_period_instants(pairs: list[tuple[date, int]], data_dir: Path) -> pl.DataFrame:
    """The instant `silver/elexon/fuelhh.py` derives for each settlement pair.

    Driven through the REAL `FuelHHTransformer.transform`, from a frame in the
    vendor's own bronze spelling, so this is the value Elexon silver actually
    carries rather than a re-implementation of it here. `transform()` touches
    no disk; `data_dir` only satisfies the constructor.
    """
    raw_df = pl.DataFrame(
        {
            "settlementDate": [day for day, _ in pairs],
            "settlementPeriod": [period for _, period in pairs],
            "fuelType": ["WIND"] * len(pairs),
            "generation": [0.0] * len(pairs),
        }
    )
    silver = FuelHHTransformer(data_dir).transform(raw_df)
    return silver.select(
        pl.col("settlement_date"),
        pl.col("settlement_period").cast(pl.Int64),
        pl.col("timestamp_utc"),
    )


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "expected_rows"),
    [
        pytest.param(EMB_CSV, EMB_EXPECTED_ROWS, id="ordinary-day-with-a-day-roll"),
        pytest.param(EMB_SPRING_CSV, 46, id="spring-forward-46-periods"),
        pytest.param(EMB_AUTUMN_CSV, 50, id="autumn-back-50-periods"),
    ],
)
async def test_this_sources_event_time_uses_the_repos_one_settlement_convention(
    router: respx.MockRouter,
    short_data_dir: Path,
    body: bytes,
    expected_rows: int,
) -> None:
    """T-18 check 1: NESO `event_time` == Elexon `timestamp_utc`, pair for pair.

    Both sides are the REAL transformers, so the assertion is that the two
    sources agree, not that two copies of the same helper agree. The DST
    parameters are what give the check teeth: a naive
    `midnight + 30min * (period - 1)` convention agrees with
    `settlement_period_to_utc` on every ordinary day and diverges by an hour
    across the fold, so an ordinary-day-only comparison would pass against a
    parallel convention. 46 and 50 periods are exactly the days where it
    cannot.
    """
    _wire_dataset_legs_with_body(router, EMB_DATASET, body)
    start, end = _window()

    response = await _fetch_dataset(_source_config(), EMB_DATASET, start, end)
    BronzeWriter(short_data_dir).write(response)

    transformer = get_transformer(SOURCE, EMB_DATASET, short_data_dir)
    assert transformer.run(end.date(), run_id="t18") == expected_rows
    assert transformer.last_excluded_row_count == 0, (
        "every fixture period is valid for its own day, so a D-27 exclusion "
        "here means the comparison below is running over a truncated set"
    )

    neso = pl.read_parquet(_silver_files(short_data_dir, EMB_DATASET, end.date())[0]).select(
        "settlement_date", "settlement_period", "event_time"
    )
    pairs = [
        (row["settlement_date"], row["settlement_period"]) for row in neso.iter_rows(named=True)
    ]
    elexon = _fuelhh_period_instants(pairs, short_data_dir)

    joined = neso.join(elexon, on=["settlement_date", "settlement_period"], how="inner")
    assert joined.height == expected_rows, (
        "the join lost rows, so the equality below would be asserted over a "
        "subset and a divergent pair could pass unseen"
    )
    assert joined["event_time"].to_list() == joined["timestamp_utc"].to_list(), (
        "this source derives a settlement instant that Elexon's transformer "
        "does not -- i.e. a SECOND settlement convention now exists in the repo"
    )


def test_time_gmt_corroborates_the_period_end_instant() -> None:
    """T-18 check 2, NON-BINDING: an observation guard, never a dependency.

    On the real captured fixture (`_probe/sample_embedded-forecast-current.csv`,
    carried verbatim as `embedded_forecast.csv`), the vendor's
    `DATE_GMT` + `TIME_GMT` pair equals the settlement period's **end** in UTC.
    Nothing in the pipeline reads it: D-26 carries `TIME_GMT` through as an
    unparsed `time_gmt_raw` and derives `event_time` from the settlement pair
    alone, precisely because this convention is observed rather than documented.

    **A future failure here means NESO changed convention** -- it does not mean
    the pipeline is wrong, and it must not be "fixed" by editing the expected
    value. It is a tripwire on an undocumented vendor behaviour, and its
    resolution is a vendor question (TODO-02), not a code change.

    The two DST fixtures are deliberately NOT included: they are hand-authored
    to this same convention, so asserting it over them would only prove the
    fixtures are self-consistent.
    """
    rows = list(csv.DictReader(io.StringIO(EMB_CSV.decode("utf-8"))))
    assert len(rows) == EMB_EXPECTED_ROWS, "the real capture is 20 complete rows"

    mismatches: list[str] = []
    for row in rows:
        settlement_date = datetime.strptime(row["SETTLEMENT_DATE"], "%Y-%m-%dT%H:%M:%S").date()
        period = int(row["SETTLEMENT_PERIOD"])
        stamped = datetime.combine(
            datetime.strptime(row["DATE_GMT"], "%Y-%m-%dT%H:%M:%S").date(),
            time.fromisoformat(row["TIME_GMT"]),
            tzinfo=UTC,
        )
        period_end = settlement_period_to_utc(settlement_date, period) + timedelta(minutes=30)
        if stamped != period_end:
            mismatches.append(
                f"{settlement_date} SP{period}: vendor stamped {stamped.isoformat()}, "
                f"period end is {period_end.isoformat()}"
            )

    assert not mismatches, (
        "NESO's DATE_GMT/TIME_GMT no longer equals the settlement period's END "
        f"in UTC: {mismatches}. D-26 keeps the pipeline independent of this "
        "column, so nothing is broken -- but the vendor's convention moved and "
        "the vault page's corroboration needs revisiting (TODO-02)"
    )


def test_local_fuelhh_parquet_agrees_in_magnitude_where_both_sources_exist() -> None:
    """T-18 check 1's extension: real on-disk silver, never a hard dependency.

    Compares total GB generation for overlapping half-hours between
    `silver/elexon/fuelhh` and `silver/neso_data_portal/historic_generation_mix`
    as a MAGNITUDE sanity check -- the two are measured differently (fuelhh is
    transmission-metered by fuel; the generation mix includes embedded
    estimates), so the tolerance is deliberately loose and only a
    factor-of-scale error, e.g. GW read as MW, would trip it.

    Skips with a stated reason whenever either side is absent locally. Local
    data is a convenience, never a test dependency: this file must be green on
    a clean checkout with no `data/` at all.

    **Which root it looks in, and why it usually skips.** The repo-root
    conftest's autouse `_clear_gridflow_path_env` deletes `GRIDFLOW_DATA_DIR`
    so a machine's real root cannot leak into the suite, which leaves
    `load_settings()` on `config/settings.yaml`'s repo-relative `./data`. That
    is deliberately NOT worked around here -- reading the env var directly
    would reintroduce exactly the leak the fixture exists to prevent -- and it
    is the same path the plan names. So on a machine whose data lives
    elsewhere (this one: `C:\\gridflow-data`) this check skips, and it is meant
    to: the binding cross-validation is the convention test above, which needs
    no local data at all.
    """
    data_dir = load_settings().pipeline.data_dir
    elexon_dir = data_dir / "silver" / "elexon" / "fuelhh"
    neso_dir = data_dir / "silver" / SOURCE / HGM_DATASET

    missing = [str(path) for path in (elexon_dir, neso_dir) if not path.exists()]
    if missing:
        pytest.skip(f"no local silver to cross-check against: {missing}")

    elexon = (
        scan_parquet_dir(elexon_dir)
        .group_by("timestamp_utc")
        .agg(pl.col("generation_mw").sum().alias("elexon_mw"))
        .collect()
    )
    neso = (
        scan_parquet_dir(neso_dir)
        .select("timestamp_utc", pl.col("generation").alias("neso_mw"))
        .unique(subset=["timestamp_utc"], keep="last")
        .collect()
    )

    overlap = elexon.join(neso, on="timestamp_utc", how="inner").filter(
        (pl.col("elexon_mw") > 0) & (pl.col("neso_mw") > 0)
    )
    if overlap.height == 0:
        pytest.skip("local silver exists for both sources but shares no half-hour")

    sample = overlap.sort("timestamp_utc").head(24)
    ratios = (sample["neso_mw"] / sample["elexon_mw"]).to_list()
    assert all(0.5 <= ratio <= 2.0 for ratio in ratios), (
        f"GB total generation differs by more than 2x on {sample.height} "
        f"overlapping half-hour(s): ratios {ratios}. The two sources measure "
        "differently, so a small spread is expected -- this is a unit/scale "
        "check, and a failure means one side is out by an order of magnitude"
    )
