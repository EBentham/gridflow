"""P0.8 / R2-F08 — per-day partitioning of multi-day ingest windows.

Pre-fix, ENTSO-E and ENTSO-G batch a multi-day ``fetch()`` window into ONE
request, and stamp every returned row's bronze ``data_date`` as the window's
*start* date (``connectors/entsoe/client.py:263``,
``connectors/entsog/client.py:68``) rather than the calendar date the data
actually refers to (the documented ``data_date`` contract,
``connectors/base.py:47-49``). Two independent failure modes follow:

- ENTSO-E: a wide transform window re-reads the SAME window-start bronze
  partition for every date via the covering-partition fallback
  (``silver/base.py:443-471``), so every date's silver file ends up holding a
  full copy of the whole window's rows — row-count duplication across dates
  (Test A / acceptance criteria 1-2).
- ENTSO-G (generic family): the generic transformers read ONLY the exact-date
  partition (``silver/entsog/generic.py:181-193``), so days 2..N of the window
  are fetched and physically written to bronze under day 1's partition, but
  are UNREACHABLE by their own day's transform — silent data loss (Test B /
  acceptance criterion 4). The "stranding" mechanism (later-day record ids
  physically present in the window-start bronze body, but the later day's
  ``run()`` returning 0) is captured once by a throwaway pre-fix probe (never
  committed) and its observed signature is recorded verbatim in Test B's
  docstring below.

This module is fail-first: run against pre-fix code, Test A and Test B fail
on their very first topology assertions (partition/response counts), not on
downstream row-count details — see each test's docstring for the exact
recorded pre-fix signature.

R2-A Task 5 (F-10 guard, ``R2-A-PLAN.md`` S2/A-5): the day-ahead-prices mock
(``_day_ahead_prices_xml``/``_day_ahead_prices_handler``) now over-spans to
whole CET/CEST DELIVERY days via ``ZoneInfo("Europe/Brussels")`` — mirroring
the vendor's real, measured over-span (S1.4) — instead of always echoing
back exactly the requested window. Before this change the mock never
over-spanned, so the per-day row-count parity assertion below
(``expected_counts = [24, 24, 24, 23]``) could never observe a vendor
over-span at all: it was a vacuous guard. As of this commit alone (R2-A
Task 4's neighbour-durability trim is a separate executor's work, not yet
applied here), the guard now BITES — ``row_counts`` comes back
``[48, 48, 48, 48]`` instead. The new
``test_entsoe_cest_over_span_is_trimmed_to_the_requested_utc_day`` pins the
same mechanism in isolation (24 vs 48). Both are expected to go GREEN once
Task 4 lands the durability-gated trim; both are RED right now, by design
(the guard being ABLE to fail is what this commit proves).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import polars as pl
import pytest
import respx

import gridflow.silver.entsog  # noqa: F401 — registers the generic ENTSO-G transformers
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.client import EntsoeConnector
from gridflow.connectors.entsoe.endpoints import DOC_TYPES, ENTSOE_DT_FORMAT
from gridflow.connectors.entsog.client import EntsogConnector
from gridflow.silver.entsoe.day_ahead_prices import DayAheadPricesTransformer
from gridflow.silver.entsoe.generation_units_master_data import (
    GenerationUnitsMasterDataTransformer,
)
from gridflow.silver.registry import get_transformer
from gridflow.storage.paths import PathBuilder

ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
ENTSOG_BASE = "https://transparency.entsog.eu/api/v1"
ENTSOE_FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"


def _entsoe_config() -> SourceConfig:
    return (
        load_settings()
        .get_source_config("entsoe")
        .model_copy(update={"api_key": "test-token", "rate_limit_per_second": 1000, "timeout": 5})
    )


def _entsog_config() -> SourceConfig:
    return (
        load_settings()
        .get_source_config("entsog")
        .model_copy(update={"rate_limit_per_second": 1000, "timeout": 5})
    )


# ---------------------------------------------------------------------------
# Test A helpers — synthetic ENTSO-E day-ahead-prices XML builder
#
# R2-A Task 5 (F-10 guard): the handler now over-spans to whole CET/CEST
# DELIVERY days, exactly mirroring the vendor's real, measured behaviour
# (R2-A-PLAN.md S1.4) instead of a hardcoded ``hours=23``-style shortcut. A
# UTC request touching any part of a Brussels calendar day gets that WHOLE
# day's document back, unioned across every Brussels day the request
# touches. Measured ground truth this mirrors: a clean 24h UTC request for
# 2024-01-15 (winter, CET = UTC+1) returned points spanning
# 2024-01-14T23:00Z -> 2024-01-16T23:00Z (48h, both Brussels days the
# request's [00:00, 24:00) UTC span touches). Before this change the mock
# always echoed back exactly the requested window, so the per-day guard
# (below) could never observe an over-span at all -- it was vacuous.
# ---------------------------------------------------------------------------

BRUSSELS_TZ = ZoneInfo("Europe/Brussels")


def _brussels_day_bounds_utc(instant: datetime) -> tuple[datetime, datetime]:
    """Return the ``[start, end)`` UTC bounds of ``instant``'s Brussels calendar day."""
    local = instant.astimezone(BRUSSELS_TZ)
    day_start_local = datetime(local.year, local.month, local.day, tzinfo=BRUSSELS_TZ)
    day_end_local = day_start_local + timedelta(days=1)
    return day_start_local.astimezone(UTC), day_end_local.astimezone(UTC)


def _day_ahead_prices_xml(period_start: datetime, period_end: datetime) -> bytes:
    """Build a synthetic A44 Publication_MarketDocument for GB day-ahead prices.

    Mirrors the namespace/element shape of
    ``tests/fixtures/entsoe/day_ahead_prices_gb.xml`` so
    ``parse_timeseries_xml`` accepts it. Sequential PT60M points spanning
    ``[period_start, period_end)`` — deterministic, distinct prices so a
    duplicated/over-spanning day is visibly detectable.
    """
    hours = round((period_end - period_start).total_seconds() / 3600)
    start_s = period_start.strftime("%Y-%m-%dT%H:%MZ")
    end_s = period_end.strftime("%Y-%m-%dT%H:%MZ")
    points = "".join(
        f"<Point><position>{i}</position><price.amount>{50.0 + i}</price.amount></Point>"
        for i in range(1, hours + 1)
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
  <mRID>p08-parity-{start_s}</mRID>
  <revisionNumber>1</revisionNumber>
  <type>A44</type>
  <createdDateTime>{start_s}</createdDateTime>
  <period.timeInterval>
    <start>{start_s}</start>
    <end>{end_s}</end>
  </period.timeInterval>
  <TimeSeries>
    <mRID>1</mRID>
    <businessType>A62</businessType>
    <in_Domain.mRID codingScheme="A01">10YGB----------A</in_Domain.mRID>
    <out_Domain.mRID codingScheme="A01">10YGB----------A</out_Domain.mRID>
    <currency_Unit.name>EUR</currency_Unit.name>
    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
    <curveType>A01</curveType>
    <Period>
      <timeInterval>
        <start>{start_s}</start>
        <end>{end_s}</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      {points}
    </Period>
  </TimeSeries>
</Publication_MarketDocument>""".encode()


def _day_ahead_prices_handler(request: httpx.Request) -> httpx.Response:
    """Simulate the vendor's real delivery-day (CET/CEST) alignment.

    The response is the union of every Brussels calendar day the requested
    ``[periodStart, periodEnd)`` UTC window touches -- NEVER simply the
    requested window itself, and never a hardcoded DST-magnitude shortcut
    (rejecting the same "assume-a-magnitude" shortcut the plan's D-3d
    documents rejecting for ownership granularity).
    """
    params = dict(request.url.params)
    request_start = datetime.strptime(params["periodStart"], ENTSOE_DT_FORMAT).replace(tzinfo=UTC)
    request_end = datetime.strptime(params["periodEnd"], ENTSOE_DT_FORMAT).replace(tzinfo=UTC)

    doc_start, _ = _brussels_day_bounds_utc(request_start)
    # request_end is exclusive; the union must be anchored on the last
    # COVERED instant, one microsecond before it.
    _, doc_end = _brussels_day_bounds_utc(request_end - timedelta(microseconds=1))

    return httpx.Response(
        200,
        content=_day_ahead_prices_xml(doc_start, doc_end),
        headers={"content-type": "text/xml"},
    )


@respx.mock
@pytest.mark.asyncio
async def test_entsoe_multi_day_window_partitions_per_day_and_transforms_without_duplication(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acceptance criteria (a) + (b): 4-day ENTSO-E window -> 4 exact bronze
    partitions, no cross-date row duplication, and (post Task 5) a
    transform-window date with no bronze partition is stale-not-regenerated.

    Pre-fix failure signature (run against pre-fix code, 2026-07-16): the
    first-covered-day (2026-05-01) partition assertion passes, but the
    2026-05-02 partition assertion fails immediately::

        AssertionError: expected exactly one raw file in
        .../bronze/entsoe/day_ahead_prices/2026/05/02, got []
        assert 0 == 1

    i.e. only the window-start (2026-05-01) bronze partition exists — the
    connector made one request for the whole window and the writer stamped
    every row under ``data_date=2026-05-01``. (Downstream, had this test's
    partition-existence assertions been relaxed, every date's silver file
    would hold all 95 hours via the covering-partition fallback — the
    row-count parity assertion ``[24, 24, 24, 23]`` would fail the same way.)
    """
    monkeypatch.setattr("gridflow.connectors.entsoe.client.DEFAULT_ZONES", ["GB"])
    respx.get(f"{ENTSOE_BASE}/api").mock(side_effect=_day_ahead_prices_handler)

    start = datetime(2026, 5, 1, tzinfo=UTC)
    end = datetime(2026, 5, 4, 23, 0, tzinfo=UTC)
    dates = [date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3), date(2026, 5, 4)]

    async with EntsoeConnector(_entsoe_config()) as connector:
        responses = await connector.fetch("day_ahead_prices", start, end)

    writer = BronzeWriter(tmp_path)
    for response in responses:
        writer.write(response)

    paths = PathBuilder(tmp_path)

    # --- criterion (a): one exact bronze partition per covered day ---
    for d in dates:
        partition = paths.bronze_date_dir("entsoe", "day_ahead_prices", d)
        raw_files = [p for p in partition.glob("raw_*") if not p.name.endswith(".meta.json")]
        assert len(raw_files) == 1, f"expected exactly one raw file in {partition}, got {raw_files}"
        sidecar = raw_files[0].with_name(f"{raw_files[0].stem}.meta.json")
        meta = json.loads(sidecar.read_text())
        assert meta["data_date"] == d.isoformat()

    # --- criterion (b): per-day row-count parity, no duplication ---
    transformer = DayAheadPricesTransformer(tmp_path)
    expected_counts = [24, 24, 24, 23]
    row_counts = [transformer.run(d, run_id="p08-parity") for d in dates]
    assert row_counts == expected_counts

    frames = [pl.read_parquet(paths.silver_file("entsoe", "day_ahead_prices", d)) for d in dates]
    union = pl.concat(frames)
    assert len(union) == 95
    dup_key = union.select(["timestamp_utc", "area_code"])
    assert dup_key.n_unique() == 95, "duplicate (timestamp_utc, area_code) pairs across days"

    # --- durability step (Task 5): a missing bronze day never silently
    # borrows a neighbour's bronze again; the existing silver file is stale,
    # not regenerated ---
    silver_may_03 = paths.silver_file("entsoe", "day_ahead_prices", date(2026, 5, 3))
    pre_hash = hashlib.sha256(silver_may_03.read_bytes()).hexdigest()

    bronze_may_03 = paths.bronze_date_dir("entsoe", "day_ahead_prices", date(2026, 5, 3))
    for f in bronze_may_03.glob("*"):
        f.unlink()
    bronze_may_03.rmdir()

    rerun_rows = transformer.run(date(2026, 5, 3), run_id="p08-parity-rerun")
    assert rerun_rows == 0

    post_hash = hashlib.sha256(silver_may_03.read_bytes()).hexdigest()
    assert post_hash == pre_hash, "05-03 silver file was regenerated instead of left stale"


# ---------------------------------------------------------------------------
# R2-A Task 5 — the per-day guard must be ABLE to fail on vendor over-span (F-10)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_entsoe_cest_over_span_is_trimmed_to_the_requested_utc_day(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A-5 CEST variant: 24 (branch, once R2-A Task 4 lands) vs 48 (this
    commit alone -- Task 4's neighbour-durability trim is a separate
    executor's work, not yet applied here).

    A single UTC day request during CEST (Brussels = UTC+2, DST active in
    July) gets a 48h over-spanning vendor response from this module's mock
    (matching the measured winter case at S1.4 -- any full UTC day crosses
    into the next Brussels calendar day at its tail, regardless of season).
    The bronze partition for 2024-07-15 durably contains all 48 returned
    hours; once Task 4's per-instant durability gate fires, the transform
    trims back to the requested UTC day's own 24 hours.
    """
    monkeypatch.setattr("gridflow.connectors.entsoe.client.DEFAULT_ZONES", ["GB"])
    respx.get(f"{ENTSOE_BASE}/api").mock(side_effect=_day_ahead_prices_handler)

    start = datetime(2024, 7, 15, tzinfo=UTC)
    end = datetime(2024, 7, 17, tzinfo=UTC)
    target_date = date(2024, 7, 15)

    async with EntsoeConnector(_entsoe_config()) as connector:
        responses = await connector.fetch("day_ahead_prices", start, end)

    writer = BronzeWriter(tmp_path)
    for response in responses:
        writer.write(response)

    transformer = DayAheadPricesTransformer(tmp_path)
    rows = transformer.run(target_date, run_id="r2a-cest-overspan")
    assert rows == 24, "the requested UTC day's own 24 hours, trimmed of the CEST over-span"


# ---------------------------------------------------------------------------
# Task 3 — additional ENTSO-E chunking-boundary coverage
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_entsoe_two_day_mid_day_window_chunks_per_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 2-day mid-day window issues one request per unit PER clamped sub-window."""
    monkeypatch.setattr("gridflow.connectors.entsoe.client.DEFAULT_ZONES", ["GB"])
    route = respx.get(f"{ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=b"<root />", headers={"content-type": "text/xml"})
    )

    start = datetime(2024, 1, 15, 6, 0, tzinfo=UTC)
    end = datetime(2024, 1, 17, 0, 0, tzinfo=UTC)

    async with EntsoeConnector(_entsoe_config()) as connector:
        responses = await connector.fetch("actual_load", start, end)

    assert len(route.calls) == 2  # 1 zone (patched) x 2 sub-windows
    period_pairs = sorted(
        (dict(call.request.url.params)["periodStart"], dict(call.request.url.params)["periodEnd"])
        for call in route.calls
    )
    assert period_pairs == [
        ("202401150600", "202401160000"),
        ("202401160000", "202401170000"),
    ]
    assert {r.data_date for r in responses} == {date(2024, 1, 15), date(2024, 1, 16)}


@respx.mock
@pytest.mark.asyncio
async def test_entsoe_degenerate_window_issues_one_request_per_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """start == end collapses to the legacy single-request shape (degenerate guard)."""
    monkeypatch.setattr("gridflow.connectors.entsoe.client.DEFAULT_ZONES", ["GB"])
    route = respx.get(f"{ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=b"<root />", headers={"content-type": "text/xml"})
    )

    instant = datetime(2024, 1, 15, 6, 0, tzinfo=UTC)

    async with EntsoeConnector(_entsoe_config()) as connector:
        await connector.fetch("actual_load", instant, instant)

    assert len(route.calls) == 1
    params = dict(route.calls[0].request.url.params)
    assert params["periodStart"] == params["periodEnd"] == "202401150600"


@respx.mock
@pytest.mark.asyncio
async def test_entsoe_generation_units_master_data_exempt_from_chunking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`date_param` doc types (only generation_units_master_data) request one
    date per window, unaffected by the window's calendar span — chunking a
    single-date snapshot query would be an incoherent N-times-identical
    request."""
    monkeypatch.setattr("gridflow.connectors.entsoe.client.DEFAULT_ZONES", ["GB"])
    route = respx.get(f"{ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=b"<root />", headers={"content-type": "text/xml"})
    )

    start = datetime(2024, 1, 15, tzinfo=UTC)
    end = datetime(2024, 1, 18, tzinfo=UTC)  # 3-day window

    async with EntsoeConnector(_entsoe_config()) as connector:
        responses = await connector.fetch("generation_units_master_data", start, end)

    assert len(route.calls) == 1
    params = dict(route.calls[0].request.url.params)
    assert "periodStart" not in params
    assert params["Implementation_DateAndOrTime"] == "2024-01-15"
    assert responses[0].data_date == date(2024, 1, 15)


# ---------------------------------------------------------------------------
# Task 4 — ENTSO-G backfill-chunk contract case
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_entsog_midnight_aligned_backfill_chunk_issues_one_request() -> None:
    """A date-aligned ``run_backfill`` 1-day chunk ``[D 00:00Z, D+1 00:00Z)``
    issues exactly one request with ``from == to == D`` — never a boundary
    double-fetch (Sol finding 1 / the half-open ``day_subwindows`` rationale)."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        from_date = date.fromisoformat(params["from"])
        to_date = date.fromisoformat(params["to"])
        return httpx.Response(
            200,
            content=_nominations_body(from_date, to_date),
            headers={"content-type": "application/json"},
        )

    respx.get(f"{ENTSOG_BASE}/operationalData").mock(side_effect=handler)

    start = datetime(2026, 5, 1, tzinfo=UTC)
    end = datetime(2026, 5, 2, tzinfo=UTC)

    async with EntsogConnector(_entsog_config()) as connector:
        responses = await connector.fetch("nominations", start, end)

    assert len(requests) == 1
    params = dict(requests[0].url.params)
    assert params["from"] == params["to"] == "2026-05-01"
    assert len(responses) == 1
    assert responses[0].data_date == date(2026, 5, 1)


# ---------------------------------------------------------------------------
# Test B — ENTSO-G generic-family data loss (orchestrator decision 5)
# ---------------------------------------------------------------------------


def _nominations_body(from_date: date, to_date: date) -> bytes:
    """Two distinct-id records for EVERY day in ``[from_date, to_date]`` inclusive."""
    records = []
    current = from_date
    while current <= to_date:
        stamp = current.strftime("%Y%m%d")
        for suffix in ("a", "b"):
            records.append(
                {
                    "id": f"nom-{stamp}-{suffix}",
                    "dataSet": "1",
                    "indicator": "Nomination",
                    "periodType": "day",
                    "periodFrom": f"{current.isoformat()} 06:00:00",
                    "periodTo": f"{current.isoformat()} 06:00:00",
                    "operatorKey": "UK-TSO-0001",
                    "operatorLabel": "National Gas Transmission",
                    "pointKey": "ITP-00005",
                    "pointLabel": "Bacton (IUK)",
                    "directionKey": "exit",
                    "unit": "kWh/d",
                    "value": "1000000",
                }
            )
        current += timedelta(days=1)
    return json.dumps(
        {
            "meta": {"count": len(records), "total": len(records)},
            "operationalData": records,
        }
    ).encode()


def _entsog_nominations_handler(request: httpx.Request) -> httpx.Response:
    params = dict(request.url.params)
    from_date = date.fromisoformat(params["from"])
    to_date = date.fromisoformat(params["to"])
    return httpx.Response(
        200,
        content=_nominations_body(from_date, to_date),
        headers={"content-type": "application/json"},
    )


@respx.mock
@pytest.mark.asyncio
async def test_entsog_multi_day_window_generic_family_data_loss(tmp_path) -> None:
    """Acceptance criterion 4: a multi-day ENTSO-G window must produce one
    ``RawResponse``/bronze partition per covered day (half-open derivation),
    with the generic family returning rows for every covered day (not just
    day 1).

    **Pre-fix stranding signature, recorded from a one-time throwaway probe
    (scratchpad-only, deleted immediately after use, never committed; run
    2026-07-16 against pre-fix code, identical window/mock shape to this
    test). Verbatim probe console output**::

        PROBE: number of RawResponses = 1
        PROBE: response data_date=2026-05-01
        PROBE: partition exists: .../bronze/entsog/nominations/2026/05/01
        PROBE: body raw_..._....json contains 'nom-20260501-a': True
        PROBE: body raw_..._....json contains 'nom-20260502-a': True
        PROBE: body raw_..._....json contains 'nom-20260503-a': True
        PROBE: run(2026-05-01) = 2
        PROBE: run(2026-05-02) = 0
        PROBE: run(2026-05-03) = 0

    In words: exactly ONE ``RawResponse`` is returned by
    ``EntsogConnector.fetch``, with ``data_date == date(2026, 5, 1)`` (the
    window-start date). Only the 2026-05-01 bronze partition exists on disk;
    05-02 and 05-03 never get their own partition. The 05-01 bronze raw JSON
    body PHYSICALLY CONTAINS the day-2 and day-3 record ids (as well as
    day-1's) — the data was genuinely fetched and written, not omitted by
    the mock. ``run(date(2026, 5, 2))`` and ``run(date(2026, 5, 3))`` both
    return 0 (the generic family's ``_bronze_files`` is exact-partition-only,
    ``silver/entsog/generic.py:181-193`` — no covering-fallback for this
    family), while ``run(date(2026, 5, 1))`` returns 2 (the
    ``date_window_dataset`` filter, ``generic.py:102-105``, matches only
    day 1's 2 records out of the 6 physically present in the 05-01 file).

    This is the "stranding" mechanism criterion 4 requires be fixed: later
    days' content is fetched and durably written, but permanently
    unreachable by their own day's transform. Fixing the connector to chunk
    per day (so each day gets its OWN bronze partition) is what this test
    pins going green.

    **This test's own pre-fix failure signature** (run against pre-fix code,
    2026-07-16): fails on its very first topology assertion,
    ``assert len(requests) == 3`` -> ``AssertionError: assert 1 == 3``. The
    connector issues exactly one request covering the whole window.
    """
    start = datetime(2026, 5, 1, tzinfo=UTC)
    end = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
    covered_days = [date(2026, 5, 1), date(2026, 5, 2), date(2026, 5, 3)]

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _entsog_nominations_handler(request)

    respx.get(f"{ENTSOG_BASE}/operationalData").mock(side_effect=handler)

    async with EntsogConnector(_entsog_config()) as connector:
        responses = await connector.fetch("nominations", start, end)

    # --- post-fix topology assertions (fail-first: pre-fix, 1 request / 1
    # partition, both asserted counts below) ---
    assert len(requests) == 3
    request_from_to = sorted((r.url.params["from"], r.url.params["to"]) for r in requests)
    assert request_from_to == [
        ("2026-05-01", "2026-05-01"),
        ("2026-05-02", "2026-05-02"),
        ("2026-05-03", "2026-05-03"),
    ]
    for from_value, to_value in request_from_to:
        assert from_value == to_value

    assert len(responses) == 3
    assert {r.data_date for r in responses} == set(covered_days)

    writer = BronzeWriter(tmp_path)
    for response in responses:
        writer.write(response)

    paths = PathBuilder(tmp_path)
    for d in covered_days:
        partition = paths.bronze_date_dir("entsog", "nominations", d)
        raw_files = [p for p in partition.glob("raw_*") if not p.name.endswith(".meta.json")]
        assert len(raw_files) == 1

    transformer = get_transformer("entsog", "nominations", tmp_path)
    for d in covered_days:
        assert transformer.run(d, run_id="p08-entsog-parity") == 2


# ---------------------------------------------------------------------------
# Task 5 — generation_units_master_data accepted-change acceptance test
# ---------------------------------------------------------------------------


def test_generation_units_master_data_trails_by_one_run_under_exact_only(
    tmp_path: Path,
) -> None:
    """`generation_units_master_data` is exempt from ENTSO-E chunking (its
    `date_param` branch already requests a single date per window), so under
    the P0.8 exact-partition-only silver read policy it now trails by one
    run instead of getting a fabricated same-day fallback copy.

    Pre-fix: seeding the fixture at partition D and running for D+1 returned
    the D snapshot mis-labelled as D+1 (the covering-partition fallback). Post-
    fix (this test): D+1 gets 0 rows and no silver file is written for D+1 —
    the honest snapshot for D+1 only appears once D+1 becomes a window-start
    partition of its own (accepted behaviour change, documented in the plan's
    "generation_units_master_data under exact-only" subsection).
    """
    fixture_body = (ENTSOE_FIXTURES / "generation_units_master_data_gb.xml").read_bytes()
    target_date = date(2026, 5, 10)

    response = RawResponse(
        body=fixture_body,
        content_type="text/xml",
        source="entsoe",
        dataset="generation_units_master_data",
        request_url=f"{ENTSOE_BASE}/api",
        request_params={"documentType": DOC_TYPES["generation_units_master_data"].document_type},
        http_status=200,
        data_date=target_date,
    )
    BronzeWriter(tmp_path).write(response)

    transformer = GenerationUnitsMasterDataTransformer(tmp_path)
    assert transformer.run(target_date, run_id="p08-master-data") > 0

    next_day = target_date + timedelta(days=1)
    assert transformer.run(next_day, run_id="p08-master-data") == 0

    paths = PathBuilder(tmp_path)
    assert not paths.silver_file("entsoe", "generation_units_master_data", next_day).exists()
