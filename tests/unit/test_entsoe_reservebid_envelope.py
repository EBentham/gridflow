"""RED-first ENTSO-E A37 ReserveBid envelope, conservation, zero-match-guard
and mixed-envelope tests (N-21).

GROUP A tests are RED on ``master @ e7be850`` and GREEN only after the D-1 /
D-9 / D-10 fix (Task 2) lands. Per Sec 3c of the plan, no GROUP A assertion
is satisfiable by an empty frame, a no-op, or a price-sourced value -- each
one names the specific killing assertion documented there.

GROUP B tests are invariants: GREEN both before and after Task 2.

**Fixture derivation (D-4a).**
``tests/fixtures/entsoe/balancing_energy_bids_a37_reservebid_de_20260601.xml``
is derived from the git-ignored probe
``.planning/phases/R3-test-integrity/probes/entsoe_A37_extracted.xml``
(``.gitignore:50``, F-8) by keeping every document-level child verbatim and
the first 3 ``<Bid_TimeSeries>`` elements verbatim -- no element renamed,
reordered or edited -- then closing the root tag. Derivation command::

    python - <<'PY'
    from pathlib import Path
    import re
    data = Path(
        ".planning/phases/R3-test-integrity/probes/entsoe_A37_extracted.xml"
    ).read_text(encoding="utf-8")
    ends = [m.end() for m in re.finditer(r"</Bid_TimeSeries>", data)]
    fixture = data[: ends[2]] + "\n</ReserveBid_MarketDocument>\n"
    Path(
        "tests/fixtures/entsoe/balancing_energy_bids_a37_reservebid_de_20260601.xml"
    ).write_text(fixture, encoding="utf-8", newline="\n")
    PY

The committed fixture, never the probe, is what every test below reads
(V-4: no test in this module has an executable dependency on a path under
``.planning/``).

**D-9a: inline documents, never fixtures.** The guard-trip document (A6) and
the mixed-envelope document (A7) are module-level byte literals, not files
under ``tests/fixtures/``. The A37 fixture cannot exercise the guard --
post-fix it matches all 3 committed series and the guard correctly stays
quiet (F-11) -- and a committed guard-tripping or mixed fixture would make
B4 (the corpus-silence invariant) permanently red (FM-19).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from lxml import etree

import gridflow.silver.entsoe  # noqa: F401 -- registers every entsoe transformer
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.endpoints import ENTSOE_DT_FORMAT
from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.entsoe.h8_balancing import BalancingEnergyBidsTransformer
from gridflow.silver.partition_window import RequestWindow, exclude_out_of_window

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"
A37_FIXTURE = FIXTURES / "balancing_energy_bids_a37_reservebid_de_20260601.xml"

#: F-3a: the three quantities and prices the derived fixture pins, numerically
#: disjoint (quantities ``4.0``, prices ``-14997``/``-14980``) so a
#: value-sourcing bug (reading energy_Price.amount as the quantity, D-3,
#: Sol finding 2) cannot hide behind a nullability check.
F3A_MRIDS = ["rQPZL5twXh3YlszYIFEkT1", "rQPZL7AvjUIrQiEqYoHUl1", "rQPZLcidt0LNHS3LFrmfLe"]
F3A_QUANTITIES = [4.0, 4.0, 4.0]
F3A_PRICES = [-14997.0, -14980.0, -14980.0]


def _root_local_name(xml_bytes: bytes) -> str:
    root = etree.fromstring(xml_bytes)
    tag = root.tag
    return tag.split("}", 1)[1] if "}" in tag else tag


def _serialize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """D-2: convert ``timestamp_utc`` to an ISO string so the baseline
    comparison runs on plain JSON-comparable values on both sides."""
    out = []
    for record in records:
        serialised = dict(record)
        serialised["timestamp_utc"] = serialised["timestamp_utc"].isoformat()
        out.append(serialised)
    return out


def _write_entsoe_partition(
    data_dir: Path,
    *,
    source: str,
    dataset: str,
    partition_date: date,
    body: bytes,
) -> None:
    """Write one bronze raw body + sidecar through the real ``BronzeWriter``
    harness (the T4 pattern from
    ``test_entsoe_event_window_family_mechanics.py``), parameterised by
    source/dataset so the written sidecar's identity always matches the
    transformer under test (Sol pass-1 finding 6 precedent)."""
    response = RawResponse(
        body=body,
        content_type="text/xml",
        source=source,
        dataset=dataset,
        request_url="https://web-api.tp.entsoe.eu/api",
        request_params={
            "periodStart": datetime(2026, 6, 1, tzinfo=UTC).strftime(ENTSOE_DT_FORMAT),
            "periodEnd": datetime(2026, 6, 2, tzinfo=UTC).strftime(ENTSOE_DT_FORMAT),
        },
        data_date=partition_date,
    )
    BronzeWriter(data_dir).write(response)


# ---------------------------------------------------------------------------
# D-2 point 3 / V-2: the full-record baseline for every non-ReserveBid
# committed fixture, generated from ``master @ e7be850`` BEFORE this unit
# reshaped ``balancing_energy_bids_gb.xml`` (D-4b) -- so its entry here still
# reflects the pre-reshape ``Balancing_MarketDocument`` / bare-``TimeSeries``
# shape (1 series x 2 points, ``original_market_product == "A01"``). After
# the reshape that entry becomes unused, not stale-and-wrong: the fixture's
# root is now ``ReserveBid_MarketDocument``, so B1 below skips it by
# construction and the exclusion-set assertion pins that explicitly.
# Datetimes are ISO strings on both sides (D-2). Machine-generated -- do not
# hand-edit; regenerate per the derivation note in
# ``R4-b-SUMMARY.md`` if it ever needs to change.
# ---------------------------------------------------------------------------
_B1_BASELINE_JSON = """
{
 "activated_balancing_prices_gb.xml": {
  "price.amount": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 110.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 115.5
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "A02",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 72.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "A02",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 68.5
   }
  ],
  "quantity": []
 },
 "activated_balancing_qty_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-qty-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 320.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-qty-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 280.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-qty-gb-20240115",
    "document_status": "",
    "flow_direction": "A02",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 140.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-act-bal-qty-gb-20240115",
    "document_status": "",
    "flow_direction": "A02",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 160.0
   }
  ]
 },
 "actual_generation_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-gen-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B01",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-gen-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B01",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1150.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-gen-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 5400.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-gen-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 5600.0
   }
  ]
 },
 "actual_generation_units_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-DRAX-3",
    "asset_name": "Drax Unit 3",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-actual-gen-units-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "UNIT-DRAX-3",
    "unit_name": "Drax Unit 3",
    "value": 610.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-DRAX-3",
    "asset_name": "Drax Unit 3",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-actual-gen-units-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "UNIT-DRAX-3",
    "unit_name": "Drax Unit 3",
    "value": 625.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-SOLAR-1",
    "asset_name": "Solar Farm 1",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-actual-gen-units-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "UNIT-SOLAR-1",
    "unit_name": "Solar Farm 1",
    "value": 45.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-SOLAR-1",
    "asset_name": "Solar Farm 1",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-actual-gen-units-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "UNIT-SOLAR-1",
    "unit_name": "Solar Farm 1",
    "value": 60.0
   }
  ]
 },
 "actual_load_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A04",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-load-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 28500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A04",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-load-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 28100.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A04",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-load-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 27900.0
   }
  ]
 },
 "aggregated_balancing_energy_bids_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "10YGB----------A",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B74",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-aggregated-balancing-energy-bids-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "aggregated-bid-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 92.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "10YGB----------A",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B74",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-aggregated-balancing-energy-bids-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "aggregated-bid-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 87.0
   }
  ]
 },
 "balancing_energy_bids_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B74",
    "connecting_domain": "10YGB----------A",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-balancing-energy-bids-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "A01",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "A05",
    "timeseries_mrid": "bid-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 45.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B74",
    "connecting_domain": "10YGB----------A",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-balancing-energy-bids-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "A01",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "A05",
    "timeseries_mrid": "bid-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 52.0
   }
  ]
 },
 "balancing_financial_expenses_income_gb.xml": {
  "price.amount": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B10",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-balancing-financial-expenses-income-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "financial-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 35.5
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B10",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-balancing-financial-expenses-income-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "financial-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 37.25
   }
  ],
  "quantity": []
 },
 "contracted_reserves_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T03:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T04:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T05:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T06:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T07:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T08:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T09:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T10:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T11:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T12:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T13:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T14:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T15:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T16:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T17:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T18:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T19:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T20:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T21:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T22:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A95",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T23:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T03:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T04:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T05:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T06:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T07:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T08:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T09:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T10:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T11:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T12:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T13:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T14:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T15:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T16:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T17:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T18:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T19:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T20:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T21:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T22:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A96",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-contracted-res-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T23:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 350.0
   }
  ]
 },
 "cross_border_flows_gb_fr.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-flow-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-flow-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1800.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-flow-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 2000.0
   }
  ]
 },
 "cross_zonal_balancing_capacity_gb_fr.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "10YGB----------A",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "",
    "connecting_domain": "10YFR-RTE------C",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-cross-zonal-balancing-capacity-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "A01",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "cross-zonal-capacity-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 210.0
   },
   {
    "acquiring_domain": "10YGB----------A",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "",
    "connecting_domain": "10YFR-RTE------C",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-cross-zonal-balancing-capacity-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "A01",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "cross-zonal-capacity-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 215.0
   }
  ]
 },
 "current_balancing_state_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "10YGB----------A",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B33",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-current-balancing-state-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "state-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 125.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "10YGB----------A",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B33",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-current-balancing-state-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "state-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": -75.0
   }
  ]
 },
 "day_ahead_prices_gb.xml": {
  "price.amount": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A62",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T13:00:00Z",
    "document_mrid": "fixture-dap-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 85.5
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A62",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T13:00:00Z",
    "document_mrid": "fixture-dap-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 82.3
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A62",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T13:00:00Z",
    "document_mrid": "fixture-dap-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 79.1
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A62",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T13:00:00Z",
    "document_mrid": "fixture-dap-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T03:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 78.0
   }
  ],
  "quantity": []
 },
 "forecast_margin_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-forecast-margin-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "P1D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 4200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-forecast-margin-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "P1D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-16T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 3900.0
   }
  ]
 },
 "generation_forecast_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-gen-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B01",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1100.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-gen-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B01",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1050.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-gen-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B16",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 4200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-gen-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B16",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 4500.0
   }
  ]
 },
 "generation_units_master_data_gb.xml": {
  "price.amount": [],
  "quantity": []
 },
 "h6_market_price_gb_fr.xml": {
  "price.amount": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B10",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-h6-price-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 42.5
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B10",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-h6-price-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 39.25
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B10",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "EUR",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-h6-price-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 45.0
   }
  ],
  "quantity": []
 },
 "h6_market_quantity_gb_fr.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B05",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-h6-quantity-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B05",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-h6-quantity-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1250.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "B05",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-h6-quantity-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1180.0
   }
  ]
 },
 "imbalance_prices_gb.xml": {
  "price.amount": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A19",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 95.5
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A19",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 102.25
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A20",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 88.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A20",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "GBP",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-prices-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 91.75
   }
  ],
  "quantity": []
 },
 "imbalance_volume_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A19",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-vol-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 150.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A19",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-vol-gb-20240115",
    "document_status": "",
    "flow_direction": "A01",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A20",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-vol-gb-20240115",
    "document_status": "",
    "flow_direction": "A02",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 80.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A20",
    "connecting_domain": "",
    "control_area_domain": "10YGB----------A",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-imb-vol-gb-20240115",
    "document_status": "",
    "flow_direction": "A02",
    "in_domain": "",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 120.0
   }
  ]
 },
 "installed_capacity_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A29",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-01T00:00:00Z",
    "document_mrid": "fixture-ic-gb-20240101",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "P1Y",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-01T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 15200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A29",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-01T00:00:00Z",
    "document_mrid": "fixture-ic-gb-20240101",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B18",
    "reason_code": "",
    "resolution": "P1Y",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-01T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 27500.0
   }
  ]
 },
 "installed_capacity_units_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-DRAX-3",
    "asset_name": "Drax Unit 3",
    "business_type": "A29",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-01T00:00:00Z",
    "document_mrid": "fixture-ic-units-gb-20240101",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "P1Y",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-01T00:00:00+00:00",
    "unit_mrid": "UNIT-DRAX-3",
    "unit_name": "Drax Unit 3",
    "value": 660.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-HEYSHAM-2",
    "asset_name": "Heysham 2",
    "business_type": "A29",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-01T00:00:00Z",
    "document_mrid": "fixture-ic-units-gb-20240101",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B14",
    "reason_code": "",
    "resolution": "P1Y",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-01T00:00:00+00:00",
    "unit_mrid": "UNIT-HEYSHAM-2",
    "unit_name": "Heysham 2",
    "value": 1240.0
   }
  ]
 },
 "load_forecast_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 29100.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:30:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 28800.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT30M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 28600.0
   }
  ]
 },
 "load_forecast_monthly_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-month-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "P1D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 30200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-month-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "P1D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-16T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 29900.0
   }
  ]
 },
 "load_forecast_weekly_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-wk-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "",
    "reason_code": "",
    "resolution": "P7D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 31500.0
   }
  ]
 },
 "load_forecast_yearly_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-year-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "P1D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 31500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-load-fc-year-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "P1D",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-16T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 31000.0
   }
  ]
 },
 "net_transfer_capacity_gb_fr.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A25",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ntc-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 2000.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A25",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ntc-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 2000.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A25",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ntc-gb-fr-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T02:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 1800.0
   }
  ]
 },
 "outages_consumption_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-consumption-outage-gb-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "consumption-ts-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 150.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-consumption-outage-gb-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "consumption-ts-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 125.0
   }
  ]
 },
 "outages_generation_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-DRAX-3",
    "asset_name": "Drax Unit 3",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-outage-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "UNIT-DRAX-3",
    "unit_name": "Drax Unit 3",
    "value": 800.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-DRAX-3",
    "asset_name": "Drax Unit 3",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-outage-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "UNIT-DRAX-3",
    "unit_name": "Drax Unit 3",
    "value": 800.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-HEYSHAM-2",
    "asset_name": "Heysham 2",
    "business_type": "A54",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-outage-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B14",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "UNIT-HEYSHAM-2",
    "unit_name": "Heysham 2",
    "value": 1200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "UNIT-HEYSHAM-2",
    "asset_name": "Heysham 2",
    "business_type": "A54",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-outage-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B14",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "UNIT-HEYSHAM-2",
    "unit_name": "Heysham 2",
    "value": 1200.0
   }
  ]
 },
 "outages_offshore_grid_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "ASSET-OFFSHORE-HUB-1",
    "asset_name": "Offshore Hub 1",
    "business_type": "",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-offshore-grid-outage-gb-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "offshore-ts-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 300.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "ASSET-OFFSHORE-HUB-1",
    "asset_name": "Offshore Hub 1",
    "business_type": "",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-offshore-grid-outage-gb-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "offshore-ts-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 275.0
   }
  ]
 },
 "outages_production_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "PROD-DRAX-3",
    "asset_name": "Drax Production Unit 3",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-production-outage-gb-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "production-ts-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "PROD-DRAX-3",
    "unit_name": "Drax Production Unit 3",
    "value": 700.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "PROD-DRAX-3",
    "asset_name": "Drax Production Unit 3",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-production-outage-gb-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "B02",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "production-ts-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "PROD-DRAX-3",
    "unit_name": "Drax Production Unit 3",
    "value": 650.0
   }
  ]
 },
 "outages_transmission_gb_fr.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "ASSET-IFA-1",
    "asset_name": "IFA Interconnector Circuit 1",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-transmission-outage-gb-fr-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "2",
    "standard_market_product": "",
    "timeseries_mrid": "transmission-ts-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "ASSET-IFA-1",
    "asset_name": "IFA Interconnector Circuit 1",
    "business_type": "A53",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-transmission-outage-gb-fr-20240115",
    "document_status": "A05",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YFR-RTE------C",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "2",
    "standard_market_product": "",
    "timeseries_mrid": "transmission-ts-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 450.0
   }
  ]
 },
 "procured_balancing_capacity_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "10YGB----------A",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-procured-balancing-capacity-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "A01",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "capacity-1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 500.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "10YGB----------A",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "",
    "document_mrid": "fixture-procured-balancing-capacity-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "",
    "market_agreement_type": "A01",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "capacity-1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 525.0
   }
  ]
 },
 "water_reservoirs_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-water-reservoirs-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 18000.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-15T01:00:00Z",
    "document_mrid": "fixture-water-reservoirs-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "",
    "production_type": "",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 17950.0
   }
  ]
 },
 "wind_solar_forecast_gb.xml": {
  "price.amount": [],
  "quantity": [
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ws-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 3200.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ws-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B19",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "1",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 3400.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ws-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B18",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T00:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 7800.0
   },
   {
    "acquiring_domain": "",
    "area_domain": "",
    "asset_mrid": "",
    "asset_name": "",
    "business_type": "A01",
    "connecting_domain": "",
    "control_area_domain": "",
    "currency_unit": "",
    "document_created_at": "2024-01-14T12:00:00Z",
    "document_mrid": "fixture-ws-fc-gb-20240115",
    "document_status": "",
    "flow_direction": "",
    "in_domain": "10YGB----------A",
    "market_agreement_type": "",
    "original_market_product": "",
    "out_domain": "10YGB----------A",
    "production_type": "B18",
    "reason_code": "",
    "resolution": "PT60M",
    "revision_number": "1",
    "standard_market_product": "",
    "timeseries_mrid": "2",
    "timestamp_utc": "2024-01-15T01:00:00+00:00",
    "unit_mrid": "",
    "unit_name": "",
    "value": 8100.0
   }
  ]
 }
}
"""
B1_BASELINE: dict[str, dict[str, list[dict[str, Any]]]] = json.loads(_B1_BASELINE_JSON)


# ---------------------------------------------------------------------------
# D-9a inline byte-literal documents. Never committed as files under
# tests/fixtures/ (FM-19): a guard-tripping or mixed-envelope fixture would
# make B4 (corpus-silence invariant) permanently red.
# ---------------------------------------------------------------------------

#: A6 (D-9): a Balancing_MarketDocument root (NOT ReserveBid) carrying a
#: <Bid_TimeSeries> element. Under the default (non-ReserveBid) accepted set
#: {"TimeSeries"}, Bid_TimeSeries never matches -- matched_series stays 0 --
#: but it IS series-shaped (its local name ends with "TimeSeries"), so the
#: D-9 guard must fire. Today (pre-fix) there is no guard at all: 0 records,
#: no log line (FM-18 requires this NOT be the A37 fixture, which matches
#: 100% post-fix and cannot exercise the guard, F-11).
GUARD_TRIP_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:0">
  <mRID>inline-guard-trip-doc</mRID>
  <Bid_TimeSeries>
    <mRID>trip-1</mRID>
  </Bid_TimeSeries>
</Balancing_MarketDocument>
"""

#: B2: a bare <TimeSeries> that IS matched (default accepted set) but
#: carries only a Reason and no Period/Point -- the A87-shaped case (F-9).
#: matched_series becomes 1, so the D-9 guard must NOT fire even though 0
#: records are returned.
MATCHED_NO_POINTS_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:0">
  <mRID>inline-a87-shaped-doc</mRID>
  <TimeSeries>
    <mRID>reason-only-1</mRID>
    <Reason>
      <code>999</code>
    </Reason>
  </TimeSeries>
</Balancing_MarketDocument>
"""

#: A7 / D-10 (Sol finding 4): a ReserveBid root carrying BOTH accepted
#: series names -- one <Bid_TimeSeries> (quantity 11) and one bare
#: <TimeSeries> (quantity 22). Measured today (F-12): 1 record,
#: ('t-1', 22.0) -- the Bid_TimeSeries half is silently dropped, no log at
#: all. D-10 must refuse the whole document instead: 0 records, ERROR
#: naming both names.
MIXED_ENVELOPE_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-mixed-doc</mRID>
  <Bid_TimeSeries>
    <mRID>b-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity.quantity>11</quantity.quantity>
      </Point>
    </Period>
  </Bid_TimeSeries>
  <TimeSeries>
    <mRID>t-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>22</quantity>
      </Point>
    </Period>
  </TimeSeries>
</ReserveBid_MarketDocument>
"""

#: B3 (Sol finding 4, the other half of D-10's required property): a
#: ReserveBid root carrying ONLY a bare <TimeSeries> must still parse (G-1)
#: -- pins the accepted set's breadth so a future maintainer cannot restore
#: an exclusive swap and re-create N-21 (FM-15).
BARE_TIMESERIES_ONLY_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-bare-only-doc</mRID>
  <TimeSeries>
    <mRID>bare-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>9</quantity>
      </Point>
    </Period>
  </TimeSeries>
</ReserveBid_MarketDocument>
"""

#: A10 / Sec 9.5: a ReserveBid Point with exactly ONE value-bearing child,
#: `quantity.quantity`, and no price element of any spelling -- every
#: element name observed in `entsoe_A37_extracted.xml` (D-4c). Ordering-
#: immune by construction: there is nothing for element order to decide.
#: Cross-constrained against a "return [] for everything" impl by A1, B3 and
#: this document's own assertion 1 (value_tag="quantity" -> 1 record, 7.0).
A10_SCOPED_ALIAS_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-a10-scoped-alias-doc</mRID>
  <Bid_TimeSeries>
    <mRID>a10-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-06-01T00:00Z</start>
        <end>2026-06-01T00:15Z</end>
      </timeInterval>
      <resolution>PT15M</resolution>
      <Point>
        <position>1</position>
        <quantity.quantity>7</quantity.quantity>
      </Point>
    </Period>
  </Bid_TimeSeries>
</ReserveBid_MarketDocument>
"""

#: A11 / Sec 9.5 (Sol pass-3, FM-22): a ReserveBid Point presenting BOTH
#: accepted spellings of the same value under value_tag="quantity" --
#: <quantity>11</quantity> and <quantity.quantity>22</quantity.quantity>.
#: This shape is ADVERSARIAL and NEVER OBSERVED (same posture as
#: MIXED_ENVELOPE_DOCUMENT, D-9a, FM-19) -- it is constructed to prove the
#: D-11 predicate is order-invariant, not to model a real vendor payload.
#: Measured RED at HEAD cd9e99d (F-15): quantity-first -> 1 record, 22.0;
#: quantity.quantity-first -> 1 record, 11.0; no log either way. Two
#: separate literals (rather than one document reordered at runtime) so the
#: committed bytes are the exact adversarial fixture in each ordering.
A11_AMBIGUOUS_POINT_QUANTITY_FIRST_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-a11-ambiguous-point-doc</mRID>
  <Bid_TimeSeries>
    <mRID>a11-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>11</quantity>
        <quantity.quantity>22</quantity.quantity>
      </Point>
    </Period>
  </Bid_TimeSeries>
</ReserveBid_MarketDocument>
"""

A11_AMBIGUOUS_POINT_QUANTITY_QUANTITY_FIRST_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-a11-ambiguous-point-doc</mRID>
  <Bid_TimeSeries>
    <mRID>a11-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity.quantity>22</quantity.quantity>
        <quantity>11</quantity>
      </Point>
    </Period>
  </Bid_TimeSeries>
</ReserveBid_MarketDocument>
"""

#: D-11 (Sol review, finding 1): a <Point> presenting the SAME accepted
#: value tag TWICE (rather than two DIFFERENT accepted spellings, as A11
#: does) must also be refused. D-11 is specified to count occurrences of
#: accepted value tags, not distinct names -- a set-based implementation
#: (``seen.add(tag); if len(seen) > 1: return []``) would pass A10, both
#: A11 parametrizations and B5, yet silently accept this document via
#: last-write-wins/element-order dependence, which is the exact defect
#: D-11 exists to remove. The two duplicate values (11 and 22) are
#: deliberately UNEQUAL so a last-write-wins implementation is observable
#: (a wrong implementation would return 1 record with value 22.0, not an
#: error) rather than passing by coincidence of the values matching.
DUPLICATE_QUANTITY_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-duplicate-quantity-doc</mRID>
  <Bid_TimeSeries>
    <mRID>dup-quantity-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>11</quantity>
        <quantity>22</quantity>
      </Point>
    </Period>
  </Bid_TimeSeries>
</ReserveBid_MarketDocument>
"""

#: D-11 (Sol review, finding 1): the ``quantity.quantity`` counterpart of
#: DUPLICATE_QUANTITY_DOCUMENT above -- two accepted-alias children of the
#: SAME spelling, reversed values (22 then 11) relative to the sibling
#: fixture so neither fixture's pass/fail depends on which value happens
#: to land last across the pair.
DUPLICATE_QUANTITY_QUANTITY_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<ReserveBid_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:2">
  <mRID>inline-duplicate-quantity-quantity-doc</mRID>
  <Bid_TimeSeries>
    <mRID>dup-quantity-quantity-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity.quantity>22</quantity.quantity>
        <quantity.quantity>11</quantity.quantity>
      </Point>
    </Period>
  </Bid_TimeSeries>
</ReserveBid_MarketDocument>
"""

#: B5 / Sec 9.5: reproduces the shape measured at
#: `.planning/phases/R3-test-integrity/probes/entsoe_A15_extracted.xml:35-39`
#: (the probe is git-ignored, F-8, so only the SHAPE is inlined here) -- a
#: real Balancing_MarketDocument Point carrying <quantity> and
#: <procurement_Price.amount> as siblings, selected by DIFFERENT value_tag
#: values rather than multi-matching under either one (F-17). This is the
#: executable proof that D-11 does not reach the general suffix-rule path.
B5_SUFFIX_SIBLING_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<Balancing_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:0">
  <mRID>inline-b5-suffix-sibling-doc</mRID>
  <TimeSeries>
    <mRID>b5-1</mRID>
    <connecting_Domain.mRID>10Y-TEST</connecting_Domain.mRID>
    <Period>
      <timeInterval>
        <start>2026-01-01T00:00Z</start>
        <end>2026-01-01T01:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point>
        <position>1</position>
        <quantity>1</quantity>
        <procurement_Price.amount>0.11</procurement_Price.amount>
      </Point>
    </Period>
  </TimeSeries>
</Balancing_MarketDocument>
"""


class TestGroupAReservebidEnvelope:
    """GROUP A -- RED on master @ e7be850, GREEN after Task 2."""

    def test_a37_parses_with_quantity_value_tag(self) -> None:
        """A1: the committed A37 fixture parses to 3 records through the
        transformer's own value_tag ('quantity'), with the exact F-3a
        values -- not merely a non-empty result (D-3, Sec 3c). RED today:
        0 records (FM-1/FM-2, the two-layer defect)."""
        records = parse_timeseries_xml(A37_FIXTURE.read_bytes(), value_tag="quantity")

        assert [record["value"] for record in records] == F3A_QUANTITIES
        assert [record["timeseries_mrid"] for record in records] == F3A_MRIDS

    def test_a37_record_fields(self) -> None:
        """A2: the first record carries the F-3a domain/type fields
        alongside the exact quantity value -- a price-sourced or
        wrong-column implementation fails on ``value``."""
        records = parse_timeseries_xml(A37_FIXTURE.read_bytes(), value_tag="quantity")

        first = records[0]
        assert first["connecting_domain"] == "10Y1001A1001A82H"
        assert first["business_type"] == "B74"
        assert first["flow_direction"] == "A02"
        assert first["standard_market_product"] == "A07"
        assert first["resolution"] == "PT15M"
        assert first["currency_unit"] == "EUR"
        assert first["timestamp_utc"] == datetime(2026, 6, 1, tzinfo=UTC)
        assert first["value"] == 4.0

    def test_a37_rows_are_conserved_through_transform(self) -> None:
        """A3 / D-7 (Sol finding 1): transformed height equals parsed
        height -- NOT an ``n_unique(bid_mrid) == height`` identity, which is
        tautological after ``.unique()`` on a subset containing bid_mrid and
        cannot detect a collapse. A row-losing dedup fails this even though
        it would pass the tautological version."""
        parsed = parse_timeseries_xml(A37_FIXTURE.read_bytes(), value_tag="quantity")
        raw_df = pl.DataFrame(parsed)
        transformer = BalancingEnergyBidsTransformer.__new__(BalancingEnergyBidsTransformer)
        result = transformer.transform(raw_df)

        assert result.height == len(parsed) == 3
        assert sorted(result["quantity_mw"].to_list()) == [4.0, 4.0, 4.0]
        assert set(result.columns) == set(BalancingEnergyBidsTransformer.output_cols)

    def test_a37_rows_are_retained_by_event_window_filter(self) -> None:
        """A4: the recorded request window's lower edge coincides exactly
        with the delivery instant (F-5); HALF_OPEN retains it. An empty
        frame would satisfy ``dropped == 0`` too (Sol finding 3) -- the
        height assertions before AND after are what an empty frame cannot
        satisfy."""
        parsed = parse_timeseries_xml(A37_FIXTURE.read_bytes(), value_tag="quantity")
        raw_df = pl.DataFrame(parsed)
        transformer = BalancingEnergyBidsTransformer.__new__(BalancingEnergyBidsTransformer)
        result_df = transformer.transform(raw_df)
        assert result_df.height == 3

        window = RequestWindow(
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        result = exclude_out_of_window(result_df, "timestamp_utc", window)

        assert result.frame.height == 3
        assert result.dropped == 0
        assert result.unclassified == 0
        assert result.all_dropped is False

    def test_a37_rows_all_drop_loudly_when_window_starts_later(self) -> None:
        """A5: shifting the window start past the delivery instant drops
        100% of the rows -- loud (``all_dropped``), pinning the mechanism
        without re-implementing it (F-5's adversarial row)."""
        parsed = parse_timeseries_xml(A37_FIXTURE.read_bytes(), value_tag="quantity")
        raw_df = pl.DataFrame(parsed)
        transformer = BalancingEnergyBidsTransformer.__new__(BalancingEnergyBidsTransformer)
        result_df = transformer.transform(raw_df)
        assert result_df.height == 3

        window = RequestWindow(
            start=datetime(2026, 6, 1, 0, 15, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
            param_names=("periodStart", "periodEnd"),
        )
        result = exclude_out_of_window(result_df, "timestamp_utc", window)

        assert result.dropped == 3
        assert result.all_dropped is True

    def test_zero_match_with_series_shaped_elements_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A6 / D-9: a populated document that matches NO series element at
        all must be LOUD -- the exact class N-21 belongs to (a 144KB payload
        parsing to zero rows with no signal, G-2). RED today: ``[]`` with no
        log record at all. Must NOT use the A37 fixture (F-11, FM-18): it
        matches 100% post-fix and can never trip this guard."""
        with caplog.at_level("WARNING", logger="gridflow.connectors.entsoe.parsers"):
            records = parse_timeseries_xml(GUARD_TRIP_DOCUMENT, value_tag="quantity")

        assert records == []
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("Bid_TimeSeries" in r.getMessage() for r in warnings), (
            f"expected a WARNING naming Bid_TimeSeries; got: {[r.getMessage() for r in warnings]}"
        )

    def test_mixed_reservebid_envelope_is_refused_loudly(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A7 / D-10 (Sol finding 4): a ReserveBid document presenting BOTH
        accepted series names must be refused -- zero records, ERROR naming
        both. Neither a swap (silently drops one) nor a naive union
        (silently inflates or, on colliding keys, silently corrupts via
        ``keep="last"``) is safe (F-12, measured both directions). RED
        today: 1 record ``('t-1', 22.0)`` and no log at all."""
        with caplog.at_level("ERROR", logger="gridflow.connectors.entsoe.parsers"):
            records = parse_timeseries_xml(MIXED_ENVELOPE_DOCUMENT, value_tag="quantity")

        assert records == []
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert any(
            "Bid_TimeSeries" in r.getMessage() and "TimeSeries" in r.getMessage() for r in errors
        ), f"expected an ERROR naming both series names; got: {[r.getMessage() for r in errors]}"

    def test_transformer_selects_its_own_value_tag(self, tmp_path: Path) -> None:
        """A8 (Sol finding 2b): exercises ``read_bronze()`` choosing
        ``self.value_tag`` itself, rather than the test naming it (as A1
        does) -- the only test that goes through the real transformer's own
        bronze-read path."""
        assert BalancingEnergyBidsTransformer.value_tag == "quantity"

        _write_entsoe_partition(
            tmp_path,
            source="entsoe",
            dataset="balancing_energy_bids",
            partition_date=date(2026, 6, 1),
            body=A37_FIXTURE.read_bytes(),
        )
        transformer = BalancingEnergyBidsTransformer(tmp_path)
        raw_df = transformer.read_bronze(date(2026, 6, 1))

        assert raw_df.height == 3
        assert sorted(raw_df["value"].to_list()) == [4.0, 4.0, 4.0]

    def test_reservebid_honours_a_non_quantity_value_tag(self) -> None:
        """A9 (Sec 9.4/9.5 -- SUPERSEDED): this docstring previously claimed
        A9 was "the ONLY constraint that the ReserveBid value-tag alias
        stays scoped to the value_tag the caller actually requested." That
        claim is FALSIFIED (Sol pass-3): all three <Point>s in the A37
        fixture place quantity.quantity BEFORE energy_Price.amount (F-14),
        and parsers.py:471-484 overwrites `value` per matching child, so an
        implementation that also matches quantity.quantity under
        value_tag="price.amount" still returns the LATER price and passes
        this assertion unchanged.

        A9 is demoted to a POSITIVE CONTROL: the end-to-end proof that the
        suffix rule (`_matches_value_tag`) correctly sources
        energy_Price.amount on real vendor bytes. It keeps every assertion
        it had. The actual scoping gate is A10
        (test_reservebid_alias_is_scoped_to_the_requested_value_tag), which
        is ordering-immune by construction; the ordering ambiguity itself is
        closed by D-11 and proven order-invariant by A11."""
        records = parse_timeseries_xml(A37_FIXTURE.read_bytes(), value_tag="price.amount")

        assert [record["value"] for record in records] == F3A_PRICES

    def test_reservebid_alias_is_scoped_to_the_requested_value_tag(self) -> None:
        """A10 (Sec 9.5): the scoping gate that supersedes A9's falsified
        claim. The document's Point has exactly ONE value-bearing child,
        quantity.quantity, and no price element -- ordering-immune by
        construction, since no permutation of the document changes which
        child wins. Assertion 1 (value_tag="quantity" -> 1 record, 7.0) is
        what stops assertion 2 (value_tag="price.amount" -> []) from being
        satisfiable by "the document is unparseable"; the two must be in the
        SAME test so they cannot drift apart. Honest limitation (not a new
        finding): assertion 2's zero records are produced silently --
        matched_series == 1 so D-9 correctly stays quiet, and the Point is
        skipped by the pre-existing `value is None` branch with no counter.
        That is R-3's accepted class, exercised here on a deliberately
        contrived document; it is not a production hazard for caller 1,
        whose value_tag is pinned to "quantity" and asserted by A8."""
        records_quantity = parse_timeseries_xml(A10_SCOPED_ALIAS_DOCUMENT, value_tag="quantity")
        assert len(records_quantity) == 1
        assert records_quantity[0]["value"] == 7.0

        records_price = parse_timeseries_xml(A10_SCOPED_ALIAS_DOCUMENT, value_tag="price.amount")
        assert records_price == []

    @pytest.mark.parametrize(
        "document",
        [
            pytest.param(A11_AMBIGUOUS_POINT_QUANTITY_FIRST_DOCUMENT, id="quantity-first"),
            pytest.param(
                A11_AMBIGUOUS_POINT_QUANTITY_QUANTITY_FIRST_DOCUMENT,
                id="quantity.quantity-first",
            ),
        ],
    )
    def test_reservebid_point_with_two_accepted_value_tags_is_refused_in_either_order(
        self, document: bytes, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A11 / D-11 (Sec 9.3, FM-22, Sol pass-3): a <Point> presenting MORE
        THAN ONE accepted value tag (here, both `quantity` and
        `quantity.quantity` under value_tag="quantity") must be refused --
        zero records, ERROR naming both spellings -- in EITHER child
        ordering. This shape is ADVERSARIAL and NEVER OBSERVED (same posture
        as MIXED_ENVELOPE_DOCUMENT, D-9a, FM-19); it exists to prove the
        predicate is order-invariant, not to model a real payload. RED at
        HEAD cd9e99d (F-15): 1 record, 22.0 (quantity-first) or 11.0
        (quantity.quantity-first), with NO log either way -- the load-
        bearing RED reason is the missing ERROR, not the record count
        (`records == []` already holds on master @ e7be850, F-15a). Both
        assertions are load-bearing for that reason. Cross-constrained
        against a "refuse every ReserveBid" implementation by A1 (3
        records), B3 (1 record, 9.0) and A10 assertion 1 (1 record, 7.0)."""
        with caplog.at_level("ERROR", logger="gridflow.connectors.entsoe.parsers"):
            records = parse_timeseries_xml(document, value_tag="quantity")

        assert records == []
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        got = [r.getMessage() for r in errors]
        assert any(
            "quantity" in r.getMessage() and "quantity.quantity" in r.getMessage() for r in errors
        ), f"expected an ERROR naming both quantity and quantity.quantity; got: {got}"

    @pytest.mark.parametrize(
        "document",
        [
            pytest.param(DUPLICATE_QUANTITY_DOCUMENT, id="duplicate-quantity"),
            pytest.param(DUPLICATE_QUANTITY_QUANTITY_DOCUMENT, id="duplicate-quantity.quantity"),
        ],
    )
    def test_reservebid_point_with_duplicate_identical_value_tag_is_refused(
        self, document: bytes, caplog: pytest.LogCaptureFixture
    ) -> None:
        """D-11 (Sol review, finding 1): a <Point> presenting the SAME
        accepted value tag TWICE must be refused exactly like A11's two
        DIFFERENT accepted spellings -- D-11 is specified to count
        occurrences of accepted value tags, not distinct names. A
        set-based implementation (``seen.add(tag); if len(seen) > 1:
        return []``) would pass A10, both A11 parametrizations and B5
        while silently accepting this document via last-write-wins
        (returning 1 record with the LATER duplicate's value), which is
        the exact defect D-11 exists to remove. The fixture's two
        duplicate values are unequal so that failure mode is observable
        rather than passing by coincidence."""
        with caplog.at_level("ERROR", logger="gridflow.connectors.entsoe.parsers"):
            records = parse_timeseries_xml(document, value_tag="quantity")

        assert records == []
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        got = [r.getMessage() for r in errors]
        assert any(
            "quantity" in r.getMessage() and "quantity.quantity" in r.getMessage() for r in errors
        ), f"expected an ERROR naming both quantity and quantity.quantity; got: {got}"


class TestGroupBInvariants:
    """GROUP B -- invariants, GREEN before and after Task 2."""

    def test_non_reservebid_documents_are_unchanged(self) -> None:
        """B1 / D-2 point 3 / V-2: for every committed fixture whose root is
        NOT ReserveBid_MarketDocument, the full parsed record list (every
        field, not merely a count -- FM-14) is identical to the pre-Task-1
        baseline, under both value_tag values. The ReserveBid-root exclusion
        set must be exactly the two ReserveBid fixtures -- it cannot
        silently grow to swallow an unrelated regression."""
        skipped: list[str] = []
        checked: list[str] = []
        for fixture_path in sorted(FIXTURES.glob("*.xml")):
            root_name = _root_local_name(fixture_path.read_bytes())
            if root_name == "ReserveBid_MarketDocument":
                skipped.append(fixture_path.name)
                continue
            checked.append(fixture_path.name)
            baseline_entry = B1_BASELINE[fixture_path.name]
            for value_tag in ("quantity", "price.amount"):
                actual = _serialize_records(
                    parse_timeseries_xml(fixture_path.read_bytes(), value_tag=value_tag)
                )
                assert actual == baseline_entry[value_tag], (
                    f"{fixture_path.name} ({value_tag}): parsed records diverged from "
                    "the pre-Task-1 baseline (D-2 point 3)"
                )

        assert checked, "sanity: at least one non-ReserveBid fixture must be exercised"
        assert set(skipped) == {
            "balancing_energy_bids_a37_reservebid_de_20260601.xml",
            "balancing_energy_bids_gb.xml",
        }, "the ReserveBid-root exclusion set must not silently grow (D-2)"

    def test_matched_series_without_points_does_not_warn(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """B2: a bare <TimeSeries> that IS matched but carries only a
        Reason and no Point (the A87 shape, F-9) must never trigger the D-9
        guard -- counting MATCHES, not records, is what makes the guard
        precise (FM-16). Green vacuously today; the claim is that it stays
        green after the guard lands."""
        with caplog.at_level("WARNING", logger="gridflow.connectors.entsoe.parsers"):
            records = parse_timeseries_xml(MATCHED_NO_POINTS_DOCUMENT, value_tag="quantity")

        assert records == []
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert not any("matched 0 series" in r.getMessage() for r in warnings), (
            f"a matched-but-pointless series must not trip the zero-match guard; "
            f"got: {[r.getMessage() for r in warnings]}"
        )

    def test_reservebid_with_only_bare_timeseries_parses(self) -> None:
        """B3: a ReserveBid document carrying ONLY a bare <TimeSeries> must
        still parse -- 1 record with the exact value 9.0, not merely
        non-empty. Pins the accepted set's breadth so a future maintainer
        cannot restore an exclusive swap and re-create N-21 (FM-15);
        cross-constrains A7 against a "refuse all ReserveBid" design.
        Measured green today (F-11)."""
        records = parse_timeseries_xml(BARE_TIMESERIES_ONLY_DOCUMENT, value_tag="quantity")

        assert len(records) == 1
        assert records[0]["value"] == 9.0

    def test_guard_is_silent_across_the_committed_corpus(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """B4 / V-8: parsing every committed fixture under both value_tag
        values must trip ZERO D-9 guard warnings, ZERO D-10 mixed-envelope
        errors AND ZERO D-11 value-tag-ambiguity errors (Sec 9.4/9.5) --
        neither committed ReserveBid fixture is mixed and no <Point> in
        either carries two accepted value tags (F-16). Vacuously green
        today (no guard exists yet); the real claim is post-fix, and it is
        what forbids ever committing a guard-tripping, mixed or value-tag-
        ambiguous fixture (FM-19)."""
        with caplog.at_level("WARNING", logger="gridflow.connectors.entsoe.parsers"):
            for fixture_path in sorted(FIXTURES.glob("*.xml")):
                data = fixture_path.read_bytes()
                for value_tag in ("quantity", "price.amount"):
                    parse_timeseries_xml(data, value_tag=value_tag)

        guard_hits = [
            r
            for r in caplog.records
            if r.levelname in ("WARNING", "ERROR") and "N-21" in r.getMessage()
        ]
        assert guard_hits == [], (
            "expected zero D-9/D-10/D-11 diagnostics across the committed corpus; got: "
            f"{[(r.levelname, r.getMessage()) for r in guard_hits]}"
        )

        d11_hits = [
            r for r in caplog.records if r.levelname == "ERROR" and "D-11" in r.getMessage()
        ]
        assert d11_hits == [], (
            f"expected zero D-11 errors across the committed corpus; got: "
            f"{[r.getMessage() for r in d11_hits]}"
        )

    def test_general_value_tag_suffix_matching_is_unchanged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """B5 (Sec 9.5): the executable statement that D-11 does NOT reach
        the general suffix-rule path -- the boundary R-6 sits on. Reproduces
        the shape measured at
        `.planning/phases/R3-test-integrity/probes/entsoe_A15_extracted.xml:35-39`
        (the probe is git-ignored, F-8, so only the shape is inlined): a
        real Balancing_MarketDocument Point carrying <quantity> and
        <procurement_Price.amount> as siblings. The two candidate spellings
        are selected by DIFFERENT value_tag values, so the Point never
        multi-matches (F-17) -- 1 record under each value_tag, no ERROR and
        no WARNING either way. Without this test, a future executor could
        "simplify" D-11 by dropping the `len(accepted_value_tags) > 1` gate
        and only V-8's corpus sweep would notice."""
        with caplog.at_level("WARNING", logger="gridflow.connectors.entsoe.parsers"):
            records_quantity = parse_timeseries_xml(
                B5_SUFFIX_SIBLING_DOCUMENT, value_tag="quantity"
            )
            records_price = parse_timeseries_xml(
                B5_SUFFIX_SIBLING_DOCUMENT, value_tag="price.amount"
            )

        assert len(records_quantity) == 1
        assert records_quantity[0]["value"] == 1.0
        assert len(records_price) == 1
        assert records_price[0]["value"] == 0.11

        diagnostics = [r for r in caplog.records if r.levelname in ("WARNING", "ERROR")]
        assert diagnostics == [], (
            f"expected no WARNING/ERROR from the parser logger; got: "
            f"{[(r.levelname, r.getMessage()) for r in diagnostics]}"
        )
