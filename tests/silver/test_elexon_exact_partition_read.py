"""G-1 gate: every Elexon ``read_bronze`` is exact-partition-only (R2-A Task 2).

Audited in the plan (S1.6): zero Elexon call sites for
``_bronze_path_for_date`` / ``_find_covering_bronze_partition`` /
``_bronze_date_dirs`` — every registered transformer (except
``bmunits_reference``, static reference data with no date dimension) builds
its exact bronze path literally in ``read_bronze()``. This test turns a
future covering-fallback regression into a CI failure (R-9) and is expected
to be GREEN on ``master`` (it pins a pre-existing property, not a R2-A fix).
"""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

import gridflow.silver.elexon  # noqa: F401 -- registers every elexon transformer
from gridflow.silver.elexon._publication_window import (
    PUBLICATION_WINDOW_EXEMPT,
    publication_window_params,
)
from gridflow.silver.registry import get_transformer, list_transformers
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

_COVERING_FALLBACK_EXEMPT = frozenset({"bmunits_reference"})
"""Static reference data with no date dimension -- reads across all dates by
design (``bmunits.py``'s ``rglob`` over the whole bronze tree), not a G-1
violation."""

# A single generic-but-realistic raw row, built from the union of every
# registered Elexon transformer's own column_mapping/required-columns (raw
# vendor field names on the left of each transform()'s rename_map). Seeding
# D-1 with THIS (rather than an empty ``{"data": []}``) means a reintroduced
# covering-partition fallback would not just leak a file into ``read_bronze``
# -- it would leak a row that survives that dataset's own ``transform()`` and
# shows up as a nonzero ``run()`` count, which an empty-payload seed could
# never prove (the previous seed passed identically whether or not the bug
# were present, since zero raw records always transform to zero rows).
#
# Verified (not universal by construction, documented honestly): produces a
# nonzero ``transform()`` output for 30 of the 32 datasets exercised by this
# loop (all except ``bmunits_reference``, separately exempted above). The two
# gaps are BOTH a pre-existing, real ambiguity in the dataset's OWN
# column_mapping, not a gap in this fixture: ``fou2t14d``/``uou2t14d`` map
# both ``forecastDate`` and ``settlementDate`` onto the same
# ``settlement_date`` target column, so a row carrying both raw field names
# at once (needed here so ``tsdfd``'s own ``forecastDate`` -> ``forecast_date``
# mapping still resolves) collides on rename for those two specifically --
# the same class of either/or column-alias ambiguity FIX 3
# (``wind_forecast.py``) closes for windfor's settlement-vs-timestamp shape.
# For those two datasets this loop's non-leak assertion remains exactly as
# strong as before (still asserts ``rows == 0``), just not upgraded into a
# genuine leak-detector -- a real gap, named rather than hidden.
_NON_EMPTY_ROW: dict[str, object] = {
    "settlementDate": "2026-07-10",
    "settlementPeriod": 1,
    "publishTime": "2026-07-10T23:00:00Z",
    "demand": 100.0,
    "generation": 100.0,
    "quantity": 100.0,
    "fuelType": "WIND",
    "bmUnit": "T_TEST-1",
    "elexonBmUnit": "T_TEST-1",
    "psrType": "B16",
    "dataProviderId": "APX",
    "midPrice": 50.0,
    "volume": 10.0,
    "indicatedImbalance": 5.0,
    "indicatedMargin": 5.0,
    "forecastDate": "2026-07-10",
    "contractIdentification": "C1",
    "mrid": "MRID1",
    "createdTime": "2026-07-10T23:00:00Z",
    "outputUsable": 100.0,
    "reportDateTime": "2026-07-10T23:00:00Z",
    "frequency": 50.0,
    "temperature": 10.0,
    "measurementDate": "2026-07-10",
    "systemSellPrice": 50.0,
    "systemBuyPrice": 55.0,
    "netImbalanceVolume": 5.0,
    "settlementRunType": "SF",
    "priceDerivationCode": "N",
    "acceptanceNumber": 1,
    "levelFrom": 10.0,
    "levelTo": 20.0,
    "bidPrice": 5.0,
    "offerPrice": 6.0,
    "id": "adj1",
    "boundary": "N",
    "netBuyPriceAdjustment": 0.0,
    "netSellPriceAdjustment": 0.0,
    "netBuyVolumeAdjustment": 0.0,
    "netSellVolumeAdjustment": 0.0,
    "offerVolume": 1.0,
    "bidVolume": 1.0,
    "totalAcceptedOfferVolume": 1.0,
    "totalAcceptedBidVolume": 1.0,
    "pricedAcceptedOffersVolume": 1.0,
    "pricedAcceptedBidsVolume": 1.0,
    "senderIdentification": "S1",
    "receiverIdentification": "R1",
    "resourceProvider": "RP1",
    "tradeDirection": "BUY",
    "tradeQuantity": 1.0,
    "tradePrice": 1.0,
    "traderUnit": "TU1",
    "startTime": "2026-07-10T23:00:00Z",
    "endTime": "2026-07-10T23:30:00Z",
    "transmissionSystemDemand": 100.0,
    "initialForecast": 100.0,
    "name": "Test Unit",
    "bmUnitName": "Test Unit",
    "registeredCapacity": 100.0,
    "generationCapacity": 100.0,
    "companyName": "Test Co",
    "leadPartyName": "Test Co",
    "gspGroupId": "G1",
    "nationalGridBmUnit": "T_TEST-1",
    "bidOfferPairNumber": 1,
    "deemedBoFlag": False,
    "soFlag": False,
    "storProviderFlag": False,
    "rrFlag": False,
    "timeFrom": "2026-07-10T23:00:00Z",
    "revisionNumber": 1,
    "messageType": "MT1",
    "messageHeading": "MH1",
    "eventType": "ET1",
    "unavailabilityType": "UT1",
    "participantId": "P1",
    "registrationCode": "RC1",
    "assetId": "A1",
    "assetType": "AT1",
    "affectedUnit": "AU1",
    "affectedUnitEIC": "EIC1",
    "biddingZone": "BZ1",
    "component": "C1",
    "cost": 1.0,
    "publishingPeriodCommencingTime": "2026-07-10T23:00:00Z",
    "lossOfLoadProbability": 0.1,
    "deratedMargin": 1.0,
    "normal": 10.0,
    "low": 5.0,
    "high": 15.0,
    "netBuyPriceCostAdjustmentEnergy": 0.0,
    "netBuyPriceVolumeAdjustmentEnergy": 0.0,
    "netBuyPriceVolumeAdjustmentSystem": 0.0,
    "buyPricePriceAdjustment": 0.0,
    "netSellPriceCostAdjustmentEnergy": 0.0,
    "netSellPriceVolumeAdjustmentEnergy": 0.0,
}


def _seed_minimal_partition(bronze_dir: Path) -> None:
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_20260710T000000Z_aaaa1111.json").write_text(
        json.dumps({"data": [_NON_EMPTY_ROW]})
    )


def test_no_elexon_transformer_reads_outside_its_exact_partition(tmp_path: Path) -> None:
    """G-1: seeding bronze at D-1 only must never leak into ``run(D)``."""
    registered = list_transformers("elexon")
    assert registered, "elexon transformers must be registered before this test runs"

    target_date = date(2026, 7, 11)
    prior_date = date(2026, 7, 10)

    checked = 0
    for source, dataset in registered:
        if dataset in _COVERING_FALLBACK_EXEMPT:
            continue
        case_dir = tmp_path / source / dataset
        _seed_minimal_partition(PathBuilder(case_dir).bronze_date_dir(source, dataset, prior_date))
        transformer = get_transformer(source, dataset, case_dir)
        rows = transformer.run(target_date, run_id="g1-gate")
        assert rows == 0, f"{source}/{dataset} leaked rows from a prior-day partition"
        assert not PathBuilder(case_dir).silver_file(source, dataset, target_date).exists()
        checked += 1

    assert checked == len(registered) - len(_COVERING_FALLBACK_EXEMPT)


def test_every_elexon_dataset_is_filtered_or_exempt_with_a_reason() -> None:
    """A-11: 33/33 registered datasets classify as either filtered or exempt.

    ``bod`` is excluded from this loop: it is UNREGISTERED by design
    (decommissioned by Elexon, ``EXCLUDED_ENDPOINTS``) in isolation, but
    other test modules import ``gridflow.silver.elexon.bod`` directly for
    its schema (not through ``elexon.__init__``), which triggers its
    module-level ``register_transformer`` side effect and leaks "bod" into
    the shared registry for the rest of a full-suite run. That leak is a
    pre-existing test-isolation gap unrelated to R2-A (out of scope here) --
    tolerated, not silently masked, by naming it explicitly.
    """
    registered = list_transformers("elexon")
    assert registered

    for _source, dataset in registered:
        if dataset == "bod":
            continue
        in_scope = publication_window_params(dataset) is not None
        exempt = dataset in PUBLICATION_WINDOW_EXEMPT
        assert in_scope or exempt, f"{dataset} is neither filtered nor exempt with a reason"
        assert not (in_scope and exempt), f"{dataset} is both in scope and exempt"

    assert len(PUBLICATION_WINDOW_EXEMPT) == 11
