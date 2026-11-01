"""Silver transformer for Elexon Settlement Market Depth."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonMarketDepth
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer

_VOLUME_COLUMNS = (
    "indicated_imbalance_mwh",
    "offer_volume_mwh",
    "bid_volume_mwh",
    "total_accepted_offer_volume_mwh",
    "total_accepted_bid_volume_mwh",
    "priced_accepted_offers_volume_mwh",
    "priced_accepted_bids_volume_mwh",
)


class MarketDepthTransformer(ElexonTransformer):
    """Transform Elexon market depth data from bronze to silver."""

    source = "elexon"
    dataset = "market_depth"
    schema_cls = ElexonMarketDepth
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "indicatedImbalance": "indicated_imbalance_mwh",
        "offerVolume": "offer_volume_mwh",
        "bidVolume": "bid_volume_mwh",
        "totalAcceptedOfferVolume": "total_accepted_offer_volume_mwh",
        "totalAcceptedBidVolume": "total_accepted_bid_volume_mwh",
        "pricedAcceptedOffersVolume": "priced_accepted_offers_volume_mwh",
        "pricedAcceptedBidsVolume": "priced_accepted_bids_volume_mwh",
    }
    REQUIRED = ("settlement_date", "settlement_period")
    CASTS = dict.fromkeys(_VOLUME_COLUMNS, pl.Float64())
    OUTPUT_COLS = ("settlement_date", "settlement_period", "timestamp_utc", *_VOLUME_COLUMNS)


register_transformer("elexon", "market_depth", MarketDepthTransformer)
