"""Silver transformer for Elexon Net Balancing Services Adjustment Data (NETBSAD)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonNETBSAD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer

# G5-W1.1 (2026-05): the live API replaced the four coarse adjustment fields
# with eight finer ones (cost vs volume, energy vs system, buy vs sell). They
# are a different decomposition, not aggregations of the old four, so silver
# carries both sets and the bronze era decides which is populated.
_ADJUSTMENT_COLUMNS = (
    # Legacy 4 (pre-2026 bronze)
    "net_buy_price_adjustment",
    "net_sell_price_adjustment",
    "net_buy_volume_adjustment",
    "net_sell_volume_adjustment",
    # Current 8 (2026+ bronze), buy side
    "net_buy_price_cost_adjustment_energy",
    "net_buy_price_volume_adjustment_energy",
    "net_buy_price_volume_adjustment_system",
    "buy_price_price_adjustment",
    # Current 8, sell side
    "net_sell_price_cost_adjustment_energy",
    "net_sell_price_volume_adjustment_energy",
    "net_sell_price_volume_adjustment_system",
    "sell_price_price_adjustment",
)


class NETBSADTransformer(ElexonTransformer):
    """Transform Elexon NETBSAD data from bronze to silver."""

    source = "elexon"
    dataset = "netbsad"
    schema_cls = ElexonNETBSAD
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "netBuyPriceAdjustment": "net_buy_price_adjustment",
        "netSellPriceAdjustment": "net_sell_price_adjustment",
        "netBuyVolumeAdjustment": "net_buy_volume_adjustment",
        "netSellVolumeAdjustment": "net_sell_volume_adjustment",
        "netBuyPriceCostAdjustmentEnergy": "net_buy_price_cost_adjustment_energy",
        "netBuyPriceVolumeAdjustmentEnergy": "net_buy_price_volume_adjustment_energy",
        "netBuyPriceVolumeAdjustmentSystem": "net_buy_price_volume_adjustment_system",
        "buyPricePriceAdjustment": "buy_price_price_adjustment",
        "netSellPriceCostAdjustmentEnergy": "net_sell_price_cost_adjustment_energy",
        "netSellPriceVolumeAdjustmentEnergy": "net_sell_price_volume_adjustment_energy",
        "netSellPriceVolumeAdjustmentSystem": "net_sell_price_volume_adjustment_system",
        "sellPricePriceAdjustment": "sell_price_price_adjustment",
    }
    REQUIRED = ("settlement_date", "settlement_period")
    CASTS = dict.fromkeys(_ADJUSTMENT_COLUMNS, pl.Float64())
    OUTPUT_COLS = ("settlement_date", "settlement_period", "timestamp_utc", *_ADJUSTMENT_COLUMNS)


register_transformer("elexon", "netbsad", NETBSADTransformer)
