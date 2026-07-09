"""Runtime schema manifest for gridflow silver and serving relations."""

from __future__ import annotations

import importlib
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import get_transformer, list_transformers

if TYPE_CHECKING:
    from pydantic import BaseModel

RelationKind = Literal["silver", "gold", "serving_alias"]
DateColSqlType = Literal["DATE", "TIMESTAMPTZ"]
ColumnsSource = Literal["pydantic_schema", "declared_dynamic", "gold_sql", "serving_alias"]

BITEMPORAL_EXCLUDE: tuple[str, ...] = (
    "event_time",
    "available_at",
    "source_run_id",
    "dataset_version",
    "month",
    "year",
)

_SILVER_BITEMPORAL_COLUMNS: tuple[str, ...] = (
    "event_time",
    "available_at",
    "source_run_id",
    "dataset_version",
)

_PARTITION_COLUMNS: tuple[str, str] = ("year", "month")

# WHY: the vendor Elexon BOD endpoint is decommissioned. The transformer file is
# retained for historical tests and self-registers when imported directly, so the
# runtime manifest must ignore that legacy registry entry without weakening the
# strict date-column drift alarm for any other registered transformer.
DECOMMISSIONED_DATASETS: frozenset[tuple[str, str]] = frozenset({("elexon", "bod")})

# Ratified from gridflow_models' F18 inventory on 2026-07-09, then filtered
# against the live registry after importing the six silver subpackages. The five
# serving seed aliases are represented separately in _SERVING_ALIASES. The old
# Elexon BOD endpoint remains intentionally absent: src/gridflow/silver/elexon
# keeps the transformer file but does not import/register it because the vendor
# endpoint is decommissioned. elexon/system_prices was present only as a seed row
# in the models inventory, so its settlement_date convention is carried here as
# the sole registered transformer whose date column was derived after seed
# removal.
DESIGNATED_DATE_COLS: dict[tuple[str, str], str] = {
    ("elexon", "agpt"): "settlement_date",
    ("elexon", "agws"): "settlement_date",
    ("elexon", "atl"): "settlement_date",
    ("elexon", "bmunits_reference"): "ingested_at",
    ("elexon", "boal"): "settlement_date",
    ("elexon", "disbsad"): "settlement_date",
    ("elexon", "fou2t14d"): "settlement_date",
    ("elexon", "freq"): "timestamp_utc",
    ("elexon", "fuelhh"): "settlement_date",
    ("elexon", "fuelinst"): "timestamp_utc",
    ("elexon", "imbalngc"): "settlement_date",
    ("elexon", "inddem"): "settlement_date",
    ("elexon", "indgen"): "settlement_date",
    ("elexon", "indo"): "settlement_date",
    ("elexon", "indod"): "settlement_date",
    ("elexon", "itsdo"): "settlement_date",
    ("elexon", "lolpdrm"): "settlement_date",
    ("elexon", "market_depth"): "settlement_date",
    ("elexon", "melngc"): "settlement_date",
    ("elexon", "mid"): "settlement_date",
    ("elexon", "ndf"): "settlement_date",
    ("elexon", "ndfd"): "settlement_date",
    ("elexon", "netbsad"): "settlement_date",
    ("elexon", "nonbm"): "settlement_date",
    ("elexon", "pn"): "settlement_date",
    ("elexon", "remit"): "timestamp_utc",
    ("elexon", "soso"): "settlement_date",
    ("elexon", "system_prices"): "settlement_date",
    ("elexon", "temp"): "timestamp_utc",
    ("elexon", "tsdf"): "settlement_date",
    ("elexon", "tsdfd"): "timestamp_utc",
    ("elexon", "uou2t14d"): "settlement_date",
    ("elexon", "windfor"): "timestamp_utc",
    ("entsoe", "activated_balancing_prices"): "timestamp_utc",
    ("entsoe", "activated_balancing_qty"): "timestamp_utc",
    ("entsoe", "actual_generation"): "timestamp_utc",
    ("entsoe", "actual_generation_units"): "timestamp_utc",
    ("entsoe", "actual_load"): "timestamp_utc",
    ("entsoe", "aggregated_balancing_energy_bids"): "timestamp_utc",
    ("entsoe", "auction_revenue"): "timestamp_utc",
    ("entsoe", "balancing_energy_bids"): "timestamp_utc",
    ("entsoe", "balancing_financial_expenses_income"): "timestamp_utc",
    ("entsoe", "commercial_schedules"): "timestamp_utc",
    ("entsoe", "congestion_income"): "timestamp_utc",
    ("entsoe", "congestion_management_costs"): "timestamp_utc",
    ("entsoe", "contracted_reserves"): "timestamp_utc",
    ("entsoe", "countertrading"): "timestamp_utc",
    ("entsoe", "cross_border_flows"): "timestamp_utc",
    ("entsoe", "cross_zonal_balancing_capacity"): "timestamp_utc",
    ("entsoe", "current_balancing_state"): "timestamp_utc",
    ("entsoe", "day_ahead_prices"): "timestamp_utc",
    ("entsoe", "dc_link_intraday_transfer_limits"): "timestamp_utc",
    ("entsoe", "forecast_margin"): "timestamp_utc",
    ("entsoe", "generation_forecast"): "timestamp_utc",
    ("entsoe", "generation_units_master_data"): "implementation_datetime_utc",
    ("entsoe", "imbalance_prices"): "timestamp_utc",
    ("entsoe", "imbalance_volume"): "timestamp_utc",
    ("entsoe", "installed_capacity"): "timestamp_utc",
    ("entsoe", "installed_capacity_units"): "timestamp_utc",
    ("entsoe", "load_forecast"): "timestamp_utc",
    ("entsoe", "load_forecast_monthly"): "timestamp_utc",
    ("entsoe", "load_forecast_weekly"): "timestamp_utc",
    ("entsoe", "load_forecast_yearly"): "timestamp_utc",
    ("entsoe", "net_positions"): "timestamp_utc",
    ("entsoe", "net_transfer_capacity"): "timestamp_utc",
    ("entsoe", "offered_transfer_capacity_continuous"): "timestamp_utc",
    ("entsoe", "offered_transfer_capacity_explicit"): "timestamp_utc",
    ("entsoe", "offered_transfer_capacity_implicit"): "timestamp_utc",
    ("entsoe", "outages_consumption"): "timestamp_utc",
    ("entsoe", "outages_generation"): "timestamp_utc",
    ("entsoe", "outages_offshore_grid"): "timestamp_utc",
    ("entsoe", "outages_production"): "timestamp_utc",
    ("entsoe", "outages_transmission"): "timestamp_utc",
    ("entsoe", "procured_balancing_capacity"): "timestamp_utc",
    ("entsoe", "redispatching_cross_border"): "timestamp_utc",
    ("entsoe", "redispatching_internal"): "timestamp_utc",
    ("entsoe", "total_capacity_allocated"): "timestamp_utc",
    ("entsoe", "total_nominated_capacity"): "timestamp_utc",
    ("entsoe", "transfer_capacity_use"): "timestamp_utc",
    ("entsoe", "water_reservoirs"): "timestamp_utc",
    ("entsoe", "wind_solar_forecast"): "timestamp_utc",
    ("entsog", "aggregate_interconnections"): "ingested_at",
    ("entsog", "aggregated_physical_flows"): "timestamp_utc",
    ("entsog", "allocations"): "timestamp_utc",
    ("entsog", "available_through_oversubscription"): "timestamp_utc",
    ("entsog", "available_through_surrender"): "timestamp_utc",
    ("entsog", "available_through_uioli_long_term"): "timestamp_utc",
    ("entsog", "available_through_uioli_short_term"): "timestamp_utc",
    ("entsog", "balancing_zones"): "ingested_at",
    ("entsog", "cmp_auction_premiums"): "timestamp_utc",
    ("entsog", "cmp_unavailable_firm_capacity"): "timestamp_utc",
    ("entsog", "cmp_unsuccessful_requests"): "timestamp_utc",
    ("entsog", "connection_points"): "ingested_at",
    ("entsog", "firm_available"): "timestamp_utc",
    ("entsog", "firm_booked"): "timestamp_utc",
    ("entsog", "firm_technical"): "timestamp_utc",
    ("entsog", "gcv"): "timestamp_utc",
    ("entsog", "hydrogen_content"): "timestamp_utc",
    ("entsog", "interconnections"): "timestamp_utc",
    ("entsog", "interruptible_available"): "timestamp_utc",
    ("entsog", "interruptible_booked"): "timestamp_utc",
    ("entsog", "interruptible_total"): "timestamp_utc",
    ("entsog", "interruptions"): "timestamp_utc",
    ("entsog", "methane_content"): "timestamp_utc",
    ("entsog", "nominations"): "timestamp_utc",
    ("entsog", "operator_point_directions"): "timestamp_utc",
    ("entsog", "operators"): "timestamp_utc",
    ("entsog", "oxygen_content"): "timestamp_utc",
    ("entsog", "physical_flows"): "timestamp_utc",
    ("entsog", "renominations"): "timestamp_utc",
    ("entsog", "tariff_simulations"): "timestamp_utc",
    ("entsog", "tariffs"): "timestamp_utc",
    ("entsog", "urgent_market_messages"): "timestamp_utc",
    ("entsog", "wobbe_index"): "timestamp_utc",
    ("gie_agsi", "about_listing"): "ingested_at",
    ("gie_agsi", "about_summary"): "ingested_at",
    ("gie_agsi", "news"): "ingested_at",
    ("gie_agsi", "news_item"): "gas_day",
    ("gie_agsi", "storage"): "gas_day",
    ("gie_agsi", "storage_reports"): "gas_day",
    ("gie_agsi", "unavailability"): "ingested_at",
    ("gie_alsi", "lng"): "gas_day",
    ("neso", "carbon_intensity"): "timestamp_utc",
    ("neso", "generation"): "timestamp_utc",
    ("neso", "generation_current"): "timestamp_utc",
    ("neso", "generation_pt24h"): "timestamp_utc",
    ("neso", "intensity_at"): "timestamp_utc",
    ("neso", "intensity_current"): "timestamp_utc",
    ("neso", "intensity_date"): "timestamp_utc",
    ("neso", "intensity_factors"): "ingested_at",
    ("neso", "intensity_fw24h"): "timestamp_utc",
    ("neso", "intensity_fw48h"): "timestamp_utc",
    ("neso", "intensity_period"): "timestamp_utc",
    ("neso", "intensity_pt24h"): "timestamp_utc",
    ("neso", "intensity_stats"): "timestamp_utc",
    ("neso", "intensity_stats_block"): "timestamp_utc",
    ("neso", "intensity_today"): "timestamp_utc",
    ("neso", "regional_current"): "timestamp_utc",
    ("neso", "regional_england"): "timestamp_utc",
    ("neso", "regional_intensity"): "timestamp_utc",
    ("neso", "regional_intensity_fw24h"): "timestamp_utc",
    ("neso", "regional_intensity_fw24h_postcode"): "timestamp_utc",
    ("neso", "regional_intensity_fw24h_regionid"): "timestamp_utc",
    ("neso", "regional_intensity_fw48h"): "timestamp_utc",
    ("neso", "regional_intensity_fw48h_postcode"): "timestamp_utc",
    ("neso", "regional_intensity_fw48h_regionid"): "timestamp_utc",
    ("neso", "regional_intensity_postcode"): "timestamp_utc",
    ("neso", "regional_intensity_pt24h"): "timestamp_utc",
    ("neso", "regional_intensity_pt24h_postcode"): "timestamp_utc",
    ("neso", "regional_intensity_pt24h_regionid"): "timestamp_utc",
    ("neso", "regional_intensity_regionid"): "timestamp_utc",
    ("neso", "regional_postcode"): "timestamp_utc",
    ("neso", "regional_regionid"): "timestamp_utc",
    ("neso", "regional_scotland"): "timestamp_utc",
    ("neso", "regional_wales"): "timestamp_utc",
    ("open_meteo", "forecast_demand"): "timestamp_utc",
    ("open_meteo", "forecast_solar"): "timestamp_utc",
    ("open_meteo", "forecast_wind"): "timestamp_utc",
    ("open_meteo", "historical_demand"): "timestamp_utc",
    ("open_meteo", "historical_solar"): "timestamp_utc",
    ("open_meteo", "historical_wind"): "timestamp_utc",
}


@dataclass(frozen=True)
class SilverSchemaEntry:
    """One exported schema-contract row.

    Attributes:
        source: Source namespace for source-owned relations, or ``None`` for
            cross-source serving/gold relations.
        dataset: Dataset or public serving handle name.
        relation_name: Preferred SQL relation name callers should query.
        relation_kind: Layer/kind of the relation.
        qualified_view: Source-qualified silver view name, when applicable.
        deprecated_alias: Legacy single-token silver alias, when unique.
        designated_date_col: Column to use for date-range filtering.
        date_col_sql_type: SQL type family for ``designated_date_col``.
        columns: Declared public columns, or ``None`` for dynamic schemas.
        columns_source: How ``columns`` was obtained.
        bitemporal_columns: Bitemporal columns physically carried by the relation.
        partition_columns: Storage partition columns physically carried by the relation.
    """

    source: str | None
    dataset: str
    relation_name: str
    relation_kind: RelationKind
    qualified_view: str | None
    deprecated_alias: str | None
    designated_date_col: str
    date_col_sql_type: DateColSqlType
    columns: tuple[str, ...] | None
    columns_source: ColumnsSource
    bitemporal_columns: tuple[str, ...]
    partition_columns: tuple[str, ...]


@dataclass(frozen=True)
class _ServingAliasSpec:
    """Internal declaration for a public SDK serving handle."""

    source: str | None
    dataset: str
    relation_name: str
    relation_kind: RelationKind
    designated_date_col: str


# ``weather`` is a pre-existing SDK-compat misnomer: GridflowClient.get_weather
# reads silver_elexon_itsdo, which serves GB Initial Transmission System Demand
# Outturn, not meteorological weather data.
_SERVING_ALIASES: tuple[_ServingAliasSpec, ...] = (
    _ServingAliasSpec(
        "elexon",
        "system_prices",
        "silver_elexon_system_prices",
        "serving_alias",
        "settlement_date",
    ),
    _ServingAliasSpec(
        "elexon",
        "fuel_generation",
        "silver_elexon_fuelhh",
        "serving_alias",
        "settlement_date",
    ),
    _ServingAliasSpec("gie_agsi", "gas_storage", "gold_eu_gas_storage", "gold", "gas_day"),
    _ServingAliasSpec(
        "elexon",
        "weather",
        "silver_elexon_itsdo",
        "serving_alias",
        "settlement_date",
    ),
    _ServingAliasSpec(
        None,
        "imbalance_context",
        "gold_uk_imbalance_context",
        "gold",
        "settlement_date",
    ),
)

_DATE_COL_SQL_TYPES: dict[str, DateColSqlType] = {
    "settlement_date": "DATE",
    "gas_day": "DATE",
    "timestamp_utc": "TIMESTAMPTZ",
    "implementation_datetime_utc": "TIMESTAMPTZ",
    "ingested_at": "TIMESTAMPTZ",
}

_SILVER_IMPORTS: tuple[str, ...] = (
    "gridflow.silver.elexon",
    "gridflow.silver.entsoe",
    "gridflow.silver.entsog",
    "gridflow.silver.gie",
    "gridflow.silver.neso",
    "gridflow.silver.openmeteo",
)


def get_silver_schema_manifest(
    *,
    include_serving_aliases: bool = True,
) -> tuple[SilverSchemaEntry, ...]:
    """Return the runtime silver schema manifest.

    Importing the source-specific silver packages has registration side effects:
    each package imports its transformer modules and populates the central
    registry. The imports are kept inside this function so importing
    ``gridflow.silver.schema_manifest`` for constants does not register the full
    silver fleet.

    Args:
        include_serving_aliases: Include public SDK serving/gold handle rows.

    Returns:
        Immutable manifest rows sorted by relation kind, source, and dataset.

    Raises:
        ValueError: If a registered transformer lacks a ratified date column or
            uses an unknown date-column SQL type.
    """

    _ensure_silver_transformers_registered()
    registered = sorted(
        transformer
        for transformer in list_transformers()
        if transformer not in DECOMMISSIONED_DATASETS
    )
    aliases = _deprecated_aliases(registered)
    entries = [_silver_entry(source, dataset, aliases) for source, dataset in registered]
    if include_serving_aliases:
        entries.extend(_serving_alias_entry(spec) for spec in _SERVING_ALIASES)
    return tuple(
        sorted(
            entries,
            key=lambda entry: (
                entry.relation_kind,
                entry.source or "",
                entry.dataset,
                entry.relation_name,
            ),
        )
    )


def silver_schema_manifest_frame(
    *,
    include_serving_aliases: bool = True,
) -> pl.DataFrame:
    """Return the runtime schema manifest as a Polars DataFrame.

    Args:
        include_serving_aliases: Include public SDK serving/gold handle rows.

    Returns:
        A Polars DataFrame with one row per manifest entry.
    """

    rows = []
    for entry in get_silver_schema_manifest(include_serving_aliases=include_serving_aliases):
        row = asdict(entry)
        row["columns"] = list(entry.columns) if entry.columns is not None else None
        row["bitemporal_columns"] = list(entry.bitemporal_columns)
        row["partition_columns"] = list(entry.partition_columns)
        rows.append(row)
    return pl.DataFrame(rows)


def _ensure_silver_transformers_registered() -> None:
    """Import all silver subpackages for registry side effects."""

    for module_name in _SILVER_IMPORTS:
        importlib.import_module(module_name)


def _silver_entry(
    source: str,
    dataset: str,
    aliases: dict[tuple[str, str], str | None],
) -> SilverSchemaEntry:
    date_col = _date_col_for(source, dataset)
    relation_name = f"silver_{source}_{dataset}"
    transformer = get_transformer(source, dataset, Path("__schema_manifest__"))
    columns, columns_source = _transformer_columns(transformer)
    return SilverSchemaEntry(
        source=source,
        dataset=dataset,
        relation_name=relation_name,
        relation_kind="silver",
        qualified_view=relation_name,
        deprecated_alias=aliases[(source, dataset)],
        designated_date_col=date_col,
        date_col_sql_type=_date_col_sql_type(date_col),
        columns=columns,
        columns_source=columns_source,
        bitemporal_columns=_SILVER_BITEMPORAL_COLUMNS,
        partition_columns=_transformer_partition_columns(transformer),
    )


def _serving_alias_entry(spec: _ServingAliasSpec) -> SilverSchemaEntry:
    columns: tuple[str, ...] | None
    columns_source: ColumnsSource
    if spec.relation_kind == "gold":
        columns = _gold_sql_columns(spec.relation_name)
        columns_source = "gold_sql"
    else:
        columns = None
        columns_source = "serving_alias"

    qualified_view = spec.relation_name if spec.relation_name.startswith("silver_") else None
    return SilverSchemaEntry(
        source=spec.source,
        dataset=spec.dataset,
        relation_name=spec.relation_name,
        relation_kind=spec.relation_kind,
        qualified_view=qualified_view,
        deprecated_alias=None,
        designated_date_col=spec.designated_date_col,
        date_col_sql_type=_date_col_sql_type(spec.designated_date_col),
        columns=columns,
        columns_source=columns_source,
        bitemporal_columns=(),
        partition_columns=(),
    )


def _date_col_for(source: str, dataset: str) -> str:
    key = (source, dataset)
    date_col = DESIGNATED_DATE_COLS.get(key)
    if date_col is None:
        raise ValueError(f"No designated date column registered for {source}/{dataset}")
    return date_col


def _date_col_sql_type(date_col: str) -> DateColSqlType:
    sql_type = _DATE_COL_SQL_TYPES.get(date_col)
    if sql_type is None:
        raise ValueError(f"No SQL type registered for designated date column {date_col!r}")
    return sql_type


def _deprecated_aliases(registered: list[tuple[str, str]]) -> dict[tuple[str, str], str | None]:
    dataset_counts: dict[str, int] = {}
    for _source, dataset in registered:
        dataset_counts[dataset] = dataset_counts.get(dataset, 0) + 1
    return {
        (source, dataset): f"silver_{dataset}" if dataset_counts[dataset] == 1 else None
        for source, dataset in registered
    }


def _transformer_columns(
    transformer: BaseSilverTransformer,
) -> tuple[tuple[str, ...] | None, ColumnsSource]:
    schema_cls: type[BaseModel] | None = transformer.schema_cls
    if schema_cls is None:
        return None, "declared_dynamic"
    return tuple(schema_cls.model_fields), "pydantic_schema"


def _transformer_partition_columns(transformer: BaseSilverTransformer) -> tuple[str, ...]:
    if getattr(transformer, "reference_dataset", False):
        return ()
    if type(transformer)._write_silver is BaseSilverTransformer._write_silver:
        return _PARTITION_COLUMNS
    if transformer.source == "elexon" and transformer.dataset == "bmunits_reference":
        return ()
    return _PARTITION_COLUMNS


def _gold_sql_columns(relation_name: str) -> tuple[str, ...]:
    view_name = relation_name.removeprefix("gold_")
    sql_path = Path(__file__).resolve().parents[1] / "gold" / "views" / f"{view_name}.sql"
    sql = sql_path.read_text(encoding="utf-8")
    match = re.search(r"\bSELECT\b(?P<select>.*?)\bFROM\b", sql, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        raise ValueError(f"Could not find SELECT list in {sql_path}")

    columns: list[str] = []
    for raw_line in match.group("select").splitlines():
        line = raw_line.split("--", maxsplit=1)[0].strip().rstrip(",")
        if not line:
            continue
        alias_match = re.search(r"\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)$", line, flags=re.IGNORECASE)
        if alias_match is not None:
            columns.append(alias_match.group(1))
            continue
        columns.append(line.rsplit(".", maxsplit=1)[-1].strip('"'))
    return tuple(columns)
