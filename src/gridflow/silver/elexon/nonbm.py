"""Silver transformer for Elexon Non-BM STOR Generation (NONBM)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.schemas.elexon import ElexonNonBM
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# NONBM ignores these names and returns its latest record whatever the window
# (connectors/elexon/endpoints.py), so a body fetched with them is not a
# response for its partition. Such bodies stay in bronze (immutable).
_IGNORED_WINDOW_PARAMS = frozenset({"publishDateTimeFrom", "publishDateTimeTo"})


def _fetched_with_ignored_window(raw_path: Path) -> bool:
    """Return True when the body's sidecar records the ignored window params."""
    try:
        meta = json.loads(raw_path.with_suffix(".meta.json").read_text())
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(
            "NONBM: cannot read the sidecar for %s (%s); its request window is "
            "unknown, so the body is kept",
            raw_path,
            e,
        )
        return False
    params = meta.get("request_params") if isinstance(meta, dict) else None
    return isinstance(params, dict) and not _IGNORED_WINDOW_PARAMS.isdisjoint(params)


class NONBMTransformer(BaseSilverTransformer):
    """Transform Elexon NONBM data from bronze to silver."""

    source = "elexon"
    dataset = "nonbm"
    schema_cls = ElexonNonBM
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8: verbatim from unique() below

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.json")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                data = json.loads(f.read_text())
                records = data.get("data", []) if isinstance(data, dict) else data
                if _fetched_with_ignored_window(f):
                    self.last_excluded_row_count += len(records)
                    logger.warning(
                        "NONBM: excluded %d row(s) from %s, fetched with "
                        "publishDateTimeFrom/To, which the vendor ignores",
                        len(records),
                        f,
                    )
                    continue
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "publishTime": "published_at",
            "generation": "generation_mw",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period", "generation_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in NONBM: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
                pl.col("generation_mw").cast(pl.Float64),
            ]
        )

        df = df.with_columns(
            pl.struct(["settlement_date", "settlement_period"])
            .map_elements(
                lambda row: settlement_period_to_utc(
                    row["settlement_date"], row["settlement_period"]
                ),
                return_dtype=pl.Datetime("us", "UTC"),
            )
            .alias("timestamp_utc")
        )

        # G6 (W2.2 pattern): cast `published_at` to UTC datetime so the
        # column survives to silver well-typed.
        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )
        else:
            # WHY: the silver schema declares published_at as a nullable contract
            # column. Emit it as typed-null when bronze lacks the publish field so the
            # silver schema is deterministic and partition globs don't drift across
            # history (a missing column breaks SELECT * reads spanning files that do
            # carry it).
            df = df.with_columns(pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at"))

        df = df.unique(subset=["settlement_date", "settlement_period"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "settlement_date",
            "settlement_period",
            "timestamp_utc",
            "generation_mw",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "nonbm", NONBMTransformer)
