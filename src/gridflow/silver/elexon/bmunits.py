"""Silver transformer for Elexon BM Unit reference data."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonBMUnit
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.storage.parquet import write_parquet

logger = logging.getLogger(__name__)


class BMUnitsTransformer(BaseSilverTransformer):
    """Transform Elexon BM Unit reference data from bronze to silver.

    This is reference data (no date dimension), so it writes a single
    file rather than date-partitioned Parquet.

    Note on timestamps: ``available_at`` is the authoritative bitemporal
    publication timestamp added by ``BaseSilverTransformer``; ``ingested_at``
    is retained for backward compatibility as the local processing
    timestamp. Under ``--reingest`` the two diverge.

    Null/empty ``bm_unit_id`` -- C-7 (ruled fail-hard by Bobbo 2026-08-16),
    NARROWED by Bobbo 2026-09-09 (ADR-032). ``bm_unit_id`` is this
    transformer's entity key (``ENTITY_KEY_COLUMNS = ("bm_unit_id",)``), so a
    null-key row still must never reach silver: it cannot be joined by any
    downstream consumer, and because the dedup below is ``keep="last"`` on
    that same key, two null-key rows would silently collapse into one.

    What changed is the disposition, not the guarantee. Keyless rows are now
    **dropped and logged at ERROR with their vendor identities**, instead of
    aborting the whole transform. C-7 protects the silver key space; it was
    never meant to require the vendor to be complete. This is not a bypass
    flag -- there is no way for a caller to let a keyless row through, and if
    *every* row is keyless the transform still raises, because that is a
    broken payload rather than a vendor gap.

    Rationale is in ADR-032; the measurement behind it is
    ``gridflow_models/.planning/phases/v1.9-S2-realised-residual-perfect-prog/
    BMUNITS-NULL-KEY-VERIFICATION.md``: 90 of 3060 rows keyless on
    2026-09-01, confirmed still null on a live 2026-09-09 call, and dropping
    them costs zero join coverage (every one of the 2467 distinct units in
    ``pn`` silver and all 386 in ``boal`` resolves against a keyed row).
    Bronze retains every dropped row, so nothing is destroyed.
    """

    source = "elexon"
    dataset = "bmunits_reference"
    schema_cls = ElexonBMUnit
    # 1.1.0: keyless vendor rows are dropped and logged rather than aborting
    # the transform (C-7 narrowed, ADR-032). A unit's absence from this dataset
    # no longer implies the vendor did not send it -- the version stamp is how a
    # consumer tells the two regimes apart.
    DATASET_VERSION: ClassVar[str] = "1.1.0"
    ENTITY_KEY_COLUMNS = ("bm_unit_id",)  # D-8: verbatim from unique() below

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        # Reference data has no date partitioning; read latest file from any date dir
        if not self.bronze_dir.exists():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        # Search all date directories for the most recent file
        for f in sorted(self.bronze_dir.rglob("raw_*.json"), reverse=True):
            if f.name.endswith(".meta.json"):
                continue
            try:
                data = json.loads(f.read_text())
                records = data.get("data", []) if isinstance(data, dict) else data
                rows.extend(records)
                break  # Use only the most recent file
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "bmUnit": "bm_unit_id",
            "elexonBmUnit": "bm_unit_id",
            "name": "bm_unit_name",
            "bmUnitName": "bm_unit_name",
            "fuelType": "fuel_type",
            "registeredCapacity": "registered_capacity_mw",
            "generationCapacity": "registered_capacity_mw",
            "companyName": "company_name",
            "leadPartyName": "company_name",
            "gspGroupId": "gsp_group_id",
            "nationalGridBmUnit": "national_grid_bm_unit",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        if "bm_unit_id" not in raw_df.columns:
            logger.error("Missing required column 'bm_unit_id' in BMUnits")
            return pl.DataFrame()

        df = raw_df.with_columns(pl.col("bm_unit_id").cast(pl.Utf8))

        # C-7 (ruled 2026-08-16), NARROWED 2026-09-09 -- see ADR-032 and the
        # class docstring. bm_unit_id is ENTITY_KEY_COLUMNS: a null-key row
        # cannot be joined downstream, and the keep="last" dedup a few lines
        # below would silently collapse two null-key rows into one. So it must
        # not reach silver -- but a vendor gap in a reference dataset should
        # not take down a dataset other joins depend on. Drop the keyless rows
        # here, before dedup, and log every identity so the drop is auditable
        # rather than silent.
        # `strip_chars()` widens C-7's original `== ""` to catch a whitespace-only
        # key. Deliberate, and it is this change that makes it necessary: under
        # fail-hard a single null aborted the whole transform, so in the real
        # payload NO row reached silver and a " " key was unreachable in
        # practice. Dropping the nulls and writing the rest makes it reachable --
        # it would become its own entity key, join against nothing, and two such
        # rows would collapse under the keep="last" dedup below, which is the
        # exact hazard C-7 exists to prevent. Measured no-op on today's data:
        # 0 whitespace-only and 0 padded keys in 3060 bronze rows.
        is_keyless = pl.col("bm_unit_id").is_null() | (pl.col("bm_unit_id").str.strip_chars() == "")
        keyless = df.filter(is_keyless)
        if not keyless.is_empty():
            if "national_grid_bm_unit" in keyless.columns:
                # A row keyless in BOTH identity fields has no vendor identity at
                # all; say so rather than logging the literal string "None",
                # which reads as a unit named None.
                identities = sorted(
                    "<no national_grid_bm_unit>" if v is None else str(v)
                    for v in keyless["national_grid_bm_unit"].to_list()
                )
            else:
                identities = ["<national_grid_bm_unit column absent from this payload>"]
            df = df.filter(~is_keyless)
            # D-40: rows DECLARED INVALID AND REMOVED must reach the run status
            # through rows_invalid (runner.py:271-275, :1232-1234), so the drop
            # lands as completed_with_warnings rather than a silent success. The
            # ERROR log below carries the identities; this carries the count to
            # every consumer that reads TransformResult instead of stderr.
            # `+=`, never `=`, matching the repo-wide idiom -- run() resets it
            # (base.py:959). Reference: neso_data_portal/
            # embedded_wind_solar_forecast.py:318.
            self.last_excluded_row_count += keyless.height
            # ERROR, not WARNING: this is vendor data loss. Full identities, not
            # a sample -- a fill-forward will need to know exactly which units
            # went missing, and the count alone cannot say which.
            logger.error(
                "%s/%s: dropped %d of %d row(s) with a null or empty-string "
                "bm_unit_id (ENTITY_KEY_COLUMNS). A null-key row cannot be joined "
                "by any downstream consumer and would collapse under the keep='last' "
                "dedup, so it is excluded from silver rather than aborting the "
                "transform (C-7 as narrowed by ADR-032). Bronze retains every "
                "dropped row. Affected national_grid_bm_unit: %s",
                self.source,
                self.dataset,
                keyless.height,
                keyless.height + df.height,
                identities,
            )

        # A payload in which NOTHING is keyed is a broken feed, not a vendor
        # gap -- C-7's fail-closed still applies there.
        if df.is_empty():
            raise ValueError(
                f"{self.source}/{self.dataset}: every one of the "
                f"{keyless.height} row(s) in this payload has a null or "
                "empty-string bm_unit_id, this transformer's entity key. That is "
                "a broken feed rather than a vendor gap, so the transform fails "
                "closed rather than writing an empty reference dataset over a "
                "good one."
            )

        if "registered_capacity_mw" in df.columns:
            df = df.with_columns(pl.col("registered_capacity_mw").cast(pl.Float64))

        df = df.unique(subset=["bm_unit_id"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "bm_unit_id",
            "bm_unit_name",
            "fuel_type",
            "registered_capacity_mw",
            "company_name",
            "gsp_group_id",
            "national_grid_bm_unit",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("bm_unit_id")

    def _write_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        available_at: datetime,
    ) -> None:
        """Override: write a single reference file (not date-partitioned)."""
        final_path = self.silver_dir / "bmunits_reference.parquet"
        write_parquet(df, final_path)


register_transformer("elexon", "bmunits_reference", BMUnitsTransformer)
