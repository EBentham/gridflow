"""Silver transformer for NESO's Daily Wind Availability resource (D-24).

A 2-14-day-ahead per-BMU wind availability forecast, republished as a whole
file. That shape is structurally ``elexon/fou2t14d`` — which the repo already
rules ``APPEND_ONLY`` — so this transformer takes the same combination
(``APPEND_ONLY`` + ``VINTAGE_PER_BRONZE_FILE``, D-21): every capture keeps its
own vintage, and ``silver_neso_data_portal_daily_wind_availability_latest``
serves the one current row per ``(bmu_id, availability_date)``.

**The header contract is declared HERE, not imported from the connector**, and
that duplication is deliberate. The connector's ``DATASETS`` entry enforces the
same header at fetch time (D-36's admission rung); this module enforces it at
transform time. Two independent declarations mean a drift between them is a
real, findable defect — the full-path E2E is what compares them — rather than a
single point that could be edited once and be wrong in both places at once.

**Fixture provenance, disclosed**: Stage A captured no CSV sample for this
resource, so the unit fixture is hand-authored from the research-asserted
header. D-19's exact-header contract makes a wrong guess loud (a
``CsvHeaderDriftError``, at fetch time as well as here), and a live-marked test
pins the real header against the portal.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.neso_data_portal import NesoDailyWindAvailability
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.csv_bronze import read_csv_bronze_body
from gridflow.silver.neso_data_portal._bronze import provenance_for
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS: tuple[str, ...] = ("BMU_ID", "Date", "MW")
"""The vendor header this dataset contracts for (D-24), exact and ordered."""

_COLUMN_MAPPING: dict[str, str] = {
    "BMU_ID": "bmu_id",
    "Date": "availability_date",
    "MW": "availability_mw",
}
"""Vendor name -> silver name, recorded once so no cast can miss by spelling."""

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "bmu_id",
    "availability_date",
    "availability_mw",
    "timestamp_utc",
    "published_at",
    "data_provider",
)


class DailyWindAvailabilityTransformer(BaseSilverTransformer):
    """Transform NESO ``daily_wind_availability`` bronze CSV into silver."""

    source = "neso_data_portal"
    dataset = "daily_wind_availability"
    schema_cls = NesoDailyWindAvailability
    APPEND_ONLY: ClassVar[bool] = True
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True
    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.csv"
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = (
        "bmu_id",
        "availability_date",
        "published_at",
    )
    """D-24's grain. ``published_at`` is in the key UNCONDITIONALLY: this
    dataset is APPEND_ONLY precisely so successive vendor publications coexist,
    and a key without the publication instant would collapse them. The coarser
    business key — one winning vintage per ``(bmu_id, availability_date)`` —
    lives in ``LATEST_VIEW_SPECS``, which is a read-time concern.
    """

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Read one bronze body and attach its vendor publication instant.

        Args:
            raw_path: A ``raw_*.csv`` body in the exact date partition.

        Returns:
            The body's rows as all-``Utf8`` columns plus a ``published_at``
            column typed ``pl.Datetime("us", "UTC")`` — the dtype
            ``_add_bitemporal_columns`` requires, so a mistyped column raises
            there rather than silently mistyping ``available_at``.

            An **empty** frame when the sidecar cannot supply the provenance
            D-23 requires. That is not a soft failure: ``run()`` skips the file
            loudly and records it as ``UNUSABLE_PROVENANCE`` (D-41), so the
            date reports ``completed_with_warnings`` — or ``failed`` when every
            body was declined — instead of a silent success over a lost
            vintage.

        Raises:
            NotCsvBodyError: The stored bytes are not CSV at all.
            CsvHeaderDriftError: The header is not :data:`EXPECTED_COLUMNS`.
        """
        provenance = provenance_for(raw_path)
        if provenance is None:
            return pl.DataFrame()

        frame = read_csv_bronze_body(
            raw_path.read_bytes(),
            expected_columns=EXPECTED_COLUMNS,
            source_label=str(raw_path),
        )
        return frame.with_columns(
            pl.lit(provenance.published_at).cast(pl.Datetime("us", "UTC")).alias("published_at")
        )

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read every bronze body for ``target_date`` as one frame.

        Unused on this transformer's own branch — ``VINTAGE_PER_BRONZE_FILE``
        drives :meth:`read_bronze_file` once per body so each capture keeps its
        own vintage — but the base class declares it abstract, so it is
        implemented **honestly** rather than raising: the same glob, in the
        same exact date partition, delegating to the same reader. A
        ``NotImplementedError`` here would be a false statement about a
        supported method.

        Args:
            target_date: The date partition to read.

        Returns:
            The concatenated rows, or an empty frame when the partition does
            not exist or every body was declined.
        """
        partition = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not partition.exists():
            return pl.DataFrame()

        frames = [
            frame
            for raw_path in sorted(partition.glob(self.BRONZE_BODY_GLOB))
            if not raw_path.name.endswith(".meta.json")
            and not (frame := self.read_bronze_file(raw_path)).is_empty()
        ]
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="vertical")

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Normalise one bronze vintage into the D-24 silver contract.

        Every cast is ``strict=True`` (D-19): the reader hands over all-``Utf8``
        columns deliberately, so a non-numeric ``MW`` or a non-ISO ``Date``
        raises here instead of arriving in silver as a null that reads exactly
        like a genuine vendor absence.

        ``bmu_id`` is carried **verbatim** — no case folding, no stripping, no
        prefix normalisation. BM unit ids are stored as-is repo-wide.

        Args:
            raw_df: One bronze body's frame from :meth:`read_bronze_file`.

        Returns:
            The silver frame, uniquely grained by
            :attr:`ENTITY_KEY_COLUMNS`, or an empty frame for empty input.

        Raises:
            polars.exceptions.InvalidOperationError: A value did not cast.
        """
        if raw_df.is_empty():
            return pl.DataFrame()

        df = raw_df.rename({k: v for k, v in _COLUMN_MAPPING.items() if k in raw_df.columns})
        df = df.with_columns(
            pl.col("bmu_id").cast(pl.Utf8, strict=True),
            pl.col("availability_date").cast(pl.Date, strict=True),
            pl.col("availability_mw").cast(pl.Float64, strict=True),
        )

        # D-25: this dataset has no natural instant of its own. Emitting none
        # would drop it into `_event_time_expr`'s midnight-of-target-date
        # fallback -- and since D-13 partitions bronze on the INGEST window's
        # end, that would stamp every row with the fetch window rather than
        # with its own availability day. SP1 of the GB availability day is the
        # honest derivation, and `settlement_period_to_utc` is DST-fold-safe,
        # so a BST day starts at 23:00Z the day before and a GMT day at 00:00Z.
        df = df.with_columns(
            pl.col("availability_date")
            .map_elements(
                lambda value: settlement_period_to_utc(value, 1),
                return_dtype=pl.Datetime("us", "UTC"),
            )
            .alias("timestamp_utc"),
            pl.lit(self.source).alias("data_provider"),
        )

        df = df.unique(subset=list(self.ENTITY_KEY_COLUMNS), keep="last")
        return df.select(_OUTPUT_COLUMNS).sort("availability_date", "bmu_id")


register_transformer(
    "neso_data_portal", "daily_wind_availability", DailyWindAvailabilityTransformer
)
