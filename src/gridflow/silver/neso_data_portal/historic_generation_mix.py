"""Silver transformer for NESO's Historic GB Generation Mix resource (D-24).

A half-hourly generation-mix and carbon-intensity series running from 2009 to
the present, republished as one whole 62 MB file on every refresh. That shape
forces the ``APPEND_ONLY`` + ``VINTAGE_PER_BRONZE_FILE`` combination (D-21):
the resource's own CKAN ``notes`` field says *"The data is subject to change
due to a data cleansing process"*, which is ADR-025's triggering condition
verbatim, so successive captures are genuinely different truths about the same
instants and must coexist. ``silver_neso_data_portal_historic_generation_mix``
is therefore the vintage archive and
``silver_neso_data_portal_historic_generation_mix_latest`` — one row per
``timestamp_utc`` — is the consumer surface.

**The naive ``DATETIME`` is UTC, and that is DOCUMENTED, not inferred.** The
bodies this transformer reads carry stamps like ``2009-06-01T05:00:00`` with no
offset and no accompanying statement. The UTC claim comes from a different
endpoint entirely: the ``datastore_search`` field metadata captured at
``.planning/phases/neso-data-portal/_probe/datastore_historic-generation-mix.json``,
whose ``DATETIME.info.description`` reads *"Date and time of the historic
generation mix and carbon intensity, given in UTC (Coordinated Universal
Time"*. A plain CSV download never exposes that, so the reading is recorded
here and in ADR-030 rather than left to look self-evident.

The corollary is the guard in :meth:`HistoricGenerationMixTransformer.transform`:
a ``DATETIME`` that *does* carry an offset is a **different** vendor contract,
and applying the documented naive-is-UTC reading to it would shift every row by
that offset with nothing in silver to show for it. Such a body raises.

**The header contract is declared HERE, not imported from the connector**,
matching ``daily_wind_availability``: the connector enforces the same 34
columns at fetch time (D-36's admission rung) and this module enforces them at
transform time. Two independent declarations make a drift between them a real,
findable defect — the full-path E2E is what compares them.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.neso_data_portal import NesoHistoricGenerationMix
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.csv_bronze import read_csv_bronze_body
from gridflow.silver.neso_data_portal._bronze import provenance_for
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS: tuple[str, ...] = (
    "DATETIME",
    "GAS",
    "COAL",
    "NUCLEAR",
    "WIND",
    "WIND_EMB",
    "HYDRO",
    "IMPORTS",
    "BIOMASS",
    "OTHER",
    "SOLAR",
    "STORAGE",
    "GENERATION",
    "CARBON_INTENSITY",
    "LOW_CARBON",
    "ZERO_CARBON",
    "RENEWABLE",
    "FOSSIL",
    "GAS_perc",
    "COAL_perc",
    "NUCLEAR_perc",
    "WIND_perc",
    "WIND_EMB_perc",
    "HYDRO_perc",
    "IMPORTS_perc",
    "BIOMASS_perc",
    "OTHER_perc",
    "SOLAR_perc",
    "STORAGE_perc",
    "GENERATION_perc",
    "LOW_CARBON_perc",
    "ZERO_CARBON_perc",
    "RENEWABLE_perc",
    "FOSSIL_perc",
)
"""The vendor header this dataset contracts for (D-24), exact and ordered.

**34 columns**, counted from the Stage-A capture
``_probe/sample_historic-generation-mix.csv``, which D-24 names as the
authority. The plan's prose said 37 through revision 13; revision 14's ERRATUM
corrected the count against the file.
"""

COLUMN_MAPPING: dict[str, str] = {
    "DATETIME": "timestamp_utc",
    "GAS": "gas",
    "COAL": "coal",
    "NUCLEAR": "nuclear",
    "WIND": "wind",
    "WIND_EMB": "wind_emb",
    "HYDRO": "hydro",
    "IMPORTS": "imports",
    "BIOMASS": "biomass",
    "OTHER": "other",
    "SOLAR": "solar",
    "STORAGE": "storage",
    "GENERATION": "generation",
    "CARBON_INTENSITY": "carbon_intensity",
    "LOW_CARBON": "low_carbon",
    "ZERO_CARBON": "zero_carbon",
    "RENEWABLE": "renewable",
    "FOSSIL": "fossil",
    "GAS_perc": "gas_pct",
    "COAL_perc": "coal_pct",
    "NUCLEAR_perc": "nuclear_pct",
    "WIND_perc": "wind_pct",
    "WIND_EMB_perc": "wind_emb_pct",
    "HYDRO_perc": "hydro_pct",
    "IMPORTS_perc": "imports_pct",
    "BIOMASS_perc": "biomass_pct",
    "OTHER_perc": "other_pct",
    "SOLAR_perc": "solar_pct",
    "STORAGE_perc": "storage_pct",
    "GENERATION_perc": "generation_pct",
    "LOW_CARBON_perc": "low_carbon_pct",
    "ZERO_CARBON_perc": "zero_carbon_pct",
    "RENEWABLE_perc": "renewable_pct",
    "FOSSIL_perc": "fossil_pct",
}
"""Vendor name -> silver name, recorded ONCE so no cast can miss by spelling.

Snake_case of the vendor's own name, with two deliberate departures: the
instant is ``timestamp_utc`` (the repo-wide name ``_event_time_expr`` reads),
and the vendor's ``_perc`` suffix becomes ``_pct``, the spelling used across
the rest of gridflow's silver layer. Every one of the 34 contracted columns has
an entry; nothing is dropped.
"""

_NUMERIC_COLUMNS: tuple[str, ...] = tuple(
    silver_name for vendor_name, silver_name in COLUMN_MAPPING.items() if vendor_name != "DATETIME"
)

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "timestamp_utc",
    *_NUMERIC_COLUMNS,
    "published_at",
    "data_provider",
)

_VENDOR_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# A trailing `Z` or `±HH:MM` / `±HHMM`. Anchored at the end: the point is to
# detect an OFFSET, and the `-` separators inside a date (`2009-01-01`) must not
# read as one, which is why the separator is a colon or nothing but never a
# hyphen.
_OFFSET_SUFFIX_PATTERN = r"(?:Z|[+-][0-9]{2}:?[0-9]{2})$"

_OFFSET_SUFFIX_RE = re.compile(_OFFSET_SUFFIX_PATTERN)


class HistoricGenerationMixTransformer(BaseSilverTransformer):
    """Transform NESO ``historic_generation_mix`` bronze CSV into silver."""

    source = "neso_data_portal"
    dataset = "historic_generation_mix"
    schema_cls = NesoHistoricGenerationMix
    APPEND_ONLY: ClassVar[bool] = True
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True
    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.csv"
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ("timestamp_utc", "published_at")
    """D-24's grain. ``published_at`` is in the key UNCONDITIONALLY: this
    dataset is APPEND_ONLY precisely because NESO cleanses and republishes the
    whole history, so two captures legitimately disagree about one instant and a
    key without the publication instant would collapse them into one. The
    coarser business key — one winning vintage per ``timestamp_utc`` — lives in
    ``LATEST_VIEW_SPECS``, which is a read-time concern.
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
            D-23 requires. ``run()`` then skips the file loudly and records it
            as ``UNUSABLE_PROVENANCE`` (D-41), so the date reports
            ``completed_with_warnings`` — or ``failed`` when every body was
            declined — instead of a silent success over a lost vintage.

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
        implemented **honestly** rather than raising.

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
        columns deliberately, so a non-numeric fuel value or a malformed
        ``DATETIME`` raises here instead of arriving in silver as a null that
        reads exactly like a genuine vendor absence.

        Args:
            raw_df: One bronze body's frame from :meth:`read_bronze_file`.

        Returns:
            The silver frame, uniquely grained by
            :attr:`ENTITY_KEY_COLUMNS`, or an empty frame for empty input.

        Raises:
            ValueError: A ``DATETIME`` carries a UTC offset. See the module
                docstring: the naive-is-UTC reading is a documented claim about
                a NAIVE stamp, and re-applying it to an offset-bearing one
                would silently shift every row by that offset.
            polars.exceptions.InvalidOperationError: A value did not cast.
        """
        if raw_df.is_empty():
            return pl.DataFrame()

        self._reject_offset_bearing_datetimes(raw_df)

        df = raw_df.rename({k: v for k, v in COLUMN_MAPPING.items() if k in raw_df.columns})
        df = df.with_columns(
            pl.col("timestamp_utc")
            .str.to_datetime(_VENDOR_DATETIME_FORMAT, time_unit="us", strict=True)
            .dt.replace_time_zone("UTC"),
            *[pl.col(column).cast(pl.Float64, strict=True) for column in _NUMERIC_COLUMNS],
        )
        df = df.with_columns(pl.lit(self.source).alias("data_provider"))

        df = df.unique(subset=list(self.ENTITY_KEY_COLUMNS), keep="last")
        return df.select(_OUTPUT_COLUMNS).sort("timestamp_utc")

    @staticmethod
    def _reject_offset_bearing_datetimes(raw_df: pl.DataFrame) -> None:
        """Raise if any raw ``DATETIME`` carries a UTC offset.

        Checked on the raw ``Utf8`` column, BEFORE any parse: once a string has
        been through a datetime parser the offset is either gone or silently
        applied, and either way the drift is no longer observable. Polars' own
        cast semantics are deliberately not relied on here — the guard makes the
        failure deterministic and lets the message name the offending value.

        Args:
            raw_df: The unparsed bronze frame, all columns ``Utf8``.

        Raises:
            ValueError: At least one ``DATETIME`` value ends in ``Z`` or an
                ``±HH:MM`` offset.
        """
        offending = raw_df.filter(pl.col("DATETIME").str.contains(_OFFSET_SUFFIX_PATTERN))
        if offending.is_empty():
            return

        sample = offending["DATETIME"].to_list()[:3]
        message = (
            f"NESO historic_generation_mix DATETIME carries a UTC offset in "
            f"{offending.height} row(s), e.g. {sample}. gridflow reads this column as "
            "naive-and-UTC on the strength of the datastore_search field metadata "
            "(_probe/datastore_historic-generation-mix.json, DATETIME.info.description); "
            "an offset-bearing stamp is a DIFFERENT vendor contract, and re-reading it "
            "under the same rule would shift every row silently. Refusing to transform."
        )
        logger.error(message)
        raise ValueError(message)


register_transformer(
    "neso_data_portal", "historic_generation_mix", HistoricGenerationMixTransformer
)
