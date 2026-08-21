"""Silver transformer for NESO's Embedded Solar and Wind Forecast (D-24).

A rolling day-ahead forecast of GB *embedded* (distribution-connected, and so
invisible to transmission metering) wind and solar output, republished as a
whole file several times a day under a **date-stamped filename**. Like its two
siblings it is ``APPEND_ONLY`` + ``VINTAGE_PER_BRONZE_FILE`` (D-21): each
capture is a distinct forecast vintage for the same settlement periods, and
``silver_neso_data_portal_embedded_wind_solar_forecast_latest`` — one row per
``(settlement_date, settlement_period)`` — is the consumer surface.

**The issue instant comes from the vendor's filename, never from our clock.**
``202608161825_embedded_forecast.csv`` carries its own 12-digit
``YYYYMMDDHHMM`` token, read as UTC per D-15 and parsed at the single
provenance site (``_bronze.provenance_for``). That module parses the token
wherever one exists and takes no view on which datasets must have one —
**requiredness is the caller's**, and this is the caller that requires it. A
body whose filename carries no token yields an EMPTY frame and a WARNING
(FM-05), because ``issue_time`` is part of this dataset's entity key: a
fabricated one would mint a vintage that never existed and outrank the real
forecasts in the ``_latest`` projection.

**This module emits no ``timestamp_utc``, deliberately** (D-26).
``_event_time_expr`` prefers a ``timestamp_utc`` column over the settlement
pair, and only the pair branch calls the DST-fold-safe
``settlement_period_to_utc``. Emitting one here would silently take
``event_time`` off the safe path on the 46- and 50-period days — the only days
it matters on. ``TIME_GMT`` is therefore carried unparsed as ``time_gmt_raw``
and read by nothing; ``DATE_GMT``, its calendar half, is not emitted at all
(D-24's column contract), and bronze retains both permanently.

**Out-of-calendar settlement periods are excluded, counted and named** (D-27,
FM-16). A period that does not exist on its settlement date cannot be assigned
an honest ``event_time`` — SP49 on a 48-period day lands in the *next*
settlement day, SP0 in the *previous* one — so the row is removed rather than
written with a wrong-day instant. The exclusion is not merely logged: it
accumulates into ``last_excluded_row_count`` (D-40), which ``run_transform``
folds into the total it already reports, so the run lands as
``completed_with_warnings`` instead of a silent ``success``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.neso_data_portal import (
    NesoEmbeddedWindSolarForecast,
    is_valid_settlement_period,
)
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.csv_bronze import read_csv_bronze_body
from gridflow.silver.neso_data_portal._bronze import provenance_for
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_periods_in_day

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS: tuple[str, ...] = (
    "DATE_GMT",
    "TIME_GMT",
    "SETTLEMENT_DATE",
    "SETTLEMENT_PERIOD",
    "EMBEDDED_WIND_FORECAST",
    "EMBEDDED_WIND_CAPACITY",
    "EMBEDDED_SOLAR_FORECAST",
    "EMBEDDED_SOLAR_CAPACITY",
)
"""The vendor header this dataset contracts for (D-24), exact and ordered.

Declared HERE rather than imported from the connector's ``DATASETS`` entry,
which enforces the same header at fetch time (D-36's admission rung). Two
independent declarations make a drift between them a real, findable defect —
the full-path E2E is what compares them — rather than a single point that could
be edited once and be wrong in both places at once.
"""

_COLUMN_MAPPING: dict[str, str] = {
    "TIME_GMT": "time_gmt_raw",
    "SETTLEMENT_DATE": "settlement_date",
    "SETTLEMENT_PERIOD": "settlement_period",
    "EMBEDDED_WIND_FORECAST": "embedded_wind_forecast",
    "EMBEDDED_WIND_CAPACITY": "embedded_wind_capacity",
    "EMBEDDED_SOLAR_FORECAST": "embedded_solar_forecast",
    "EMBEDDED_SOLAR_CAPACITY": "embedded_solar_capacity",
}
"""Vendor name -> silver name, recorded once so no cast can miss by spelling.

``DATE_GMT`` is absent on purpose and not by oversight: D-24's column contract
omits it. It is the calendar half of the same undocumented GMT stamp as
``TIME_GMT`` (whose start-vs-end convention NESO does not state), while the
authoritative time reference is the settlement pair. Bronze keeps the bytes, so
adding it later is a re-transform, not a re-ingest.
"""

_MW_COLUMNS: tuple[str, ...] = (
    "embedded_wind_forecast",
    "embedded_wind_capacity",
    "embedded_solar_forecast",
    "embedded_solar_capacity",
)

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "settlement_date",
    "settlement_period",
    "issue_time",
    "time_gmt_raw",
    *_MW_COLUMNS,
    "published_at",
    "data_provider",
)

# The vendor writes SETTLEMENT_DATE as a midnight-stamped datetime
# (`2026-08-16T00:00:00`), not a bare date. Parsed with an explicit format and
# `strict=True` so a change of shape raises instead of nulling the column that
# every downstream key is built from.
_VENDOR_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class EmbeddedWindSolarForecastTransformer(BaseSilverTransformer):
    """Transform NESO ``embedded_wind_solar_forecast`` bronze CSV into silver."""

    source = "neso_data_portal"
    dataset = "embedded_wind_solar_forecast"
    schema_cls = NesoEmbeddedWindSolarForecast
    APPEND_ONLY: ClassVar[bool] = True
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True
    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.csv"
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = (
        "settlement_date",
        "settlement_period",
        "issue_time",
    )
    """D-24's grain. ``issue_time`` — not ``published_at`` — is the vintage axis
    in the key: this vendor stamps the forecast's own issue instant into the
    filename, and it is the finer, more meaningful of the two clocks. The
    coarser business key, one winning vintage per settlement period, lives in
    ``LATEST_VIEW_SPECS``.
    """

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Read one bronze body and attach its vintage columns.

        Args:
            raw_path: A ``raw_*.csv`` body in the exact date partition.

        Returns:
            The body's rows as all-``Utf8`` columns plus ``published_at`` and
            ``issue_time``, both typed ``pl.Datetime("us", "UTC")`` —
            ``published_at``'s dtype is the one ``_add_bitemporal_columns``
            requires, so a mistyped column raises there rather than silently
            mistyping ``available_at``.

            An **empty** frame when the sidecar cannot supply the provenance
            D-23 requires, **or** when its ``resource_filename`` carries no
            12-digit issue token. Both are loud: ``run()`` skips the file and
            records it as ``UNUSABLE_PROVENANCE`` (D-41), so the date reports
            ``completed_with_warnings`` — or ``failed`` when every body was
            declined — instead of a silent success over a lost vintage.

        Raises:
            NotCsvBodyError: The stored bytes are not CSV at all.
            CsvHeaderDriftError: The header is not :data:`EXPECTED_COLUMNS`.
        """
        provenance = provenance_for(raw_path)
        if provenance is None:
            return pl.DataFrame()

        if provenance.issue_time is None:
            logger.warning(
                "NESO embedded_wind_solar_forecast provenance unusable for %s: "
                "resource_filename %r carries no 12-digit issue token, so issue_time "
                "cannot be established. It is part of this dataset's entity key and is "
                "NEVER substituted from the fetch clock (D-23/FM-05); declining the body",
                raw_path,
                provenance.resource_filename,
            )
            return pl.DataFrame()

        frame = read_csv_bronze_body(
            raw_path.read_bytes(),
            expected_columns=EXPECTED_COLUMNS,
            source_label=str(raw_path),
        )
        return frame.with_columns(
            pl.lit(provenance.published_at).cast(pl.Datetime("us", "UTC")).alias("published_at"),
            pl.lit(provenance.issue_time).cast(pl.Datetime("us", "UTC")).alias("issue_time"),
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

        Every cast is ``strict=True`` (D-19), and the D-27 filter runs after the
        casts so the predicate sees a real ``date`` and a real ``int`` rather
        than the vendor's strings.

        Args:
            raw_df: One bronze body's frame from :meth:`read_bronze_file`.

        Returns:
            The silver frame, uniquely grained by :attr:`ENTITY_KEY_COLUMNS`
            and carrying **no** ``timestamp_utc`` (D-26), or an empty frame for
            empty input.

        Raises:
            polars.exceptions.InvalidOperationError: A value did not cast.
        """
        if raw_df.is_empty():
            return pl.DataFrame()

        df = raw_df.rename({k: v for k, v in _COLUMN_MAPPING.items() if k in raw_df.columns})
        df = df.with_columns(
            pl.col("settlement_date")
            .str.to_datetime(_VENDOR_DATE_FORMAT, time_unit="us", strict=True)
            .dt.date(),
            pl.col("settlement_period").cast(pl.Int64, strict=True),
            pl.col("time_gmt_raw").cast(pl.Utf8, strict=True),
            *[pl.col(column).cast(pl.Float64, strict=True) for column in _MW_COLUMNS],
        )

        df = self._exclude_out_of_calendar_periods(df)
        if df.is_empty():
            return pl.DataFrame()

        df = df.with_columns(pl.lit(self.source).alias("data_provider"))
        df = df.unique(subset=list(self.ENTITY_KEY_COLUMNS), keep="last")
        return df.select(_OUTPUT_COLUMNS).sort("settlement_date", "settlement_period")

    def _exclude_out_of_calendar_periods(self, df: pl.DataFrame) -> pl.DataFrame:
        """Drop rows whose settlement period does not exist on their date (D-27).

        The exclusion is **accumulated** into ``last_excluded_row_count`` with
        ``+=`` and never assigned: this transformer is
        ``VINTAGE_PER_BRONZE_FILE``, so ``transform()`` runs once per bronze
        body against a single reset in ``run()``, and an assignment would report
        only the last body's exclusions (D-40). The counter is deliberately NOT
        ``last_validation_failure_count``, which documents its rows as still
        written.

        Args:
            df: The cast frame, with a real ``date`` and ``int`` settlement pair.

        Returns:
            The frame with out-of-calendar rows removed.
        """
        valid = (
            df.select(
                pl.struct(["settlement_date", "settlement_period"])
                .map_elements(
                    lambda row: is_valid_settlement_period(
                        row["settlement_date"], row["settlement_period"]
                    ),
                    return_dtype=pl.Boolean,
                )
                .alias("valid")
            )
            .to_series()
            .fill_null(value=False)
        )

        declined = df.filter(~valid)
        if declined.is_empty():
            return df

        for row in declined.iter_rows(named=True):
            logger.warning(
                "NESO embedded_wind_solar_forecast: excluding settlement date %s period %s — "
                "that day has %d settlement periods, so this period does not exist. Keeping it "
                "would place the row in a neighbouring settlement day with a fabricated "
                "event_time (D-27/FM-16); the bronze bytes retain it permanently",
                row["settlement_date"].isoformat(),
                row["settlement_period"],
                settlement_periods_in_day(row["settlement_date"]),
            )

        # `+=`, never `=`. See the docstring: one reset per run(), one
        # transform() per bronze body.
        self.last_excluded_row_count += declined.height
        return df.filter(valid)


register_transformer(
    "neso_data_portal", "embedded_wind_solar_forecast", EmbeddedWindSolarForecastTransformer
)
