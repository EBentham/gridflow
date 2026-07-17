"""Abstract base class for silver-layer transformers."""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import polars as pl
from pydantic import BaseModel, ValidationError

from gridflow.storage.parquet import write_parquet
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)

_VALIDATION_SAMPLE_LIMIT = 5
"""Max distinct validation-error strings logged per ``run()`` (fail-soft; bounded)."""

_EXACT_PARTITION_ONLY_SOURCES: frozenset[str] = frozenset({"entsoe"})
"""Sources whose connectors write day-exact bronze partitions (P0.8 / R2-F08).

As of P0.8, ``EntsoeConnector.fetch`` chunks every multi-day window into one
request per covered UTC calendar day, so a correctly-fetched ENTSO-E date
either has its own exact bronze partition or has no bronze at all. For these
sources, any covering-fallback hit (``_find_covering_bronze_partition``) would
fabricate wrong-day rows: it would silently relabel a neighbouring day's rows
under the requested date's silver file, reproducing the R2-F08 duplication bug
chunking exists to fix. This mirrors the project's own precedent for exactly
this failure class — ``VINTAGE_PER_BRONZE_FILE`` / ADR-025 (class docstring
above, "Only the EXACT date partition is read — never the multi-day
covering-partition fallback") and the ENTSO-G generic family's exact-only
``_bronze_files`` (``silver/entsog/generic.py``).

Source-scoped (not a per-transformer ``ClassVar`` flag) because the exact-only
guarantee is a property of the *connector's* write layout established by this
unit: every current and future ENTSO-E transformer is covered automatically,
and a new ENTSO-E dataset cannot silently reintroduce the fallback by
forgetting to set a flag. ``_find_covering_bronze_partition`` itself is not
modified — NESO and Open-Meteo/ALSI resolution (the other callers) are
unaffected; ``tests/silver/test_partition_fallback.py``'s ``test_source``
stub pins that the fallback stays intact for non-ENTSO-E sources.
"""


def gas_day_event_time_expr(column: str = "gas_day") -> pl.Expr:
    """Build the fixed-06:00 UTC event-time expression for a gas day.

    Fixed 06:00 UTC is Gridflow's project labelling convention required by the
    project ``CLAUDE.md``, the GIE vendor README, P0.6, and R1-F06. It
    deliberately differs from the broader DST-aware vault page while tracked
    follow-up P0.6-DOC-1 is unresolved. Any future convention change requires
    another major dataset-version bump.

    Args:
        column: Name of the ``pl.Date`` gas-day column.

    Returns:
        A UTC-aware Polars expression aliased to ``event_time``.
    """
    return (pl.col(column).cast(pl.Datetime("us", "UTC")) + pl.duration(hours=6)).alias(
        "event_time"
    )


class BaseSilverTransformer(ABC):
    """Base class for bronze -> silver transformations.

    Subclasses implement:
    - source: the data source name
    - dataset: the dataset name
    - read_bronze(): read and parse raw bronze files
    - transform(): apply normalisation, validation, deduplication
    """

    source: str
    dataset: str
    schema_cls: ClassVar[type[BaseModel] | None] = None
    """Opt-in Pydantic schema for full-frame silver validation (VTA-SCHEMA-01).

    When set, ``run()`` validates every row of the ``transform()`` output against
    this model, fail-soft: failures are counted into
    ``last_validation_failure_count`` and surfaced by the CLI as
    ``completed_with_warnings`` — never raised, never dropped. ``None`` (the
    default) skips validation, which cleanly excludes generic/dynamic transformers
    that have no fixed Pydantic contract (the ENTSO-G generic family incl. CMP, the
    GIE generic JSON family). Subclasses that serve one dataset set this as a class
    attribute; one-class-many-schemas transformers (NESO) set it per instance.
    """
    write_silver_csv: bool = False
    """Opt-in for the per-date silver CSV sidecar (CH3-02 / CH-PERF-02 / C4-1).

    Default ``False``: ``run()`` writes only the canonical Parquet partition. The
    legacy always-on ``_write_csv`` emitted an unpartitioned ``.csv`` alongside
    every Parquet write on every run, doubling the silver write surface for a
    sidecar no read/gold/quality consumer reads (on-demand CSV is served by the
    ``export_csv`` CLI command). Set per-instance at the call boundary from
    ``PipelineSettings.write_silver_csv`` — a plain instance attribute (not
    ``ClassVar``), mirroring ``last_unmapped_count``, so the boundary assignment
    is clean under ``mypy --strict`` and never leaks class state across runs.
    """
    last_unmapped_count: int = 0
    """Count of rows whose enum code was unmapped in the most recent ``run()``.

    Reset to 0 at the start of every ``run()`` and set by ``transform()`` when a
    transformer maps an enum with an unmapped-code sentinel (ADR-022). The CLI
    reads it after each per-date ``run()`` to thread the unmapped total into
    ``PipelineRunTracker.complete_with_warnings``. Resetting in ``run()`` (not
    only on ``transform()``'s happy path) keeps a date with no bronze or missing
    columns from being charged the previous date's count.
    """
    last_validation_failure_count: int = 0
    """Count of rows that failed ``schema_cls`` validation in the most recent ``run()``.

    Reset to 0 at the start of every ``run()`` (before any early return) and set by
    the central ``_validate_against_schema`` step on the ``transform()`` output. The
    CLI accumulates it across dates and threads the total into
    ``PipelineRunTracker.complete_with_warnings`` (parallel to
    ``last_unmapped_count``; VTA-SCHEMA-01). Rows that fail validation are still
    written — the count is the only signal (fail-soft).
    """
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = ()
    APPEND_ONLY: ClassVar[bool] = False
    """Per-dataset opt-in for revision-preserving silver writes.

    Default ``False`` keeps the F0 atomic-replace behaviour: each
    ``(dataset, target_date)`` pair maps to a single Parquet file that is
    overwritten on each run. When ``True`` the writer emits a run-suffixed
    filename derived from ``available_at`` so successive runs coexist in the
    partition directory and downstream readers apply ``QUALIFY``-style
    selection at read time. See ``docs/DECISION_LOG/ADR-018-append-only-
    run-suffixed-files.md`` for the trade-off discussion. Only datasets that
    publish meaningful revisions (REMIT, FOU2T14D) should opt in.
    """
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = False
    """Opt in to assigning one availability timestamp per bronze raw file.

    The default keeps the established whole-date read and single availability
    timestamp. Revision feeds whose bronze fetch time is their only vintage
    marker set this to ``True`` and implement ``read_bronze_file``.

    Contract notes for opt-ins (ADR-025):
    - Only the EXACT date partition is read — never the multi-day
      covering-partition fallback, which would stamp a prior day's rows into a
      wrong-dated vintage file (review finding, v0.17 PR-A).
    - A raw file without a parseable sidecar timestamp is SKIPPED loudly: a
      ``now()`` fallback would mint a new non-idempotent vintage filename on
      every re-transform and poison point-in-time reads.
    - Transformers that ASSIGN ``last_unmapped_count`` inside ``transform()``
      (the ADR-022 enum-mapping pattern) must convert to accumulation before
      opting in, or per-frame counts overwrite each other.
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.bronze_dir = data_dir / "bronze" / self.source / self.dataset
        self.silver_dir = data_dir / "silver" / self.source / self.dataset

    @abstractmethod
    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read and parse all bronze files for a given date.
        Returns a raw DataFrame before validation."""
        ...

    @abstractmethod
    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Apply source-specific normalisation, validation, and deduplication.
        Returns a clean DataFrame matching the silver schema."""
        ...

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Read one raw bronze file for per-file vintage capture.

        Only transformers opting into ``VINTAGE_PER_BRONZE_FILE`` need to
        implement this method; the ordinary ``read_bronze`` path is unchanged.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement read_bronze_file for per-file vintages"
        )

    def run(
        self,
        target_date: date,
        run_id: str | None = None,
        reingest: bool = False,
    ) -> int:
        """Execute the full bronze -> silver pipeline for one date.

        Returns the number of rows written.
        """
        # Reset the per-run warning counters before either early-return path so a
        # date with no bronze / missing columns is never charged a prior date's
        # count (ADR-022 unmapped + VTA-SCHEMA-01 validation; the CLI accumulates
        # both after each per-date run).
        self.last_unmapped_count = 0
        self.last_validation_failure_count = 0

        resolved_run_id = run_id or f"adhoc-{datetime.now(UTC).isoformat()}"
        frames: list[pl.DataFrame] = []
        saw_bronze = False

        if self.VINTAGE_PER_BRONZE_FILE:
            # EXACT date partition only — the covering-partition fallback in
            # _bronze_date_dirs would stamp a prior day's rows into a file named
            # for target_date (wrong-dated vintage; see class docstring).
            exact_dir = (
                self.bronze_dir
                / str(target_date.year)
                / f"{target_date.month:02d}"
                / f"{target_date.day:02d}"
            )
            date_dirs = [exact_dir] if exact_dir.exists() else []
            for date_dir in date_dirs:
                for raw_path in sorted(date_dir.glob("raw_*.json")):
                    # The data glob also matches sidecars (raw_*.meta.json) — skip them.
                    if raw_path.name.endswith(".meta.json"):
                        continue
                    available_at = self._timestamp_from_sidecar(raw_path.with_suffix(".meta.json"))
                    if available_at is None:
                        # Skip loudly: a now() fallback would mint a fresh
                        # non-idempotent vintage file on every re-transform.
                        logger.warning(
                            "Skipping bronze file with no usable sidecar timestamp "
                            "(cannot assign an honest vintage): %s",
                            raw_path,
                        )
                        continue
                    raw_df = self.read_bronze_file(raw_path)
                    if raw_df.is_empty():
                        continue
                    saw_bronze = True
                    clean_df = self._process_frame(
                        raw_df, target_date, resolved_run_id, available_at
                    )
                    if clean_df is not None:
                        frames.append(clean_df)
        else:
            available_at = (
                self._available_at_from_bronze(target_date) if reingest else datetime.now(UTC)
            )
            raw_df = self.read_bronze(target_date)
            if not raw_df.is_empty():
                saw_bronze = True
                clean_df = self._process_frame(raw_df, target_date, resolved_run_id, available_at)
                if clean_df is not None:
                    frames.append(clean_df)

        if not frames:
            # Distinguish "nothing to read" from "read but transformed to zero
            # rows" (the latter already logged per frame by _process_frame).
            if not saw_bronze:
                logger.warning(f"No bronze data for {self.source}/{self.dataset} on {target_date}")
            return 0

        if self.write_silver_csv:
            # Frames can differ in optional columns (e.g. run_type present in one
            # vintage only) — diagonal concat null-fills instead of raising (CL-1).
            self._write_csv(pl.concat(frames, how="diagonal"), target_date)

        total_rows = sum(len(frame) for frame in frames)
        logger.info(
            f"Silver write: {self.source}/{self.dataset} {target_date} -> {total_rows} rows"
        )
        return total_rows

    def _process_frame(
        self,
        raw_df: pl.DataFrame,
        target_date: date,
        run_id: str,
        available_at: datetime,
    ) -> pl.DataFrame | None:
        """Transform, validate, stamp, and write one bronze-vintage frame."""
        if raw_df.is_empty():
            return None

        clean_df = self.transform(raw_df)
        if clean_df.is_empty():
            logger.warning(f"Transform produced 0 rows for {target_date}")
            return None

        # Enforce the declared Pydantic schema on the FULL frame, fail-soft
        # (VTA-SCHEMA-01): failures are counted + logged here and surfaced by the
        # CLI as completed_with_warnings — never raised, never dropped (CLAUDE.md
        # hard rule). Validated on the transform() output, before bitemporal
        # columns are stamped (schemas do not declare those). No-op when
        # schema_cls is None (generic/dynamic transformers, incl. ENTSO-G CMP).
        # Accumulates across vintage frames (reset once at run() start).
        self.last_validation_failure_count += self._validate_against_schema(clean_df)
        clean_df = self._add_bitemporal_columns(
            clean_df,
            target_date=target_date,
            run_id=run_id,
            available_at=available_at,
        )
        self._write_silver(clean_df, target_date, available_at=available_at)
        return clean_df

    def _validate_against_schema(self, df: pl.DataFrame) -> int:
        """Validate every row of the transform output against ``schema_cls``, fail-soft.

        Returns the number of rows that failed Pydantic validation. **Never raises
        and never drops a row**: invalid rows are still written to silver; the
        returned count is the only signal, threaded by the CLI into
        ``PipelineRunTracker.complete_with_warnings`` (the CLAUDE.md hard rule —
        validation failures are logged, counted, and surfaced, never silently
        dropped). A no-op returning ``0`` when ``schema_cls`` is ``None`` (generic/
        dynamic transformers with no fixed contract) or the frame is empty.

        Validates the ``transform()`` output (pre-bitemporal): the schema describes
        exactly those columns, and ``BaseSchema``'s ``extra="ignore"`` means any
        additional columns are tolerated. ``strict=True`` schemas will surface real
        ``Field(ge/le)`` / tz breaches on later (non-first) rows as warnings — that
        is the intended fail-soft behaviour, not an error.
        """
        schema = self.schema_cls
        if schema is None or df.is_empty():
            return 0

        failures = 0
        sample: list[str] = []
        for row in df.iter_rows(named=True):
            try:
                schema.model_validate(row)
            except ValidationError as exc:
                failures += 1
                if len(sample) < _VALIDATION_SAMPLE_LIMIT:
                    sample.append(str(exc).replace("\n", " ")[:300])
            except Exception as exc:  # noqa: BLE001
                # Fail-soft is an ABSOLUTE guarantee — one row must never crash the
                # whole date's transform. Pydantic v2 only wraps ValueError/
                # AssertionError into ValidationError, so a custom field_validator
                # raising e.g. TypeError/KeyError would otherwise escape this method
                # and propagate out of run(). Count it like any other invalid row,
                # but log it LOUDLY with its type so a genuine code bug stays
                # visible (surfaced, never silently swallowed).
                failures += 1
                if len(sample) < _VALIDATION_SAMPLE_LIMIT:
                    sample.append(f"{type(exc).__name__}: {str(exc)[:280]}")
                logger.warning(
                    "Unexpected %s validating %s/%s row against %s: %s",
                    type(exc).__name__,
                    self.source,
                    self.dataset,
                    schema.__name__,
                    exc,
                )

        if failures:
            logger.warning(
                "Schema validation: %d/%d row(s) failed %s for %s/%s "
                "(completed_with_warnings; rows still written). Sample: %s",
                failures,
                len(df),
                schema.__name__,
                self.source,
                self.dataset,
                sample,
            )
        return failures

    def _add_bitemporal_columns(
        self,
        df: pl.DataFrame,
        target_date: date,
        run_id: str,
        available_at: datetime,
    ) -> pl.DataFrame:
        """Add modelling lineage columns before silver output is persisted.

        ``available_at = coalesce(published_at, ingest_time)`` (ADR-025 §3): when
        the transformer emitted a ``published_at`` column (the vendor publication
        vintage), it becomes ``available_at`` per row; rows with a null
        ``published_at`` fall back to the ingest/reingest scalar. Datasets that
        emit no ``published_at`` column keep byte-identical ``available_at``.
        """
        if available_at.tzinfo is None:
            available_at = available_at.replace(tzinfo=UTC)
        else:
            available_at = available_at.astimezone(UTC)

        ingest_stamp = pl.lit(available_at).cast(pl.Datetime("us", "UTC"))
        # ADR-025 §3: available_at = coalesce(published_at, ingest_time), ROW-WISE.
        # pl.coalesce is per-row, so a mixed-null frame (Elexon publishTime is
        # per-record; a date's ENTSO-E bronze can mix files with and without
        # createdDateTime) falls back to the ingest scalar on null rows rather
        # than writing a null available_at — which gridflow_models' fail-closed
        # availability barrier rejects wholesale. A frame-level column swap would not.
        if "published_at" in df.columns:
            available_at_expr = pl.coalesce(pl.col("published_at"), ingest_stamp).alias(
                "available_at"
            )
        else:
            available_at_expr = ingest_stamp.alias("available_at")

        return df.with_columns(
            [
                self._event_time_expr(df, target_date),
                available_at_expr,
                pl.lit(run_id).alias("source_run_id"),
                pl.lit(self.DATASET_VERSION).alias("dataset_version"),
            ]
        )

    def _event_time_expr(self, df: pl.DataFrame, target_date: date) -> pl.Expr:
        """Return the expression used for the row's semantic event time."""
        column = self._event_time_column()
        if column in df.columns:
            return pl.col(column).cast(pl.Datetime("us", "UTC")).alias("event_time")

        if {"settlement_date", "settlement_period"}.issubset(df.columns):
            return (
                pl.struct(["settlement_date", "settlement_period"])
                .map_elements(
                    lambda row: settlement_period_to_utc(
                        row["settlement_date"],
                        row["settlement_period"],
                    ),
                    return_dtype=pl.Datetime("us", "UTC"),
                )
                .alias("event_time")
            )

        logger.debug(
            "Falling back to target-date event_time for %s/%s on %s",
            self.source,
            self.dataset,
            target_date,
        )
        return (
            pl.lit(datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC))
            .cast(pl.Datetime("us", "UTC"))
            .alias("event_time")
        )

    def _event_time_column(self) -> str:
        """Name of the transform output column that represents event time."""
        return "timestamp_utc"

    def _available_at_from_bronze(self, target_date: date) -> datetime:
        """Reconstruct historical availability from bronze sidecar metadata."""
        timestamps: list[datetime] = []
        for date_dir in self._bronze_date_dirs(target_date):
            for meta_path in sorted(date_dir.glob("raw_*.meta.json")):
                timestamp = self._timestamp_from_sidecar(meta_path)
                if timestamp is not None:
                    timestamps.append(timestamp)

        if timestamps:
            return max(timestamps)

        fallback = datetime.now(UTC)
        logger.warning(
            "No bronze sidecar timestamp found for %s/%s on %s; using %s",
            self.source,
            self.dataset,
            target_date,
            fallback.isoformat(),
        )
        return fallback

    def _bronze_date_dirs(self, target_date: date) -> list[Path]:
        """Candidate bronze date directories for this dataset/date."""
        suffix = Path(str(target_date.year)) / f"{target_date.month:02d}" / f"{target_date.day:02d}"
        candidates = [self.bronze_dir / suffix]

        # Some aggregate transformers read from explicit sibling partitions, e.g.
        # open_meteo/historical reads bronze/open_meteo/historical_london.
        parent = self.bronze_dir.parent
        if parent.exists():
            for sibling_dataset in self.BRONZE_SIBLING_DATASETS:
                candidates.append(parent / sibling_dataset / suffix)

        existing = [p for p in candidates if p.exists()]
        if not existing and self.source not in _EXACT_PARTITION_ONLY_SOURCES:
            # No exact-date partition found; fall back to the nearest covering
            # partition so that transformers that iterate _bronze_date_dirs()
            # also benefit from the Variant A/B fix. Skipped for exact-only
            # sources (P0.8) — see _EXACT_PARTITION_ONLY_SOURCES docstring.
            fallback = self._find_covering_bronze_partition(target_date)
            if fallback is not None:
                return [fallback]
        return [path for path in candidates if path.exists()]

    def _find_covering_bronze_partition(
        self,
        target_date: date,
        max_lookback_days: int = 35,
        *,
        bronze_dir: Path | None = None,
    ) -> Path | None:
        """Return the nearest prior bronze partition likely to contain data for target_date.

        Used when the connector batched multiple days into one fetch and stored
        all files under the window-start date rather than the target date.
        Scans back up to max_lookback_days looking for any partition that has raw files.
        Returns None if nothing found within the window.

        Pass ``bronze_dir`` to search a location-specific directory instead of
        ``self.bronze_dir`` (used by multi-location transformers such as openmeteo).
        """
        base = bronze_dir if bronze_dir is not None else self.bronze_dir
        for delta in range(1, max_lookback_days + 1):
            candidate_date = target_date - timedelta(days=delta)
            candidate_path = (
                base
                / str(candidate_date.year)
                / f"{candidate_date.month:02d}"
                / f"{candidate_date.day:02d}"
            )
            if candidate_path.exists() and any(candidate_path.glob("raw_*")):
                return candidate_path
        return None

    def _bronze_path_for_date(
        self,
        target_date: date,
        max_lookback_days: int = 35,
    ) -> Path | None:
        """Return the bronze partition path to read for target_date.

        Returns exact partition if it exists and has raw files, falls back to
        the nearest covering partition, or None if nothing found. For sources
        in ``_EXACT_PARTITION_ONLY_SOURCES`` (P0.8), never falls back — the
        exact partition or nothing, since a covering-fallback hit for those
        sources would fabricate wrong-day rows (see that constant's docstring).
        """
        exact = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if exact.exists() and any(exact.glob("raw_*")):
            return exact
        if self.source in _EXACT_PARTITION_ONLY_SOURCES:
            return None
        return self._find_covering_bronze_partition(target_date, max_lookback_days)

    @staticmethod
    def _timestamp_from_sidecar(meta_path: Path) -> datetime | None:
        """Extract the most useful timestamp field from a bronze sidecar."""
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Failed to parse bronze sidecar %s: %s", meta_path, exc)
            return None

        # Preference order, most-to-least authoritative as the historical
        # availability anchor. `written_at` (durable bronze write completion)
        # is preferred over `fetched_at` (stamped at RawResponse construction,
        # before any paging/retries) so reingest reconstructs availability from
        # the true write time rather than a pre-write proxy. The direction is
        # conservative (written_at >= fetched_at), so this never makes a row
        # look available earlier than it truly was. `response_received_at` is
        # an as-yet-unwritten reserved key kept for forward compatibility.
        for key in ("available_at", "written_at", "response_received_at", "fetched_at"):
            raw_value = meta.get(key)
            if raw_value:
                timestamp = BaseSilverTransformer._parse_timestamp(raw_value)
                if timestamp is not None:
                    return timestamp
        return None

    @staticmethod
    def _parse_timestamp(raw_value: object) -> datetime | None:
        if isinstance(raw_value, datetime):
            value = raw_value
        elif isinstance(raw_value, str):
            normalized = raw_value.replace("Z", "+00:00")
            try:
                value = datetime.fromisoformat(normalized)
            except ValueError:
                logger.warning("Could not parse bronze sidecar timestamp: %s", raw_value)
                return None
        else:
            return None

        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _write_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        available_at: datetime,
    ) -> None:
        """Write DataFrame to partitioned Parquet.

        Default branch (``APPEND_ONLY = False``) writes a single file per
        ``(dataset, target_date)`` and overwrites it on each run via
        :func:`write_parquet`'s atomic temp-then-rename. The ``APPEND_ONLY =
        True`` branch suffixes the filename with the ISO ``available_at``
        timestamp so re-ingest with a sidecar-derived ``available_at`` is
        idempotent (two reingest passes produce the same path and the second
        cleanly replaces the first), while distinct live runs produce
        distinct files. See ``docs/DECISION_LOG/ADR-018``.
        """
        out_dir = self.silver_dir / f"year={target_date.year}" / f"month={target_date.month:02d}"
        if self.APPEND_ONLY:
            run_stamp = available_at.isoformat().replace(":", "-").replace("+", "-")
            filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}_run{run_stamp}.parquet"
        else:
            filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.parquet"
        final_path = out_dir / filename
        write_parquet(df, final_path)

    def _write_csv(self, df: pl.DataFrame, target_date: date) -> None:
        """Write DataFrame to CSV at data/silver/{source}/{dataset}/{dataset}_{YYYYMMDD}.csv."""
        csv_dir = self.silver_dir
        csv_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.csv"
        final_path = csv_dir / filename
        tmp_path = csv_dir / f".tmp_{filename}"
        df.write_csv(tmp_path)
        os.replace(tmp_path, final_path)
        logger.debug(f"Wrote CSV: {len(df)} rows to {final_path}")
