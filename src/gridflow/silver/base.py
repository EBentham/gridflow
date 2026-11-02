"""Abstract base class for silver-layer transformers."""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, assert_never

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from pathlib import Path

import polars as pl
from pydantic import BaseModel, ValidationError

from gridflow.silver.partition_window import (
    IntervalSemantics,
    OwnershipVerdict,
    RequestWindow,
    WindowReason,
    exclude_out_of_window,
    filter_frame_to_window,
    neighbour_owns,
    partition_request_window,
)
from gridflow.storage.parquet import write_parquet
from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VintagePolicy:
    """Dated approximation of availability for events before a UTC cutover.

    Attributes:
        name: Versioned policy label, distinct from vendor and ingest-clock.
        lag: Estimated delay from the transformer's event time.
        dated: Date the assumption was recorded.
        rule: Human-readable assumption, rationale, and verification caveats.
        applies_before: Exclusive UTC event-time boundary for reconstruction.
    """

    name: str
    lag: timedelta
    dated: date
    rule: str
    applies_before: datetime

    def __post_init__(self) -> None:
        """Reject invalid declarations before any rows can be stamped."""
        if not self.name.strip() or self.name in {"vendor", "ingest-clock"}:
            raise ValueError("VintagePolicy name must be non-empty and not a reserved label")
        if self.lag < timedelta(0):
            raise ValueError("VintagePolicy lag must be non-negative")
        if not (
            self.applies_before.tzinfo == UTC
            or (
                self.applies_before.utcoffset() == timedelta(0)
                and getattr(self.applies_before.tzinfo, "key", None) == "UTC"
            )
        ):
            raise ValueError("VintagePolicy applies_before must be tz-aware UTC")


_VALIDATION_SAMPLE_LIMIT = 5
"""Max distinct validation-error strings logged per ``run()`` (fail-soft; bounded)."""

_NON_VALUE_COLUMNS = frozenset({"published_at", "ingested_at", "data_provider"})
"""Columns :meth:`BaseSilverTransformer.dedup_to_grain` never compares as values:
two copies of one row from different fetches differ only in these."""

_BRONZE_VINTAGE_COLUMN = "gf_bronze_vintage"
"""Transient per-row bronze stamp for ``LOCKSTEP_BRONZE_READ`` reads; never reaches silver.

No frame-level ``available_at`` is right once two bodies with different stamps
contribute rows: ``max`` hides the earlier body's rows from a point-in-time
query between the two, and ``min`` leaks the later body's. So each row carries
its own body's stamp into the ingest-time arm of
``coalesce(published_at, ingest_time)`` (ADR-025).

Lower snake case so ENTSO-G's ``_camel_to_snake`` leaves it alone. The
collision guard compares normalised names, because vendor spellings such as
``gfBronzeVintage`` normalise onto this one.
"""

_SIDECAR_TIMESTAMP_KEYS: tuple[str, ...] = (
    "available_at",
    "written_at",
    "response_received_at",
    "fetched_at",
)
"""Bronze sidecar timestamp keys, most authoritative first.

``written_at`` (the bronze write) beats ``fetched_at`` (taken before paging and
retries), and ``written_at >= fetched_at``, so the order never makes a row look
available earlier than it was. ``response_received_at`` is reserved. A present
key that fails to parse does not end the search: a later key can still vouch.

``bronze/writer.py`` records ``written_at`` just before the body write, so
every availability claim here is over the recorded stamp.
"""


class BronzeVouchReason(StrEnum):
    """Why a bronze body could not be vouched for by its own sidecar (ADR-028)."""

    NO_SIDECAR = "NO_SIDECAR"
    """No ``<body stem>.meta.json`` beside the body: a crash between the body
    write and the sidecar write."""
    UNREADABLE_SIDECAR = "UNREADABLE_SIDECAR"
    """``OSError``/``JSONDecodeError`` on the sidecar, or syntactically valid
    but non-object JSON (``[1, 2]``, ``"text"``, ``3``, ``null``)."""
    NO_TIMESTAMP_KEY = "NO_TIMESTAMP_KEY"
    """Valid sidecar object, but no key from :data:`_SIDECAR_TIMESTAMP_KEYS`
    was present and truthy."""
    UNPARSEABLE_TIMESTAMP = "UNPARSEABLE_TIMESTAMP"
    """Every present-and-truthy key failed to parse, not merely the first."""
    UNUSABLE_PROVENANCE = "UNUSABLE_PROVENANCE"
    """The sidecar vouched, but the source-specific provenance the transformer
    requires was absent or unparseable, so ``read_bronze_file`` returned an
    empty frame and the body was declined."""
    UNPARSEABLE_BODY = "UNPARSEABLE_BODY"
    """The body itself could not be parsed, so its whole date was refused."""


class UnparseableBronzeError(ValueError):
    """Bronze bodies for one date could not be parsed, so the date is refused.

    Raised by :meth:`BaseSilverTransformer._read_bronze_records` once every
    body of the date has been tried, so the error names all of them. It is
    raised before anything is written for that date, which is what lets the
    runner keep the old silver file and carry on with the next date.
    """

    def __init__(self, paths: Sequence[Path]) -> None:
        self.paths = tuple(paths)
        super().__init__("unparseable bronze bodies: " + ", ".join(map(str, self.paths)))


class BronzeReadSelection(StrEnum):
    """Which vouched bronze bodies a lockstep read consumes.

    Applied to the vouched list, never before vouching: truncating first would
    let one orphan newest body empty the frame.
    """

    ALL = "ALL"
    """Examine every candidate; read every one that vouches."""
    NEWEST_VOUCHED = "NEWEST_VOUCHED"
    """Walk candidates in order and stop at the first that vouches. The bodies
    stepped over are reported as unvouched."""


@dataclass(frozen=True)
class VouchedBronzeSet:
    """The one bronze listing that both the frame read and the vintage use.

    Built from one scan and one sidecar read per examined candidate, so the
    files read and the files that stamp ``available_at`` cannot disagree. It
    offers no frame-level stamp: each row takes its own body's stamp.

    Attributes:
        entries: ``(body path, that body's recorded stamp)`` pairs, in
            candidate order.
        unvouched: ``(body path, reason)`` for every examined candidate that
            could not vouch. Excluded from the frame and the vintage, never
            modified.
        examined: How many candidates were probed. ``NEWEST_VOUCHED`` stops
            at the first vouched one.
    """

    entries: tuple[tuple[Path, datetime], ...]
    unvouched: tuple[tuple[Path, BronzeVouchReason], ...]
    examined: int

    @property
    def paths(self) -> tuple[Path, ...]:
        """The body paths whose rows may enter the frame, in candidate order."""
        return tuple(path for path, _ in self.entries)


@dataclass(frozen=True)
class SidecarDiagnostic:
    """One failure seen while classifying a sidecar, in the order it occurred.

    Attributes:
        key: The :data:`_SIDECAR_TIMESTAMP_KEYS` entry that failed, or ``None``
            for a file-level failure (unreadable / undecodable sidecar).
        reason: The classification this individual failure carries.
        detail: Text for the WARNING the logging wrapper emits, or ``None``
            for a failure that is not logged: a present value that is neither
            a string nor a datetime fails silently.
    """

    key: str | None
    reason: BronzeVouchReason
    detail: str | None = None


@dataclass(frozen=True)
class SidecarRead:
    """Pure classification of one bronze sidecar.

    Attributes:
        timestamp: The vouched tz-aware UTC stamp, or ``None``.
        reason: ``None`` iff ``timestamp`` is not ``None``.
        diagnostics: Every failure seen, in order, including ones before a
            later key vouched, so the logging wrapper can warn once per
            failed key.
        non_object_json: ``True`` iff the sidecar held syntactically valid
            JSON that is not an object.
        payload: That non-object payload, which the logging wrapper calls
            ``.get`` on to raise the parser's ``AttributeError``. Meaningless
            unless ``non_object_json`` is ``True``.
    """

    timestamp: datetime | None
    reason: BronzeVouchReason | None
    diagnostics: tuple[SidecarDiagnostic, ...] = ()
    non_object_json: bool = False
    payload: Any = None


_EXACT_PARTITION_ONLY_SOURCES: frozenset[str] = frozenset({"entsoe", "entsog"})
"""Sources whose connectors write one bronze partition per UTC day.

Their connectors split every multi-day window into one request per day, so the
covering-partition fallback in :meth:`BaseSilverTransformer._bronze_path_for_date`
could only relabel a neighbouring day's rows under the requested date. Keyed by
source because the guarantee belongs to the connector, so a new dataset cannot
forget it. NESO batches 14 days per fetch and enforces its own exact-partition
read in ``GenericNesoJsonTransformer._bronze_candidates`` (ADR-026).
"""

_PUBLICATION_WINDOW_FILTER_SOURCES: frozenset[str] = frozenset({"elexon"})
"""Sources whose bronze partitions are publication windows, not settlement days.

Elexon's ``PUBLISH_DATETIME`` chunks are closed at both ends and share their
boundary instant with the next chunk, so a boundary record lands in both
partitions unless it is trimmed under the neighbour-durability proof. Keyed by
source for the same reason as ``_EXACT_PARTITION_ONLY_SOURCES``; per-dataset
scope and exemptions live in ``silver.elexon._publication_window``.
"""


@dataclass(frozen=True)
class _PublicationWindowPlan:
    """One partition's request-window filter inputs, shared by Elexon's
    publication-window filter and ENTSO-E's event-window filter.

    Resolved once per ``run()`` from the partition's own sidecars, all or
    nothing. The neighbour-durability proof a CLOSED window needs reads
    another partition, so it waits until the frame has a row at the boundary.
    """

    column: str
    window: RequestWindow
    from_param: str
    to_param: str
    interval_semantics: IntervalSemantics = IntervalSemantics.CLOSED
    """The vendor's own request interval, which picks the filter:

    - ``CLOSED`` (default, Elexon): a boundary row is requested by both
      adjacent chunks, so dropping it needs proof that the neighbour holds it
      (:meth:`BaseSilverTransformer._apply_publication_window_filter`). The
      lower bound is never enforced.
    - ``HALF_OPEN`` (ENTSO-E): the request is ``[start, end)`` and rows
      outside it were never requested by this partition, so they are always
      dropped (:meth:`BaseSilverTransformer._apply_event_window_filter`)."""


def gas_day_event_time_expr(column: str = "gas_day") -> pl.Expr:
    """Build the fixed-06:00 UTC event-time expression for a gas day.

    Fixed 06:00 UTC is gridflow's labelling convention for gas days and is
    deliberately not DST-aware. Changing it needs a major dataset-version bump.

    Args:
        column: Name of the ``pl.Date`` gas-day column.

    Returns:
        A UTC-aware Polars expression aliased to ``event_time``.
    """
    return (pl.col(column).cast(pl.Datetime("us", "UTC")) + pl.duration(hours=6)).alias(
        "event_time"
    )


def append_only_run_stamp(available_at: datetime) -> str:
    """Format an APPEND_ONLY filename's ``run`` suffix (ADR-018), the one site.

    ``_write_silver`` names a vintage file with it, and the NESO Data Portal
    engine derives the same name to find or claim a file.
    """
    return available_at.isoformat().replace(":", "-").replace("+", "-")


class BaseSilverTransformer(ABC):
    """Base class for bronze -> silver transformations.

    Subclasses implement:
    - source: the data source name
    - dataset: the dataset name
    - _parse_bronze_body() (the vendor bases do this), or read_bronze() itself
    - transform(): apply normalisation, validation, deduplication
    """

    source: str
    dataset: str
    PARTITION_DATE_COLUMN: ClassVar[str | None] = None
    """Opt in to declared covering-set reads and destination-row ownership trimming."""
    PARTITION_SOURCE_OFFSETS: ClassVar[tuple[int, ...]] = (0,)
    """Ordered source-date offsets used by partition-owning transformers."""
    schema_cls: ClassVar[type[BaseModel] | None] = None
    """Optional Pydantic schema every ``transform()`` output row is validated against.

    Fail-soft: failures are counted into ``last_validation_failure_count`` and
    the run completes with warnings; rows are never raised on or dropped.
    ``None`` skips validation, for generic families with no fixed contract
    (ENTSO-G, GIE).
    """
    write_silver_csv: bool = False
    """Also write the per-date silver CSV sidecar.

    Off by default: no reader uses it, and the ``export_csv`` command serves
    CSV on demand. ``run_transform`` sets it per instance from
    ``PipelineSettings.write_silver_csv``.
    """
    last_unmapped_count: int = 0
    """Rows whose enum code was unmapped in the most recent ``run()`` (ADR-022).

    Reset at the start of every ``run()``, so a date that returns early is not
    charged the previous date's count.
    """
    last_start_time_fallback_count: int = 0
    last_publication_fallback_count: int = 0
    """Source-read row occurrences that used a settlement-label fallback.

    Declaring covering-set runs may count the same source row again for an adjacent
    destination. This is not a unique-entity or discarded-row count.
    """
    last_partition_trimmed_count: int = 0
    """Prepared source-read occurrences owned by a date other than destination D."""
    last_partition_trim_unrecoverable_count: int = 0
    """Partition-trim occurrences outside the dataset's declared covering set."""
    last_partition_trim_details: tuple[tuple[date, date, str, int], ...] = ()
    """Bounded ``(source, destination, owner, count)`` ownership diagnostics."""
    last_source_exclusion_details: tuple[str, ...] = ()
    """Bounded dated source-partition exclusion diagnostics."""
    last_validation_failure_count: int = 0
    """Rows that failed ``schema_cls`` validation in the most recent ``run()``.

    The rows are still written, so this count is their only signal.
    ``run_transform`` adds :attr:`last_excluded_row_count` to it, so the
    reported ``rows_invalid`` covers rows written despite failing validation
    and rows a transformer removed.
    """
    last_excluded_row_count: int = 0
    """Rows a transformer declared invalid and removed in the most recent ``run()``.

    Transformers add to it with ``+=`` inside ``transform()``, never assign:
    the per-file branch calls ``transform()`` once per body against one reset.
    It exists because the validator only sees ``transform()``'s output, so
    without it a run could drop rows and still report ``success``.
    ``run_transform`` folds it into the validation-failure total; the
    excluding transformer's per-row WARNING tells the two apart.
    """
    last_partition_filter_unresolved_count: int = 0
    """Request-window evaluations that could not resolve in the most recent ``run()``.

    Declaring transformers evaluate each prepared covering-set source, so
    adjacent destinations may count the same partition again.
    """
    last_partition_filter_all_dropped_count: int = 0
    """Rows dropped because the event-window filter excluded a whole frame.

    Only the HALF_OPEN (ENTSO-E) path increments it; the CLOSED (Elexon) path
    refuses such a trim instead. A 100% drop on an in-scope dataset usually
    means it was opted in by mistake, so ``run_transform`` fails the whole date
    range on a nonzero total rather than reporting warnings. A partition-owning
    transformer counts only drops from its own destination date, so a
    neighbour's drop cannot fail a healthy destination.
    """
    last_unvouched_bronze: frozenset[tuple[Path, BronzeVouchReason]] = frozenset()
    """Bronze bodies the most recent ``run()`` excluded because their own
    sidecar could not vouch for them, with the reason (ADR-028).

    Pairs rather than a count, so ``run_transform`` can union them across
    dates: a family that rescans its whole tree per date would otherwise count
    one orphan once per date, and a separate reason counter could not say which
    reason a newly seen path had. Each path has one reason per read.
    """
    last_unvouched_total_exclusion: bool = False
    """``True`` when the most recent ``run()`` examined bronze and none of it vouched.

    Distinct from no bronze at all, which is not a failure. ``run_transform``
    fails the dataset on it: nothing was written, so only a failed status
    stops an older Parquet file passing as current.
    """
    last_unaccounted_empty_frames: int = 0
    """Frames ``transform()`` emptied without counting why, in the most recent ``run()``.

    An empty output is fine when the transformer counted what it declined
    (``last_excluded_row_count`` or ``last_unmapped_count`` moved inside
    ``transform()``), and a silent failure when neither moved, as when
    ``system_prices`` meets a missing required column. Only per-file and
    partition-owning transformers count it; covering-set runs may count the
    same body again for an adjacent destination.
    """
    last_total_unaccounted_exclusion: bool = False
    """``True`` when the most recent ``run()`` examined bronze, produced no
    frame, and at least one of its empty frames was unaccounted.

    ``run_transform`` fails the dataset on it, for the same reason as
    :attr:`last_unvouched_total_exclusion`.
    """
    LOCKSTEP_BRONZE_READ: ClassVar[bool] = False
    """Resolve the bronze read set and the vintage from one scan.

    ``run()`` lists :meth:`_bronze_candidates` once and threads the resulting
    :class:`VouchedBronzeSet` into both the frame read and ``available_at``,
    so the files read and the files that stamp them are the same set.
    Vouching depends only on the bronze writer's body-then-sidecar order, so
    it suits every source; it is opt-in because turning it on changes which
    existing bronze is read. ``run()`` checks :attr:`VINTAGE_PER_BRONZE_FILE`
    first, so a transformer setting both gets the per-file branch.
    """
    EVENT_WINDOW_FILTER: ClassVar[bool] = False
    """Opt in, per transformer, to the ENTSO-E event-window filter.

    ``run()`` resolves the partition's own ``[periodStart, periodEnd)`` request
    window and drops ``timestamp_utc`` rows outside it. ENTSO-E returns whole
    CET/CEST delivery days beyond the request, and those rows were never
    requested by this partition, so no neighbour proof is needed. ADR-026
    Appendix A and ``silver/entsoe/_event_window.py`` record each dataset's
    verdict.
    """
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    VINTAGE_POLICY: ClassVar[VintagePolicy | None] = None
    _listed_bronze: tuple[date, list[Path]] | None = None
    """The plain ``run()``'s one listing while it reads; see :meth:`_bronze_read_set`."""
    APPEND_ONLY: ClassVar[bool] = False
    """Keep every run's silver file instead of replacing one file per date (ADR-018).

    Each run writes a file suffixed with its ``available_at``, and readers
    select the latest at read time. Only datasets that keep several vintages
    per business key opt in, each with a ``LATEST_VIEW_SPECS`` entry.
    """
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = False
    """Give each bronze body its own availability timestamp (ADR-025).

    For revision feeds whose fetch time is their only vintage marker; opt-ins
    implement ``read_bronze_file``. Only the exact date partition is read: the
    covering fallback would stamp a prior day's rows into a file named for
    this date. A body without a parseable sidecar timestamp is skipped loudly,
    because a ``now()`` fallback would mint a new vintage on every
    re-transform. A transformer that assigns ``last_unmapped_count`` must add
    to it instead before opting in, or per-body counts overwrite each other.
    """
    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.json"
    """Which files in a bronze partition are bodies.

    ENTSO-E overrides it with ``"raw_*.xml"`` and the NESO Data Portal with
    ``"raw_*.csv"``. Every reader drops ``*.meta.json`` whatever the glob: a
    sidecar is never a body. On the per-file branches it also decides which
    vintages are written, because each body's sidecar names its silver file.
    """
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ()
    """The dataset's grain: the columns that make one ``transform()`` row per
    real-world entity.

    :meth:`dedup_to_grain` and the duplicate quality check (``cli.py``) key on
    it, so it must include the vintage column (``published_at``) when bronze
    carries several observations per business key: a column left out is a row
    silently lost. ``system_prices`` and ``remit`` keep every vintage without a
    dedup and declare their ``LATEST_VIEW_SPECS`` key. Empty is a bug for any
    registered Elexon or ENTSO-E dataset. Order does not matter: consumers
    compare it as a set.
    """
    OPTIONAL_ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ()
    """Key columns included only when present on the frame being checked, such
    as ``fou2t14d``'s ``settlement_period``. See :meth:`resolve_entity_key`.
    """

    @classmethod
    def resolve_entity_key(cls, available_columns: Iterable[str]) -> tuple[str, ...] | None:
        """Return ``ENTITY_KEY_COLUMNS`` plus whichever optional key columns are present.

        ``None`` when ``ENTITY_KEY_COLUMNS`` is empty. A transformer whose
        ``transform()`` chooses between exclusive shapes
        (``WindForecastTransformer``) overrides it to mirror that choice. A
        classmethod, so the CLI can resolve a key without an instance.
        """
        if not cls.ENTITY_KEY_COLUMNS:
            return None
        available = set(available_columns)
        return tuple(cls.ENTITY_KEY_COLUMNS) + tuple(
            c for c in cls.OPTIONAL_ENTITY_KEY_COLUMNS if c in available
        )

    def dedup_to_grain(self, df: pl.DataFrame) -> pl.DataFrame:
        """Keep one row per declared grain and count the conflicting rows dropped.

        Repeats with the same silver values (overlapping fetches) drop quietly.
        Rows that share the grain but differ in a schema value keep the last one
        read (bronze is read in fetch order, so "last read" is not "newest") and
        count into ``last_excluded_row_count``, so the run completes with
        warnings instead of losing them silently.
        """
        key = self.resolve_entity_key(df.columns)
        if not key:
            raise ValueError(f"{self.source}/{self.dataset} declares no ENTITY_KEY_COLUMNS")
        kept = df.unique(subset=list(key), keep="last", maintain_order=True)
        fields = self.schema_cls.model_fields if self.schema_cls is not None else df.columns
        values = [c for c in df.columns if c in fields and c not in key]
        values = [c for c in values if c not in _NON_VALUE_COLUMNS]
        lost = df.unique(subset=[*key, *values]).height - kept.height
        if lost:
            self.last_excluded_row_count += lost
            logger.warning(
                "%s/%s: %d row(s) share grain %s with different values; kept the last read",
                self.source,
                self.dataset,
                lost,
                list(key),
            )
        return kept

    def __init__(self, data_dir: Path):
        if self.PARTITION_DATE_COLUMN is not None and self.LOCKSTEP_BRONZE_READ:
            raise ValueError(
                f"{type(self).__name__} cannot set both PARTITION_DATE_COLUMN "
                "and LOCKSTEP_BRONZE_READ"
            )
        self.data_dir = data_dir
        self.bronze_dir = data_dir / "bronze" / self.source / self.dataset
        self.silver_dir = data_dir / "silver" / self.source / self.dataset

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read and parse all bronze bodies for a date into one raw frame.

        Reads :meth:`_bronze_read_set` through :meth:`_read_bronze_records`,
        so a body that cannot be parsed refuses the whole date
        (:class:`UnparseableBronzeError`) instead of vanishing while its
        siblings overwrite the partition. Override this only to change how the
        frame is built from the records; choose files in
        :meth:`_bronze_candidates`, so "the files read" stays one listing.
        """
        pairs = self._read_bronze_records(self._bronze_read_set(target_date), target_date)
        rows = [record for _, records in pairs for record in records]
        return pl.DataFrame(rows) if rows else pl.DataFrame()

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

    def _bronze_read_selection(self) -> BronzeReadSelection:
        """This run's selection policy for a lockstep read.

        Families whose policy depends on instance state (the ENTSO-G generic
        family's ``reference_dataset``) override it.
        """
        return BronzeReadSelection.ALL

    def _bronze_candidates(self, target_date: date) -> list[Path]:
        """Resolve the ordered bronze BODY paths read for ``target_date``.

        The one listing of the files read: :meth:`read_bronze` reads these, a
        ``LOCKSTEP_BRONZE_READ`` run vouches these, and a plain reingest stamps
        the frame from these (:meth:`_vintage_of`). The default is the bodies of
        the exact date partition. To pick files another way, override this, not
        ``read_bronze``, so everything derived from the files agrees with the
        read. Select by path names and sidecar contents only, never by mtimes
        or the clock, so the same bronze always gives the same listing.

        Return the full ordered list: any "newest only" choice belongs to
        :class:`BronzeReadSelection`, after vouching. Never return
        ``*.meta.json``: a sidecar read as a body has no sidecar of its own, so
        it would classify ``NO_SIDECAR`` and fail every healthy run.

        Args:
            target_date: The date being transformed.

        Returns:
            Body paths in the family's own selection order.
        """
        day = PathBuilder(self.data_dir).bronze_date_dir(
            self.source, self.dataset, target_date, dataset_dir=self.bronze_dir
        )
        return sorted(
            path for path in day.glob(self.BRONZE_BODY_GLOB) if not path.name.endswith(".meta.json")
        )

    def _bronze_read_set(self, target_date: date) -> list[Path]:
        """The bodies a :meth:`read_bronze` of ``target_date`` reads.

        Inside a plain ``run()`` this is the one listing that run took, which
        also stamps the frame on reingest: a second listing could differ (a
        sidecar landing can move H8's latest-fetch choice) and split the rows
        from their stamp. Outside a run it lists afresh.
        """
        listed = self._listed_bronze
        if listed is not None and listed[0] == target_date:
            return list(listed[1])
        return self._bronze_candidates(target_date)

    def _read_bronze_records(
        self,
        paths: Sequence[Path],
        target_date: date,
    ) -> tuple[tuple[Path, list[dict[str, Any]]], ...]:
        """Parse each bronze body and apply this family's date filter.

        The default parses every path with :meth:`_parse_bronze_body`. A body
        that raises ``OSError`` or ``ValueError`` (bad JSON or UTF-8, malformed
        XML, a refused envelope) does not vanish: once every path has been
        tried, :class:`UnparseableBronzeError` names them all, before anything
        is written for the date. Any other exception is a parser bug and
        propagates as it is.

        Returns per-file record lists and builds no frame, so the lockstep
        branch can build its rows and stamps from one object. ``target_date``
        is required because the ENTSO-G families filter rows by date inside the
        read. Log at most one record above ``DEBUG`` per call.

        Args:
            paths: Body paths, in candidate order (vouched ones, under lockstep).
            target_date: The date being transformed.

        Returns:
            ``(path, records)`` pairs in the same order as ``paths``.
        """
        pairs: list[tuple[Path, list[dict[str, Any]]]] = []
        failures: list[tuple[Path, Exception]] = []
        for path in paths:
            try:
                pairs.append((path, self._parse_bronze_body(path)))
            except (OSError, ValueError) as exc:
                failures.append((path, exc))
        if failures:
            logger.error(
                "%s/%s %s: refusing the date, %d bronze body(ies) could not be parsed: %s",
                self.source,
                self.dataset,
                target_date,
                len(failures),
                "; ".join(f"{path}: {exc}" for path, exc in failures),
            )
            raise UnparseableBronzeError([path for path, _ in failures])
        return tuple(pairs)

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        """Parse one bronze body into its raw records.

        Raise ``ValueError`` (or let ``OSError`` through) when the body is not
        valid for this vendor; :meth:`_read_bronze_records` turns that into a
        refusal of the date. The vendor bases implement it
        (``silver/elexon/_base.py``, ``silver/entsoe/_base.py``). A caller that
        writes as it goes, such as a ``VINTAGE_PER_BRONZE_FILE`` reader, must
        catch the error itself: earlier bodies' vintages are already written.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement _parse_bronze_body or read_bronze"
        )

    def _normalise_raw_column_name(self, column: str) -> str:
        """Normalisation this family applies to RAW bronze column names.

        Used only by the lockstep branch's reserved-name collision guard.
        Families that fold vendor spellings together (ENTSO-G's
        ``_camel_to_snake``) override it, so a vendor field spelled
        ``gfBronzeVintage`` cannot replace the true bronze stamp.
        """
        return column

    def _reset_run_counters(self) -> None:
        """Reset every per-run ``last_*`` counter.

        Called first by :meth:`run` and by the NESO Data Portal engine, before
        any early return, so a date is never charged the previous date's counts.
        """
        self.last_unmapped_count = 0
        self.last_start_time_fallback_count = 0
        self.last_publication_fallback_count = 0
        self.last_partition_trimmed_count = 0
        self.last_partition_trim_unrecoverable_count = 0
        self.last_partition_trim_details = ()
        self.last_source_exclusion_details = ()
        self.last_validation_failure_count = 0
        self.last_excluded_row_count = 0
        self.last_partition_filter_unresolved_count = 0
        self.last_partition_filter_all_dropped_count = 0
        self.last_unvouched_bronze = frozenset()
        self.last_unvouched_total_exclusion = False
        self.last_unaccounted_empty_frames = 0
        self.last_total_unaccounted_exclusion = False

    def run(
        self,
        target_date: date,
        run_id: str | None = None,
        reingest: bool = False,
    ) -> int:
        """Execute the full bronze -> silver pipeline for one date.

        Returns the number of rows written.
        """
        self._reset_run_counters()

        resolved_run_id = run_id or f"adhoc-{datetime.now(UTC).isoformat()}"
        if self.PARTITION_DATE_COLUMN is not None:
            return self._run_partition_owned(target_date, resolved_run_id, reingest)

        frames: list[pl.DataFrame] = []
        # The per-file branch counts rows instead of keeping frames, so one
        # date's memory is bounded by one body; there `frames` collects only
        # for the opt-in CSV sidecar.
        released_rows = 0
        released_frames = 0
        saw_bronze = False

        # At most one plan applies: the publication filter is Elexon-only and
        # no Elexon transformer sets EVENT_WINDOW_FILTER.
        window_plan = self._resolve_publication_window_plan(
            target_date
        ) or self._resolve_event_window_plan(target_date)

        if self.VINTAGE_PER_BRONZE_FILE:
            # Exact date partition only: a covering-partition fallback would
            # stamp a prior day's rows into a file named for target_date.
            exact_dir = (
                self.bronze_dir
                / str(target_date.year)
                / f"{target_date.month:02d}"
                / f"{target_date.day:02d}"
            )
            date_dirs = [exact_dir] if exact_dir.exists() else []
            # Collected in locals and published in the `finally`, so a body
            # that raises mid-loop still leaves the earlier exclusions on record.
            unvouched: list[tuple[Path, BronzeVouchReason]] = []
            examined = 0
            try:
                for date_dir in date_dirs:
                    for raw_path in sorted(date_dir.glob(self.BRONZE_BODY_GLOB)):
                        if raw_path.name.endswith(".meta.json"):
                            continue
                        examined += 1
                        available_at = self._timestamp_from_sidecar(
                            raw_path.with_suffix(".meta.json")
                        )
                        if available_at is None:
                            logger.warning(
                                "Skipping bronze file with no usable sidecar timestamp "
                                "(cannot assign an honest vintage): %s",
                                raw_path,
                            )
                            # The classifier supplies only the reason, which is
                            # never None here; acceptance stays with
                            # _timestamp_from_sidecar.
                            classified = self._read_sidecar_timestamp(
                                raw_path.with_suffix(".meta.json")
                            )
                            unvouched.append(
                                (
                                    raw_path,
                                    classified.reason or BronzeVouchReason.NO_TIMESTAMP_KEY,
                                )
                            )
                            continue
                        raw_df = self.read_bronze_file(raw_path)
                        if raw_df.is_empty():
                            logger.warning(
                                "Skipping bronze file that read as empty (its own "
                                "provenance is unusable): %s",
                                raw_path,
                            )
                            unvouched.append((raw_path, BronzeVouchReason.UNUSABLE_PROVENANCE))
                            continue
                        saw_bronze = True
                        clean_df = self._process_frame(
                            raw_df, target_date, resolved_run_id, available_at, window_plan
                        )
                        if clean_df is not None:
                            self._write_silver(clean_df, target_date, available_at=available_at)
                            released_rows += clean_df.height
                            released_frames += 1
                            if self.write_silver_csv:
                                frames.append(clean_df)
                        del raw_df, clean_df
            finally:
                self.last_unvouched_bronze = frozenset(unvouched)
                # Over declined FILES, not unprocessed frames: a body whose
                # rows were all excluded was consumed, and its rows are counted
                # as warnings. run_transform checks this rung before warnings,
                # so the looser predicate would fail a run that should warn.
                self.last_unvouched_total_exclusion = examined > 0 and len(unvouched) == examined
                # Declined bodies are bronze: skip the "No bronze data" warning.
                saw_bronze = saw_bronze or bool(unvouched)
                self.last_total_unaccounted_exclusion = (
                    examined > 0 and released_frames == 0 and self.last_unaccounted_empty_frames > 0
                )
        elif self.LOCKSTEP_BRONZE_READ:
            # One scan and one sidecar read per candidate, used for both the
            # rows and their stamps, so the two cannot disagree.
            candidates = self._bronze_candidates(target_date)
            vouched = self._resolve_vouched_bronze_set(candidates, self._bronze_read_selection())
            self.last_unvouched_bronze = frozenset(vouched.unvouched)
            self.last_unvouched_total_exclusion = vouched.examined > 0 and not vouched.entries
            # Unvouched bodies get their own report, so skip the "No bronze
            # data" warning; with no candidates at all it still fires.
            saw_bronze = saw_bronze or bool(vouched.unvouched)
            if vouched.entries:
                stamp_by_path = dict(vouched.entries)
                pairs = self._read_bronze_records(vouched.paths, target_date)
                rows = [record for _, records in pairs for record in records]
                # Vouched bronze holding only off-date rows still warns.
                saw_bronze = saw_bronze or bool(rows)
                if rows:
                    # A reader returning records for a path that was never
                    # vouched would stamp them with a sibling's timestamp.
                    smuggled = [path for path, _ in pairs if path not in stamp_by_path]
                    if smuggled:
                        raise ValueError(
                            f"{self.source}/{self.dataset}: the bronze reader returned "
                            f"records for {smuggled}, which are not in the vouched read "
                            "set. The read set and the stamp set must be ONE set."
                        )
                    live_now = None if reingest else datetime.now(UTC)
                    stamps = [
                        stamp_by_path[path] if live_now is None else live_now
                        for path, records in pairs
                        for _ in records
                    ]
                    raw_df = pl.DataFrame(rows, infer_schema_length=None)
                    collisions = [
                        column
                        for column in raw_df.columns
                        if self._normalise_raw_column_name(column) == _BRONZE_VINTAGE_COLUMN
                    ]
                    if collisions:
                        raise ValueError(
                            f"{self.source}/{self.dataset}: bronze column(s) {collisions} "
                            f"normalise onto the reserved bronze-vintage carrier "
                            f"{_BRONZE_VINTAGE_COLUMN!r}. A vendor value there would "
                            "silently replace the true bronze stamp and corrupt "
                            "available_at. Rename the carrier before proceeding."
                        )
                    raw_df = raw_df.with_columns(
                        pl.Series(_BRONZE_VINTAGE_COLUMN, stamps, dtype=pl.Datetime("us", "UTC"))
                    )
                    clean_df = self._process_frame(
                        raw_df,
                        target_date,
                        resolved_run_id,
                        live_now if live_now is not None else stamps[0],
                        window_plan,
                        vintage_column=_BRONZE_VINTAGE_COLUMN,
                    )
                    if clean_df is not None:
                        self._write_silver(
                            clean_df,
                            target_date,
                            available_at=live_now if live_now is not None else stamps[0],
                        )
                        frames.append(clean_df)
        else:
            # One listing feeds both the read and the reingest stamp.
            bodies = self._bronze_candidates(target_date)
            self._listed_bronze = (target_date, bodies)
            try:
                raw_df = self.read_bronze(target_date)
            finally:
                self._listed_bronze = None
            if not raw_df.is_empty():
                saw_bronze = True
                available_at = (
                    self._vintage_of(bodies, target_date) if reingest else datetime.now(UTC)
                )
                clean_df = self._process_frame(
                    raw_df, target_date, resolved_run_id, available_at, window_plan
                )
                if clean_df is not None:
                    self._write_silver(clean_df, target_date, available_at=available_at)
                    frames.append(clean_df)

        if not frames and released_frames == 0:
            # Distinguish "nothing to read" from "read but transformed to zero
            # rows" (the latter already logged per frame by _process_frame).
            if not saw_bronze:
                logger.warning(f"No bronze data for {self.source}/{self.dataset} on {target_date}")
            return 0

        if self.write_silver_csv:
            # Frames can differ in optional columns (e.g. run_type present in one
            # vintage only) — diagonal concat null-fills instead of raising.
            self._write_csv(pl.concat(frames, how="diagonal"), target_date)

        total_rows = (
            released_rows if self.VINTAGE_PER_BRONZE_FILE else sum(len(frame) for frame in frames)
        )
        logger.info(
            f"Silver write: {self.source}/{self.dataset} {target_date} -> {total_rows} rows"
        )
        return total_rows

    def _run_partition_owned(
        self,
        destination_date: date,
        run_id: str,
        reingest: bool,
    ) -> int:
        """Run an opted-in transformer over its exact declared covering set."""
        source_dates = self._partition_source_dates(destination_date)
        if self.VINTAGE_PER_BRONZE_FILE:
            return self._run_partition_owned_per_body(source_dates, destination_date, run_id)
        return self._run_partition_owned_plain(source_dates, destination_date, run_id, reingest)

    def _partition_source_dates(self, owner_date: date) -> tuple[date, ...]:
        """Translate the ordered dataset offsets into exact source dates for an owner."""
        return tuple(
            owner_date + timedelta(days=offset) for offset in self.PARTITION_SOURCE_OFFSETS
        )

    def _is_partition_owner_recoverable(self, source_date: date, owner_date: date) -> bool:
        """Return whether source S belongs to the declared covering set for owner T."""
        return source_date in self._partition_source_dates(owner_date)

    def _source_window_plan(self, source_date: date) -> _PublicationWindowPlan | None:
        """Resolve exactly one source-relative window plan for a prepared source."""
        return self._resolve_publication_window_plan(
            source_date
        ) or self._resolve_event_window_plan(source_date)

    def _run_partition_owned_plain(
        self,
        source_dates: tuple[date, ...],
        destination_date: date,
        run_id: str,
        reingest: bool,
    ) -> int:
        """Prepare declared source inputs, combine, deduplicate, trim, and write once.

        On reingest each source's rows carry that source's own capture stamp
        (:meth:`_vintage_of` over the bodies it read), so after the combine no
        row holds another capture's stamp.
        """
        now = datetime.now(UTC)
        raw_inputs: list[tuple[date, pl.DataFrame, datetime]] = []
        for source_date in source_dates:
            # One listing per source feeds both its read and its reingest stamp.
            bodies = self._bronze_candidates(source_date)
            self._listed_bronze = (source_date, bodies)
            try:
                raw_df = self.read_bronze(source_date)
            finally:
                self._listed_bronze = None
            stamp = self._vintage_of(bodies, source_date) if reingest and raw_df.height else now
            raw_inputs.append((source_date, raw_df, stamp))
        if all(raw_df.is_empty() for _, raw_df, _ in raw_inputs):
            logger.warning(
                "No bronze data for %s/%s on %s",
                self.source,
                self.dataset,
                destination_date,
            )
            return 0

        own_input_nonempty = any(
            source_date == destination_date and not raw_df.is_empty()
            for source_date, raw_df, _ in raw_inputs
        )
        # Names an append-only file; MID and FUELHH replace one file per date.
        available_at = max(stamp for _, raw_df, stamp in raw_inputs if raw_df.height)
        prepared: list[tuple[date, pl.DataFrame]] = []
        fallback_total = 0
        own_unaccounted = 0
        own_all_dropped = 0
        try:
            for source_date, raw_df, stamp in raw_inputs:
                if raw_df.is_empty():
                    continue
                unaccounted_before = self.last_unaccounted_empty_frames
                all_dropped_before = self.last_partition_filter_all_dropped_count
                self.last_start_time_fallback_count = 0
                try:
                    clean_df = self._process_frame(
                        raw_df,
                        source_date,
                        run_id,
                        stamp,
                        self._source_window_plan(source_date),
                    )
                finally:
                    fallback_total += self.last_start_time_fallback_count
                    unaccounted_delta = self.last_unaccounted_empty_frames - unaccounted_before
                    if unaccounted_delta:
                        self._append_source_exclusion(
                            f"source={source_date} destination={destination_date} "
                            "prepared no output for an unaccounted reason"
                        )
                    if source_date == destination_date:
                        own_unaccounted += unaccounted_delta
                        own_all_dropped += (
                            self.last_partition_filter_all_dropped_count - all_dropped_before
                        )
                if clean_df is not None:
                    prepared.append((source_date, clean_df))
        finally:
            self.last_start_time_fallback_count = fallback_total
            self.last_partition_filter_all_dropped_count = own_all_dropped

        own_prepared = any(source_date == destination_date for source_date, _ in prepared)
        self.last_total_unaccounted_exclusion = (
            own_input_nonempty and not own_prepared and own_unaccounted > 0
        )
        if not prepared:
            return 0

        ledger_frames: list[pl.DataFrame] = []
        for source_date, frame in prepared:
            ledger_frames.append(
                self._record_partition_ownership(
                    frame,
                    source_date,
                    destination_date,
                    trim=False,
                )
            )

        combined = pl.concat(ledger_frames, how="diagonal")
        entity_key = self.resolve_entity_key(combined.columns)
        if entity_key is None:
            raise ValueError(
                f"{self.source}/{self.dataset}: PARTITION_DATE_COLUMN requires a declared "
                "entity key for deterministic cross-source deduplication"
            )
        dedup_key = list(entity_key)
        if "run_type" in combined.columns and "run_type" not in dedup_key:
            dedup_key.append("run_type")
        combined = combined.unique(subset=dedup_key, keep="last", maintain_order=True)
        sort_columns = list(dict.fromkeys(["timestamp_utc", *dedup_key]))
        combined = combined.sort([column for column in sort_columns if column in combined.columns])
        combined = self._select_partition_owner(combined, destination_date)

        if combined.is_empty() and not self._silver_destination(destination_date).exists():
            logger.info(
                "Successful ownership-empty processing for %s/%s destination %s; "
                "no existing destination to replace",
                self.source,
                self.dataset,
                destination_date,
            )
            return 0

        self._write_silver(combined, destination_date, available_at=available_at)
        if self.write_silver_csv:
            self._write_csv(combined, destination_date)
        logger.info(
            "Silver write: %s/%s %s -> %d rows",
            self.source,
            self.dataset,
            destination_date,
            combined.height,
        )
        return combined.height

    def _run_partition_owned_per_body(
        self,
        source_dates: tuple[date, ...],
        destination_date: date,
        run_id: str,
    ) -> int:
        """Prepare and ownership-trim exact per-body captures without collapsing vintages."""
        unvouched: list[tuple[Path, BronzeVouchReason]] = []
        source_stats: dict[date, dict[str, int]] = {
            source_date: {"examined": 0, "unvouched": 0, "prepared": 0, "unaccounted": 0}
            for source_date in source_dates
        }
        csv_frames: list[pl.DataFrame] = []
        total_rows = 0
        fallback_total = 0
        own_all_dropped = 0
        try:
            for source_date in source_dates:
                date_dir = PathBuilder(self.data_dir).bronze_date_dir(
                    self.source,
                    self.dataset,
                    source_date,
                    dataset_dir=self.bronze_dir,
                )
                if not date_dir.exists():
                    continue
                window_resolved = False
                window_plan: _PublicationWindowPlan | None = None
                for raw_path in sorted(date_dir.glob(self.BRONZE_BODY_GLOB)):
                    if raw_path.name.endswith(".meta.json"):
                        continue
                    source_stats[source_date]["examined"] += 1
                    available_at = self._timestamp_from_sidecar(raw_path.with_suffix(".meta.json"))
                    if available_at is None:
                        logger.warning(
                            "Skipping bronze file with no usable sidecar timestamp "
                            "(cannot assign an honest vintage): %s",
                            raw_path,
                        )
                        classified = self._read_sidecar_timestamp(
                            raw_path.with_suffix(".meta.json")
                        )
                        reason = classified.reason or BronzeVouchReason.NO_TIMESTAMP_KEY
                        unvouched.append((raw_path, reason))
                        source_stats[source_date]["unvouched"] += 1
                        self._append_source_exclusion(
                            f"source={source_date} destination={destination_date} "
                            f"path={raw_path} reason={reason}"
                        )
                        continue
                    raw_df = self.read_bronze_file(raw_path)
                    if raw_df.is_empty():
                        logger.warning(
                            "Skipping bronze file that read as empty (its own "
                            "provenance is unusable): %s",
                            raw_path,
                        )
                        unvouched.append((raw_path, BronzeVouchReason.UNUSABLE_PROVENANCE))
                        source_stats[source_date]["unvouched"] += 1
                        self._append_source_exclusion(
                            f"source={source_date} destination={destination_date} "
                            f"path={raw_path} reason={BronzeVouchReason.UNUSABLE_PROVENANCE}"
                        )
                        continue
                    if not window_resolved:
                        window_plan = self._source_window_plan(source_date)
                        window_resolved = True
                    unaccounted_before = self.last_unaccounted_empty_frames
                    all_dropped_before = self.last_partition_filter_all_dropped_count
                    self.last_start_time_fallback_count = 0
                    try:
                        clean_df = self._process_frame(
                            raw_df,
                            source_date,
                            run_id,
                            available_at,
                            window_plan,
                        )
                    finally:
                        fallback_total += self.last_start_time_fallback_count
                        unaccounted_delta = self.last_unaccounted_empty_frames - unaccounted_before
                        source_stats[source_date]["unaccounted"] += unaccounted_delta
                        if source_date == destination_date:
                            own_all_dropped += (
                                self.last_partition_filter_all_dropped_count - all_dropped_before
                            )
                        if unaccounted_delta:
                            self._append_source_exclusion(
                                f"source={source_date} destination={destination_date} "
                                "prepared no output for an unaccounted reason"
                            )
                    if clean_df is None:
                        continue
                    source_stats[source_date]["prepared"] += 1
                    owned = self._record_partition_ownership(
                        clean_df,
                        source_date,
                        destination_date,
                    )
                    if owned.is_empty():
                        logger.info(
                            "Successful ownership-empty body for %s/%s source %s "
                            "destination %s: %s",
                            self.source,
                            self.dataset,
                            source_date,
                            destination_date,
                            raw_path,
                        )
                        continue
                    self._write_silver(owned, destination_date, available_at=available_at)
                    csv_frames.append(owned)
                    total_rows += owned.height
        finally:
            self.last_start_time_fallback_count = fallback_total
            self.last_partition_filter_all_dropped_count = own_all_dropped
            self.last_unvouched_bronze = frozenset(unvouched)
            own = source_stats[destination_date]
            self.last_unvouched_total_exclusion = (
                own["examined"] > 0 and own["unvouched"] == own["examined"]
            )
            self.last_total_unaccounted_exclusion = (
                own["examined"] > 0 and own["prepared"] == 0 and own["unaccounted"] > 0
            )

        if not any(stats["examined"] for stats in source_stats.values()):
            logger.warning(
                "No bronze data for %s/%s on %s",
                self.source,
                self.dataset,
                destination_date,
            )
        if self.write_silver_csv and csv_frames:
            self._write_csv(pl.concat(csv_frames, how="diagonal"), destination_date)
        if total_rows:
            logger.info(
                "Silver write: %s/%s %s -> %d rows",
                self.source,
                self.dataset,
                destination_date,
                total_rows,
            )
        return total_rows

    def _append_source_exclusion(self, detail: str) -> None:
        """Retain bounded, dated source-exclusion diagnostics for the runner."""
        if len(self.last_source_exclusion_details) < 50:
            self.last_source_exclusion_details = (*self.last_source_exclusion_details, detail)

    def _record_partition_ownership(
        self,
        frame: pl.DataFrame,
        source_date: date,
        destination_date: date,
        *,
        trim: bool = True,
    ) -> pl.DataFrame:
        """Record foreign owners before deduplication and optionally select owner D."""
        column = self._partition_owner_column(frame)
        owners = frame.get_column(column).cast(pl.Date, strict=False).to_list()
        counts: dict[date | None, int] = {}
        for owner in owners:
            if owner == destination_date:
                continue
            counts[owner] = counts.get(owner, 0) + 1
        for owner, count in counts.items():
            owner_text = owner.isoformat() if isinstance(owner, date) else "unclassifiable"
            recoverable = isinstance(owner, date) and self._is_partition_owner_recoverable(
                source_date, owner
            )
            self.last_partition_trimmed_count += count
            if not recoverable:
                self.last_partition_trim_unrecoverable_count += count
                logger.warning(
                    "Unsafe partition ownership trim for %s/%s: source=%s "
                    "destination=%s owner=%s count=%d",
                    self.source,
                    self.dataset,
                    source_date,
                    destination_date,
                    owner_text,
                    count,
                )
            if len(self.last_partition_trim_details) < 50:
                self.last_partition_trim_details = (
                    *self.last_partition_trim_details,
                    (source_date, destination_date, owner_text, count),
                )
        return self._select_partition_owner(frame, destination_date) if trim else frame

    def _select_partition_owner(
        self,
        frame: pl.DataFrame,
        destination_date: date,
    ) -> pl.DataFrame:
        """Return only rows whose declared owner is destination D."""
        column = self._partition_owner_column(frame)
        owner_expr = pl.col(column).cast(pl.Date, strict=False) == pl.lit(destination_date)
        return frame.filter(owner_expr.fill_null(False))

    def _partition_owner_column(self, frame: pl.DataFrame) -> str:
        """Return the declared ownership column, failing consistently if absent."""
        column = self.PARTITION_DATE_COLUMN
        if column is None or column not in frame.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: declared partition ownership column "
                f"{column!r} is missing from the prepared frame"
            )
        return column

    def _process_frame(
        self,
        raw_df: pl.DataFrame,
        target_date: date,
        run_id: str,
        available_at: datetime,
        window_plan: _PublicationWindowPlan | None = None,
        *,
        vintage_column: str | None = None,
    ) -> pl.DataFrame | None:
        """Transform, filter, validate, and stamp one bronze-vintage frame.

        Args:
            raw_df: The raw bronze frame.
            target_date: The date being transformed.
            run_id: The pipeline run id stamped into ``source_run_id``.
            available_at: The frame-level ingest vintage. Ignored as the
                ingest-time source when ``vintage_column`` is set (each row
                carries its own).
            window_plan: The resolved request-window filter plan, if any.
            vintage_column: Name of the transient per-row bronze-vintage
                column on ``raw_df``, for ``LOCKSTEP_BRONZE_READ`` reads. When
                set, it is the ingest-time source and is dropped from the
                returned frame: it must never reach silver.

        Raises:
            ValueError: ``vintage_column`` was requested but ``transform()``
                dropped it, so per-row ``available_at`` cannot be derived.
        """
        if raw_df.is_empty():
            return None

        # The only two counters a transformer can move inside transform(); the
        # validator's counter moves later, on a frame an empty output never reaches.
        excluded_before = self.last_excluded_row_count
        unmapped_before = self.last_unmapped_count
        clean_df = self.transform(raw_df)
        if clean_df.is_empty():
            logger.warning(f"Transform produced 0 rows for {target_date}")
            # `!=`, never `>`: the ENTSO-E transformers assign
            # last_unmapped_count rather than add to it.
            accounted = (
                self.last_excluded_row_count != excluded_before
                or self.last_unmapped_count != unmapped_before
            )
            # Gated: counting this for every transformer would change their
            # reported status. A window-filter empty (below) is counted by
            # last_partition_filter_all_dropped_count instead.
            if (self.VINTAGE_PER_BRONZE_FILE or self.PARTITION_DATE_COLUMN is not None) and not (
                accounted
            ):
                self.last_unaccounted_empty_frames += 1
            return None

        if window_plan is not None:
            clean_df = (
                self._apply_event_window_filter(clean_df, window_plan, target_date)
                if window_plan.interval_semantics is IntervalSemantics.HALF_OPEN
                else self._apply_publication_window_filter(clean_df, window_plan, target_date)
            )
            if clean_df.is_empty():
                # Only the HALF_OPEN filter drops a whole frame (counted and
                # logged there); the CLOSED filter refuses to.
                return None

        # Validated before the bitemporal columns exist: schemas do not declare
        # them. Fail-soft, and accumulated across a run's frames.
        self.last_validation_failure_count += self._validate_against_schema(clean_df)
        if vintage_column is not None and vintage_column not in clean_df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: transform() dropped the transient "
                f"bronze-vintage column {vintage_column!r}; per-row available_at "
                "cannot be derived. Carry it through the output projection."
            )
        clean_df = self._add_bitemporal_columns(
            clean_df,
            target_date=target_date,
            run_id=run_id,
            available_at=available_at,
            vintage_column=vintage_column,
        )
        if vintage_column is not None:
            clean_df = clean_df.drop(vintage_column)
        return clean_df

    def _resolve_publication_window_plan(self, target_date: date) -> _PublicationWindowPlan | None:
        """Resolve this partition's request window for Elexon's publication-window filter.

        ``None`` disables the filter: for sources not in
        ``_PUBLICATION_WINDOW_FILTER_SOURCES``, for out-of-scope or exempt
        datasets, and for a partition whose window cannot be resolved (an
        orphan body, an invalid sidecar, an unparseable bound). That last case
        is counted and logged, never narrowed to filtering what it can.

        ``silver.elexon._publication_window`` is imported here, not at module
        level, because ``silver.elexon`` imports this module.
        """
        if self.source not in _PUBLICATION_WINDOW_FILTER_SOURCES:
            return None

        from gridflow.silver.elexon._publication_window import (
            publication_window_column,
            publication_window_params,
        )

        params = publication_window_params(self.dataset)
        if params is None:
            logger.debug(
                "Partition-window filter: %s/%s is out of scope or exempt",
                self.source,
                self.dataset,
            )
            return None
        from_param, to_param = params

        partition_dir = PathBuilder(self.data_dir).bronze_date_dir(
            self.source,
            self.dataset,
            target_date,
            dataset_dir=self.bronze_dir,
        )
        window, reason = partition_request_window(
            partition_dir,
            from_param,
            to_param,
            expect_source=self.source,
            expect_dataset=self.dataset,
        )
        if window is None:
            self.last_partition_filter_unresolved_count += 1
            logger.warning(
                "Partition-window filter unresolved for %s/%s on %s (%s); "
                "filtering disabled for this partition (all-or-nothing)",
                self.source,
                self.dataset,
                target_date,
                reason,
            )
            return None

        return _PublicationWindowPlan(
            column=publication_window_column(self.dataset),
            window=window,
            from_param=from_param,
            to_param=to_param,
        )

    def _apply_publication_window_filter(
        self,
        df: pl.DataFrame,
        plan: _PublicationWindowPlan,
        target_date: date,
    ) -> pl.DataFrame:
        """Trim ``df`` to ``plan.window``, dropping a boundary row only when the
        successor partition provably holds it.

        An absent filter column raises rather than silently skipping the
        filter. The successor partition is read only when a row sits at or
        after the window end. The lower bound is never enforced.
        """
        if plan.column not in df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: publication-window filter column "
                f"{plan.column!r} is absent from transform() output. Declare an "
                "override in PUBLICATION_WINDOW_COLUMN or an exemption in "
                "PUBLICATION_WINDOW_EXEMPT (silver/elexon/_publication_window.py)."
            )

        has_boundary_rows = df.filter(pl.col(plan.column) >= plan.window.end).height > 0
        if has_boundary_rows:
            upper_bound_ownership = neighbour_owns(
                self.bronze_dir,
                plan.window.end,
                plan.from_param,
                plan.to_param,
                expect_source=self.source,
                expect_dataset=self.dataset,
            )
        else:
            upper_bound_ownership = OwnershipVerdict(False, WindowReason.NOT_RESOLVED)

        result = filter_frame_to_window(
            df,
            plan.column,
            plan.window,
            upper_bound_ownership=upper_bound_ownership,
        )

        if result.refused:
            logger.error(
                "Partition-window filter would drop the entire %s/%s frame for "
                "%s; keeping every row instead",
                self.source,
                self.dataset,
                target_date,
            )
        if result.dropped:
            logger.warning(
                "Partition-window filter dropped %d row(s) for %s/%s on %s: a "
                "durable covering chunk in the successor partition owns the "
                "boundary instant",
                result.dropped,
                self.source,
                self.dataset,
                target_date,
            )
        if result.unclassified:
            logger.warning(
                "Partition-window filter: %d row(s) with an unparseable/null %s "
                "could not be classified and were kept for %s/%s on %s",
                result.unclassified,
                plan.column,
                self.source,
                self.dataset,
                target_date,
            )
        if result.below_window:
            logger.warning(
                "Partition-window filter: %d row(s) below the window start were "
                "kept for %s/%s on %s (the lower bound is not enforced for this "
                "source)",
                result.below_window,
                self.source,
                self.dataset,
                target_date,
            )
        if result.retained_reasons:
            logger.warning(
                "Partition-window filter: %d row(s) retained at an unproven "
                "boundary for %s/%s on %s: %s",
                result.boundary_retained,
                self.source,
                self.dataset,
                target_date,
                result.retained_reasons,
            )

        return result.frame

    def _resolve_event_window_plan(self, target_date: date) -> _PublicationWindowPlan | None:
        """Resolve this partition's request window for the ENTSO-E event-window filter.

        ``None`` disables the filter: when ``EVENT_WINDOW_FILTER`` is off, and
        when the partition's window cannot be resolved, which is counted and
        logged. The params are always ``periodStart``/``periodEnd`` and the
        column always ``timestamp_utc``.
        """
        if not self.EVENT_WINDOW_FILTER:
            return None

        from_param, to_param = "periodStart", "periodEnd"
        partition_dir = PathBuilder(self.data_dir).bronze_date_dir(
            self.source,
            self.dataset,
            target_date,
            dataset_dir=self.bronze_dir,
        )
        window, reason = partition_request_window(
            partition_dir,
            from_param,
            to_param,
            expect_source=self.source,
            expect_dataset=self.dataset,
        )
        if window is None:
            self.last_partition_filter_unresolved_count += 1
            logger.warning(
                "Event-window filter unresolved for %s/%s on %s (%s); "
                "filtering disabled for this partition (all-or-nothing)",
                self.source,
                self.dataset,
                target_date,
                reason,
            )
            return None

        return _PublicationWindowPlan(
            column="timestamp_utc",
            window=window,
            from_param=from_param,
            to_param=to_param,
            interval_semantics=IntervalSemantics.HALF_OPEN,
        )

    def _apply_event_window_filter(
        self,
        df: pl.DataFrame,
        plan: _PublicationWindowPlan,
        target_date: date,
    ) -> pl.DataFrame:
        """Drop every row outside ``plan.window``, the ENTSO-E event-window filter.

        ENTSO-E may return whole CET/CEST delivery days beyond the
        ``[periodStart, periodEnd)`` request. Those rows were never requested
        by this partition, so silver drops them without any neighbour proof,
        even when no neighbouring partition exists; bronze still holds the
        records. Every drop is counted and logged. An absent filter column raises.
        """
        if plan.column not in df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: event-window filter column "
                f"{plan.column!r} is absent from transform() output. Declare an "
                "exemption in EVENT_WINDOW_FILTER_EXEMPT "
                "(silver/entsoe/_event_window.py) or unset EVENT_WINDOW_FILTER."
            )

        result = exclude_out_of_window(df, plan.column, plan.window)

        if result.all_dropped:
            # A whole frame outside its own window usually means the dataset
            # was opted in by mistake: the counter makes run_transform fail
            # the date range instead of leaving only a log line.
            self.last_partition_filter_all_dropped_count += result.dropped
            logger.error(
                "Event-window filter excluded 100%% of the %s/%s frame for %s "
                "(%d row(s)): none fall inside the partition's own recorded "
                "[%s, %s) request window (%s). Rows are correctly dropped, not "
                "retained -- but a 100%% drop may indicate this dataset was "
                "opted into EVENT_WINDOW_FILTER by mistake; verify its window "
                "semantics",
                self.source,
                self.dataset,
                target_date,
                result.dropped,
                plan.window.start,
                plan.window.end,
                WindowReason.OUT_OF_REQUEST_SCOPE,
            )
        elif result.dropped:
            logger.warning(
                "Event-window filter excluded %d out-of-scope row(s) for %s/%s "
                "on %s: outside the partition's own recorded [%s, %s) request "
                "window (%s — never requested by this partition; no neighbour "
                "proof needed)",
                result.dropped,
                self.source,
                self.dataset,
                target_date,
                plan.window.start,
                plan.window.end,
                WindowReason.OUT_OF_REQUEST_SCOPE,
            )
        if result.unclassified:
            logger.warning(
                "Event-window filter: %d row(s) with an unparseable/null %s "
                "could not be classified and were kept for %s/%s on %s",
                result.unclassified,
                plan.column,
                self.source,
                self.dataset,
                target_date,
            )

        return result.frame

    def _validate_against_schema(self, df: pl.DataFrame) -> int:
        """Validate every row of the transform output against ``schema_cls``, fail-soft.

        Returns the number of rows that failed. Never raises and never drops a
        row: invalid rows are still written, and the count is the only signal,
        which ``run_transform`` reports as warnings. ``0`` when ``schema_cls``
        is ``None`` or the frame is empty.
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
                # Pydantic wraps only ValueError/AssertionError into
                # ValidationError, so a validator raising anything else would
                # crash the whole date. Count it as invalid and log its type
                # loudly, so a code bug stays visible.
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
        *,
        vintage_column: str | None = None,
    ) -> pl.DataFrame:
        """Add modelling lineage columns before silver output is persisted.

        ``available_at = coalesce(published_at, ingest_time)`` per row
        (ADR-025): the vendor's ``published_at`` wins, and a null falls back to
        the ingest stamp. That stamp is the ``available_at`` argument or, with
        ``vintage_column``, each row's own bronze body stamp.

        With ``VINTAGE_POLICY``, ADR-031 replaces only the fallback for events
        strictly before its cutover whose event time plus lag precedes ingest.
        These rows carry the policy name; other rows carry vendor/ingest-clock.
        Labels are lineage derived after measurement validation, not schema fields.

        Raises:
            TypeError: ``published_at`` is present but not a ``pl.Datetime``,
                which ``pl.coalesce`` would let through, mistyping ``available_at``.
        """
        if available_at.tzinfo is None:
            available_at = available_at.replace(tzinfo=UTC)
        else:
            available_at = available_at.astimezone(UTC)

        ingest_stamp = (
            pl.col(vintage_column).cast(pl.Datetime("us", "UTC"))
            if vintage_column is not None
            else pl.lit(available_at).cast(pl.Datetime("us", "UTC"))
        )
        # Row-wise, so a frame mixing null and non-null published_at falls back
        # per row instead of writing a null available_at, which gridflow_models'
        # fail-closed availability barrier would reject.
        if "published_at" in df.columns:
            published_dtype = df.schema["published_at"]
            if not isinstance(published_dtype, pl.Datetime):
                raise TypeError(
                    f"{self.source}/{self.dataset}: published_at must be a "
                    f"pl.Datetime dtype (tz-aware UTC), got {published_dtype!r}. "
                    "Fix the transformer's published_at emission — do not rely "
                    "on pl.coalesce to catch a mistyped column."
                )
            available_at_expr = pl.coalesce(pl.col("published_at"), ingest_stamp).alias(
                "available_at"
            )
        else:
            available_at_expr = ingest_stamp.alias("available_at")

        policy = self.VINTAGE_POLICY
        if policy is not None:
            df = df.with_columns(self._event_time_expr(df, target_date))
            reconstructed = pl.col("event_time") + policy.lag
            use_policy = (
                (pl.col("event_time") < policy.applies_before) & (reconstructed < ingest_stamp)
            ).fill_null(False)
            fallback = pl.when(use_policy).then(reconstructed).otherwise(ingest_stamp)
            label = pl.when(use_policy).then(pl.lit(policy.name)).otherwise(pl.lit("ingest-clock"))
            if "published_at" in df.columns:
                available_at_expr = pl.coalesce(pl.col("published_at"), fallback)
                label = (
                    pl.when(pl.col("published_at").is_not_null())
                    .then(pl.lit("vendor"))
                    .otherwise(label)
                )
            else:
                available_at_expr = fallback
            return df.with_columns(
                available_at_expr.alias("available_at"),
                pl.lit(run_id).alias("source_run_id"),
                pl.lit(self.DATASET_VERSION).alias("dataset_version"),
                label.alias("vintage_policy"),
            )

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
        if "timestamp_utc" in df.columns:
            return pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")).alias("event_time")

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

    def _vintage_of(self, bodies: Sequence[Path], target_date: date) -> datetime:
        """The reingest stamp of a frame read from ``bodies``: their newest sidecar stamp.

        One stamp per frame, so rows from an older body are stamped late, never
        early. A body without a usable sidecar makes the stamp ``now()``:
        borrowing its siblings' stamp could date its rows before they existed.
        """
        missing: list[Path] = []
        stamps: list[datetime] = []
        for body in bodies:
            meta_path = body.with_suffix(".meta.json")
            read = self._read_sidecar_timestamp(meta_path)
            self._log_sidecar_parse_warnings(meta_path, read)
            if read.timestamp is None:
                missing.append(body)
            else:
                stamps.append(read.timestamp)
        if stamps and not missing:
            return max(stamps)
        now = datetime.now(UTC)
        logger.warning(
            "%s/%s %s: %s; stamping %s",
            self.source,
            self.dataset,
            target_date,
            f"{len(missing)} bronze body(ies) read have no usable sidecar: {missing}"
            if missing
            else "no bronze body listed for the rows read",
            now.isoformat(),
        )
        return now

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
        *,
        bronze_dir: Path | None = None,
    ) -> Path | None:
        """Return the bronze partition path to read for target_date.

        Returns exact partition if it exists and has raw files, falls back to
        the nearest covering partition, or None if nothing found. Sources in
        ``_EXACT_PARTITION_ONLY_SOURCES`` never fall back. Pass ``bronze_dir``
        to resolve within another directory, such as one Open-Meteo location's.
        """
        base = bronze_dir if bronze_dir is not None else self.bronze_dir
        exact = base / str(target_date.year) / f"{target_date.month:02d}" / f"{target_date.day:02d}"
        if exact.exists() and any(exact.glob("raw_*")):
            return exact
        if self.source in _EXACT_PARTITION_ONLY_SOURCES:
            return None
        return self._find_covering_bronze_partition(
            target_date, max_lookback_days, bronze_dir=bronze_dir
        )

    def _resolve_vouched_bronze_set(
        self,
        candidates: Sequence[Path],
        selection: BronzeReadSelection,
    ) -> VouchedBronzeSet:
        """Classify an already-listed candidate list into vouched and unvouched.

        Does no globbing, since a second scan could disagree with the caller's,
        and reads each examined sidecar once, keeping its stamp in the result.
        ``NEWEST_VOUCHED`` stops at the first body that vouches. Logs only at
        ``DEBUG``: reporting is the caller's job.

        Args:
            candidates: Body paths, in the caller's own selection order.
            selection: Which vouched bodies to consume.

        Returns:
            The :class:`VouchedBronzeSet` threaded to both derivations.
        """
        match selection:
            case BronzeReadSelection.ALL:
                stop_at_first_vouched = False
            case BronzeReadSelection.NEWEST_VOUCHED:
                stop_at_first_vouched = True
            case _:  # pragma: no cover - exhaustiveness guard (mypy does not check enum matches)
                assert_never(selection)

        entries: list[tuple[Path, datetime]] = []
        unvouched: list[tuple[Path, BronzeVouchReason]] = []
        examined = 0
        for candidate in candidates:
            examined += 1
            read = self._read_sidecar_timestamp(candidate.with_suffix(".meta.json"))
            if read.timestamp is not None:
                entries.append((candidate, read.timestamp))
                if stop_at_first_vouched:
                    break
                continue
            unvouched.append((candidate, read.reason or BronzeVouchReason.NO_TIMESTAMP_KEY))

        logger.debug(
            "Vouched bronze for %s/%s: %d of %d examined candidate(s) usable",
            self.source,
            self.dataset,
            len(entries),
            examined,
        )
        return VouchedBronzeSet(
            entries=tuple(entries),
            unvouched=tuple(unvouched),
            examined=examined,
        )

    @staticmethod
    def _read_sidecar_timestamp(meta_path: Path) -> SidecarRead:
        """Classify one bronze sidecar, with no logging, clock, globbing or writes.

        Callers decide what to log, so the lockstep path can report one
        aggregate per ``run_transform`` instead of one WARNING per file. Unlike
        :meth:`_timestamp_from_sidecar`, it classifies valid non-object JSON as
        ``UNREADABLE_SIDECAR`` instead of raising.

        Args:
            meta_path: The sidecar path (``<body stem>.meta.json``).

        Returns:
            A :class:`SidecarRead` whose ``reason`` is ``None`` iff the file
            vouched.
        """
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            # A missing sidecar (an orphan body) and an unreadable one need
            # different repairs, so the aggregate must be able to tell them apart.
            file_level_reason = (
                BronzeVouchReason.NO_SIDECAR
                if isinstance(exc, FileNotFoundError)
                else BronzeVouchReason.UNREADABLE_SIDECAR
            )
            return SidecarRead(
                timestamp=None,
                reason=file_level_reason,
                diagnostics=(
                    SidecarDiagnostic(key=None, reason=file_level_reason, detail=str(exc)),
                ),
            )

        if not isinstance(meta, dict):
            return SidecarRead(
                timestamp=None,
                reason=BronzeVouchReason.UNREADABLE_SIDECAR,
                non_object_json=True,
                payload=meta,
            )

        diagnostics: list[SidecarDiagnostic] = []
        reason = BronzeVouchReason.NO_TIMESTAMP_KEY
        for key in _SIDECAR_TIMESTAMP_KEYS:
            raw_value = meta.get(key)
            if not raw_value:
                continue
            timestamp, loggable_failure = BaseSilverTransformer._coerce_sidecar_timestamp(raw_value)
            if timestamp is not None:
                return SidecarRead(timestamp=timestamp, reason=None, diagnostics=tuple(diagnostics))
            # Fall through: a later key can still vouch.
            reason = BronzeVouchReason.UNPARSEABLE_TIMESTAMP
            diagnostics.append(
                SidecarDiagnostic(
                    key=key,
                    reason=BronzeVouchReason.UNPARSEABLE_TIMESTAMP,
                    detail=str(raw_value) if loggable_failure else None,
                )
            )
        return SidecarRead(timestamp=None, reason=reason, diagnostics=tuple(diagnostics))

    @staticmethod
    def _timestamp_from_sidecar(meta_path: Path) -> datetime | None:
        """Return a bronze sidecar's timestamp, logging each parse failure.

        A logging wrapper over :meth:`_read_sidecar_timestamp` for the
        per-file (``VINTAGE_PER_BRONZE_FILE``) branches; the frame-level stamps
        use the classifier directly. On valid JSON that is not an object it
        raises ``AttributeError`` instead of returning ``None``.
        """
        read = BaseSilverTransformer._read_sidecar_timestamp(meta_path)
        BaseSilverTransformer._log_sidecar_parse_warnings(meta_path, read)
        if read.non_object_json:
            # `.get` on the non-dict payload raises the AttributeError.
            payload: Any = read.payload
            payload.get(_SIDECAR_TIMESTAMP_KEYS[0])
        return read.timestamp

    @staticmethod
    def _log_sidecar_parse_warnings(meta_path: Path, read: SidecarRead) -> None:
        """Log the parse warnings a classified sidecar read carries."""
        for diagnostic in read.diagnostics:
            if diagnostic.detail is None:
                # A value that is neither a string nor a datetime fails silently.
                continue
            if diagnostic.key is None:
                logger.warning(
                    "Failed to parse bronze sidecar %s: %s", meta_path, diagnostic.detail
                )
            else:
                logger.warning("Could not parse bronze sidecar timestamp: %s", diagnostic.detail)

    @staticmethod
    def _coerce_sidecar_timestamp(raw_value: object) -> tuple[datetime | None, bool]:
        """Coerce one sidecar value to a UTC datetime, without logging.

        Args:
            raw_value: The sidecar value to coerce.

        Returns:
            ``(timestamp, loggable_failure)``. ``loggable_failure`` is ``True``
            only for a string that :meth:`datetime.fromisoformat` rejects; any
            other non-datetime value fails silently with ``(None, False)``. A
            naive value is taken as UTC.
        """
        if isinstance(raw_value, datetime):
            value = raw_value
        elif isinstance(raw_value, str):
            normalized = raw_value.replace("Z", "+00:00")
            try:
                value = datetime.fromisoformat(normalized)
            except ValueError:
                return None, True
        else:
            return None, False

        if value.tzinfo is None:
            return value.replace(tzinfo=UTC), False
        return value.astimezone(UTC), False

    def _write_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        available_at: datetime,
    ) -> None:
        """Write DataFrame to partitioned Parquet.

        By default one file per ``(dataset, target_date)``, atomically
        replaced on each run. With ``APPEND_ONLY`` the filename carries the
        ``available_at`` argument (ADR-018), so a reingest with the same
        sidecar stamp replaces its own file while distinct live runs coexist.
        That argument is the file's capture stamp; the rows' ``available_at``
        column may carry a different, vendor-derived instant.
        """
        out_dir = PathBuilder(self.data_dir).silver_partition_dir(
            self.source,
            self.dataset,
            target_date,
            dataset_dir=self.silver_dir,
        )
        if self.APPEND_ONLY:
            run_stamp = append_only_run_stamp(available_at)
            filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}_run{run_stamp}.parquet"
            final_path = out_dir / filename
        else:
            final_path = self._silver_destination(target_date)
        write_parquet(df, final_path)

    def _silver_destination(self, target_date: date) -> Path:
        """Return the one non-append-only destination, honoring ``silver_dir``."""
        return PathBuilder(self.data_dir).silver_file(
            self.source,
            self.dataset,
            target_date,
            dataset_dir=self.silver_dir,
        )

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
