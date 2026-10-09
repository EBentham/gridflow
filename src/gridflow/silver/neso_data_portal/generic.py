"""The NESO generic silver engine: one transformer class per recorded family (ADR-034).

A registry family with a frozen schema record (``registry/record.py``) is
transformed here, capture by capture, into one append-only output per
(capture, family) plus one completion record (P-6, P-7). A non-legacy family
without a record is registered ingest-only and skipped loudly by transform
(P-13). The three legacy keys keep their bespoke transformers.

**Generation (P-15).** :func:`register_generated` runs at import, looks the
registry up through ``registry.load_registry`` at call time (unit A's test
seam), and installs, per family, the transformer class, its ``_latest`` spec
and its ingest-only reason. Tests install a :class:`GeneratedSet` through
``monkeypatch.setitem`` instead, so nothing leaks into registry-wide loops.

**One run, one run id.** ``run()`` resolves the run id once, exactly as the
base ``run()`` does, and every capture of the invocation is stamped with it.
The run id is lineage only (B7 excludes it): every clock and every path comes
from bronze and the record.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.captures import scan_dataset
from gridflow.connectors.neso_data_portal.registry import (
    LEGACY_KEYS,
    SilverDisposition,
)
from gridflow.connectors.neso_data_portal.registry.record import has_issue_time
from gridflow.silver import registry as silver_registry
from gridflow.silver.base import BronzeVouchReason, append_only_run_stamp
from gridflow.silver.latest_views import _SETTLEMENT_RUN_RANK, LATEST_VIEW_SPECS, LatestViewSpec
from gridflow.silver.neso_data_portal.casting import (
    ExclusionTally,
    epoch_for,
    epoch_formats,
    finish_capture,
    record_dtypes,
    type_child,
)
from gridflow.silver.neso_data_portal.completion import (
    COMPLETION_DUCKDB_COLUMNS,
    COMPLETION_RELATION,
    NesoCaptureFailedError,
    Versions,
    capture_context,
    capture_id_for,
    completion_dir,
    completion_row,
    is_valid,
    read_completion,
    record_completion,
    scan_completions,
    write_failure,
)
from gridflow.silver.neso_data_portal.readers import read_children
from gridflow.silver.owned_relations import (
    RegisteredRelationsTransformer,
    SupportRelation,
)
from gridflow.storage.parquet import write_parquet
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry import (
        FamilyEntry,
        Registry,
        ResourceEntry,
    )
    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord
    from gridflow.silver.date_columns import DateColSqlType

logger = logging.getLogger(__name__)

__all__ = [
    "ENGINE_VERSION",
    "FILES_REASON",
    "INGEST_ONLY_REASON",
    "EmptyCaptureError",
    "GeneratedSet",
    "GenericNesoTransformer",
    "OutputCollisionError",
    "families_of",
    "generated_registrations",
    "latest_spec_for_record",
    "make_generic_transformer",
    "output_columns",
    "register_generated",
    "resource_of",
]

SOURCE = "neso_data_portal"

ENGINE_VERSION = "1"
"""Bumped on any output-affecting engine change; part of every output's
``dataset_version`` and every completion record, so a bump re-transforms."""

INGEST_ONLY_REASON = "ingest-only: no frozen schema record"
FILES_REASON = "non-tabular family: catalogue only"

_DUCKDB_TYPES: dict[str, str] = {
    "string": "VARCHAR",
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "date": "DATE",
    "datetime": "TIMESTAMPTZ",
}
_TIEBREAK: tuple[str, ...] = ("capture_written_at", "bronze_capture_id")


class OutputCollisionError(Exception):
    """An output path already holds another capture's rows (B5's never-collide)."""


class EmptyCaptureError(Exception):
    """A header-only body that is not a valid empty capture, or a marker on rows."""


def resource_of(capture: Capture, dir_key: str, registry: Registry) -> ResourceEntry | None:
    """Return the registry resource a capture belongs to (P-2; ADR-037 P-14).

    The capture's sidecar ``resource_id`` in the registry, else the exact
    ``(resource_name, ckan_format)`` in the directory family's package. The
    one resolution the COVERED exemption (:func:`families_of`) and the
    equivalence proof's scope share.

    Args:
        capture: A usable capture.
        dir_key: The bronze directory (family key) it was found under.
        registry: The loaded registry.

    Returns:
        The resource, or ``None`` when neither rule resolves one.
    """
    entry = registry.resources.get(capture.resource_id)
    resource = entry[1] if entry is not None else None
    if resource is None:
        meta: Any = json.loads(capture.sidecar.read_text(encoding="utf-8"))
        params = meta.get("request_params") or {}
        fmt = str(params.get("ckan_format", "")).upper()
        package, _family = registry.families[dir_key]
        resource = next(
            (r for r in package.resources if r.name == capture.resource_name and r.format == fmt),
            None,
        )
    return resource


def families_of(capture: Capture, dir_key: str, registry: Registry) -> dict[str, tuple[str, ...]]:
    """Return the families a capture feeds and, per family, its child ids (P-2).

    The capture's resource is :func:`resource_of`. Each child's ``SILVER(k)``
    when the resource has children, else the resource's own ``SILVER(k)``;
    ``DOC``, ``GIS``, ``HOLD`` and ``COVERED`` feed nothing.

    Args:
        capture: A usable capture.
        dir_key: The bronze directory (family key) it was found under.
        registry: The loaded registry.

    Returns:
        Family key -> child ids (``()`` for a plain body).
    """
    resource = resource_of(capture, dir_key, registry)
    if resource is None:
        return {}
    out: dict[str, list[str]] = {}
    if resource.children:
        for child in resource.children:
            if isinstance(child.disposition, SilverDisposition):
                out.setdefault(child.disposition.key, []).append(child.child)
    elif isinstance(resource.disposition, SilverDisposition):
        out[resource.disposition.key] = []
    return {key: tuple(children) for key, children in out.items()}


class GenericNesoTransformer(RegisteredRelationsTransformer):
    """Base of every generated NESO transformer (P-6). Subclassed per family.

    ``read_bronze`` and ``transform`` are not used: the engine reads, types and
    writes capture by capture in :meth:`run_captures` and never calls
    ``_process_frame``.
    """

    source = SOURCE
    schema_cls = None
    APPEND_ONLY: ClassVar[bool] = True
    PARTITION_DATE_COLUMN: ClassVar[str | None] = None
    RECORD: ClassVar[SchemaRecord]
    GENERATED_DATE_COLUMN: ClassVar[str]
    GENERATED_DATE_SQL_TYPE: ClassVar[DateColSqlType]

    @classmethod
    def versions(cls) -> Versions:
        """The versions this family's completion records must carry."""
        return Versions(cls.RECORD.version, ENGINE_VERSION, cls.DATASET_VERSION, generic=True)

    @classmethod
    def output_columns(cls) -> list[tuple[str, str]]:
        """See :func:`output_columns` (P-11's typed-empty base view)."""
        return output_columns(cls.RECORD)

    @classmethod
    def support_relations(cls, data_dir: Path) -> tuple[SupportRelation, ...]:
        """The completion relation every ``_latest`` of the engine may read (P-11)."""
        return (
            SupportRelation(
                COMPLETION_RELATION, completion_dir(data_dir), COMPLETION_DUCKDB_COLUMNS
            ),
        )

    @classmethod
    def completions(cls, data_dir: Path) -> pl.LazyFrame:
        """Every completion record under ``data_dir`` (the quality CLI's input)."""
        return scan_completions(data_dir)

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Not used (ADR-034 P-6): the engine reads per capture in ``run_captures``."""
        raise NotImplementedError(
            f"{type(self).__name__}: the generic engine reads capture by capture "
            "(ADR-034 P-6); call run() or run_captures()"
        )

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Not used (ADR-034 P-6): typing is ``casting.type_child`` per child."""
        raise NotImplementedError(
            f"{type(self).__name__}: the generic engine types per child (ADR-034 P-4); "
            "call run() or run_captures()"
        )

    def run(
        self,
        target_date: date,
        run_id: str | None = None,
        reingest: bool = False,
    ) -> int:
        """Transform every not-yet-valid capture of ``target_date``'s bronze partition.

        Args:
            target_date: The bronze date directory.
            run_id: The lineage run id; resolved once, as the base ``run()`` does.
            reingest: Unused: every clock already comes from the sidecar.

        Returns:
            Rows written.

        Raises:
            NesoCaptureFailedError: Any capture failed (after every other
                capture of the date was written).
        """
        resolved_run_id = run_id or f"adhoc-{datetime.now(UTC).isoformat()}"
        return self.run_captures(target_date, None, resolved_run_id)

    def run_captures(self, target_date: date, only: frozenset[str] | None, run_id: str) -> int:
        """Transform the date's captures (or only those in ``only``), one at a time.

        Args:
            target_date: The bronze date directory.
            only: Capture ids to restrict to (the drain), or ``None``.
            run_id: The one run id every output of this call carries.

        Returns:
            Rows written.

        Raises:
            NesoCaptureFailedError: Any capture failed.
        """
        self._reset_run_counters()
        if self.write_silver_csv:
            logger.warning(
                "%s/%s: the generic engine writes no silver CSV (ADR-034: memory bound); "
                "use export_csv",
                SOURCE,
                self.dataset,
            )
        registry = registry_module.load_registry()
        _package, family = registry.families[self.dataset]
        record = self.RECORD
        paths = PathBuilder(self.data_dir)
        candidates: list[tuple[Capture, str, tuple[str, ...]]] = []
        unvouched: list[tuple[Path, BronzeVouchReason]] = []
        failures: list[tuple[str, str, str]] = []
        released_rows = 0
        try:
            for dir_key in (self.dataset, *record.siblings):
                scan = scan_dataset(
                    paths.bronze_dir(SOURCE, dir_key),
                    registry,
                    partition=target_date,
                    require_provenance=False,
                )
                if dir_key == self.dataset:
                    unvouched.extend(
                        (item.sidecar, BronzeVouchReason.UNUSABLE_PROVENANCE)
                        for item in scan.unusable
                    )
                    unvouched.extend(
                        (orphan, BronzeVouchReason.NO_SIDECAR) for orphan in scan.orphans
                    )
                for capture in scan.captures:
                    children = families_of(capture, dir_key, registry).get(self.dataset)
                    if children is None:
                        continue
                    if only is not None and capture_id_for(capture.body, self.data_dir) not in only:
                        continue
                    candidates.append((capture, dir_key, children))

            versions = self.versions()
            for capture, dir_key, children in candidates:
                capture_id = capture_id_for(capture.body, self.data_dir)
                existing = read_completion(self.data_dir, self.dataset, capture_id)
                if existing is not None and is_valid(existing, self.data_dir, versions):
                    continue
                try:
                    released_rows += self._run_capture(
                        capture, dir_key, children, target_date, run_id, family, versions
                    )
                except Exception as exc:  # noqa: BLE001 - one capture never stops the next
                    from gridflow.pipeline.runner import describe_exception

                    write_failure(self.data_dir, self.dataset, capture_id, target_date, exc)
                    failures.append((capture_id, type(exc).__name__, describe_exception(exc)))
                    logger.error(
                        "%s/%s: capture %s failed and was not written: %s: %s",
                        SOURCE,
                        self.dataset,
                        capture_id,
                        type(exc).__name__,
                        describe_exception(exc),
                    )
        finally:
            self.last_unvouched_bronze = frozenset(unvouched)
            # Every body on the date was unvouched: a vouched capture, whether
            # transformed now or already complete, is a body that was not.
            self.last_unvouched_total_exclusion = bool(unvouched) and not candidates
            self.last_total_unaccounted_exclusion = False
        if not candidates and not unvouched:
            logger.warning(f"No bronze data for {SOURCE}/{self.dataset} on {target_date}")
        if failures:
            raise NesoCaptureFailedError(self.dataset, target_date, failures)
        logger.info(
            "Silver write: %s/%s %s -> %d rows", SOURCE, self.dataset, target_date, released_rows
        )
        return released_rows

    def output_path(self, target_date: date, capture_id: str, written_at: datetime) -> Path:
        """The one output path of (capture, family) (P-6)."""
        directory = PathBuilder(self.data_dir).silver_partition_dir(
            SOURCE, self.dataset, target_date, dataset_dir=self.silver_dir
        )
        digest = hashlib.sha256(capture_id.encode("utf-8")).hexdigest()[:32]
        stamp = append_only_run_stamp(written_at)
        return (
            directory
            / f"{self.dataset}_{target_date.strftime('%Y%m%d')}_run{stamp}_{digest}.parquet"
        )

    def _run_capture(
        self,
        capture: Capture,
        dir_key: str,
        children: tuple[str, ...],
        target_date: date,
        run_id: str,
        family: FamilyEntry,
        versions: Versions,
    ) -> int:
        record = self.RECORD
        ctx = capture_context(capture, record, self.data_dir)
        tables = list(read_children(ctx.body, record, children))
        common: dict[str, Any] = {
            "family": self.dataset,
            "capture_id": ctx.capture_id,
            "source_key": dir_key,
            "partition_date": ctx.partition_date,
            "resource_id": ctx.resource_id,
            "body_sha256": ctx.body_sha256,
            "capture_written_at": ctx.capture_written_at,
            "children": list(children),
            "versions": versions,
        }
        if all(table.frame.height == 0 for table in tables):
            if not ctx.empty_capture:
                raise EmptyCaptureError(
                    f"{ctx.capture_id}: a header-only body without the empty_capture marker"
                )
            if not family.empty_allowed:
                raise EmptyCaptureError(
                    f"{ctx.capture_id}: header-only, but {self.dataset} does not allow empty"
                )
            for table in tables:
                epoch_formats(epoch_for(record, table.header), ctx.resource_filename)
            record_completion(
                self.data_dir,
                completion_row(
                    **common,
                    published_at=ctx.published_at,
                    outcome="valid_empty",
                    row_count=0,
                    rows_excluded=0,
                    output_path=None,
                ),
            )
            return 0

        typed = [type_child(table, record, ctx) for table in tables]
        del tables
        tally = ExclusionTally()
        for child in typed:
            tally.merge(child.tally)
        frame = finish_capture([child.frame for child in typed], tally, record, ctx, self.dataset)
        del typed
        if ctx.empty_capture:
            raise EmptyCaptureError(
                f"{ctx.capture_id}: the empty_capture marker is set on a body with rows"
            )
        frame = self._add_bitemporal_columns(
            frame,
            target_date=ctx.partition_date,
            run_id=run_id,
            available_at=ctx.capture_written_at,
        )
        path = self.output_path(target_date, ctx.capture_id, ctx.capture_written_at)
        _guard_collision(path, ctx.capture_id)
        write_parquet(frame, path)
        rows = frame.height
        published = ctx.published_at
        if record.vintage == "issue_time_evidenced":
            published = frame.get_column("published_at").max()  # type: ignore[assignment]
        del frame
        record_completion(
            self.data_dir,
            completion_row(
                **common,
                published_at=published,
                outcome="populated",
                row_count=rows,
                rows_excluded=tally.total,
                output_path=path.relative_to(self.data_dir).as_posix(),
            ),
        )
        self.last_excluded_row_count += tally.total
        return rows


def _guard_collision(path: Path, capture_id: str) -> None:
    """Raise unless ``path`` is absent or holds exactly ``capture_id``'s rows."""
    if not path.exists():
        return
    try:
        ids = set(
            pl.scan_parquet(path, hive_partitioning=False)
            .select(pl.col("bronze_capture_id").unique())
            .collect()
            .to_series()
        )
    except (OSError, pl.exceptions.PolarsError) as exc:
        raise OutputCollisionError(f"{path} exists and cannot be read ({exc})") from exc
    if ids != {capture_id}:
        raise OutputCollisionError(
            f"{path} already holds rows of {sorted(ids)}; refusing to replace it with {capture_id}"
        )


def _date_column(record: SchemaRecord) -> tuple[str, DateColSqlType]:
    if record.temporal.kind in ("sp_pair", "date_sp1", "month"):
        assert record.temporal.date_column is not None
        return record.temporal.date_column, "DATE"
    return "timestamp_utc", "TIMESTAMPTZ"


def make_generic_transformer(key: str, record: SchemaRecord) -> type[GenericNesoTransformer]:
    """Build the transformer class of one recorded family (P-6).

    Args:
        key: The family key (the silver dataset name).
        record: Its frozen schema record.

    Returns:
        A ``GenericNesoTransformer`` subclass bound to ``key`` and ``record``.
    """
    date_column, date_type = _date_column(record)
    attributes: dict[str, Any] = {
        "dataset": key,
        "RECORD": record,
        "ENTITY_KEY_COLUMNS": tuple(record.entity_key),
        "DATASET_VERSION": f"{record.version}+e{ENGINE_VERSION}",
        "GENERATED_DATE_COLUMN": date_column,
        "GENERATED_DATE_SQL_TYPE": date_type,
        "__doc__": f"Generated NESO transformer for {key} (record {record.version}).",
    }
    name = "Generic_" + "".join(part.title() for part in key.split("_"))
    return type(name, (GenericNesoTransformer,), attributes)


def latest_spec_for_record(record: SchemaRecord, key: str) -> LatestViewSpec:
    """The generated ``_latest`` spec of one recorded family (P-10).

    ``key_columns`` is the entity key minus ``issue_time`` and the run-type
    column; ordering is ``issue_time`` (when declared), then ``available_at``;
    the run type is ranked; ``capture_written_at`` and ``bronze_capture_id``
    (plus the run-type column, only when set) break every remaining tie. A
    ``whole_capture`` record's ``latest_partition`` selects per resource
    (ADR-039).
    """
    excluded = {"issue_time", record.run_type_column}
    key_columns = tuple(c for c in record.entity_key if c not in excluded)
    order = (("issue_time",) if has_issue_time(record) else ()) + ("available_at",)
    run_type = record.run_type_column
    tiebreak = _TIEBREAK + ((run_type,) if run_type is not None else ())
    whole = record.latest == "whole_capture"
    return LatestViewSpec(
        key_columns=key_columns,
        order_columns=order,
        rank_column=run_type,
        rank_map=_SETTLEMENT_RUN_RANK if run_type is not None else None,
        tiebreak_columns=tiebreak,
        mode="whole_capture" if whole else "key_latest",
        completion_relation=COMPLETION_RELATION if whole else None,
        completion_family=key if whole else None,
        completion_partition=record.latest_partition if whole else None,
    )


def output_columns(record: SchemaRecord) -> list[tuple[str, str]]:
    """The ordered ``(name, DuckDB type)`` list of one written output (P-11).

    ``record_columns`` then the bitemporal stamp, then the two Hive partition
    columns DuckDB appends.
    """
    columns = [(name, _DUCKDB_TYPES[dtype]) for name, dtype in record_dtypes(record).items()]
    columns.extend(
        [
            ("event_time", "TIMESTAMPTZ"),
            ("available_at", "TIMESTAMPTZ"),
            ("source_run_id", "VARCHAR"),
            ("dataset_version", "VARCHAR"),
            ("month", "BIGINT"),
            ("year", "BIGINT"),
        ]
    )
    return columns


@dataclass(frozen=True)
class GeneratedSet:
    """Everything generation registers for one registry (P-15).

    Attributes:
        transformers: Family key -> generated transformer class.
        specs: ``(source, key)`` -> generated ``_latest`` spec.
        ingest_only: ``(source, key)`` -> ``(reason, warn)``.
    """

    transformers: dict[str, type[GenericNesoTransformer]] = field(default_factory=dict)
    specs: dict[tuple[str, str], LatestViewSpec] = field(default_factory=dict)
    ingest_only: dict[tuple[str, str], tuple[str, bool]] = field(default_factory=dict)


def generated_registrations(registry: Registry) -> GeneratedSet:
    """Derive the generated set of ``registry`` (pure; registers nothing).

    Args:
        registry: The loaded registry.

    Returns:
        The transformer classes and specs of every recorded family, and the
        ingest-only reason of every other non-legacy family.
    """
    generated = GeneratedSet()
    for key in sorted(registry.families):
        if key in LEGACY_KEYS:
            continue
        _package, family = registry.families[key]
        if family.record is not None:
            generated.transformers[key] = make_generic_transformer(key, family.record)
            generated.specs[(SOURCE, key)] = latest_spec_for_record(family.record, key)
        elif family.kind == "files":
            generated.ingest_only[(SOURCE, key)] = (FILES_REASON, False)
        else:
            generated.ingest_only[(SOURCE, key)] = (INGEST_ONLY_REASON, True)
    return generated


def register_generated(registry: Registry | None = None) -> GeneratedSet:
    """Register the generated set into the silver registries (runs at import).

    Args:
        registry: The registry to generate from; ``None`` looks up
            ``registry.load_registry()`` at call time.

    Returns:
        What was registered.
    """
    generated = generated_registrations(registry or registry_module.load_registry())
    for key, cls in generated.transformers.items():
        silver_registry.register_transformer(SOURCE, key, cls)
    LATEST_VIEW_SPECS.update(generated.specs)
    for (source, key), (reason, warn) in generated.ingest_only.items():
        silver_registry.register_ingest_only(source, key, reason, warn=warn)
    return generated


register_generated()
