"""Vault snapshot materializer for the NESO Open Data Portal catalogue (D-32).

Implements PHASE.md ruling 4 verbatim:

* ``snapshots/<UTC-snapshot-id>/`` is **immutable once complete**. It carries
  ``catalog-snapshot.json``, ``provenance.json`` and ``sha256sums.txt``.
* ``sha256sums.txt`` is written **last** and is therefore the completeness
  marker: a directory without it is incomplete *by definition* (FM-06). An
  incomplete directory is never deleted and never rewritten — it is left in
  place and named in the next run's log (ADR-029 spirit).
* ``provenance.json`` is populated **only** from the ``CatalogDiscovery``
  request traces (D-17). No field is synthesised, defaulted or blanked: a
  missing trace field raises :class:`IncompleteProvenanceError` rather than
  producing a hash-verified record of nothing (FM-14).
* ``catalog-manifest.json`` names the active snapshot by id and by the hash of
  its ``sha256sums.txt``. It is advanced **only after** every file is re-hashed
  and verified (FM-09), through a temp file plus :func:`os.replace` (FM-08)
  with a bounded retry for the vault's OneDrive sync locks (FM-10). A partial
  or failed refresh never advances it, and never advances it *silently*.
* Metadata evidence only. Dataset row samples may not be committed to the
  vault, so a payload carrying CKAN datastore ``records`` is rejected outright
  rather than filtered — a filter would make the guard's own failure invisible.

No user path is hardcoded: the output root is an explicit ``--out``, defaulting
to ``$GRIDFLOW_VAULT_DIR/30-vendors/neso-data-portal/_generated`` and required
when that variable is unset.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from gridflow.config.settings import load_settings
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from types import TracebackType

    from gridflow.connectors.neso_data_portal.client import CatalogDiscovery

__all__ = [
    "CATALOG_FILENAME",
    "CHECKSUMS_FILENAME",
    "MANIFEST_FILENAME",
    "PROVENANCE_FILENAME",
    "SNAPSHOTS_DIRNAME",
    "SNAPSHOT_ID_FORMAT",
    "CatalogDiscoverer",
    "IncompleteProvenanceError",
    "ManifestAdvanceError",
    "RowSampleRejectedError",
    "SnapshotError",
    "SnapshotVerificationError",
    "advance_manifest",
    "build_snapshot",
    "main",
    "verify_snapshot",
]

logger = logging.getLogger(__name__)

SNAPSHOT_ID_FORMAT = "%Y%m%dT%H%M%SZ"
"""UTC and lexicographically sortable, so ``sorted()`` is chronological order."""

SNAPSHOTS_DIRNAME = "snapshots"
CATALOG_FILENAME = "catalog-snapshot.json"
PROVENANCE_FILENAME = "provenance.json"
CHECKSUMS_FILENAME = "sha256sums.txt"
MANIFEST_FILENAME = "catalog-manifest.json"

SOURCE_NAME = "neso_data_portal"

VAULT_DIR_ENV = "GRIDFLOW_VAULT_DIR"
DEFAULT_OUT_RELATIVE = ("30-vendors", "neso-data-portal", "_generated")

REPLACE_ATTEMPTS = 3
"""FM-10: three attempts, then a loud failure — never a silent stale manifest."""

REPLACE_BACKOFF_SECONDS = 0.5

ROW_SAMPLE_KEYS = frozenset({"records"})
"""CKAN's datastore row key, and deliberately only that one.

``records`` is the key ``datastore_search`` returns rows under. It never
appears in ``package_search``/``package_show`` metadata, so its presence means
row data reached a payload bound for the vault, which PHASE.md ruling 4
forbids. The set is deliberately not padded with guesses: an invented key list
would refuse legitimate metadata on a vendor field rename, and this project
does not invent vendor semantics.
"""

_HEX64 = re.compile(r"[0-9a-f]{64}")
_MISSING = object()


class SnapshotError(Exception):
    """Base class for every snapshot-materializer failure."""


class IncompleteProvenanceError(SnapshotError):
    """A request trace lacked a field ``provenance.json`` requires (FM-14)."""


class RowSampleRejectedError(SnapshotError):
    """A payload carried dataset rows, which may not be written to the vault."""


class SnapshotVerificationError(SnapshotError):
    """A snapshot directory does not match its own ``sha256sums.txt`` (FM-09)."""


class ManifestAdvanceError(SnapshotError):
    """``catalog-manifest.json`` could not be advanced (FM-08, FM-10)."""


class CatalogDiscoverer(Protocol):
    """Anything that can produce a reconciled catalogue with its traces.

    Structural, so the materializer depends on the *result* of D-17 rather than
    on the connector class — which is what lets the crash-state tests run fully
    offline against a stub.
    """

    async def discover_catalog(self) -> CatalogDiscovery:
        """Return the reconciled catalogue and one trace per HTTP call."""
        ...


class ConnectorSession(Protocol):
    """An async context manager yielding a :class:`CatalogDiscoverer`."""

    async def __aenter__(self) -> CatalogDiscoverer:
        """Open the session."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the session."""
        ...


# ---------------------------------------------------------------------------
# Time and identity
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Return the current tz-aware UTC time.

    A named seam rather than an inline ``datetime.now`` call: two snapshots
    built inside the same second would otherwise collide on their id, which the
    immutability tests need to be able to avoid deterministically.
    """
    return datetime.now(UTC)


def _snapshot_id(moment: datetime) -> str:
    """Format a UTC instant as a lexicographically sortable snapshot id."""
    return moment.astimezone(UTC).strftime(SNAPSHOT_ID_FORMAT)


def _iso_utc(moment: datetime) -> str:
    """Render a tz-aware instant as a ``Z``-suffixed UTC ISO-8601 string."""
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Provenance — every field sourced from the trace, none defaulted (FM-14)
# ---------------------------------------------------------------------------


def _trace_field(index: int, trace: object, name: str) -> Any:
    """Read one trace attribute, refusing to substitute a blank for an absence."""
    value = getattr(trace, name, _MISSING)
    if value is _MISSING:
        raise IncompleteProvenanceError(
            f"request trace {index} carries no {name!r}. provenance.json has no other "
            "source for it (D-17), so the snapshot is not written rather than written "
            "with a placeholder in a hash-verified evidence file"
        )
    return value


def _trace_utc(index: int, trace: object, name: str) -> datetime:
    """Read one trace timestamp, requiring it to be tz-aware UTC."""
    value = _trace_field(index, trace, name)
    if not isinstance(value, datetime):
        raise IncompleteProvenanceError(
            f"request trace {index} field {name!r} is {type(value).__name__}, not a datetime"
        )
    offset = value.utcoffset()
    if offset is None:
        raise IncompleteProvenanceError(
            f"request trace {index} field {name!r} is naive; provenance timings are UTC"
        )
    if offset.total_seconds() != 0:
        raise IncompleteProvenanceError(
            f"request trace {index} field {name!r} carries offset {offset}, not UTC"
        )
    return value


def _trace_str_mapping(index: int, trace: object, name: str) -> dict[str, str]:
    """Read one trace string-to-string mapping (params, headers)."""
    value = _trace_field(index, trace, name)
    if not isinstance(value, dict):
        raise IncompleteProvenanceError(
            f"request trace {index} field {name!r} is {type(value).__name__}, not a mapping"
        )
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise IncompleteProvenanceError(
                f"request trace {index} field {name!r} carries a non-string entry {key!r}"
            )
    return {str(key): str(item) for key, item in sorted(value.items())}


def _provenance_entry(index: int, trace: object) -> dict[str, Any]:
    """Build one ``provenance.json`` request record from one request trace.

    Args:
        index: Position of the trace in the discovery result, used in errors.
        trace: A :class:`~gridflow.connectors.neso_data_portal.client.RequestTrace`.

    Returns:
        The JSON-ready record: normalized params, both timings, status, the
        recorded response headers and the body hash.

    Raises:
        IncompleteProvenanceError: Any required field is absent or is not the
            kind of value provenance requires (FM-14).
    """
    action = _trace_field(index, trace, "action")
    if not isinstance(action, str) or not action:
        raise IncompleteProvenanceError(f"request trace {index} has an empty 'action'")

    params = _trace_str_mapping(index, trace, "params")
    headers = _trace_str_mapping(index, trace, "headers")

    started_at = _trace_utc(index, trace, "started_at")
    finished_at = _trace_utc(index, trace, "finished_at")
    if finished_at < started_at:
        raise IncompleteProvenanceError(
            f"request trace {index} finished at {finished_at.isoformat()} before it started "
            f"at {started_at.isoformat()}"
        )

    status_code = _trace_field(index, trace, "status_code")
    if isinstance(status_code, bool) or not isinstance(status_code, int):
        raise IncompleteProvenanceError(
            f"request trace {index} field 'status_code' is not an integer status"
        )

    body_sha256 = _trace_field(index, trace, "body_sha256")
    if not isinstance(body_sha256, str) or _HEX64.fullmatch(body_sha256) is None:
        raise IncompleteProvenanceError(
            f"request trace {index} field 'body_sha256' is not a lowercase sha256 hex digest"
        )

    return {
        "action": action,
        "params": params,
        "started_at": _iso_utc(started_at),
        "finished_at": _iso_utc(finished_at),
        "status_code": status_code,
        "headers": headers,
        "body_sha256": body_sha256,
    }


def _provenance_document(snapshot_id: str, traces: Iterable[object]) -> dict[str, Any]:
    """Build the whole ``provenance.json`` payload from the discovery traces."""
    requests = [_provenance_entry(index, trace) for index, trace in enumerate(traces)]
    if not requests:
        raise IncompleteProvenanceError(
            "the discovery result carries no request traces at all; a snapshot without "
            "provenance is exactly what FM-14 exists to prevent"
        )
    return {"snapshot_id": snapshot_id, "source": SOURCE_NAME, "requests": requests}


# ---------------------------------------------------------------------------
# The metadata-only guard
# ---------------------------------------------------------------------------


def _reject_row_samples(payload: object, path: str = "packages") -> None:
    """Refuse any payload carrying CKAN datastore rows.

    Args:
        payload: The catalogue payload about to be written.
        path: Dotted position of ``payload`` within the document, for the error.

    Raises:
        RowSampleRejectedError: A :data:`ROW_SAMPLE_KEYS` key was found.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str) and key.lower() in ROW_SAMPLE_KEYS:
                raise RowSampleRejectedError(
                    f"catalogue payload at {path}.{key} carries dataset rows. The vault "
                    "snapshot holds metadata evidence only (PHASE.md ruling 4); the payload "
                    "is refused rather than stripped, so the guard cannot fail in silence"
                )
            _reject_row_samples(value, f"{path}.{key}")
    elif isinstance(payload, (list, tuple)):
        for position, value in enumerate(payload):
            _reject_row_samples(value, f"{path}[{position}]")


# ---------------------------------------------------------------------------
# Writing — sha256sums.txt last, as the completeness marker (FM-06)
# ---------------------------------------------------------------------------


def _write_json(path: Path, document: dict[str, Any]) -> None:
    """Write one JSON document as exact UTF-8 bytes with a trailing newline.

    Bytes rather than text mode on purpose: the file is about to be hashed, and
    a platform default encoding or line ending would make the digest depend on
    the machine that produced it.
    """
    body = json.dumps(document, indent=2, ensure_ascii=False, sort_keys=False)
    path.write_bytes(body.encode("utf-8") + b"\n")


def _sha256_file(path: Path) -> str:
    """Return the sha256 hex digest of a file's exact bytes."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _snapshot_members(directory: Path) -> list[Path]:
    """List the hashable members of a snapshot directory, excluding the sums file."""
    return sorted(
        (entry for entry in directory.iterdir() if entry.name != CHECKSUMS_FILENAME),
        key=lambda entry: entry.name,
    )


def _write_checksums(directory: Path) -> Path:
    """Hash every file in the directory and write ``sha256sums.txt`` **last**.

    Args:
        directory: The snapshot directory, already carrying its data files.

    Returns:
        The path of the written checksum file.

    Raises:
        SnapshotVerificationError: The directory holds a sub-directory, which a
            flat snapshot never does and which no checksum line could describe.
    """
    lines = []
    for entry in _snapshot_members(directory):
        if not entry.is_file():
            raise SnapshotVerificationError(
                f"snapshot {directory.name} contains {entry.name!r}, which is not a file; "
                "a snapshot directory is flat so that every member is hashable"
            )
        lines.append(f"{_sha256_file(entry)}  {entry.name}\n")
    checksums = directory / CHECKSUMS_FILENAME
    checksums.write_bytes("".join(lines).encode("utf-8"))
    return checksums


def _log_incomplete_snapshots(snapshots_root: Path) -> None:
    """Name every incomplete snapshot directory, and leave every one in place."""
    if not snapshots_root.is_dir():
        return
    incomplete = sorted(
        entry.name
        for entry in snapshots_root.iterdir()
        if entry.is_dir() and not (entry / CHECKSUMS_FILENAME).exists()
    )
    if incomplete:
        logger.warning(
            "NESO catalogue snapshots without %s (incomplete, never advanced to, and "
            "deliberately not deleted): %s",
            CHECKSUMS_FILENAME,
            ", ".join(incomplete),
        )


def _catalog_document(snapshot_id: str, packages: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Build the ``catalog-snapshot.json`` payload from the discovered packages."""
    return {
        "snapshot_id": snapshot_id,
        "source": SOURCE_NAME,
        "package_count": len(packages),
        "packages": list(packages),
    }


async def build_snapshot(
    connector: CatalogDiscoverer, out_root: Path, *, dry_run: bool = False
) -> Path:
    """Discover the catalogue and materialize one immutable snapshot directory.

    The write order is the contract: the data files first, then
    ``sha256sums.txt``, so an interruption anywhere before the last write
    leaves a directory that is incomplete by definition and that the manifest
    was never advanced to (FM-06).

    Args:
        connector: An open connector, or any :class:`CatalogDiscoverer`.
        out_root: The ``_generated`` root holding ``snapshots/`` and the
            manifest. Nothing is written outside it.
        dry_run: Discover, guard and validate, but write nothing at all.

    Returns:
        The snapshot directory — the one written, or the one a real run would
        have written when ``dry_run`` is set.

    Raises:
        RowSampleRejectedError: The payload carried dataset rows.
        IncompleteProvenanceError: A trace field required by provenance was absent.
        FileExistsError: The snapshot id already exists. Existing snapshots are
            never reopened, rewritten or bumped past — immutability by
            construction, not by convention.
    """
    discovery = await connector.discover_catalog()

    snapshot_id = _snapshot_id(_utcnow())
    snapshots_root = out_root / SNAPSHOTS_DIRNAME
    snapshot_dir = snapshots_root / snapshot_id

    _reject_row_samples(discovery.packages)
    catalog = _catalog_document(snapshot_id, discovery.packages)
    provenance = _provenance_document(snapshot_id, discovery.traces)

    if dry_run:
        logger.info(
            "dry run: would write %s with %d packages and %d request traces; no file written",
            snapshot_dir,
            len(discovery.packages),
            len(discovery.traces),
        )
        return snapshot_dir

    _log_incomplete_snapshots(snapshots_root)
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    _write_json(snapshot_dir / CATALOG_FILENAME, catalog)
    _write_json(snapshot_dir / PROVENANCE_FILENAME, provenance)
    _write_checksums(snapshot_dir)
    logger.info(
        "wrote NESO catalogue snapshot %s (%d packages)", snapshot_id, len(discovery.packages)
    )
    return snapshot_dir


# ---------------------------------------------------------------------------
# Verification and the atomic manifest advance
# ---------------------------------------------------------------------------


def _read_checksums(checksums: Path) -> dict[str, str]:
    """Parse ``sha256sums.txt`` into a filename-to-digest mapping."""
    recorded: dict[str, str] = {}
    for number, line in enumerate(checksums.read_bytes().decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        digest, separator, name = line.partition("  ")
        if not separator or _HEX64.fullmatch(digest) is None or not name:
            raise SnapshotVerificationError(
                f"{checksums}: line {number} is not a 'sha256  filename' record"
            )
        recorded[name] = digest
    return recorded


def verify_snapshot(directory: Path) -> None:
    """Re-hash every file in a snapshot directory against ``sha256sums.txt``.

    Bidirectional on purpose (FM-09): a listed file that is absent, an on-disk
    file that is not listed, and a digest that does not match are all
    mutations of a directory the contract calls immutable, and all three name
    the offending file.

    Args:
        directory: The snapshot directory to verify.

    Raises:
        SnapshotVerificationError: The directory is incomplete, has extra or
            missing members, or a member's bytes have changed.
    """
    checksums = directory / CHECKSUMS_FILENAME
    if not checksums.is_file():
        raise SnapshotVerificationError(
            f"snapshot {directory} has no {CHECKSUMS_FILENAME}, so it is incomplete by "
            "definition (FM-06) and must not be advanced to"
        )

    recorded = _read_checksums(checksums)
    present = {entry.name for entry in _snapshot_members(directory)}

    for name in sorted(set(recorded) - present):
        raise SnapshotVerificationError(
            f"snapshot {directory}: {name!r} is listed in {CHECKSUMS_FILENAME} but is missing"
        )
    for name in sorted(present - set(recorded)):
        raise SnapshotVerificationError(
            f"snapshot {directory}: {name!r} is present but not listed in "
            f"{CHECKSUMS_FILENAME}; a completed snapshot is immutable"
        )
    for name, digest in sorted(recorded.items()):
        actual = _sha256_file(directory / name)
        if actual != digest:
            raise SnapshotVerificationError(
                f"snapshot {directory}: {name!r} hashes to {actual}, but "
                f"{CHECKSUMS_FILENAME} records {digest}"
            )


def _replace_with_retry(source: Path, destination: Path) -> None:
    """``os.replace`` with FM-10's bounded retry, then a loud failure.

    The vault lives under OneDrive, whose sync process takes transient handles
    on the file being replaced. Retrying is legitimate; giving up quietly is
    not — a stale manifest reported as advanced is the silent-data-bug class.

    Raises:
        ManifestAdvanceError: Every attempt failed. The destination is
            untouched and the temp file is removed.
    """
    last_error: OSError | None = None
    for attempt in range(1, REPLACE_ATTEMPTS + 1):
        try:
            os.replace(source, destination)
        except OSError as error:
            last_error = error
            logger.warning(
                "os.replace onto %s failed on attempt %d/%d: %s",
                destination,
                attempt,
                REPLACE_ATTEMPTS,
                error,
            )
            if attempt < REPLACE_ATTEMPTS:
                time.sleep(REPLACE_BACKOFF_SECONDS)
        else:
            return

    source.unlink(missing_ok=True)
    raise ManifestAdvanceError(
        f"could not replace {destination} after {REPLACE_ATTEMPTS} attempts; the manifest "
        f"still points at its previous snapshot: {last_error}"
    ) from last_error


def advance_manifest(out_root: Path, snapshot_id: str) -> None:
    """Point ``catalog-manifest.json`` at a snapshot, after verifying it.

    Verification comes first and without exception: PHASE.md ruling 4 says a
    partial or failed refresh never advances the manifest, so the only path to
    a manifest write runs through :func:`verify_snapshot`. The write itself is
    temp file plus :func:`os.replace`, on the same directory as the manifest so
    the replace stays atomic, which makes the manifest wholly old or wholly new
    and never truncated (FM-08).

    Args:
        out_root: The ``_generated`` root.
        snapshot_id: The snapshot directory name to advance to.

    Raises:
        SnapshotVerificationError: The snapshot is incomplete or has drifted.
        ManifestAdvanceError: The replace failed on every attempt (FM-10).
    """
    snapshot_dir = out_root / SNAPSHOTS_DIRNAME / snapshot_id
    verify_snapshot(snapshot_dir)

    manifest = out_root / MANIFEST_FILENAME
    document = {
        "snapshot_id": snapshot_id,
        "sha256sums_sha256": _sha256_file(snapshot_dir / CHECKSUMS_FILENAME),
    }

    out_root.mkdir(parents=True, exist_ok=True)
    temporary = out_root / f"{MANIFEST_FILENAME}.{snapshot_id}.tmp"
    _write_json(temporary, document)
    _replace_with_retry(temporary, manifest)
    logger.info("catalog-manifest.json now points at snapshot %s", snapshot_id)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def _default_out_root() -> Path | None:
    """Derive the default output root from ``GRIDFLOW_VAULT_DIR``, if it is set."""
    vault_dir = os.environ.get(VAULT_DIR_ENV)
    if not vault_dir:
        return None
    return Path(vault_dir).joinpath(*DEFAULT_OUT_RELATIVE)


def _default_connector_session() -> ConnectorSession:
    """Build the real connector from the loaded configuration."""
    return NesoDataPortalConnector(load_settings().sources[SOURCE_NAME])


def _build_parser() -> argparse.ArgumentParser:
    """Build the module's argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m gridflow.connectors.neso_data_portal.catalog_snapshot",
        description=(
            "Materialize a provenanced, hash-verified NESO catalogue snapshot and advance "
            "the manifest to it."
        ),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Output root. Defaults to "
            f"${VAULT_DIR_ENV}/{'/'.join(DEFAULT_OUT_RELATIVE)}; required when that "
            "variable is unset. No path is hardcoded."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover, guard and validate provenance, but write nothing.",
    )
    return parser


async def _run(
    session_factory: Callable[[], ConnectorSession], out_root: Path, *, dry_run: bool
) -> Path:
    """Open a connector session, build a snapshot and advance the manifest."""
    async with session_factory() as connector:
        snapshot_dir = await build_snapshot(connector, out_root, dry_run=dry_run)
    if not dry_run:
        advance_manifest(out_root, snapshot_dir.name)
    return snapshot_dir


def main(
    argv: Sequence[str] | None = None,
    *,
    session_factory: Callable[[], ConnectorSession] | None = None,
) -> int:
    """Run the materializer.

    Args:
        argv: Command-line arguments, defaulting to ``sys.argv[1:]``.
        session_factory: Seam for the connector session, so the command can be
            driven offline against a stub.

    Returns:
        ``0`` on success; ``1`` on any snapshot failure, which is named in the
        log rather than swallowed (FM-09 requires a non-zero exit).
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    out_root: Path | None = args.out if args.out is not None else _default_out_root()
    if out_root is None:
        parser.error(f"--out is required when {VAULT_DIR_ENV} is unset")

    factory = session_factory if session_factory is not None else _default_connector_session
    try:
        snapshot_dir = asyncio.run(_run(factory, Path(out_root), dry_run=args.dry_run))
    except SnapshotError as error:
        logger.error("NESO catalogue snapshot failed: %s", error)
        return 1

    if args.dry_run:
        logger.info("dry run complete; %s was not written", snapshot_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
