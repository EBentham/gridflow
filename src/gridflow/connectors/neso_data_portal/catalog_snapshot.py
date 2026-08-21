"""Vault snapshot materializer for the NESO Open Data Portal catalogue (D-32).

Implements PHASE.md ruling 4 verbatim:

* ``snapshots/<UTC-snapshot-id>/`` is **immutable once complete**. It carries
  ``catalog-snapshot.json``, ``provenance.json`` and ``sha256sums.txt``.
* ``sha256sums.txt`` is installed **last** and is therefore the completeness
  marker: a directory without it is incomplete *by definition* (FM-06). It is
  staged and installed with :func:`os.replace` like the manifest, so a torn
  marker cannot exist — everything downstream tests the marker by existence,
  and a half-written one would make an incomplete snapshot look complete. An
  incomplete directory is never deleted and never rewritten — it is left in
  place and named in the next run's log (ADR-029 spirit).
* ``provenance.json`` is populated **only** from the ``CatalogDiscovery``
  request traces (D-17). No field is synthesised, defaulted or blanked: a
  missing trace field raises :class:`IncompleteProvenanceError` rather than
  producing a hash-verified record of nothing (FM-14).
* ``catalog-manifest.json`` names the active snapshot by id and by the hash of
  its ``sha256sums.txt``. It is advanced **only after** the contract members
  are proven present and every file is re-hashed and verified (FM-09), through
  a temp file plus :func:`os.replace` (FM-08)
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
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from gridflow.config.settings import load_settings
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import TracebackType

    from gridflow.connectors.neso_data_portal.client import CatalogDiscovery

__all__ = [
    "CATALOG_DOCUMENT_KEYS",
    "CATALOG_FILENAME",
    "CHECKSUMS_FILENAME",
    "MANIFEST_FILENAME",
    "MEMBER_BUILDERS",
    "PROVENANCE_DOCUMENT_KEYS",
    "PROVENANCE_FILENAME",
    "PROVENANCE_REQUEST_KEYS",
    "REQUIRED_MEMBERS",
    "SNAPSHOTS_DIRNAME",
    "SNAPSHOT_ID_FORMAT",
    "CatalogDiscoverer",
    "IncompleteProvenanceError",
    "InvalidDocumentError",
    "ManifestAdvanceError",
    "RowSampleRejectedError",
    "SnapshotError",
    "SnapshotVerificationError",
    "SnapshotWriteError",
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
TEMP_SUFFIX = ".tmp"

CATALOG_DOCUMENT_KEYS: tuple[str, ...] = ("snapshot_id", "source", "package_count", "packages")
PROVENANCE_DOCUMENT_KEYS: tuple[str, ...] = ("snapshot_id", "source", "requests")
PROVENANCE_REQUEST_KEYS: tuple[str, ...] = (
    "action",
    "params",
    "started_at",
    "finished_at",
    "status_code",
    "headers",
    "body_sha256",
)

"""The key tuples above are the writer's construction order, nothing more.

They are **not** a description of validity that a verifier could check against.
What a valid document *is* lives in :func:`_catalog_document` and
:func:`_provenance_document`, and the verifier learns it by calling them — see
:data:`MEMBER_BUILDERS`.
"""

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


class InvalidDocumentError(SnapshotError):
    """A snapshot document is not one the writer could have produced."""


class SnapshotVerificationError(SnapshotError):
    """A snapshot directory does not match its own ``sha256sums.txt`` (FM-09)."""


class SnapshotWriteError(SnapshotError):
    """A snapshot file could not be installed, leaving the snapshot incomplete."""


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


def _require_snapshot_id(value: object) -> str:
    """Accept only an id :func:`_snapshot_id` could have produced.

    Checked by **round trip**, not by pattern: the value is parsed with
    :data:`SNAPSHOT_ID_FORMAT` and re-formatted through the same function the
    writer uses, so anything the formatter would not emit is refused. A regex
    would be a second description of the format, and a second description is
    the thing that drifts.

    Args:
        value: The candidate id — a generated one on the way out, a parsed
            document's ``snapshot_id`` or a directory name on the way in.

    Returns:
        The id, unchanged.

    Raises:
        InvalidDocumentError: The value is not a string, or is not the exact
            rendering :func:`_snapshot_id` produces for the instant it names.
    """
    if not isinstance(value, str):
        raise InvalidDocumentError(
            f"snapshot id {value!r} is a {type(value).__name__}, not a string"
        )
    try:
        moment = datetime.strptime(value, SNAPSHOT_ID_FORMAT).replace(tzinfo=UTC)
    except ValueError as error:
        raise InvalidDocumentError(
            f"snapshot id {value!r} is not a UTC {SNAPSHOT_ID_FORMAT} instant, so it is not "
            f"an id this materializer could have produced ({error})"
        ) from error
    if _snapshot_id(moment) != value:
        raise InvalidDocumentError(
            f"snapshot id {value!r} does not round-trip through the writer's own formatter "
            f"(it re-renders as {_snapshot_id(moment)!r})"
        )
    return value


# ---------------------------------------------------------------------------
# Provenance — every field sourced from the trace, none defaulted (FM-14)
# ---------------------------------------------------------------------------


def _trace_field(index: int, trace: object, name: str) -> Any:
    """Read one trace field, refusing to substitute a blank for an absence.

    Reads an attribute from a :class:`RequestTrace` on the way out, and a key
    from a parsed ``provenance.json`` request on the way in. **One accessor is
    what makes one validator possible**: the verifier feeds parsed records back
    through :func:`_provenance_entry`, so every rejection the writer performs
    is a rejection the verifier performs, without either side describing the
    other's rules.
    """
    value = trace[name] if isinstance(trace, Mapping) and name in trace else _MISSING
    if value is _MISSING and not isinstance(trace, Mapping):
        value = getattr(trace, name, _MISSING)
    if value is _MISSING:
        raise IncompleteProvenanceError(
            f"request trace {index} carries no {name!r}. provenance.json has no other "
            "source for it (D-17), so the snapshot is not written rather than written "
            "with a placeholder in a hash-verified evidence file"
        )
    return value


def _parse_iso_utc(index: int, name: str, value: str) -> datetime:
    """Parse a rendered provenance timestamp, refusing anything the writer would not emit."""
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise IncompleteProvenanceError(
            f"request trace {index} field {name!r} is {value!r}, which is not an ISO-8601 "
            f"instant ({error})"
        ) from error
    if moment.utcoffset() is None or _iso_utc(moment) != value:
        raise IncompleteProvenanceError(
            f"request trace {index} field {name!r} is {value!r}, which is not the UTC form "
            "this materializer writes"
        )
    return moment


def _trace_utc(index: int, trace: object, name: str) -> datetime:
    """Read one trace timestamp, requiring it to be tz-aware UTC.

    A string is accepted only if it is exactly what :func:`_iso_utc` emits for
    the instant it names — the same round-trip rule as the snapshot id. That is
    what lets a rendered timestamp read back in be checked by the very code
    that rendered it.
    """
    value = _trace_field(index, trace, name)
    if isinstance(value, str):
        value = _parse_iso_utc(index, name, value)
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

    return dict(
        zip(
            PROVENANCE_REQUEST_KEYS,
            (
                action,
                params,
                _iso_utc(started_at),
                _iso_utc(finished_at),
                status_code,
                headers,
                body_sha256,
            ),
            strict=True,
        )
    )


def _provenance_document(snapshot_id: object, traces: object) -> dict[str, Any]:
    """Build ``provenance.json`` from request traces — **and validate them**.

    This is the only description of a valid provenance document in the module.
    The writer calls it with ``CatalogDiscovery`` traces; the verifier calls it
    with the request records parsed back off disk. Both therefore get every
    rejection :func:`_provenance_entry` performs — an empty ``action``, a
    non-mapping ``params`` or ``headers``, an unparseable or reversed
    timestamp, a boolean ``status_code``, a malformed body hash — without the
    verifier restating a single one of them (PHASE.md ruling 11).

    Args:
        snapshot_id: The id the document must name.
        traces: Request traces, or the parsed ``requests`` list.

    Returns:
        The document the writer would write for these inputs.

    Raises:
        InvalidDocumentError: The id or the trace collection is not a shape the
            writer could have been handed.
        IncompleteProvenanceError: Any trace is incomplete or invalid (FM-14),
            including the empty collection — which the writer refuses to
            produce, and the verifier therefore refuses to accept.
    """
    identity = _require_snapshot_id(snapshot_id)
    if not isinstance(traces, (list, tuple)):
        raise InvalidDocumentError(
            f"'requests' is a {type(traces).__name__}, not a list of request records"
        )
    requests = [_provenance_entry(index, trace) for index, trace in enumerate(traces)]
    if not requests:
        raise IncompleteProvenanceError(
            "the discovery result carries no request traces at all; a snapshot without "
            "provenance is exactly what FM-14 exists to prevent"
        )
    return dict(zip(PROVENANCE_DOCUMENT_KEYS, (identity, SOURCE_NAME, requests), strict=True))


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


def _serialize_document(document: dict[str, Any]) -> bytes:
    """Render one snapshot document as the exact bytes that go on disk.

    **The only serializer in the module** — separators, indent, key order,
    encoding and trailing newline all live here once. That is what lets
    verification compare *bytes* rather than parsed values, which matters more
    than it sounds: Python equality is not type-strict, so a document carrying
    ``package_count: false`` beside zero packages, ``true`` beside one, or
    ``2.0`` beside two compares equal to the integer the writer computes and
    would be a false fixed point (Sol pass 4). ``false`` and ``0`` do not
    serialize alike, so at the byte level the question does not arise.

    Bytes rather than text mode on purpose: the file is about to be hashed, and
    a platform default encoding or line ending would make the digest depend on
    the machine that produced it.
    """
    body = json.dumps(document, indent=2, ensure_ascii=False, sort_keys=False)
    return body.encode("utf-8") + b"\n"


def _write_json(path: Path, document: dict[str, Any]) -> None:
    """Write one JSON document through the module's single serializer."""
    path.write_bytes(_serialize_document(document))


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
    """Hash every file in the directory and install ``sha256sums.txt`` **last**.

    The marker is **staged and installed with** :func:`os.replace`, not written
    in place. Everything downstream tests the marker by *existence* — the
    incomplete-snapshot log, and the FM-06 contract itself — so a torn but
    present marker would make a half-written snapshot look complete, which is
    the one state the whole write order exists to prevent. Staging makes it
    impossible by construction rather than unlikely (Sol pass 1, minor 2).

    Args:
        directory: The snapshot directory, already carrying its data files.

    Returns:
        The path of the installed checksum file.

    Raises:
        SnapshotVerificationError: The directory holds a sub-directory, which a
            flat snapshot never does and which no checksum line could describe.
        SnapshotWriteError: The marker could not be installed (FM-10's lock).
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
    staged = directory / f"{CHECKSUMS_FILENAME}{TEMP_SUFFIX}"
    staged.write_bytes("".join(lines).encode("utf-8"))
    _replace_with_retry(
        staged,
        checksums,
        error_class=SnapshotWriteError,
        consequence=(
            "the snapshot has no completeness marker and so stays incomplete, which is the "
            "correct outcome but must not be reported as a success"
        ),
    )
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


def _catalog_document(snapshot_id: object, packages: object) -> dict[str, Any]:
    """Build ``catalog-snapshot.json`` from the packages — **and validate them**.

    The only description of a valid catalogue document in the module. It owns
    the metadata-only guard, so the guard runs on the way out *and* on the way
    back in: a catalogue carrying dataset rows is refused by the verifier for
    the same reason and through the same call the writer refuses it (PHASE.md
    ruling 11). ``package_count`` is computed rather than trusted, so a
    document whose count disagrees with its own packages simply is not the
    document this function builds.

    **An empty catalogue is valid**, deliberately: ``discover_catalog`` treats
    zero packages reconciling against a declared count of zero as a legitimate
    answer, so the writer can honestly produce one and a verifier that refused
    it would fail on the real vault rather than on a bad snapshot. Provenance
    is the asymmetric case — see :func:`_provenance_document`.

    Args:
        snapshot_id: The id the document must name.
        packages: The discovered package payloads, or the parsed ``packages``.

    Returns:
        The document the writer would write for these inputs.

    Raises:
        InvalidDocumentError: The id or the package collection is not a shape
            the writer could have been handed.
        RowSampleRejectedError: A payload carried dataset rows.
    """
    identity = _require_snapshot_id(snapshot_id)
    if not isinstance(packages, (list, tuple)):
        raise InvalidDocumentError(
            f"'packages' is a {type(packages).__name__}, not a list of package payloads"
        )
    _reject_row_samples(packages)
    return dict(
        zip(
            CATALOG_DOCUMENT_KEYS,
            (identity, SOURCE_NAME, len(packages), list(packages)),
            strict=True,
        )
    )


MEMBER_BUILDERS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    CATALOG_FILENAME: lambda document: _catalog_document(
        document.get("snapshot_id"), document.get("packages")
    ),
    PROVENANCE_FILENAME: lambda document: _provenance_document(
        document.get("snapshot_id"), document.get("requests")
    ),
}
"""How the verifier re-derives each member — **through the writer's own code**.

``verify_snapshot`` parses a member, feeds its fields back through the function
that built it, and requires the result to equal what is on disk. There is no
second description of validity to drift: every rule the writer enforces is
enforced on the way back in because it is literally the same call, and any
document the writer could not have produced fails either by raising inside the
builder or by not matching what the builder returns.
"""

REQUIRED_MEMBERS: tuple[str, ...] = tuple(MEMBER_BUILDERS)
"""The members a snapshot must carry to be worth activating.

``sha256sums.txt`` proves a directory is *self-consistent*, not that it is a
snapshot: an empty marker over an empty directory verifies perfectly, so does
any self-consistent subset, and so do two zero-byte members listed with the
hash of emptiness (Sol passes 1 and 2).
"""


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


def _member_error(directory: Path, name: str, defect: str) -> SnapshotVerificationError:
    """Build the one error shape every member defect is reported through."""
    return SnapshotVerificationError(f"snapshot {directory}: {name!r} {defect}")


def _disagreeing_keys(recorded: dict[str, Any], rebuilt: dict[str, Any]) -> list[str]:
    """Name the keys on which a member disagrees with what the writer builds.

    Diagnostics only — the verdict is the byte comparison. Values are compared
    by :func:`repr` rather than by ``==`` deliberately: the defects this exists
    to describe include ``false`` where an integer belongs and ``2.0`` where a
    ``2`` belongs, and ``==`` calls both of those equal. A message that said
    "no keys disagree" about a document that failed would be worse than none.
    """
    return sorted(
        key
        for key in set(recorded) | set(rebuilt)
        if repr(recorded.get(key, _MISSING)) != repr(rebuilt.get(key, _MISSING))
    )


def _revalidate_member(directory: Path, name: str) -> None:
    """Parse one required member and re-derive it **through the writer's code**.

    Hash agreement proves the bytes have not changed since the marker was
    installed. It proves nothing about what those bytes *are*: zero-byte
    members listed with the hash of emptiness agree with their marker
    perfectly, and so does a catalogue full of dataset rows.

    So the parsed document's own fields are handed back to the function that
    built it (:data:`MEMBER_BUILDERS`), the result is re-serialized through
    :func:`_serialize_document`, and those **bytes must equal the bytes on
    disk**. Anything the writer would have rejected raises inside the builder;
    anything the writer would have written differently fails the comparison.
    **There is no second description of validity here to drift out of step**
    (PHASE.md ruling 11).

    The comparison is at the byte level rather than between parsed documents
    because Python equality is not type-strict: ``false == 0`` and ``2.0 == 2``,
    so a catalogue whose ``package_count`` is ``false`` beside zero packages
    would be a false fixed point under ``==`` (Sol pass 4). One code path
    includes one serialization, so the round trip is closed where the evidence
    actually lives — in the bytes that were hashed.

    Raises:
        SnapshotVerificationError: The member is empty, is not JSON, is not a
            JSON object, was rejected by the writer's own validation, or is not
            byte-for-byte what the writer emits from its own fields.
    """
    raw = (directory / name).read_bytes()
    if not raw.strip():
        raise _member_error(
            directory, name, "is empty; a present, correctly-hashed empty file is not evidence"
        )
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as error:
        raise _member_error(directory, name, f"is not valid JSON ({error})") from error
    if not isinstance(document, dict):
        raise _member_error(
            directory,
            name,
            f"is a JSON {type(document).__name__}, not the object the writer writes",
        )

    if document.get("snapshot_id") != directory.name:
        raise _member_error(
            directory,
            name,
            f"names snapshot {document.get('snapshot_id')!r}, but it sits in {directory.name!r}",
        )

    try:
        rebuilt = MEMBER_BUILDERS[name](document)
    except SnapshotError as error:
        raise _member_error(
            directory, name, f"is not a document this materializer could have written: {error}"
        ) from error

    if _serialize_document(rebuilt) != raw:
        differing = _disagreeing_keys(document, rebuilt)
        detail = (
            f"disagreeing keys: {differing}"
            if differing
            else "its keys all agree, so the difference is one of ordering or formatting"
        )
        raise _member_error(
            directory,
            name,
            f"is not byte-for-byte what the writer emits from its own fields; {detail}",
        )


def verify_snapshot(directory: Path) -> None:
    """Re-hash every file in a snapshot directory and validate its contents.

    Bidirectional on purpose (FM-09): a listed file that is absent, an on-disk
    file that is not listed, and a digest that does not match are all
    mutations of a directory the contract calls immutable, and all three name
    the offending file.

    Self-consistency is necessary and **not sufficient**, so it is neither the
    first check nor the last. The contract members are required by name first,
    because an empty marker over an empty directory is self-consistent and so
    is any subset. Their contents are validated last, because "present and
    correctly hashed" says nothing about what is inside: two zero-byte members
    listed with the hash of emptiness satisfy every membership and digest check
    there is. Bytes are checked before meaning so that a mutated member is
    reported as a mutation rather than as a shape defect.

    Args:
        directory: The snapshot directory to verify.

    Raises:
        SnapshotVerificationError: The directory is incomplete, is missing a
            contract member, has extra or missing members, a member's bytes
            have changed, or a required member is not the document the writer
            writes.
    """
    checksums = directory / CHECKSUMS_FILENAME
    if not checksums.is_file():
        raise SnapshotVerificationError(
            f"snapshot {directory} has no {CHECKSUMS_FILENAME}, so it is incomplete by "
            "definition (FM-06) and must not be advanced to"
        )

    recorded = _read_checksums(checksums)
    present = {entry.name for entry in _snapshot_members(directory)}

    for name in REQUIRED_MEMBERS:
        if name not in present or name not in recorded:
            raise SnapshotVerificationError(
                f"snapshot {directory} does not carry {name!r} both on disk and in "
                f"{CHECKSUMS_FILENAME}. A self-consistent subset is not a snapshot, so it "
                "must not be activated"
            )

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

    try:
        _require_snapshot_id(directory.name)
    except InvalidDocumentError as error:
        raise SnapshotVerificationError(
            f"snapshot directory {directory.name!r} is not an id this materializer could "
            f"have produced: {error}"
        ) from error
    for name in REQUIRED_MEMBERS:
        _revalidate_member(directory, name)


def _replace_with_retry(
    source: Path,
    destination: Path,
    *,
    error_class: type[SnapshotError],
    consequence: str,
) -> None:
    """``os.replace`` with FM-10's bounded retry, then a loud failure.

    The vault lives under OneDrive, whose sync process takes transient handles
    on the file being replaced. Retrying is legitimate; giving up quietly is
    not — a stale manifest reported as advanced is the silent-data-bug class.

    Args:
        source: The staged temp file.
        destination: The path being installed, atomically.
        error_class: Which failure this replace represents.
        consequence: What the caller is left with, named in the error.

    Raises:
        SnapshotError: Every attempt failed. The destination is untouched and
            the temp file is removed. The concrete class is ``error_class``.
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
    raise error_class(
        f"could not replace {destination} after {REPLACE_ATTEMPTS} attempts; "
        f"{consequence}: {last_error}"
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
    temporary = out_root / f"{MANIFEST_FILENAME}.{snapshot_id}{TEMP_SUFFIX}"
    _write_json(temporary, document)
    _replace_with_retry(
        temporary,
        manifest,
        error_class=ManifestAdvanceError,
        consequence="the manifest still points at its previous snapshot",
    )
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
