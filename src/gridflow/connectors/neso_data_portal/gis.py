"""Catalogue facts for the NESO ``GIS`` resources (ADR-037 P-16).

A ``GIS`` resource is never transformed to silver; the catalogue states what
each captured body holds: per layer, its feature count, bounding box and CRS.
Stdlib only. Every ZIP level is read through the verified container reader
(:mod:`gridflow.silver.neso_data_portal.containers`, P-4), so a GIS archive
gets the same caps, completeness and CRC proof as a data container.

CLI::

    python -m gridflow.connectors.neso_data_portal.gis (--check|--write)
        [--path P] [--data-dir D]

covers the newest usable capture of every registry resource whose
disposition is ``GIS`` and writes or checks ``docs/neso_data_portal/gis-facts.json``.
Exit 0 agree or written, 1 drift or an unreadable body, 2 usage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import struct
import sys
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.files import replace_atomically
from gridflow.connectors.neso_data_portal.registry.record import CHILD_SEPARATOR
from gridflow.silver.neso_data_portal.containers import (
    Container,
    ContainerReadError,
    open_container,
    read_entry,
)

if TYPE_CHECKING:
    import zipfile
    from collections.abc import Iterator, Sequence

    from gridflow.connectors.neso_data_portal.registry import Registry

__all__ = [
    "DEFAULT_PATH",
    "GisFactsError",
    "LayerFacts",
    "facts_document",
    "layer_facts",
    "main",
]

DEFAULT_PATH = Path("docs/neso_data_portal/gis-facts.json")
SOURCE = "neso_data_portal"
MAX_DEPTH = 2

Format = Literal["geojson", "gpkg", "zip"]
_FORMATS: dict[str, Format] = {"GEOJSON": "geojson", "GPKG": "gpkg", "ZIP": "zip"}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUOTED = re.compile(r'"([^"]*)"')
_SHX_HEADER = 100
_SHX_RECORD = 8


class GisFactsError(Exception):
    """A GIS body cannot be read into layer facts."""


@dataclass(frozen=True)
class LayerFacts:
    """One layer of a GIS body.

    Attributes:
        path: The layer's location: ``""`` for a bare GeoJSON body, the
            GPKG table name, or the archive member path (nested levels joined
            by ``::``).
        kind: ``geojson``, ``gpkg`` or ``shapefile``.
        feature_count: Features in the layer.
        bbox: ``(min_x, min_y, max_x, max_y)`` in the layer's CRS, or
            ``None`` when the body states none.
        crs: The CRS name as the body states it, or ``None``.
        crs_source: Where ``crs`` came from.
    """

    path: str
    kind: Literal["geojson", "gpkg", "shapefile"]
    feature_count: int
    bbox: tuple[float, float, float, float] | None
    crs: str | None
    crs_source: str


def _join(prefix: str, name: str) -> str:
    return f"{prefix}{CHILD_SEPARATOR}{name}" if prefix else name


def _positions(coordinates: Any) -> Iterator[tuple[float, float]]:
    if isinstance(coordinates, list) and coordinates and isinstance(coordinates[0], int | float):
        if len(coordinates) < 2:
            raise GisFactsError(f"position with fewer than two numbers: {coordinates!r}")
        yield float(coordinates[0]), float(coordinates[1])
    elif isinstance(coordinates, list):
        for item in coordinates:
            yield from _positions(item)
    else:
        raise GisFactsError(f"coordinates are not an array: {coordinates!r}")


def _geometry_positions(geometry: Any) -> Iterator[tuple[float, float]]:
    if geometry is None:
        return
    if not isinstance(geometry, dict):
        raise GisFactsError("geometry is not an object")
    if geometry.get("type") == "GeometryCollection":
        for member in geometry.get("geometries") or []:
            yield from _geometry_positions(member)
        return
    yield from _positions(geometry.get("coordinates"))


def _geojson(data: bytes, path: str) -> LayerFacts:
    try:
        document = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise GisFactsError(f"{path or 'body'}: not JSON: {exc}") from exc
    features = document.get("features") if isinstance(document, dict) else None
    if not isinstance(features, list):
        raise GisFactsError(f"{path or 'body'}: no features array")
    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    for feature in features:
        if not isinstance(feature, dict):
            raise GisFactsError(f"{path or 'body'}: a feature is not an object")
        for x, y in _geometry_positions(feature.get("geometry")):
            min_x, min_y = min(min_x, x), min(min_y, y)
            max_x, max_y = max(max_x, x), max(max_y, y)
    bbox = (min_x, min_y, max_x, max_y) if min_x != float("inf") else None
    crs_member = document.get("crs")
    name = None
    if isinstance(crs_member, dict) and isinstance(crs_member.get("properties"), dict):
        value = crs_member["properties"].get("name")
        name = value if isinstance(value, str) else None
    if name is None:
        return LayerFacts(path, "geojson", len(features), bbox, "OGC:CRS84", "RFC 7946 §4 default")
    return LayerFacts(path, "geojson", len(features), bbox, name, "crs member")


def _gpkg(data: bytes, prefix: str) -> list[LayerFacts]:
    connection = sqlite3.connect(":memory:")
    try:
        connection.deserialize(data)
        rows = connection.execute(
            "SELECT table_name, min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents "
            "WHERE data_type = ? ORDER BY table_name",
            ("features",),
        ).fetchall()
        layers: list[LayerFacts] = []
        for table, min_x, min_y, max_x, max_y, srs_id in rows:
            if not isinstance(table, str) or not _IDENTIFIER.fullmatch(table):
                raise GisFactsError(f"{prefix or 'body'}: unsafe GPKG table name {table!r}")
            srs = connection.execute(
                "SELECT organization, organization_coordsys_id FROM gpkg_spatial_ref_sys "
                "WHERE srs_id = ?",
                (srs_id,),
            ).fetchone()
            crs = f"{srs[0]}:{srs[1]}" if srs is not None else None
            # Identifiers cannot be bound; the name passed _IDENTIFIER above.
            (count,) = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
            corners = (min_x, min_y, max_x, max_y)
            bbox = (
                (float(min_x), float(min_y), float(max_x), float(max_y))
                if all(value is not None for value in corners)
                else None
            )
            layers.append(
                LayerFacts(
                    _join(prefix, table),
                    "gpkg",
                    int(count),
                    bbox,
                    crs,
                    "gpkg_spatial_ref_sys" if crs is not None else "no srs row",
                )
            )
    except sqlite3.Error as exc:
        raise GisFactsError(f"{prefix or 'body'}: not a readable GeoPackage: {exc}") from exc
    finally:
        connection.close()
    return layers


def _shapefile(
    container: Container, shp: zipfile.ZipInfo, by_name: dict[str, zipfile.ZipInfo], prefix: str
) -> LayerFacts:
    stem = shp.filename[: -len(".shp")]
    shx = by_name.get(f"{stem}.shx".lower())
    if shx is None:
        raise GisFactsError(f"{_join(prefix, shp.filename)}: no .shx beside the .shp")
    index_size = len(read_entry(container, shx)) - _SHX_HEADER
    if index_size < 0 or index_size % _SHX_RECORD:
        raise GisFactsError(f"{_join(prefix, shx.filename)}: not a shapefile index")
    header = read_entry(container, shp)
    if len(header) < 68:
        raise GisFactsError(f"{_join(prefix, shp.filename)}: truncated .shp header")
    min_x, min_y, max_x, max_y = struct.unpack_from("<4d", header, 36)
    prj = by_name.get(f"{stem}.prj".lower())
    crs: str | None = None
    source = "no .prj"
    if prj is not None:
        match = _QUOTED.search(read_entry(container, prj).decode("utf-8", errors="replace"))
        crs = match.group(1) if match else None
        source = ".prj" if crs is not None else ".prj without a quoted name"
    return LayerFacts(
        _join(prefix, shp.filename),
        "shapefile",
        index_size // _SHX_RECORD,
        (min_x, min_y, max_x, max_y),
        crs,
        source,
    )


def _zip(data: bytes, prefix: str, depth: int) -> list[LayerFacts]:
    if depth > MAX_DEPTH:
        raise GisFactsError(f"{prefix}: archive nested deeper than {MAX_DEPTH}")
    try:
        container = open_container(data, prefix or "body")
        by_name = {info.filename.lower(): info for info in container.files()}
        layers: list[LayerFacts] = []
        for info in container.files():
            suffix = PurePosixPath(info.filename).suffix.lower()
            path = _join(prefix, info.filename)
            if suffix == ".shp":
                layers.append(_shapefile(container, info, by_name, prefix))
            elif suffix == ".zip":
                layers.extend(_zip(read_entry(container, info), path, depth + 1))
            elif suffix == ".geojson":
                layers.append(_geojson(read_entry(container, info), path))
            elif suffix == ".gpkg":
                layers.extend(_gpkg(read_entry(container, info), path))
    except ContainerReadError as exc:
        raise GisFactsError(f"{prefix or 'body'}: {exc}") from exc
    return layers


def layer_facts(data: bytes, fmt: Format) -> list[LayerFacts]:
    """Return the layer facts of one GIS body (P-16).

    Args:
        data: The body bytes.
        fmt: ``geojson``, ``gpkg`` or ``zip`` (a shapefile archive; nested
            archives, ``.geojson`` and ``.gpkg`` members recurse to depth 2).

    Returns:
        One :class:`LayerFacts` per layer, in body order.

    Raises:
        GisFactsError: The body is not readable as ``fmt``, a GPKG table name
            is not a plain identifier, a ``.shp`` has no ``.shx``, or a
            container is refused by the verified reader.
    """
    if fmt == "geojson":
        return [_geojson(data, "")]
    if fmt == "gpkg":
        return _gpkg(data, "")
    return _zip(data, "", 1)


def _layer_json(layer: LayerFacts) -> dict[str, Any]:
    entry = asdict(layer)
    entry["bbox"] = list(layer.bbox) if layer.bbox is not None else None
    return entry


def facts_document(registry: Registry, data_dir: Path) -> list[dict[str, Any]]:
    """Read the newest usable capture of every ``GIS`` resource (read-only).

    Args:
        registry: The loaded registry.
        data_dir: The data root holding ``bronze/neso_data_portal``.

    Returns:
        One entry per GIS resource, sorted by ``resource_id``.

    Raises:
        GisFactsError: A GIS resource has no usable capture, an unsupported
            format, or an unreadable body.
    """
    from gridflow.connectors.neso_data_portal.captures import newest_by_resource, scan_dataset
    from gridflow.silver.neso_data_portal.completion import capture_id_for
    from gridflow.storage.paths import PathBuilder

    paths = PathBuilder(data_dir)
    newest: dict[str, dict[str, Any]] = {}
    entries: list[dict[str, Any]] = []
    for resource_id in sorted(registry.resources):
        package, resource = registry.resources[resource_id]
        if resource.disposition.kind != "GIS":
            continue
        fmt = _FORMATS.get(resource.format)
        if fmt is None:
            raise GisFactsError(f"resource {resource_id}: no GIS reader for {resource.format}")
        if resource.family not in newest:
            scan = scan_dataset(paths.bronze_dir(SOURCE, resource.family), registry)
            newest[resource.family] = newest_by_resource(scan.captures)
        capture = newest[resource.family].get(resource_id)
        if capture is None:
            raise GisFactsError(f"resource {resource_id}: no usable capture")
        data = capture.body.read_bytes()
        entries.append(
            {
                "resource_id": resource_id,
                "package": package.package,
                "capture_id": capture_id_for(capture.body, data_dir),
                "body_sha256": hashlib.sha256(data).hexdigest(),
                "layers": [_layer_json(layer) for layer in layer_facts(data, fmt)],
            }
        )
    return entries


def render(entries: Sequence[dict[str, Any]]) -> str:
    """The committed JSON text: two-space indent, sorted keys, LF, final newline."""
    return json.dumps(list(entries), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point. Exit 0 agree or written, 1 drift or unreadable, 2 usage."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.connectors.neso_data_portal.gis")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument("--path", type=Path, default=DEFAULT_PATH)
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    data_dir: Path | None = args.data_dir
    if data_dir is None:
        from gridflow.config.settings import load_settings

        data_dir = load_settings().pipeline.data_dir
    try:
        rendered = render(facts_document(registry_module.load_registry(), data_dir))
    except (GisFactsError, OSError) as exc:
        print(f"gis facts: {exc}", file=sys.stderr)
        return 1
    path: Path = args.path
    if args.write:
        path.parent.mkdir(parents=True, exist_ok=True)
        replace_atomically(path, rendered.encode("utf-8"))
        print(f"wrote {path}")
        return 0
    try:
        committed = path.read_bytes().decode("utf-8").replace("\r\n", "\n")
    except (OSError, UnicodeDecodeError) as exc:
        print(f"cannot read {path}: {exc}", file=sys.stderr)
        return 1
    if committed != rendered:
        print(
            f"{path} disagrees with bronze; run "
            "`python -m gridflow.connectors.neso_data_portal.gis --write`",
            file=sys.stderr,
        )
        return 1
    print(f"{path} agrees with bronze")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
