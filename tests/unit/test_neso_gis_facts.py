"""GIS catalogue facts (ADR-037 P-16).

Rows T-X4-1, T-X4-2 and the facts half of T-X1-6 of the unit X test matrix.
The committed-file assertion runs in a fresh interpreter, as unit A's
registry tests do.
"""

from __future__ import annotations

import json
import sqlite3
import struct
import subprocess
import sys
import textwrap
import zlib
from pathlib import Path
from typing import Any

import pytest
from _container_support import (
    FIXTURES,
    forbid_zipfile_reads,  # noqa: F401 - a fixture
    patch_headers,
    zip_bytes,
)

from gridflow.connectors.neso_data_portal.gis import GisFactsError, LayerFacts, layer_facts

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _geojson(features: list[dict[str, Any]], crs: str | None = None) -> bytes:
    document: dict[str, Any] = {"type": "FeatureCollection", "features": features}
    if crs is not None:
        document["crs"] = {"type": "name", "properties": {"name": crs}}
    return json.dumps(document).encode("utf-8")


def _feature(geometry: dict[str, Any] | None) -> dict[str, Any]:
    return {"type": "Feature", "properties": {}, "geometry": geometry}


FEATURES = [
    _feature({"type": "Point", "coordinates": [3.0, -1.0, 99.0]}),
    _feature(
        {"type": "Polygon", "coordinates": [[[0.0, 0.0], [5.0, 0.0], [5.0, 2.0], [0.0, 0.0]]]}
    ),
    _feature(None),
    _feature(
        {
            "type": "GeometryCollection",
            "geometries": [{"type": "MultiPoint", "coordinates": [[-4.0, 7.5], [1.0, 1.0]]}],
        }
    ),
]


def _gpkg(table: str = "zones", rows: int = 3, srs: tuple[str, int] = ("EPSG", 27700)) -> bytes:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE gpkg_spatial_ref_sys (
            srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT,
            organization_coordsys_id INTEGER, definition TEXT
        );
        CREATE TABLE gpkg_contents (
            table_name TEXT PRIMARY KEY, data_type TEXT, min_x REAL, min_y REAL,
            max_x REAL, max_y REAL, srs_id INTEGER
        );
        CREATE TABLE notes (id INTEGER);
        """
    )
    connection.execute(
        "INSERT INTO gpkg_spatial_ref_sys VALUES (?, ?, ?, ?, ?)", ("bng", 1, srs[0], srs[1], "x")
    )
    connection.execute(
        "INSERT INTO gpkg_contents VALUES (?, ?, ?, ?, ?, ?, ?)",
        (table, "features", 1.0, 2.0, 30.0, 40.0, 1),
    )
    connection.execute(
        "INSERT INTO gpkg_contents VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("notes", "attributes", None, None, None, None, None),
    )
    quoted = '"' + table.replace('"', '""') + '"'
    connection.execute(f"CREATE TABLE {quoted} (id INTEGER)")
    connection.executemany(f"INSERT INTO {quoted} VALUES (?)", [(n,) for n in range(rows)])
    connection.commit()
    data = connection.serialize()
    connection.close()
    return data


def _shapefile(
    stem: str, count: int, bbox: tuple[float, float, float, float], prj: bool = True
) -> list[tuple[str, bytes]]:
    header = bytearray(100)
    struct.pack_into(">i", header, 0, 9994)
    struct.pack_into("<4d", header, 36, *bbox)
    entries = [
        (f"{stem}.shp", bytes(header) + b"\x00" * 16),
        (f"{stem}.shx", bytes(header) + b"\x00" * (8 * count)),
        (f"{stem}.dbf", b"\x03"),
    ]
    if prj:
        entries.append((f"{stem}.prj", b'PROJCS["British_National_Grid",GEOGCS["GCS_OSGB_1936"]]'))
    return entries


class TestGeoJson:
    def test_count_bbox_and_stated_crs(self) -> None:
        (layer,) = layer_facts(_geojson(FEATURES, "urn:ogc:def:crs:EPSG::27700"), "geojson")
        assert layer == LayerFacts(
            "", "geojson", 4, (-4.0, -1.0, 5.0, 7.5), "urn:ogc:def:crs:EPSG::27700", "crs member"
        )

    def test_absent_crs_is_the_rfc_7946_default(self) -> None:
        (layer,) = layer_facts(_geojson(FEATURES), "geojson")
        assert (layer.crs, layer.crs_source) == ("OGC:CRS84", "RFC 7946 §4 default")

    def test_no_positions_has_no_bbox(self) -> None:
        (layer,) = layer_facts(_geojson([_feature(None)]), "geojson")
        assert (layer.feature_count, layer.bbox) == (1, None)

    @pytest.mark.parametrize("body", [b"{", b'{"type": "Feature"}', b"[]"])
    def test_not_a_feature_collection_is_refused(self, body: bytes) -> None:
        with pytest.raises(GisFactsError):
            layer_facts(body, "geojson")


class TestGpkg:
    def test_feature_layers_with_bbox_srs_and_count(self) -> None:
        assert layer_facts(_gpkg(), "gpkg") == [
            LayerFacts(
                "zones", "gpkg", 3, (1.0, 2.0, 30.0, 40.0), "EPSG:27700", "gpkg_spatial_ref_sys"
            )
        ]

    @pytest.mark.parametrize("table", ['zones"; DROP TABLE gpkg_contents; --', "1zones", "a b"])
    def test_a_table_name_that_is_not_an_identifier_is_refused(self, table: str) -> None:
        with pytest.raises(GisFactsError, match="unsafe GPKG table name"):
            layer_facts(_gpkg(table), "gpkg")

    def test_not_a_geopackage_is_refused(self) -> None:
        with pytest.raises(GisFactsError, match="GeoPackage"):
            layer_facts(b"not sqlite" * 100, "gpkg")


class TestShapefileZip:
    def test_nested_to_depth_two_with_geojson_and_gpkg_members(self) -> None:
        inner = zip_bytes(_shapefile("deep/b", 2, (-1.0, -2.0, 3.0, 4.0), prj=False))
        body = zip_bytes(
            [
                *_shapefile("top/a", 5, (10.0, 20.0, 30.0, 40.0)),
                ("top/inner.zip", inner),
                ("top/layer.geojson", _geojson(FEATURES)),
                ("top/layer.gpkg", _gpkg(rows=7)),
                ("top/readme.txt", b"ignored"),
            ],
            dirs=("top/",),
        )
        assert layer_facts(body, "zip") == [
            LayerFacts(
                "top/a.shp",
                "shapefile",
                5,
                (10.0, 20.0, 30.0, 40.0),
                "British_National_Grid",
                ".prj",
            ),
            LayerFacts(
                "top/inner.zip::deep/b.shp", "shapefile", 2, (-1.0, -2.0, 3.0, 4.0), None, "no .prj"
            ),
            LayerFacts(
                "top/layer.geojson",
                "geojson",
                4,
                (-4.0, -1.0, 5.0, 7.5),
                "OGC:CRS84",
                "RFC 7946 §4 default",
            ),
            LayerFacts(
                "top/layer.gpkg::zones",
                "gpkg",
                7,
                (1.0, 2.0, 30.0, 40.0),
                "EPSG:27700",
                "gpkg_spatial_ref_sys",
            ),
        ]

    def test_an_archive_nested_deeper_than_two_is_refused(self) -> None:
        deepest = zip_bytes(_shapefile("c", 1, (0.0, 0.0, 1.0, 1.0)))
        body = zip_bytes([("one.zip", zip_bytes([("two.zip", deepest)]))])
        with pytest.raises(GisFactsError, match="nested deeper"):
            layer_facts(body, "zip")

    def test_a_shp_without_its_index_is_refused(self) -> None:
        entries = [e for e in _shapefile("a", 1, (0.0, 0.0, 1.0, 1.0)) if e[0] != "a.shx"]
        with pytest.raises(GisFactsError, match="no .shx"):
            layer_facts(zip_bytes(entries), "zip")

    def test_a_ragged_index_is_refused(self) -> None:
        entries = [
            (name, data + b"\x00" if name.endswith(".shx") else data)
            for name, data in _shapefile("a", 1, (0.0, 0.0, 1.0, 1.0))
        ]
        with pytest.raises(GisFactsError, match="not a shapefile index"):
            layer_facts(zip_bytes(entries), "zip")

    @pytest.mark.parametrize("member", ["a.dbf", "readme.txt"])
    def test_a_corrupt_member_the_facts_never_read_refuses_the_archive(self, member: str) -> None:
        body = zip_bytes([*_shapefile("a", 1, (0.0, 0.0, 1.0, 1.0)), ("readme.txt", b"notes")])
        body = patch_headers(body, member, crc=0xDEADBEEF)
        with pytest.raises(GisFactsError, match="body"):
            layer_facts(body, "zip")

    def test_a_corrupt_member_of_a_nested_archive_refuses_the_archive(self) -> None:
        inner = patch_headers(
            zip_bytes(_shapefile("b", 1, (0.0, 0.0, 1.0, 1.0))), "b.dbf", crc=0xDEADBEEF
        )
        body = zip_bytes([*_shapefile("a", 1, (0.0, 0.0, 1.0, 1.0)), ("inner.zip", inner)])
        with pytest.raises(GisFactsError, match="inner.zip"):
            layer_facts(body, "zip")

    def test_a_corrupt_dbf_in_the_real_archive_is_refused(self) -> None:
        """Sol REVIEW-DIFF-1 #1: the committed archive with its DBF CRC changed."""
        data = (PROJECT_ROOT / FIXTURES / "tnuos_gen_zones.zip").read_bytes()
        corrupt = patch_headers(data, "TNUoSGenZones.dbf", crc=zlib.crc32(b"not the dbf"))
        with pytest.raises(GisFactsError):
            layer_facts(corrupt, "zip")

    def test_t_x1_6_real_shapefile_archive(self) -> None:
        """T-X1-6 (facts): the committed TNUoS generation-zones archive."""
        data = (PROJECT_ROOT / FIXTURES / "tnuos_gen_zones.zip").read_bytes()
        (layer,) = layer_facts(data, "zip")
        assert layer.kind == "shapefile" and layer.path == "TNUoSGenZones.shp"
        assert layer.feature_count > 0
        assert layer.bbox is not None and len(layer.bbox) == 4
        assert layer.bbox[0] < layer.bbox[2] and layer.bbox[1] < layer.bbox[3]
        assert (layer.crs, layer.crs_source) == ("GCS_WGS_1984", ".prj")


def test_t_x4_2_every_gis_resource_has_committed_facts() -> None:
    """T-X4-2: the committed facts cover exactly the registry's GIS resources."""
    code = """
        import json
        from pathlib import Path
        from gridflow.connectors.neso_data_portal.registry import load_registry

        registry = load_registry()
        gis = sorted(
            rid for rid, (_p, res) in registry.resources.items() if res.disposition.kind == "GIS"
        )
        facts = json.loads(Path("docs/neso_data_portal/gis-facts.json").read_text("utf-8"))
        ids = [entry["resource_id"] for entry in facts]
        assert ids == gis, (ids, gis)
        assert len(ids) == 20, len(ids)
        for entry in facts:
            assert entry["package"] == registry.resources[entry["resource_id"]][0].package
            assert entry["layers"], entry["resource_id"]
            for layer in entry["layers"]:
                assert layer["feature_count"] > 0, (entry["resource_id"], layer)
                assert len(layer["bbox"]) == 4, (entry["resource_id"], layer)
        print("OK")
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "OK" in result.stdout
