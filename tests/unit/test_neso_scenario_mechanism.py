"""Scenario unpivot and edition in the NESO generic engine (v0.22-SC, ADR-042).

T-SC1..T-SC4 of the unit plan, over synthetic records and the existing registry only
(every assertion that needs the FES ED1 pilot record lives in ``test_neso_sc_pilot.py``):

- T-SC1: every generated family that existed at master ``34992b6`` is byte-unchanged
  against the T0 golden (I-1 / SC5);
- T-SC2: the record rules of ``UnpivotSpec``, ``ValueSpec``, ``edition_by_filename``
  and V-18 (P-1);
- T-SC3: unpivot typing through ``run()`` (SC1);
- T-SC4: the edition dimension, its exact filename map and ``_latest`` (SC2).
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pydantic
import pytest
from _neso_dem1h_pin import dump, generated_pin
from _neso_generic_support import install_generated, write_capture
from _neso_registry_support import column, epoch, family, package, record, resource
from _neso_sc_pin import PIN_PATH
from test_neso_dem1_records import _short_base

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import SchemaRecord
from gridflow.connectors.neso_data_portal.registry.record import (
    EDITION,
    PROJECTION_YEAR,
    RESERVED,
    VALUE,
    HeaderEpoch,
    RecordError,
    UnpivotSpec,
    ValueSpec,
    epoch_outputs,
    validate_record,
)
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, latest_select_sql, select_latest_vintage
from gridflow.silver.neso_data_portal.casting import (
    DuplicateEntityKeyError,
    HeaderEpochError,
    UnmappedResourceEditionError,
    edition_for,
    record_columns,
    type_child,
)
from gridflow.silver.neso_data_portal.completion import (
    CaptureContext,
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    scan_completions,
)
from gridflow.silver.neso_data_portal.equivalence import metadata_dependencies
from gridflow.silver.neso_data_portal.generic import output_columns
from gridflow.silver.neso_data_portal.readers import ChildTable
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE = "neso_data_portal"
PKG = "cccccccc-5c00-4000-8000-000000000000"
DAY = date(2026, 10, 7)


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root (MAX_PATH); gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="sc", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def unpivot_epoch(
    index: list[dict[str, Any]],
    years: list[tuple[str, int]],
    *,
    header: list[str] | None = None,
    value: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One unpivot ``HeaderEpoch``: index specs, then the year labels (header written out)."""
    return {
        "header": header
        if header is not None
        else [spec["source"] for spec in index] + [label for label, _year in years],
        "columns": index,
        "issue": {"kind": "none"},
        "unpivot": {
            "years": [[label, year] for label, year in years],
            "value": value if value is not None else {"dtype": "float64", "nullable": True},
        },
    }


def _index() -> list[dict[str, Any]]:
    return [column("Region", "region"), column("Unit", "unit")]


EPOCH_A_YEARS = [("2030", 2030), ("2031", 2031), ("2032", 2032)]
EPOCH_B_YEARS = [("FY32", 2032), ("2033", 2033)]
SYNTH_KEY = ("region", "unit", "projection_year")


def scn_record(
    epochs: list[dict[str, Any]] | None = None,
    *,
    entity_key: tuple[str, ...] = SYNTH_KEY,
    latest: str = "whole_capture",
    **extra: Any,
) -> dict[str, Any]:
    """T-SC3's ``scn_synth`` record: epochs A and B, ``temporal none``, family scope."""
    document = record(
        epochs=epochs
        if epochs is not None
        else [unpivot_epoch(_index(), EPOCH_A_YEARS), unpivot_epoch(_index(), EPOCH_B_YEARS)],
        temporal={"kind": "none"},
        entity_key=entity_key,
        latest=latest,
    )
    document.update(extra)
    return document


def _validate(document: dict[str, Any]) -> None:
    validate_record(
        SchemaRecord.model_validate(document),
        key="scn",
        kind="tabular",
        legacy=False,
        package_families={"scn": True},
        family_url_types=frozenset({"upload"}),
    )


def _refused(rule: str, document: dict[str, Any]) -> str:
    with pytest.raises(RecordError) as info:
        _validate(document)
    message = str(info.value)
    assert message.startswith(f"{rule}:"), message
    return message


def _shape_refused(document: dict[str, Any], fragment: str, model: Any = SchemaRecord) -> None:
    with pytest.raises(pydantic.ValidationError) as info:
        model.model_validate(document)
    assert fragment in str(info.value), str(info.value)


def _failure_names(info: pytest.ExceptionInfo[NesoCaptureFailedError]) -> list[str]:
    return [cls for _capture, cls, _message in info.value.failures]


def _silver(data: Path, key: str) -> pl.DataFrame:
    return pl.concat(
        pl.read_parquet(path, hive_partitioning=False)
        for path in sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    )


def _query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()


def both_latest(db: Path, data: Path, key: str, as_of: datetime | None) -> pl.DataFrame:
    """The catalogue's ``_latest`` (or the parameterised select) = Polars; the SQL rows."""
    view = f"silver_{SOURCE}_{key}"
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    if as_of is None:
        sql = _query(db, f'SELECT * FROM "{view}_latest"')
    else:
        columns = set(_query(db, f'SELECT * FROM "{view}" LIMIT 0').columns)
        select = latest_select_sql(view, spec, columns, as_of_param=True)
        assert select is not None
        sql = _query(db, select, {"as_of": as_of.isoformat()})
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    lf = pl.scan_parquet(files, hive_partitioning=False)
    polars = select_latest_vintage(lf, spec, as_of, completions=scan_completions(data)).collect()
    assert sorted(sql["bronze_capture_id"].to_list()) == sorted(
        polars["bronze_capture_id"].to_list()
    ), (key, as_of)
    return sql


# --------------------------------------------------------------------------- #
# T-SC1: existing keys byte-unchanged (I-1)
# --------------------------------------------------------------------------- #


class TestByteUnchanged:
    def test_t_sc1_every_family_at_the_base_matches_the_golden(self) -> None:
        """Detects any change to a pre-existing generated family's ``_latest`` SQL (both
        as-of modes), record dump, output columns or DEM-1 engine output against the golden
        written on the untouched base (master ``34992b6``), and any family vanishing. Keys
        added since are T-SC8's."""
        golden = json.loads(PIN_PATH.read_text(encoding="utf-8"))
        current = json.loads(dump(generated_pin()))
        for section in ("sql", "records", "columns", "engine"):
            assert set(golden[section]) <= set(current[section]), section
            for key, value in golden[section].items():
                assert current[section][key] == value, (section, key)


# --------------------------------------------------------------------------- #
# T-SC2: record rules (P-1)
# --------------------------------------------------------------------------- #


class TestRecordRules:
    def test_the_synthetic_unpivot_record_validates(self) -> None:
        """The positive control every negative below breaks one rule of."""
        _validate(scn_record())
        rec = SchemaRecord.model_validate(scn_record())
        names = [spec.name for spec in epoch_outputs(rec.epochs[0])]
        assert names == ["region", "unit", PROJECTION_YEAR, VALUE]

    def test_t_sc2_a_empty_years_are_refused(self) -> None:
        """Detects an unpivot that declares no year column (nothing to reshape)."""
        document = unpivot_epoch(_index(), [])
        document["header"] = ["Region", "Unit"]
        _shape_refused(document, "years", HeaderEpoch)

    @pytest.mark.parametrize(
        ("years", "fragment"),
        [
            ([("2030", 2030), ("2030", 2031)], "repeats a year label"),
            ([("2030", 2030), ("FY30", 2030)], "repeats a projection year"),
            ([("", 2030)], "empty year label"),
        ],
        ids=["label", "year", "empty-label"],
    )
    def test_t_sc2_b_a_repeated_label_or_year_is_refused(
        self, years: list[tuple[str, int]], fragment: str
    ) -> None:
        """Detects an ambiguous year map: one label twice, or two labels on one year."""
        _shape_refused(
            {"years": years, "value": {"dtype": "float64", "nullable": True}}, fragment, UnpivotSpec
        )

    @pytest.mark.parametrize(
        ("header", "years", "fragment"),
        [
            (["Region", "Unit", "2030"], [("Unit", 2029), ("2030", 2030)], "overlap"),
            (["Region", "Unit", "Extra", "2030"], [("2030", 2030)], "do not cover"),
            (["Unit", "Region", "2030"], [("2030", 2030)], "header order"),
            (["Region", "Unit", "2031", "2030"], [("2030", 2030), ("2031", 2031)], "header order"),
        ],
        ids=["label-in-columns", "header-in-neither", "columns-out-of-order", "years-out-of-order"],
    )
    def test_t_sc2_c_columns_and_years_partition_the_header_in_order(
        self, header: list[str], years: list[tuple[str, int]], fragment: str
    ) -> None:
        """Detects an unpivot epoch whose index columns and year labels overlap, leave a
        header entry undeclared, or are declared out of header order."""
        _shape_refused(unpivot_epoch(_index(), years, header=header), fragment, HeaderEpoch)

    @pytest.mark.parametrize("source", [VALUE, PROJECTION_YEAR])
    def test_t_sc2_d_an_index_source_named_like_a_generated_column_is_refused(
        self, source: str
    ) -> None:
        """Detects an index column the unpivot would collide with (Polars ``DuplicateError``)."""
        index = [column("Region", "region"), column(source, "vendor_col")]
        _shape_refused(unpivot_epoch(index, [("2030", 2030)]), "generated", HeaderEpoch)

    @pytest.mark.parametrize(
        ("value", "fragment"),
        [
            ({"dtype": "string", "nullable": True, "min": 0}, "numeric"),
            ({"dtype": "string", "nullable": True, "max": 1}, "numeric"),
            ({"dtype": "date", "nullable": True}, "dtype"),
        ],
        ids=["string-min", "string-max", "date"],
    )
    def test_t_sc2_e_value_spec_shape(self, value: dict[str, Any], fragment: str) -> None:
        """Detects bounds on a string value, and a value dtype the unpivot cannot cast."""
        _shape_refused(value, fragment, ValueSpec)

    @pytest.mark.parametrize(
        ("mapping", "fragment"),
        [
            ([], "non-empty"),
            ([["", 2024]], "non-empty"),
            ([["a.csv", 2024], ["a.csv", 2025]], "repeats"),
        ],
        ids=["empty-map", "empty-filename", "duplicate-filename"],
    )
    def test_t_sc2_f_a_malformed_edition_map_is_refused(
        self, mapping: list[list[Any]], fragment: str
    ) -> None:
        """Detects an ambiguous or vacuous edition map."""
        document = scn_record(edition_by_filename=mapping)
        _shape_refused(document, fragment)

    def test_t_sc2_f_two_filenames_may_share_an_edition(self) -> None:
        """Detects a rule refusing a re-versioned file of one edition (the map is exact)."""
        rec = SchemaRecord.model_validate(
            scn_record(edition_by_filename=[["a.csv", 2024], ["a_v2.csv", 2024]])
        )
        assert rec.edition_by_filename == (("a.csv", 2024), ("a_v2.csv", 2024))

    def test_t_sc2_g_v18_a_every_epoch_declares_year_and_value(self) -> None:
        """Detects a long epoch beside an unpivot epoch that lacks ``value``."""
        long = epoch([*_index(), column("Year", PROJECTION_YEAR, "int64", nullable=False)])
        document = scn_record([unpivot_epoch(_index(), EPOCH_A_YEARS), long])
        assert "projection_year" in _refused("V-18", document)

    def test_t_sc2_g_v18_b_projection_year_is_in_the_key(self) -> None:
        """Detects an unpivot record whose key cannot tell one year's row from another's."""
        _refused("V-18", scn_record(entity_key=("region", "unit")))

    def test_t_sc2_g_v18_c_edition_is_in_the_key_with_the_map(self) -> None:
        """Detects editions collapsing onto one key (an edition map without ``edition``)."""
        _refused("V-18", scn_record(edition_by_filename=[["a.csv", 2024]]))

    def test_t_sc2_g_v18_d_an_edition_family_partitions_by_resource(self) -> None:
        """Detects a family-scope whole-capture selection that would keep one edition."""
        document = scn_record(
            entity_key=(EDITION, *SYNTH_KEY), edition_by_filename=[["a.csv", 2024]]
        )
        _refused("V-18", document)

    def test_t_sc2_g_v1_an_index_silver_name_value_collides(self) -> None:
        """Detects an index column whose silver name collides with the generated ``value``."""
        index = [column("Region", "region"), column("Value", VALUE)]
        _refused("V-1", scn_record([unpivot_epoch(index, EPOCH_A_YEARS)]))

    def test_t_sc2_g_v1_a_long_epoch_types_projection_year_alike(self) -> None:
        """Detects a long epoch declaring ``projection_year`` with another dtype."""
        long = epoch(
            [
                *_index(),
                column("Year", PROJECTION_YEAR, "string", nullable=False),
                column("Val", VALUE, "float64"),
            ]
        )
        _refused("V-1", scn_record([unpivot_epoch(_index(), EPOCH_A_YEARS), long]))

    def test_t_sc2_g_v4_edition_in_the_key_needs_the_map(self) -> None:
        """Detects ``edition`` admitted to a key the engine never stamps."""
        _refused("V-4", scn_record(entity_key=(EDITION, *SYNTH_KEY)))

    def test_t_sc2_g_v2_edition_is_reserved_and_collected(self) -> None:
        """Detects a vendor column colliding with the engine's ``edition`` stamp: the
        name is reserved and the parametrised V-2 test collects a case for it."""
        assert EDITION in RESERVED
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "--collect-only",
                "-q",
                "-p",
                "no:cacheprovider",
                "tests/unit/test_neso_record.py",
                "-k",
                "test_v2_reserved_name",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=300,
            check=False,
        )
        assert "test_v2_reserved_name[edition]" in result.stdout, result.stdout

    def test_t_sc2_h_the_default_dump_carries_no_new_field(self) -> None:
        """Detects a non-``None`` default on a new field, which would change every
        record's ``exclude_none`` dump (and stale every COVERED grant)."""
        plain = SchemaRecord.model_validate(record()).model_dump(mode="json", exclude_none=True)
        assert "edition_by_filename" not in plain
        assert all("unpivot" not in item for item in plain["epochs"])

    def test_t_sc2_h_an_opted_in_record_round_trips(self) -> None:
        """Detects a field lost or reshaped through ``model_dump``/``model_validate``."""
        document = scn_record(
            entity_key=("resource_id", EDITION, *SYNTH_KEY),
            edition_by_filename=[["a.csv", 2024], ["b.csv", 2025]],
            latest_partition="resource_id",
        )
        rec = SchemaRecord.model_validate(document)
        again = SchemaRecord.model_validate(rec.model_dump(mode="json"))
        assert again == rec
        assert again.epochs[1].unpivot is not None
        assert again.epochs[1].unpivot.years == (("FY32", 2032), ("2033", 2033))

    def test_t_sc2_i_every_committed_record_loads(self) -> None:
        """Detects a committed record broken by the new shape rules, V-4, V-18 or the
        reserved ``edition`` (in a fresh interpreter, so nothing imported can mask it)."""
        code = textwrap.dedent(
            """
            from gridflow.connectors.neso_data_portal.registry import load_registry
            families = load_registry().families
            recorded = [k for k, (_p, f) in families.items() if f.record is not None]
            print("OK", len(recorded))
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        assert "OK" in result.stdout, result.stdout

    def test_t_sc2_j_epoch_outputs_is_the_identity_without_unpivot(self) -> None:
        """Detects ``epoch_outputs`` rebuilding (or extending) a non-unpivot epoch's specs,
        which would change every existing family's typing, columns and exclusion."""
        loaded = registry_module.load_registry()
        seen = 0
        for _package, entry in loaded.families.values():
            if entry.record is None:
                continue
            for item in entry.record.epochs:
                if item.unpivot is None:
                    assert epoch_outputs(item) is item.columns
                    seen += 1
        assert seen > 0


# --------------------------------------------------------------------------- #
# T-SC3: unpivot typing (SC1)
# --------------------------------------------------------------------------- #

SYNTH = "scn_synth"
SYNTH_RESOURCE = ("cccccccc-5c00-4000-8000-0000000000a1", "Scenario synth", "scn.csv")
HEADER_A = b"Region,Unit,2030,2031,2032\n"
HEADER_B = b"Region,Unit,FY32,2033\n"


def install_synth(monkeypatch: pytest.MonkeyPatch, data: Path, rec: dict[str, Any]) -> Any:
    rid, name, _filename = SYNTH_RESOURCE
    document = package("pkg-scn", PKG, [family(SYNTH, record=rec)], [resource(rid, name, SYNTH)])
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    return generated


def capture_synth(data: Path, body: bytes, written: datetime) -> str:
    rid, name, filename = SYNTH_RESOURCE
    path, _sidecar = write_capture(
        data,
        SYNTH,
        package_slug="pkg-scn",
        package_id=PKG,
        resource_id=rid,
        resource_name=name,
        resource_filename=filename,
        body=body,
        written_at=written,
        ckan_last_modified=written.replace(tzinfo=None).isoformat(),
        partition=DAY,
    )
    return capture_id_for(path, data)


def _ctx(filename: str = "scn.csv") -> CaptureContext:
    return CaptureContext(
        capture_id="bronze/neso_data_portal/scn_synth/2026/10/07/raw_x.csv",
        partition_date=DAY,
        body=Path("raw_x.csv"),
        sidecar=Path("raw_x.meta.json"),
        capture_written_at=_t(12),
        resource_id=SYNTH_RESOURCE[0],
        resource_filename=filename,
        url_type="upload",
        body_sha256="0" * 64,
        empty_capture=False,
        published_at=_t(11),
    )


def _table(header: list[str], rows: list[list[str | None]]) -> ChildTable:
    frame = pl.DataFrame(
        {name: [row[i] for row in rows] for i, name in enumerate(header)},
        schema=dict.fromkeys(header, pl.Utf8),
    )
    return ChildTable(child_id="", header=tuple(header), frame=frame)


class TestUnpivotTyping:
    def test_t_sc3_a_wide_rows_become_typed_long_rows(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a lost or invented long row, a blank cell coerced to a value, a year
        read from the label instead of the declared map, or a value cast leniently."""
        generated = install_synth(monkeypatch, data, scn_record())
        capture_synth(data, HEADER_A + b"N,GW,1.5,,3\nS,GWh,4,5,-6.25\n", _t(8))
        generated.transformers[SYNTH](data).run(DAY, run_id="r")
        silver = _silver(data, SYNTH)
        assert silver.schema[PROJECTION_YEAR] == pl.Int64
        assert silver.schema[VALUE] == pl.Float64
        rows = sorted(
            silver.select("region", "unit", PROJECTION_YEAR, VALUE).iter_rows(),
            key=lambda row: (row[0], row[2]),
        )
        assert rows == [
            ("N", "GW", 2030, 1.5),
            ("N", "GW", 2031, None),
            ("N", "GW", 2032, 3.0),
            ("S", "GWh", 2030, 4.0),
            ("S", "GWh", 2031, 5.0),
            ("S", "GWh", 2032, -6.25),
        ]

    def test_t_sc3_b_each_epoch_types_into_one_output_schema(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a second epoch producing a different column set or dtypes, and a
        label (``FY32``) parsed instead of mapped."""
        generated = install_synth(monkeypatch, data, scn_record())
        capture_synth(data, HEADER_A + b"N,GW,1,2,3\n", _t(8))
        capture_synth(data, HEADER_B + b"N,GW,7,8\n", _t(9))
        generated.transformers[SYNTH](data).run(DAY, run_id="r")
        frames = [
            pl.read_parquet(path, hive_partitioning=False)
            for path in sorted((data / "silver" / SOURCE / SYNTH).rglob("[!.]*.parquet"))
        ]
        assert len(frames) == 2
        assert frames[0].schema == frames[1].schema
        second = next(frame for frame in frames if frame.height == 2)
        assert dict(second.select(PROJECTION_YEAR, VALUE).iter_rows()) == {2032: 7.0, 2033: 8.0}

    def test_t_sc3_c_an_uncastable_year_cell_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a lenient cast after the unpivot (D-41): one ``x`` fails the capture,
        which writes no output and no completion (FM-2)."""
        generated = install_synth(monkeypatch, data, scn_record())
        capture_id = capture_synth(data, HEADER_A + b"N,GW,1,x,3\n", _t(8))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[SYNTH](data).run(DAY, run_id="r")
        assert _failure_names(info) == ["InvalidOperationError"]
        assert read_completion(data, SYNTH, capture_id) is None
        assert not list(data.rglob("silver/**/*.parquet"))

    def test_t_sc3_d_exclusion_judges_long_rows(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects exclusion run on the wide row (one blank year dropping every year of
        it) or uncounted: under a non-nullable ``value`` with ``min 0``, one blank and one
        ``-1`` exclude exactly those two long rows (FM-1)."""
        value = {"dtype": "float64", "nullable": False, "min": 0}
        rec = scn_record([unpivot_epoch(_index(), EPOCH_A_YEARS, value=value)])
        body_rows: list[list[str | None]] = [
            ["N", "GW", None, "1", "2"],
            ["S", "GW", "-1", "3", "4"],
        ]
        typed = type_child(
            _table(["Region", "Unit", "2030", "2031", "2032"], body_rows),
            SchemaRecord.model_validate(rec),
            _ctx(),
        )
        assert typed.tally.counts == {"null": 1, "range": 1}

        generated = install_synth(monkeypatch, data, rec)
        capture_synth(data, HEADER_A + b"N,GW,,1,2\nS,GW,-1,3,4\n", _t(8))
        transformer = generated.transformers[SYNTH](data)
        transformer.run(DAY, run_id="r")
        assert transformer.last_excluded_row_count == 2
        kept = sorted(_silver(data, SYNTH).select("region", PROJECTION_YEAR).iter_rows())
        assert kept == [("N", 2031), ("N", 2032), ("S", 2031), ("S", 2032)]

    def test_t_sc3_e_an_undeclared_column_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects an extra vendor column (or a new year) passing silently (FM-5)."""
        generated = install_synth(monkeypatch, data, scn_record())
        capture_synth(data, b"Region,Unit,2030,2031,2032,2034\nN,GW,1,2,3,4\n", _t(8))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[SYNTH](data).run(DAY, run_id="r")
        assert _failure_names(info) == [HeaderEpochError.__name__]

    def test_t_sc3_f_the_written_columns_are_the_record_columns(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a year label (or a temporary name) leaking into silver, the manifest's
        column list disagreeing with the file, or ``row_count`` counting wide rows (FM-10)."""
        generated = install_synth(monkeypatch, data, scn_record())
        capture_id = capture_synth(data, HEADER_A + b"N,GW,1,2,3\nS,GW,4,5,6\n", _t(8))
        generated.transformers[SYNTH](data).run(DAY, run_id="r")
        (path,) = sorted((data / "silver" / SOURCE / SYNTH).rglob("[!.]*.parquet"))
        written = pl.read_parquet(path, hive_partitioning=False)
        rec = SchemaRecord.model_validate(scn_record())
        columns = record_columns(rec)
        assert tuple(written.columns[: len(columns)]) == columns
        assert not {"2030", "2031", "2032", "FY32", "2033"} & set(written.columns)
        assert written.columns == [name for name, _type in output_columns(rec)][:-2]
        completion = read_completion(data, SYNTH, capture_id)
        assert completion is not None
        assert completion["row_count"] == written.height == 6

    def test_t_sc3_g_a_repeated_dimension_tuple_fails_loudly(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects duplicate long rows deduplicated (or kept) instead of failing (FM-9)."""
        generated = install_synth(monkeypatch, data, scn_record())
        capture_synth(data, HEADER_A + b"N,GW,1,2,3\nN,GW,4,5,6\n", _t(8))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[SYNTH](data).run(DAY, run_id="r")
        assert _failure_names(info) == [DuplicateEntityKeyError.__name__]

    def test_t_sc3_h_a_zero_height_table_is_typed(self) -> None:
        """Detects a zero-height unpivot leaving ``projection_year`` as String, which
        breaks the concat beside a populated child (FM-11)."""
        rec = SchemaRecord.model_validate(scn_record())
        typed = type_child(_table(["Region", "Unit", "2030", "2031", "2032"], []), rec, _ctx())
        assert typed.frame.height == 0
        assert typed.frame.schema[PROJECTION_YEAR] == pl.Int64
        assert typed.frame.schema[VALUE] == pl.Float64


# --------------------------------------------------------------------------- #
# T-SC4: edition (SC2)
# --------------------------------------------------------------------------- #

ED = "scn_ed"
ED_RESOURCES = {
    "A": ("cccccccc-5c00-4000-8000-00000000000a", "Edition A", "ed_2024.csv"),
    "B": ("cccccccc-5c00-4000-8000-00000000000b", "Edition B", "ed_2025.csv"),
    "C": ("cccccccc-5c00-4000-8000-00000000000c", "Edition C", "ed_2099.csv"),
    "D": ("cccccccc-5c00-4000-8000-00000000000d", "Edition D", "ed_2025b.csv"),
}
ED_MAP = [["ed_2024.csv", 2024], ["ed_2025.csv", 2025], ["ed_2025b.csv", 2025]]
ED_HEADER = b"Region,2030,2031\n"


def edition_record(**overrides: Any) -> dict[str, Any]:
    """A resource-partitioned unpivot record with an exact edition map."""
    document = scn_record(
        [unpivot_epoch([column("Region", "region")], [("2030", 2030), ("2031", 2031)])],
        entity_key=("resource_id", EDITION, "region", PROJECTION_YEAR),
        edition_by_filename=ED_MAP,
        latest_partition="resource_id",
    )
    document.update(overrides)
    return document


def install_edition(monkeypatch: pytest.MonkeyPatch, data: Path) -> Any:
    entries = [resource(rid, name, ED) for rid, name, _filename in ED_RESOURCES.values()]
    fam = family(ED, record=edition_record(), empty_allowed=True)
    _registry, generated = install_generated(
        monkeypatch, data / "_registry", [package("pkg-ed", PKG, [fam], entries)]
    )
    return generated


def capture_edition(data: Path, letter: str, body: bytes, written: datetime, **kwargs: Any) -> str:
    rid, name, filename = ED_RESOURCES[letter]
    path, _sidecar = write_capture(
        data,
        ED,
        package_slug="pkg-ed",
        package_id=PKG,
        resource_id=rid,
        resource_name=name,
        resource_filename=filename,
        body=body,
        written_at=written,
        ckan_last_modified=written.replace(tzinfo=None).isoformat(),
        partition=DAY,
        **kwargs,
    )
    return capture_id_for(path, data)


class TestEdition:
    def test_t_sc4_a_every_edition_is_stamped_and_served(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects editions collapsing in ``_latest`` (one surviving), a wrong or missing
        ``edition`` stamp, or a false ``overlap`` between editions with one grain (FM-6)."""
        generated = install_edition(monkeypatch, data)
        body = ED_HEADER + b"N,1,2\nS,3,4\n"
        a = capture_edition(data, "A", body, _t(8))
        b = capture_edition(data, "B", body, _t(9))
        generated.transformers[ED](data).run(DAY, run_id="r")
        silver = _silver(data, ED)
        assert silver.schema[EDITION] == pl.Int64
        stamped = dict(
            silver.group_by("bronze_capture_id").agg(pl.col(EDITION).unique()).iter_rows()
        )
        assert stamped == {a: [2024], b: [2025]}
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        for as_of in (None, _t(10)):
            served = both_latest(db, data, ED, as_of)
            assert sorted(served["bronze_capture_id"].to_list()) == sorted([a] * 4 + [b] * 4)
            assert sorted(set(served[EDITION].to_list())) == [2024, 2025]
        assert both_latest(db, data, ED, _t(8, 30))[EDITION].unique().to_list() == [2024]
        report = reconcile(data, registry_module.load_registry(), [ED], DAY)
        assert report.gaps == ()
        assert "SUMMARY overlap 0" in report.lines()

    @pytest.mark.parametrize("empty", [False, True], ids=["populated", "valid-empty"])
    def test_t_sc4_bc_an_unmapped_filename_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, empty: bool
    ) -> None:
        """Detects a guessed (or skipped, on the header-only path) edition for a file the
        record does not map: the capture fails, writes nothing and reconcile reports it
        ``failed`` (FM-3, FM-4)."""
        generated = install_edition(monkeypatch, data)
        body = ED_HEADER if empty else ED_HEADER + b"N,1,2\n"
        extra = {"empty_capture": True} if empty else {}
        capture_id = capture_edition(data, "C", body, _t(8), **extra)
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[ED](data).run(DAY, run_id="r")
        assert _failure_names(info) == [UnmappedResourceEditionError.__name__]
        message = info.value.failures[0][2]
        assert "ed_2099.csv" in message and "ed_2024.csv" in message
        assert read_completion(data, ED, capture_id) is None
        assert not list(data.rglob("silver/**/*.parquet"))
        report = reconcile(data, registry_module.load_registry(), [ED], DAY)
        assert [(gap.category, gap.capture_id) for gap in report.gaps] == [("failed", capture_id)]

    def test_t_sc4_d_a_republished_edition_overlaps_and_both_are_served(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects an invented precedence between two resources carrying one edition's
        grain, or the overlap passing silently or drainably (FM-7)."""
        generated = install_edition(monkeypatch, data)
        b = capture_edition(data, "B", ED_HEADER + b"N,1,2\n", _t(8))
        d = capture_edition(data, "D", ED_HEADER + b"N,5,6\n", _t(9))
        generated.transformers[ED](data).run(DAY, run_id="r")
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        served = both_latest(db, data, ED, None)
        assert sorted(served["bronze_capture_id"].to_list()) == sorted([b, b, d, d])
        loaded = registry_module.load_registry()
        report = reconcile(data, loaded, [ED], DAY)
        assert sorted(gap.capture_id for gap in report.gaps) == sorted([b, d])
        assert {gap.category for gap in report.gaps} == {"overlap"}
        assert all(not gap.drainable for gap in report.gaps)
        after = drain(data, loaded, [ED], DAY, lambda: None)
        assert after.drained == ()

    def test_t_sc4_e_the_edition_map_is_a_metadata_dependency(self) -> None:
        """Detects a COVERED grant surviving a renamed file of an edition family (FM-14),
        and the dependency leaking onto records without the map."""
        mapped = SchemaRecord.model_validate(edition_record())
        assert "resource_filename" in metadata_dependencies(mapped)
        plain = SchemaRecord.model_validate(edition_record(edition_by_filename=None))
        assert metadata_dependencies(plain) == ("ckan_last_modified", "empty_capture", "url_type")

    def test_edition_for_is_exact(self) -> None:
        """Detects a normalised, prefix or fallback edition lookup."""
        mapped = SchemaRecord.model_validate(edition_record())
        assert edition_for(mapped, "ed_2025b.csv") == 2025
        for near in ("ED_2025.csv", "ed_2025.CSV", " ed_2025.csv", "ed_2025", ""):
            with pytest.raises(UnmappedResourceEditionError):
                edition_for(mapped, near)
        plain = SchemaRecord.model_validate(scn_record())
        assert edition_for(plain, "anything.csv") is None
