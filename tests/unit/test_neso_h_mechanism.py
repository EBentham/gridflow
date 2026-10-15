"""The shared-layer mechanisms v0.22-H adds for system frequency (ADR-043).

- **T-F1** every generated family that existed at master ``4c4b8ec`` is byte-unchanged
  (I-1 / H8): ``_latest`` SQL in both as-of modes, record dumps, output columns and the
  DEM-1 engine outputs, against the golden written on the untouched base;
- **T-F2/T-F3** ``ColumnSpec.alt_formats``: its shape rules, and the typing of a column's
  rendering variants (a date-only midnight beside full cells) with a loud
  ``UnparsedDatetimeError`` for a cell no format parses;
- **T-F4** ``SchemaRecord.overlap_group`` and V-21: a group is two or more
  resource-partitioned members with one grain of one dtype.

Synthetic packages go through a tmp registry (``install_generated``) and a short data root.
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_dem1h_pin import dump, generated_pin
from _neso_generic_support import install_generated, write_capture
from _neso_h_pin import PIN_PATH
from _neso_registry_support import column, epoch, family, package, record, resource, write_registry

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import RegistryError, SchemaRecord
from gridflow.connectors.neso_data_portal.registry.record import ColumnSpec
from gridflow.silver.neso_data_portal.casting import UnparsedDatetimeError
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

SOURCE = "neso_data_portal"
DAY = date(2026, 10, 8)
PKG = "f4e90000-0000-4000-8000-000000000000"
SLUG = "pkg-freq"
KEY = "freq"
RID = "f4e90000-0000-4000-8000-00000000000a"
R3_FORMAT = "%Y-%m-%d %H:%M:%S"


def _short_base() -> str:
    """The drive root on Windows (ADR-036: engine names pass MAX_PATH there); tmp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="hf", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def r3_dtm(**overrides: Any) -> dict[str, Any]:
    """P-2's R3 ``dtm`` column: ISO, GMT-stated (zone UTC), date-only midnights allowed."""
    spec: dict[str, Any] = {
        "source": "dtm",
        "name": "dtm",
        "dtype": "datetime",
        "format": R3_FORMAT,
        "alt_formats": ["%Y-%m-%d"],
        "zone": "UTC",
        "nullable": False,
    }
    spec.update(overrides)
    return {name: value for name, value in spec.items() if value is not None}


def freq_record(dtm: dict[str, Any] | None = None, **overrides: Any) -> dict[str, Any]:
    """A system-frequency-shaped record: ``(dtm, f)``, per-resource whole-capture selection."""
    columns = [dtm if dtm is not None else r3_dtm(), column("f", "f", "float64")]
    document = record(
        epochs=[epoch(columns)],
        temporal={"kind": "utc_instant", "column": "dtm"},
        entity_key=("resource_id", "dtm"),
        latest="whole_capture",
    )
    document["latest_partition"] = "resource_id"
    document.update(overrides)
    return document


def _install(monkeypatch: pytest.MonkeyPatch, data: Path, rec: dict[str, Any]) -> Any:
    fam = family(KEY, record=rec, empty_allowed=True)
    document = package(SLUG, PKG, [fam], [resource(RID, "Freq A", KEY)])
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    return generated


def _capture(data: Path, body: bytes) -> str:
    path, _sidecar = write_capture(
        data,
        KEY,
        package_slug=SLUG,
        package_id=PKG,
        resource_id=RID,
        resource_name="Freq A",
        resource_filename="a.csv",
        body=body,
        written_at=datetime(2026, 10, 8, 9, tzinfo=UTC),
        partition=DAY,
    )
    return capture_id_for(path, data)


def _silver(data: Path, key: str = KEY) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    assert files
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path, key: str = KEY) -> bool:
    return not list((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def _body(*cells: str) -> bytes:
    return b"dtm,f\r\n" + b"".join(f"{cell},50.0\r\n".encode() for cell in cells)


# --------------------------------------------------------------------------- #
# T-F1: existing keys byte-unchanged (I-1)
# --------------------------------------------------------------------------- #


class TestByteUnchanged:
    def test_t_f1_every_family_at_the_base_matches_the_golden(self) -> None:
        """Detects any change to a pre-existing generated family's ``_latest`` SQL (both
        as-of modes), record dump (so a COVERED evidence digest), output columns or DEM-1
        engine output against the golden written on the untouched base (master
        ``4c4b8ec``), and any family vanishing. A non-``None`` default on ``alt_formats``
        or ``overlap_group`` changes every dump and fails here."""
        golden = json.loads(PIN_PATH.read_text(encoding="utf-8"))
        current = json.loads(dump(generated_pin()))
        for section in ("sql", "records", "columns", "engine"):
            assert set(golden[section]) <= set(current[section]), section
            for key, value in golden[section].items():
                assert current[section][key] == value, (section, key)


# --------------------------------------------------------------------------- #
# T-F2: alt_formats shape (P-3)
# --------------------------------------------------------------------------- #


class TestAltFormatsShape:
    @pytest.mark.parametrize(
        ("overrides", "fragment"),
        [
            ({"dtype": "date", "format": "%Y-%m-%d", "zone": None}, "datetime columns only"),
            ({"dtype": "string", "format": None, "zone": None}, "datetime columns only"),
            ({"dtype": "float64", "format": None, "zone": None}, "datetime columns only"),
            ({"format": None}, "needs a format"),
            ({"format": "%Y-%m-%d %H:%M:%S %z", "zone": None}, "%z"),
            ({"alt_formats": ["%Y-%m-%d %z"]}, "%z"),
            ({"alt_formats": []}, "non-empty"),
            ({"alt_formats": [""]}, "empty"),
            ({"alt_formats": ["%Y-%m-%d", "%Y-%m-%d"]}, "repeats an entry"),
            ({"alt_formats": [R3_FORMAT]}, "repeats format"),
        ],
        ids=[
            "date",
            "string",
            "float64",
            "no-format",
            "format-z",
            "entry-z",
            "empty-tuple",
            "empty-entry",
            "duplicate",
            "equals-format",
        ],
    )
    def test_t_f2_a_malformed_alternative_is_refused_naming_the_column(
        self, overrides: dict[str, Any], fragment: str
    ) -> None:
        """Detects an alternative format accepted where it would be ignored (a non-datetime
        or offset-carrying column), could never apply (empty, a repeat of ``format``) or is
        ambiguous (a duplicate)."""
        spec = {
            "source": "dtm",
            "name": "dtm",
            "dtype": "datetime",
            "format": R3_FORMAT,
            "alt_formats": ["%Y-%m-%d"],
            "zone": "UTC",
            "nullable": False,
        }
        spec.update(overrides)
        spec = {name: value for name, value in spec.items() if value is not None}
        with pytest.raises(ValueError) as info:
            ColumnSpec.model_validate(spec)
        message = str(info.value)
        assert "'dtm'" in message, message
        assert fragment in message, message

    def test_t_f2_the_r3_column_loads_and_a_plain_column_dumps_no_field(self) -> None:
        """The positive control, and I-1: a column without the field dumps no key for it."""
        spec = ColumnSpec.model_validate(r3_dtm())
        assert spec.alt_formats == ("%Y-%m-%d",)
        plain = ColumnSpec.model_validate(r3_dtm(alt_formats=None))
        assert "alt_formats" not in plain.model_dump(mode="json", exclude_none=True)
        dumped = SchemaRecord.model_validate(record()).model_dump(mode="json", exclude_none=True)
        assert "alt_formats" not in json.dumps(dumped)
        assert "overlap_group" not in dumped


# --------------------------------------------------------------------------- #
# T-F3: alt_formats typing (P-3, FM-4, FM-5)
# --------------------------------------------------------------------------- #


def _failure_names(info: pytest.ExceptionInfo[NesoCaptureFailedError]) -> list[str]:
    return [cls for _capture, cls, _message in info.value.failures]


class TestAltFormatsTyping:
    def test_t_f3_a_a_date_only_midnight_types_and_a_full_cell_keeps_its_seconds(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a date-only midnight excluded or failed (ruling 6), or a full cell read
        by the shorter alternative (truncated to midnight): both rows type, zero excluded."""
        generated = _install(monkeypatch, data, freq_record())
        capture_id = _capture(data, _body("2026-08-01", "2026-08-01 00:00:01"))
        generated.transformers[KEY](data).run(DAY, run_id="r")
        silver = _silver(data).sort("dtm")
        assert silver["dtm"].to_list() == [
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 1, 0, 0, 1, tzinfo=UTC),
        ]
        assert silver.schema["dtm"] == pl.Datetime("us", "UTC")
        assert silver["timestamp_utc"].to_list() == silver["dtm"].to_list()
        completion = read_completion(data, KEY, capture_id)
        assert completion is not None
        assert completion["row_count"] == 2

    @pytest.mark.parametrize(
        "cell",
        ["2026-08-01T00:00:00", "2026-08-01 24:00:00", "01/08/2026"],
        ids=["iso-t", "hour-24", "day-first"],
    )
    def test_t_f3_b_a_cell_no_format_parses_fails_the_capture(
        self, data: Path, monkeypatch: pytest.MonkeyPatch, cell: str
    ) -> None:
        """Detects a non-strict parse coercing or excluding a cell no format reads (D-41):
        the capture fails with ``UnparsedDatetimeError`` naming the column and the cell,
        and writes no output and no completion."""
        generated = _install(monkeypatch, data, freq_record())
        capture_id = _capture(data, _body("2026-08-01 00:00:00", cell))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[KEY](data).run(DAY, run_id="r")
        assert _failure_names(info) == [UnparsedDatetimeError.__name__]
        message = info.value.failures[0][2]
        assert "'dtm'" in message and cell in message and "1 cell(s)" in message
        assert read_completion(data, KEY, capture_id) is None
        assert _no_silver(data)

    def test_t_f3_c_a_blank_cell_is_a_null_exclusion_not_a_parse_error(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the strictness pass treating a null cell as unparsed: a blank ``dtm`` is
        excluded under ``null`` (tallied) and the other row is written."""
        generated = _install(monkeypatch, data, freq_record())
        capture_id = _capture(data, b"dtm,f\r\n,50.0\r\n2026-08-01,50.1\r\n")
        generated.transformers[KEY](data).run(DAY, run_id="r")
        silver = _silver(data)
        assert silver["dtm"].to_list() == [datetime(2026, 8, 1, tzinfo=UTC)]
        completion = read_completion(data, KEY, capture_id)
        assert completion is not None
        assert completion["row_count"] == 1
        assert completion["rows_excluded"] == 1

    def test_t_f3_d_without_the_field_a_date_only_cell_still_fails(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the alternative parse leaking into a column that does not declare it:
        the same family without ``alt_formats`` fails the date-only cell strictly."""
        generated = _install(monkeypatch, data, freq_record(r3_dtm(alt_formats=None)))
        capture_id = _capture(data, _body("2026-08-01", "2026-08-01 00:00:01"))
        with pytest.raises(NesoCaptureFailedError) as info:
            generated.transformers[KEY](data).run(DAY, run_id="r")
        assert len(info.value.failures) == 1
        assert _failure_names(info)[0] != UnparsedDatetimeError.__name__
        assert read_completion(data, KEY, capture_id) is None


# --------------------------------------------------------------------------- #
# T-F4: overlap_group and V-21 (P-4, FM-10)
# --------------------------------------------------------------------------- #

R1_DTM = {"source": "dtm", "name": "dtm", "dtype": "string", "nullable": False}
R2_DTM = {
    "source": "dtm",
    "name": "dtm",
    "dtype": "datetime",
    "format": "%Y-%m-%d %H:%M:%S %z",
    "nullable": False,
}


def _group_package(members: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """A bronze-home family ``home`` and one sibling-fed recorded family per member."""
    families = [family("home")]
    resources: list[dict[str, Any]] = []
    for index, (key, rec) in enumerate(sorted(members.items())):
        rec = {**rec, "siblings": ["home"]}
        families.append(family(key, record=rec))
        rid = f"f4e90000-0000-4000-8000-0000000001{index:02d}"
        resources.append(
            resource(rid, f"Month {index}", "home", disposition={"kind": "SILVER", "key": key})
        )
    return package(SLUG, PKG, families, resources)


def _load_group(tmp_path: Path, members: dict[str, dict[str, Any]]) -> Any:
    directory = write_registry(tmp_path / "registry", [_group_package(members)])
    return registry_module.load_registry(directory)


def _typed(**overrides: Any) -> dict[str, Any]:
    return freq_record(overlap_group="system_frequency", **overrides)


class TestOverlapGroupRules:
    def test_a_two_member_group_loads(self, tmp_path: Path) -> None:
        """The positive control: an R2 member and an R3 member share a datetime grain."""
        loaded = _load_group(
            tmp_path,
            {
                "fam_a": _typed(),
                "fam_b": _typed(epochs=[epoch([R2_DTM, column("f", "f", "float64")])]),
            },
        )
        groups = {key: loaded.families[key][1].record.overlap_group for key in ("fam_a", "fam_b")}
        assert groups == {"fam_a": "system_frequency", "fam_b": "system_frequency"}

    def test_t_f4_a_a_one_member_group_is_refused(self, tmp_path: Path) -> None:
        """Detects a group of one, which would check nothing beyond its own family."""
        with pytest.raises(RegistryError) as info:
            _load_group(tmp_path, {"fam_a": _typed(), "fam_b": freq_record()})
        assert "V-21:" in str(info.value) and "one member" in str(info.value)

    def test_t_f4_b_a_raw_string_grain_cannot_join_a_typed_group(self, tmp_path: Path) -> None:
        """Detects R1 (raw day-first ``dtm`` strings) joining the R2/R3 group, where its
        labels would be compared with typed instants (FM-10): the dtypes differ."""
        r1 = _typed(
            epochs=[epoch([R1_DTM, column("f", "f", "float64")])], temporal={"kind": "none"}
        )
        with pytest.raises(RegistryError) as info:
            _load_group(tmp_path, {"fam_a": _typed(), "fam_b": r1})
        message = str(info.value)
        assert "V-21:" in message and "'dtm'" in message and "string" in message

    def test_t_f4_b_a_different_grain_is_refused(self, tmp_path: Path) -> None:
        """Detects two members whose per-resource keys are different columns."""
        other = _typed(
            epochs=[epoch([r3_dtm(), column("f", "f", "float64")])],
            entity_key=["resource_id", "dtm", "f"],
        )
        with pytest.raises(RegistryError) as info:
            _load_group(tmp_path, {"fam_a": _typed(), "fam_b": other})
        assert "V-21:" in str(info.value) and "grain" in str(info.value)

    def test_t_f4_c_a_member_without_the_partition_is_refused(self, tmp_path: Path) -> None:
        """Detects a family-scope member, whose ``_latest`` has no resource column to compare."""
        flat = _typed(entity_key=["dtm"])
        del flat["latest_partition"]
        with pytest.raises(RegistryError) as info:
            _load_group(tmp_path, {"fam_a": _typed(), "fam_b": flat})
        assert "V-21:" in str(info.value) and "latest_partition" in str(info.value)

    @pytest.mark.parametrize("name", ["System", "1freq", "freq-group", ""])
    def test_t_f4_d_a_malformed_group_name_is_refused(self, name: str) -> None:
        """Detects a group name that is not a silver name."""
        with pytest.raises(ValueError, match="overlap_group"):
            SchemaRecord.model_validate(freq_record(overlap_group=name))


# --------------------------------------------------------------------------- #
# T-F5: interval pre-check soundness (P-5, FM-11)
# --------------------------------------------------------------------------- #


def _spans(*spans: list[int | None], dtype: Any = pl.Int64) -> pl.LazyFrame:
    """One capture per span; each span lists the capture's key values."""
    captures = [f"c{index}" for index, span in enumerate(spans) for _value in span]
    values = [value for span in spans for value in span]
    return pl.LazyFrame(
        {"bronze_capture_id": captures, "k": values, "resource_id": captures},
        schema={"bronze_capture_id": pl.Utf8, "k": dtype, "resource_id": pl.Utf8},
    )


class TestProvenDisjoint:
    @pytest.mark.parametrize(
        ("spans", "expected"),
        [
            (([1, 5], [6, 9]), True),
            (([6, 9], [1, 5]), True),
            (([1, 5], [5, 9]), False),
            (([1, 9], [3, 4]), False),
            (([1, 5], [6, None]), False),
        ],
        ids=["disjoint", "disjoint-unsorted", "touching", "nested", "null"],
    )
    def test_t_f5_the_sweep_decides_only_proven_disjoint_spans(
        self, spans: tuple[list[int | None], ...], expected: bool
    ) -> None:
        """Detects a sweep that misses a touching or nested span (a key served twice
        reported as disjoint), or that lets a null key skip the exact path."""
        from gridflow.silver.neso_data_portal.reconcile import _proven_disjoint

        assert _proven_disjoint(_spans(*spans), ["k"]) is expected

    def test_t_f5_a_two_column_grain_takes_the_exact_path(self) -> None:
        """Detects the pre-check deciding a composite grain it cannot order."""
        from gridflow.silver.neso_data_portal.reconcile import _proven_disjoint

        frame = _spans([1, 5], [6, 9]).with_columns(pl.lit(1).alias("j"))
        assert _proven_disjoint(frame, ["k", "j"]) is False

    def test_t_f5_a_float_grain_takes_the_exact_path(self) -> None:
        """Detects the pre-check deciding a dtype outside its allowlist (NaN ordering)."""
        from gridflow.silver.neso_data_portal.reconcile import _proven_disjoint

        assert _proven_disjoint(_spans([1, 5], [6, 9], dtype=pl.Float64), ["k"]) is False


INT_KEY = "intfam"
INT_RIDS = [f"f4e90000-0000-4000-8000-0000000002{index:02d}" for index in range(4)]
KINDS = ("disjoint", "touching", "interleaved", "null")


def _int_record() -> dict[str, Any]:
    columns = [column("K", "k", "int64"), column("V", "v", "float64")]
    document = record(
        epochs=[epoch(columns)],
        temporal={"kind": "none"},
        entity_key=("resource_id", "k"),
        latest="whole_capture",
    )
    document["latest_partition"] = "resource_id"
    return document


def _round_values(rng: Any, kind: str, count: int) -> list[list[int | None]]:
    if kind in ("disjoint", "null"):
        values: list[list[int | None]] = [
            list(range(20 * i, 20 * i + rng.randint(1, 10))) for i in range(count)
        ]
        if kind == "null":
            values[rng.randrange(count)].append(None)
        return values
    if kind == "touching":
        return [list(range(10 * i, 10 * i + 11)) for i in range(count)]
    return [sorted(rng.sample(range(30), rng.randint(3, 12))) for _ in range(count)]


def _int_body(values: list[int | None], fill: float) -> bytes:
    cells = ["" if value is None else str(value) for value in values]
    return ("K,V\n" + "".join(f"{cell},{fill}\n" for cell in cells)).encode()


class TestPreCheckRoundTrips:
    def test_t_f5_reconcile_is_equal_with_and_without_the_pre_check(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the pre-check changing any reconcile report: twelve seeded rounds of 2-4
        resources with disjoint, touching, interleaved or null-bearing key ranges each give
        identical lines with the pre-check and with it forced to ``False``. The pre-check
        must short-circuit in a disjoint round and an overlap must fire in a touching or
        interleaved round, so the comparison is never vacuous."""
        import random

        from gridflow.silver.neso_data_portal import reconcile as reconcile_module

        rng = random.Random(20261015)
        real = reconcile_module._proven_disjoint
        verdicts: dict[str, list[bool]] = {kind: [] for kind in KINDS}
        overlaps: dict[str, int] = dict.fromkeys(KINDS, 0)
        for index in range(12):
            kind = KINDS[index % len(KINDS)]
            count = rng.randint(2, 4)
            root = data / f"r{index}"
            entries = [resource(INT_RIDS[i], f"Int {i}", INT_KEY) for i in range(count)]
            fam = family(INT_KEY, record=_int_record(), empty_allowed=True)
            document = package("pkg-int", PKG, [fam], entries)
            _registry, generated = install_generated(monkeypatch, root / "_registry", [document])
            for i, values in enumerate(_round_values(rng, kind, count)):
                write_capture(
                    root,
                    INT_KEY,
                    package_slug="pkg-int",
                    package_id=PKG,
                    resource_id=INT_RIDS[i],
                    resource_name=f"Int {i}",
                    body=_int_body(values, float(i)),
                    written_at=datetime(2026, 10, 8, 9, i, tzinfo=UTC),
                    partition=DAY,
                )
            generated.transformers[INT_KEY](root).run(DAY, run_id="r")
            loaded = registry_module.load_registry()

            def spy(selected: pl.LazyFrame, grain: list[str], _kind: str = kind) -> bool:
                verdict: bool = real(selected, grain)
                verdicts[_kind].append(verdict)
                return verdict

            with monkeypatch.context() as patch:
                patch.setattr(reconcile_module, "_proven_disjoint", spy)
                checked = reconcile_module.reconcile(root, loaded, [INT_KEY], DAY)
            with monkeypatch.context() as patch:
                patch.setattr(reconcile_module, "_proven_disjoint", lambda *_args: False)
                exact = reconcile_module.reconcile(root, loaded, [INT_KEY], DAY)
            assert checked.lines() == exact.lines(), (index, kind)
            overlaps[kind] += sum(gap.category == "overlap" for gap in exact.gaps)
        assert True in verdicts["disjoint"]
        assert verdicts["null"] and not any(verdicts["null"])
        assert not any(verdicts["touching"]) and not any(verdicts["interleaved"])
        assert overlaps["touching"] > 0 and overlaps["interleaved"] > 0
        assert overlaps["disjoint"] == 0 and overlaps["null"] == 0


# --------------------------------------------------------------------------- #
# T-F6: group overlap (P-5, FM-12, FM-13)
# --------------------------------------------------------------------------- #

GROUP = "system_frequency"
GROUP_SLUG = "pkg-grp"
HOME = "home_freq"
CSV_A, CSV_B, ZIP_C = "grp_csv_a", "grp_csv_b", "grp_zip_c"
GROUP_RIDS = {
    CSV_A: "f4e90000-0000-4000-8000-00000000030a",
    CSV_B: "f4e90000-0000-4000-8000-00000000030b",
    ZIP_C: "f4e90000-0000-4000-8000-00000000030c",
}
GROUP_NAMES = {CSV_A: "Month A", CSV_B: "Month B", ZIP_C: "Month C"}
MEMBER = "m.csv"


def _member_record(reader: str) -> dict[str, Any]:
    rec = freq_record(r3_dtm(alt_formats=None), overlap_group=GROUP, siblings=[HOME])
    if reader == "zip_member":
        rec["reader"] = "zip_member"
        rec["zip_member"] = {"member_pattern": "m\\.csv", "inner": "csv"}
    return rec


def _group_document() -> dict[str, Any]:
    families = [
        family(HOME),
        family(CSV_A, record=_member_record("csv")),
        family(CSV_B, record=_member_record("csv")),
        family(ZIP_C, record=_member_record("zip_member")),
    ]
    zipped = {"kind": "SILVER", "key": ZIP_C}
    resources = [
        resource(
            GROUP_RIDS[key], GROUP_NAMES[key], HOME, disposition={"kind": "SILVER", "key": key}
        )
        for key in (CSV_A, CSV_B)
    ]
    resources.append(
        resource(
            GROUP_RIDS[ZIP_C],
            GROUP_NAMES[ZIP_C],
            HOME,
            disposition=zipped,
            children=[{"child": MEMBER, "disposition": zipped}],
        )
    )
    return package(GROUP_SLUG, PKG, families, resources)


def _install_group(monkeypatch: pytest.MonkeyPatch, data: Path) -> Any:
    _registry, generated = install_generated(monkeypatch, data / "_registry", [_group_document()])
    return generated


def _zip(member: bytes) -> bytes:
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MEMBER, member)
    return buffer.getvalue()


def _seconds(*instants: str) -> bytes:
    return b"dtm,f\r\n" + b"".join(f"{instant},50.0\r\n".encode() for instant in instants)


def _group_capture(data: Path, key: str, body: bytes) -> str:
    zipped = key == ZIP_C
    extension = "zip" if zipped else "csv"
    path, _sidecar = write_capture(
        data,
        HOME,
        package_slug=GROUP_SLUG,
        package_id=PKG,
        resource_id=GROUP_RIDS[key],
        resource_name=GROUP_NAMES[key],
        resource_filename=f"{key}.{extension}",
        body=_zip(body) if zipped else body,
        written_at=datetime(2026, 10, 8, 9, tzinfo=UTC),
        partition=DAY,
        extension=extension,
    )
    return capture_id_for(path, data)


def _run_group(generated: Any, data: Path) -> None:
    for key in (CSV_A, CSV_B, ZIP_C):
        generated.transformers[key](data).run(DAY, run_id="r")


def _latest_ids(data: Path, key: str) -> list[str]:
    from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
    from gridflow.silver.neso_data_portal.completion import scan_completions

    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    lf = pl.scan_parquet(files, hive_partitioning=False)
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    selected = select_latest_vintage(lf, spec, completions=scan_completions(data)).collect()
    return sorted(selected["bronze_capture_id"].to_list())


def _overlap_gaps(report: Any) -> list[Any]:
    return [gap for gap in report.gaps if gap.category == "overlap"]


def _entry(key: str, captures: list[str]) -> dict[str, Any]:
    return {
        "family": key,
        "category": "overlap",
        "captures": sorted(captures),
        "reason": "two months of one series serve one instant (synthetic)",
        "question": "Which month serves the shared instant?",
        "evidence": "v0.22-H T-F6",
        "ruling": "861",
    }


def _ledgered(data: Path, ledger: list[dict[str, Any]]) -> Any:
    directory = write_registry(
        data / "_ledger", [_group_document()], reconcile_adjudications=ledger
    )
    return registry_module.load_registry(directory)


class TestGroupOverlap:
    def test_t_f6_a_contiguous_members_report_no_overlap(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a false cross-family overlap on contiguous months."""
        from gridflow.silver.neso_data_portal.reconcile import reconcile

        generated = _install_group(monkeypatch, data)
        _group_capture(data, CSV_A, _seconds("2026-01-31 23:59:58", "2026-01-31 23:59:59"))
        _group_capture(data, CSV_B, _seconds("2026-02-01 00:00:00", "2026-02-01 00:00:01"))
        _group_capture(data, ZIP_C, _seconds("2026-02-01 00:00:02"))
        _run_group(generated, data)
        report = reconcile(data, registry_module.load_registry(), [CSV_A, CSV_B, ZIP_C], DAY)
        assert report.clean, report.lines()
        assert "SUMMARY overlap 0" in report.lines()

    def test_t_f6_b_one_instant_in_two_families_is_served_and_reported(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a cross-family overlap passing silently (the per-family check cannot see
        it) or an invented precedence: both rows stay in their families' ``_latest``, both
        captures get a non-drainable ``overlap`` gap under their own family naming the other
        as a peer, and the drain leaves them."""
        from gridflow.silver.neso_data_portal.reconcile import drain, reconcile

        generated = _install_group(monkeypatch, data)
        a = _group_capture(data, CSV_A, _seconds("2026-01-31 23:59:59", "2026-02-01 00:00:00"))
        c = _group_capture(data, ZIP_C, _seconds("2026-02-01 00:00:00", "2026-02-01 00:00:01"))
        _run_group(generated, data)
        assert _latest_ids(data, CSV_A) == [a, a]
        assert _latest_ids(data, ZIP_C) == [c, c]

        loaded = registry_module.load_registry()
        report = reconcile(data, loaded, [CSV_A, CSV_B, ZIP_C], DAY)
        overlaps = _overlap_gaps(report)
        assert {(gap.family, gap.capture_id) for gap in overlaps} == {(CSV_A, a), (ZIP_C, c)}
        assert len(report.gaps) == 2 and not report.passed
        by_capture = {gap.capture_id: gap for gap in overlaps}
        assert by_capture[a].peers == (c,) and by_capture[c].peers == (a,)
        assert by_capture[a].detail == (
            f"1 key(s) also served by resource(s) ['{GROUP_RIDS[ZIP_C]}']; "
            "first: dtm=2026-02-01 00:00:00+00:00"
        )
        assert all(not gap.drainable for gap in overlaps)
        assert "SUMMARY overlap 2" in report.lines()

        refreshed: list[bool] = []
        after = drain(data, loaded, [CSV_A, CSV_B, ZIP_C], DAY, lambda: refreshed.append(True))
        assert after.drained == () and refreshed == []
        assert [gap.line() for gap in _overlap_gaps(after)] == [gap.line() for gap in overlaps]

    def test_t_f6_c_one_checked_member_still_reads_the_whole_group(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the group check narrowing to the named family (FM-13): reconciling only
        A still reports A's gap with B's capture as its peer, and no gap under B."""
        from gridflow.silver.neso_data_portal.reconcile import reconcile

        generated = _install_group(monkeypatch, data)
        a = _group_capture(data, CSV_A, _seconds("2026-02-01 00:00:00"))
        b = _group_capture(data, CSV_B, _seconds("2026-02-01 00:00:00"))
        _run_group(generated, data)
        report = reconcile(data, registry_module.load_registry(), [CSV_A], DAY)
        assert [(gap.category, gap.family, gap.capture_id, gap.peers) for gap in report.gaps] == [
            ("overlap", CSV_A, a, (b,))
        ]

    def test_t_f6_d_one_entry_per_family_adjudicates_the_cross_family_overlap(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects group adjudication leaving a foreign peer stale (each family's entry must
        name the other family's capture to cover its gap), and detects it hiding what must
        stay open: the two entries load with no referential problem and the report passes
        with two adjudicated gaps and no stale entry, also when only one member is checked;
        a third family's capture joining the overlap leaves every gap open."""
        from gridflow.silver.neso_data_portal.reconcile import STALE_ADJUDICATION, reconcile

        generated = _install_group(monkeypatch, data)
        a = _group_capture(data, CSV_A, _seconds("2026-02-01 00:00:00"))
        b = _group_capture(data, CSV_B, _seconds("2026-02-01 00:00:00"))
        _run_group(generated, data)
        loaded = _ledgered(data, [_entry(CSV_A, [a, b]), _entry(CSV_B, [a, b])])
        entries = registry_module.load_reconcile_adjudications(loaded.root)
        assert registry_module.reconcile_adjudication_problems(loaded, entries) == []

        report = reconcile(data, loaded, [CSV_A, CSV_B, ZIP_C], DAY)
        assert report.passed, report.lines()
        assert sorted((item.gap.family, item.gap.capture_id) for item in report.adjudicated) == [
            (CSV_A, a),
            (CSV_B, b),
        ]
        assert "SUMMARY adjudicated 2" in report.lines()
        assert f"SUMMARY {STALE_ADJUDICATION} 0" in report.lines()

        only_a = reconcile(data, loaded, [CSV_A], DAY)
        assert only_a.passed and len(only_a.adjudicated) == 1, only_a.lines()

        c = _group_capture(data, ZIP_C, _seconds("2026-02-01 00:00:00"))
        generated.transformers[ZIP_C](data).run(DAY, run_id="r")
        widened = reconcile(data, loaded, [CSV_A, CSV_B, ZIP_C], DAY)
        assert {(gap.family, gap.capture_id) for gap in _overlap_gaps(widened)} == {
            (CSV_A, a),
            (CSV_B, b),
            (ZIP_C, c),
        }
        assert not widened.adjudicated and not widened.passed

    def test_t_f6_d_an_entry_whose_overlap_has_gone_is_stale(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects the foreign-peer rule hiding a stale entry: with no overlap left, every
        capture each entry names is a ``stale_adjudication`` gap."""
        from gridflow.silver.neso_data_portal.reconcile import STALE_ADJUDICATION, reconcile

        generated = _install_group(monkeypatch, data)
        a = _group_capture(data, CSV_A, _seconds("2026-01-31 23:59:59"))
        b = _group_capture(data, CSV_B, _seconds("2026-02-01 00:00:00"))
        _run_group(generated, data)
        loaded = _ledgered(data, [_entry(CSV_A, [a, b]), _entry(CSV_B, [a, b])])
        report = reconcile(data, loaded, [CSV_A, CSV_B], DAY)
        stale = sorted(
            (gap.family, gap.capture_id)
            for gap in report.gaps
            if gap.category == STALE_ADJUDICATION
        )
        assert stale == sorted([(CSV_A, a), (CSV_A, b), (CSV_B, a), (CSV_B, b)])

    def test_t_f6_e_an_unreadable_output_fails_the_check_for_each_checked_member(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Detects a group check that cannot read one member's output passing silently
        (FM-12): every checked member gets one ``overlap check failed`` gap."""
        from gridflow.silver.neso_data_portal.reconcile import reconcile

        generated = _install_group(monkeypatch, data)
        _group_capture(data, CSV_A, _seconds("2026-01-31 23:59:59"))
        _group_capture(data, CSV_B, _seconds("2026-02-01 00:00:00"))
        _run_group(generated, data)
        (output,) = sorted((data / "silver" / SOURCE / CSV_B).rglob("*.parquet"))
        output.write_bytes(output.read_bytes()[:40])
        report = reconcile(data, registry_module.load_registry(), [CSV_A, CSV_B], DAY)
        failed = [
            gap.family
            for gap in _overlap_gaps(report)
            if gap.capture_id == "-" and gap.detail.startswith("overlap check failed: ")
        ]
        assert sorted(failed) == [CSV_A, CSV_B]
        assert all(not gap.drainable for gap in _overlap_gaps(report))
