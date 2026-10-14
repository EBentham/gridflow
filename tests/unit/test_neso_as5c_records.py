"""The AS-5c records: the FFR post-tender workbook record over 59 workbooks and the three Phase-2
FFR ZIP-archive records (v0.22-K-AS-5c).

Two package files carry the records: ``firm-frequency-response-post-tender-reports`` (one record,
``ffr_post_tender_reports_workbook``, reader ``xlsx`` with one composite block per resource,
ADR-037 amendment 1) and ``phase-2-ffr-auction-results-summary`` (the block-order, linear-order and
results-by-unit archives, reader ``zip_member`` csv, all held). Engine tests write the AS-5c
fixtures (``tests/fixtures/neso_data_portal/as5c/``, provenance in ``PROVENANCE.md``) and the
committed ``containers/tr129.xlsm`` as captures under a short tmp data root and run the transformer
the package registry generates; nothing touches ``C:/gridflow-data``.

Record decisions under test (K-AS-5c-SPEC, PLAN K-1..K-14):

- The block map is keyed by ``resource_id``; an unlisted resource fails the capture (V-20).
- Workbook columns take AS-5a's names, dtypes and nullability by position (43 for A:AQ blocks,
  the same list without ``post_code`` for A:AP); dates and window clocks stay raw ``string``
  (temporal ``none``); tender references keep their stored ``<v>`` text.
- TR123 (a ``14,4`` MW cell) and TR125 (populated column AQ under a blank heading) are resource
  HOLDs; no workbook is COVERED.
- The archives are held on PHASE2-ARCHIVE-SEMANTICS; the 2019-11-22 by-unit member is a HOLD
  child (its two conflicting ``UF_LJP12`` rows are never selected between).
"""

from __future__ import annotations

import copy
import json
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _container_support import (  # noqa: F401 - forbid_zipfile_reads is a fixture
    Cell,
    forbid_zipfile_reads,
    lineage_cells,
    lineage_workbook,
)
from _neso_generic_support import install_generated, write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, Held, load_registry
from gridflow.connectors.neso_data_portal.registry.record import has_issue_time
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
)
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry import Registry
    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

pytestmark = pytest.mark.usefixtures("forbid_zipfile_reads")

SOURCE = "neso_data_portal"
ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "neso_data_portal"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
UNIT = "v0.22-K-AS-5c"

FFR_PKG = "firm-frequency-response-post-tender-reports"
FFR_PKG_ID = "1162a519-b66d-48f6-96ae-1a69ff17951f"
WB = "ffr_post_tender_reports_workbook"
WB_DIR = "ffr_post_tender_reports_files"
AS5A_FFR = "ffr_post_tender_reports_csv_ext"
LINEAGES = ("tr122", "tr112", "tr126", "tr108", "tr144")
LINEAGE_MODIFIED = "2020-03-10T10:00:00.000001"
WRITTEN = datetime(2026, 10, 8, 11, 0, 50, tzinfo=UTC)

TR123 = "2e49daa5-90eb-4157-bcef-6fcf29e75085"
TR125 = "4bea886e-3aad-4a8d-9bec-1c8e770739b0"
TR123_REASON = (
    "TODO: FFR-TR123-COMMA-MW — the TR123 (March 2020) post-tender workbook's High Frequency "
    "Response (max.) @ 0.2Hz (MW) cell AH85 (Tender Ref 123.08400000000036) holds the text "
    '"14,4", which fails the record\'s strict float64 type for high_frequency_response_0_2hz; is '
    "it 14.4 MW written with a decimal comma, and can NESO correct the upload? Re-disposition only "
    "after a declared lexical-number mechanism or a corrected upload (a new last_modified)"
)
TR125_REASON = (
    "TODO: FFR-TR125-UNHEADED-AQ — the TR125 (May 2020) post-tender workbook has text in column "
    'AQ for 15 tender rows (63–68, 125–127, 133, 154–156, 170–171, e.g. "Mutual exclusive '
    'tenders") under a blank composite header (AQ2:AQ4), so the column has no heading to name it '
    "from; what does NESO call this column? Re-disposition after a vendor heading or an approved "
    "positional raw-column contract"
)
HOLD_REASONS = {TR123: TR123_REASON, TR125: TR125_REASON}

TR129 = "5a1f039a-c6c8-43a2-9ae4-069a0a0b32b2"

ATLAS: list[tuple[str, str, int, int, int, str]] = [
    # (resource prefix, exact sheet, header_first, data_first, data_last, columns), transcribed
    # from RESEARCH U1's per-workbook atlas for the 59 SILVER resources in package order.
    ("bf1bbc9a", "TR122", 2, 7, 165, "A:AP"),  # TR122
    ("e692dc29", "TR121 ", 2, 7, 121, "A:AP"),  # TR121
    ("653110b8", "TR120", 2, 7, 121, "A:AP"),  # TR120
    ("0544683b", "TR119", 2, 7, 114, "A:AP"),  # TR119
    ("d397ffa6", "Sheet1", 2, 7, 127, "A:AP"),  # TR118
    ("8c1dc03e", "TR117", 2, 7, 134, "A:AP"),  # TR117
    ("1eb0314b", "Sheet1", 2, 7, 115, "A:AP"),  # TR116
    ("1c1c7788", "TR115 ", 2, 7, 113, "A:AP"),  # TR115
    ("93802a8f", "Sheet1", 2, 7, 111, "A:AP"),  # TR114
    ("aa3c7d58", "Sheet1", 2, 7, 97, "A:AP"),  # TR113
    ("dbaa2bb4", "Sheet1", 3, 8, 98, "A:AP"),  # TR112
    ("d16afe87", "Sheet1", 3, 8, 78, "A:AP"),  # TR111
    ("50488f63", "Sheet1", 3, 8, 97, "A:AP"),  # TR110
    ("27ad6287", "Sheet1", 3, 8, 78, "A:AP"),  # TR109
    ("31854ad2", "Sheet1", 6, 11, 432, "A:AP"),  # TR108
    ("93dac57b", "TR124", 2, 7, 167, "A:AP"),  # TR124
    ("9137187c", "TR126", 4, 9, 190, "A:AP"),  # TR126
    ("68529290", "TR127", 2, 7, 181, "A:AP"),  # TR127 original
    ("16773f53", "TR127", 4, 9, 183, "A:AP"),  # TR127 amended
    ("6e6b47ea", "TR128", 2, 7, 177, "A:AP"),  # TR128
    ("5a1f039a", "TR129", 2, 7, 162, "A:AP"),  # TR129
    ("4b4a2e83", "TR130", 2, 7, 134, "A:AP"),  # TR130
    ("c325243e", "TR131", 2, 7, 139, "A:AP"),  # TR131
    ("ce75bd6e", "TR132", 2, 7, 129, "A:AP"),  # TR132
    ("0f72be2e", "TR133", 2, 7, 120, "A:AP"),  # TR133
    ("36c2cf05", "TR134", 2, 7, 112, "A:AP"),  # TR134
    ("7912d65a", "TR136", 2, 7, 138, "A:AP"),  # TR136 original
    ("eaddd8aa", "TR135", 2, 7, 124, "A:AP"),  # TR135
    ("a85e6149", "TR136", 3, 8, 139, "A:AP"),  # TR136 amended
    ("6e4f83e8", "TR137", 2, 7, 131, "A:AP"),  # TR137
    ("bc47f2cb", "TR138", 2, 7, 137, "A:AP"),  # TR138
    ("a5b3f43d", "TR139", 2, 7, 103, "A:AP"),  # TR139
    ("d96d15ed", "TR140", 2, 7, 96, "A:AQ"),  # TR140
    ("4df9b9b7", "TR141", 2, 7, 100, "A:AQ"),  # TR141
    ("89134181", "TR142", 2, 7, 170, "A:AQ"),  # TR142
    ("a2888ffb", "TR143 ", 2, 7, 207, "A:AQ"),  # TR143
    ("6cd10acf", "TR144", 2, 7, 246, "A:AQ"),  # TR144
    ("29014d2b", "TR145", 2, 7, 235, "A:AQ"),  # TR145
    ("85ec5689", "TR146", 2, 7, 202, "A:AQ"),  # TR146
    ("8ec9b10f", "TR147", 2, 7, 218, "A:AQ"),  # TR147
    ("c72a9877", "TR148", 2, 7, 209, "A:AQ"),  # TR148
    ("46313224", "TR149", 2, 7, 233, "A:AQ"),  # TR149
    ("e5c028dd", "TR150", 2, 7, 265, "A:AQ"),  # TR150
    ("0ad0f7f5", "TR151", 2, 7, 258, "A:AQ"),  # TR151
    ("08ac3a6c", "TR152", 2, 7, 272, "A:AQ"),  # TR152
    ("854e29a9", "TR153", 2, 7, 278, "A:AQ"),  # TR153
    ("e4da45cc", "TR154", 2, 7, 268, "A:AQ"),  # TR154
    ("244c9023", "TR155", 2, 7, 287, "A:AQ"),  # TR155
    ("1d0def5b", "TR156", 2, 7, 275, "A:AQ"),  # TR156
    ("8540c6b2", "TR157", 2, 7, 297, "A:AQ"),  # TR157
    ("987467c5", "TR158", 2, 7, 316, "A:AQ"),  # TR158
    ("ed783d99", "TR159", 2, 7, 212, "A:AQ"),  # TR159
    ("5aa12080", "TR160", 2, 7, 209, "A:AQ"),  # TR160
    ("634c7528", "TR161", 2, 7, 215, "A:AQ"),  # TR161
    ("9a707d8f", "TR162", 2, 7, 200, "A:AQ"),  # TR162
    ("c3526774", "TR163", 2, 7, 194, "A:AQ"),  # TR163
    ("152ba991", "TR164", 2, 7, 192, "A:AQ"),  # TR164
    ("2e561c2b", "TR165", 2, 7, 185, "A:AQ"),  # TR165
    ("8bb5bf74", "TR166", 2, 7, 155, "A:AQ"),  # TR166
]


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short tmp data root; transformer modules imported under the real registry."""
    pipeline_runner.import_transformers()
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(prefix="c", ignore_cleanup_errors=True) as root:
        yield Path(root)


def _package_doc(slug: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(
        (REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8")
    )
    return document


def _record(key: str) -> SchemaRecord:
    record = load_registry().families[key][1].record
    assert record is not None, key
    return record


def _family_doc(document: dict[str, Any], key: str) -> dict[str, Any]:
    (family,) = [f for f in document["families"] if f["key"] == key]
    return family


def _workbook_resources(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [r for r in document["resources"] if r["format"] in ("XLSX", "XLSM")]


def _outputs(data: Path, key: str) -> list[Path]:
    return sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))


def _silver(data: Path, key: str) -> pl.DataFrame:
    return pl.concat([pl.read_parquet(p, hive_partitioning=False) for p in _outputs(data, key)])


def _resource_doc(document: dict[str, Any], resource_id: str) -> dict[str, Any]:
    (resource,) = [r for r in document["resources"] if r["id"] == resource_id]
    return resource


def _lineage_capture(data: Path, body: bytes, lineage: dict[str, Any]) -> str:
    resource = _resource_doc(_package_doc(FFR_PKG), lineage["resource_id"])
    path, _sidecar = write_capture(
        data,
        WB_DIR,
        package_slug=FFR_PKG,
        package_id=FFR_PKG_ID,
        resource_id=lineage["resource_id"],
        resource_name=resource["name"],
        body=body,
        written_at=WRITTEN,
        ckan_last_modified=LINEAGE_MODIFIED,
        resource_filename="post-tender-report.xlsx",
        ckan_format=resource["format"],
        extension="xlsx",
        partition=DAY,
    )
    return capture_id_for(path, data)


def _install_lineage(
    monkeypatch: pytest.MonkeyPatch, data: Path, lineage: dict[str, Any], **change: int
) -> tuple[Registry, Any]:
    """The committed FFR package with the lineage resource's block cut to its fixture rows."""
    document = copy.deepcopy(_package_doc(FFR_PKG))
    record = _family_doc(document, WB)["record"]
    (block,) = [b for b in record["xlsx_blocks"] if b["resource_id"] == lineage["resource_id"]]
    block["data_last"] = lineage["header_first"] + 7
    block.update(change)
    return install_generated(monkeypatch, data / "_reg", [document])  # type: ignore[no-any-return]


def _run_failing(generated: Any, data: Path, capture_id: str) -> tuple[str, str]:
    """Run the workbook transformer on a capture that must fail: ``(error class, message)``."""
    with pytest.raises(NesoCaptureFailedError) as info:
        generated.transformers[WB](data).run(DAY, run_id="r")
    ((got, cls, message),) = info.value.failures
    assert got == capture_id
    assert read_completion(data, WB, capture_id) is None
    assert read_failure(data, WB, capture_id) is not None
    assert _outputs(data, WB) == []
    return cls, message


# --------------------------------------------------------------------------- #
# A-1 / A-2a / A-9 / A-11: dispositions, block map and record shape
# --------------------------------------------------------------------------- #


def test_a1_every_workbook_resource_is_dispositioned_on_evidence() -> None:
    """Detects a workbook left on the X container reason, a SILVER resource whose tender sheet is
    not routed to the record, or a hold without its TODO: question (I-1): 59 resources and their
    tender sheets are SILVER(workbook), TR123 and TR125 (resource and sheet) are HOLDs with the
    plan's reasons under this unit, TR121's empty ``Sheet1`` stays DOC, and nothing in either AS-5
    package still says a record is pending in AS-5."""
    document = _package_doc(FFR_PKG)
    resources = _workbook_resources(document)
    assert len(resources) == 61
    silver = 0
    for resource in resources:
        sheets = [c for c in resource["children"] if c["disposition"] != {"kind": "DOC"}]
        (sheet,) = sheets
        if resource["id"] in HOLD_REASONS:
            hold = {"kind": "HOLD", "reason": HOLD_REASONS[resource["id"]], "unit": UNIT}
            assert resource["disposition"] == hold
            assert sheet["disposition"] == hold
            continue
        silver += 1
        assert resource["disposition"] == {"kind": "SILVER", "key": WB}
        assert sheet["disposition"] == {"kind": "SILVER", "key": WB}
    assert silver == 59
    (tr121,) = [r for r in resources if r["id"].startswith("e692dc29")]
    assert {c["child"]: c["disposition"] for c in tr121["children"]}["Sheet1"] == {"kind": "DOC"}
    for slug in (FFR_PKG,):
        text = json.dumps(_package_doc(slug), ensure_ascii=False)
        assert "records pending in AS-5" not in text, slug
        assert "record pending in AS-5" not in text, slug


def test_a1_block_map_covers_exactly_the_silver_resources_and_their_sheets() -> None:
    """Detects a SILVER workbook without a block (its capture would fail, FM-6), a block for a held
    resource, or a block naming another sheet: the map's resource ids are exactly the SILVER
    workbooks and each block's sheet is that resource's SILVER child."""
    registry = load_registry()
    blocks = _record(WB).xlsx_blocks
    assert blocks is not None
    silver = {
        r.id: r
        for _key, (_p, r) in registry.resources.items()
        if r.disposition.kind == "SILVER" and getattr(r.disposition, "key", None) == WB
    }
    assert {b.resource_id for b in blocks} == set(silver)
    for block in blocks:
        (child,) = [c for c in silver[block.resource_id].children if c.disposition.kind == "SILVER"]
        assert child.child == block.sheet


def test_a1_epoch_columns_are_the_as5a_columns_by_name() -> None:
    """Detects a workbook column typed differently from the CSV record's column of the same name
    (the two tables must be comparable): every epoch column's dtype and nullability equal AS-5a's
    column of that name, every epoch is AS-5a's 43 names (A:AQ) or the 42 without ``post_code``,
    and no column carries a format, token or trim."""
    as5a = {c.name: c for epoch in _record(AS5A_FFR).epochs for c in epoch.columns}
    names43 = [c.name for c in max(_record(AS5A_FFR).epochs, key=lambda e: len(e.columns)).columns]
    names42 = [n for n in names43 if n != "post_code"]
    epochs = _record(WB).epochs
    assert len(epochs) == 16
    assert sorted(len(e.columns) for e in epochs) == [42] * 8 + [43] * 8
    for epoch in epochs:
        assert [c.name for c in epoch.columns] in (names42, names43)
        assert epoch.issue.kind == "none"
        for column in epoch.columns:
            reference = as5a[column.name]
            assert (column.dtype, column.nullable) == (reference.dtype, reference.nullable)
            assert column.format is None and column.trim is None and not column.null_tokens


def test_a2a_the_block_map_equals_the_atlas() -> None:
    """Detects a block shifted, shortened or retargeted against the measured atlas (FM-3, the
    accepted positional residual, is pinned here): the committed map equals the literal atlas
    table, in package order, three header rows each."""
    blocks = _record(WB).xlsx_blocks
    assert blocks is not None
    got = [
        (b.resource_id[:8], b.sheet, b.header_first, b.data_first, b.data_last, b.columns)
        for b in blocks
    ]
    assert got == ATLAS
    assert all(b.header_last == b.header_first + 2 for b in blocks)
    assert sum(b.data_last - b.data_first + 1 for b in blocks) == 10094


def test_a9_no_workbook_is_covered() -> None:
    """Detects a COVERED grant without the harness proof (I-9): no FFR resource or child is
    COVERED; the 16 vintage-passing nominations are SILVER workbooks."""
    document = _package_doc(FFR_PKG)
    text = json.dumps(document)
    assert '"COVERED"' not in text
    nominated = ("140", "142", "144", "145", "147", "148", "149", "152", "156", "157", "158")
    nominated += ("159", "161", "164", "165", "166")
    sheets = {b.sheet for b in _record(WB).xlsx_blocks or ()}
    assert {f"TR{n}" for n in nominated} <= sheets


def test_a11_the_workbook_record_is_eligible_and_its_package_stays_eligible() -> None:
    """Detects a hold or a per-record override on the workbook family: it inherits its eligible
    package, reads ``xlsx`` blocks only and selects the newest capture per resource."""
    package, family = load_registry().families[WB]
    record = _record(WB)
    assert record.eligibility is None
    assert effective_eligibility(package, family) == Eligible(status="eligible")
    assert package.eligibility == Eligible(status="eligible")
    assert (record.reader, record.xlsx, record.encoding) == ("xlsx", None, "utf-8")
    assert (record.latest, record.latest_partition) == ("whole_capture", "resource_id")
    assert record.entity_key == ("resource_id", "tender_ref")
    assert record.siblings == (WB_DIR,)
    assert family.transformer is None
    assert Held  # the archive records below are held


def test_a5_workbook_clocks_stay_raw_and_published_at_is_the_upload() -> None:
    """Detects an issue time, a typed date or window clock, or a vintage other than the upload's
    last_modified on the workbook record (I-5, K-2)."""
    record = _record(WB)
    assert not has_issue_time(record)
    assert record.vintage == "ckan_last_modified"
    assert record.temporal.kind == "none"
    for epoch in record.epochs:
        for column in epoch.columns:
            if column.name.startswith(("frame_from_", "frame_to_")) or column.name in (
                "start_date",
                "end_date",
            ):
                assert column.dtype == "string", column.name


# --------------------------------------------------------------------------- #
# A-2b / A-2c / A-3 / A-10: the engine over the lineage fixtures and tr129.xlsm
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name", LINEAGES)
def test_a2b_each_lineage_loads_exactly_its_tender_rows(
    name: str, data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects an example, notice or footer row loaded, a tender row lost, an excluded row, or a
    label tuple outside the committed epochs (I-2, I-3, FM-7): through the generated transformer
    each lineage loads its 3 tender rows with their stored references, no example or footer value,
    ``rows_excluded == 0``, ``published_at`` = the sidecar last_modified, no ``issue_time``."""
    body, lineage = lineage_workbook(name)
    _registry, generated = _install_lineage(monkeypatch, data, lineage)
    capture_id = _lineage_capture(data, body, lineage)
    assert generated.transformers[WB](data).run(DAY, run_id="r") == 3
    frame = _silver(data, WB)
    h = lineage["header_first"]
    refs = [lineage["cells"][f"A{h + offset}"][1] for offset in (5, 6, 7)]
    assert sorted(frame["tender_ref"].to_list()) == sorted(refs)
    assert set(frame["resource_id"].to_list()) == {lineage["resource_id"]}
    assert set(frame["child_id"].to_list()) == {lineage["sheet"]}
    strings = [c for c, t in frame.schema.items() if t == pl.Utf8]
    values = {v for c in strings for v in frame[c].drop_nulls().to_list()}
    assert {"Example Company", "Code", "Reason", "Rejection Codes"}.isdisjoint(values)
    assert "issue_time" not in frame.columns
    assert frame["published_at"].unique().to_list() == [datetime(2020, 3, 10, 10, 0, 0, 1, UTC)]
    completion = read_completion(data, WB, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    assert frame.schema["frame_from_mon_fri"] == pl.Utf8
    assert frame.schema["start_date"] == pl.Utf8
    assert frame.schema["availability_fee"] == pl.Float64


@pytest.mark.parametrize(
    ("change", "rule"),
    [
        ({"data_last": -1}, "(n)"),
        ({"data_last": 1}, "(l)"),
        ({"header_first": 1, "header_last": 1}, "(h)"),
    ],
    ids=["data-last-short", "data-last-long", "header-down"],
)
def test_a2c_a_changed_block_fails_the_capture_loud(
    change: dict[str, int], rule: str, data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a block map that disagrees with the sheet loading anything (FM-1, FM-2, FM-4): each
    change fails the whole capture with one failure record and no output."""
    body, lineage = lineage_workbook("tr122")
    h = lineage["header_first"]
    base = {"data_last": h + 7, "header_first": h, "header_last": h + 2}
    shifted = {key: base[key] + delta for key, delta in change.items()}
    _registry, generated = _install_lineage(monkeypatch, data, lineage, **shifted)
    capture_id = _lineage_capture(data, body, lineage)
    cls, message = _run_failing(generated, data, capture_id)
    assert cls == "XlsxBlockError" and rule in message, message


def test_a2c_an_unlisted_resource_fails_with_no_fallback(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a SILVER workbook read without a declared block (FM-6): removing the resource's
    block fails its capture by name."""
    body, lineage = lineage_workbook("tr122")
    document = copy.deepcopy(_package_doc(FFR_PKG))
    record = _family_doc(document, WB)["record"]
    record["xlsx_blocks"] = [
        b for b in record["xlsx_blocks"] if b["resource_id"] != lineage["resource_id"]
    ]
    _registry, generated = install_generated(monkeypatch, data / "_reg", [document])
    capture_id = _lineage_capture(data, body, lineage)
    cls, message = _run_failing(generated, data, capture_id)
    assert cls == "XlsxBlockError" and "has no declared xlsx block" in message


def _edited(name: str, row_offset: int, column: str, cell: Cell) -> tuple[bytes, dict[str, Any]]:
    _body, lineage = lineage_workbook(name)
    cells = lineage_cells(lineage)
    cells[f"{column}{lineage['header_first'] + row_offset}"] = cell
    return lineage_workbook(name, cells=cells)


def test_a3a_stored_numbers_cast_from_their_stored_text(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a numeric cell rendered through a display format before the cast (I-3): TR122's
    reference ``122.16500000000073`` is the silver ``tender_ref`` string exactly and an
    ``availability_fee`` stored as ``850.5949936745257`` casts to that float."""
    body, lineage = _edited("tr122", 5, "U", ("n", "850.5949936745257"))
    _registry, generated = _install_lineage(monkeypatch, data, lineage)
    _lineage_capture(data, body, lineage)
    generated.transformers[WB](data).run(DAY, run_id="r")
    frame = _silver(data, WB)
    assert "122.16500000000073" in frame["tender_ref"].to_list()
    fee = frame.filter(pl.col("tender_ref") == lineage["cells"]["A7"][1])["availability_fee"]
    assert fee.to_list() == [850.5949936745257]


def test_a3b_one_bad_number_fails_the_whole_capture(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a partial load around one bad cell (FM-8, why TR123 is held): a ``14,4`` text cell
    in ``high_frequency_response_0_2hz`` fails the capture with no output."""
    body, lineage = _edited("tr122", 6, "AH", ("s", "14,4"))
    _registry, generated = _install_lineage(monkeypatch, data, lineage)
    capture_id = _lineage_capture(data, body, lineage)
    cls, _message = _run_failing(generated, data, capture_id)
    assert cls == "InvalidOperationError"


def test_a10_the_real_macro_workbook_labelled_xlsx_loads_its_156_rows(data: Path) -> None:
    """Detects a body read by its catalogue label or suffix (I-10): the committed real TR129 body
    (``.xlsm`` with ``xl/vbaProject.bin``, catalogue ``XLSX``) loads exactly its 156 tender rows
    under the committed record and block, with zero exclusions."""
    path, _sidecar = write_capture(
        data,
        WB_DIR,
        package_slug=FFR_PKG,
        package_id=FFR_PKG_ID,
        resource_id=TR129,
        resource_name="Post Tender Report TR129 September 2020",
        body=(FIXTURES / "containers" / "tr129.xlsm").read_bytes(),
        written_at=datetime(2026, 10, 8, 11, 1, 54, tzinfo=UTC),
        ckan_last_modified="2020-10-21T07:51:24.169039",
        resource_filename="post-tender-report-tr129-september-2020-ext.xlsm",
        ckan_format="XLSX",
        extension="xlsm",
        partition=DAY,
    )
    assert get_transformer(SOURCE, WB, data).run(DAY, run_id="r") == 156
    frame = _silver(data, WB)
    assert frame.height == 156 and frame["tender_ref"].n_unique() == 156
    completion = read_completion(data, WB, capture_id_for(path, data))
    assert completion is not None and completion["rows_excluded"] == 0
