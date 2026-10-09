"""The strict UTF-8 gate before the NESO CSV pre-parse (v0.22-K-IC-2H, ADR-040 §Amendment 1).

A body that is not valid in its record's declared encoding fails with ``UnicodeDecodeError``
whatever the bad byte's position (a data row, the header, beyond the pre-parse's reach, a
truncated tail, behind a BOM, inside a markup body), and the validation holds at most one
bounded chunk, never a decoded copy of the whole body. A valid body is handed on as the same
object, so the success path is byte-unchanged.
"""

from __future__ import annotations

import random
import tracemalloc
from typing import TYPE_CHECKING

import pytest

from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord
from gridflow.silver.csv_bronze import NotCsvBodyError, read_csv_bronze_body
from gridflow.silver.neso_data_portal import readers
from gridflow.silver.neso_data_portal.readers import read_csv_body

if TYPE_CHECKING:
    from pathlib import Path

BOM = b"\xef\xbb\xbf"
HEADER = ("Operational Date", "Flow To GB", "Flow From GB", "Reason for restriction")
HEAD = ",".join(HEADER).encode() + b"\r\n"
DATA_ROW = HEAD + b"20241018 20:00-21:00,1060,1060,\r\n20241018 21:00-22:00,1060,\xa00,\r\n"
CHUNK_SIZES = (1, 2, 3, 7, 1 << 20)
ALPHABET = (
    b"a",
    b",",
    b"\r\n",
    b"\xa0",
    b"\xc2",
    b"\xc2\xa0",
    b"\xe2\x82\xac",
    b"\xe2\x82",
    b"\xe2",
    b"\x82",
    b"\xf0\x9f\x98\x80",
    b"\xf0\x9f",
    b"\xed\xa0\x80",
    b"\xef\xbb\xbf",
    b"\xff",
    b"\xc0\xaf",
    b"\xf4\x90\x80\x80",
)
"""Tokens whose concatenations cover valid multi-byte text, lone continuation and lead bytes,
truncated sequences, surrogates, overlongs, out-of-range code points and the BOM."""


def _record(encoding: str = "utf-8", header: tuple[str, ...] = HEADER) -> SchemaRecord:
    """A one-epoch all-string record (the shape of ``TestCsvMemberParity``)."""
    return SchemaRecord.model_validate(
        {
            "version": "1",
            "reader": "csv",
            "encoding": encoding,
            "epochs": [
                {
                    "header": list(header),
                    "columns": [
                        {"source": h, "name": f"c{i}", "dtype": "string", "nullable": True}
                        for i, h in enumerate(header)
                    ],
                    "issue": {"kind": "none"},
                }
            ],
            "temporal": {"kind": "none"},
            "entity_key": ["c0"],
            "latest": "key_latest",
            "vintage": "capture_fallback",
        }
    )


def _decode_error(raw: bytes) -> UnicodeDecodeError:
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return exc
    raise AssertionError("the body is valid UTF-8")


def _large(bad: bool) -> bytes:
    """A body over 6 MiB (beyond the pre-parse's reach, E4) whose last row may carry ``0xA0``."""
    rows = [HEAD]
    size = len(HEAD)
    index = 0
    while size < 6 * (1 << 20) + 1024:
        row = f"L{index:08d},1060,1050,\r\n".encode()
        rows.append(row)
        size += len(row)
        index += 1
    rows.append(b"LAST,1060,\xa00,\r\n" if bad else b"LAST,1060,0,\r\n")
    return b"".join(rows)


INVALID: dict[str, tuple[bytes, str]] = {
    "data_row": (DATA_ROW, "utf-8"),
    "header": (HEAD.replace(b"Reason for restriction", b"Reason for\xa0restriction"), "utf-8"),
    "large": (_large(bad=True), "utf-8"),
    "truncated": (HEAD + b"20241018 21:00-22:00,1060,0,\xe2\x82", "utf-8"),
    "bom_utf8": (BOM + DATA_ROW, "utf-8"),
    "bom_utf8_sig": (BOM + DATA_ROW, "utf-8-sig"),
    "html_bad": (b"<html>\xa0</html>", "utf-8"),
}


def _parity_bodies() -> list[bytes]:
    generator = random.Random(1)
    bodies = [
        b"".join(generator.choice(ALPHABET) for _ in range(generator.randint(0, 14)))
        for _ in range(600)
    ]
    named = [
        b"ab\xe2\x82\xacd" + b"\xe2\x82",  # truncated tail
        b"abc\xe2\x82\xac\xe2\x82\xac\xf0\x9f\x98\x80",  # sequences split across small chunks
        BOM + b"a,b\r\n\xa0",  # BOM + bad byte
        b"a\xed\xa0\x80b",  # surrogate
        b"\xe2\x82x",  # a split sequence broken by an ASCII byte
    ]
    return bodies + named


@pytest.mark.parametrize("chunk", CHUNK_SIZES)
def test_validator_matches_bytes_decode(monkeypatch: pytest.MonkeyPatch, chunk: int) -> None:
    """T-ENC-1. Detects a chunked validator that disagrees with a whole-body decode: a
    sequence split across a chunk boundary rejected, a dangling tail accepted, or an error
    position relative to the chunk instead of the body. Each body raises iff
    ``bytes.decode`` raises, with the same message, and the exception holds the body itself."""
    monkeypatch.setattr(readers, "_UTF8_CHUNK", chunk)
    invalid = 0
    for raw in _parity_bodies():
        try:
            raw.decode("utf-8")
        except UnicodeDecodeError as expected:
            invalid += 1
            with pytest.raises(UnicodeDecodeError) as info:
                readers._validate_utf8(raw)
            assert str(info.value) == str(expected), raw
            assert info.value.object is raw
        else:
            assert readers._validate_utf8(raw) is None, raw
    assert invalid >= 100


def test_multibyte_characters_straddling_every_boundary_validate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-ENC-2. Detects a validator that does not carry a partial sequence across chunks: a
    valid body of 2-, 3- and 4-byte characters at every offset returns ``None`` at chunk 3."""
    monkeypatch.setattr(readers, "_UTF8_CHUNK", 3)
    text = "a£€😀" * 50 + "£😀€b" * 50
    for shift in range(4):
        assert readers._validate_utf8(("a" * shift + text).encode("utf-8")) is None


def test_validation_holds_at_most_a_bounded_chunk() -> None:
    """T-ENC-3 (I-2a). Detects a whole-body decode or copy in the gate: over a 32 MiB valid
    body the traced peak stays within 4 MiB (a whole decode costs 32 MiB)."""
    raw = b"20241018 21:00-22:00,1060,1050,\r\n" * ((32 << 20) // 33)
    tracemalloc.start()
    try:
        readers._validate_utf8(raw)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak <= 4 << 20, peak


@pytest.mark.parametrize("case", list(INVALID))
def test_every_invalid_body_fails_with_unicode_decode_error(tmp_path: Path, case: str) -> None:
    """T-ENC-4. Detects a failure class that depends on where the bad byte sits (the pre-parse's
    ``ComputeError`` for a data row, the bronze reader's ``NotCsvBodyError`` for a header,
    large, BOM or markup body): every invalid body raises exactly ``UnicodeDecodeError`` with
    ``bytes.decode``'s message."""
    raw, encoding = INVALID[case]
    path = tmp_path / "b.csv"
    path.write_bytes(raw)
    with pytest.raises(UnicodeDecodeError) as info:
        list(read_csv_body(path, _record(encoding), ()))
    assert type(info.value) is UnicodeDecodeError
    assert str(info.value) == str(_decode_error(raw))


@pytest.mark.parametrize("raw", [b"<html/>", b"\r\n"], ids=["html_ok", "empty"])
def test_valid_markup_and_empty_bodies_keep_not_csv_body_error(tmp_path: Path, raw: bytes) -> None:
    """T-ENC-4 controls. Detects the gate swallowing the bronze reader's markup and empty-body
    rules: a valid ``<`` body and a blank body still raise ``NotCsvBodyError``."""
    path = tmp_path / "b.csv"
    path.write_bytes(raw)
    with pytest.raises(NotCsvBodyError):
        list(read_csv_body(path, _record(), ()))


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
@pytest.mark.parametrize("bom", [False, True], ids=["plain", "bom"])
def test_a_valid_body_passes_through_uncopied(tmp_path: Path, encoding: str, bom: bool) -> None:
    """T-ENC-5 (I-1b). Detects the gate copying or altering a valid body: the UTF-8-alias path
    returns the identical object, and the frame equals the bronze reader's on the same bytes."""
    raw = (BOM if bom else b"") + HEAD + "20241018 21:00-22:00,1060,£0,\r\n".encode()
    record = _record(encoding)
    assert readers._utf8_body(raw, record) is raw
    path = tmp_path / "b.csv"
    path.write_bytes(raw)
    (table,) = read_csv_body(path, record, ())
    expected = read_csv_bronze_body(raw, expected_columns=HEADER, source_label=str(path))
    assert table.header == HEADER
    assert table.frame.equals(expected)


@pytest.mark.parametrize("case", ["data_row", "header", "large"])
def test_a_csv_member_fails_exactly_as_a_csv_body(tmp_path: Path, case: str) -> None:
    """T-ENC-6. Detects a ZIP CSV member read without the gate: the member path raises the
    same ``UnicodeDecodeError`` as the body path on the same bytes."""
    raw, encoding = INVALID[case]
    record = _record(encoding)
    path = tmp_path / "b.csv"
    path.write_bytes(raw)
    with pytest.raises(UnicodeDecodeError) as body_info:
        list(read_csv_body(path, record, ()))
    with pytest.raises(UnicodeDecodeError) as member_info:
        readers._csv_member_table(raw, record, "member")
    assert type(member_info.value) is UnicodeDecodeError
    assert str(member_info.value) == str(body_info.value)


def test_a_non_utf8_record_keeps_the_strict_transcode(tmp_path: Path) -> None:
    """T-ENC-7. Detects the transcode for a declared non-UTF-8 encoding lost or loosened: a
    ``cp1252`` record reads the ``0xA0`` body with a no-break space in the cell, and a byte
    ``cp1252`` does not define raises ``UnicodeDecodeError``."""
    path = tmp_path / "b.csv"
    path.write_bytes(DATA_ROW)
    (table,) = read_csv_body(path, _record("cp1252"), ())
    assert table.frame[HEADER[2]].to_list() == ["1060", " 0"]
    path.write_bytes(DATA_ROW.replace(b"\xa0", b"\x81"))
    with pytest.raises(UnicodeDecodeError):
        list(read_csv_body(path, _record("cp1252"), ()))
