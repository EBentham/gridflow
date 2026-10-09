"""Adjudicated reconcile gaps (v0.22-GEN-2H, ADR-040).

Every test builds a synthetic resource-partitioned family on a short tmp data root through
the multi-resource helpers (``install_multi`` / ``capture_multi``), so nothing reads or writes
``C:/gridflow-data``. The CLI is driven in process through ``main`` with ``GRIDFLOW_DATA_DIR``
pointed at the tmp root.

**I-1 (byte-unchanged).** :func:`w0_outputs` renders world W0 (two overlap gaps, a
duplicate-key failure, a missing later capture and a ghost completion) through
``reconcile``, the CLI and ``drain``. ``tests/fixtures/neso_data_portal/gen2h/
reconcile_base_pin.json`` was written from it on the untouched base (master ``0f77c85``),
before any ``src/`` edit of the unit; with no adjudication entry the output must stay equal.
"""

from __future__ import annotations

import contextlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from test_neso_multi_resource import DAY, HEADER, KEY, capture_multi, install_multi

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.reconcile import main
from gridflow.connectors.neso_data_portal.registry import (
    RECONCILE_ADJUDICATIONS_FILE,
    RegistryError,
)
from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    completion_row,
    record_completion,
)
from gridflow.silver.neso_data_portal.reconcile import drain, reconcile

PIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "neso_data_portal"
    / "gen2h"
    / "reconcile_base_pin.json"
)
CUTOFF = DAY.isoformat()
GHOST = f"bronze/neso_data_portal/{KEY}/2026/10/07/raw_ghost.csv"


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 7, hour, minute, tzinfo=UTC)


def point_settings(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point ``load_settings`` (and so the CLI) at ``data``; no gold views."""
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(data / "cat.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(data / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    pipeline_runner.import_transformers()


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A short data root the settings (and so the CLI) point at."""
    root = tmp_path_factory.mktemp("j")
    point_settings(root, monkeypatch)
    return root


def run_cli(*args: str) -> tuple[int, list[str]]:
    """``main(args)`` with stdout captured; returns ``(exit code, lines)``."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = main(list(args))
    return code, buffer.getvalue().splitlines()


def build_w0(data: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """World W0 on the base: every gap category a synthetic family can show.

    - A = (P1, P2) and B = (P2), transformed: two ``overlap`` gaps;
    - C repeats one pair: ``failed`` (``DuplicateEntityKeyError``), drainable;
    - a later capture of A written after the run: ``missing``, drainable;
    - one ghost completion: ``orphaned`` (a).

    Every name and timestamp is fixed, so every line is deterministic.

    Returns:
        The capture ids by role.
    """
    generated = install_multi(monkeypatch, data)
    a = capture_multi(data, "A", HEADER + b"2026-10-07,1,1.0\n2026-10-07,2,2.0\n", _t(8))
    b = capture_multi(data, "B", HEADER + b"2026-10-07,2,5.0\n", _t(9))
    c = capture_multi(data, "C", HEADER + b"2026-10-07,3,1.0\n2026-10-07,3,2.0\n", _t(9, 30))
    with contextlib.suppress(NesoCaptureFailedError):
        generated.transformers[KEY](data).run(DAY, run_id="r")
    later = capture_multi(data, "A", HEADER + b"2026-10-07,1,7.0\n", _t(10))
    record_completion(
        data,
        completion_row(
            family=KEY,
            capture_id=GHOST,
            source_key=KEY,
            partition_date=DAY,
            resource_id="eeeeeeee-0000-4000-8000-00000000000a",
            body_sha256="0" * 64,
            capture_written_at=_t(7),
            published_at=_t(7),
            outcome="valid_empty",
            row_count=0,
            rows_excluded=0,
            output_path=None,
            children=[],
            versions=generated.transformers[KEY].versions(),
        ),
    )
    return {"a": a, "b": b, "c": c, "later": later}


def w0_outputs(data: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Build W0, then render ``reconcile`` (API and CLI) and ``drain``; the pin's shape."""
    build_w0(data, monkeypatch)
    loaded = registry_module.load_registry()
    api = reconcile(data, loaded, [KEY], DAY)
    code, lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert lines == api.lines()
    drained = drain(data, loaded, [KEY], DAY, lambda: None)
    return {
        "reconcile": {"exit": code, "lines": lines},
        "drain": {"lines": drained.lines()},
    }


def test_i1_reconcile_output_is_byte_unchanged_without_entries(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects any change to reconcile's or drain's lines, or to the CLI exit code, for a
    family no adjudication entry names (I-1 (a)): W0 against the pin written on the base."""
    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    assert w0_outputs(data, monkeypatch) == pin


# --------------------------------------------------------------------------- #
# Classification (A1-A4, T-ADJ-*)
# --------------------------------------------------------------------------- #

TWO = HEADER + b"2026-10-07,1,1.0\n2026-10-07,2,2.0\n"
DUP = HEADER + b"2026-10-07,3,1.0\n2026-10-07,3,2.0\n"
REASON = "two archives publish one key with conflicting values"
QUESTION = "Which archive is authoritative for the shared key?"


def write_ledger(data: Path, entries: list[dict[str, Any]]) -> None:
    """Rewrite the installed registry's reconcile adjudication ledger (read per call)."""
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(entries), encoding="utf-8"
    )


def entry(category: str, *captures: str, **fields: Any) -> dict[str, Any]:
    """One ledger entry on family ``multi``; ``failed`` gets the duplicate-guard cause."""
    document: dict[str, Any] = {
        "family": KEY,
        "category": category,
        "captures": list(captures),
        "reason": REASON,
        "question": QUESTION,
        "evidence": "synthetic",
        "ruling": "547",
    }
    if category == "failed":
        document["cause"] = "DuplicateEntityKeyError"
    document.update(fields)
    return document


def _run(generated: Any, data: Path) -> None:
    with contextlib.suppress(NesoCaptureFailedError):
        generated.transformers[KEY](data).run(DAY, run_id="r")


def _report(data: Path) -> Any:
    return reconcile(data, registry_module.load_registry(), [KEY], DAY)


def _gap_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if line.startswith("GAP ")]


def _adjudicated_lines(lines: list[str]) -> list[str]:
    return [line for line in lines if line.startswith("ADJUDICATED ")]


def _overlap_world(data: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Any, str, str]:
    generated = install_multi(monkeypatch, data)
    a = capture_multi(data, "A", TWO, _t(8))
    b = capture_multi(data, "B", HEADER + b"2026-10-07,2,5.0\n", _t(9))
    _run(generated, data)
    return generated, a, b


def test_a1_an_adjudicated_overlap_exits_0_and_is_listed(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A1: detects an adjudicated overlap still failing the run, or passing silently: with
    the entry the CLI exits 0, prints no ``GAP`` line and lists both captures as
    ``ADJUDICATED`` with ruling, reason and question; ``passed`` but never ``clean``.
    Without the entry the same world exits 1 with two ``GAP overlap`` lines."""
    _generated, a, b = _overlap_world(data, monkeypatch)
    code, lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert code == 1
    assert [line.split()[1] for line in _gap_lines(lines)] == ["overlap", "overlap"]
    assert not any(line.startswith("SUMMARY adjudicated") for line in lines)

    write_ledger(data, [entry("overlap", a, b)])
    code, lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert code == 0, lines
    assert _gap_lines(lines) == []
    adjudicated = _adjudicated_lines(lines)
    assert [line.split()[1:3] for line in adjudicated] == [["overlap", KEY]] * 2
    assert {line.split()[4] for line in adjudicated} == {a, b}
    for line in adjudicated:
        assert line.endswith(f"[ruling 547; reason: {REASON}; question: {QUESTION}]"), line
    assert "SUMMARY adjudicated 2" in lines
    assert "SUMMARY stale_adjudication 0" in lines
    assert "SUMMARY overlap 0" in lines
    assert "SUMMARY families=1 skipped=0 gaps=0" in lines
    report = _report(data)
    assert report.passed and not report.clean
    assert report.gaps == ()


def test_a2_an_entry_for_x_does_not_cover_y(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A2: detects an entry covering every gap of its category: two duplicate-key failures
    on two resources, an entry for X only; Y stays an open ``GAP failed`` and the run exits 1."""
    generated = install_multi(monkeypatch, data)
    x = capture_multi(data, "A", DUP, _t(8))
    y = capture_multi(data, "B", DUP, _t(8))
    _run(generated, data)
    write_ledger(data, [entry("failed", x)])
    code, lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert code == 1
    assert [line.split()[4] for line in _adjudicated_lines(lines)] == [x]
    gaps = _gap_lines(lines)
    assert len(gaps) == 1 and gaps[0].startswith(f"GAP failed {KEY} 2026-10-07 {y} ")


def test_a3_a_stale_entry_exits_nonzero(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A3: detects an adjudication that outlives its gap (the vendor fixed the data): an
    entry naming a capture that loads cleanly is itself a ``stale_adjudication`` gap."""
    generated = install_multi(monkeypatch, data)
    clean = capture_multi(data, "A", TWO, _t(8))
    _run(generated, data)
    write_ledger(data, [entry("failed", clean)])
    code, lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert code == 1
    assert _gap_lines(lines) == [
        f"GAP stale_adjudication {KEY} 2026-10-07 {clean} ruling 547: no live failed gap "
        "on this capture matches the entry"
    ]
    assert "SUMMARY stale_adjudication 1" in lines and "SUMMARY adjudicated 0" in lines
    assert _report(data).gaps[0].drainable is False


@pytest.mark.parametrize("broken", ["malformed", "missing"])
def test_a4_a_malformed_or_missing_ledger_exits_2(
    data: Path, monkeypatch: pytest.MonkeyPatch, broken: str
) -> None:
    """A4 / T-REG-2 at the CLI: detects a ledger error read as no entries (a pass): a
    non-adjudicable category, or no ledger file, is a usage error, exit 2, no report."""
    _generated, a, b = _overlap_world(data, monkeypatch)
    ledger = data / "_registry" / RECONCILE_ADJUDICATIONS_FILE
    if broken == "missing":
        ledger.unlink()
    else:
        write_ledger(data, [entry("missing", a, b)])
    buffer = io.StringIO()
    errors = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(errors):
        code = main([KEY, "--cutoff", CUTOFF])
    assert code == 2
    assert buffer.getvalue() == ""
    assert RECONCILE_ADJUDICATIONS_FILE in errors.getvalue()


def test_a4_a_ledger_entry_the_registry_does_not_back_exits_2(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects reconcile running on a ledger whose capture names a foreign resource."""
    _generated, a, _b = _overlap_world(data, monkeypatch)
    foreign = a.replace(
        "eeeeeeee-0000-4000-8000-00000000000a", "ffffffff-0000-4000-8000-0000000000ff"
    )
    write_ledger(data, [entry("failed", foreign)])
    code, _lines = run_cli(KEY, "--cutoff", CUTOFF)
    assert code == 2
    with pytest.raises(RegistryError, match="not in package"):
        _report(data)


def test_t_adj_3_a_partly_stale_entry_keeps_its_live_capture(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-ADJ-3 (FM-9): detects an entry going wholly stale, or wholly live, when one of its
    captures clears: X stays adjudicated, Y alone is ``stale_adjudication``."""
    generated = install_multi(monkeypatch, data)
    x = capture_multi(data, "A", DUP, _t(8))
    y = capture_multi(data, "B", TWO, _t(8))
    _run(generated, data)
    write_ledger(data, [entry("failed", x, y)])
    report = _report(data)
    assert [(g.gap.category, g.gap.capture_id) for g in report.adjudicated] == [("failed", x)]
    assert [(g.category, g.capture_id) for g in report.gaps] == [("stale_adjudication", y)]


def test_t_adj_4_another_cause_stays_open_and_the_entry_goes_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-ADJ-4 (FM-4): detects a ``failed`` entry covering a failure of another cause: a
    body whose every row is excluded fails with ``AllRowsExcludedError``; its gap stays
    open and the entry is reported stale."""
    generated = install_multi(monkeypatch, data)
    x = capture_multi(data, "A", HEADER + b"2026-10-07,99,1.0\n2026-10-07,98,2.0\n", _t(8))
    _run(generated, data)
    write_ledger(data, [entry("failed", x)])
    report = _report(data)
    assert report.adjudicated == ()
    assert [(g.category, g.capture_id) for g in report.gaps] == [
        ("failed", x),
        ("stale_adjudication", x),
    ]
    assert report.gaps[0].cause == "AllRowsExcludedError"


def test_t_adj_5_out_of_scope_entries_neither_cover_nor_go_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-ADJ-5 (FM-8): detects an entry acting outside the run's scope: reconciling another
    family, or a cutoff before the entry's capture date, is byte-equal to no ledger."""
    from _neso_generic_support import install_generated
    from _neso_registry_support import family, package, record, resource
    from test_neso_multi_resource import PKG, RESOURCES, partitioned_record

    entries = [resource(rid, name, KEY) for rid, name, _f in RESOURCES.values()]
    entries.append(resource("eeeeeeee-0000-4000-8000-0000000000d0", "Other", "other"))
    document = package(
        "pkg-multi",
        PKG,
        [
            family(KEY, record=partitioned_record(), empty_allowed=True),
            family("other", record=record()),
        ],
        entries,
    )
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    x = capture_multi(data, "A", DUP, _t(8))
    _run(generated, data)
    loaded = registry_module.load_registry()
    before = DAY.replace(day=6)
    other = reconcile(data, loaded, ["other"], DAY).lines()
    early = reconcile(data, loaded, [KEY], before).lines()
    write_ledger(data, [entry("failed", x)])
    assert reconcile(data, loaded, ["other"], DAY).lines() == other
    assert reconcile(data, loaded, [KEY], before).lines() == early
    assert reconcile(data, loaded, [KEY], DAY).passed


def test_t_adj_6_a_changed_overlap_reopens(data: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T-ADJ-6 (FM-2, FM-5): detects an overlap entry covering a different overlap.
    (b) A third resource C overlapping A: A's gap (peer C) and C's stay open and the entry
    goes stale on A; B's gap (peer A only) stays adjudicated.
    (a) A newer B capture is then selected: A, B_new and C are open overlaps and the entry
    is stale on both A and B_old."""
    generated, a, b_old = _overlap_world(data, monkeypatch)
    write_ledger(data, [entry("overlap", a, b_old)])
    assert _report(data).passed

    c = capture_multi(data, "C", HEADER + b"2026-10-07,1,4.0\n", _t(9, 30))
    _run(generated, data)
    report = _report(data)
    assert sorted((g.category, g.capture_id) for g in report.gaps) == sorted(
        [("overlap", a), ("overlap", c), ("stale_adjudication", a)]
    )
    assert [g.gap.capture_id for g in report.adjudicated] == [b_old]
    assert {g.capture_id: g.peers for g in report.gaps if g.category == "overlap"}[a] == tuple(
        sorted([b_old, c])
    )

    b_new = capture_multi(data, "B", HEADER + b"2026-10-07,2,6.0\n", _t(11))
    _run(generated, data)
    report = _report(data)
    assert sorted((g.category, g.capture_id) for g in report.gaps) == sorted(
        [
            ("overlap", a),
            ("overlap", b_new),
            ("overlap", c),
            ("stale_adjudication", a),
            ("stale_adjudication", b_old),
        ]
    )
    assert report.adjudicated == ()


def test_t_adj_7_drain_never_reruns_an_adjudicated_failure(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-ADJ-7 (FM-10, FM-11): detects the drain re-running an adjudicated failed capture
    (rewriting its failure record) or dropping the adjudicated bucket from its report."""
    from gridflow.silver.neso_data_portal.completion import failure_path

    generated = install_multi(monkeypatch, data)
    x = capture_multi(data, "A", DUP, _t(8))
    _run(generated, data)
    capture_multi(data, "B", TWO, _t(9))
    record_path = failure_path(data, KEY, x)
    stamp = (record_path.read_bytes(), record_path.stat().st_mtime_ns)
    write_ledger(data, [entry("failed", x)])
    after = drain(data, registry_module.load_registry(), [KEY], DAY, lambda: None)
    assert after.drained == ((KEY, DAY, 1),)
    assert (record_path.read_bytes(), record_path.stat().st_mtime_ns) == stamp
    assert [g.gap.capture_id for g in after.adjudicated] == [x]
    assert after.gaps == ()
