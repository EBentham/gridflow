"""A capture stranded by an aborted pipeline is recovered by the drain alone (ADR-034 T-B6-3).

Day 1: F1 captures while F2's ingest fails, so the real ``gridflow pipeline``
exits before transform and F1's new capture is never transformed. Days 2-4 F1
is unchanged, so the day-4 ``--last 24h`` refresh never revisits day 1. Only
reconcile sees the gap; the drain recovers exactly that capture, leaves the
day's already-recorded sibling capture untouched, and a repeat drain is a no-op.
Ingest is mocked; everything else (CLI, transform, ledger, catalogue) is real,
over a tmp data root.
"""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "unit"))

from _neso_generic_support import install_generated, write_capture  # noqa: E402
from _neso_registry_support import family, package, record, resource  # noqa: E402

from gridflow import cli as cli_module  # noqa: E402
from gridflow.cli import app  # noqa: E402
from gridflow.connectors.neso_data_portal.reconcile import main as reconcile_main  # noqa: E402
from gridflow.pipeline import runner as pipeline_runner  # noqa: E402
from gridflow.pipeline.runner import DatasetResult  # noqa: E402
from gridflow.silver.neso_data_portal.completion import read_completion  # noqa: E402

SOURCE = "neso_data_portal"
PKG = "dddddddd-0000-4000-8000-000000000000"
HEADER = b"SettlementDate,SettlementPeriod,Unit,Value\n"
FAMILIES = ("fam_one", "fam_two")


def _rid(index: int) -> str:
    return f"dddddddd-0000-4000-8000-{index:012d}"


def _capture(data: Path, day: date, hour: int, value: str) -> str:
    written = datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
    body = HEADER + f"{day.isoformat()},1,A,{value}\n".encode()
    path, _sidecar = write_capture(
        data,
        "fam_one",
        package_slug="pkg-gen",
        package_id=PKG,
        resource_id=_rid(1),
        resource_name="Fam_One",
        body=body,
        written_at=written,
        ckan_last_modified=written.replace(tzinfo=None).isoformat(),
        partition=day,
    )
    return path.relative_to(data).as_posix()


def _ingest(results: dict[str, str], on_call: Any = None) -> Any:
    def _run_ingest(
        ctx: Any, source: str, datasets: list[str], start: Any, end: Any, **kwargs: Any
    ) -> list[DatasetResult]:
        if on_call is not None:
            on_call()
        return [
            DatasetResult(
                source=source,
                dataset=ds,
                operation="ingest",
                status=results[ds],
                error="mocked ingest failure" if results[ds] == "failed" else None,
            )
            for ds in datasets
        ]

    return _run_ingest


def _state(data: Path) -> dict[str, Any]:
    return {
        "ledger": {
            p.relative_to(data).as_posix(): p.read_bytes()
            for p in (data / "state").rglob("*.parquet")
        },
        "outputs": {
            p.relative_to(data).as_posix(): p.stat().st_mtime_ns
            for p in (data / "silver").rglob("*.parquet")
        },
    }


@pytest.mark.integration
def test_t_b6_3_a_stranded_capture_is_recovered_by_the_drain_only(
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = tmp_path_factory.mktemp("t")
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(data / "cat.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(data / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    pipeline_runner.import_transformers()
    document = package(
        "pkg-gen",
        PKG,
        [family(key, record=record()) for key in FAMILIES],
        [resource(_rid(i), key.title(), key) for i, key in enumerate(FAMILIES, start=1)],
    )
    _registry, generated = install_generated(monkeypatch, data / "_registry", [document])
    monkeypatch.setattr(cli_module, "_resolve_datasets", lambda *args: list(FAMILIES))
    today = datetime.now(UTC).date()
    day1 = today - timedelta(days=3)

    sibling = _capture(data, day1, 8, "1")
    generated.transformers["fam_one"](data).run(day1, run_id="day1-morning")
    assert read_completion(data, "fam_one", sibling) is not None

    stranded: list[str] = []
    monkeypatch.setattr(
        pipeline_runner,
        "run_ingest",
        _ingest(
            {"fam_one": "success", "fam_two": "failed"},
            lambda: stranded.append(_capture(data, day1, 12, "2")),
        ),
    )
    day1_run = CliRunner().invoke(
        app,
        ["pipeline", SOURCE, "--start", day1.isoformat(), "--end", day1.isoformat()],
    )
    assert day1_run.exit_code == 1, day1_run.output
    assert "Ingestion failed" in day1_run.output
    (capture,) = stranded
    assert read_completion(data, "fam_one", capture) is None

    monkeypatch.setattr(
        pipeline_runner, "run_ingest", _ingest({"fam_one": "success", "fam_two": "success"})
    )
    day4_run = CliRunner().invoke(app, ["pipeline", SOURCE, "--last", "24h"])
    assert day4_run.exit_code == 0, day4_run.output
    assert read_completion(data, "fam_one", capture) is None

    cutoff = today.isoformat()
    assert reconcile_main(["--all", "--cutoff", cutoff]) == 1
    gaps = [line for line in capsys.readouterr().out.splitlines() if line.startswith("GAP ")]
    assert gaps == [f"GAP missing fam_one {day1.isoformat()} {capture} no completion"]

    sibling_ledger = (data / "state").rglob("*.parquet")
    before = {p: p.read_bytes() for p in sibling_ledger}
    sibling_output = read_completion(data, "fam_one", sibling)
    assert sibling_output is not None
    sibling_mtime = (data / sibling_output["output_path"]).stat().st_mtime_ns

    assert reconcile_main(["--all", "--cutoff", cutoff, "--drain"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert f"SUMMARY drained fam_one {day1.isoformat()} 1" in out
    assert read_completion(data, "fam_one", capture) is not None
    assert (data / sibling_output["output_path"]).stat().st_mtime_ns == sibling_mtime
    for path, content in before.items():
        assert path.read_bytes() == content

    settled = _state(data)
    assert reconcile_main(["--all", "--cutoff", cutoff, "--drain"]) == 0
    assert not [
        line for line in capsys.readouterr().out.splitlines() if line.startswith("SUMMARY drained")
    ]
    assert _state(data) == settled
