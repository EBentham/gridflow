"""``gridflow backfill --preset``: a named or file preset of datasets over one window.

The runner is faked, so nothing here fetches: each test checks what the CLI
hands ``runner.run_backfill`` per source, and what it prints for a source whose
API key is unset.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.config.settings import load_settings
from gridflow.pipeline import runner

if TYPE_CHECKING:
    from conftest import IsolatedPaths

cli = CliRunner()
NOTEBOOKS_DIR = Path(__file__).resolve().parents[2] / "notebooks"
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_SILVER_VIEW = re.compile(r"\"(silver_\w+)\"")
# A gold view the notebooks read, and the silver dataset it is built from.
_GOLD_INPUTS = {"gold_eu_gas_storage": ("gie_agsi", "storage")}


@pytest.fixture
def backfills(isolated_env: IsolatedPaths, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Every ``run_backfill`` call as ``(source, datasets, start, end, chunk_days)``."""
    calls: list[Any] = []

    def _fake(settings, source, datasets, start_dt, end_dt, *, chunk_days, on_chunk):
        calls.append((source, datasets, start_dt, end_dt, chunk_days))
        return runner.RunReport()

    monkeypatch.setattr(runner, "run_backfill", _fake)
    return calls


def _notebook_datasets() -> set[tuple[str, str]]:
    """The ``(source, dataset)`` pairs behind every view the data-layer notebooks read."""
    sources = load_settings().sources
    pairs = set()
    for path in NOTEBOOKS_DIR.glob("[0-9][0-9]_*.py"):
        text = path.read_text(encoding="utf-8")
        pairs |= {pair for view, pair in _GOLD_INPUTS.items() if f'"{view}"' in text}
        for view in _SILVER_VIEW.findall(text):
            name = view.removeprefix("silver_").removesuffix("_latest")
            source = max((s for s in sources if name.startswith(f"{s}_")), key=len)
            pairs.add((source, name.removeprefix(f"{source}_")))
    return pairs


def test_backfill_preset_tour_expands_to_datasets(
    backfills: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects the tour preset drifting from what 00_overview and 01_sources read
    (their 11 datasets, counting the gold storage view as its silver input), a
    window other than the last 7 complete UTC days, or chunks wider than a day
    outside Open-Meteo: NESO files a multi-day response under its first day, so a
    tour rerun a day later would leave six days twice in silver. Open-Meteo takes
    the week in one chunk, one request per site."""
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    monkeypatch.setenv("GIE_API_KEY", "test-key")
    today = datetime.now(UTC).date()

    result = cli.invoke(app, ["backfill", "--preset", "tour"])

    assert result.exit_code == 0, result.output
    fetched = [(source, ds) for source, datasets, *_ in backfills for ds in datasets]
    assert len(fetched) == len(set(fetched)) == 10
    assert set(fetched) == _notebook_datasets()
    windows = {(start, end) for _, _, start, end, _ in backfills}
    assert len(windows) == 1
    ((start, end),) = windows
    assert end.date() in {today, datetime.now(UTC).date()}
    assert end == datetime(end.year, end.month, end.day, tzinfo=UTC)
    assert end - start == timedelta(days=7)
    chunks = {source: chunk for source, *_, chunk in backfills}
    assert chunks == {source: 7 if source == "open_meteo" else 1 for source in chunks}
    assert "Backfill complete" in result.output


def test_preset_skips_keyed_source_without_key(
    backfills: list[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a keyed source being fetched (and failing) without its key, or a
    skip that does not say which variable to set."""
    monkeypatch.setenv("ENTSOE_API_KEY", "")
    monkeypatch.setenv("GIE_API_KEY", "")

    result = cli.invoke(app, ["backfill", "--preset", "tour"])

    assert result.exit_code == 0, result.output
    assert {source for source, *_ in backfills} == {"elexon", "neso", "open_meteo", "entsog"}
    skips = sorted(line for line in result.output.splitlines() if line.startswith("Skipping "))
    assert len(skips) == 3, result.output
    assert "entsoe" in skips[0] and "ENTSOE_API_KEY" in skips[0]
    assert ["gie_agsi" in skips[1], "gie_alsi" in skips[2]] == [True, True]
    assert all("GIE_API_KEY" in line for line in skips[1:])


def test_preset_path_loads_yaml(backfills: list[Any], tmp_path: Path) -> None:
    """Detects a path being looked up as a packaged name, or a start/end window
    read other than as the CLI reads it (a bare end date is inclusive)."""
    preset = tmp_path / "mine.yaml"
    preset.write_text(
        "datasets:\n"
        "  - {source: elexon, dataset: fuelhh}\n"
        "  - {source: neso, dataset: carbon_intensity}\n"
        "  - {source: elexon, dataset: system_prices}\n"
        "window: {start: 2024-01-15, end: 2024-01-16}\n",
        encoding="utf-8",
    )

    result = cli.invoke(app, ["backfill", "--preset", str(preset)])

    assert result.exit_code == 0, result.output
    start, end = datetime(2024, 1, 15, tzinfo=UTC), datetime(2024, 1, 17, tzinfo=UTC)
    assert backfills == [
        ("elexon", ["fuelhh", "system_prices"], start, end, 1),
        ("neso", ["carbon_intensity"], start, end, 1),
    ]


def test_preset_takes_chunk_days(backfills: list[Any]) -> None:
    """``--chunk-days`` replaces every entry's own chunk size, Open-Meteo's included."""
    result = cli.invoke(app, ["backfill", "--preset", "tour", "--chunk-days", "3"])

    assert result.exit_code == 0, result.output
    assert {chunk for *_, chunk in backfills} == {3}


def test_preset_entry_chunk_days_reaches_the_backfill(backfills: list[Any], tmp_path: Path) -> None:
    """Detects an entry's chunk_days not reaching ``run_backfill``, reaching
    another dataset of the same source, or an entry without one getting other
    than one-day chunks."""
    preset = tmp_path / "chunks.yaml"
    preset.write_text(
        "datasets:\n"
        "  - {source: open_meteo, dataset: historical_wind, chunk_days: 31}\n"
        "  - {source: elexon, dataset: fuelhh}\n"
        "  - {source: open_meteo, dataset: historical_demand}\n"
        "  - {source: open_meteo, dataset: historical_solar, chunk_days: 31}\n"
        "window: {start: 2024-01-01, end: 2024-02-29}\n",
        encoding="utf-8",
    )

    result = cli.invoke(app, ["backfill", "--preset", str(preset)])

    assert result.exit_code == 0, result.output
    start, end = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)
    assert backfills == [
        ("open_meteo", ["historical_wind", "historical_solar"], start, end, 31),
        ("open_meteo", ["historical_demand"], start, end, 1),
        ("elexon", ["fuelhh"], start, end, 1),
    ]


@pytest.mark.parametrize("days", [1, 35])
def test_preset_loader_takes_open_meteo_chunk_days(tmp_path: Path, days: int) -> None:
    """Detects either end of Open-Meteo's chunk range (1 to the 35-day look-back) being refused."""
    preset = tmp_path / "ok.yaml"
    preset.write_text(
        "datasets:\n"
        f"  - {{source: open_meteo, dataset: forecast_wind, chunk_days: {days}}}\n"
        "  - {source: neso, dataset: carbon_intensity}\n"
        "window: {last_days: 7}\n",
        encoding="utf-8",
    )

    loaded = runner.load_preset(str(preset), load_settings())

    assert loaded.chunk_days == {
        ("open_meteo", "forecast_wind"): days,
        ("neso", "carbon_intensity"): 1,
    }


def _chunk_entry(source: str, dataset: str, chunk_days: str) -> str:
    return (
        f"datasets: [{{source: {source}, dataset: {dataset}, chunk_days: {chunk_days}}}]\n"
        "window: {last_days: 7}\n"
    )


_TWO_WINDOWS = (
    "datasets: [{source: elexon, dataset: fuelhh}]\n"
    "window: {last_days: 7, start: 2024-01-01, end: 2024-01-02}\n"
)


@pytest.mark.parametrize(
    ("args", "yaml_text", "message"),
    [
        pytest.param(["elexon", "--preset", "tour"], None, "--preset", id="source-with-preset"),
        pytest.param(
            ["--preset", "tour", "--start", "2024-01-01"], None, "--preset", id="window-with-preset"
        ),
        pytest.param(["--preset", "no-such-preset"], None, "tour", id="unknown-name"),
        pytest.param(
            ["--preset", "tour", "--chunk-days", "36"], None, "at most 35", id="tour-past-look-back"
        ),
        pytest.param(
            [
                "open_meteo",
                "historical_wind",
                "--start",
                "2024-01-01",
                "--end",
                "2024-03-01",
                "--chunk-days",
                "36",
            ],
            None,
            "--chunk-days",
            id="open-meteo-past-look-back",
        ),
        pytest.param([], _TWO_WINDOWS, "window", id="two-window-forms"),
        pytest.param(
            [],
            "datasets: [{source: nowhere, dataset: fuelhh}]\nwindow: {last_days: 7}\n",
            "nowhere",
            id="unknown-source",
        ),
        pytest.param(
            [],
            "datasets: [{source: elexon, dataset: nothing}]\nwindow: {last_days: 7}\n",
            "nothing",
            id="unknown-dataset",
        ),
        pytest.param(
            [],
            "datasets:\n"
            "  - {source: open_meteo, dataset: historical_wind, chunk_days: 7}\n"
            "  - {source: open_meteo, dataset: historical_wind}\n"
            "window: {last_days: 7}\n",
            "listed twice",
            id="dataset-listed-twice",
        ),
        pytest.param(
            [],
            _chunk_entry("neso", "carbon_intensity", "7"),
            "Open-Meteo",
            id="chunk-days-outside-open-meteo",
        ),
        pytest.param(
            [], _chunk_entry("elexon", "fuelhh", "1"), "Open-Meteo", id="chunk-days-1-elexon"
        ),
        pytest.param(
            [], _chunk_entry("open_meteo", "historical_wind", "0"), "1 to 35", id="chunk-days-0"
        ),
        pytest.param(
            [], _chunk_entry("open_meteo", "historical_wind", "36"), "1 to 35", id="chunk-days-36"
        ),
        pytest.param(
            [], _chunk_entry("open_meteo", "historical_wind", "7.5"), "1 to 35", id="chunk-days-7.5"
        ),
        pytest.param(
            [], _chunk_entry("open_meteo", "historical_wind", "7.0"), "1 to 35", id="chunk-days-7.0"
        ),
        pytest.param(
            [], _chunk_entry("open_meteo", "historical_wind", "'7'"), "1 to 35", id="chunk-days-str"
        ),
        pytest.param(
            [],
            _chunk_entry("open_meteo", "historical_wind", "true"),
            "1 to 35",
            id="chunk-days-bool",
        ),
        pytest.param(
            [],
            _chunk_entry("open_meteo", "historical_wind", "null"),
            "1 to 35",
            id="chunk-days-null",
        ),
    ],
)
def test_preset_usage_errors_exit_2(
    backfills: list[Any],
    tmp_path: Path,
    args: list[str],
    yaml_text: str | None,
    message: str,
) -> None:
    """Bad presets and conflicting arguments are usage errors, before any fetch."""
    if yaml_text is not None:
        preset = tmp_path / "bad.yaml"
        preset.write_text(yaml_text, encoding="utf-8")
        args = [*args, "--preset", str(preset)]

    result = cli.invoke(app, ["backfill", *args])

    assert result.exit_code == 2, result.output
    assert message in _ANSI_RE.sub("", result.output)
    assert backfills == []


def test_backfill_without_preset_still_needs_a_source_and_window(backfills: list[Any]) -> None:
    result = cli.invoke(app, ["backfill", "elexon", "fuelhh", "--start", "2024-01-15"])

    assert result.exit_code == 2, result.output
    assert backfills == []
