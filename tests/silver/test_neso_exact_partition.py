"""Pin NESO's exact-partition read and the D-6 covered-but-not-owned signal.

P0-a-1 (v0.19 "Silver Truth"): the measured 5x / 80.0% NESO silver duplication
is caused by the covering-partition fallback re-reading one batched bronze
body under every target date in its window
(``_find_covering_bronze_partition``, ``silver/base.py``). This module pins
the fix -- ``GenericNesoJsonTransformer._bronze_files`` reads the EXACT date
partition only, never falls back -- and the accompanying D-6 signal: a target
date covered by, but not owning, a batched partition must warn loudly and
name its owner, never masquerade as "no bronze at all".

Fixture convention (plan T-1): bronze fixtures use the production naming --
body ``raw_<ts>_<hash>.json`` with sidecar ``raw_<ts>_<hash>.meta.json``
colocated (``bronze/writer.py``); sidecar JSON carries ``source``,
``dataset``, and ``request_params`` with ``from_dt``/``to_dt`` as
``NESO_DATETIME_FORMAT`` strings (``%Y-%m-%dT%H:%MZ``), the shape
``_parse_bound`` accepts via its Z -> +00:00 ISO path. No live API, no
``C:\\gridflow-data`` access -- every fixture lives under ``tmp_data_dir``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.connectors.neso.endpoints import NESO_DATETIME_FORMAT
from gridflow.silver.neso.carbon_intensity import CarbonIntensityTransformer
from gridflow.silver.registry import get_transformer
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

_SOURCE = "neso"
_DATASET = "carbon_intensity"


def _intensity_payload(rows: list[tuple[datetime, datetime]]) -> dict[str, Any]:
    """Build a minimal ``/intensity``-family payload with one row per (from, to)."""
    return {
        "data": [
            {
                "from": start.strftime(NESO_DATETIME_FORMAT),
                "to": end.strftime(NESO_DATETIME_FORMAT),
                "intensity": {"forecast": 200, "actual": 210, "index": "moderate"},
            }
            for start, end in rows
        ]
    }


def _write_bronze_body(
    partition_dir: Path,
    stem: str,
    payload: dict[str, Any],
    *,
    source: str = _SOURCE,
    dataset: str = _DATASET,
    request_params: dict[str, Any] | None = None,
) -> Path:
    """Write a production-shaped bronze body + colocated sidecar.

    ``stem`` is the ``raw_<ts>_<hash>`` filename core; the caller supplies it
    so tests can control sort order deterministically.
    """
    partition_dir.mkdir(parents=True, exist_ok=True)
    body_path = partition_dir / f"raw_{stem}.json"
    body_path.write_text(json.dumps(payload))
    meta: dict[str, Any] = {"source": source, "dataset": dataset}
    if request_params is not None:
        meta["request_params"] = request_params
    meta_path = partition_dir / f"raw_{stem}.meta.json"
    meta_path.write_text(json.dumps(meta))
    return body_path


def _day(target_date: date, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(target_date.year, target_date.month, target_date.day, hour, minute, tzinfo=UTC)


def _window_params(start: date, end: date) -> dict[str, str]:
    """A NESO ``request_params`` shape covering ``[start, end)`` in whole days."""
    return {
        "from_dt": _day(start).strftime(NESO_DATETIME_FORMAT),
        "to_dt": _day(end).strftime(NESO_DATETIME_FORMAT),
    }


def test_exact_partition_used_when_present(tmp_data_dir: Path) -> None:
    """A body under the target date's own exact partition is read directly."""
    d0 = date(2026, 8, 1)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    body = _write_bronze_body(
        exact_dir,
        "20260801T000000Z_aaaaaaaa",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params=_window_params(d0, d0 + timedelta(days=1)),
    )

    assert transformer._bronze_files(d0) == [body]


def test_no_covering_fallback_for_neso(tmp_data_dir: Path) -> None:
    """The direct regression pin: a body under D0 only must not answer D0+1.

    Fails on master, where ``_bronze_path_for_date``'s covering fallback
    re-reads the D0 body for D0+1.
    """
    d0 = date(2026, 8, 1)
    d1 = d0 + timedelta(days=1)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    _write_bronze_body(
        exact_dir,
        "20260801T000000Z_aaaaaaaa",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params=_window_params(d0, d1),
    )

    assert transformer._bronze_files(d1) == []
    assert transformer.run(d1) == 0
    silver_file = PathBuilder(tmp_data_dir).silver_file(_SOURCE, _DATASET, d1)
    assert not silver_file.exists()


def test_five_day_window_materialises_once(tmp_data_dir: Path) -> None:
    """The 80.0% -> 0% pin: one 5-day-spanning body must write exactly one file.

    Fails on master with a 5x row count spread across 5 files.
    """
    d0 = date(2026, 8, 1)
    dates = [d0 + timedelta(days=i) for i in range(5)]
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    rows = [(_day(d), _day(d, 0, 30)) for d in dates]
    _write_bronze_body(
        exact_dir,
        "20260801T000000Z_bbbbbbbb",
        _intensity_payload(rows),
        request_params=_window_params(d0, d0 + timedelta(days=5)),
    )

    total_written = sum(transformer.run(d) for d in dates)

    silver_files = list(PathBuilder(tmp_data_dir).silver_dir(_SOURCE, _DATASET).rglob("*.parquet"))
    assert len(silver_files) == 1
    assert total_written == len(rows)
    assert pl.read_parquet(silver_files[0]).height == len(rows)


def test_daily_iteration_datasets_are_unaffected(tmp_data_dir: Path) -> None:
    """Control group (D-4-adjacent): per-day bodies still materialise per day.

    Must pass on master AND after -- ``intensity_date`` writes an exact
    partition for every target date, so the covering-fallback removal never
    engages.
    """
    dataset = "intensity_date"
    transformer = get_transformer(_SOURCE, dataset, tmp_data_dir)
    dates = [date(2026, 8, 1) + timedelta(days=i) for i in range(3)]
    for d in dates:
        exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, dataset, d)
        _write_bronze_body(
            exact_dir,
            f"{d:%Y%m%d}T000000Z_cccccccc",
            _intensity_payload([(_day(d), _day(d, 0, 30))]),
            dataset=dataset,
            request_params={"date": d.isoformat()},
        )

    for d in dates:
        assert transformer.run(d) == 1


def test_reference_dataset_reads_newest_body_regardless_of_date(tmp_data_dir: Path) -> None:
    """D-3: the ``reference_dataset`` branch is untouched by the exact-read fix."""
    dataset = "intensity_factors"
    transformer = get_transformer(_SOURCE, dataset, tmp_data_dir)
    older_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, dataset, date(2026, 1, 1))
    newer_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, dataset, date(2026, 8, 1))
    _write_bronze_body(
        older_dir,
        "20260101T000000Z_dddddddd",
        {"data": [{"gas": 100}]},
        dataset=dataset,
    )
    newer = _write_bronze_body(
        newer_dir,
        "20260801T000000Z_eeeeeeee",
        {"data": [{"gas": 120}]},
        dataset=dataset,
    )

    assert transformer._bronze_files(date(2020, 1, 1)) == [newer]


def test_neso_data_portal_uses_vintage_per_bronze_file() -> None:
    """D-4's structural pin: NDP's immunity depends on this staying ``True``.

    A future refactor that drops ``VINTAGE_PER_BRONZE_FILE`` from any of the
    three NDP transformers re-opens the covering-fallback exposure this
    ticket exists to close -- this must fail loudly, not silently.
    """
    from gridflow.silver.neso_data_portal.daily_wind_availability import (
        DailyWindAvailabilityTransformer,
    )
    from gridflow.silver.neso_data_portal.embedded_wind_solar_forecast import (
        EmbeddedWindSolarForecastTransformer,
    )
    from gridflow.silver.neso_data_portal.historic_generation_mix import (
        HistoricGenerationMixTransformer,
    )

    assert DailyWindAvailabilityTransformer.VINTAGE_PER_BRONZE_FILE is True
    assert EmbeddedWindSolarForecastTransformer.VINTAGE_PER_BRONZE_FILE is True
    assert HistoricGenerationMixTransformer.VINTAGE_PER_BRONZE_FILE is True


def test_covered_but_not_owned_date_warns_and_names_the_owner(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """D-6 arm (b1): a definite claim, only when a body's OWN window covers the date.

    Two bodies under D0's partition: one whose sidecar window ``[D0, D0+5)``
    covers the target D0+1, and one whose sidecar is unresolvable (only one
    bound present). The mixed shape is deliberate -- production directories
    look exactly like this. Fails on master, where D0+1 silently
    re-materialises a duplicate instead of warning.
    """
    caplog.set_level(logging.WARNING, logger="gridflow")
    d0 = date(2026, 8, 1)
    target = d0 + timedelta(days=1)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    _write_bronze_body(
        exact_dir,
        "20260801T000000Z_11111111",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params=_window_params(d0, d0 + timedelta(days=5)),
    )
    _write_bronze_body(
        exact_dir,
        "20260801T000100Z_22222222",
        _intensity_payload([(_day(d0, 1), _day(d0, 1, 30))]),
        request_params={"from_dt": _day(d0).strftime(NESO_DATETIME_FORMAT)},  # missing to_dt
    )

    caplog.clear()
    result = transformer.run(target)

    assert result == 0
    silver_file = PathBuilder(tmp_data_dir).silver_file(_SOURCE, _DATASET, target)
    assert not silver_file.exists()

    messages = [r.message for r in caplog.records]
    definite = [m for m in messages if "NESO covered-but-not-owned:" in m]
    assert len(definite) == 1, messages
    assert str(d0) in definite[0]
    assert "2026-08-01T00:00:00+00:00" in definite[0]
    assert "2026-08-06T00:00:00+00:00" in definite[0]
    assert f"gridflow transform neso {_DATASET} --start {d0} --end {d0} --reingest" in definite[0]


def test_no_neso_warning_when_no_covering_directory_exists(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """D-6 arm (a): genuinely no bronze anywhere -- silent, base's generic line stands."""
    caplog.set_level(logging.WARNING, logger="gridflow")
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    target = date(2026, 8, 15)

    caplog.clear()
    result = transformer.run(target)

    assert result == 0
    messages = [r.message for r in caplog.records]
    assert not any("NESO covered-but-not-owned" in m for m in messages)


def test_prior_but_non_covering_partition_is_hedged_when_resolved(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """D-6 arm (b2), variant (i): a resolved window that does not cover the date.

    A prior partition's single body resolves to ``[D0, D0+1)``; target is
    D0+3. The window is valid but does not contain the target day, so
    ownership must be hedged, not claimed.
    """
    caplog.set_level(logging.WARNING, logger="gridflow")
    d0 = date(2026, 8, 1)
    target = d0 + timedelta(days=3)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    _write_bronze_body(
        exact_dir,
        "20260801T000000Z_33333333",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params=_window_params(d0, d0 + timedelta(days=1)),
    )

    caplog.clear()
    result = transformer.run(target)

    assert result == 0
    messages = [r.message for r in caplog.records]
    hedged = [m for m in messages if "NESO covered-but-not-owned (unconfirmed):" in m]
    assert len(hedged) == 1, messages
    assert "owned by" not in hedged[0]
    assert "may not exist" in hedged[0]
    assert "1 window(s) resolved but do not cover" in hedged[0]


def test_prior_but_non_covering_partition_is_hedged_when_unresolvable(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """D-6 arm (b2), variant (ii): an unresolvable sidecar (``MISSING_PARAM``).

    Ownership cannot be confirmed OR denied from a body with no usable
    window -- the hedge, with its failing ``WindowReason``, is the only
    honest signal.
    """
    caplog.set_level(logging.WARNING, logger="gridflow")
    d0 = date(2026, 8, 1)
    target = d0 + timedelta(days=3)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    _write_bronze_body(
        exact_dir,
        "20260801T000000Z_44444444",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params={"from_dt": _day(d0).strftime(NESO_DATETIME_FORMAT)},  # missing to_dt
    )

    caplog.clear()
    result = transformer.run(target)

    assert result == 0
    messages = [r.message for r in caplog.records]
    hedged = [m for m in messages if "NESO covered-but-not-owned (unconfirmed):" in m]
    assert len(hedged) == 1, messages
    assert "owned by" not in hedged[0]
    assert "missing_param" in hedged[0].lower()


def test_covering_lookup_is_absent_from_the_success_path(
    tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pins the "detection only, miss path only" clause.

    With an exact partition present, the covering-partition walk (up to 35
    iterations) must never run at all -- monkeypatching it to raise proves
    the success path never reaches it.
    """
    d0 = date(2026, 8, 1)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    body = _write_bronze_body(
        exact_dir,
        "20260801T000000Z_55555555",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params=_window_params(d0, d0 + timedelta(days=1)),
    )

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("covering lookup must not run on the success path")

    monkeypatch.setattr(transformer, "_find_covering_bronze_partition", _boom)

    assert transformer._bronze_files(d0) == [body]


def test_disjoint_windows_bridging_a_gap_is_hedged(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Sol pass 5, major 3's pin: an aggregate envelope must not fabricate ownership.

    Two bodies with windows ``[D0, D0+2)`` and ``[D0+4, D0+6)`` bracket a gap
    at D0+3. The ENVELOPE ``[D0, D0+6)`` contains D0+3, but no single body
    does -- fails against an envelope-based implementation (e.g. one using
    ``partition_request_window``) by construction.
    """
    caplog.set_level(logging.WARNING, logger="gridflow")
    d0 = date(2026, 8, 1)
    target = d0 + timedelta(days=3)
    transformer = CarbonIntensityTransformer(tmp_data_dir)
    exact_dir = PathBuilder(tmp_data_dir).bronze_date_dir(_SOURCE, _DATASET, d0)
    _write_bronze_body(
        exact_dir,
        "20260801T000000Z_66666666",
        _intensity_payload([(_day(d0), _day(d0, 0, 30))]),
        request_params=_window_params(d0, d0 + timedelta(days=2)),
    )
    _write_bronze_body(
        exact_dir,
        "20260801T000100Z_77777777",
        _intensity_payload([(_day(d0 + timedelta(days=4)), _day(d0 + timedelta(days=4), 0, 30))]),
        request_params=_window_params(d0 + timedelta(days=4), d0 + timedelta(days=6)),
    )

    caplog.clear()
    result = transformer.run(target)

    assert result == 0
    messages = [r.message for r in caplog.records]
    hedged = [m for m in messages if "NESO covered-but-not-owned (unconfirmed):" in m]
    assert len(hedged) == 1, messages
    assert "owned by" not in hedged[0]
    assert "2 window(s) resolved but do not cover" in hedged[0]
