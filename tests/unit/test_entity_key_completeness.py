"""D-8: the committed entity-key golden map and its completeness (R2-A Task 3).

Covers A-13 (golden map committed + resolved keys match it +
order-insensitivity) and windfor's alternate key shape. That every
transformer declares a grain, and that ``dedup_to_grain`` enforces it, is
pinned in ``test_dedup_to_grain.py``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

import gridflow.silver.elexon  # noqa: F401 -- registers every elexon transformer
from gridflow.cli import _entity_key_for
from gridflow.quality.checks import check_duplicates
from gridflow.silver.elexon.fou2t14d import FOU2T14DTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer
from gridflow.silver.registry import get_transformer_class, list_transformers

GOLDEN_MAP_PATH = Path(__file__).parent.parent / "fixtures" / "entity_keys_golden.json"


def _load_golden_map() -> dict[str, dict[str, list[str]]]:
    data = json.loads(GOLDEN_MAP_PATH.read_text())
    return {k: v for k, v in data.items() if not k.startswith("_")}


def test_golden_map_is_committed_and_covers_every_registered_dataset() -> None:
    golden = _load_golden_map()
    registered = {f"{source}/{dataset}" for source, dataset in list_transformers("elexon")}
    assert registered.issubset(golden.keys()), registered - golden.keys()


def test_resolved_entity_keys_match_the_golden_map() -> None:
    golden = _load_golden_map()
    for source, dataset in list_transformers("elexon"):
        cls = get_transformer_class(source, dataset)
        assert cls is not None
        golden_entry = golden[f"{source}/{dataset}"]

        # D-8: set-equality, not list/order equality.
        assert set(cls.ENTITY_KEY_COLUMNS) == set(golden_entry["required"]), dataset
        assert set(cls.OPTIONAL_ENTITY_KEY_COLUMNS) == set(golden_entry["optional"]), dataset


def test_unique_subset_is_order_insensitive() -> None:
    """D-8: unique(subset=...) is SET semantics -- declaration order (and the
    golden map's list order) is explicitly NOT part of the contract."""
    declared = set(FOU2T14DTransformer.ENTITY_KEY_COLUMNS) | set(
        FOU2T14DTransformer.OPTIONAL_ENTITY_KEY_COLUMNS
    )
    reordered_comparison_set = {
        "published_at",
        "settlement_period",
        "fuel_type",
        "settlement_date",
    }
    assert declared == reordered_comparison_set

    # And directly against the golden map's own (independently-ordered) lists.
    golden = _load_golden_map()["elexon/fou2t14d"]
    reversed_required = list(reversed(golden["required"]))
    assert set(reversed_required) == set(golden["required"])
    assert set(reversed_required) | set(golden["optional"]) == declared


class TestWindforAlternateKeyShape:
    """FIX 3 (F-16 follow-up): windfor's real transform() fallback --
    timestamp_utc-only when neither settlement_date nor settlement_period is
    present -- is a mutually EXCLUSIVE required-column swap, not an additive
    OPTIONAL_ENTITY_KEY_COLUMNS refinement. ``resolve_entity_key`` must
    mirror ``transform()``'s own conditional exactly for BOTH shapes, and
    ``check_duplicates`` must not fail loud on genuinely-present columns for
    the fallback shape (the bug this fix closes)."""

    def test_settlement_shape_resolves_the_primary_key(self) -> None:
        available = ["settlement_date", "settlement_period", "published_at", "latest_forecast_mw"]
        assert WindForecastTransformer.resolve_entity_key(available) == (
            "settlement_date",
            "settlement_period",
            "published_at",
        )

    def test_start_time_fallback_shape_resolves_to_timestamp_utc(self) -> None:
        """The real fallback shape: settlement columns are wholly absent."""
        available = ["timestamp_utc", "published_at", "latest_forecast_mw"]
        assert WindForecastTransformer.resolve_entity_key(available) == (
            "timestamp_utc",
            "published_at",
        )

    def test_fallback_shape_without_published_at(self) -> None:
        available = ["timestamp_utc", "latest_forecast_mw"]
        assert WindForecastTransformer.resolve_entity_key(available) == ("timestamp_utc",)

    def test_cli_entity_key_for_resolves_fallback_shape_without_raising(self) -> None:
        """RED before FIX 3: the old additive resolver declared
        (settlement_date, settlement_period) unconditionally, so
        check_duplicates raised "Key columns not found" for genuinely valid
        fallback-shaped windfor data once the registry was populated
        (FIX 1)."""
        df = pl.DataFrame(
            {
                "timestamp_utc": [
                    datetime(2026, 7, 11, 0, 0, tzinfo=UTC),
                    datetime(2026, 7, 11, 0, 30, tzinfo=UTC),
                ],
                "published_at": [
                    datetime(2026, 7, 10, 23, 0, tzinfo=UTC),
                    datetime(2026, 7, 10, 23, 0, tzinfo=UTC),
                ],
                "latest_forecast_mw": [100.0, 105.0],
            }
        )
        entity_key = _entity_key_for("elexon", "windfor", df.columns)
        assert entity_key == ["timestamp_utc", "published_at"]

        result = check_duplicates(
            duckdb.from_arrow(df), entity_key, source="elexon", dataset="windfor"
        )
        assert result.passed is True
        assert "not found" not in result.detail

    def test_transform_output_fallback_shape_is_uniquely_grained_by_resolved_key(self) -> None:
        """Proves resolve_entity_key's fallback branch against the REAL
        transform() output, not just a synthetic frame."""
        transformer = WindForecastTransformer.__new__(WindForecastTransformer)
        raw = pl.DataFrame(
            [
                {
                    "startTime": "2026-07-11T00:00:00Z",
                    "generation": 100.0,
                    "publishTime": "2026-07-10T23:00:00Z",
                },
                {
                    "startTime": "2026-07-11T00:00:00Z",  # duplicate key (full, with published_at)
                    "generation": 101.0,
                    "publishTime": "2026-07-10T23:00:00Z",
                },
                {
                    "startTime": "2026-07-11T00:30:00Z",
                    "generation": 102.0,
                    "publishTime": "2026-07-10T23:00:00Z",
                },
            ]
        )
        out = transformer.transform(raw)
        assert "settlement_date" not in out.columns
        key = list(WindForecastTransformer.resolve_entity_key(out.columns))
        assert key == ["timestamp_utc", "published_at"]
        assert out.select(key).n_unique() == len(out)
        assert len(out) == 2

    def test_transform_output_settlement_shape_is_uniquely_grained_by_resolved_key(self) -> None:
        transformer = WindForecastTransformer.__new__(WindForecastTransformer)
        raw = pl.DataFrame(
            [
                {
                    "settlementDate": "2026-07-11",
                    "settlementPeriod": 1,
                    "generation": 100.0,
                    "publishTime": "2026-07-10T23:00:00Z",
                },
                {
                    "settlementDate": "2026-07-11",
                    "settlementPeriod": 1,  # duplicate key (full, with published_at)
                    "generation": 101.0,
                    "publishTime": "2026-07-10T23:00:00Z",
                },
                {
                    "settlementDate": "2026-07-11",
                    "settlementPeriod": 2,
                    "generation": 102.0,
                    "publishTime": "2026-07-10T23:00:00Z",
                },
            ]
        )
        out = transformer.transform(raw)
        key = list(WindForecastTransformer.resolve_entity_key(out.columns))
        assert key == ["settlement_date", "settlement_period", "published_at"]
        assert out.select(key).n_unique() == len(out)
        assert len(out) == 2
