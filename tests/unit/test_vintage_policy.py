"""Availability reconstruction, boundary, and no-policy regression tests (ADR-031)."""

from __future__ import annotations

import hashlib
import io
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from gridflow.silver.base import BaseSilverTransformer, VintagePolicy
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.openmeteo.forecast import (
    ForecastDemandWeather,
    ForecastSolarWeather,
    ForecastWindWeather,
)
from gridflow.silver.openmeteo.historical import (
    HistoricalDemandWeather,
    HistoricalSolarWeather,
    HistoricalWindWeather,
)
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from pathlib import Path

POLICY_CLASSES = (
    MIDTransformer,
    SystemPriceTransformer,
    HistoricalDemandWeather,
    HistoricalWindWeather,
    HistoricalSolarWeather,
)
CAPTURE = datetime(2026, 9, 6, tzinfo=UTC)
OLD = datetime(2021, 1, 15, tzinfo=UTC)


def _raw(transformer: BaseSilverTransformer, event: datetime) -> pl.DataFrame:
    """Supply a minimal vendor-shaped fixture to the real transformer."""
    if isinstance(transformer, MIDTransformer):
        return pl.DataFrame(
            {
                "settlementDate": [event.date().isoformat()],
                "settlementPeriod": [1],
                "dataProvider": ["APXMIDP"],
                "price": [50.0],
                "volume": [100.0],
            }
        )
    if isinstance(transformer, SystemPriceTransformer):
        return pl.DataFrame(
            {
                "settlementDate": [event.date().isoformat()],
                "settlementPeriod": [1],
                "systemSellPrice": [50.0],
                "systemBuyPrice": [50.0],
                "netImbalanceVolume": [100.0],
            }
        )
    return pl.DataFrame(
        {
            "time": [event.strftime("%Y-%m-%dT%H:%M")],
            "location": ["london"],
            "latitude": [51.5],
            "longitude": [-0.1],
            "temperature_2m": [10.0],
        }
    )


@pytest.mark.parametrize(
    "transformer_cls,reconstructed,label",
    [
        (
            MIDTransformer,
            datetime(2021, 1, 15, 0, 35, tzinfo=UTC),
            "elexon-mid/vp-2026-09b",
        ),
        (
            SystemPriceTransformer,
            datetime(2021, 1, 15, 1, 30, tzinfo=UTC),
            "elexon-system_prices/vp-2026-09",
        ),
        (
            HistoricalDemandWeather,
            datetime(2021, 1, 20, tzinfo=UTC),
            "open_meteo-historical_demand/vp-2026-09",
        ),
        (
            HistoricalWindWeather,
            datetime(2021, 1, 20, tzinfo=UTC),
            "open_meteo-historical_wind/vp-2026-09",
        ),
        (
            HistoricalSolarWeather,
            datetime(2021, 1, 20, tzinfo=UTC),
            "open_meteo-historical_solar/vp-2026-09",
        ),
    ],
)
@pytest.mark.parametrize("old", [True, False], ids=["backfill-era", "live-era"])
def test_real_transformer_policy_arms(
    tmp_path: Path,
    transformer_cls: type[BaseSilverTransformer],
    reconstructed: datetime,
    label: str,
    old: bool,
) -> None:
    transformer = transformer_cls(tmp_path)
    event = OLD if old else CAPTURE
    transformed = transformer.transform(_raw(transformer, event))
    assert transformed.height == 1
    assert "vintage_policy" not in transformed.columns
    assert transformer.schema_cls is not None
    assert "vintage_policy" not in transformer.schema_cls.model_fields
    result = transformer._add_bitemporal_columns(transformed, event.date(), "run", CAPTURE)
    policy = transformer.VINTAGE_POLICY
    assert policy is not None
    expected = reconstructed if old else CAPTURE
    assert result["available_at"].to_list() == [expected]
    assert result["vintage_policy"].to_list() == [label if old else "ingest-clock"]
    assert result.schema["available_at"] == pl.Datetime("us", "UTC")
    assert result.schema["vintage_policy"] == pl.String


@pytest.mark.parametrize("transformer_cls", POLICY_CLASSES)
def test_strict_cutover_and_ingest_boundaries(
    tmp_path: Path, transformer_cls: type[BaseSilverTransformer]
) -> None:
    transformer = transformer_cls(tmp_path)
    policy = transformer.VINTAGE_POLICY
    assert policy is not None
    cut = policy.applies_before
    before = cut - timedelta(microseconds=1)
    events = [before, cut, cut + timedelta(microseconds=1), OLD, OLD, None]
    captures = [CAPTURE, CAPTURE, CAPTURE, OLD + policy.lag, OLD + policy.lag / 2, CAPTURE]
    df = pl.DataFrame(
        {
            "timestamp_utc": pl.Series(events, dtype=pl.Datetime("us", "UTC")),
            "capture": captures,
        }
    )
    result = transformer._add_bitemporal_columns(
        df, OLD.date(), "run", CAPTURE, vintage_column="capture"
    )
    assert result["available_at"].to_list() == [before + policy.lag, *captures[1:]]
    assert result["vintage_policy"].to_list() == [policy.name, *["ingest-clock"] * 5]


def test_vendor_priority_and_lockstep_captures(tmp_path: Path) -> None:
    transformer = MIDTransformer(tmp_path)
    policy = transformer.VINTAGE_POLICY
    assert policy is not None
    published = CAPTURE + timedelta(days=1)
    df = pl.DataFrame(
        {
            "timestamp_utc": [OLD] * 3,
            "capture": [CAPTURE, CAPTURE, OLD + timedelta(minutes=1)],
            "published_at": pl.Series([published, None, None], dtype=pl.Datetime("us", "UTC")),
        }
    )
    result = transformer._add_bitemporal_columns(
        df, OLD.date(), "run", CAPTURE, vintage_column="capture"
    )
    assert result["available_at"].to_list() == [
        published,
        OLD + policy.lag,
        OLD + timedelta(minutes=1),
    ]
    assert result["vintage_policy"].to_list() == ["vendor", policy.name, "ingest-clock"]


@pytest.mark.parametrize("dtype", [pl.String, pl.Int64, pl.Datetime("us")])
def test_policy_preserves_published_dtype_guard(tmp_path: Path, dtype: pl.DataType) -> None:
    df = pl.DataFrame({"timestamp_utc": [OLD]}).with_columns(
        pl.lit(None).cast(dtype).alias("published_at")
    )
    with pytest.raises((TypeError, pl.exceptions.SchemaError)):
        MIDTransformer(tmp_path)._add_bitemporal_columns(df, OLD.date(), "run", CAPTURE)


@pytest.mark.parametrize(
    "transformer_cls",
    [
        ForecastDemandWeather,
        ForecastWindWeather,
        ForecastSolarWeather,
        FuelHHTransformer,
        INDOTransformer,
    ],
)
def test_no_policy_emits_no_label(
    tmp_path: Path, transformer_cls: type[BaseSilverTransformer]
) -> None:
    transformer = transformer_cls(tmp_path)
    assert transformer.VINTAGE_POLICY is None
    result = transformer._add_bitemporal_columns(
        pl.DataFrame({"timestamp_utc": [OLD]}), OLD.date(), "run", CAPTURE
    )
    assert "vintage_policy" not in result.columns
    assert result["available_at"].to_list() == [CAPTURE]


def test_no_policy_published_frame_and_parquet_bytes(tmp_path: Path) -> None:
    """Pin the pre-policy lineage contract, including column order and bytes."""
    transformer = FuelHHTransformer(tmp_path)
    df = transformer.transform(
        pl.DataFrame(
            {
                "settlementDate": ["2021-01-15"],
                "settlementPeriod": [1],
                "fuelType": ["GAS"],
                "generation": [100.0],
                "publishTime": ["2021-01-15T01:00:00Z"],
            }
        )
    )
    assert df["published_at"].null_count() == 0
    expected = df.with_columns(
        pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")).alias("event_time"),
        pl.col("published_at").alias("available_at"),
        pl.lit("fixed-run").alias("source_run_id"),
        pl.lit(transformer.DATASET_VERSION).alias("dataset_version"),
    )
    actual = transformer._add_bitemporal_columns(df, OLD.date(), "fixed-run", CAPTURE)
    assert_frame_equal(actual, expected)
    expected_bytes, actual_bytes = io.BytesIO(), io.BytesIO()
    expected.write_parquet(expected_bytes)
    actual.write_parquet(actual_bytes)
    assert actual_bytes.getvalue() == expected_bytes.getvalue()
    assert (
        hashlib.sha256(actual_bytes.getvalue()).digest()
        == hashlib.sha256(expected_bytes.getvalue()).digest()
    )


@pytest.mark.parametrize("day,period", [(date(2025, 3, 30), 46), (date(2025, 10, 26), 50)])
@pytest.mark.parametrize("transformer_cls", [MIDTransformer, SystemPriceTransformer])
def test_period_end_lags_across_dst(
    tmp_path: Path, day: date, period: int, transformer_cls: type[BaseSilverTransformer]
) -> None:
    transformer = transformer_cls(tmp_path)
    df = _raw(transformer, datetime.combine(day, datetime.min.time(), UTC)).with_columns(
        pl.lit(period).alias("settlementPeriod")
    )
    transformed = transformer.transform(df)
    result = transformer._add_bitemporal_columns(transformed, day, "run", CAPTURE)
    after_end = timedelta(minutes=5 if transformer_cls is MIDTransformer else 60)
    assert (
        result["available_at"][0]
        == settlement_period_to_utc(day, period) + timedelta(minutes=30) + after_end
    )


@pytest.mark.parametrize(
    "cutover",
    [
        datetime(2026, 8, 1),
        datetime(2026, 8, 1, tzinfo=timezone(timedelta(hours=1))),
        datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/London")),
    ],
)
def test_policy_rejects_non_utc_cutover(cutover: datetime) -> None:
    with pytest.raises(ValueError, match="tz-aware UTC"):
        VintagePolicy("test/vp-2026-09", timedelta(days=1), OLD.date(), "test", cutover)


@pytest.mark.parametrize("cutover", [CAPTURE, datetime(2026, 8, 1, tzinfo=ZoneInfo("UTC"))])
def test_policy_accepts_utc_cutover(cutover: datetime) -> None:
    policy = VintagePolicy("test/vp-2026-09", timedelta(days=1), OLD.date(), "test", cutover)
    assert policy.applies_before == cutover


@pytest.mark.parametrize("name", ["", " ", "vendor", "ingest-clock"])
def test_policy_rejects_invalid_labels(name: str) -> None:
    with pytest.raises(ValueError, match="reserved label"):
        VintagePolicy(name, timedelta(days=1), OLD.date(), "test", CAPTURE)


def test_policy_is_frozen_and_rejects_negative_lag() -> None:
    policy = VintagePolicy("test/vp-2026-09", timedelta(days=1), OLD.date(), "test", CAPTURE)
    with pytest.raises(FrozenInstanceError):
        policy.name = "changed"  # type: ignore[misc]
    with pytest.raises(ValueError, match="non-negative"):
        replace(policy, lag=timedelta(microseconds=-1))


def test_invalid_generated_label_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A derivation bug must fail loudly rather than write an undeclared label."""
    transformer = MIDTransformer(tmp_path)
    policy = transformer.VINTAGE_POLICY
    assert policy is not None
    original_lit = pl.lit

    def faulty_label(value: object) -> pl.Expr:
        return original_lit("undeclared" if value == policy.name else value)

    monkeypatch.setattr(pl, "lit", faulty_label)
    with pytest.raises(ValueError, match="invalid vintage_policy label"):
        transformer._add_bitemporal_columns(
            pl.DataFrame({"timestamp_utc": [OLD]}), OLD.date(), "run", CAPTURE
        )
