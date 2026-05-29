"""Regression: entsog cmp_* silver schema is intentionally dynamic, and gridflow's
tolerant read paths unify it. Guards the ADR-021 decision.

This is the honest inverse of ``test_indo_schema_drift_regression.py``. The indo test
asserts the on-disk store is *uniform* (reads without ``union_by_name`` on purpose)
because indo's ``published_at`` is a declared, always-present nullable contract column.

The cmp_* envelope columns (``data_set``, ``point_type``, ``is_archived``,
``booking_platform_key``, ...) are different in kind: the
``GenericEntsogJsonTransformer`` passes through whatever fields the API returns, and
those fields ride in only on archived CMP records whose ``capacityFrom`` happens to
fall on the target date (see ADR-021). So a given day's partition carries them iff its
date-filtered slice coincidentally includes ≥1 such record — the schema is genuinely
data-dependent.

This test therefore asserts the opposite of uniformity:
1. two days transformed from the same window produce *different* schemas (drift is
   by-design, not a bug to be "fixed" by fabricating a contract); and
2. gridflow's tolerant read paths — DuckDB ``union_by_name=true`` (the silver-view
   shape, ``storage/duckdb.py``) and Polars ``missing_columns="insert"``
   (``storage/parquet.py``) — unify the drifted glob without error.

If someone removes that reader tolerance, or "normalises" the transformer into a
fixed schema, this test fails and points back at ADR-021.
"""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import pytest

# Registry side-effect: importing the sub-package registers the generic entsog
# transformers (Pitfall: must import explicitly, not rely on gridflow.silver).
import gridflow.silver.entsog  # noqa: F401
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

# The archived record's capacityFrom lands on this day, so its envelope columns
# survive the date-window filter only in this partition.
_ENRICHED_DAY = date(2026, 5, 1)
_PLAIN_DAY = date(2026, 5, 2)

# Columns present only on the enriched partition. A representative split of the
# eight observed on disk: typed-sparse fields plus one all-null typeless field.
_TYPED_ENVELOPE = {"data_set", "id_point_type", "is_archived", "point_type"}
_TYPELESS_ENVELOPE = "booking_platform_key"

_CMP_DATASETS = [
    ("cmp_unsuccessful_requests", "cmpUnsuccessfulRequests"),
    ("cmp_unavailable_firm_capacity", "cmpUnavailables"),
]


def _plain_record(day: date) -> dict[str, Any]:
    """A normal CMP request row, dated to ``day`` via periodFrom (the priority field)."""
    return {
        "id": f"plain-{day.isoformat()}",
        "pointKey": "UK-TSO-0001ITP-00005",
        "periodFrom": f"{day.isoformat()}T00:00:00+00:00",
    }


def _archived_record() -> dict[str, Any]:
    """An archived/historical CMP record. No periodFrom; its effective filter-date
    comes from capacityFrom (_ENRICHED_DAY). Carries the dynamic envelope fields the
    API attaches to such records — including an all-null one (booking_platform_key).
    """
    return {
        "id": "archived-cmp-record",
        "pointKey": "UK-TSO-0001ITP-00005",
        "capacityFrom": f"{_ENRICHED_DAY.isoformat()}T06:00:00+02:00",
        "dataSet": 2,
        "idPointType": 0,
        "isArchived": True,
        "pointType": "Cross-Border Transmission IP within EU",
        "bookingPlatformKey": None,  # always-null in observed data -> pl.Null dtype
    }


def _write_cmp_bronze(
    bronze_root: Path, day: date, response_key: str, records: list[dict[str, Any]]
) -> None:
    """Write a cmp_* bronze partition mirroring GenericEntsogJsonTransformer.read_bronze:
    bronze/entsog/<dataset>/<YYYY>/<MM>/<DD>/raw_*.json with a {response_key: [...]} body.
    """
    partition = bronze_root / str(day.year) / f"{day.month:02d}" / f"{day.day:02d}"
    partition.mkdir(parents=True, exist_ok=True)
    (partition / "raw_test.json").write_text(json.dumps({response_key: records}))


@pytest.mark.parametrize("dataset,response_key", _CMP_DATASETS)
def test_cmp_schema_is_dynamic_but_tolerant_readers_unify(
    tmp_data_dir: Path, dataset: str, response_key: str
) -> None:
    bronze_root = tmp_data_dir / "bronze" / "entsog" / dataset
    archived = _archived_record()
    # Same window stored under each day; the archived record appears in both, but its
    # capacityFrom only matches _ENRICHED_DAY.
    _write_cmp_bronze(bronze_root, _ENRICHED_DAY, response_key, [_plain_record(_ENRICHED_DAY), archived])
    _write_cmp_bronze(bronze_root, _PLAIN_DAY, response_key, [_plain_record(_PLAIN_DAY), archived])

    transformer = get_transformer("entsog", dataset, tmp_data_dir)
    assert transformer.run(_ENRICHED_DAY) == 2  # plain + archived survive the filter
    assert transformer.run(_PLAIN_DAY) == 1  # archived filtered out; only plain remains

    silver_dir = tmp_data_dir / "silver" / "entsog" / dataset

    def _partition(day: date) -> pl.DataFrame:
        path = (
            silver_dir
            / f"year={day.year}"
            / f"month={day.month:02d}"
            / f"{dataset}_{day.strftime('%Y%m%d')}.parquet"
        )
        return pl.read_parquet(path)

    enriched = _partition(_ENRICHED_DAY)
    plain = _partition(_PLAIN_DAY)

    # 1. The two partitions genuinely drift (by-design), and the envelope columns are
    #    the difference. This documents that the drift is expected — not a bug.
    enriched_cols, plain_cols = set(enriched.columns), set(plain.columns)
    assert plain_cols < enriched_cols, "enriched partition must be a strict superset"
    drift = enriched_cols - plain_cols
    assert _TYPED_ENVELOPE <= drift
    assert _TYPELESS_ENVELOPE in drift
    # The typeless field is all-null and carries no concrete type, exactly as on disk.
    assert enriched.schema[_TYPELESS_ENVELOPE] == pl.Null
    # The common contract columns are present in both partitions.
    for contract_col in ("timestamp_utc", "data_provider", "event_time", "available_at"):
        assert contract_col in plain_cols and contract_col in enriched_cols

    glob = (silver_dir / "**" / "*.parquet").as_posix()

    # 2a. DuckDB silver-view shape (union_by_name=true) unifies the drift.
    con = duckdb.connect()
    try:
        unified = con.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            "ORDER BY id",
            [glob],
        ).pl()
    finally:
        con.close()
    assert unified.height == 3  # 2 enriched-day rows + 1 plain-day row
    assert _TYPED_ENVELOPE <= set(unified.columns)
    # Only the single archived row carries point_type; the other two are null-filled.
    assert unified["point_type"].null_count() == 2

    # 2b. gridflow's Polars read path (missing_columns="insert") unifies it too.
    via_polars = pl.read_parquet(glob, hive_partitioning=True, missing_columns="insert")
    assert via_polars.height == 3
    assert _TYPED_ENVELOPE <= set(via_polars.columns)
