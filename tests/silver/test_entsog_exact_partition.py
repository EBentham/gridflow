"""R2-g / F-05: ENTSO-G reads the EXACT bronze partition or nothing.

``EntsogConnector.fetch`` chunks every multi-day window into one request per
covered UTC calendar day, so a correctly-fetched ENTSO-G date either has its
own exact bronze partition or has no bronze at all. Any covering-fallback hit
would relabel a NEIGHBOURING day's rows under the requested date -- the
wrong-day fabrication class the per-day chunking exists to prevent, and the
open half of F-05.

The rule is pinned on BOTH axes (exact present / exact absent), because a
one-sided pin would let the other half regress silently.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import gridflow.silver.entsog  # noqa: F401 -- registers the generic entsog family
from gridflow.silver.entsog.physical_flows import PhysicalFlowsTransformer
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path


TARGET = date(2026, 5, 10)
COVERING = date(2026, 5, 5)  # 5 days earlier -- well inside the 35-day lookback
STAMP = datetime(2026, 5, 10, 9, 15, tzinfo=UTC)
GENERIC_DATASET = "nominations"


def _partition(data_dir: Path, dataset: str, day: date) -> Path:
    path = (
        data_dir
        / "bronze"
        / "entsog"
        / dataset
        / str(day.year)
        / f"{day.month:02d}"
        / f"{day.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


def _flow(point: str, day: date) -> dict[str, Any]:
    return {
        "indicator": "Physical Flow",
        "periodFrom": f"{day.isoformat()}T06:00:00Z",
        "pointKey": point,
        "directionKey": "entry",
        "value": "1000000",
        "unit": "kWh/d",
    }


def _seed_flows(directory: Path, point: str, day: date) -> Path:
    body = directory / "raw_0900_a.json"
    body.write_text(json.dumps({"operationalData": [_flow(point, day)]}))
    body.with_suffix(".meta.json").write_text(json.dumps({"written_at": STAMP.isoformat()}))
    return body


def _seed_generic(directory: Path, record_id: str, day: date) -> Path:
    body = directory / "raw_0900_a.json"
    body.write_text(
        json.dumps(
            {
                "nominations": [
                    {
                        "id": record_id,
                        "periodFrom": f"{day.isoformat()}T06:00:00Z",
                        "pointKey": "ITP-00001",
                    }
                ]
            }
        )
    )
    body.with_suffix(".meta.json").write_text(json.dumps({"written_at": STAMP.isoformat()}))
    return body


# --------------------------------------------------------------------------- #
# Axis 1: the exact partition is present -> read it
# --------------------------------------------------------------------------- #


class TestExactPartitionPresent:
    def test_read_caller_returns_the_exact_partition(self, tmp_path: Path) -> None:
        """T3-g, READ caller."""
        exact = _partition(tmp_path, "physical_flows", TARGET)
        _seed_flows(exact, "ONDATE", TARGET)

        assert PhysicalFlowsTransformer(tmp_path)._bronze_path_for_date(TARGET) == exact

    def test_both_families_transform_the_exact_partition(self, tmp_path: Path) -> None:
        """T3-g, end to end."""
        _seed_flows(_partition(tmp_path, "physical_flows", TARGET), "ONDATE", TARGET)
        _seed_generic(_partition(tmp_path, GENERIC_DATASET, TARGET), "on", TARGET)

        assert PhysicalFlowsTransformer(tmp_path).run(TARGET, reingest=True) == 1
        assert get_transformer("entsog", GENERIC_DATASET, tmp_path).run(TARGET, reingest=True) == 1


# --------------------------------------------------------------------------- #
# Axis 2: the exact partition is absent, a covering one exists -> read NOTHING
# --------------------------------------------------------------------------- #


class TestExactPartitionAbsent:
    def test_read_caller_refuses_the_covering_partition(self, tmp_path: Path) -> None:
        """T3-h, READ caller -- F-05's open half."""
        covering = _partition(tmp_path, "physical_flows", COVERING)
        _seed_flows(covering, "WRONGDAY", COVERING)

        assert PhysicalFlowsTransformer(tmp_path)._bronze_path_for_date(TARGET) is None

    def test_transform_yields_no_rows_rather_than_the_covering_partitions(
        self, tmp_path: Path
    ) -> None:
        """T3-h, end to end: 0 rows, never a neighbouring day's rows relabelled."""
        covering = _partition(tmp_path, "physical_flows", COVERING)
        _seed_flows(covering, "WRONGDAY", COVERING)

        transformer = PhysicalFlowsTransformer(tmp_path)
        assert transformer.run(TARGET, reingest=True) == 0

        silver_dir = tmp_path / "silver" / "entsog" / "physical_flows"
        written = list(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []
        assert written == [], f"no silver may be written from a covering partition: {written}"
