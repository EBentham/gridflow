"""Every publication vintage reaches silver, and ``_latest`` picks the newest.

Moved from ``test_fou2t14d_vintage_retention.py`` and widened to every dataset
whose grain carries ``published_at``: the Elexon forecasts that keep each
publication and the ENTSO-E forecasts that keep each observation. Two bronze
bodies publish the same business keys at 06:00 and 18:00; silver must hold
both, ``_latest`` the 18:00 one, and an ``as_of`` of 12:00 the 06:00 one.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest

import gridflow.silver.elexon  # noqa: F401 -- registers every Elexon transformer
import gridflow.silver.entsoe  # noqa: F401 -- registers every ENTSO-E transformer
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.silver.registry import get_transformer_class
from gridflow.storage.duckdb import _register_views
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Callable

DAY = date(2024, 1, 15)
OLDER = datetime(2024, 1, 15, 6, tzinfo=UTC)
NEWER = datetime(2024, 1, 15, 18, tzinfo=UTC)
ENTSOE_FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"


def _stamp(at: datetime) -> str:
    return at.strftime("%Y-%m-%dT%H:%M:%SZ")


def _elexon(
    record: Callable[[float, str], dict[str, object]],
) -> Callable[[datetime, float], bytes]:
    return lambda at, value: json.dumps({"data": [record(value, _stamp(at))]}).encode()


_PERIOD = {"settlementDate": "2024-01-16", "settlementPeriod": 3}
_ELEXON_BODIES: dict[str, Callable[[datetime, float], bytes]] = {
    "fou2t14d": _elexon(
        lambda v, t: {
            "forecastDate": "2024-01-17",
            "fuelType": "WIND",
            "outputUsable": v,
            "publishTime": t,
        }
    ),
    "uou2t14d": _elexon(
        lambda v, t: {
            "forecastDate": "2024-01-17",
            "bmUnit": "T_ABC-1",
            "nationalGridBmUnit": "ABC-1",
            "fuelType": "WIND",
            "outputUsable": v,
            "publishTime": t,
        }
    ),
    "inddem": _elexon(lambda v, t: {**_PERIOD, "boundary": "N", "demand": v, "publishTime": t}),
    "indgen": _elexon(lambda v, t: {**_PERIOD, "boundary": "N", "generation": v, "publishTime": t}),
    "tsdf": _elexon(lambda v, t: {**_PERIOD, "boundary": "N", "demand": v, "publishTime": t}),
    "tsdfd": _elexon(lambda v, t: {"forecastDate": "2024-01-17", "demand": v, "publishTime": t}),
    "lolpdrm": _elexon(
        lambda v, t: {**_PERIOD, "lossOfLoadProbability": 0.0, "deratedMargin": v, "publishTime": t}
    ),
    "imbalngc": _elexon(
        lambda v, t: {**_PERIOD, "boundary": "N", "imbalance": v, "publishTime": t}
    ),
    "melngc": _elexon(lambda v, t: {**_PERIOD, "boundary": "N", "margin": v, "publishTime": t}),
}


def _entsoe(fixture: str) -> Callable[[datetime, float], bytes]:
    """The GB fixture re-stamped at ``at``, with every quantity shifted by ``value``."""
    text = (ENTSOE_FIXTURES / fixture).read_text(encoding="utf-8")

    def body(at: datetime, value: float) -> bytes:
        stamped = re.sub(
            r"<createdDateTime>[^<]*</createdDateTime>",
            f"<createdDateTime>{_stamp(at)}</createdDateTime>",
            text,
        )
        shifted = re.sub(
            r"<quantity>([^<]*)</quantity>",
            lambda m: f"<quantity>{float(m.group(1)) + value}</quantity>",
            stamped,
        )
        return shifted.encode()

    return body


_ENTSOE_BODIES: dict[str, Callable[[datetime, float], bytes]] = {
    name: _entsoe(f"{name}_gb.xml")
    for name in (
        "load_forecast",
        "load_forecast_weekly",
        "load_forecast_monthly",
        "load_forecast_yearly",
        "generation_forecast",
        "wind_solar_forecast",
    )
}

CASES = [("elexon", name) for name in _ELEXON_BODIES] + [
    ("entsoe", name) for name in _ENTSOE_BODIES
]


def _seed(data_dir: Path, source: str, dataset: str) -> None:
    bodies = _ELEXON_BODIES if source == "elexon" else _ENTSOE_BODIES
    suffix = "json" if source == "elexon" else "xml"
    partition = data_dir / "bronze" / source / dataset / "2024" / "01" / "15"
    partition.mkdir(parents=True)
    (partition / f"raw_20240115T0600Z.{suffix}").write_bytes(bodies[dataset](OLDER, 1000.0))
    (partition / f"raw_20240115T1800Z.{suffix}").write_bytes(bodies[dataset](NEWER, 1750.0))


@pytest.mark.parametrize(("source", "dataset"), CASES, ids=[f"{s}/{d}" for s, d in CASES])
def test_publication_vintages_reach_silver(tmp_path: Path, source: str, dataset: str) -> None:
    data_dir = tmp_path / "data"
    _seed(data_dir, source, dataset)
    cls = get_transformer_class(source, dataset)
    assert cls is not None

    written = cls(data_dir).run(DAY, run_id="vintages")

    silver = pl.read_parquet(
        PathBuilder(data_dir).silver_root() / source / dataset / "**" / "*.parquet"
    )
    assert written == silver.height
    # Each vintage is a full copy, and each row is available when it was published.
    assert silver["published_at"].value_counts().sort("published_at")["count"].to_list() == [
        silver.height // 2,
        silver.height // 2,
    ]
    assert silver["available_at"].to_list() == silver["published_at"].to_list()

    assert cls.APPEND_ONLY, "a run would overwrite the earlier observations"
    spec = LATEST_VIEW_SPECS[(source, dataset)]
    latest = select_latest_vintage(silver.lazy(), spec).collect()
    assert latest.height == silver.height // 2
    assert latest["published_at"].unique().to_list() == [NEWER]
    as_of = select_latest_vintage(silver.lazy(), spec, as_of=datetime(2024, 1, 15, 12, tzinfo=UTC))
    assert as_of.collect()["published_at"].unique().to_list() == [OLDER]

    con = duckdb.connect(":memory:")
    try:
        paths = PathBuilder(data_dir)
        _register_views(con, paths.silver_root(), paths.gold_root())
        view = f"silver_{source}_{dataset}_latest"
        sql = f"SELECT count(*), count(DISTINCT epoch_us(published_at)) FROM {view}"
        assert con.execute(sql).fetchone() == (latest.height, 1)
    finally:
        con.close()
