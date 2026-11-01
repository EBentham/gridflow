"""Silver dedups to each dataset's declared grain, and only conflicts count.

Every Elexon and ENTSO-E transformer ends ``transform()`` with
``dedup_to_grain``, so a column missing from ``ENTITY_KEY_COLUMNS`` is a row
silently lost. These tests pin the helper's accounting, that every transformer
declares a grain, and the columns the grain was missing.
"""

from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from pydantic import BaseModel

import gridflow.silver.elexon
import gridflow.silver.entsoe  # noqa: F401 -- registers every ENTSO-E transformer
from gridflow.schemas.elexon import ElexonUOU2T14D
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.elexon.boal import BOALTransformer
from gridflow.silver.elexon.imbalngc import ImbalNGCTransformer
from gridflow.silver.elexon.melngc import MelNGCTransformer
from gridflow.silver.elexon.pn import PNTransformer
from gridflow.silver.elexon.soso import SOSOTransformer
from gridflow.silver.elexon.uou2t14d import UOU2T14DTransformer
from gridflow.silver.registry import get_transformer_class, list_transformers

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"
SILVER_ROOT = Path(gridflow.silver.elexon.__file__).parent.parent


def _bare(cls: type[BaseSilverTransformer]) -> BaseSilverTransformer:
    return cls.__new__(cls)


class _Values(BaseModel):
    k: int
    v: float
    published_at: datetime | None = None


class _Probe(BaseSilverTransformer):
    source = "test"
    dataset = "probe"
    schema_cls: ClassVar[type[BaseModel] | None] = _Values
    ENTITY_KEY_COLUMNS = ("k",)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return self.dedup_to_grain(raw_df)


def test_dedup_to_grain_counts_only_conflicts() -> None:
    """An exact repeat drops quietly; a differing row on one grain is counted once."""
    raw = pl.DataFrame(
        {
            "k": [1, 1, 2, 2],
            "v": [10.0, 10.0, 20.0, 21.0],
            # A repeat from a later fetch differs only in non-value columns.
            "published_at": [
                datetime(2026, 9, 1, tzinfo=UTC),
                datetime(2026, 9, 2, tzinfo=UTC),
                None,
                None,
            ],
        }
    )
    probe = _bare(_Probe)

    out = probe.transform(raw)

    assert out["k"].to_list() == [1, 2]
    assert out.filter(pl.col("k") == 2)["v"].to_list() == [21.0]
    assert probe.last_excluded_row_count == 1


_REGISTERED = [*list_transformers("elexon"), *list_transformers("entsoe")]
_IDS = [f"{source}/{dataset}" for source, dataset in _REGISTERED]


@pytest.mark.parametrize(("source", "dataset"), _REGISTERED, ids=_IDS)
def test_every_transformer_declares_a_grain(source: str, dataset: str) -> None:
    assert len(_REGISTERED) > 70, "the Elexon and ENTSO-E registries did not load"
    cls = get_transformer_class(source, dataset)
    assert cls is not None
    assert cls.ENTITY_KEY_COLUMNS, f"{source}/{dataset} declares no ENTITY_KEY_COLUMNS"


def test_no_elexon_or_entsoe_module_dedups_inline() -> None:
    """The grain lives in ENTITY_KEY_COLUMNS only; an inline key drifts from it."""
    offenders = []
    for path in sorted(
        [*SILVER_ROOT.glob("elexon/*.py"), *SILVER_ROOT.glob("entsoe/*.py")]
    ):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "unique"
                and any(kw.arg == "subset" for kw in node.keywords)
            ):
                offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == []


def _pn_row(time_from: str, level: float) -> dict[str, object]:
    return {
        "settlementDate": "2026-09-15",
        "settlementPeriod": 1,
        "timeFrom": time_from,
        "timeTo": "2026-09-14T23:30:00Z",
        "levelFrom": level,
        "levelTo": level,
        "bmUnit": "T_ABC-1",
    }


def _boal_row(time_from: str, level: float) -> dict[str, object]:
    return {
        "settlementDate": "2026-09-15",
        "settlementPeriodFrom": 3,
        "timeFrom": time_from,
        "timeTo": "2026-09-15T01:00:00Z",
        "levelFrom": level,
        "levelTo": level,
        "acceptanceNumber": 11572,
        "acceptanceTime": "2026-09-15T00:00:00Z",
        "bmUnit": "T_ABC-1",
    }


@pytest.mark.parametrize(
    ("cls", "row"),
    [(PNTransformer, _pn_row), (BOALTransformer, _boal_row)],
    ids=["pn", "boal"],
)
def test_segments_reach_silver(cls: type[BaseSilverTransformer], row) -> None:  # type: ignore[no-untyped-def]
    """Two segments of one unit-period are two rows, each with its ``time_from``."""
    raw = pl.DataFrame(
        [row("2026-09-14T23:00:00Z", 1.0), row("2026-09-14T23:29:00Z", 0.0)]
    )
    transformer = _bare(cls)

    out = transformer.transform(raw)

    assert out["time_from"].sort().to_list() == [
        datetime(2026, 9, 14, 23, 0, tzinfo=UTC),
        datetime(2026, 9, 14, 23, 29, tzinfo=UTC),
    ]
    assert transformer.last_excluded_row_count == 0


def _ngc_rows(value_field: str) -> list[dict[str, object]]:
    base = {
        "publishTime": "2026-09-15T23:47:00Z",
        "settlementDate": "2026-09-16",
        "settlementPeriod": 3,
    }
    return [
        {**base, value_field: 1688, "boundary": "B1"},
        {**base, value_field: 55, "boundary": "N"},
    ]


def _uou_rows() -> list[dict[str, object]]:
    base = {
        "publishTime": "2026-09-15T08:00:00Z",
        "forecastDate": "2026-09-17",
        "fuelType": "WIND",
        "bmUnit": None,
    }
    return [
        {**base, "nationalGridBmUnit": "ABC-1", "outputUsable": 10},
        {**base, "nationalGridBmUnit": "ABC-2", "outputUsable": 20},
    ]


def _soso_rows() -> list[dict[str, object]]:
    base = {
        "publishTime": "2026-09-16T19:03:11Z",
        "contractIdentification": "SO_20260916_2100_9",
        "tradeDirection": "Bid",
        "tradeQuantity": 25.0,
        "tradePrice": 827.97,
        "startTime": "2026-09-16T21:00:00Z",
        "settlementDate": "2026-09-16",
    }
    return [{**base, "traderUnit": "MOYLE_SN"}, {**base, "traderUnit": "EWIC_SN"}]


@pytest.mark.parametrize(
    ("cls", "rows", "column"),
    [
        (ImbalNGCTransformer, _ngc_rows("imbalance"), "boundary"),
        (MelNGCTransformer, _ngc_rows("margin"), "boundary"),
        (UOU2T14DTransformer, _uou_rows(), "national_grid_bm_unit"),
        (SOSOTransformer, _soso_rows(), "trader_unit"),
    ],
    ids=[
        "imbalngc-boundary",
        "melngc-boundary",
        "uou2t14d-null-bmunit",
        "soso-trader_unit",
    ],
)
def test_grain_columns_preserved(
    cls: type[BaseSilverTransformer], rows: list[dict[str, object]], column: str
) -> None:
    """Two rows differing only in a grain column both survive, and validate."""
    transformer = _bare(cls)

    out = transformer.transform(pl.DataFrame(rows))

    assert out.height == 2
    assert out[column].n_unique() == 2
    assert transformer.last_excluded_row_count == 0
    assert transformer.schema_cls is not None
    for row in out.iter_rows(named=True):
        transformer.schema_cls(**row)


def test_null_bm_unit_row_validates() -> None:
    row = UOU2T14DTransformer.__new__(UOU2T14DTransformer).transform(
        pl.DataFrame(_uou_rows())
    )
    ElexonUOU2T14D(**row.row(0, named=True))


@pytest.mark.parametrize(
    ("cls", "fixture", "rows"),
    [
        (PNTransformer, "pn_response.json", 3),
        (BOALTransformer, "boal_response.json", 3),
        (UOU2T14DTransformer, "uou2t14d_response.json", 3),
    ],
    ids=["pn", "boal", "uou2t14d"],
)
def test_old_input_shapes_still_transform(
    cls: type[BaseSilverTransformer], fixture: str, rows: int
) -> None:
    """Bronze without ``timeFrom`` or ``nationalGridBmUnit`` keeps the old grain."""
    data = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))

    out = _bare(cls).transform(pl.DataFrame(data["data"]))

    assert out.height == rows
