"""Silver dedups to each dataset's declared grain, and only conflicts count.

Every Elexon and ENTSO-E transformer ends ``transform()`` with
``dedup_to_grain``, so a column missing from ``ENTITY_KEY_COLUMNS`` is a row
silently lost. These tests pin the helper's accounting, that every transformer
declares a grain, and the columns the grain was missing.
"""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from pydantic import BaseModel

import gridflow.silver.elexon
import gridflow.silver.entsoe  # noqa: F401 -- registers every ENTSO-E transformer
from gridflow.silver.base import BaseSilverTransformer
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
