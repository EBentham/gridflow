"""Silver transformers whose catalogue relations exist by registration (ADR-034 I-1).

The catalogue (``storage/duckdb.py``) and the quality CLI stay source-free: a
transformer class that subclasses :class:`RegisteredRelationsTransformer`
declares the typed columns of its outputs and the support relations its
``_latest`` view reads, and the catalogue creates all of them for every
registered subclass, whether or not any Parquet has been written yet.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from gridflow.silver.base import BaseSilverTransformer

if TYPE_CHECKING:
    from pathlib import Path

    import polars as pl

__all__ = ["RegisteredRelationsTransformer", "SupportRelation"]


@dataclass(frozen=True)
class SupportRelation:
    """One relation a ``_latest`` view reads besides its base view.

    Attributes:
        name: The DuckDB relation name.
        directory: Where its Parquet files live.
        columns: Its ordered ``(name, DuckDB type)`` list, for the typed-empty
            view registered while ``directory`` holds no Parquet.
    """

    name: str
    directory: Path
    columns: tuple[tuple[str, str], ...]


class RegisteredRelationsTransformer(BaseSilverTransformer):
    """A transformer whose base, ``_latest`` and support relations always exist."""

    @classmethod
    @abstractmethod
    def output_columns(cls) -> list[tuple[str, str]]:
        """The ordered ``(name, DuckDB type)`` list of one written output.

        Includes the Hive partition columns DuckDB appends, in its order, so a
        typed-empty base view and a view over written Parquet agree.
        """

    @classmethod
    @abstractmethod
    def support_relations(cls, data_dir: Path) -> tuple[SupportRelation, ...]:
        """The relations this family's ``_latest`` view reads, under ``data_dir``."""

    @classmethod
    @abstractmethod
    def completions(cls, data_dir: Path) -> pl.LazyFrame:
        """The completion records a ``whole_capture`` selection needs."""
