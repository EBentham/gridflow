"""Shared builders for the held NESO gold view tests (v0.22-G, ADR-041).

Not a test module (no ``test_`` prefix). Each world writes synthetic bronze
captures in the real records' vendor headers, with the real registry's package
and resource identities, runs the real generated transformers, registers the
catalogue with the real ``refresh_views``, and only then executes a held view's
SQL itself (:func:`register_held`), so a test can assert on the catalogue both
before and after.
"""

from __future__ import annotations

import csv
import io
import os
import sys
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "unit"))

from _neso_generic_support import write_capture  # noqa: E402

from gridflow.connectors.neso_data_portal import registry as registry_module  # noqa: E402
from gridflow.gold.contracts import GoldViewContract, sql_path  # noqa: E402
from gridflow.silver.latest_views import latest_select_sql, select_latest_vintage  # noqa: E402
from gridflow.silver.neso_data_portal.completion import capture_id_for  # noqa: E402
from gridflow.silver.registry import get_transformer_class  # noqa: E402
from gridflow.storage.duckdb import refresh_views  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from datetime import date

SOURCE = "neso_data_portal"
TS = pl.Datetime("us", "UTC")


def _short_base() -> str:
    """The drive root on Windows (family keys plus run-id names pass MAX_PATH under the
    long per-user temp directory, ADR-036); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@contextmanager
def short_root() -> Iterator[Path]:
    """A temporary data root with a short path, removed on exit."""
    with tempfile.TemporaryDirectory(
        prefix="gg", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """A tz-aware UTC datetime."""
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def capture(
    data: Path,
    family: str,
    resource_index: int,
    header: Sequence[str],
    rows: Sequence[Sequence[str]],
    written: datetime,
    lm: datetime | None,
) -> str:
    """Write one bronze capture of ``family`` with its real registry identity.

    Args:
        data: The data root.
        family: The registry family key.
        resource_index: Which of the family's resources (registry order).
        header: The vendor header of one of the record's epochs.
        rows: The body rows, as vendor text.
        written: The sidecar ``written_at`` (also the bronze partition date).
        lm: The CKAN ``last_modified``; ``None`` writes a dump capture.

    Returns:
        The capture id.
    """
    package, _family = registry_module.load_registry().families[family]
    resource = [r for r in package.resources if r.family == family][resource_index]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    path, _sidecar = write_capture(
        data,
        family,
        package_slug=package.package,
        package_id=package.package_id,
        resource_id=resource.id,
        resource_name=resource.name,
        body=buffer.getvalue().encode("utf-8"),
        written_at=written,
        ckan_last_modified=lm.replace(tzinfo=None).isoformat() if lm is not None else None,
        url_type=resource.url_type,
        partition=written.date(),
    )
    return capture_id_for(path, data)


def run_family(data: Path, family: str, day: date) -> int:
    """Transform ``family``'s bronze partition ``day`` with its generated transformer."""
    cls = get_transformer_class(SOURCE, family)
    assert cls is not None, family
    return cls(data).run(day, run_id="g")


def catalogue(data: Path, seed: Callable[[Path], None]) -> Path:
    """Seed the existing gold views' inputs, then register with the real ``refresh_views``.

    Args:
        data: The data root.
        seed: The ``seed_silver`` fixture's callable (the support module never
            imports a conftest).

    Returns:
        The DuckDB catalogue path.
    """
    seed(data)
    db = data / "g.duckdb"
    refresh_views(db, data)
    return db


def register_held(db: Path, contract: GoldViewContract) -> None:
    """Execute a held view's SQL on the catalogue (the default glob never does)."""
    con = duckdb.connect(str(db))
    try:
        con.execute(sql_path(contract).read_text(encoding="utf-8"))
    finally:
        con.close()


def query(db: Path, sql: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    """Run ``sql`` read-only; TIMESTAMPTZ columns come back in UTC (``.pl()``, no pytz)."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        frame = con.execute(sql, params).pl() if params else con.execute(sql).pl()
    finally:
        con.close()
    return to_utc(frame)


def to_utc(frame: pl.DataFrame) -> pl.DataFrame:
    """Every tz-aware column converted to UTC."""
    return frame.with_columns(
        pl.col(name).dt.convert_time_zone("UTC").cast(TS)
        for name, dtype in frame.schema.items()
        if isinstance(dtype, pl.Datetime)
    )


def view_columns(db: Path, relation: str) -> list[str]:
    """The relation's columns, in order."""
    return query(db, f'SELECT * FROM "{relation}" LIMIT 0').columns


def both_as_of(db: Path, contract: GoldViewContract, as_of: datetime) -> pl.DataFrame:
    """The contract's point-in-time selection at ``as_of`` in SQL and in Polars.

    Asserts the two renderers return the same rows, then returns them sorted by
    the key and capture id.
    """
    spec = contract.point_in_time
    relation = contract.relation_name
    select = latest_select_sql(relation, spec, set(view_columns(db, relation)), as_of_param=True)
    assert select is not None
    sql_rows = query(db, select, {"as_of": as_of.isoformat()})
    frame = query(db, f'SELECT * FROM "{relation}"')
    polars_rows = select_latest_vintage(frame.lazy(), spec, as_of=as_of).collect()
    order = [*spec.key_columns, "bronze_capture_id"]
    left = sql_rows.sort(order, nulls_last=True)
    right = polars_rows.sort(order, nulls_last=True)
    assert left.to_dicts() == right.to_dicts()
    return left
