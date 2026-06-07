"""CH4-07 / CH-TEST-01 (C1-6): coverage for the GridflowClient.get_* SDK surface.

These are characterization tests for the six public ``get_*`` query helpers on
:class:`gridflow.serving.client.GridflowClient`. They build a small DuckDB
catalogue by hand (raw ``CREATE TABLE`` / ``INSERT`` — mirroring the ``tiny_duckdb``
fixture in ``test_client_reopen_readonly.py``) seeded with the SOURCE-QUALIFIED
silver/gold views each method actually queries (``silver_elexon_system_prices``,
``silver_elexon_fuelhh``, ``silver_elexon_itsdo``, ``gold_eu_gas_storage``,
``gold_uk_imbalance_context``), then assert:

  - each SILVER-backed method returns the expected rows for a date range;
  - each GOLD-backed method (``get_gas_storage``, ``get_imbalance_context``)
    returns the expected rows even though its gold SQL view carries none of the
    bitemporal columns;
  - the ``start``/``end`` filter values are BOUND parameters, not interpolated
    (a bound malformed date reaches the DATE caster as opaque data and raises a
    ``ConversionException`` — an interpolated quote would instead be a parser
    error; and a quote in a VARCHAR filter returns no rows rather than injecting);
  - a MISSING view raises a clear ``CatalogException`` (not a silent empty frame);
  - the bitemporal/partitioning columns are EXCLUDEd from the public surface;
  - the deprecated ``get_generation_by_fuel`` shim emits a ``DeprecationWarning``
    and reads ``silver_elexon_fuelhh``.

FIXED (CH4-07 stop-condition find, resolved on ``fix/ch4-architecture-hygiene``):
the two GOLD-backed methods used to ``SELECT * EXCLUDE`` six bitemporal columns
the gold SQL views never carry, so they raised ``BinderException`` on any real
catalogue. The client now excludes only the bitemporal columns ACTUALLY present
in the queried relation (``GridflowClient._present_bitemporal_exclude_clause``):
silver views still exclude all six, the gold SQL views exclude none and bind
cleanly. The gold fixtures stay modelled honestly (no bitemporal columns,
matching ``gold/views/*.sql``) and the three gold tests assert the real rows.

Coverage split (pinned): ``close()``/``reopen_readonly()``/context-manager
lifecycle live in ``test_client_reopen_readonly.py``; the ``get_*`` query surface
lives here.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import duckdb
import pytest

from gridflow.serving.client import _BITEMPORAL_EXCLUDE, GridflowClient

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

# The six bitemporal/partitioning columns the client SELECT * EXCLUDEs. Silver
# parquet views carry all six; the gold cross-source SQL views carry none of
# them, and the client now excludes only the columns actually present in the
# queried relation (so the gold methods bind cleanly with no EXCLUDE clause).
# Do NOT make the gold fixtures carry the six columns — that would mask the
# present-columns behavior the gold tests below assert.
_BITEMPORAL_COLS = list(_BITEMPORAL_EXCLUDE)


def _create_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    columns: Sequence[tuple[str, str]],
    rows: Sequence[tuple[object, ...]],
    *,
    with_bitemporal: bool = True,
) -> None:
    """Create one seeded table named ``name`` modelling a real silver/gold view.

    The fixture must mirror the ACTUAL on-disk/registered schema so that the
    client's ``SELECT * EXCLUDE (...)`` binds exactly as it would in production:

      - silver parquet views DO carry the six bitemporal/partitioning columns
        (``with_bitemporal=True``, the default);
      - the gold cross-source SQL views (``gold_eu_gas_storage``,
        ``gold_uk_imbalance_context``) are explicit-column SELECTs that carry
        NONE of the six (``with_bitemporal=False``). Faking those columns on a
        gold table would mask a real binder mismatch in the client.

    Args:
        con: An open writeable DuckDB connection.
        name: The view/table name the client SELECTs from (source-qualified).
        columns: ``(column_name, sql_type)`` pairs for the PUBLIC surface.
        rows: One tuple per row holding the public-column values in order.
        with_bitemporal: When True, append the six bitemporal/partitioning
            columns (silver layout). When False, the table holds only the public
            columns (gold SQL-view layout).
    """
    public_defs = ", ".join(f"{col} {sql_type}" for col, sql_type in columns)
    if with_bitemporal:
        bitemporal_defs = ", ".join(f"{col} VARCHAR" for col in _BITEMPORAL_COLS)
        con.execute(f"CREATE TABLE {name} ({public_defs}, {bitemporal_defs})")
    else:
        con.execute(f"CREATE TABLE {name} ({public_defs})")

    n_extra = len(_BITEMPORAL_COLS) if with_bitemporal else 0
    placeholders = ", ".join(["?"] * (len(columns) + n_extra))
    for row in rows:
        # Fill the bitemporal columns with stable sentinels; the client EXCLUDEs
        # them, so their values never reach an assertion.
        extra_vals = [f"bt_{col}" for col in _BITEMPORAL_COLS] if with_bitemporal else []
        con.execute(f"INSERT INTO {name} VALUES ({placeholders})", [*row, *extra_vals])


@pytest.fixture
def catalogue(tmp_path: Path) -> Path:
    """Build a DuckDB catalogue seeded with the five views the client queries.

    Returns the catalogue path; tests open a read-only GridflowClient over it.
    Each view's PUBLIC columns mirror the live silver/gold schema the matching
    ``get_*`` method filters/orders by; the bitemporal columns are appended by
    ``_create_view`` so ``SELECT * EXCLUDE (...)`` resolves.
    """
    db_path = tmp_path / "catalogue.duckdb"
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        _create_view(
            con,
            "silver_elexon_system_prices",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP"), ("price", "DOUBLE")],
            [
                ("2024-01-10", "2024-01-10 00:00:00", 40.0),
                ("2024-01-15", "2024-01-15 00:00:00", 50.0),
                ("2024-01-20", "2024-01-20 00:00:00", 60.0),
            ],
        )
        _create_view(
            con,
            "silver_elexon_fuelhh",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("fuel_type", "VARCHAR"),
                ("generation_mw", "DOUBLE"),
            ],
            [
                ("2024-01-15", "2024-01-15 00:00:00", "CCGT", 1000.0),
                ("2024-01-15", "2024-01-15 00:00:00", "WIND", 500.0),
                ("2024-01-25", "2024-01-25 00:00:00", "CCGT", 1100.0),
            ],
        )
        _create_view(
            con,
            "silver_elexon_itsdo",
            [("timestamp_utc", "TIMESTAMP"), ("location", "VARCHAR"), ("demand_mw", "DOUBLE")],
            [
                ("2024-01-15 00:00:00", "GB", 30000.0),
                ("2024-01-15 00:00:00", "LONDON", 5000.0),
                ("2024-01-25 00:00:00", "GB", 31000.0),
            ],
        )
        # Gold cross-source views are explicit-column SQL views with NO
        # bitemporal/partitioning columns (see gold/views/*.sql) — model them
        # honestly so the client's EXCLUDE binds against the real schema.
        _create_view(
            con,
            "gold_eu_gas_storage",
            [("gas_day", "DATE"), ("country_code", "VARCHAR"), ("pct_full", "DOUBLE")],
            [
                ("2024-01-15", "DE", 70.0),
                ("2024-01-15", "FR", 65.0),
                ("2024-01-25", "DE", 60.0),
            ],
            with_bitemporal=False,
        )
        _create_view(
            con,
            "gold_uk_imbalance_context",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("system_sell_price", "DOUBLE"),
            ],
            [
                ("2024-01-15", "2024-01-15 00:00:00", 55.0),
                ("2024-01-25", "2024-01-25 00:00:00", 65.0),
            ],
            with_bitemporal=False,
        )
    finally:
        con.close()
    return db_path


# --------------------------------------------------------------------------- #
# Happy-path: each method returns the expected rows for a date range
# --------------------------------------------------------------------------- #


def test_get_system_prices_returns_rows_in_range(catalogue: Path) -> None:
    """get_system_prices filters on settlement_date BETWEEN start AND end."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_system_prices("2024-01-12", "2024-01-18")
    finally:
        client.close()
    # Only the 2024-01-15 row falls in [12, 18]; 01-10 and 01-20 are excluded.
    assert df["settlement_date"].to_list() == [date(2024, 1, 15)]
    assert df["price"].to_list() == [50.0]


def test_get_fuel_generation_returns_rows_in_range(catalogue: Path) -> None:
    """get_fuel_generation returns the half-hourly fuel mix for the range."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_fuel_generation("2024-01-15", "2024-01-15")
    finally:
        client.close()
    # Both 01-15 fuel rows; the 01-25 row is out of range.
    assert sorted(df["fuel_type"].to_list()) == ["CCGT", "WIND"]
    assert df.height == 2


def test_get_weather_returns_rows_in_range(catalogue: Path) -> None:
    """get_weather reads silver_elexon_itsdo (demand-role), filtered by date."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_weather("2024-01-15", "2024-01-15")
    finally:
        client.close()
    assert df.height == 2
    assert sorted(df["location"].to_list()) == ["GB", "LONDON"]


def test_get_weather_location_filter(catalogue: Path) -> None:
    """The optional location filter narrows the result to one site."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_weather("2024-01-15", "2024-01-25", location="GB")
    finally:
        client.close()
    assert df["location"].unique().to_list() == ["GB"]
    assert df.height == 2


# The gold SQL views carry no bitemporal columns; the client now excludes only
# the columns actually present, so these methods bind cleanly and return the gold
# view's public columns verbatim (no EXCLUDE clause fires). Asserting the public
# columns are returned in full proves the present-columns helper picked an empty
# exclude set for the gold relations.
def test_get_gas_storage_returns_rows_in_range(catalogue: Path) -> None:
    """get_gas_storage reads gold_eu_gas_storage filtered by gas_day."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_gas_storage("2024-01-15", "2024-01-15")
    finally:
        client.close()
    assert df.height == 2
    assert sorted(df["country_code"].to_list()) == ["DE", "FR"]
    # The gold view's public columns are returned in full (no bitemporal EXCLUDE).
    assert set(df.columns) == {"gas_day", "country_code", "pct_full"}


def test_get_gas_storage_country_filter(catalogue: Path) -> None:
    """The optional country_code filter narrows to a single country."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_gas_storage("2024-01-15", "2024-01-25", country_code="DE")
    finally:
        client.close()
    assert df["country_code"].unique().to_list() == ["DE"]
    assert df.height == 2


def test_get_imbalance_context_returns_rows_in_range(catalogue: Path) -> None:
    """get_imbalance_context reads gold_uk_imbalance_context by settlement_date."""
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_imbalance_context("2024-01-14", "2024-01-16")
    finally:
        client.close()
    assert df.height == 1
    assert df["system_sell_price"].to_list() == [55.0]
    # The gold view's public columns are returned in full (no bitemporal EXCLUDE).
    assert set(df.columns) == {"settlement_date", "timestamp_utc", "system_sell_price"}


# --------------------------------------------------------------------------- #
# Bitemporal/partitioning columns are EXCLUDEd from the public surface
# --------------------------------------------------------------------------- #


# Scoped to the silver-backed methods: only their views carry the bitemporal
# columns, so a passing "not in df.columns" assert proves the EXCLUDE actually
# fired. The gold methods carry none of these columns (asserted by the gold tests
# above via the full public column set), so they need no separate EXCLUDE check.
@pytest.mark.parametrize(
    "method_name",
    [
        "get_system_prices",
        "get_fuel_generation",
        "get_weather",
    ],
)
def test_bitemporal_columns_excluded(catalogue: Path, method_name: str) -> None:
    """No silver-backed get_* method leaks the bitemporal/partitioning columns.

    A passing assert proves the SELECT * EXCLUDE (...) actually fired (the
    seeded silver tables carry all six columns).
    """
    client = GridflowClient(db_path=catalogue)
    try:
        df = getattr(client, method_name)("2024-01-01", "2024-12-31")
    finally:
        client.close()
    for excluded in _BITEMPORAL_EXCLUDE:
        assert excluded not in df.columns, f"{method_name} leaked excluded column {excluded}"


# --------------------------------------------------------------------------- #
# Parameter binding: start/end (and string filters) are bound, not interpolated
# --------------------------------------------------------------------------- #


def test_date_params_are_bound_not_interpolated(catalogue: Path) -> None:
    """A malformed bound date arg reaches the DATE caster as opaque data.

    Proof of parameterization: with binding, ``"not-a-date"`` hits DuckDB's date
    converter and raises a ConversionException. Were the value f-string
    interpolated into the SQL, a stray quote would instead surface as a
    ParserException (or silently inject). The conversion error is the discriminator.
    """
    client = GridflowClient(db_path=catalogue)
    try:
        with pytest.raises(duckdb.ConversionException):
            client.get_system_prices("not-a-date", "2024-12-31")
    finally:
        client.close()


def test_quote_in_date_param_does_not_inject(catalogue: Path) -> None:
    """A SQL-injection-style quote in a date arg cannot break out of the query.

    DuckDB's lenient date caster reads the leading ``2024-01-15`` and ignores the
    trailing ``' OR '1'='1`` junk, so the query still returns exactly the single
    in-range row — never the whole table, and never a parser error. That is only
    possible because the value is a bound parameter.
    """
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_system_prices("2024-01-15' OR '1'='1", "2024-01-15")
    finally:
        client.close()
    # If the quote had escaped the literal, the WHERE would be defeated and all
    # three rows returned; binding keeps it to the single 2024-01-15 row.
    assert df.height == 1
    assert df["price"].to_list() == [50.0]


def test_quote_in_string_filter_param_is_bound(catalogue: Path) -> None:
    """A quote in the VARCHAR location filter binds (returns no rows, no inject).

    An injection-style location string matches no row (there is no location named
    ``' OR '1'='1``), so the result is empty — a positive, exception-free proof
    that the filter value is parameterized rather than interpolated (which would
    have returned every row or raised a parser error).
    """
    client = GridflowClient(db_path=catalogue)
    try:
        df = client.get_weather("2024-01-01", "2024-12-31", location="' OR '1'='1")
    finally:
        client.close()
    assert df.is_empty()


# --------------------------------------------------------------------------- #
# Missing view raises a clear error (not a silent empty frame)
# --------------------------------------------------------------------------- #


def test_missing_view_raises_catalog_error(tmp_path: Path) -> None:
    """A get_* method over a catalogue lacking its view raises CatalogException.

    The empty catalogue has no silver_elexon_system_prices view, so the query
    must fail loudly rather than return an empty DataFrame that a caller could
    mistake for "no data in range".
    """
    db_path = tmp_path / "empty.duckdb"
    con = duckdb.connect(str(db_path), read_only=False)
    # A throwaway table so the file is a valid catalogue but lacks every view.
    con.execute("CREATE TABLE placeholder (x INTEGER)")
    con.close()

    client = GridflowClient(db_path=db_path)
    try:
        with pytest.raises(duckdb.CatalogException):
            client.get_system_prices("2024-01-01", "2024-12-31")
    finally:
        client.close()


# --------------------------------------------------------------------------- #
# Deprecated get_generation_by_fuel shim
# --------------------------------------------------------------------------- #


def test_get_generation_by_fuel_warns_and_reads_fuelhh(catalogue: Path) -> None:
    """The deprecated shim emits a DeprecationWarning and reads silver_elexon_fuelhh.

    It selects timestamp_utc/fuel_type/generation_mw from the fuelhh view (the
    silver_generation_by_fuel duplicate was removed), so the returned rows match
    the fuelhh data for the range.
    """
    client = GridflowClient(db_path=catalogue)
    try:
        with pytest.warns(DeprecationWarning, match="get_generation_by_fuel"):
            df = client.get_generation_by_fuel("2024-01-15", "2024-01-15")
    finally:
        client.close()
    assert sorted(df["fuel_type"].to_list()) == ["CCGT", "WIND"]
    assert set(df.columns) == {"timestamp_utc", "fuel_type", "generation_mw"}
    assert df["generation_mw"].to_list() == [1000.0, 500.0]
