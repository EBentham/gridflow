"""I-8/I-9: the behavioural usage proof for GridflowClient's serving-alias contract (D-8).

Static analysis of client method bodies was tried twice (a literal-vs-constant
comparison, then an AST source-shape scan) and evaded both times -- see
N-5-PLAN.md D-8. This suite replaces both with a BEHAVIOURAL proof: redirect
each handle's module constant to a sentinel relation and observe whether the
method actually reads it. An inlined literal cannot echo a sentinel it never
sees, so there is nothing left to evade.

Arm 1 (I-8, I-9 arm 1): the manifest row for each ``_SERVING_ALIASES`` entry
equals the client's module constant for that handle. Iterates
``_SERVING_ALIASES`` itself, not a ``relation_kind == "serving_alias"``
filter -- that would silently exclude the two gold-backed handles.

Arm 2a (I-9 arm 2a, the FROM site): seed the real relation and a
``sentinel_rel_<handle>`` relation with distinguishable rows; redirect the
constant to the sentinel; assert the returned frame carries the SENTINEL's
rows, not the real relation's.

Arm 2b (I-9 arm 2b, the exclude site): the discriminator is ``event_time``,
NOT ``available_at`` -- ``get_system_prices`` and ``get_imbalance_context``
both pass ``retain=_VINTAGE_VISIBLE`` (which contains ``available_at``), so an
``available_at`` discriminator would be vacuous for those two handles
(N-5-PLAN.md F17). ``event_time`` is in ``BITEMPORAL_EXCLUDE`` and retained by
nothing.

This file carries its own minimal ``_seed`` helper rather than importing the
private ``_create_view`` from ``test_serving_client.py``, which stays
UNTOUCHED (its unmodified pass is the A-10 evidence for D-8's constant lift).
Runs entirely offline against ``tmp_path`` -- never ``C:\\gridflow-data``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pytest

import gridflow.serving.client as client_module
from gridflow.serving.client import GridflowClient
from gridflow.silver import schema_manifest

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


def _seed(
    con: duckdb.DuckDBPyConnection,
    name: str,
    columns: Sequence[tuple[str, str]],
    rows: Sequence[tuple[object, ...]],
) -> None:
    """Create a minimal seeded table -- exactly the columns the caller declares.

    Unlike ``test_serving_client.py``'s ``_create_view``, this never
    auto-appends bitemporal sentinel columns: arm 2b needs precise, per-relation
    control over which bitemporal columns are present (specifically
    ``event_time``), so callers declare every column explicitly.
    """
    coldefs = ", ".join(f"{col} {sql_type}" for col, sql_type in columns)
    con.execute(f"CREATE TABLE {name} ({coldefs})")
    placeholders = ", ".join(["?"] * len(columns))
    for row in rows:
        con.execute(f"INSERT INTO {name} VALUES ({placeholders})", list(row))


def _catalogue(tmp_path: Path) -> Path:
    return tmp_path / "catalogue.duckdb"


# --------------------------------------------------------------------------- #
# Arm 1 (I-8, I-9 arm 1): manifest row == client constant, for ALL FIVE handles
# --------------------------------------------------------------------------- #

# {alias dataset -> (client method name, client constant name)}. Deliberately
# hand-maintained (not derived), so an unmapped _SERVING_ALIASES row is a
# FAILURE below, never a silent skip (F15).
_HANDLE_MAP: dict[str, tuple[str, str]] = {
    "system_prices": ("get_system_prices", "_REL_SYSTEM_PRICES"),
    "fuel_generation": ("get_fuel_generation", "_REL_FUEL_GENERATION"),
    "gas_storage": ("get_gas_storage", "_REL_GAS_STORAGE"),
    "weather": ("get_weather", "_REL_WEATHER"),
    "imbalance_context": ("get_imbalance_context", "_REL_IMBALANCE_CONTEXT"),
}

# get_generation_by_fuel is a deprecated shim with no _SERVING_ALIASES row and
# is deliberately excluded from _HANDLE_MAP by name (D-8) -- it duplicates
# silver_elexon_fuelhh and is not a member of the public serving-alias
# contract this suite proves.
_EXCLUDED_BY_NAME = "get_generation_by_fuel"


def test_every_sdk_serving_handle_reads_via_its_constant() -> None:
    """I-8 / I-9 arm 1: every _SERVING_ALIASES row matches its client constant.

    Iterates _SERVING_ALIASES itself, not a relation_kind == "serving_alias"
    filter (Sol pass-1 finding 3) -- that filter would have silently passed
    with the two gold-backed handles unchecked. An alias row absent from
    _HANDLE_MAP is a failure here, never a skip (F15).
    """
    assert _EXCLUDED_BY_NAME not in {method for method, _const in _HANDLE_MAP.values()}
    seen_datasets: set[str] = set()
    for spec in schema_manifest._SERVING_ALIASES:
        seen_datasets.add(spec.dataset)
        if spec.dataset not in _HANDLE_MAP:
            pytest.fail(
                f"_SERVING_ALIASES row {spec.dataset!r} has no entry in this test's "
                f"_HANDLE_MAP -- an unmapped serving-alias row is a coverage failure, "
                f"never a silent skip (F15, N-5-PLAN.md I-9)."
            )
        _method_name, constant_name = _HANDLE_MAP[spec.dataset]
        constant_value = getattr(client_module, constant_name)
        assert spec.relation_name == constant_value, (
            f"{spec.dataset}: manifest relation_name {spec.relation_name!r} != "
            f"client constant {constant_name}={constant_value!r}"
        )
    assert seen_datasets == set(_HANDLE_MAP), (
        f"_HANDLE_MAP covers {set(_HANDLE_MAP)} but _SERVING_ALIASES has "
        f"{seen_datasets} -- keep them in exact sync."
    )


def test_system_prices_alias_matches_client_relation() -> None:
    """I-8: the named regression pin for the specific row R1-A left stale.

    A failure here localises the defect immediately -- exactly what the D-4
    instance (the system_prices alias silently drifting stale) lacked.
    """
    (spec,) = [s for s in schema_manifest._SERVING_ALIASES if s.dataset == "system_prices"]
    assert spec.relation_name == client_module._REL_SYSTEM_PRICES


# --------------------------------------------------------------------------- #
# Arm 2a (I-9 arm 2a): the FROM site -- redirect the constant, observe rows
# --------------------------------------------------------------------------- #


def test_get_system_prices_reads_via_its_constant_from_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redirecting _REL_SYSTEM_PRICES to a sentinel returns the sentinel's rows."""
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "silver_elexon_system_prices_latest",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP"), ("marker", "VARCHAR")],
            [("2024-01-15", "2024-01-15 00:00:00", "real")],
        )
        _seed(
            con,
            "sentinel_rel_system_prices",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP"), ("marker", "VARCHAR")],
            [("2024-01-15", "2024-01-15 00:00:00", "sentinel")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_SYSTEM_PRICES", "sentinel_rel_system_prices")
    assert client_module._REL_SYSTEM_PRICES == "sentinel_rel_system_prices"  # redirect took effect

    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_system_prices("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert df["marker"].to_list() == ["sentinel"]


def test_get_fuel_generation_reads_via_its_constant_from_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "silver_elexon_fuelhh",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("fuel_type", "VARCHAR"),
                ("marker", "VARCHAR"),
            ],
            [("2024-01-15", "2024-01-15 00:00:00", "CCGT", "real")],
        )
        _seed(
            con,
            "sentinel_rel_fuel_generation",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("fuel_type", "VARCHAR"),
                ("marker", "VARCHAR"),
            ],
            [("2024-01-15", "2024-01-15 00:00:00", "CCGT", "sentinel")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_FUEL_GENERATION", "sentinel_rel_fuel_generation")
    assert client_module._REL_FUEL_GENERATION == "sentinel_rel_fuel_generation"

    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_fuel_generation("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert df["marker"].to_list() == ["sentinel"]


def test_get_gas_storage_reads_via_its_constant_from_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "gold_eu_gas_storage",
            [("gas_day", "DATE"), ("country_code", "VARCHAR"), ("marker", "VARCHAR")],
            [("2024-01-15", "DE", "real")],
        )
        _seed(
            con,
            "sentinel_rel_gas_storage",
            [("gas_day", "DATE"), ("country_code", "VARCHAR"), ("marker", "VARCHAR")],
            [("2024-01-15", "DE", "sentinel")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_GAS_STORAGE", "sentinel_rel_gas_storage")
    assert client_module._REL_GAS_STORAGE == "sentinel_rel_gas_storage"

    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_gas_storage("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert df["marker"].to_list() == ["sentinel"]


def test_get_weather_reads_via_its_constant_from_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "silver_elexon_itsdo",
            [("timestamp_utc", "TIMESTAMP"), ("location", "VARCHAR"), ("marker", "VARCHAR")],
            [("2024-01-15 00:00:00", "GB", "real")],
        )
        _seed(
            con,
            "sentinel_rel_weather",
            [("timestamp_utc", "TIMESTAMP"), ("location", "VARCHAR"), ("marker", "VARCHAR")],
            [("2024-01-15 00:00:00", "GB", "sentinel")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_WEATHER", "sentinel_rel_weather")
    assert client_module._REL_WEATHER == "sentinel_rel_weather"

    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_weather("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert df["marker"].to_list() == ["sentinel"]


def test_get_imbalance_context_reads_via_its_constant_from_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "gold_uk_imbalance_context",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP"), ("marker", "VARCHAR")],
            [("2024-01-15", "2024-01-15 00:00:00", "real")],
        )
        _seed(
            con,
            "sentinel_rel_imbalance_context",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP"), ("marker", "VARCHAR")],
            [("2024-01-15", "2024-01-15 00:00:00", "sentinel")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_IMBALANCE_CONTEXT", "sentinel_rel_imbalance_context")
    assert client_module._REL_IMBALANCE_CONTEXT == "sentinel_rel_imbalance_context"

    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_imbalance_context("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert df["marker"].to_list() == ["sentinel"]


# --------------------------------------------------------------------------- #
# Arm 2b (I-9 arm 2b): the exclude site -- a NON-RETAINED event_time asymmetry
# --------------------------------------------------------------------------- #
#
# get_system_prices and get_imbalance_context both retain=_VINTAGE_VISIBLE
# ("available_at",); using available_at as the discriminator would be
# VACUOUS for those two (F17) -- event_time is in BITEMPORAL_EXCLUDE and
# retained by nothing, so its presence/absence always moves the emitted
# clause regardless of retain= posture.


def test_system_prices_exclude_site_uses_its_constant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Silver-backed: sentinel WITHOUT event_time, real WITH it.

    Correct code introspects the (redirected) sentinel's columns to build the
    exclude clause, so it never tries to exclude a column the sentinel lacks
    and binds cleanly. An exclude call hardcoded to the real relation's
    literal would introspect columns the sentinel does not carry and raise
    BinderException instead (M6).
    """
    assert "event_time" not in client_module._VINTAGE_VISIBLE
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "silver_elexon_system_prices_latest",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("event_time", "TIMESTAMPTZ"),
            ],
            [("2024-01-15", "2024-01-15 00:00:00", "2024-01-15 00:00:00+00")],
        )
        _seed(
            con,
            "sentinel_rel_system_prices",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP")],
            [("2024-01-15", "2024-01-15 00:00:00")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_SYSTEM_PRICES", "sentinel_rel_system_prices")
    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_system_prices("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert "event_time" not in df.columns


def test_fuel_generation_exclude_site_uses_its_constant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Silver-backed, no retain=: sentinel WITHOUT event_time, real WITH it."""
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "silver_elexon_fuelhh",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("fuel_type", "VARCHAR"),
                ("event_time", "TIMESTAMPTZ"),
            ],
            [("2024-01-15", "2024-01-15 00:00:00", "CCGT", "2024-01-15 00:00:00+00")],
        )
        _seed(
            con,
            "sentinel_rel_fuel_generation",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP"), ("fuel_type", "VARCHAR")],
            [("2024-01-15", "2024-01-15 00:00:00", "CCGT")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_FUEL_GENERATION", "sentinel_rel_fuel_generation")
    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_fuel_generation("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert "event_time" not in df.columns


def test_weather_exclude_site_uses_its_constant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Silver-backed, no retain=: sentinel WITHOUT event_time, real WITH it."""
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "silver_elexon_itsdo",
            [
                ("timestamp_utc", "TIMESTAMP"),
                ("location", "VARCHAR"),
                ("event_time", "TIMESTAMPTZ"),
            ],
            [("2024-01-15 00:00:00", "GB", "2024-01-15 00:00:00+00")],
        )
        _seed(
            con,
            "sentinel_rel_weather",
            [("timestamp_utc", "TIMESTAMP"), ("location", "VARCHAR")],
            [("2024-01-15 00:00:00", "GB")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_WEATHER", "sentinel_rel_weather")
    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_weather("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert "event_time" not in df.columns


def test_gas_storage_exclude_site_uses_its_constant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gold-backed: INVERTED -- sentinel WITH event_time, real WITHOUT.

    Production gold views carry no bitemporal columns, so the discriminator
    must run the other way: correct code introspects the (redirected)
    sentinel's columns, finds event_time present, and EXCLUDEs it -- an
    exclude call hardcoded to the real relation's literal would see no
    event_time to exclude and leak it into the result instead.
    """
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "gold_eu_gas_storage",
            [("gas_day", "DATE"), ("country_code", "VARCHAR")],
            [("2024-01-15", "DE")],
        )
        _seed(
            con,
            "sentinel_rel_gas_storage",
            [("gas_day", "DATE"), ("country_code", "VARCHAR"), ("event_time", "TIMESTAMPTZ")],
            [("2024-01-15", "DE", "2024-01-15 00:00:00+00")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_GAS_STORAGE", "sentinel_rel_gas_storage")
    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_gas_storage("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert "event_time" not in df.columns


def test_imbalance_context_exclude_site_uses_its_constant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gold-backed, retain=_VINTAGE_VISIBLE: INVERTED, same as gas_storage.

    event_time is not in _VINTAGE_VISIBLE (only available_at is), so this
    works precisely because event_time -- unlike available_at -- is never
    retained for this handle (F17).
    """
    assert "event_time" not in client_module._VINTAGE_VISIBLE
    db_path = _catalogue(tmp_path)
    con = duckdb.connect(str(db_path))
    try:
        _seed(
            con,
            "gold_uk_imbalance_context",
            [("settlement_date", "DATE"), ("timestamp_utc", "TIMESTAMP")],
            [("2024-01-15", "2024-01-15 00:00:00")],
        )
        _seed(
            con,
            "sentinel_rel_imbalance_context",
            [
                ("settlement_date", "DATE"),
                ("timestamp_utc", "TIMESTAMP"),
                ("event_time", "TIMESTAMPTZ"),
            ],
            [("2024-01-15", "2024-01-15 00:00:00", "2024-01-15 00:00:00+00")],
        )
    finally:
        con.close()

    monkeypatch.setattr(client_module, "_REL_IMBALANCE_CONTEXT", "sentinel_rel_imbalance_context")
    client = GridflowClient(db_path=db_path)
    try:
        df = client.get_imbalance_context("2024-01-01", "2024-01-31")
    finally:
        client.close()
    assert "event_time" not in df.columns
