"""On reingest, the plain path stamps a frame from the bodies it actually read.

``available_at`` is the newest sidecar stamp among ``_bronze_candidates``, the
same listing the reader reads, and nothing else. Each case below used to stamp
rows from other bronze than the rows came from: newer content with an older
stamp (lookahead), or the wall clock.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import gridflow.silver.neso  # noqa: F401 -- registers the NESO transformers
from gridflow.silver.elexon.bmunits import BMUnitsTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.entsoe.actual_load import ActualLoadTransformer
from gridflow.silver.gie.agsi import AboutListingTransformer, UnavailabilityTransformer
from gridflow.silver.openmeteo.forecast import ForecastDemandWeather
from gridflow.silver.registry import get_transformer
from gridflow.storage.paths import PathBuilder

FIXTURES = Path(__file__).parent.parent / "fixtures"
OLD = datetime(2026, 9, 1, 12, tzinfo=UTC)
NEW = datetime(2026, 9, 26, 19, 8, 5, tzinfo=UTC)


def _day(root: Path, source: str, dataset: str, day: date) -> Path:
    return PathBuilder(root).bronze_date_dir(source, dataset, day)


def _put(
    directory: Path,
    name: str,
    body: Any,
    *,
    written_at: datetime | None = None,
    sidecar_bytes: bytes | None = None,
    request_params: dict[str, str] | None = None,
) -> Path:
    """Write a bronze body and, unless both stamps are None, its own sidecar."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(body if isinstance(body, bytes) else json.dumps(body).encode())
    meta = path.with_suffix(".meta.json")
    if sidecar_bytes is not None:
        meta.write_bytes(sidecar_bytes)
    elif written_at is not None:
        meta.write_text(
            json.dumps({"written_at": written_at.isoformat(), "request_params": request_params})
        )
    return path


def _silver(root: Path, source: str, dataset: str) -> pl.DataFrame:
    files = sorted((root / "silver" / source / dataset).rglob("*.parquet"))
    assert files, f"no silver written for {source}/{dataset}"
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal")


def _set_mtime(path: Path, mtime: float) -> None:
    os.utime(path, (mtime, mtime))


def test_bmunits_reingest_stamps_the_newest_body_it_read(tmp_data_dir: Path) -> None:
    """Reingesting 1 Sep reads the 26 Sep body, so it carries the 26 Sep stamp."""
    _put(
        _day(tmp_data_dir, "elexon", "bmunits_reference", date(2026, 9, 1)),
        "raw_20260901T120000Z_a.json",
        {"data": [{"bmUnit": "OLD", "fuelType": "CCGT"}]},
        written_at=OLD,
    )
    _put(
        _day(tmp_data_dir, "elexon", "bmunits_reference", date(2026, 9, 26)),
        "raw_20260926T190805Z_b.json",
        {"data": [{"bmUnit": "NEW", "fuelType": "WIND"}]},
        written_at=NEW,
    )

    BMUnitsTransformer(tmp_data_dir).run(date(2026, 9, 1), reingest=True)

    df = _silver(tmp_data_dir, "elexon", "bmunits_reference")
    assert df["bm_unit_id"].to_list() == ["NEW"]
    assert df["available_at"].to_list() == [NEW]


def test_neso_reference_reingest_stamps_the_newest_body_it_read(tmp_data_dir: Path) -> None:
    _put(
        _day(tmp_data_dir, "neso", "intensity_factors", date(2026, 9, 1)),
        "raw_20260901T120000Z_a.json",
        {"data": [{"Gas": 100}]},
        written_at=OLD,
    )
    _put(
        _day(tmp_data_dir, "neso", "intensity_factors", date(2026, 9, 26)),
        "raw_20260926T190805Z_b.json",
        {"data": [{"Gas": 120}]},
        written_at=NEW,
    )

    get_transformer("neso", "intensity_factors", tmp_data_dir).run(date(2026, 9, 1), reingest=True)

    df = _silver(tmp_data_dir, "neso", "intensity_factors")
    assert df["factor_gco2_kwh"].to_list() == [120.0]
    assert df["available_at"].to_list() == [NEW]


def _listing(eic: str) -> dict[str, Any]:
    return {
        "data": [
            {
                "name": eic,
                "eic": eic,
                "country": "DE",
                "type": "SSO",
                "url": f"https://agsi.gie.eu/api?company={eic}",
                "facilities": [],
            }
        ]
    }


def _seed_two_listing_partitions(root: Path) -> tuple[Path, Path]:
    old_dir = _day(root, "gie_agsi", "about_listing", date(2026, 9, 1))
    new_dir = _day(root, "gie_agsi", "about_listing", date(2026, 9, 20))
    _put(old_dir, "raw_old.json", _listing("21X-OLD"), written_at=OLD)
    _put(new_dir, "raw_new.json", _listing("21X-NEW"), written_at=NEW)
    return old_dir, new_dir


@pytest.mark.parametrize("older_partition_touched_last", [False, True])
def test_agsi_fallback_reads_and_stamps_the_latest_date_partition(
    tmp_data_dir: Path, older_partition_touched_last: bool
) -> None:
    """No exact partition: the latest DATE is read and stamped, whatever the mtimes."""
    old_dir, new_dir = _seed_two_listing_partitions(tmp_data_dir)
    offset = 100 if older_partition_touched_last else -100
    _set_mtime(old_dir, new_dir.stat().st_mtime + offset)

    AboutListingTransformer(tmp_data_dir).run(date(2026, 9, 10), reingest=True)

    df = _silver(tmp_data_dir, "gie_agsi", "about_listing")
    assert set(df["entity_code"].to_list()) == {"21X-NEW"}
    assert set(df["available_at"].to_list()) == {NEW}


def test_agsi_fallback_stamp_ignores_an_mtime_bump_after_the_read(
    tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A concurrent write touching the older partition between read and stamp."""
    old_dir, new_dir = _seed_two_listing_partitions(tmp_data_dir)
    _set_mtime(old_dir, new_dir.stat().st_mtime - 100)
    original = AboutListingTransformer.read_bronze

    def read_then_touch(self: AboutListingTransformer, target_date: date) -> pl.DataFrame:
        frame = original(self, target_date)
        _set_mtime(old_dir, new_dir.stat().st_mtime + 100)
        return frame

    monkeypatch.setattr(AboutListingTransformer, "read_bronze", read_then_touch)

    AboutListingTransformer(tmp_data_dir).run(date(2026, 9, 10), reingest=True)

    df = _silver(tmp_data_dir, "gie_agsi", "about_listing")
    assert set(df["entity_code"].to_list()) == {"21X-NEW"}
    assert set(df["available_at"].to_list()) == {NEW}


def _outage(start: str, end: str) -> dict[str, Any]:
    return {
        "published": "2026-08-02 05:50:36",
        "country": {"name": "France", "code": "FR"},
        "company": {"name": "Storengy", "eic": "21X000000001083B"},
        "facility": {"name": "VGS SEDIANE B", "eic": "21W0000000000702"},
        "start": start,
        "end": end,
        "volume": "0.000",
        "injection": "0.0",
        "withdrawal": "167.5",
        "description": "N/A",
        "end_flag": "Confirmed",
        "type": "Planned",
    }


def test_agsi_unavailability_stamps_the_window_body_it_read(tmp_data_dir: Path) -> None:
    """The nearest prior directory is not what was read; the window body is."""
    window_written = datetime(2026, 8, 9, 7, tzinfo=UTC)
    _put(
        _day(tmp_data_dir, "gie_agsi", "unavailability", date(2026, 8, 1)),
        "raw_window.json",
        {"data": [_outage("2026-08-06 06:00:00", "2026-08-08 06:00:00")]},
        written_at=window_written,
        request_params={"start": "2026-08-01", "end": "2026-08-31"},
    )
    _put(
        _day(tmp_data_dir, "gie_agsi", "unavailability", date(2026, 8, 5)),
        "raw_other.json",
        {"data": [_outage("2026-08-05 06:00:00", "2026-08-05 12:00:00")]},
        written_at=datetime(2026, 8, 5, 7, tzinfo=UTC),
        request_params={"start": "2026-08-05", "end": "2026-08-05"},
    )

    UnavailabilityTransformer(tmp_data_dir).run(date(2026, 8, 7), reingest=True)

    df = _silver(tmp_data_dir, "gie_agsi", "unavailability")
    assert set(df["available_at"].to_list()) == {window_written}


_OM_TARGET = date(2024, 1, 15)


@pytest.mark.parametrize(
    ("london_day", "expected"),
    [
        # London exact, Birmingham in its covering partition: the max of both.
        (_OM_TARGET, datetime(2024, 1, 16, 10, tzinfo=UTC)),
        # Every location covering: still the bronze stamps, never the wall clock.
        (date(2024, 1, 13), datetime(2024, 1, 16, 10, tzinfo=UTC)),
    ],
)
def test_openmeteo_stamps_every_location_it_read(
    tmp_data_dir: Path, london_day: date, expected: datetime
) -> None:
    payload = json.loads((FIXTURES / "openmeteo" / "historical_london_response.json").read_text())
    _put(
        _day(tmp_data_dir, "open_meteo", "forecast_demand__london", london_day),
        "raw_a.json",
        payload,
        written_at=datetime(2024, 1, 15, 10, tzinfo=UTC),
    )
    _put(
        _day(tmp_data_dir, "open_meteo", "forecast_demand__birmingham", date(2024, 1, 13)),
        "raw_b.json",
        payload,
        written_at=datetime(2024, 1, 16, 10, tzinfo=UTC),
    )

    ForecastDemandWeather(tmp_data_dir).run(_OM_TARGET, reingest=True)

    df = _silver(tmp_data_dir, "open_meteo", "forecast_demand")
    assert set(df["location"].to_list()) == {"london", "birmingham"}
    assert set(df["available_at"].to_list()) == {expected}


_ELEXON_TARGET = date(2024, 1, 15)
_STAMP = datetime(2024, 1, 16, 9, 30, tzinfo=UTC)


@pytest.mark.parametrize("neighbour", ["orphan body", "undecodable sidecar", "stray file"])
def test_a_body_read_without_a_usable_sidecar_stamps_now(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture, neighbour: str
) -> None:
    """Never borrow a sibling's stamp, never crash; a non-body file is not read."""
    payload = json.loads((FIXTURES / "elexon" / "ndf_response.json").read_text())
    for row in payload["data"]:
        row["demand"] = row.pop("nationalDemand")
    day = _day(tmp_data_dir, "elexon", "indo", _ELEXON_TARGET)
    _put(day, "raw_a.json", payload, written_at=_STAMP)
    if neighbour == "orphan body":
        _put(day, "raw_b.json", payload)
    elif neighbour == "undecodable sidecar":
        # Invalid in UTF-8 and undefined in cp1252, so it fails to decode anywhere.
        _put(day, "raw_b.json", payload, sidecar_bytes=b"\x81\x8d\xff")
    else:
        _put(day, "raw_other.bin", b"not a body")

    before = datetime.now(UTC)
    with caplog.at_level(logging.WARNING):
        INDOTransformer(tmp_data_dir).run(_ELEXON_TARGET, reingest=True)
    after = datetime.now(UTC)

    stamps = set(_silver(tmp_data_dir, "elexon", "indo")["available_at"].to_list())
    if neighbour == "stray file":
        assert stamps == {_STAMP}
        return
    (stamp,) = stamps
    assert before <= stamp <= after
    assert "no usable sidecar" in caplog.text


def test_entsoe_default_hook_lists_only_xml_bodies(tmp_data_dir: Path) -> None:
    """A stray non-XML file next to a vouched body is neither read nor stamped."""
    day = _day(tmp_data_dir, "entsoe", "actual_load", _ELEXON_TARGET)
    body = _put(
        day,
        "raw_a.xml",
        (FIXTURES / "entsoe" / "actual_load_gb.xml").read_bytes(),
        written_at=_STAMP,
    )
    _put(day, "raw_other.json", b"{}")
    transformer = ActualLoadTransformer(tmp_data_dir)

    candidates = transformer._bronze_candidates(_ELEXON_TARGET)
    assert candidates == [body]
    assert transformer._vintage_of(candidates, _ELEXON_TARGET) == _STAMP
