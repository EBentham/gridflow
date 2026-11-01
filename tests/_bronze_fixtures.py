"""Write committed response fixtures into a bronze tree, as an ingest leaves them.

Not a test module. Transformer tests seed bronze with these, and
``tests/notebooks/conftest.py`` chains them into the notebooks' sample store.
Every response fixture used here covers 2024-01-15 (GIE's span 13 to 15 January).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.endpoints import DOC_TYPES
from gridflow.connectors.entsog.endpoints import ENDPOINTS as ENTSOG_ENDPOINTS
from gridflow.connectors.entsog.endpoints import build_params as build_entsog_params
from gridflow.connectors.neso.endpoints import ENDPOINTS as NESO_ENDPOINTS
from gridflow.connectors.neso.endpoints import build_path as build_neso_path
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from gridflow.silver.base import BaseSilverTransformer

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_DATE = date(2024, 1, 15)
_DAY_START = datetime(2024, 1, 15, tzinfo=UTC)
_DAY_END = _DAY_START + timedelta(days=1)
ENTSOE_BASE = "https://web-api.tp.entsoe.eu"


def write_fuelhh_bronze(
    data_dir: Path,
    target: date = FIXTURE_DATE,
    *,
    include_start_time: bool = True,
) -> None:
    """Drop one real fuelhh response into the bronze partition for ``target``."""
    bronze_dir = PathBuilder(data_dir).bronze_date_dir("elexon", "fuelhh", target)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    if not include_start_time:
        for row in payload["data"]:
            row.pop("startTime", None)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    (bronze_dir / "raw_test.meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": "fuelhh",
                "written_at": "2024-01-15T12:00:00+00:00",
                "data_date": target.isoformat(),
                "request_params": {
                    "publishDateTimeFrom": f"{target.isoformat()}T00:00:00Z",
                    "publishDateTimeTo": (f"{(target + timedelta(days=1)).isoformat()}T00:00:00Z"),
                },
                "page": 1,
                "total_pages": 1,
            }
        )
    )


def write_entsoe_fixture_bronze(tmp_data_dir: Path, dataset: str, fixture_name: str) -> Path:
    body = (FIXTURES / "entsoe" / fixture_name).read_bytes()
    response = RawResponse(
        body=body,
        content_type="text/xml",
        source="entsoe",
        dataset=dataset,
        fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        request_url=f"{ENTSOE_BASE}/api",
        request_params={"documentType": DOC_TYPES[dataset].document_type},
        api_version="v1",
        http_status=200,
        data_date=FIXTURE_DATE,
    )
    return BronzeWriter(tmp_data_dir).write(response)


def write_openmeteo_historical_bronze(
    data_dir: Path,
    target_date: date,
    fetched_at: datetime | None = None,
) -> None:
    payload = json.loads((FIXTURES / "openmeteo" / "historical_london_response.json").read_text())
    # F7.5: per-location bronze uses double-underscore separator.
    bronze_dir = PathBuilder(data_dir).bronze_date_dir(
        "open_meteo", "historical_demand__london", target_date
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    if fetched_at is not None:
        (bronze_dir / "raw_test.meta.json").write_text(
            json.dumps(
                {
                    "source": "open_meteo",
                    "dataset": "historical_demand__london",
                    "fetched_at": fetched_at.isoformat(),
                    "data_date": target_date.isoformat(),
                }
            )
        )


def _write_json_capture(
    data_dir: Path,
    source: str,
    dataset: str,
    fixture: str,
    request_url: str,
    request_params: dict[str, Any],
) -> Path:
    """Write a JSON response fixture through ``BronzeWriter``, sidecar and all."""
    response = RawResponse(
        body=(FIXTURES / fixture).read_bytes(),
        content_type="application/json",
        source=source,
        dataset=dataset,
        fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        request_url=request_url,
        request_params=request_params,
        api_version="v1",
        http_status=200,
        data_date=FIXTURE_DATE,
    )
    return BronzeWriter(data_dir).write(response)


def write_system_prices_bronze(data_dir: Path) -> Path:
    # The settlement date goes in the path, so the request has no parameters.
    return _write_json_capture(
        data_dir,
        "elexon",
        "system_prices",
        "elexon/system_prices_response.json",
        f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/{FIXTURE_DATE}",
        {},
    )


def write_carbon_intensity_bronze(data_dir: Path) -> Path:
    path, path_values = build_neso_path(
        NESO_ENDPOINTS["carbon_intensity"], start=_DAY_START, end=_DAY_END
    )
    return _write_json_capture(
        data_dir,
        "neso",
        "carbon_intensity",
        "neso/carbon_intensity_response.json",
        f"https://api.carbonintensity.org.uk{path}",
        path_values,
    )


def write_entsog_flows_bronze(data_dir: Path) -> Path:
    params = build_entsog_params(ENTSOG_ENDPOINTS["physical_flows"], start=_DAY_START, end=_DAY_END)
    return _write_json_capture(
        data_dir,
        "entsog",
        "physical_flows",
        "entsog/physical_flows_response.json",
        "https://transparency.entsog.eu/api/v1/operationaldatas",
        params,
    )


def write_bronze_payload(
    transformer: BaseSilverTransformer,
    target_date: date,
    payload: dict,
) -> None:
    bronze_dir = PathBuilder(transformer.data_dir).bronze_date_dir(
        transformer.source, transformer.dataset, target_date
    )
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload), encoding="utf-8")


def seed_gie_fixture(
    transformer: BaseSilverTransformer,
    target_date: date,
    filename: str,
) -> None:
    payload = json.loads((FIXTURES / "gie" / filename).read_text(encoding="utf-8"))
    write_bronze_payload(transformer, target_date, payload)
