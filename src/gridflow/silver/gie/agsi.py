"""Silver transformers for GIE AGSI gas storage transparency data."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

import polars as pl

from gridflow.connectors.gie.endpoints import parse_listing_inventory
from gridflow.schemas.gie import GasStorage
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


def _safe_float(val: Any) -> float | None:
    """Parse a string or numeric value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_date(val: Any) -> date | None:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except ValueError:
        return None


def _safe_datetime(val: Any) -> datetime | None:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.astimezone(UTC) if val.tzinfo else val.replace(tzinfo=UTC)
    text = str(val).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-":
        text = f"{text}T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _json_string(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _camel_to_snake(value: str) -> str:
    value = value.replace(" ", "_").replace("-", "_")
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"_+", "_", value).strip("_").lower()


def _normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        normalised = _camel_to_snake(str(key))
        if normalised in result and result[normalised] not in (None, ""):
            continue
        result[normalised] = value
    return result


def _extract_data_records(payload: Any, response_key: str = "data") -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []

    records = payload.get(response_key)
    if isinstance(records, list):
        return [row for row in records if isinstance(row, dict)]
    if isinstance(records, dict):
        return [records]

    for key, value in payload.items():
        if key == "meta":
            continue
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


class GasStorageTransformer(BaseSilverTransformer):
    """Transform GIE AGSI storage reports from bronze to silver."""

    source = "gie_agsi"
    dataset = "storage"
    schema_cls = GasStorage

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        for path in self._bronze_files(target_date):
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                logger.warning("Failed to parse GIE AGSI bronze file %s: %s", path, exc)
                continue

            request_params = _metadata_request_params(path)
            for record in _extract_data_records(payload):
                enriched = dict(record)
                enriched["__request_type"] = request_params.get("type")
                enriched["__request_country"] = request_params.get("country")
                enriched["__request_company"] = request_params.get("company")
                enriched["__request_facility"] = request_params.get("facility")
                rows.append(enriched)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, infer_schema_length=None)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        now = datetime.now(UTC)
        output: list[dict[str, Any]] = []
        for original in raw_df.to_dicts():
            row = _normalise_row(original)
            gas_day = _safe_date(_first(row, "gas_day_start", "gas_day"))
            if gas_day is None:
                logger.error("Missing required gas day in GIE AGSI storage row")
                continue

            request_type = _first(row, "__request_type", "request_type")
            request_country = _first(row, "__request_country", "request_country")
            request_company = _first(row, "__request_company", "request_company")
            request_facility = _first(row, "__request_facility", "request_facility")
            entity_level = _storage_entity_level(
                request_type=request_type,
                request_country=request_country,
                request_company=request_company,
                request_facility=request_facility,
            )
            entity_code = str(
                _first(row, "code", "entity_code", "eic", "country_code")
                or request_facility
                or request_company
                or request_country
                or request_type
                or ""
            )
            entity_name = str(_first(row, "name", "entity_name", "country_name") or entity_code)
            country_code = str(request_country or _first(row, "country_code", "country") or "")
            if not country_code and entity_level == "country":
                country_code = entity_code

            country_name = (
                entity_name if country_code == entity_code else _first(row, "country_name")
            )

            output.append(
                {
                    "gas_day": gas_day,
                    "gas_day_end": _safe_datetime(_first(row, "gas_day_end")),
                    "updated_at": _safe_datetime(_first(row, "updated_at")),
                    "entity_level": entity_level,
                    "entity_code": entity_code,
                    "entity_name": entity_name,
                    "entity_url": _first(row, "url", "entity_url"),
                    "country_code": country_code,
                    "country_name": country_name,
                    "gas_in_storage_gwh": _safe_float(_first(row, "gas_in_storage")),
                    "consumption_gwh": _safe_float(_first(row, "consumption")),
                    "consumption_full_pct": _safe_float(_first(row, "consumption_full")),
                    "injection_gwh": _safe_float(_first(row, "injection")),
                    "withdrawal_gwh": _safe_float(_first(row, "withdrawal")),
                    "net_withdrawal_gwh": _safe_float(_first(row, "net_withdrawal")),
                    "working_gas_volume_gwh": _safe_float(_first(row, "working_gas_volume")),
                    "injection_capacity_gwh_per_day": _safe_float(
                        _first(row, "injection_capacity")
                    ),
                    "withdrawal_capacity_gwh_per_day": _safe_float(
                        _first(row, "withdrawal_capacity")
                    ),
                    "contracted_capacity_gwh_per_day": _safe_float(
                        _first(row, "contracted_capacity")
                    ),
                    "available_capacity_gwh_per_day": _safe_float(
                        _first(row, "available_capacity")
                    ),
                    "covered_capacity_gwh_per_day": _safe_float(_first(row, "covered_capacity")),
                    "storage_pct_full": _safe_float(_first(row, "full", "storage_pct_full")),
                    "trend": _safe_float(_first(row, "trend")),
                    "status": _first(row, "status"),
                    "info": _json_string(_first(row, "info")),
                    "data_provider": "gie_agsi",
                    "ingested_at": now,
                }
            )

        if not output:
            return pl.DataFrame()

        deduped = {
            (
                row["gas_day"],
                row["entity_level"],
                row["entity_code"],
                row.get("entity_url"),
            ): row
            for row in output
        }
        df = pl.DataFrame(list(deduped.values()), infer_schema_length=None)
        ordered = [
            "gas_day",
            "gas_day_end",
            "updated_at",
            "entity_level",
            "entity_code",
            "entity_name",
            "entity_url",
            "country_code",
            "country_name",
            "gas_in_storage_gwh",
            "consumption_gwh",
            "consumption_full_pct",
            "injection_gwh",
            "withdrawal_gwh",
            "net_withdrawal_gwh",
            "working_gas_volume_gwh",
            "injection_capacity_gwh_per_day",
            "withdrawal_capacity_gwh_per_day",
            "contracted_capacity_gwh_per_day",
            "available_capacity_gwh_per_day",
            "covered_capacity_gwh_per_day",
            "storage_pct_full",
            "trend",
            "status",
            "info",
            "data_provider",
            "ingested_at",
        ]
        available = [col for col in ordered if col in df.columns]
        return df.select(available).sort(["gas_day", "entity_level", "entity_code"])

    def _bronze_files(self, target_date: date) -> list[Path]:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return []
        return [
            path
            for path in sorted(bronze_path.glob("raw_*.json"))
            if not path.name.endswith(".meta.json")
        ]


class StorageReportsTransformer(GasStorageTransformer):
    """Catalog-aligned AGSI storage reports transformer."""

    dataset = "storage_reports"
    # Excluded from VT1 schema enforcement: this generic catalog-aligned variant
    # is under rework on another branch. It subclasses GasStorageTransformer, so
    # without this explicit override it would inherit ``GasStorage`` — pin to
    # ``None`` so the central validator skips it (matches the VT1 worklist).
    # mypy narrows the inherited attr to ``type[GasStorage]`` at the parent, so the
    # ``None`` override needs a local ignore (the ABC's real type is broader).
    schema_cls = None  # type: ignore[assignment]


class AgsiJsonTransformer(BaseSilverTransformer):
    """Transform flat AGSI JSON records into deterministic silver output."""

    source = "gie_agsi"
    dataset: str
    fallback_to_latest_partition = True
    response_key = "data"
    datetime_columns = {
        "start_at",
        "end_at",
        "updated_at",
        "created_at",
        "event_start",
        "event_end",
        "publication_date",
        "publication_date_time",
        "gas_day_start",
        "gas_day_end",
    }
    numeric_suffixes = ("_volume", "_capacity", "_count", "_pct", "_percentage")
    numeric_names = {
        "value",
        "total",
        "capacity",
        "available_capacity",
        "unavailable_capacity",
        "working_gas_volume",
    }

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        for path in self._bronze_files(target_date):
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                logger.warning("Failed to parse AGSI bronze file %s: %s", path, exc)
                continue
            rows.extend(self._records_from_payload(payload))
        return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for original in raw_df.to_dicts():
            row = _normalise_row(original)
            normalised: dict[str, Any] = {}
            for key, value in row.items():
                if isinstance(value, (dict, list)):
                    normalised[key] = _json_string(value)
                elif key in self.datetime_columns or key.endswith("_at"):
                    normalised[key] = _safe_datetime(value)
                elif self._looks_numeric(key):
                    normalised[key] = _safe_float(value)
                else:
                    normalised[key] = value
            normalised["data_provider"] = "gie_agsi"
            normalised["ingested_at"] = datetime.now(UTC)
            rows.append(normalised)

        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows, infer_schema_length=None)
        dedup_subset = [
            col for col in ("id", "url", "turl", "entity_code", "eic") if col in df.columns
        ]
        if dedup_subset:
            df = df.unique(subset=dedup_subset, keep="last")
        sort_cols = [
            col
            for col in ("timestamp_utc", "start_at", "entity_level", "entity_code", "url")
            if col in df.columns
        ]
        return df.sort(sort_cols) if sort_cols else df

    def _records_from_payload(self, payload: Any) -> list[dict[str, Any]]:
        return _extract_data_records(payload, self.response_key)

    def _bronze_files(self, target_date: date) -> list[Path]:
        files = self._bronze_files_for_date(target_date)
        if files or not self.fallback_to_latest_partition:
            return files
        return self._latest_bronze_files()

    def _bronze_files_for_date(self, target_date: date) -> list[Path]:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return []
        return [
            path
            for path in sorted(bronze_path.glob("raw_*.json"))
            if not path.name.endswith(".meta.json")
        ]

    def _latest_bronze_files(self) -> list[Path]:
        files = self._all_bronze_files()
        if not files:
            return []
        latest_parent = max({path.parent for path in files}, key=lambda path: path.stat().st_mtime)
        return sorted(path for path in files if path.parent == latest_parent)

    def _all_bronze_files(self) -> list[Path]:
        return [
            path
            for path in self.bronze_dir.rglob("raw_*.json")
            if not path.name.endswith(".meta.json")
        ]

    def _looks_numeric(self, column: str) -> bool:
        return (
            column in self.numeric_names
            or column.endswith(self.numeric_suffixes)
            or column.startswith("total_")
        )


class AboutSummaryTransformer(AgsiJsonTransformer):
    """Transform AGSI about summary/reference payloads."""

    dataset = "about_summary"

    def _records_from_payload(self, payload: Any) -> list[dict[str, Any]]:
        records = _about_summary_records(payload)
        if records:
            return records
        return super()._records_from_payload(payload)


class AboutListingTransformer(AgsiJsonTransformer):
    """Transform AGSI flat listing payloads into company and facility rows."""

    dataset = "about_listing"

    def _records_from_payload(self, payload: Any) -> list[dict[str, Any]]:
        inventory = parse_listing_inventory(payload)
        rows: list[dict[str, Any]] = []
        for company in inventory.companies:
            rows.append(
                {
                    "entity_level": "company",
                    "entity_code": company.eic,
                    "entity_name": company.name,
                    "country_code": company.country,
                    "entity_type": company.entity_type,
                    "entity_url": company.url,
                }
            )
        for facility in inventory.facilities:
            rows.append(
                {
                    "entity_level": "facility",
                    "entity_code": facility.eic,
                    "entity_name": facility.name,
                    "country_code": facility.country,
                    "entity_type": facility.entity_type,
                    "entity_url": facility.url,
                    "company_code": facility.company_eic,
                    "company_name": facility.company_name,
                }
            )
        return rows


class NewsTransformer(AgsiJsonTransformer):
    """Transform AGSI service-announcement/news listing payloads."""

    dataset = "news"


class NewsItemTransformer(AgsiJsonTransformer):
    """Transform AGSI service-announcement/news detail payloads."""

    dataset = "news_item"

    def _records_from_payload(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return []
        records = super()._records_from_payload(payload)
        if records:
            return records
        return [payload] if isinstance(payload, dict) else []


class UnavailabilityTransformer(AgsiJsonTransformer):
    """Transform AGSI unavailability report payloads."""

    dataset = "unavailability"
    fallback_to_latest_partition = False

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        for path in self._bronze_files(target_date):
            try:
                payload = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                logger.warning("Failed to parse AGSI bronze file %s: %s", path, exc)
                continue
            for record in self._records_from_payload(payload):
                if _unavailability_record_overlaps(record, target_date):
                    rows.append(record)
        return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

    def _bronze_files(self, target_date: date) -> list[Path]:
        files = self._bronze_files_for_date(target_date)
        if files:
            return files

        return [
            path
            for path in self._all_bronze_files()
            if _metadata_request_window_contains(path, target_date)
        ]


def _storage_entity_level(
    *,
    request_type: Any,
    request_country: Any,
    request_company: Any,
    request_facility: Any,
) -> str:
    if request_facility:
        return "facility"
    if request_company:
        return "company"
    if request_country:
        return "country"
    if request_type:
        return "aggregate_type"
    return "country"


def _about_summary_records(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for company in _about_summary_companies(payload):
        company_data = company.get("data") if isinstance(company.get("data"), dict) else {}
        country = company_data.get("country") or company.get("country")
        company_code = str(company.get("eic") or company.get("code") or "")
        company_name = str(company.get("name") or company.get("short_name") or company_code)

        rows.append(
            {
                "entity_level": "company",
                "entity_code": company_code,
                "entity_name": company_name,
                "short_name": company.get("short_name"),
                "country_code": _nested_text(country, "code"),
                "country_name": _nested_text(country, "name"),
                "entity_type": company_data.get("type") or company.get("type"),
                "aggregate_code": company_data.get("code"),
                "aggregate_name": company_data.get("name"),
                "publication_link": company.get("publication_link"),
                "transparency_template": company.get("transparency_template"),
                "operational_information": company.get("operational_information"),
                "available_capacities": company.get("available_capacities"),
                "tariffs": company.get("tariffs"),
                "has_image": bool(company.get("image")),
            }
        )

        for facility in company.get("facilities") or []:
            if not isinstance(facility, dict):
                continue
            facility_country = facility.get("country") or country
            facility_code = str(facility.get("eic") or facility.get("code") or "")
            rows.append(
                {
                    "entity_level": "facility",
                    "entity_code": facility_code,
                    "entity_name": facility.get("name")
                    or facility.get("short_name")
                    or facility_code,
                    "country_code": _nested_text(facility_country, "code"),
                    "country_name": _nested_text(facility_country, "name"),
                    "entity_type": facility.get("type"),
                    "operational_start_date": facility.get("operational_start_date"),
                    "operational_end_date": facility.get("operational_end_date"),
                    "company_code": company_code,
                    "company_name": company_name,
                    "aggregate_code": company_data.get("code"),
                    "aggregate_name": company_data.get("name"),
                }
            )

    return rows


def _about_summary_companies(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [
            row
            for row in value
            if isinstance(row, dict) and isinstance(row.get("facilities"), list)
        ]
    if not isinstance(value, dict):
        return []

    if isinstance(value.get("facilities"), list):
        return [value]

    rows: list[dict[str, Any]] = []
    for child in value.values():
        rows.extend(_about_summary_companies(child))
    return rows


def _nested_text(value: Any, key: str) -> str | None:
    if isinstance(value, dict):
        nested = value.get(key)
        return str(nested) if nested not in (None, "") else None
    return str(value) if key == "code" and value not in (None, "") else None


def _unavailability_record_overlaps(record: dict[str, Any], target_date: date) -> bool:
    row = _normalise_row(record)
    event_start = _safe_date(_first(row, "event_start", "start_at", "gas_day_start"))
    event_end = _safe_date(_first(row, "event_end", "end_at", "gas_day_end"))
    if event_start is None and event_end is None:
        return True
    event_start = event_start or event_end
    event_end = event_end or event_start
    return event_start <= target_date <= event_end


def _metadata_request_window_contains(path: Path, target_date: date) -> bool:
    request_params = _metadata_request_params(path)
    start = _safe_date(request_params.get("start"))
    end = _safe_date(request_params.get("end"))
    if start is None and end is None:
        return False
    start = start or end
    end = end or start
    return start <= target_date <= end


def _metadata_request_params(path: Path) -> dict[str, Any]:
    meta_path = path.with_name(f"{path.stem}.meta.json")
    if not meta_path.exists():
        return {}
    try:
        metadata = json.loads(meta_path.read_text())
    except json.JSONDecodeError:
        return {}
    request_params = metadata.get("request_params", {})
    return request_params if isinstance(request_params, dict) else {}


def _register() -> None:
    register_transformer("gie_agsi", "storage", GasStorageTransformer)
    register_transformer("gie_agsi", "storage_reports", StorageReportsTransformer)
    register_transformer("gie_agsi", "about_summary", AboutSummaryTransformer)
    register_transformer("gie_agsi", "about_listing", AboutListingTransformer)
    register_transformer("gie_agsi", "news", NewsTransformer)
    register_transformer("gie_agsi", "news_item", NewsItemTransformer)
    register_transformer("gie_agsi", "unavailability", UnavailabilityTransformer)


_register()
