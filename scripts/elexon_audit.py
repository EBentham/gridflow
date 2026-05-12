"""Elexon end-to-end audit harness.

For each registered Elexon endpoint:
  1. Attempt a minimal live fetch (yesterday, 1-hour window for PUBLISH_DATETIME,
     single date for SETTLEMENT_DATE / DATE_PATH, all periods for
     SETTLEMENT_DATE_PERIOD, single call for NO_PARAMS).
  2. Write any responses to a temp bronze dir.
  3. Run the registered silver transformer.
  4. Record connector/transformer status + sample row count.

Writes results to scripts/elexon_audit_results.json.

Usage
-----
    python scripts/elexon_audit.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import tempfile
import traceback
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("elexon_audit")
logger.setLevel(logging.INFO)

from gridflow.bronze.writer import BronzeWriter  # noqa: E402
from gridflow.config.settings import load_settings  # noqa: E402
from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle  # noqa: E402
from gridflow.connectors.registry import get_connector  # noqa: E402
from gridflow.silver.registry import get_transformer, list_transformers  # noqa: E402

import gridflow.connectors.elexon  # noqa: E402,F401  trigger registration
import gridflow.silver.elexon  # noqa: E402,F401   trigger registration


def _fetch_window(param_style: ParamStyle, target: date) -> tuple[datetime, datetime]:
    start = datetime(target.year, target.month, target.day, 0, 0, tzinfo=UTC)
    if param_style == ParamStyle.PUBLISH_DATETIME:
        end = start + timedelta(hours=1)
    else:
        end = start + timedelta(days=1)
    return start, end


async def _smoke_fetch(connector, dataset: str, start: datetime, end: datetime):
    async with connector:
        return await connector.fetch(dataset, start, end)


def audit_endpoint(
    dataset: str,
    endpoint,
    source_config,
    bronze_root: Path,
    target_date: date,
) -> dict:
    result = {
        "endpoint": dataset,
        "param_style": endpoint.param_style.value,
        "path": endpoint.path,
        "fetch_ok": False,
        "fetch_error": None,
        "n_responses": 0,
        "total_body_bytes": 0,
        "transformer_registered": False,
        "transformer_class": None,
        "transform_ok": False,
        "transform_error": None,
        "n_silver_rows": 0,
        "sample_columns": [],
    }

    # Step 1: connector fetch
    start, end = _fetch_window(endpoint.param_style, target_date)
    try:
        connector = get_connector("elexon", source_config)
        responses = asyncio.run(_smoke_fetch(connector, dataset, start, end))
        result["fetch_ok"] = True
        result["n_responses"] = len(responses)
        result["total_body_bytes"] = sum(len(r.body) for r in responses)
    except Exception as exc:
        result["fetch_error"] = f"{type(exc).__name__}: {exc}"
        return result

    if not responses:
        result["fetch_error"] = "no responses returned"
        return result

    # Step 2: write to bronze
    data_dir = bronze_root / dataset
    data_dir.mkdir(parents=True, exist_ok=True)
    writer = BronzeWriter(data_dir)
    for resp in responses:
        writer.write(resp)

    # Step 3: locate transformer
    registered = list_transformers("elexon")
    if ("elexon", dataset) not in registered:
        result["transformer_registered"] = False
        return result
    result["transformer_registered"] = True

    try:
        transformer = get_transformer("elexon", dataset, data_dir)
        result["transformer_class"] = type(transformer).__name__
        # Determine the partition date the bronze writer used
        partition_date = responses[0].data_date or responses[0].fetched_at.date()
        df = transformer.transform(transformer.read_bronze(partition_date))
        result["transform_ok"] = True
        result["n_silver_rows"] = df.height
        result["sample_columns"] = df.columns
    except Exception as exc:
        result["transform_error"] = f"{type(exc).__name__}: {exc}"
        result["transform_traceback"] = traceback.format_exc(limit=4)

    return result


def main() -> None:
    settings = load_settings()
    source_config = settings.get_source_config("elexon")

    target = date.today() - timedelta(days=2)  # 2 days back to ensure data settled

    registered = set(list_transformers("elexon"))
    results: list[dict] = []

    with tempfile.TemporaryDirectory(prefix="elexon_audit_") as tmp:
        bronze_root = Path(tmp)
        for dataset, endpoint in ENDPOINTS.items():
            print(f"--- {dataset} ({endpoint.param_style.value}) ---")
            r = audit_endpoint(dataset, endpoint, source_config, bronze_root, target)
            status = (
                "OK" if r["fetch_ok"] and r["transform_ok"]
                else "FETCH_OK" if r["fetch_ok"]
                else "FAIL"
            )
            print(
                f"   {status} | fetch={r['fetch_ok']} bytes={r['total_body_bytes']} "
                f"transform={r['transform_ok']} silver_rows={r['n_silver_rows']}"
            )
            if r["fetch_error"]:
                print(f"   fetch_error: {r['fetch_error']}")
            if r["transform_error"]:
                print(f"   transform_error: {r['transform_error']}")
            results.append(r)

    transformer_only = sorted(
        ds for src, ds in registered if ds not in ENDPOINTS
    )

    summary = {
        "target_date": target.isoformat(),
        "audited_at": datetime.now(UTC).isoformat(),
        "n_endpoints": len(ENDPOINTS),
        "n_transformers_registered": len(registered),
        "transformers_without_endpoint": transformer_only,
        "results": results,
    }

    out_path = _PROJECT_ROOT / "scripts" / "elexon_audit_results.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
