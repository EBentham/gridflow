"""One-shot seeder for the NESO Data Portal registry (ADR-033, PLAN P-2).

Run once, by the unit-A executor, against the snapshot of record and the
ratified dataset matrix; the committed registry JSON files are the artifact.
**Never re-run after the capture sweep**: keys freeze once bronze exists (P-4),
and this script refuses to write into a directory that already holds any
``*.json``. Its inputs live under the git-ignored ``.planning/`` tree, so no test
imports it; its one run is evidence in the PR body.

Usage::

    python scripts/seed_neso_registry.py --snapshot <catalog-snapshot.json> \
        --matrix <v0.22-DATASET-MATRIX.md> --out <registry dir> \
        [--emit-fixture <path>]
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from gridflow.connectors.neso_data_portal.registry import (
    KEY_PATTERN,
    LEGACY_KEYS,
    RegistryError,
    dump_json,
    load_registry,
)

_MIB = 1024 * 1024
DEFAULT_MAX_DOWNLOAD_BYTES = 512 * _MIB

STEM_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,27}$")
MAX_KEY_LENGTH = 40

STOPWORDS = frozenset({"the", "of", "and", "for", "to", "a", "in", "by", "with", "data"})

HOLD_N_QUESTION = "third-party origin cited in package notes; licence carve-out"

LEGACY_FAMILIES: dict[str, tuple[str, int]] = {
    # package slug -> (legacy key, max_download_bytes) — values unchanged from
    # endpoints.DATASETS on master f64e745.
    "daily-wind-availability": ("daily_wind_availability", 8 * _MIB),
    "historic-generation-mix": ("historic_generation_mix", 256 * _MIB),
    "embedded-wind-and-solar-forecasts": ("embedded_wind_solar_forecast", 8 * _MIB),
}

EMBEDDED_PACKAGE = "embedded-wind-and-solar-forecasts"
EMBEDDED_LIVE_NAME = "Embedded Solar and Wind Forecast"
EMBEDDED_ARCHIVE_KEY = "embedded_wind_solar_forecast_archive"

DOC_FORMATS = frozenset({"PDF", "PNG", "DOC", "PPT", "TXT"})
GIS_FORMATS = frozenset({"GEOJSON", "GPKG"})
CLASSIFY_FORMATS = frozenset({"XLSX", "XLSM", "ZIP"})

# Hand-written, one per package. Legacy packages' stems are their legacy keys.
PACKAGE_STEMS: dict[str, str] = {
    "1-day-ahead-demand-forecast": "demand_forecast_1d",
    "14-days-ahead-operational-metered-wind-forecasts": "metered_wind_forecast_14d",
    "14-days-ahead-wind-forecasts": "wind_forecast_14d",
    "2-14-days-ahead-national-demand-forecast": "national_demand_fc_2_14d",
    "2-day-ahead-demand-forecast": "demand_forecast_2d",
    "24-months-ahead-constraint-cost-forecast": "constraint_cost_fc_24m",
    "24-months-ahead-constraint-limits": "constraint_limits_24m",
    "7-day-ahead-national-forecast": "national_forecast_7d",
    "aahedc-tariffs": "aahedc_tariffs",
    "aggregated-bsad": "aggregated_bsad",
    "ancillary-services-important-industry-notifications": "as_industry_notifications",
    "balancing-reserve-auction-requirement-forecast": "br_auction_requirement_fc",
    "balancing-services-adjustment-data-forward-contracts": "bsad_forward_contracts",
    "balancing-services-contract-enactment": "bs_contract_enactment",
    "balancing-services-use-of-system-bsuos-daily-forecast": "bsuos_daily_forecast",
    "brit-ned": "brit_ned",
    "bsuos-fixed-tariffs": "bsuos_fixed_tariffs",
    "bsuos-monthly-forecast": "bsuos_monthly_forecast",
    "building-heat-model": "building_heat_model",
    "capacity-market-register": "capacity_market",
    "carbon-intensity-of-balancing-actions": "ci_balancing_actions",
    "constraint-breakdown": "constraint_breakdown",
    "constraint-management-intertrip-service-information-cmis": "cmis_intertrip",
    "contract-transfer-of-obligation": "contract_transfer_obligation",
    "country-carbon-intensity-forecast": "country_ci_forecast",
    "current-balancing-services-use-of-system-bsuos-data": "current_bsuos",
    "daily-balancing-costs-balancing-services-use-of-system": "daily_balancing_costs",
    "daily-balancing-volume-balancing-services-use-of-system": "daily_balancing_volume",
    "daily-demand-update": "daily_demand_update",
    "daily-opmr": "daily_opmr",
    "daily-wind-availability": "daily_wind_availability",
    "data-portal-planned-changes-known-issues": "portal_known_issues",
    "day-ahead-constraint-flows-and-limits": "da_constraint_flows_limits",
    "day-ahead-half-hourly-demand-forecast-performance": "da_demand_fc_performance",
    "day-ahead-power-exchange-prices-nordpool": "nordpool_da_prices",
    "day-ahead-wind-forecast": "da_wind_forecast",
    "demand-flexibility": "demand_flexibility",
    "demand-flexibility-service": "dfs",
    "demand-flexibility-service-live-events": "dfs_live_events",
    "demand-flexibility-service-test-events": "dfs_test_events",
    "demand-profile-dates": "demand_profile_dates",
    "disaggregated-bsad": "disaggregated_bsad",
    "dynamic-containment-4-day-forecast": "dc_forecast_4d",
    "dynamic-containment-data": "dynamic_containment",
    "dynamic-moderation-requirements": "dm_requirements",
    "dynamic-regulation-requirements": "dr_requirements",
    "eac-auction-results": "eac_results",
    "eac-br-auction-results": "eac_br_results",
    "eac-br-mock-auction-results": "eac_br_mock_results",
    "eac-mock-auction-results": "eac_mock_results",
    "eleclink": "eleclink",
    "embedded-register": "embedded_register",
    "embedded-wind-and-solar-forecasts": "embedded_wind_solar_forecast",
    "etys-gb-transmission-system-boundaries": "etys_boundaries",
    "fes-electricity-demand-summary-data-table-ed1": "fes_ed1_electricity_demand",
    "fes-european-electricity-supply-data-table-es2": "fes_es2_european_supply",
    "fes-flexibility-data-table-data-table-flx1": "fes_flx1_flexibility",
    "fes-natural-gas-demand-definitions-ed4": "fes_ed4_gas_demand_defs",
    "fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3": (
        "fes_ed3_heat_demand"
    ),
    "fes-road-transport-notes-data-table-ed6": "fes_ed6_road_transport_notes",
    "fes-road-transport-summary-data-table-ed5": "fes_ed5_road_transport",
    "fes-whole-system-gas-supply-data-table-ws1": "fes_ws1_gas_supply",
    "fes-whole-system-gas-supply-emissions-data-table-ws2": "fes_ws2_gas_emissions",
    "firm-frequency-response-post-tender-reports": "ffr_post_tender_reports",
    "future-energy-scenario-electricity-supply-data-table-es1": "fes_es1_electricity_supply",
    "future-energy-scenario-fes-building-block-data": "fes_building_blocks",
    "gb-system-inertia-bid-and-offer-costs": "inertia_bid_offer_costs",
    "gis-boundaries-for-gb-dno-license-areas": "gis_dno_license_areas",
    "gis-boundaries-for-gb-generation-charging-zones": "gis_gen_charging_zones",
    "gis-boundaries-for-gb-grid-supply-points": "gis_grid_supply_points",
    "historic-demand-data": "historic_demand",
    "historic-generation-mix": "historic_generation_mix",
    "historic-gtma-grid-trade-master-agreement-trades-data": "gtma_trades",
    "ifa": "ifa",
    "ifa2": "ifa2",
    "index-linked-contract-volume": "index_linked_contract_volume",
    "interconnector-register": "interconnector_register",
    "interconnector-requirement-and-auction-summary-data": "ic_requirement_auction",
    "levelised-cost-of-green-hydrogen": "lcoh",
    "local-authority-level-spatial-heat-model-outputs-fes": "fes_la_heat_model",
    "long-term-2-52-weeks-ahead-national-demand-forecast": "demand_forecast_2_52w",
    "long-term-forecasts-for-dc-dm-dr-requirements": "dc_dm_dr_long_term_fc",
    "monthly-operational-metered-wind-output": "metered_wind_output_monthly",
    "monthly-utilisation-data-of-voltage-contracted-units": "voltage_units_utilisation",
    "national-carbon-intensity-forecast": "national_ci_forecast",
    "national-demand-balancing-mechanism-units": "national_demand_bmus",
    "negative-reserve-active-power-margin-nrapm-forecast": "nrapm_forecast",
    "nemolink": "nemolink",
    "non-bm-ancillary-service-dispatch-platform-asdp-instructions": "asdp_instructions",
    "non-bm-ancillary-service-dispatch-platform-asdp-window-prices": "asdp_window_prices",
    "nsl": "nsl",
    "obligatory-reactive-power-service-orps-utilisation": "orps_utilisation",
    "obp-non-bm-physical-notifications": "obp_physical_notifications",
    "obp-non-bm-reserve-instructions": "obp_reserve_instructions",
    "obp-reserve-availability-utilisation-price": "obp_reserve_avail_price",
    "operational-transparency-forum-network-congestion-data": "otf_network_congestion",
    "optional-downward-flexibility-management-odfm-market-information": ("odfm_market_information"),
    "outturn-voltage-costs": "outturn_voltage_costs",
    "phase-2-ffr-auction-results-summary": "ffr_phase2_auction",
    "quick-reserve-auction-requirement-forecast": "qr_auction_requirement_fc",
    "regional-breakdown-of-fes-data-electricity": "fes_regional",
    "regional-carbon-intensity-forecast": "regional_ci_forecast",
    "resource-adequacy-in-2030s": "resource_adequacy_2030s",
    "school-holiday-percentages": "school_holiday_percentages",
    "short-term-operating-reserve-stor-day-ahead-auction-results": "stor_da_auction_results",
    "short-term-operating-reserve-stor-day-ahead-buy-curve": "stor_da_buy_curve",
    "skip-rates": "skip_rates",
    "slow-reserve-requirement-forecast": "slow_reserve_requirement_fc",
    "ssep-onshore-publication-zone-shapefile": "ssep_publication_zones",
    "stability-midterm-y-1-utilisation-report": "stability_midterm_y1",
    "stability-pathfinder-service-information": "stability_pathfinder",
    "static-firm-frequency-response-auction-results": "sffr_auction_results",
    "static-firm-frequency-response-requirement": "sffr_requirement",
    "stor-windows": "stor_windows",
    "super-stable-export-limit-contract-enactment": "ssel_contract_enactment",
    "system-frequency-data": "system_frequency",
    "system-inertia": "system_inertia",
    "system-inertia-cost": "system_inertia_cost",
    "system-operating-plan-sop": "system_operating_plan",
    "thermal-constraint-costs": "thermal_constraint_costs",
    "transmission-entry-capacity-tec-register": "tec_register",
    "transmission-losses": "transmission_losses",
    "transmission-network-use-of-system-tnuos-tariffs": "tnuos_tariffs",
    "tresp-demand-pathways": "tresp_demand_pathways",
    "tresp-generation-pathways": "tresp_generation_pathways",
    "upcoming-trades": "upcoming_trades",
    "viking": "viking",
    "voltage-requirement": "voltage_requirement",
    "weekly-opmr": "weekly_opmr",
    "weekly-wind-availability": "weekly_wind_availability",
    "wind-bmu-boa-volumes": "wind_bmu_boa_volumes",
}

# (package slug, norm) -> key. Set where the mechanical rule's truncation left
# in-package collisions resolved only by `_2`, `_3` (an opaque, permanently frozen
# key); each override names what distinguishes the family.
FAMILY_KEY_OVERRIDES: dict[tuple[str, str], str] = {
    (
        "14-days-ahead-operational-metered-wind-forecasts",
        "day ahead operational metered wind forecast",
    ): "metered_wind_forecast_14d",
    (
        "14-days-ahead-operational-metered-wind-forecasts",
        "day ahead operational metered windfarm level wind forecast",
    ): "metered_wind_forecast_14d_windfarm",
    (
        "balancing-reserve-auction-requirement-forecast",
        "balancing reserve day ahead auction requirement forecast",
    ): "br_auction_requirement_fc_da_archive",
    (
        "balancing-reserve-auction-requirement-forecast",
        "balancing reserve day ahead auction requirements forecast",
    ): "br_auction_requirement_fc_da",
    (
        "balancing-reserve-auction-requirement-forecast",
        "balancing reserve requirements medium term forecast",
    ): "br_auction_requirement_fc_medium_term",
    ("bsuos-monthly-forecast", "monthly bsuos forecast sum"): "bsuos_monthly_forecast_fc_summary",
    (
        "bsuos-monthly-forecast",
        "monthly bsuos forecast sum percent",
    ): "bsuos_monthly_forecast_fc_summary_pct",
    ("bsuos-monthly-forecast", "monthly bsuos sum"): "bsuos_monthly_forecast_summary",
    ("bsuos-monthly-forecast", "monthly bsuos sum percent"): "bsuos_monthly_forecast_summary_pct",
    ("demand-flexibility", "dfs industry notification"): "demand_flexibility_industry_notification",
    ("demand-flexibility", "dfs service requirement"): "demand_flexibility_service_requirement",
    ("demand-flexibility", "dfs utilisation report"): "demand_flexibility_utilisation_report",
    ("demand-flexibility", "dfs utilisation report sum"): "demand_flexibility_utilisation_summary",
    ("dynamic-containment-4-day-forecast", "dynamic containment day forecast"): "dc_forecast_4d",
    (
        "dynamic-containment-4-day-forecast",
        "dynamic containment day forecast history",
    ): "dc_forecast_4d_history",
    ("eac-auction-results", "neso response reserve buy orders"): "eac_results_buy_orders",
    ("eac-auction-results", "neso response reserve buy orders fy"): "eac_results_buy_orders_fy",
    (
        "eac-auction-results",
        "neso response reserve daily buy orders",
    ): "eac_results_daily_buy_orders",
    (
        "eac-auction-results",
        "neso response reserve daily results by unit",
    ): "eac_results_daily_by_unit",
    ("eac-auction-results", "neso response reserve daily results sum"): "eac_results_daily_summary",
    (
        "eac-auction-results",
        "neso response reserve daily sell orders",
    ): "eac_results_daily_sell_orders",
    ("eac-auction-results", "neso response reserve results by unit"): "eac_results_by_unit",
    ("eac-auction-results", "neso response reserve results by unit fy"): "eac_results_by_unit_fy",
    ("eac-auction-results", "neso response reserve results sum"): "eac_results_summary",
    ("eac-auction-results", "neso response reserve results sum fy"): "eac_results_summary_fy",
    ("eac-auction-results", "neso response reserve sell orders"): "eac_results_sell_orders",
    ("eac-br-auction-results", "neso balancing reserve buy orders"): "eac_br_results_buy_orders",
    ("eac-br-auction-results", "neso balancing reserve results by unit"): "eac_br_results_by_unit",
    ("eac-br-auction-results", "neso balancing reserve results sum"): "eac_br_results_summary",
    ("eac-br-auction-results", "neso balancing reserve sell orders"): "eac_br_results_sell_orders",
    (
        "eac-br-mock-auction-results",
        "neso mock balancing reserve buy orders",
    ): "eac_br_mock_results_buy_orders",
    (
        "eac-br-mock-auction-results",
        "neso mock balancing reserve results by unit",
    ): "eac_br_mock_results_by_unit",
    (
        "eac-br-mock-auction-results",
        "neso mock balancing reserve results sum",
    ): "eac_br_mock_results_summary",
    (
        "eac-br-mock-auction-results",
        "neso mock balancing reserve sell orders",
    ): "eac_br_mock_results_sell_orders",
    (
        "eac-mock-auction-results",
        "neso mock response reserve buy orders",
    ): "eac_mock_results_buy_orders",
    (
        "eac-mock-auction-results",
        "neso mock response reserve daily buy orders",
    ): "eac_mock_results_daily_buy_orders",
    (
        "eac-mock-auction-results",
        "neso mock response reserve daily results by unit",
    ): "eac_mock_results_daily_by_unit",
    (
        "eac-mock-auction-results",
        "neso mock response reserve daily results sum",
    ): "eac_mock_results_daily_summary",
    (
        "eac-mock-auction-results",
        "neso mock response reserve daily sell orders",
    ): "eac_mock_results_daily_sell_orders",
    (
        "eac-mock-auction-results",
        "neso mock response reserve results by unit",
    ): "eac_mock_results_by_unit",
    (
        "eac-mock-auction-results",
        "neso mock response reserve results sum",
    ): "eac_mock_results_summary",
    (
        "eac-mock-auction-results",
        "neso mock response reserve sell orders",
    ): "eac_mock_results_sell_orders",
    (
        "firm-frequency-response-post-tender-reports",
        "post tender report tr csv",
    ): "ffr_post_tender_reports_csv",
    (
        "firm-frequency-response-post-tender-reports",
        "post tender report tr csv ext",
    ): "ffr_post_tender_reports_csv_ext",
    (
        "firm-frequency-response-post-tender-reports",
        "post tender report tr ext",
    ): "ffr_post_tender_reports_ext",
    (
        "historic-gtma-grid-trade-master-agreement-trades-data",
        "historic gtma trades",
    ): "gtma_trades_fy",
    (
        "historic-gtma-grid-trade-master-agreement-trades-data",
        "historic gtma trades data",
    ): "gtma_trades_data",
    (
        "historic-gtma-grid-trade-master-agreement-trades-data",
        "historic gtma trades data pre",
    ): "gtma_trades_data_pre",
    (
        "local-authority-level-spatial-heat-model-outputs-fes",
        "fes local authority outputs domestic all scenarios",
    ): "fes_la_heat_model_domestic",
    (
        "local-authority-level-spatial-heat-model-outputs-fes",
        "fes local authority outputs residential all scenarios",
    ): "fes_la_heat_model_residential",
    (
        "regional-breakdown-of-fes-data-electricity",
        "fes grid supply point info",
    ): "fes_regional_gsp_info",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes demand active power",
    ): "fes_regional_demand_active_power",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes demand from distributed storage sites greater than mw",
    ): "fes_regional_storage_gt_1mw",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes demand from distributed storage sites less than mw",
    ): "fes_regional_storage_lt_1mw",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes demand side response dsr",
    ): "fes_regional_dsr",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes demands from distributed storage sites greater than mw",
    ): "fes_regional_storage_gt_1mw_pre2023",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes demands from distributed storage sites less than mw",
    ): "fes_regional_storage_lt_1mw_pre2023",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes distributed generation greater than mw",
    ): "fes_regional_dg_gt_1mw",
    (
        "regional-breakdown-of-fes-data-electricity",
        "regional breakdown of fes distributed generation less than mw",
    ): "fes_regional_dg_lt_1mw",
    (
        "skip-rates",
        "in merit psa for demand side flexibility unit",
    ): "skip_rates_in_merit_psa_dsf_unit",
    ("skip-rates", "skip rate exclusion reasons"): "skip_rates_exclusion_reasons",
    ("skip-rates", "skip rate in merit all balancing mechanism"): "skip_rates_in_merit_all_bm",
    ("skip-rates", "skip rate in merit post system action"): "skip_rates_in_merit_psa",
    ("skip-rates", "skip rate stage acceptance"): "skip_rates_stage6_acceptance",
    ("skip-rates", "skip rate sum"): "skip_rates_summary",
    (
        "skip-rates",
        "skips behind constraints in merit bmu level",
    ): "skip_rates_behind_constraints_bmu",
    (
        "skip-rates",
        "skips behind constraints in merit constraint level",
    ): "skip_rates_behind_constraints_cons",
    ("skip-rates", "stage dsf technology specific skip rate"): "skip_rates_stage5_dsf_technology",
    ("skip-rates", "stage psa skip rate"): "skip_rates_stage6_psa",
    ("skip-rates", "stage psa skip rate by technology"): "skip_rates_stage5_psa_technology",
    (
        "static-firm-frequency-response-auction-results",
        "static firm frequency response auction buy orders",
    ): "sffr_auction_results_buy_orders",
    (
        "static-firm-frequency-response-auction-results",
        "static firm frequency response auction results",
    ): "sffr_auction_results_main",
    (
        "tresp-demand-pathways",
        "tresp demand pathways per grid supply point area",
    ): "tresp_demand_pathways_gsp",
    (
        "tresp-demand-pathways",
        "tresp demand pathways per resp nation and region",
    ): "tresp_demand_pathways_resp_region",
    (
        "tresp-demand-pathways",
        "tresp indicative demand pathways per local authority for england",
    ): "tresp_demand_pathways_la_england",
    (
        "tresp-demand-pathways",
        "tresp indicative demand pathways per local authority for scotland",
    ): "tresp_demand_pathways_la_scotland",
    (
        "tresp-demand-pathways",
        "tresp indicative demand pathways per local authority for wales",
    ): "tresp_demand_pathways_la_wales",
    (
        "tresp-generation-pathways",
        "tresp generation pathways per grid supply point area",
    ): "tresp_generation_pathways_gsp",
    (
        "tresp-generation-pathways",
        "tresp generation pathways per resp nation and region",
    ): "tresp_generation_pathways_resp_region",
    (
        "tresp-generation-pathways",
        "tresp indicative generation pathways per local authority for england",
    ): "tresp_generation_pathways_la_england",
    (
        "tresp-generation-pathways",
        "tresp indicative generation pathways per local authority for scotland",
    ): "tresp_generation_pathways_la_scotland",
    (
        "tresp-generation-pathways",
        "tresp indicative generation pathways per local authority for wales",
    ): "tresp_generation_pathways_la_wales",
}


# Re-typed from the retired matrix generator `.planning/research/v022-matrix.py:14-27`
# (ROADMAP claim gate row 42 retires the GENERATOR, i.e. running it to regenerate
# the matrix; this copy reuses only its family-grouping function, as PLAN P-2/E7
# directs, and E7 reproduced its 274-family count with exactly this function).
_MON = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"


def norm(name: str) -> str:
    """Normalise a resource name to its family heuristic (E7)."""
    s = name.lower().strip()
    s = re.sub(r"\(archive[^)]*\)|archived?|archive", "", s)
    s = re.sub(r"\b(19|20)\d{6}\b", "", s)
    s = re.sub(r"\b(19|20)\d{4}\b", "", s)
    s = re.sub(_MON + r"[ _-]*\d{2,4}", "", s)
    s = re.sub(r"\bfy\b", "", s)
    s = re.sub(r"\b(19|20)?\d{2}\s*[-/]\s*(19|20)?\d{2}\b", "", s)
    s = re.sub(r"\b(19|20)\d{2}\b", "", s)
    s = re.sub(_MON, "", s)
    s = re.sub(r"\b\d{1,2}\.\d{1,2}\.\d{2,4}\b", "", s)
    s = re.sub(r"version [\d.]+|v\d+", "", s)
    s = re.sub(r"[^a-z]+", " ", s).strip()
    return s


_MATRIX_ROW = re.compile(r"^\| `([a-z0-9-]+)` \|(.*)\|\s*$")


def parse_matrix(text: str) -> dict[str, tuple[str, str, str | None]]:
    """Return slug -> (archetype, refresh, hold unit or None) from the matrix tables."""
    rows: dict[str, tuple[str, str, str | None]] = {}
    for line in text.splitlines():
        match = _MATRIX_ROW.match(line)
        if match is None:
            continue
        cells = [cell.strip() for cell in match.group(2).split("|")]
        # slug | arch | refresh | vendor cadence | files | CSV fam | dump | null lm |
        # non-CSV | lic | hold | gold | batch | FE
        if len(cells) != 13:
            continue
        archetype, refresh, hold = cells[0], cells[1], cells[9]
        hold_match = re.fullmatch(r"\**HOLD\((\w[\w-]*)\)\**", hold)
        if hold != "-" and hold_match is None:
            raise SystemExit(f"matrix row {match.group(1)}: unreadable hold cell {hold!r}")
        slug = match.group(1)
        if slug in rows:
            raise SystemExit(f"matrix row {slug} repeats")
        rows[slug] = (archetype, refresh, hold_match.group(1) if hold_match else None)
    return rows


def _filename_suffix(resource: dict[str, Any]) -> str:
    url = str(resource.get("url") or "")
    filename = url.rstrip("/").rsplit("/", 1)[-1]
    return filename.rsplit(".", 1)[-1].lower() if "." in filename else ""


def _disposition(resource: dict[str, Any], family: str) -> dict[str, str]:
    fmt = str(resource["format"]).upper()
    if fmt == "CSV":
        if resource["url_type"] == "datastore" or _filename_suffix(resource) == "csv":
            return {"kind": "SILVER", "key": family}
        if _filename_suffix(resource) == "zip":
            return {
                "kind": "HOLD",
                "reason": "CSV-declared ZIP body; classification",
                "unit": "X-R",
            }
        raise SystemExit(f"resource {resource['id']}: CSV upload with no csv/zip filename")
    if fmt in CLASSIFY_FORMATS:
        return {"kind": "HOLD", "reason": "non-CSV classification", "unit": "X-R"}
    if fmt in DOC_FORMATS:
        return {"kind": "DOC"}
    if fmt in GIS_FORMATS:
        return {"kind": "GIS"}
    raise SystemExit(f"resource {resource['id']}: no disposition rule for format {fmt!r}")


def _suffix(norm_value: str, stem: str, budget: int) -> str:
    stem_words = set(stem.split("_"))
    words = [w for w in norm_value.split() if w not in STOPWORDS and w not in stem_words]
    kept: list[str] = []
    for word in words:
        candidate = "_".join([*kept, word])
        if len(candidate) > budget:
            break
        kept.append(word)
    return "_".join(kept) or "main"


def _tabular_keys(slug: str, stem: str, norms: list[str]) -> dict[str, str]:
    """Return norm -> key for one package's tabular families."""
    if len(norms) == 1:
        keys = {norms[0]: stem}
    else:
        budget = MAX_KEY_LENGTH - len(stem) - 1
        keys = {n: f"{stem}_{_suffix(n, stem, budget)}" for n in sorted(norms)}
        counts = collections.Counter(keys.values())
        seen: collections.Counter[str] = collections.Counter()
        for n in sorted(norms):
            base = keys[n]
            if counts[base] > 1:
                seen[base] += 1
                if seen[base] > 1:
                    tag = f"_{seen[base]}"
                    trimmed = _suffix(n, stem, budget - len(tag))
                    keys[n] = f"{stem}_{trimmed}{tag}"
    for n in norms:
        override = FAMILY_KEY_OVERRIDES.get((slug, n))
        if override is not None:
            keys[n] = override
    return keys


def _family(
    key: str,
    kind: str,
    archetype: str,
    refresh: str,
    *,
    legacy: bool = False,
    max_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
) -> dict[str, Any]:
    return {
        "key": key,
        "kind": kind,
        "legacy": legacy,
        "archetype": archetype,
        "refresh": refresh,
        "empty_allowed": archetype == "REG",
        "max_download_bytes": max_bytes,
        "name_regex": None,
        "transformer": "bespoke" if legacy else None,
    }


def seed_package(
    package: dict[str, Any], matrix_row: tuple[str, str, str | None]
) -> dict[str, Any]:
    """Build one package's registry document."""
    slug = str(package["name"])
    stem = PACKAGE_STEMS[slug]
    archetype, refresh, hold_unit = matrix_row
    resources = package["resources"]
    csv_resources = [r for r in resources if str(r["format"]).upper() == "CSV"]
    other_resources = [r for r in resources if str(r["format"]).upper() != "CSV"]

    tabular_archetype = "REG" if archetype == "FILE" else archetype
    families: list[dict[str, Any]] = []
    family_of: dict[str, str] = {}

    if slug in LEGACY_FAMILIES:
        legacy_key, legacy_bytes = LEGACY_FAMILIES[slug]
        if stem != legacy_key:
            raise SystemExit(f"{slug}: legacy stem must be {legacy_key!r}")
        families.append(
            _family(
                legacy_key,
                "tabular",
                tabular_archetype,
                refresh,
                legacy=True,
                max_bytes=legacy_bytes,
            )
        )
        if slug == EMBEDDED_PACKAGE:
            families.append(_family(EMBEDDED_ARCHIVE_KEY, "tabular", tabular_archetype, refresh))
            for resource in csv_resources:
                family_of[resource["id"]] = (
                    legacy_key if resource["name"] == EMBEDDED_LIVE_NAME else EMBEDDED_ARCHIVE_KEY
                )
        else:
            for resource in csv_resources:
                family_of[resource["id"]] = legacy_key
    elif csv_resources:
        norms = sorted({norm(str(r["name"])) for r in csv_resources})
        keys = _tabular_keys(slug, stem, norms)
        for n in norms:
            families.append(_family(keys[n], "tabular", tabular_archetype, refresh))
        for resource in csv_resources:
            family_of[resource["id"]] = keys[norm(str(resource["name"]))]

    if other_resources:
        files_key = f"{stem}_files"
        families.append(_family(files_key, "files", archetype, refresh))
        for resource in other_resources:
            family_of[resource["id"]] = files_key

    eligibility: dict[str, str] = (
        {"status": "held", "question": HOLD_N_QUESTION, "unit": hold_unit}
        if hold_unit is not None
        else {"status": "eligible"}
    )
    return {
        "package": slug,
        "package_id": str(package["id"]),
        "group": str(package["organization"]["name"]),
        "archetype": archetype,
        "refresh": refresh,
        "eligibility": eligibility,
        "families": families,
        "resources": [
            {
                "id": str(r["id"]),
                "name": str(r["name"]),
                "format": str(r["format"]).upper(),
                "url_type": str(r["url_type"]),
                "family": family_of[r["id"]],
                "disposition": _disposition(r, family_of[r["id"]]),
            }
            for r in resources
        ],
    }


def _registered_dataset_names() -> dict[str, set[str]]:
    from gridflow.pipeline.runner import import_transformers
    from gridflow.silver.registry import list_transformers

    import_transformers()
    names: dict[str, set[str]] = collections.defaultdict(set)
    for source, dataset in list_transformers():
        names[dataset].add(source)
    return names


def _fixture(snapshot: dict[str, Any], source_sha256: str) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "source_sha256": source_sha256,
        "packages": [
            {
                "name": p["name"],
                "id": p["id"],
                "organization": {"name": p["organization"]["name"]},
                "resources": [
                    {
                        "id": r["id"],
                        "name": r["name"],
                        "format": r["format"],
                        "url_type": r["url_type"],
                        "url": r["url"],
                        "last_modified": r["last_modified"],
                    }
                    for r in p["resources"]
                ],
            }
            for p in sorted(snapshot["packages"], key=lambda p: p["name"])
        ],
    }


def main(argv: list[str] | None = None) -> int:
    """Seed the registry; print a summary. Exit 0 on success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--emit-fixture", type=Path, default=None)
    args = parser.parse_args(argv)

    out: Path = args.out
    if out.exists() and any(out.glob("*.json")):
        print(
            f"refusing: {out} already contains *.json; keys are fixed once seeded", file=sys.stderr
        )
        return 2

    snapshot_bytes = args.snapshot.read_bytes()
    snapshot = json.loads(snapshot_bytes)
    sums_path = args.snapshot.parent / "sha256sums.txt"
    recorded = {
        line.split()[1]: line.split()[0]
        for line in sums_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    actual_sha = hashlib.sha256(snapshot_bytes).hexdigest()
    if recorded.get(args.snapshot.name) != actual_sha:
        raise SystemExit(f"snapshot sha256 {actual_sha} does not match {sums_path}")

    matrix = parse_matrix(args.matrix.read_text(encoding="utf-8"))
    slugs = {str(p["name"]) for p in snapshot["packages"]}
    if set(matrix) != slugs:
        raise SystemExit(
            f"matrix and snapshot disagree: only in matrix {sorted(set(matrix) - slugs)}, "
            f"only in snapshot {sorted(slugs - set(matrix))}"
        )
    if set(PACKAGE_STEMS) != slugs:
        raise SystemExit(
            f"PACKAGE_STEMS does not cover the snapshot: missing "
            f"{sorted(slugs - set(PACKAGE_STEMS))}, extra {sorted(set(PACKAGE_STEMS) - slugs)}"
        )
    for slug, stem in PACKAGE_STEMS.items():
        if not STEM_PATTERN.fullmatch(stem):
            raise SystemExit(f"stem {stem!r} for {slug} does not match {STEM_PATTERN.pattern}")
    if len(set(PACKAGE_STEMS.values())) != len(PACKAGE_STEMS):
        raise SystemExit("PACKAGE_STEMS repeats a stem")

    present = {
        (str(p["name"]), norm(str(r["name"])))
        for p in snapshot["packages"]
        for r in p["resources"]
        if str(r["format"]).upper() == "CSV"
    }
    stale = sorted(set(FAMILY_KEY_OVERRIDES) - present)
    if stale:
        raise SystemExit(f"FAMILY_KEY_OVERRIDES entries match no family: {stale}")

    documents = {
        str(p["name"]): seed_package(p, matrix[str(p["name"])]) for p in snapshot["packages"]
    }

    # P-3 disjointness, asserted before anything is written.
    registered = _registered_dataset_names()
    for document in documents.values():
        for family in document["families"]:
            key = family["key"]
            if not KEY_PATTERN.fullmatch(key):
                raise SystemExit(f"key {key!r} does not match {KEY_PATTERN.pattern}")
            if key in LEGACY_KEYS:
                if registered.get(key, set()) - {"neso_data_portal"}:
                    raise SystemExit(f"legacy key {key!r} is registered under another source")
            elif key in registered:
                raise SystemExit(f"key {key!r} collides with registered {sorted(registered[key])}")

    out.mkdir(parents=True, exist_ok=True)
    for slug, document in sorted(documents.items()):
        (out / f"{slug}.json").write_text(dump_json(document), encoding="utf-8", newline="\n")
    legacy_ledger = sorted(
        ({"key": key, "package": slug} for slug, (key, _b) in LEGACY_FAMILIES.items()),
        key=lambda row: row["key"],
    )
    (out / "_frozen_keys.json").write_text(dump_json(legacy_ledger), encoding="utf-8", newline="\n")
    (out / "_adjudications.json").write_text(dump_json([]), encoding="utf-8", newline="\n")

    # P-1 validity, through the real loader.
    try:
        registry = load_registry(out)
    except RegistryError as exc:
        raise SystemExit(f"seeded registry fails validation: {exc}") from exc

    if args.emit_fixture is not None:
        args.emit_fixture.parent.mkdir(parents=True, exist_ok=True)
        args.emit_fixture.write_text(
            dump_json(_fixture(snapshot, actual_sha)), encoding="utf-8", newline="\n"
        )

    dispositions = collections.Counter(
        resource.disposition.kind for _p, resource in registry.resources.values()
    )
    kinds = collections.Counter(family.kind for _p, family in registry.families.values())
    held = sorted(p.package for p in registry.packages if p.eligibility.status == "held")
    print(f"snapshot {snapshot['snapshot_id']} sha256 {actual_sha}")
    print(f"packages {len(registry.packages)}  resources {len(registry.resources)}")
    print(f"families {len(registry.families)}  by kind {dict(sorted(kinds.items()))}")
    print(f"dispositions {dict(sorted(dispositions.items()))}")
    print(f"held packages {held}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
