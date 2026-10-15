"""The Building Heat Model output / WS1 / WS2 / local-authority heat model / Resource Adequacy 2030s
frozen records (v0.22-K-SCN-3).

Eight families of five NESO packages get record version 1:

- ``building_heat_model_example_output_half`` (B2): one example January day, two runs of 48
  half-hour periods; a reference table (temporal ``none``, no edition), held E-SEM (the COP
  columns declare no unit, a blank cell has no stated meaning).
- ``fes_ws1_gas_supply`` (WS1) and ``fes_ws2_gas_emissions`` (WS2): wide, one unpivot header epoch
  per distinct header, held E-SEM. The WS1 2024 body is a resource HOLD (7128 literal ``N/A``
  cells), the WS1 2025 body fails the duplicate guard and is one adjudicated ADR-040 ``failed``
  entry.
- ``fes_la_heat_model_domestic`` / ``fes_la_heat_model_residential``: already year-long (``Year``
  is a body column, ``projection_year``), three / two header epochs, held E-SEM.
- ``resource_adequacy_2030s_portfolio`` / ``_peak_demand`` / ``_results``: already long, labelled
  April-March years kept as strings, eligible; the results table carries no edition.
- ``building_heat_model_list_input`` has no record (RULINGS 538): a repeated identical vendor row.

Every test writes recorded fixture captures into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here, not
at activation. On master the eight families have no record, so ``get_transformer`` raises.

Fixtures (``tests/fixtures/neso_data_portal/scn3/``, provenance in ``PROVENANCE.md``): each file
is a cut of the 2026-10-08 swept bronze (a scratch script, not committed): the body's header
(BOM kept) plus whole vendor records, byte for byte. ``.gitattributes`` keeps their CRLF
terminators and BOMs.
"""

from __future__ import annotations

import csv
import io
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import install_generated, write_capture
from polars.exceptions import InvalidOperationError
from test_neso_multi_resource import both_as_of
from test_neso_reconcile_adjudication import point_settings, run_cli

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal import skeleton
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import (
    RECONCILE_ADJUDICATIONS_FILE,
    DocDisposition,
    HoldDisposition,
    SilverDisposition,
    load_registry,
)
from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.casting import (
    DuplicateEntityKeyError,
    ExclusionTally,
    finish_capture,
    type_child,
)
from gridflow.silver.neso_data_portal.completion import (
    CaptureContext,
    NesoCaptureFailedError,
    capture_id_for,
    read_completion,
    read_failure,
)
from gridflow.silver.neso_data_portal.readers import read_csv_body
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "scn3"
REGISTRY_DIR = Path(registry_module.__file__).parent
DAY = date(2026, 10, 8)
RULING = "839"

B1 = "building_heat_model_list_input"
B2 = "building_heat_model_example_output_half"
WS1 = "fes_ws1_gas_supply"
WS2 = "fes_ws2_gas_emissions"
DOM = "fes_la_heat_model_domestic"
RES = "fes_la_heat_model_residential"
A1F = "resource_adequacy_2030s_portfolio"
A2F = "resource_adequacy_2030s_peak_demand"
A3F = "resource_adequacy_2030s_results"
RECORDED = (B2, WS1, WS2, DOM, RES, A1F, A2F, A3F)
HELD_FAMILIES = (B2, WS1, WS2, DOM, RES)
ELIGIBLE_FAMILIES = (A1F, A2F, A3F)
UNPIVOT_FAMILIES = (WS1, WS2)
WIDE_FAMILIES = (B2, DOM, RES, A1F, A2F, A3F)

PACKAGES = {
    B1: ("building-heat-model", "14d99094-165e-4de1-9452-137c2835b102"),
    B2: ("building-heat-model", "14d99094-165e-4de1-9452-137c2835b102"),
    WS1: ("fes-whole-system-gas-supply-data-table-ws1", "8cbe76c7-6971-40ed-8e0c-2d8c8520cd0c"),
    WS2: (
        "fes-whole-system-gas-supply-emissions-data-table-ws2",
        "cd6e42b0-13de-4f2a-bd0d-08e31bdde67f",
    ),
    DOM: (
        "local-authority-level-spatial-heat-model-outputs-fes",
        "bfa95333-542e-4625-b5d0-17ced50a89ed",
    ),
    RES: (
        "local-authority-level-spatial-heat-model-outputs-fes",
        "bfa95333-542e-4625-b5d0-17ced50a89ed",
    ),
    A1F: ("resource-adequacy-in-2030s", "531e616c-7192-443f-bee0-ed067d55b182"),
    A2F: ("resource-adequacy-in-2030s", "531e616c-7192-443f-bee0-ed067d55b182"),
    A3F: ("resource-adequacy-in-2030s", "531e616c-7192-443f-bee0-ed067d55b182"),
}

HELD_QUESTIONS = {
    B2: (
        "TODO: What unit or scale does NESO declare for `avg_COP_space_heat` and `avg_COP_dhw`? "
        "What does a blank gas, electricity or COP cell mean in the example profiles (blank is "
        "not zero)?"
    ),
    WS1: (
        "TODO: For each WS1 edition and quantity class (`TWh`, `GW`, `bcm/yr` rows), what "
        "interval or assessment date does a year header denote, and is the integer a starting or "
        "ending label? What does a blank projection cell mean? What does the literal `N/A` in "
        "`fes2024_ws1_v001.csv` mean? Which omitted dimension distinguishes the four repeated "
        "Bioenergy / Waste / `MSW, C&I (biogenic)` rows per pathway in `fes2025_ws1_v006.csv`, "
        "and can NESO publish a corrected file?"
    ),
    WS2: (
        "TODO: What emissions unit do the 2023, 2024 and 2026 WS2 year columns carry (their "
        "dictionaries give `N/A` or no unit; only 2025 states `MTCO2e`)? For each edition, is a "
        "year label a calendar or financial year, and a starting or ending label? What does a "
        "blank projection cell mean?"
    ),
    DOM: (
        "TODO: What stock assessment date does `Year` denote in the FES 2022 domestic outputs, "
        "and is it a start- or end-of-year snapshot? Is `stock Oil boiler` a number of buildings "
        "or a proportion, as the dictionary states? What does a blank hydrogen or "
        "hybrid-hydrogen cell in the 2030-2050 slices mean?"
    ),
    RES: (
        "TODO: What exact stock assessment date does `Year` represent in the FES 2024 and FES "
        "2025 residential outputs, and is it a start- or end-of-year convention?"
    ),
}

W2_HOLD_NEEDLES = (
    "fails the strict float64 cast on 7128 projection cells",
    "the literal `N/A` (264 per column 2023-2049)",
    "whose meaning NESO does not document",
    "declaring it a null token would assert an undocumented semantic",
    "re-disposition only after NESO defines `N/A`",
    "(then an exact `null_tokens` declaration)",
    "uploads a corrected file (a new last_modified)",
)

LEDGER_REASON = (
    "the 2025 body repeats four fuel type / units / category / sector / fuel / feedstock / "
    "category lookup tuples (Bioenergy, TWh, Supply, Waste, Waste, `MSW, C&I (biogenic)`, "
    "blank) once per pathway with conflicting values (e.g. Electric Engagement on CSV lines 80 "
    "and 150: 2050 `0.0` vs `13.465`), so 248 unpivoted rows share 124 keys and the duplicate "
    "guard fails the capture; it is never deduplicated and stays unloaded with its failure "
    "record"
)
LEDGER_QUESTION = (
    "Which omitted dimension distinguishes the repeated Bioenergy / Waste / MSW, C&I (biogenic) "
    "rows of FES 2025 WS1, and can NESO publish a corrected file?"
)

KEYS = {
    B2: ("resource_id", "run_id", "hh_period"),
    WS1: (
        "resource_id",
        "edition",
        "fuel_type",
        "scenario",
        "pathway",
        "units",
        "category",
        "sector",
        "fuel",
        "feedstock",
        "category_lookup",
        "projection_year",
    ),
    WS2: (
        "resource_id",
        "edition",
        "scenario",
        "pathway",
        "category",
        "sector",
        "projection_year",
    ),
    DOM: ("resource_id", "edition", "code_x", "projection_year", "scenario", "sector"),
    RES: ("resource_id", "edition", "la_code", "projection_year", "pathway", "sector"),
    A1F: ("resource_id", "edition", "name", "technology", "future_year"),
    A2F: ("resource_id", "edition", "spotlight_year", "historical_weather_year"),
    A3F: ("resource_id", "simulation_name", "spotlight_year", "historical_weather_year"),
}

DOM_FILES = tuple(
    f"fes22_la_level_outputs_{year}_domestic_all_scenarios.csv"
    for year in (2020, 2025, 2030, 2035, 2040, 2045, 2050)
)
RES_FILES = tuple(
    f"fes24_la_level_outputs_{year}_residential_all_scenarios.csv"
    for year in (2020, 2025, 2030, 2035, 2040, 2045, 2050)
) + tuple(
    f"fes25_la_level_outputs_{year}_residential_all_scenarios.csv"
    for year in (2025, 2030, 2035, 2040, 2045, 2050)
)
EDITIONS: dict[str, tuple[tuple[str, int], ...] | None] = {
    B2: None,
    WS1: (
        ("fes2023_ws1_v001.csv", 2023),
        ("fes2025_ws1_v006.csv", 2025),
        ("10yo2026_ws1_v001.csv", 2026),
    ),
    WS2: (
        ("fes2023_ws2_v001.csv", 2023),
        ("fes2024_ws2_v002.csv", 2024),
        ("fes2025_ws2_v006.csv", 2025),
        ("10yo2026_ws2_v001.csv", 2026),
    ),
    DOM: tuple((f, 2022) for f in DOM_FILES),
    RES: tuple((f, 2024 if f.startswith("fes24_") else 2025) for f in RES_FILES),
    A1F: (("portfolio-capacity-dataset.csv", 2025),),
    A2F: (("peak-demand-dataset.csv", 2025),),
    A3F: None,
}

# Vendor identifier header -> (silver name, dtype, nullable), spec by spec.
B2_COLUMNS: dict[str, tuple[str, str, bool]] = {
    "run_id": ("run_id", "string", False),
    "building_archetype": ("building_archetype", "string", False),
    "month": ("month_vendor", "string", False),
    "hh_period": ("hh_period", "int64", False),
    "start_of_hh_period": ("start_of_hh_period", "string", False),
    "heater_output_space_heat": ("heater_output_space_heat", "float64", False),
    "gas_space_heat": ("gas_space_heat", "float64", True),
    "avg_COP_space_heat": ("avg_cop_space_heat", "float64", True),
    "electricity_space_heat": ("electricity_space_heat", "float64", True),
    "heater_output_dhw": ("heater_output_dhw", "float64", False),
    "gas_dhw": ("gas_dhw", "float64", True),
    "avg_COP_dhw": ("avg_cop_dhw", "float64", True),
    "electricity_dhw": ("electricity_dhw", "float64", False),
    "total_heater_output": ("total_heater_output", "float64", False),
    "total_gas": ("total_gas", "float64", True),
    "total_electricity": ("total_electricity", "float64", False),
}
WS1_COLUMNS: dict[str, tuple[str, str, bool]] = {
    "Fuel Type": ("fuel_type", "string", False),
    "Scenario": ("scenario", "string", True),
    "Pathway": ("pathway", "string", True),
    "Units": ("units", "string", False),
    "Category": ("category", "string", False),
    "Sector": ("sector", "string", True),
    "Fuel": ("fuel", "string", True),
    "Feedstock": ("feedstock", "string", True),
    "Category Lookup": ("category_lookup", "string", True),
}
WS2_COLUMNS: dict[str, tuple[str, str, bool]] = {
    "Scenario": ("scenario", "string", True),
    "Pathway": ("pathway", "string", True),
    "Category": ("category", "string", True),
    "Sector": ("sector", "string", False),
}
A1_COLUMNS: dict[str, tuple[str, str, bool]] = {
    "Name": ("name", "string", False),
    "Category": ("category", "string", False),
    "Technology": ("technology", "string", False),
    "Future Year": ("future_year", "string", False),
    "Installed Capacity (MW)": ("installed_capacity_mw", "float64", False),
    "Stored energy (MWh)": ("stored_energy_mwh", "float64", False),
}
A2_COLUMNS: dict[str, tuple[str, str, bool]] = {
    "Spotlight year": ("spotlight_year", "string", False),
    "Historical weather year": ("historical_weather_year", "string", False),
    "Peak Demand (MW)": ("peak_demand_mw", "float64", False),
}
A3_COLUMNS: dict[str, tuple[str, str, bool]] = {
    "Simulation Name": ("simulation_name", "string", False),
    "Spotlight year": ("spotlight_year", "string", False),
    "Historical weather year": ("historical_weather_year", "string", False),
    "Weather-conditional LOLE (h/year)": ("weather_conditional_lole_h_year", "float64", False),
    "Weather-conditional EEU (MWh/year)": ("weather_conditional_eeu_mwh_year", "float64", False),
}

DOM_PREFIX: dict[str, tuple[str, str, bool]] = {
    "CODE_x": ("code_x", "string", False),
    "Local_Authority": ("local_authority", "string", False),
    "Year": ("projection_year", "int64", False),
    "Scenario": ("scenario", "string", False),
    "Sector": ("sector", "string", False),
}
RES_PREFIX: dict[str, tuple[str, str, bool]] = {
    "la_code": ("la_code", "string", False),
    "Local_Authority": ("local_authority", "string", False),
    "Year": ("projection_year", "int64", False),
    "Pathway": ("pathway", "string", False),
    "Sector": ("sector", "string", False),
}
D0_SUFFIXES = (
    "ASHP",
    "Community",
    "DH",
    "Electric resistive",
    "Electric storage",
    "GSHP",
    "Gas boiler",
    "Oil boiler",
    "Total",
    "TotalHPs",
)
R24_ONLY = (
    "stock Biofuel boiler",
    "stock Community",
    "stock DH",
    "stock Hybrid (ASHP + Biofuel boiler)",
)
R25_ONLY = (
    "stock BioLPG boiler",
    "stock Fossil fuel communal heating",
    "stock Hybrid (ASHP + BioLPG boiler)",
    "stock Low carbon district heating",
)


def snake(name: str) -> str:
    """The spec's silver name rule: lower-case, non-``[a-z0-9]`` runs as ``_``."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


@dataclass(frozen=True)
class Capture:
    """One 2026-10-08 bronze capture and its sidecar provenance."""

    family: str
    resource_id: str
    name: str
    filename: str
    edition: int | None
    modified: str
    written: str


CAPTURES: dict[str, Capture] = {
    "b2": Capture(
        B2,
        "27522340-008d-4c82-97c1-17eeb7ed1b6a",
        "Example output half-hourly demand profiles",
        "example_profiles-output_data.csv",
        None,
        "2026-07-30T15:36:52.510328",
        "2026-10-08T09:14:01.292897+00:00",
    ),
    "b1": Capture(
        B1,
        "b809e59a-e681-4ecb-af07-80997abbbd6f",
        "List of input parameters used for generating the example profiles",
        "example_profiles-list_of_input_parameters.csv",
        None,
        "2026-07-30T14:01:53.418825",
        "2026-10-08T09:14:12.279856+00:00",
    ),
    "w1": Capture(
        WS1,
        "b04e227d-f329-4ccf-8e25-821ea6ea03ca",
        "Whole System & Gas Supply (WS1) 2023",
        "fes2023_ws1_v001.csv",
        2023,
        "2023-07-10T03:30:11.101381",
        "2026-10-08T10:58:02.247022+00:00",
    ),
    "w2": Capture(
        WS1,
        "fa5697c5-9810-480f-b4c5-ab41dc53777a",
        "Whole System & Gas Supply (WS1) 2024",
        "fes2024_ws1_v001.csv",
        2024,
        "2024-07-15T06:19:57.831578",
        "2026-10-08T10:58:06.175269+00:00",
    ),
    "w3": Capture(
        WS1,
        "ebf4f503-0a13-45e6-89ad-b707748613b6",
        "Whole System & Gas Supply (WS1) 2025",
        "fes2025_ws1_v006.csv",
        2025,
        "2025-12-10T16:57:40.652852",
        "2026-10-08T10:58:08.633236+00:00",
    ),
    "w4": Capture(
        WS1,
        "0dd30611-5b9f-452d-bd34-966737d14fca",
        "Whole System & Gas Supply (WS1) 2026",
        "10yo2026_ws1_v001.csv",
        2026,
        "2026-09-16T14:49:53.687177",
        "2026-10-08T10:58:11.248826+00:00",
    ),
    "e1": Capture(
        WS2,
        "5f8f7293-4894-40c5-8f5c-e096b19e8198",
        "Whole System & Gas Supply - Emissions (WS2) 2023",
        "fes2023_ws2_v001.csv",
        2023,
        "2023-07-10T03:31:44.136572",
        "2026-10-08T10:58:14.923059+00:00",
    ),
    "e2": Capture(
        WS2,
        "31188b9d-620d-497b-80c0-83fa4fa70320",
        "Whole System & Gas Supply - Emissions (WS2) 2024",
        "fes2024_ws2_v002.csv",
        2024,
        "2024-07-16T11:16:11.122722",
        "2026-10-08T10:58:17.249461+00:00",
    ),
    "e3": Capture(
        WS2,
        "a80a94cc-b349-412c-83a0-0f5f9eaf4e7e",
        "Whole System & Gas Supply - Emissions (WS2) 2025",
        "fes2025_ws2_v006.csv",
        2025,
        "2025-12-10T16:59:01.412328",
        "2026-10-08T10:58:19.525633+00:00",
    ),
    "e4": Capture(
        WS2,
        "3aba22fd-1934-4654-a33a-f3273c973560",
        "Whole System & Gas Supply - Emissions (WS2) 2026",
        "10yo2026_ws2_v001.csv",
        2026,
        "2026-09-16T14:51:40.910171",
        "2026-10-08T10:58:21.823686+00:00",
    ),
    "d1": Capture(
        DOM,
        "c3dd66ab-cb07-4e4d-bbab-d79074ea1713",
        "FES 22 local authority outputs 2020 domestic all scenarios",
        "fes22_la_level_outputs_2020_domestic_all_scenarios.csv",
        2022,
        "2022-08-31T07:33:03.509314",
        "2026-10-08T10:55:39.546223+00:00",
    ),
    "d2": Capture(
        DOM,
        "52baf0e4-b02b-4f69-8cd5-ca52c97bf872",
        "FES 22 local authority outputs 2025 domestic all scenarios",
        "fes22_la_level_outputs_2025_domestic_all_scenarios.csv",
        2022,
        "2022-08-31T07:34:02.299496",
        "2026-10-08T10:55:42.677271+00:00",
    ),
    "d3": Capture(
        DOM,
        "f41f65e2-9f55-496b-afaa-76efcee4cda2",
        "FES 22 local authority outputs 2030 domestic all scenarios",
        "fes22_la_level_outputs_2030_domestic_all_scenarios.csv",
        2022,
        "2022-08-31T07:35:07.017785",
        "2026-10-08T10:55:45.372469+00:00",
    ),
    "r1": Capture(
        RES,
        "31d45b63-e70d-4cf1-a912-918bf3de58d5",
        "FES 2024 local authority outputs 2020 residential all scenarios",
        "fes24_la_level_outputs_2020_residential_all_scenarios.csv",
        2024,
        "2025-11-25T14:57:39.152279",
        "2026-10-08T10:56:01.676213+00:00",
    ),
    "r8": Capture(
        RES,
        "e2215328-bd74-47c2-a22f-3878bb0fbbd5",
        "FES 2025 local authority outputs 2025 residential all scenarios",
        "fes25_la_level_outputs_2025_residential_all_scenarios.csv",
        2025,
        "2025-11-25T15:04:38.401027",
        "2026-10-08T10:56:21.276371+00:00",
    ),
    "a1": Capture(
        A1F,
        "c2bba528-b75a-42c5-bb98-3af3a5b3f720",
        "Portfolio Capacities",
        "portfolio-capacity-dataset.csv",
        2025,
        "2025-12-11T13:47:53.306382",
        "2026-10-08T11:17:08.685157+00:00",
    ),
    "a2": Capture(
        A2F,
        "5ca210f2-96cb-4fcd-939d-a9f34b2b8174",
        "Peak Demand",
        "peak-demand-dataset.csv",
        2025,
        "2025-12-12T12:36:08.151108",
        "2026-10-08T11:17:05.133341+00:00",
    ),
    "a3": Capture(
        A3F,
        "71221dca-c792-4fb6-b6b4-893f392d893a",
        "Resource Adequacy Results",
        "resource-adequacy-in-the-2030s-results-dataset-.csv",
        None,
        "2025-12-12T12:34:52.812310",
        "2026-10-08T11:17:13.510244+00:00",
    ),
}

ALIASES_OF = {f: tuple(a for a, c in CAPTURES.items() if c.family == f) for f in PACKAGES}
HELD_RESOURCE = "w2"
FAILED_RESOURCE = "w3"
TYPED = tuple(a for a in CAPTURES if a not in {"b1", HELD_RESOURCE, FAILED_RESOURCE})
TYPED_UNPIVOT = tuple(a for a in TYPED if CAPTURES[a].family in UNPIVOT_FAMILIES)
TYPED_WIDE = tuple(a for a in TYPED if CAPTURES[a].family in WIDE_FAMILIES)
BOMS = ("b1", "b2", "w1", "w2", "e2")
FIRST_YEAR = {"w1": 2020, "w3": 2020, "w4": 2020, "e1": 2021, "e2": 2021, "e3": 2021, "e4": 2021}
FILENAME_OF = {a: c.filename for a, c in CAPTURES.items()}


def _short_base() -> str:
    """The drive root on Windows (the engine's run-id names pass MAX_PATH under the long
    per-user temp directory); the system temp elsewhere."""
    import os

    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root the settings point at."""
    with tempfile.TemporaryDirectory(
        prefix="sc", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        point_settings(Path(root), monkeypatch)
        yield Path(root)


def body(alias: str) -> bytes:
    """Fixture ``alias`` with the bronze original's CRLF line endings and BOM."""
    return (FIXTURES / f"{alias}.csv").read_bytes()


def lines(alias: str, raw: bytes | None = None) -> list[list[str]]:
    """Every record of the fixture as text."""
    data_bytes = raw if raw is not None else body(alias)
    return list(csv.reader(io.StringIO(data_bytes.decode("utf-8-sig"), newline="")))


def table(alias: str, raw: bytes | None = None) -> tuple[list[str], list[list[str]]]:
    """The fixture's table header and populated (not all-blank) records as text."""
    records = lines(alias, raw)
    return records[0], [line for line in records[1:] if any(line)]


def rows(alias: str, raw: bytes | None = None) -> list[dict[str, str]]:
    """The fixture's populated records keyed by header."""
    header, records = table(alias, raw)
    return [dict(zip(header, record, strict=True)) for record in records]


def label_columns(header: list[str]) -> list[str]:
    """The header's year labels, in header order."""
    return [h for h in header if h.isdigit()]


def columns_of(alias: str) -> dict[str, tuple[str, str, bool]]:
    """The spec's ``vendor header -> (silver name, dtype, nullable)`` for fixture ``alias``."""
    family = CAPTURES[alias].family
    header, _records = table(alias)
    if family == B2:
        return B2_COLUMNS
    if family == WS1:
        return WS1_COLUMNS
    if family == WS2:
        return WS2_COLUMNS
    if family == A1F:
        return A1_COLUMNS
    if family == A2F:
        return A2_COLUMNS
    if family == A3F:
        return A3_COLUMNS
    prefix = DOM_PREFIX if family == DOM else RES_PREFIX
    always = (
        {f"stock {s}" for s in D0_SUFFIXES} | {f"stock_proportion {s}" for s in D0_SUFFIXES}
        if family == DOM
        else {h for h in header if h.startswith("stock ")} - set(R24_ONLY) - set(R25_ONLY)
    )
    out: dict[str, tuple[str, str, bool]] = {}
    for name in header:
        if name in prefix:
            out[name] = prefix[name]
        else:
            out[name] = (snake(name), "float64", name not in always)
    return out


def capture(
    data: Path,
    alias: str,
    *,
    raw: bytes | None = None,
    written: datetime | None = None,
    filename: str | None = None,
    resource_id: str | None = None,
) -> str:
    """Write fixture ``alias`` (or ``raw``) as a committed capture with its real sidecar."""
    meta = CAPTURES[alias]
    slug, package_id = PACKAGES[meta.family]
    path, _sidecar = write_capture(
        data,
        meta.family,
        body=raw if raw is not None else body(alias),
        written_at=written or datetime.fromisoformat(meta.written).astimezone(UTC),
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
        resource_id=resource_id or meta.resource_id,
        resource_name=meta.name,
        resource_filename=filename or meta.filename,
        ckan_last_modified=meta.modified,
        url_type="upload",
    )
    return capture_id_for(path, data)


def _record(family: str) -> SchemaRecord:
    record = registry_module.load_registry().families[family][1].record
    assert record is not None
    return record


def _silver(data: Path, family: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / family).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _no_silver(data: Path, family: str) -> bool:
    return not list((data / "silver" / SOURCE / family).rglob("[!.]*.parquet"))


def _run(data: Path, alias: str, **kwargs: Any) -> tuple[str, int]:
    """Capture ``alias``, run its family's transformer; the capture id and rows written."""
    capture_id = capture(data, alias, **kwargs)
    written = get_transformer(SOURCE, CAPTURES[alias].family, data).run(DAY, run_id="r")
    return capture_id, written


def _replace_cell(raw: bytes, row: int, column: int, value: str) -> bytes:
    """``raw`` with table record ``row`` (0 = the header) cell ``column`` replaced.

    Only for bodies whose records never span a physical line (every SCN-3 body)."""
    bom = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
    parts = raw[len(bom) :].split(b"\r\n")
    fields = next(csv.reader([parts[row].decode("utf-8")]))
    fields[column] = value
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    parts[row] = out.getvalue().encode("utf-8")
    return bom + b"\r\n".join(parts)


def _cast(value: str, dtype: str) -> Any:
    if value == "":
        return None
    if dtype == "int64":
        return int(value)
    if dtype == "float64":
        return float(value)
    return value


def expected_long(alias: str) -> list[tuple[Any, ...]]:
    """The long rows an unpivot body must become, sorted: every silver identifier column of the
    family (``None`` for a blank or an absent one), projection year and value."""
    family = CAPTURES[alias].family
    spec = columns_of(alias)
    names = sorted({n for n, _d, _x in (WS1_COLUMNS if family == WS1 else WS2_COLUMNS).values()})
    header, records = table(alias)
    pos = {name: header.index(name) for name in header}
    out: list[tuple[Any, ...]] = []
    for record in records:
        cells = {spec[h][0]: (record[pos[h]] or None) for h in header if h in spec}
        for year in label_columns(header):
            cell = record[pos[year]]
            out.append(
                (*(cells.get(n) for n in names), int(year), float(cell) if cell != "" else None)
            )
    return sorted(out, key=repr)


def actual_long(frame: pl.DataFrame, family: str) -> list[tuple[Any, ...]]:
    """The silver rows in :func:`expected_long`'s shape."""
    names = sorted({n for n, _d, _x in (WS1_COLUMNS if family == WS1 else WS2_COLUMNS).values()})
    return sorted(
        (
            (*(row[n] for n in names), row["projection_year"], row["value"])
            for row in frame.to_dicts()
        ),
        key=repr,
    )


def expected_wide(alias: str) -> list[dict[str, Any]]:
    """The silver identifier and measure columns of a year-long body, typed per the spec."""
    spec = columns_of(alias)
    header, records = table(alias)
    return sorted(
        (
            {spec[h][0]: _cast(cell, spec[h][1]) for h, cell in zip(header, record, strict=True)}
            for record in records
        ),
        key=repr,
    )


def actual_wide(frame: pl.DataFrame, alias: str) -> list[dict[str, Any]]:
    """The silver rows restricted to the vendor columns, keys sorted like :func:`expected_wide`."""
    names = [name for name, _d, _n in columns_of(alias).values()]
    return sorted(({n: row[n] for n in names} for row in frame.to_dicts()), key=repr)


# --------------------------------------------------------------------------- #
# Fixtures and record shape
# --------------------------------------------------------------------------- #


def test_fixtures_keep_the_vendor_shape() -> None:
    """Detects a fixture cut that lost what the tests below rely on: CRLF terminators, each
    original's BOM (B1, B2, WS1 2023 and 2024 and WS2 2024 have one), the exact vendor headers,
    the B2 trailing-space archetype and blank COP / gas cells, the WS1 units, the 2024 ``N/A``
    row and the 2025 Electric Engagement pair, the blank projection tails, the Isles of Scilly
    code, the blank domestic hydrogen cells, the all-zero residential hydrogen columns, the
    trailing-space portfolio technology and the ``1999/2000`` weather label."""
    bom = b"\xef\xbb\xbf"
    for alias in CAPTURES:
        raw = body(alias)
        assert raw.startswith(bom) == (alias in BOMS), alias
        assert raw.endswith(b"\r\n") and b"\r\n" in raw, alias
    header, _ = table("b2")
    assert header[0] == "run_id" and header[2] == "month" and len(header) == 16
    b2 = rows("b2")
    assert {r["run_id"] for r in b2} == {"run_1", "run_2"}
    assert all(r["building_archetype"].endswith(" ") for r in b2)
    assert any(r["avg_COP_space_heat"] == "" for r in b2) and any(r["gas_dhw"] == "" for r in b2)
    assert any(r["avg_COP_dhw"] != "" for r in b2) and any(r["total_gas"] == "" for r in b2)
    assert [r["hh_period"] for r in b2 if r["run_id"] == "run_1"][-1] == "48"
    assert table("b1")[0] == ["parameter", "run_1", "run_2"]
    assert {r["Units"] for a in ("w1", "w4") for r in rows(a)} >= {"TWh", "GW", "bcm/yr"}
    for alias in ("w1", "w4"):
        h, _ = table(alias)
        assert h[:8] == [
            "Fuel Type",
            "Scenario",
            "Units",
            "Category",
            "Sector",
            "Fuel",
            "Feedstock",
            "Category Lookup",
        ]
        assert label_columns(h)[0] == "2020" and label_columns(h)[-1] == "2050"
    assert table("w3")[0][1] == "Pathway" and table("w2")[0][1] == "Pathway"
    assert any(r["Fuel"] == "" and r["Feedstock"] == "" for r in rows("w1"))
    assert any(r["Category Lookup"] == "" for r in rows("w1")) and any(
        r["Sector"] == "" for r in rows("w1")
    )
    assert any(r["2020"] == "" for r in rows("w1")) and any(r["2020"] == "0" for r in rows("w1"))
    assert all(r["2037"] == "" and r["2050"] == "" for r in rows("w4"))
    assert [r["2023"] for r in rows("w2")] == ["N/A"]
    w3 = rows("w3")
    pair = [r for r in w3 if r["Pathway"] == "Electric Engagement" and r["Sector"] == "Waste"]
    assert [r["2050"] for r in pair] == ["0.0", "13.465"]
    assert table("e1")[0][:2] == ["Scenario", "Sector"]
    assert table("e2")[0][:2] == table("e3")[0][:2] == ["Pathway", "Sector"]
    assert table("e4")[0][:3] == ["Scenario", "Category", "Sector"]
    for alias in ("e1", "e2", "e3", "e4"):
        h, _ = table(alias)
        assert label_columns(h)[0] == "2021" and label_columns(h)[-1] == "2050", alias
        assert any(r["2050"] == "" for r in rows(alias)), alias
    assert all(r["2037"] == "" and r["2050"] == "" for r in rows("e4"))
    assert {r["Category"] for r in rows("e4")} == {"Emissions (MtCO2e)"}
    assert table("d1")[0][:5] == ["CODE_x", "Local_Authority", "Year", "Scenario", "Sector"]
    assert "stock Hydrogen boiler" not in table("d2")[0]
    assert "stock Hydrogen boiler" in table("d3")[0]
    assert any(r["CODE_x"] == "E06000053" and r["Scenario"] == "CT" for r in rows("d2"))
    assert {r["Year"] for r in rows("d3")} == {"2030"}
    assert any(r["stock Hydrogen boiler"] == "" for r in rows("d3"))
    assert any(r["stock Hydrogen boiler"] != "" for r in rows("d3"))
    assert table("r1")[0][:5] == ["la_code", "Local_Authority", "Year", "Pathway", "Sector"]
    assert "stock Community" in table("r1")[0] and "stock Community" not in table("r8")[0]
    assert all(
        float(r["stock Hydrogen boiler"]) == 0
        and float(r["stock Hybrid (ASHP + Hydrogen boiler)"]) == 0
        for r in rows("r1")
    )
    assert any(r["la_code"] == "E06000053" for a in ("r1", "r8") for r in rows(a))
    assert any(r["Technology"].endswith(" ") for r in rows("a1"))
    assert {r["Future Year"] for r in rows("a1")} <= {"2030/31", "2035/36", "2040/41"}
    assert any(r["Historical weather year"] == "1999/2000" for r in rows("a3"))
    assert any(r["Weather-conditional LOLE (h/year)"] == "0" for r in rows("a3"))


@pytest.mark.parametrize("family", RECORDED)
def test_record_shape_matches_the_unit_spec(family: str) -> None:
    """Detects a record that drifts from the spec: one csv/utf-8 record, no issue time, temporal
    ``none``, whole-capture selection per ``resource_id``, ``ckan_last_modified`` vintage, the
    exact ``edition_by_filename`` pairs of the SILVER resources only (none for B2 and the
    results table) and the spec's entity key (``edition`` after ``resource_id`` exactly when the
    record maps editions)."""
    record = _record(family)
    assert (record.version, record.reader, record.encoding) == ("1", "csv", "utf-8")
    assert record.temporal.kind == "none"
    assert record.latest == "whole_capture"
    assert record.latest_partition == "resource_id"
    assert record.vintage == "ckan_last_modified"
    assert record.xlsx is None and record.siblings == ()
    assert all(epoch.issue.kind == "none" for epoch in record.epochs)
    assert record.edition_by_filename == EDITIONS[family]
    assert record.entity_key == KEYS[family]
    assert ("edition" in record.entity_key) == (EDITIONS[family] is not None)


def test_b2_and_the_results_table_carry_no_edition() -> None:
    """Detects an edition invented for the example profiles (no edition evidenced: the file
    modification date, the version in the filename and the example month are not one) or the
    results table (its description states none; associating it with the 2025 study is an
    inference): no map, no ``edition`` column and none in the key."""
    for family in (B2, A3F):
        record = _record(family)
        assert record.edition_by_filename is None
        names = [name for name, _type in generic.output_columns(record)]
        assert "edition" not in names and "edition" not in record.entity_key, family
    for family in (A1F, A2F):
        assert "edition" in [n for n, _t in generic.output_columns(_record(family))]


@pytest.mark.parametrize(
    ("family", "epochs"),
    [
        (WS1, (("w1", "w4"), ("w3",))),
        (WS2, (("e1",), ("e2", "e3"), ("e4",))),
    ],
)
def test_unpivot_epochs_carry_the_exact_year_labels(
    family: str, epochs: tuple[tuple[str, ...], ...]
) -> None:
    """Detects a dropped, invented or mis-mapped year, an epoch merged away or a blank column
    removed: one epoch per distinct header (WS1: ``Scenario`` for 2023 and 2026, ``Pathway`` for
    2025; WS2: ``Scenario`` / ``Sector``, ``Pathway`` / ``Sector``, ``Scenario`` / ``Category`` /
    ``Sector``), each frozen exactly as the real body's reader-returned header, the year labels
    exactly the header's year columns in order, each mapped to its integer and ending in 2050
    (the 2026 ``2037``-``2050`` blank tail included), the value a nullable float64 with no token
    or bound."""
    record = _record(family)
    assert len(record.epochs) == len(epochs)
    for epoch, aliases in zip(record.epochs, epochs, strict=True):
        for alias in aliases:
            header, _ = table(alias)
            assert list(epoch.header) == header, alias
        assert epoch.unpivot is not None
        labels = [label for label, _ in epoch.unpivot.years]
        assert labels == label_columns(list(epoch.header))
        years = [year for _, year in epoch.unpivot.years]
        assert years == [int(label) for label in labels]
        assert years == list(range(FIRST_YEAR[aliases[0]], 2051))
        assert epoch.unpivot.value.dtype == "float64" and epoch.unpivot.value.nullable is True
        assert epoch.unpivot.value.null_tokens == ()
        assert (epoch.unpivot.value.min, epoch.unpivot.value.max) == (None, None)


@pytest.mark.parametrize(
    ("family", "epochs"),
    [
        (B2, (("b2",),)),
        (DOM, (("d1",), ("d2",), ("d3",))),
        (RES, (("r1",), ("r8",))),
        (A1F, (("a1",),)),
        (A2F, (("a2",),)),
        (A3F, (("a3",),)),
    ],
)
def test_wide_records_have_no_unpivot_and_exact_epochs(
    family: str, epochs: tuple[tuple[str, ...], ...]
) -> None:
    """Detects a year-long or already-long body forced through an unpivot, an epoch dropped or
    merged (domestic D0 / D5 / D30, residential R24 / R25) or a header retyped: every epoch has
    no unpivot, is frozen exactly as its real body's reader-returned header and aligns 1:1 with
    its columns."""
    record = _record(family)
    assert len(record.epochs) == len(epochs)
    for epoch, aliases in zip(record.epochs, epochs, strict=True):
        assert epoch.unpivot is None
        for alias in aliases:
            assert list(epoch.header) == table(alias)[0], alias
        assert [c.source for c in epoch.columns] == list(epoch.header)
    names = [name for name, _type in generic.output_columns(record)]
    assert "value" not in names
    assert ("projection_year" in names) == (family in {DOM, RES})


@pytest.mark.parametrize("alias", ["b2", "w1", "w3", "w4", "e1", "e2", "e4", *("d1", "d2", "d3")])
def test_columns_have_the_spec_names_types_and_nullability(alias: str) -> None:
    """Detects an identifier cast to a number, a null token declared, a column nullable that the
    data never blanks (or the reverse), or a vendor name mangled: the epoch matching fixture
    ``alias`` declares exactly the spec's silver name, dtype and nullability per vendor column,
    no null tokens, no bounds and no trim."""
    record = _record(CAPTURES[alias].family)
    header, _ = table(alias)
    epoch = next(e for e in record.epochs if list(e.header) == header)
    spec = columns_of(alias)
    declared = {c.source: (c.name, c.dtype, c.nullable) for c in epoch.columns}
    assert declared == {h: spec[h] for h in header if h in spec and not h.isdigit()}
    for column in epoch.columns:
        assert column.null_tokens == () and (column.min, column.max) == (None, None)
        assert column.trim is None


@pytest.mark.parametrize("alias", ["r1", "r8", "a1", "a2", "a3"])
def test_residential_and_resource_adequacy_columns_have_the_spec_shape(alias: str) -> None:
    """Detects the residential and Resource Adequacy columns off spec (same checks as above for
    the remaining year-long and already-long bodies)."""
    record = _record(CAPTURES[alias].family)
    header, _ = table(alias)
    epoch = next(e for e in record.epochs if list(e.header) == header)
    spec = columns_of(alias)
    assert {c.source: (c.name, c.dtype, c.nullable) for c in epoch.columns} == spec
    for column in epoch.columns:
        assert column.null_tokens == () and column.trim is None


def test_the_silver_names_follow_the_rule_and_the_reserved_names_are_avoided() -> None:
    """Detects ``month`` kept (a reserved Hive partition name), ``Year`` kept as
    ``year_vendor``, a technology label mangled or ``CODE_x`` normalised: B2 ``month`` lands as
    ``month_vendor``, LA ``Year`` as ``projection_year`` (a column, not a date), and a sample of
    the stock / stock_proportion names are the proposal's."""
    b2 = {c.name for e in _record(B2).epochs for c in e.columns}
    assert "month_vendor" in b2 and "month" not in b2 and "avg_cop_dhw" in b2
    dom = {c.name for e in _record(DOM).epochs for c in e.columns}
    res = {c.name for e in _record(RES).epochs for c in e.columns}
    for names in (dom, res):
        assert "projection_year" in names and "year_vendor" not in names and "year" not in names
    for name in (
        "code_x",
        "stock_hybrid_ashp_biolpg_boiler",
        "stock_hybrid_ashp_hydrogen_boiler",
        "stock_proportion_totalhps",
        "stock_proportion_oil_boiler",
    ):
        assert name in dom, name
    assert "stock_proportion_total" not in dom
    for name in ("la_code", "stock_fossil_fuel_communal_heating", "stock_biofuel_boiler"):
        assert name in res, name


def test_scenario_and_pathway_are_two_nullable_columns_never_merged() -> None:
    """Detects ``Scenario`` and ``Pathway`` (WS1, WS2) renamed into one label, or the domestic
    ``Scenario`` and residential ``Pathway`` merged (the vendor does not equate them): each
    epoch declares only the one its header has, both are nullable strings (domestic and
    residential: required strings of their own family) and in the key."""
    for family in (WS1, WS2):
        record = _record(family)
        assert {"scenario", "pathway"} <= set(record.entity_key)
        for epoch in record.epochs:
            declared = {c.name for c in epoch.columns} & {"scenario", "pathway"}
            assert len(declared) == 1, (family, declared)
            for column in epoch.columns:
                if column.name in {"scenario", "pathway"}:
                    assert column.nullable is True and column.dtype == "string"
    assert [e.header[1] for e in _record(WS1).epochs] == ["Scenario", "Pathway"]
    assert [e.header[0] for e in _record(WS2).epochs] == ["Scenario", "Pathway", "Scenario"]
    dom = {c.name for e in _record(DOM).epochs for c in e.columns}
    res = {c.name for e in _record(RES).epochs for c in e.columns}
    assert "scenario" in dom and "pathway" not in dom and "code_x" in dom and "la_code" not in dom
    assert "pathway" in res and "scenario" not in res and "la_code" in res and "code_x" not in res


def test_residential_technology_columns_are_never_aliased() -> None:
    """Detects the 2024 ``Community`` / ``DH`` / ``Biofuel`` columns renamed to or merged with
    the 2025 ``Fossil fuel communal heating`` / ``Low carbon district heating`` / ``BioLPG``
    ones (NESO does not establish an alias): each edition declares only its own names, the
    other edition's names are absent from its epoch, and all eight are nullable."""
    record = _record(RES)
    r24, r25 = record.epochs
    only24 = {snake(c) for c in R24_ONLY}
    only25 = {snake(c) for c in R25_ONLY}
    names24 = {c.name for c in r24.columns}
    names25 = {c.name for c in r25.columns}
    assert only24 <= names24 and not only24 & names25
    assert only25 <= names25 and not only25 & names24
    nullable = {c.name: c.nullable for e in record.epochs for c in e.columns}
    for name in only24 | only25:
        assert nullable[name] is True, name
    shared = (names24 & names25) - {"la_code", "local_authority", "projection_year", "pathway"}
    shared -= {"sector"}
    assert len(shared) == 10
    for name in shared:
        assert nullable[name] is False, name


def test_held_and_eligible_exactly_as_ruled() -> None:
    """Detects a hold lost or reworded, or a package-level hold: B2, WS1, WS2 and both
    local-authority families are held E-SEM with the spec's verbatim question, the three
    Resource Adequacy tables are eligible (units in the headers, April-March semantics
    evidenced), and all five packages stay eligible."""
    registry = load_registry()
    for family, question in HELD_QUESTIONS.items():
        package, entry = registry.families[family]
        effective = effective_eligibility(package, entry)
        assert effective.status == "held", family
        assert effective.unit == "E-SEM"  # type: ignore[union-attr]
        assert effective.question == question  # type: ignore[union-attr]
        assert package.eligibility.status == "eligible"
    for family in ELIGIBLE_FAMILIES:
        package, entry = registry.families[family]
        assert effective_eligibility(package, entry).status == "eligible", family
        assert package.eligibility.status == "eligible"
        assert _record(family).eligibility is None


@pytest.mark.parametrize("family", RECORDED)
def test_the_family_lives_in_its_package_file(family: str) -> None:
    """Detects a record put in another package file: each record is in its own package file, and
    the file's recorded families are exactly the spec's."""
    slug, _id = PACKAGES[family]
    document = json.loads((REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8"))
    recorded = {f["key"] for f in document["families"] if f.get("record")}
    assert family in recorded
    assert recorded == {f for f in RECORDED if PACKAGES[f][0] == slug}


def test_building_inputs_have_no_record() -> None:
    """Detects ``building_heat_model_list_input`` gaining a record (RULINGS 538). The vendor body
    repeats an identical row (``DOMESTIC HOT WATER`` on CSV lines 30 and 167, both runs blank)
    and the engine's duplicate guard rejects it under every key, even the full row, so a record
    would fail every capture. Silent deduplication, a row ordinal and section forward-fill are
    new mechanisms, so the family stays ingest-only (the resource stays SILVER to it, not DOC)
    until NESO corrects the body."""
    registry = load_registry()
    entry = registry.families[B1][1]
    assert entry.record is None
    resource = registry.resources[CAPTURES["b1"].resource_id][1]
    assert resource.family == B1
    assert isinstance(resource.disposition, SilverDisposition) and resource.disposition.key == B1


def test_the_building_inputs_fixture_fails_every_key_including_the_full_row(
    tmp_path: Path,
) -> None:
    """Detects a key that would let the input list through (the claim behind the no-record
    ruling): with the real engine's own typing and duplicate guard, ``(resource_id, parameter)``
    shares four rows (the repeated heat-loss label and the repeated heading) and
    ``(resource_id, parameter, run_1, run_2)`` still shares the two identical heading rows."""
    meta = CAPTURES["b1"]
    path = tmp_path / "raw.csv"
    path.write_bytes(body("b1"))
    ctx = CaptureContext(
        capture_id="bronze/neso_data_portal/x/2026/10/08/raw.csv",
        partition_date=DAY,
        body=path,
        sidecar=tmp_path / "raw.meta.json",
        capture_written_at=datetime.fromisoformat(meta.written),
        resource_id=meta.resource_id,
        resource_filename=meta.filename,
        url_type="upload",
        body_sha256="0" * 64,
        empty_capture=False,
        published_at=datetime(2026, 7, 30, 14, 1, tzinfo=UTC),
    )

    def attempt(key: list[str]) -> DuplicateEntityKeyError:
        record = SchemaRecord.model_validate(
            {
                "version": "1",
                "reader": "csv",
                "encoding": "utf-8",
                "epochs": [
                    {
                        "header": ["parameter", "run_1", "run_2"],
                        "columns": [
                            {
                                "source": "parameter",
                                "name": "parameter",
                                "dtype": "string",
                                "nullable": False,
                            },
                            {
                                "source": "run_1",
                                "name": "run_1",
                                "dtype": "string",
                                "nullable": True,
                            },
                            {
                                "source": "run_2",
                                "name": "run_2",
                                "dtype": "string",
                                "nullable": True,
                            },
                        ],
                        "issue": {"kind": "none"},
                    }
                ],
                "temporal": {"kind": "none"},
                "entity_key": key,
                "latest": "whole_capture",
                "latest_partition": "resource_id",
                "vintage": "ckan_last_modified",
            }
        )
        (child,) = read_csv_body(path, record, ())
        typed = type_child(child, record, ctx)
        with pytest.raises(DuplicateEntityKeyError) as info:
            finish_capture([typed.frame], ExclusionTally(), record, ctx, "x")
        return info.value

    assert "4 row(s) sharing an entity key" in str(attempt(["resource_id", "parameter"]))
    full = attempt(["resource_id", "parameter", "run_1", "run_2"])
    assert "2 row(s) sharing an entity key" in str(full)


def test_ledger_has_exactly_the_one_ws1_2025_failed_entry() -> None:
    """Detects an adjudication the registry no longer backs, a wrong class or cause, an entry
    for another SCN-3 family (the WS1 2024 body is a resource HOLD, not a failure), the ruling id
    changed or the reason reworded: exactly one ``fes_ws1_gas_supply`` entry, ``failed`` /
    ``DuplicateEntityKeyError`` of ruling 839 on the 2025 capture, the spec's reason and
    question verbatim, validating against the registry."""
    entries = registry_module.load_reconcile_adjudications(None)
    mine = [e for e in entries if e.family in set(RECORDED) | {B1}]
    assert [e.family for e in mine] == [WS1]
    (entry,) = mine
    assert (entry.category, entry.cause, entry.ruling) == (
        "failed",
        "DuplicateEntityKeyError",
        RULING,
    )
    (capture_path,) = entry.captures
    assert CAPTURES[FAILED_RESOURCE].resource_id in capture_path
    assert capture_path.startswith("bronze/neso_data_portal/fes_ws1_gas_supply/2026/10/08/")
    assert entry.reason == LEDGER_REASON
    assert entry.question == LEDGER_QUESTION
    assert "K-SCN-3-SPEC" in entry.evidence and "ADR-040" in entry.evidence
    assert registry_module.reconcile_adjudication_problems(load_registry(), entries) == []


# --------------------------------------------------------------------------- #
# Typing through the generic engine
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("alias", TYPED_UNPIVOT)
def test_unpivot_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects a family without a generated transformer, a header matching no epoch (the BOM
    bodies, the Scenario / Pathway swaps), a cast the vendor body does not satisfy, a row
    excluded or lost, a blank projection cell dropped, and a stamp wrong: the capture completes
    with one long row per source row and header year label, zero exclusions, ``edition`` from
    the filename, the resource id stamped, ``projection_year`` an integer, a unique key and
    every cell equal to the vendor's."""
    meta = CAPTURES[alias]
    family = meta.family
    capture_id, written = _run(data, alias)
    header, records = table(alias)
    assert written == len(records) * len(label_columns(header))
    completion = read_completion(data, family, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == written
    assert completion["rows_excluded"] == 0

    frame = _silver(data, family)
    expected_columns = [name for name, _type in generic.output_columns(_record(family))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected_columns if c not in ("year", "month")
    ]
    assert frame.schema["projection_year"] == pl.Int64
    assert frame.schema["value"] == pl.Float64
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert set(frame["projection_year"].to_list()) == {int(y) for y in label_columns(header)}
    assert frame.select(list(KEYS[family])).is_duplicated().sum() == 0
    assert actual_long(frame, family) == expected_long(alias)


@pytest.mark.parametrize("alias", TYPED_WIDE)
def test_wide_fixture_types_with_no_exclusion(data: Path, alias: str) -> None:
    """Detects rows lost, merged, excluded or altered and a stamp wrong in the year-long and
    already-long bodies: one silver row per populated source record, zero exclusions, ``edition``
    from the filename (absent for B2 and the results table), the resource id stamped, a unique
    key and every cell equal to the vendor's under the spec's dtype (``Year`` and ``hh_period``
    int64, the labelled years strings, measures float64)."""
    meta = CAPTURES[alias]
    family = meta.family
    capture_id, written = _run(data, alias)
    _header, records = table(alias)
    assert written == len(records)
    completion = read_completion(data, family, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated" and completion["rows_excluded"] == 0
    frame = _silver(data, family)
    expected_columns = [name for name, _type in generic.output_columns(_record(family))]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected_columns if c not in ("year", "month")
    ]
    assert frame["resource_id"].to_list() == [meta.resource_id] * frame.height
    if meta.edition is None:
        assert "edition" not in frame.columns
    else:
        assert frame["edition"].to_list() == [meta.edition] * frame.height
    assert frame.select(list(KEYS[family])).is_duplicated().sum() == 0
    dtypes = {"string": pl.String, "int64": pl.Int64, "float64": pl.Float64}
    for _vendor, (name, dtype, _nullable) in columns_of(alias).items():
        assert frame.schema[name] == dtypes[dtype], name
    assert actual_wide(frame, alias) == expected_wide(alias)


def test_b2_keeps_the_clock_a_string_and_the_period_an_integer(data: Path) -> None:
    """Detects the ``HH:MM`` clock of the example day cast to an instant or time (no date or
    zone accompanies it) and the half-hour index typed as a measure: ``start_of_hh_period`` is a
    string equal to its cell, ``hh_period`` an int64, no datetime column but the engine's
    stamps, ``timestamp_utc`` the capture time, the archetype's trailing space and mojibake
    kept."""
    _run(data, "b2")
    frame = _silver(data, B2)
    assert frame.schema["start_of_hh_period"] == pl.String
    assert frame.schema["hh_period"] == pl.Int64
    assert sorted(frame["start_of_hh_period"].unique().to_list()) == sorted(
        {r["start_of_hh_period"] for r in rows("b2")}
    )
    assert all(re.fullmatch(r"\d\d:\d\d", c) for c in frame["start_of_hh_period"].to_list())
    assert {c for c, t in frame.schema.items() if isinstance(t, pl.Datetime)} <= {
        "timestamp_utc",
        "event_time",
        "available_at",
        "published_at",
        "capture_written_at",
    }
    assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(CAPTURES["b2"].written)}
    assert set(frame["building_archetype"].to_list()) == {
        r["building_archetype"] for r in rows("b2")
    }
    assert all(a.endswith(" ") for a in frame["building_archetype"].to_list())
    assert set(frame["month_vendor"].to_list()) == {"jan"}


def test_b2_blank_cells_are_null_never_zero(data: Path) -> None:
    """Detects a blank gas, electricity or COP cell read as zero or dropping its row, and a
    genuine zero turned null (a blank is not zero, FACTS g5): every blank is a null, every
    ``0`` stays ``0.0``, the five never-blank measures have no null."""
    _run(data, "b2")
    frame = _silver(data, B2)
    for vendor in (
        "gas_space_heat",
        "avg_COP_space_heat",
        "electricity_space_heat",
        "gas_dhw",
        "avg_COP_dhw",
        "total_gas",
    ):
        name = B2_COLUMNS[vendor][0]
        assert frame[name].null_count() == sum(r[vendor] == "" for r in rows("b2")), vendor
    assert frame["avg_cop_space_heat"].null_count() > 0 and frame["gas_dhw"].null_count() > 0
    zeros = sum(
        r["heater_output_space_heat"] != "" and float(r["heater_output_space_heat"]) == 0
        for r in rows("b2")
    )
    assert zeros > 0 and (frame["heater_output_space_heat"] == 0.0).sum() == zeros
    for vendor in (
        "heater_output_space_heat",
        "heater_output_dhw",
        "electricity_dhw",
        "total_heater_output",
        "total_electricity",
    ):
        assert frame[B2_COLUMNS[vendor][0]].null_count() == 0, vendor


def test_year_headers_land_as_integers_blank_cells_as_null_rows_zeros_kept(data: Path) -> None:
    """Detects a blank projection cell read as zero or dropping its row, a genuine zero turned
    null, a negative sign lost or a year label left a string (FACTS g5: no zero run is a
    sentinel): in WS1 and WS2 every blank projection cell of the fixtures is a null-valued long
    row, every ``0`` / ``0.0`` stays ``0.0``, every negative equals the vendor's and
    ``projection_year`` is an int64 of the header label."""
    for family in UNPIVOT_FAMILIES:
        aliases = [a for a in TYPED_UNPIVOT if CAPTURES[a].family == family]
        for alias in aliases:
            _run(data, alias)
        frame = _silver(data, family)
        cells = [v for a in aliases for r in rows(a) for k, v in r.items() if k.isdigit()]
        blanks = sum(v == "" for v in cells)
        zeros = sum(v != "" and float(v) == 0 for v in cells)
        negatives = sorted(float(v) for v in cells if v.startswith("-"))
        assert blanks > 0 and zeros > 0, family
        assert frame.schema["projection_year"] == pl.Int64
        assert frame["value"].null_count() == blanks, family
        assert (frame["value"] == 0.0).sum() == zeros, family
        assert sorted(frame.filter(pl.col("value") < 0)["value"].to_list()) == negatives, family
    assert negatives


def test_the_2026_blank_tails_stay_as_null_valued_rows(data: Path) -> None:
    """Detects WS1 and WS2 2026's all-blank ``2037``-``2050`` tail (FACTS: columns every row
    blank) dropped or filled: each fixture row has a row for each of the 14 blank years, all
    null, and the row count is source rows x labels."""
    for alias, family, labels in (("w4", WS1, 31), ("e4", WS2, 30)):
        _, written = _run(data, alias)
        n = len(rows(alias))
        assert written == n * labels
        tail = _silver(data, family).filter(pl.col("projection_year") >= 2037)
        assert tail.height == n * 14, family
        assert tail["value"].null_count() == tail.height, family


def test_blank_ws1_dimensions_are_null_and_keep_their_rows(data: Path) -> None:
    """Detects a row with a blank ``Sector`` / ``Fuel`` / ``Feedstock`` / ``Category Lookup``
    excluded or the blank kept as text (FACTS g5: 26 / 177 / 291 / 321 blanks in WS1 2023): the
    rows are written, the dimension is null and the three always-populated ones never are."""
    _run(data, "w1")
    frame = _silver(data, WS1)
    for vendor, name in (
        ("Sector", "sector"),
        ("Fuel", "fuel"),
        ("Feedstock", "feedstock"),
        ("Category Lookup", "category_lookup"),
    ):
        blank = sum(r[vendor] == "" for r in rows("w1"))
        assert blank > 0, vendor
        assert frame[name].null_count() == blank * 31, vendor
        assert "" not in frame[name].drop_nulls().to_list()
    for name in ("fuel_type", "units", "category", "scenario"):
        assert frame[name].null_count() == 0, name
    assert frame["pathway"].null_count() == frame.height


def test_scenario_and_pathway_land_in_separate_columns(data: Path) -> None:
    """Detects the two vendor labels merged in the silver: WS1 2023 / 2026 and WS2 2023 / 2026
    rows have a ``scenario`` and a null ``pathway``, WS2 2024 / 2025 the reverse; WS1 2025 (with
    its repeated pair removed so it types) has a ``pathway`` and a null ``scenario``; the
    ``Ten Year Outlook`` stays a scenario; the WS2 2026 ``category`` is its own column."""
    for alias in ("w1", "w4", "e1", "e2", "e3", "e4"):
        _run(data, alias)
    only_third = body("w3").split(b"\r\n")
    raw = b"\r\n".join([only_third[0], only_third[3], b""])
    _run(data, "w3", raw=raw)
    ws1 = _silver(data, WS1)
    for edition in (2023, 2026):
        part = ws1.filter(pl.col("edition") == edition)
        assert part["scenario"].null_count() == 0 and part["pathway"].null_count() == part.height
    part = ws1.filter(pl.col("edition") == 2025)
    assert part.height == 31
    assert part["pathway"].null_count() == 0 and part["scenario"].null_count() == part.height
    assert set(ws1.filter(pl.col("edition") == 2026)["scenario"].to_list()) == {"Ten Year Outlook"}
    ws2 = _silver(data, WS2)
    for edition in (2023, 2026):
        part = ws2.filter(pl.col("edition") == edition)
        assert part["scenario"].null_count() == 0 and part["pathway"].null_count() == part.height
    for edition in (2024, 2025):
        part = ws2.filter(pl.col("edition") == edition)
        assert part["pathway"].null_count() == 0 and part["scenario"].null_count() == part.height
    for edition in (2023, 2024, 2025):
        assert ws2.filter(pl.col("edition") == edition)["category"].null_count() == (
            ws2.filter(pl.col("edition") == edition).height
        )
    assert set(ws2.filter(pl.col("edition") == 2026)["category"].to_list()) == {
        "Emissions (MtCO2e)"
    }
    for family in UNPIVOT_FAMILIES:
        assert _silver(data, family).select(list(KEYS[family])).is_duplicated().sum() == 0


def test_units_stay_verbatim_and_no_unit_is_invented(data: Path) -> None:
    """Detects a unit parsed out, a label normalised, a unit column invented for WS2 or the
    local-authority families, or a global unit applied (RULINGS 707): WS1 keeps ``TWh`` / ``GW`` /
    ``bcm/yr`` per row exactly as the cell, WS2 (whose dictionaries give ``N/A`` or no unit for
    most editions) has no unit column at all, the LA stock / proportion columns carry none, and
    the Resource Adequacy units live in the vendor column names."""
    for alias in ("w1", "w4", "e1", "e4", "d3", "r1"):
        _run(data, alias)
    ws1 = _silver(data, WS1)
    assert set(ws1["units"].to_list()) == {"TWh", "GW", "bcm/yr"}
    assert sorted(ws1["units"].to_list()) == sorted(
        r["Units"] for a in ("w1", "w4") for r in rows(a) for _ in label_columns(table(a)[0])
    )
    for family in (WS2, DOM, RES):
        columns = _silver(data, family).columns
        assert not [c for c in columns if re.search(r"(^|_)units?(_|$)", c)], family
    assert "MTCO2e" not in " ".join(_silver(data, WS2).columns)
    assert {name for name, _d, _n in A1_COLUMNS.values()} >= {
        "installed_capacity_mw",
        "stored_energy_mwh",
    }
    assert {
        A2_COLUMNS["Peak Demand (MW)"][0],
        A3_COLUMNS["Weather-conditional LOLE (h/year)"][0],
    } == {
        "peak_demand_mw",
        "weather_conditional_lole_h_year",
    }


def test_la_projection_year_is_the_body_year_and_scilly_rows_are_kept_as_captured(
    data: Path,
) -> None:
    """Detects a projection year taken from the filename, a rectangular fill of the Isles of
    Scilly (``E06000053``: absent in 2020, ``CT`` and ``LW`` only later) or a dropped row:
    ``projection_year`` equals the body ``Year`` cell, each slice has exactly one value, the
    Scilly rows are exactly the fixture's and no ``FS`` / ``ST`` Scilly row appears."""
    for alias in ("d1", "d2", "d3", "r1", "r8"):
        _run(data, alias)
    for family, aliases in ((DOM, ("d1", "d2", "d3")), (RES, ("r1", "r8"))):
        frame = _silver(data, family)
        for alias in aliases:
            part = frame.filter(pl.col("resource_id") == CAPTURES[alias].resource_id)
            assert set(part["projection_year"].to_list()) == {int(r["Year"]) for r in rows(alias)}
            assert part.height == len(rows(alias))
    dom = _silver(data, DOM)
    scilly = dom.filter(pl.col("code_x") == "E06000053")
    expected = [
        (r["Scenario"], int(r["Year"]))
        for a in ("d1", "d2", "d3")
        for r in rows(a)
        if r["CODE_x"] == "E06000053"
    ]
    assert sorted(
        zip(scilly["scenario"].to_list(), scilly["projection_year"].to_list(), strict=True)
    ) == sorted(expected)
    assert {s for s, _y in expected} <= {"CT", "LW"} and expected
    assert _silver(data, RES).filter(pl.col("la_code") == "E06000053").height == sum(
        r["la_code"] == "E06000053" for a in ("r1", "r8") for r in rows(a)
    )


def test_blank_domestic_hydrogen_cells_are_null_and_zeros_stay_zero(data: Path) -> None:
    """Detects a blank hydrogen / hybrid-hydrogen cell read as zero (FACTS: 1111 / 741 blanks in
    the 2030-2050 slices, meaning unstated) and the all-zero residential hydrogen columns (1484
    zero rows each in R24 / R25 2020-2030) read as null: the domestic blanks are null, the
    nonblank hydrogen cells equal the vendor's, the residential zeros are kept values."""
    _run(data, "d3")
    dom = _silver(data, DOM)
    for vendor in (
        "stock Hydrogen boiler",
        "stock Hybrid (ASHP + Hydrogen boiler)",
        "stock_proportion Hydrogen boiler",
        "stock_proportion Hybrid (ASHP + Hydrogen boiler)",
    ):
        name = snake(vendor)
        assert dom[name].null_count() == sum(r[vendor] == "" for r in rows("d3")), vendor
    assert dom["stock_hydrogen_boiler"].null_count() > 0
    assert dom["stock_ashp"].null_count() == 0
    _run(data, "r1")
    res = _silver(data, RES)
    assert res["stock_hydrogen_boiler"].null_count() == 0
    assert (res["stock_hydrogen_boiler"] == 0.0).all()
    assert (res["stock_hybrid_ashp_hydrogen_boiler"] == 0.0).all()


def test_domestic_proportions_stay_in_vendor_scale(data: Path) -> None:
    """Detects a proportion multiplied to a percentage or relabelled (FACTS: kept in vendor
    scale) and the oil-boiler contradiction silently corrected: every ``stock_proportion`` value
    equals its cell and stays within 0..1 here, ``stock_oil_boiler`` equals its stock cell."""
    _run(data, "d1")
    frame = _silver(data, DOM)
    for name in (c for c in frame.columns if c.startswith("stock_proportion_")):
        values = frame[name].drop_nulls()
        if values.len():
            assert 0.0 <= values.min() <= values.max() <= 1.0, name  # type: ignore[operator]
    assert sorted(frame["stock_oil_boiler"].to_list()) == sorted(
        float(r["stock Oil boiler"]) for r in rows("d1")
    )


def test_ra_labels_stay_strings_and_a_trailing_space_is_kept(data: Path) -> None:
    """Detects an April-March label cast to an integer or split (``2030/31``, ``1999/2000``: a
    direct Int64 cast fails by design), a technology trimmed (the vendor's trailing space is
    data) or the portfolio ``Category`` made part of the key: the three label columns are
    strings equal to their cells, the padded technology survives byte for byte, ``category`` is
    an attribute outside the key."""
    for alias in ("a1", "a2", "a3"):
        _run(data, alias)
    a1, a2, a3 = (_silver(data, f) for f in (A1F, A2F, A3F))
    for frame in (a1, a2, a3):
        for column in ("future_year", "spotlight_year", "historical_weather_year"):
            if column in frame.columns:
                assert frame.schema[column] == pl.String
    assert set(a1["future_year"].to_list()) == {r["Future Year"] for r in rows("a1")}
    assert set(a2["historical_weather_year"].to_list()) == {
        r["Historical weather year"] for r in rows("a2")
    }
    assert "1999/2000" in a3["historical_weather_year"].to_list()
    padded = {r["Technology"] for r in rows("a1") if r["Technology"].endswith(" ")}
    assert padded and padded <= set(a1["technology"].to_list())
    assert "category" not in KEYS[A1F] and "category" in a1.columns
    with pytest.raises(InvalidOperationError):
        pl.Series(["2030/31"], dtype=pl.String).cast(pl.Int64, strict=True)


def test_ra_zero_measures_are_values(data: Path) -> None:
    """Detects a zero stored energy, installed capacity or LOLE read as missing (no source
    establishes a zero-as-missing convention): the zeros of the fixtures survive as ``0.0`` and
    no measure has a null."""
    for alias in ("a1", "a3"):
        _run(data, alias)
    a1, a3 = _silver(data, A1F), _silver(data, A3F)
    assert (a1["stored_energy_mwh"] == 0.0).sum() == sum(
        r["Stored energy (MWh)"] == "0" for r in rows("a1")
    )
    assert (a3["weather_conditional_lole_h_year"] == 0.0).sum() == sum(
        r["Weather-conditional LOLE (h/year)"] == "0" for r in rows("a3")
    )
    assert (a1["stored_energy_mwh"] == 0.0).sum() > 0 and (
        a3["weather_conditional_lole_h_year"] == 0.0
    ).sum() > 0
    for frame in (a1, a3):
        assert frame.select(pl.col(pl.Float64).null_count()).sum_horizontal().item() == 0


@pytest.mark.parametrize(
    ("alias", "cell_name", "value"),
    [
        ("w1", "2030", "N/A"),
        ("w4", "2030", "6.06 "),
        ("e1", "2030", "3,118.00"),
        ("e4", "2030", "N/A"),
        ("b2", "gas_dhw", "N/A"),
        ("a2", "Peak Demand (MW)", "N/A"),
    ],
)
def test_an_uncastable_cell_fails_the_capture(
    data: Path, alias: str, cell_name: str, value: str
) -> None:
    """Detects a record that swallows a bad cell (a null-token list, a lenient cast, a thousands
    separator, padding): the lexical forms that break other bodies (``N/A``, ``3,118.00``,
    ``6.06 ``) fail the whole capture before any row is written and leave no completion."""
    family = CAPTURES[alias].family
    raw = _replace_cell(body(alias), 2, table(alias)[0].index(cell_name), value)
    capture_id = capture(data, alias, raw=raw)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_the_ws1_2024_n_a_cell_fails_the_strict_cast_which_proves_the_resource_hold(
    data: Path,
) -> None:
    """Detects an ``N/A`` the record could read (the claim behind the resource HOLD): the real
    2024 body offered to the WS1 record as if it were a SILVER resource (under the 2025
    filename, the one that shares its ``Pathway`` header, and the 2025 resource id: a HOLD
    resource is skipped by id before any cast) fails the strict float64 cast, writes nothing and
    leaves no completion; no ``null_tokens`` declaration exists in any epoch."""
    cells = [v for r in rows("w2") for k, v in r.items() if k.isdigit()]
    assert "N/A" in cells
    with pytest.raises(InvalidOperationError):
        pl.Series([c for c in cells if c != ""], dtype=pl.String).cast(pl.Float64, strict=True)
    capture_id = capture(
        data,
        "w2",
        filename="fes2025_ws1_v006.csv",
        resource_id=CAPTURES[FAILED_RESOURCE].resource_id,
    )
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, WS1, data).run(DAY, run_id="r")
    assert read_completion(data, WS1, capture_id) is None
    assert _no_silver(data, WS1)
    assert all(e.unpivot.value.null_tokens == () for e in _record(WS1).epochs)  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("alias", "column"),
    [
        ("w1", "Units"),
        ("e1", "Sector"),
        ("d1", "CODE_x"),
        ("r1", "la_code"),
        ("a2", "Spotlight year"),
        ("b2", "run_id"),
    ],
)
def test_a_blank_identifier_is_excluded_and_counted(data: Path, alias: str, column: str) -> None:
    """Detects an identifier made nullable (a blank ``Units`` / ``Sector`` / ``CODE_x`` /
    ``la_code`` / ``Spotlight year`` / ``run_id`` becoming a null-keyed row): the source row's
    every output row is excluded, counted on the transformer and in the completion, and no null
    reaches the output."""
    family = CAPTURES[alias].family
    header, records = table(alias)
    per_row = len(label_columns(header)) or 1
    raw = _replace_cell(body(alias), 2, header.index(column), "")
    capture_id = capture(data, alias, raw=raw)
    transformer = get_transformer(SOURCE, family, data)
    written = transformer.run(DAY, run_id="r")
    assert written == (len(records) - 1) * per_row
    assert transformer.last_excluded_row_count == per_row
    completion = read_completion(data, family, capture_id)
    assert completion is not None and completion["rows_excluded"] == per_row
    assert _silver(data, family)[columns_of(alias)[column][0]].null_count() == 0


@pytest.mark.parametrize("alias", ["w1", "e1", "d1", "r1", "a1", "a2", "a3", "b2"])
def test_a_repeated_key_fails_the_capture(data: Path, alias: str) -> None:
    """Detects a key too coarse to guard the body: a repeated dimension tuple with another
    measure fails with ``DuplicateEntityKeyError`` naming the shared rows and leaves no
    completion (never deduplicated)."""
    family = CAPTURES[alias].family
    raw = body(alias)
    text = raw.decode("utf-8-sig").split("\r\n")
    header = next(csv.reader([text[0]]))
    fields = next(csv.reader([text[1]]))
    spec = columns_of(alias)
    index = next(
        i
        for i, h in enumerate(header)
        if spec.get(h, ("", "string", False))[1] == "float64" or h.isdigit()
    )
    fields[index] = "123.456"
    out = io.StringIO()
    csv.writer(out, lineterminator="").writerow(fields)
    duplicate = out.getvalue()
    assert duplicate != text[1]
    capture_id = capture(data, alias, raw=raw + duplicate.encode() + b"\r\n")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    shared = 2 * (len(label_columns(header)) or 1)
    assert f"{shared} row(s) sharing an entity key" in info.value.failures[0][2]
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_the_ws1_2025_pair_fails_the_capture_and_only_the_pair_does(data: Path) -> None:
    """Detects a duplicate caused by anything but the vendor's repeated rows (a wrong key
    column, a baseline label colliding with a year): the 2025 fixture with the Electric
    Engagement Bioenergy / Waste pair (CSV lines 80 and 150, 2050 ``0.0`` vs ``13.465``) raises
    ``DuplicateEntityKeyError`` over 62 rows (2 x 31 years), leaves no completion and writes no
    silver; the same fixture with the second row of the pair removed completes with zero
    exclusions."""
    capture_id = capture(data, "w3")
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, WS1, data).run(DAY, run_id="r")
    assert [(c, cls) for c, cls, _m in info.value.failures] == [
        (capture_id, DuplicateEntityKeyError.__name__)
    ]
    assert "62 row(s) sharing an entity key" in info.value.failures[0][2]
    assert read_completion(data, WS1, capture_id) is None
    assert _no_silver(data, WS1)


def test_removing_the_repeated_row_makes_the_ws1_2025_capture_type(data: Path) -> None:
    """Detects a collision caused by anything but the repeated rows: the same fixture with the
    second row of the Electric Engagement pair removed completes with zero exclusions, one long
    row per remaining row and year, so the failure above is the vendor's repeated tuple and
    nothing else."""
    parts = body("w3").split(b"\r\n")
    without = b"\r\n".join([parts[0], parts[1], parts[3], b""])
    capture_id, written = _run(data, "w3", raw=without)
    completion = read_completion(data, WS1, capture_id)
    assert completion is not None and completion["rows_excluded"] == 0
    assert written == 2 * 31


@pytest.mark.parametrize("family", RECORDED)
def test_a_header_outside_the_epochs_fails_loud(data: Path, family: str) -> None:
    """Detects a lenient header match: a body whose first column is renamed (a vendor schema
    change) matches no frozen epoch and fails the capture, leaving no completion and no
    silver."""
    alias = next(a for a in TYPED if CAPTURES[a].family == family)
    capture_id = capture(data, alias, raw=_replace_cell(body(alias), 0, 0, "Renamed"))
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


def test_a_residential_2025_body_under_the_2024_header_fails_loud(data: Path) -> None:
    """Detects the two residential epochs merged or matched leniently (R24 and R25 differ in
    four technology names each): a 2025 body whose renamed columns are swapped back to the 2024
    names matches neither epoch and fails."""
    raw = body("r8")
    head, rest = raw.split(b"\r\n", 1)
    swapped = head.replace(b"stock Fossil fuel communal heating", b"stock Community")
    capture_id = capture(data, "r8", raw=swapped + b"\r\n" + rest)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, RES, data).run(DAY, run_id="r")
    assert read_completion(data, RES, capture_id) is None


# --------------------------------------------------------------------------- #
# Editions
# --------------------------------------------------------------------------- #


def test_editions_are_stamped_from_the_filename_and_all_are_served(data: Path) -> None:
    """Detects an edition inferred from the vintage or the year labels, or one edition displacing
    another: WS2 2023 (vintage 2023), 2024, 2025 and 2026 stamp their filename's year and the
    domestic slices (one resource each, all 2022 although published 2022-08) are each served by
    ``_latest``."""
    ids = {a: _run(data, a)[0] for a in ("e1", "e2", "e3", "e4")}
    frame = _silver(data, WS2)
    by_capture = {
        cid: set(editions)
        for cid, editions in frame.group_by("bronze_capture_id")
        .agg(pl.col("edition").unique())
        .iter_rows()
    }
    assert by_capture == {
        ids["e1"]: {2023},
        ids["e2"]: {2024},
        ids["e3"]: {2025},
        ids["e4"]: {2026},
    }
    dom_ids = {a: _run(data, a)[0] for a in ("d1", "d2", "d3")}
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    assert set(both_as_of(db, data, WS2, None)) == set(ids.values())
    assert set(both_as_of(db, data, DOM, None)) == set(dom_ids.values())
    assert set(_silver(data, DOM)["edition"].to_list()) == {2022}


def test_the_last_year_in_a_filename_is_never_the_edition() -> None:
    """Detects an edition parsed from the filename's last year: ``fes24_..._2050_...`` is 2024,
    ``fes25_..._2050_...`` 2025 and ``fes22_..._2050_...`` 2022, never 2050 (NESO's FES 2024
    resources carry projection years 2020-2050)."""
    pairs_res = dict(_record(RES).edition_by_filename or ())
    pairs_dom = dict(_record(DOM).edition_by_filename or ())
    assert pairs_res["fes24_la_level_outputs_2050_residential_all_scenarios.csv"] == 2024
    assert pairs_res["fes25_la_level_outputs_2050_residential_all_scenarios.csv"] == 2025
    assert pairs_dom["fes22_la_level_outputs_2050_domestic_all_scenarios.csv"] == 2022
    assert len(pairs_res) == 13 and len(pairs_dom) == 7
    assert 2050 not in set(pairs_res.values()) | set(pairs_dom.values())


def test_a_2050_slice_is_stamped_with_its_edition_not_its_last_year(data: Path) -> None:
    """Detects the same defect through the engine: a residential body captured under the exact
    ``fes24_..._2050_...`` filename lands with ``edition`` 2024."""
    capture_id = capture(
        data, "r1", filename="fes24_la_level_outputs_2050_residential_all_scenarios.csv"
    )
    get_transformer(SOURCE, RES, data).run(DAY, run_id="r")
    frame = _silver(data, RES)
    assert set(frame["edition"].to_list()) == {2024}
    assert set(frame["bronze_capture_id"].to_list()) == {capture_id}


@pytest.mark.parametrize(
    ("alias", "wrong"),
    [
        ("w1", "fes2024_ws1_v001.csv"),
        ("w4", "fes2026_ws1_v001.csv"),
        ("e1", "fes2022_ws2_v001.csv"),
        ("d1", "fes22_la_level_outputs_2055_domestic_all_scenarios.csv"),
        ("r1", "fes24_la_level_outputs_2020_residential_all_scenarios.csv.bak"),
        ("r8", "fes26_la_level_outputs_2025_residential_all_scenarios.csv"),
        ("a1", "portfolio-capacity-dataset-v2.csv"),
        ("a2", "peak-demand.csv"),
    ],
)
def test_an_unmapped_filename_fails_loud(data: Path, alias: str, wrong: str) -> None:
    """Detects a fallback edition: a filename the map does not list (a new year, a suffixed copy,
    the held WS1 2024 resource's own name) fails the capture, leaves no completion and writes
    nothing."""
    family = CAPTURES[alias].family
    capture_id = capture(data, alias, filename=wrong)
    with pytest.raises(NesoCaptureFailedError):
        get_transformer(SOURCE, family, data).run(DAY, run_id="r")
    assert read_completion(data, family, capture_id) is None
    assert _no_silver(data, family)


# --------------------------------------------------------------------------- #
# Dispositions: the held resource, the failed capture, the untouched ones
# --------------------------------------------------------------------------- #


def test_the_ws1_2024_resource_is_held_with_its_reason() -> None:
    """Detects a resource routed to silver, dispositioned DOC/GIS, given an edition entry or left
    without its reason: WS1 2024 alone is a resource-level HOLD (E-SEM) quoting the FACTS
    evidence, the other three WS1 resources stay SILVER to the family, the package stays
    eligible and the held filename has no edition pair."""
    registry = load_registry()
    held = registry.resources[CAPTURES[HELD_RESOURCE].resource_id][1]
    assert held.family == WS1
    assert isinstance(held.disposition, HoldDisposition)
    assert held.disposition.unit == "E-SEM"
    for needle in W2_HOLD_NEEDLES:
        assert needle in held.disposition.reason, needle
    mapped = {fn for fn, _edition in _record(WS1).edition_by_filename or ()}
    assert CAPTURES[HELD_RESOURCE].filename not in mapped
    slug, _id = PACKAGES[WS1]
    package = next(p for p in registry.packages if p.package == slug)
    assert package.eligibility.status == "eligible"
    silver = {r.id for r in package.resources if isinstance(r.disposition, SilverDisposition)}
    assert silver == {CAPTURES[a].resource_id for a in ("w1", "w3", "w4")}
    for resource in package.resources:
        assert resource.family == WS1


def test_every_other_resource_is_silver_to_its_own_family_and_docs_unchanged() -> None:
    """Detects a resource moved to another family or disposition, or the two Building Heat Model
    files (workbook and user guide) changing from DOC: every resource of the five packages other
    than WS1 2024 is SILVER keyed to its own family (the ones with no record included), the two
    Building Heat Model files stay DOC in the files family, and every package is eligible."""
    registry = load_registry()
    for slug in {slug for slug, _id in PACKAGES.values()}:
        package = next(p for p in registry.packages if p.package == slug)
        assert package.eligibility.status == "eligible", slug
        for resource in package.resources:
            if resource.id == CAPTURES[HELD_RESOURCE].resource_id:
                continue
            if resource.family == "building_heat_model_files":
                assert isinstance(resource.disposition, DocDisposition), resource.id
                continue
            assert isinstance(resource.disposition, SilverDisposition), resource.id
            assert resource.disposition.key == resource.family
    for family in (*RECORDED, B1):
        by_family = {
            r.id
            for p in registry.packages
            for r in p.resources
            if r.family == family and isinstance(r.disposition, SilverDisposition)
        }
        wanted = {
            CAPTURES[a].resource_id
            for a in CAPTURES
            if CAPTURES[a].family == family and a != HELD_RESOURCE
        }
        assert wanted <= by_family, family
    la = next(p for p in registry.packages if p.package == PACKAGES[DOM][0])
    assert sum(r.family == DOM for r in la.resources) == 7
    assert sum(r.family == RES for r in la.resources) == 13


def test_the_held_ws1_capture_is_never_transformed_and_only_the_2025_one_fails(
    data: Path,
) -> None:
    """Detects the held WS1 2024 capture reaching an owner (a failed capture) despite its HOLD:
    with the four WS1 captures present the 2023 and 2026 editions complete, the 2024 one has no
    completion and no failure record, the 2025 one fails with its duplicate and only it has a
    failure record (the reconcile view of the same set is the adjudication test below)."""
    ids = {a: capture(data, a) for a in ("w1", "w2", "w3", "w4")}
    transformer = get_transformer(SOURCE, WS1, data)
    with pytest.raises(NesoCaptureFailedError) as info:
        transformer.run(DAY, run_id="r")
    assert [cid for cid, _cls, _m in info.value.failures] == [ids["w3"]]
    assert read_completion(data, WS1, ids["w1"]) is not None
    assert read_completion(data, WS1, ids["w4"]) is not None
    assert read_completion(data, WS1, ids["w2"]) is None
    assert read_failure(data, WS1, ids["w2"]) is None
    assert read_failure(data, WS1, ids["w3"]) is not None
    assert set(_silver(data, WS1)["resource_id"].to_list()) == {
        CAPTURES["w1"].resource_id,
        CAPTURES["w4"].resource_id,
    }


def test_the_ws1_2025_failure_is_adjudicated_not_open_and_not_stale(
    data: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects a collision capture left as an open gap (reconcile red forever), an entry that no
    longer matches its failure record (stale), or an adjudication that alters data: without the
    ledger reconcile reports one ``failed`` gap; with the committed entry (its capture id
    swapped for the fixture capture's) it is adjudicated, none is open or stale, ruling 839 is
    named, nothing is reported for the HOLD, and the silver and state bytes are equal before and
    after."""
    slug, _id = PACKAGES[WS1]
    document = json.loads((REGISTRY_DIR / f"{slug}.json").read_text(encoding="utf-8"))
    install_generated(monkeypatch, data / "_registry", [document])
    captured = {a: capture(data, a) for a in ("w1", "w2", "w3", "w4")}
    with pytest.raises(NesoCaptureFailedError) as info:
        get_transformer(SOURCE, WS1, data).run(DAY, run_id="r")
    assert [cls for _c, cls, _m in info.value.failures] == ["DuplicateEntityKeyError"]

    def snapshot() -> dict[str, bytes]:
        return {
            p.relative_to(data).as_posix(): p.read_bytes()
            for top in ("silver", "state")
            for p in sorted((data / top).rglob("*"))
            if p.is_file()
        }

    before = snapshot()
    code, out = run_cli(WS1, "--cutoff", DAY.isoformat())
    assert code == 1, out
    gaps = [line for line in out if line.startswith("GAP")]
    assert len(gaps) == 1 and "failed" in gaps[0] and captured["w3"] in gaps[0]
    committed = json.loads(
        (REGISTRY_DIR / RECONCILE_ADJUDICATIONS_FILE).read_text(encoding="utf-8")
    )
    (entry,) = [e for e in committed if e["family"] == WS1]
    swapped = [{**entry, "captures": [captured["w3"]]}]
    (data / "_registry" / RECONCILE_ADJUDICATIONS_FILE).write_text(
        registry_module.dump_json(swapped), encoding="utf-8"
    )
    code, out = run_cli(WS1, "--cutoff", DAY.isoformat())
    assert code == 0, out
    assert "SUMMARY adjudicated 1" in out
    assert "SUMMARY stale_adjudication 0" in out
    assert [line for line in out if line.startswith("GAP")] == []
    adjudicated = [line for line in out if captured["w3"] in line]
    assert len(adjudicated) == 1
    assert "DuplicateEntityKeyError" in adjudicated[0] and f"ruling {RULING}" in adjudicated[0]
    assert not [line for line in out if captured["w2"] in line]
    assert snapshot() == before


# --------------------------------------------------------------------------- #
# Vintage and generated artefacts
# --------------------------------------------------------------------------- #


def test_the_vintage_is_the_ckan_last_modified_and_as_of_is_bounded_by_it(data: Path) -> None:
    """Detects an issue-time proxy or a projection-year clock (RULINGS 529/597), or a catalogue
    view that cannot carry the columns: ``available_at`` is the CKAN ``last_modified`` (the peak
    demand table: 2025-12-12, not the 2030 spotlight), ``timestamp_utc`` stays the capture time, an
    as-of read before the vintage serves nothing and one after it serves the capture; the
    unpivoted WS2 2023 table (vintage 2023-07) behaves the same."""
    for alias, family in (("a2", A2F), ("e1", WS2)):
        meta = CAPTURES[alias]
        capture_id, _ = _run(data, alias)
        frame = _silver(data, family)
        vintage = datetime.fromisoformat(meta.modified).replace(tzinfo=UTC)
        assert set(frame["available_at"].to_list()) == {vintage}
        assert set(frame["timestamp_utc"].to_list()) == {datetime.fromisoformat(meta.written)}
        db = data / "cat.duckdb"
        init_catalogue(db, data)
        before = vintage - timedelta(days=1)
        after = datetime(vintage.year + 1, 1, 1, tzinfo=UTC)
        assert both_as_of(db, data, family, before) == []
        assert set(both_as_of(db, data, family, after)) == {capture_id}
        assert set(both_as_of(db, data, family, None)) == {capture_id}


@pytest.mark.parametrize(
    ("family", "title", "needles"),
    [
        (B2, "Building Heat Model", ("`month_vendor`", "`hh_period`", "`avg_cop_dhw`")),
        (WS1, "Whole System & Gas Supply (WS1)", ("`projection_year`", "`pathway`", "`units`")),
        (WS2, "Whole System & Gas Supply - Emissions (WS2)", ("`projection_year`", "`category`")),
        (
            DOM,
            "Local authority level spatial heat model outputs",
            ("`code_x`", "`projection_year`"),
        ),
        (RES, "Local authority level spatial heat model outputs", ("`la_code`", "`pathway`")),
        (A1F, "Resource Adequacy in the 2030s", ("`future_year`", "`installed_capacity_mw`")),
        (A2F, "Resource Adequacy in the 2030s", ("`peak_demand_mw`", "`historical_weather_year`")),
        (
            A3F,
            "Resource Adequacy in the 2030s",
            ("`simulation_name`", "`weather_conditional_eeu_mwh_year`"),
        ),
    ],
)
def test_the_skeleton_page_renders_the_new_record(
    family: str, title: str, needles: tuple[str, ...]
) -> None:
    """Detects a record the docs generator cannot render (the unpivot columns, the held question,
    the held resource)."""
    slug, _id = PACKAGES[family]
    page = skeleton.render_package(
        load_registry(),
        {
            "name": slug,
            "title": title,
            "organization": {"title": "FES: Pathways to Net Zero"},
            "license_title": "NESO Open Data Licence",
            "extras": [],
        },
        None,
    )
    for needle in (*needles, family):
        assert needle in page, needle


def test_every_fixture_is_named_in_the_provenance() -> None:
    """Detects a fixture left out of the provenance table, a hold question, HOLD reason or ledger
    text missing from it, or a non-holding TODO dropped: every committed fixture is named with
    its resource id, the five held questions, the HOLD reason, the ledger reason and question
    and the two Resource Adequacy non-holding TODOs appear verbatim."""
    provenance = (FIXTURES / "PROVENANCE.md").read_text(encoding="utf-8")
    for alias, meta in CAPTURES.items():
        assert f"`{alias}.csv`" in provenance, alias
        assert meta.resource_id in provenance, alias
    committed = {p.stem for p in FIXTURES.glob("*.csv")}
    assert committed == set(CAPTURES)
    assert re.search(r"^## Held questions", provenance, re.MULTILINE)
    for question in HELD_QUESTIONS.values():
        assert question in provenance
    assert LEDGER_REASON in provenance and LEDGER_QUESTION in provenance
    held = registry_module.load_registry().resources[CAPTURES[HELD_RESOURCE].resource_id][1]
    assert isinstance(held.disposition, HoldDisposition)
    assert held.disposition.reason in provenance
    assert "EDITION-EVIDENCE" in provenance and "VENDOR-UNIQUENESS" in provenance
