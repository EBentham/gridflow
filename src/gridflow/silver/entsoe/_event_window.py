"""ENTSO-E event-window filter scope and exemptions (R2-A Task 4 / B4 N-9).

Unlike Elexon's source-scoped publication-window filter
(``silver/elexon/_publication_window.py``, D-6), the ENTSO-E event-window
filter is opted in PER TRANSFORMER via ``BaseSilverTransformer.EVENT_WINDOW_
FILTER`` (a plain ``ClassVar[bool]``, default ``False``) -- there is no
per-dataset lookup table here that gates enforcement at runtime. This module
exists purely as the AUDIT surface: :data:`EVENT_WINDOW_FILTER_EXEMPT` records
a reason for every registered ENTSO-E dataset that is NOT opted in, so
``test_every_entsoe_transformer_is_classified`` can assert every registered
dataset is either opted in (``EVENT_WINDOW_FILTER = True`` on its own class)
or exempt with a recorded reason here -- never silently unclassified.
:data:`EVENT_WINDOW_CLASSIFICATION` is the companion, total-coverage
classification map (B4, D-1): dataset -> verdict -> citation ->
transformer-family, checked machine-side against the live class graph
(I-1..I-9 below).

**Scope (B4, N-9): the opt-in set is the union of R2-A's original 7 datasets
and B4's evidence-classified FILTER_SAFE set -- see
``EVENT_WINDOW_CLASSIFICATION`` below for the full dataset -> verdict ->
citation -> transformer-family map (R3 research, 2026-08-03/04). Every
opt-in and every EXEMPT entry rides a cited verdict; nothing here is
inferred without a citation.**

A `TODO`-marked minority remains genuinely unclassified (the record supports
no verdict either way) -- the N-9 gate and F-10 therefore remain OPEN even
after B4 lands, closing only when a future pass resolves the remainder or a
milestone-close decision accepts the residual (a Bobbo decision, not this
unit's to make).

Everything else falls into one of two exemption classes below:

1. **Named horizon exemptions** -- datasets whose window covers a forecast
   horizon (week/month/year-ahead, "PT7D" weekly snapshots) or an
   open-ended revision stream (outages), where trimming to a single UTC
   delivery day would delete nearly the whole dataset, plus
   ``generation_units_master_data`` which has no ``periodStart``/
   ``periodEnd`` at all (``date_param`` snapshot query).
2. **Unclassified** -- the datasets whose evidence supports no window
   verdict (fixture-only or unwired, never observed populated-and-
   window-compared) carry a literal `` `TODO`-marked `` reason: this plan
   deliberately does NOT infer their window semantics (CLAUDE.md: "Do not
   invent... write a TODO and stop"). Tracked as **N-9, a v0.18 milestone
   gate**. See ``EVENT_WINDOW_CLASSIFICATION`` for why each one stays open.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Classification(StrEnum):
    """A B4 (N-9) event-window verdict for one ENTSO-E dataset (D-1a).

    No evidence-quality category is authored anywhere in this module (rev 8,
    D-1a) -- only the frozen verdict itself. What the cited evidence shows
    lives in :attr:`DatasetClassification.evidence` /
    :attr:`DatasetClassification.limitation` prose, checked against the
    artifacts by the verifier's open-every-citation protocol, not by a closed
    formal system over open empirical evidence.
    """

    FILTER_SAFE = "FILTER_SAFE"
    EXEMPT = "EXEMPT"
    UNKNOWN = "UNKNOWN"


#: The datasets R2-A already classified (its 7 opt-ins plus its 13 horizon
#: exemptions) -- a frozen set-membership fact of history, not an evidence
#: category (D-1a). Equal to the 48 registered ENTSO-E datasets minus B4's
#: 28-dataset research population (R3-RESEARCH.md Sec 1.1) -- checked against
#: a live registry walk by I-9 (``test_entsoe_event_window_classification.py``),
#: never re-derived from this literal. Members carry a pointer citation and an
#: empty ``limitation`` in ``EVENT_WINDOW_CLASSIFICATION`` (I-9); membership
#: cannot overlap with the 28-dataset population.
R2A_CARRIED_DATASETS: frozenset[str] = frozenset(
    {
        # R2-A's 7 opt-ins (ADR-026 D-9).
        "day_ahead_prices",
        "actual_load",
        "load_forecast",
        "actual_generation",
        "actual_generation_units",
        "wind_solar_forecast",
        "generation_forecast",
        # R2-A's 13 named horizon exemptions (ADR-026 D-9).
        "load_forecast_weekly",
        "load_forecast_monthly",
        "load_forecast_yearly",
        "forecast_margin",
        "installed_capacity",
        "installed_capacity_units",
        "water_reservoirs",
        "outages_generation",
        "outages_consumption",
        "outages_transmission",
        "outages_offshore_grid",
        "outages_production",
        "generation_units_master_data",
    }
)


@dataclass(frozen=True, slots=True)
class DatasetClassification:
    """One dataset's B4 (N-9) event-window verdict, citation and lineage (D-1a).

    No field here is derived and no property is computed -- every field is
    authored, checked at test time against the live class graph (I-8) and the
    frozen research record (I-4, I-5, I-9), never against each other.
    """

    doc_type: str
    """Non-empty ENTSO-E document type, e.g. ``"A11"`` or ``"A25/B09"``."""

    classification: Classification
    """The frozen verdict (B-2) -- never re-derived, never moved by an
    evidence-prose correction."""

    evidence: str
    """Non-empty citation, derived from R3-RESEARCH.md Sec 1.1 and the
    measured request-vs-response intervals (B-7), stating what the cited
    artifacts show. When :attr:`probes` is empty this MUST name the vault
    page path carrying the evidence (I-4) -- except for
    :data:`R2A_CARRIED_DATASETS` members, whose evidence is the pointer
    citation (I-9), e.g. ``"R2-A D-9; reason inline in
    EVENT_WINDOW_FILTER_EXEMPT"``."""

    limitation: str
    """What the cited evidence does NOT establish. Non-empty for every entry
    NOT in :data:`R2A_CARRIED_DATASETS`; empty for every member (I-9).
    UNKNOWN entries state why the record supports no verdict."""

    probes: tuple[str, ...]
    """Filenames under ``.planning/phases/R3-test-integrity/probes/``. An
    empty tuple means vault-backed evidence with no local artifact.
    Multi-artifact entries (e.g. a ZIP's ``_extracted`` companion) list every
    file, so the verifier never has to guess which one to open."""

    family: str
    """Non-empty. The immediate base class name, or ``"own"`` when the
    immediate base is ``BaseSilverTransformer`` (I-8)."""

    transformer: str
    """Non-empty. The registered concrete transformer class name (I-8)."""


#: Every ENTSO-E dataset NOT opted into the event-window filter, with a
#: recorded reason (A-6 / A-11-equivalent completeness). Reasons are
#: verbatim from the plan's Task 4 classification (S3) -- a reviewer can
#: audit each exemption against ``connectors/entsoe/endpoints.py`` directly.
#: Entries added by B4 (the previously TODO-classified population) cite
#: ``EVENT_WINDOW_CLASSIFICATION`` below for their full evidence -- this
#: dict's reasons for those entries are short exemption labels, not the
#: complete citation.
EVENT_WINDOW_FILTER_EXEMPT: dict[str, str] = {
    # --- horizon: A31/A32/A33 forecasts (week/month/year-ahead) ---
    "load_forecast_weekly": (
        "A31 week-ahead load forecast -- horizon dataset, never trimmed to a single day"
    ),
    "load_forecast_monthly": (
        "A32 month-ahead load forecast -- horizon dataset, never trimmed to a single day"
    ),
    "load_forecast_yearly": (
        "A33 year-ahead load forecast -- horizon dataset, never trimmed to a single day"
    ),
    "forecast_margin": (
        "A33 year-ahead forecast margin -- horizon dataset, never trimmed to a single day"
    ),
    "installed_capacity": (
        "A33 year-ahead installed generation capacity -- horizon dataset, never trimmed"
    ),
    "installed_capacity_units": (
        "A33 year-ahead installed capacity per production unit -- horizon dataset, never trimmed"
    ),
    "water_reservoirs": (
        "PT7D points recorded at the week start -- trimming to a single UTC day "
        "would delete nearly the whole dataset"
    ),
    "outages_generation": (
        "unavailability of generation units -- open-ended revision horizon, never trimmed"
    ),
    "outages_consumption": (
        "aggregated unavailability of consumption units -- open-ended revision horizon"
    ),
    "outages_transmission": (
        "unavailability of transmission infrastructure -- open-ended revision horizon"
    ),
    "outages_offshore_grid": (
        "unavailability of offshore grid infrastructure -- open-ended revision horizon"
    ),
    "outages_production": (
        "unavailability of production units -- open-ended revision horizon, never trimmed"
    ),
    "generation_units_master_data": (
        "date_param snapshot query (Implementation_DateAndOrTime) -- no periodStart/"
        "periodEnd request window to filter at all"
    ),
    # --- B4 (N-9): EXEMPT on a cited reason (R3-RESEARCH.md Sec 1.1) ---
    "congestion_management_costs": (
        "live-probed P1M monthly aggregate spanning the whole calendar month (A92) -- "
        "a day-exact trim would delete the row on every day but one"
    ),
    "balancing_financial_expenses_income": (
        "live-probed P1M monthly aggregate spanning the whole calendar month (A87) -- "
        "a day-exact trim would delete the row on every day but one; independently "
        "confirms the same pattern as A92 from the structurally distinct h8_balancing.py family"
    ),
    "auction_revenue": (
        "inferred EXEMPT: shares the _H6AmountTransformer/EntsoeTransmissionMarketAmount "
        "class with the live-probed P1M congestion_management_costs (A92) sibling on the "
        "12.1.A/12.1.E article-class ground -- own probes real EMPTY, not independently "
        "confirmed (B-4)"
    ),
    "congestion_income": (
        "inferred EXEMPT: shares the _H6AmountTransformer/EntsoeTransmissionMarketAmount "
        "class with the live-probed P1M congestion_management_costs (A92) sibling on the "
        "12.1.A/12.1.E article-class ground -- own probe real EMPTY, not independently "
        "confirmed (B-4)"
    ),
    # --- unclassified: N-9, a v0.18 milestone gate. Do not infer semantics ---
    "contracted_reserves": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "current_balancing_state": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "aggregated_balancing_energy_bids": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "cross_zonal_balancing_capacity": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "activated_balancing_qty": (
        "TODO: unreachable -- not wired into DOC_TYPES or sources.yaml, "
        "so no fetch path exists and no evidence can be gathered. "
        "Cannot be classified until wired (N-22)."
    ),
}


#: The B4 (N-9) classification map -- one entry per registered ENTSO-E
#: dataset (48, I-3 total coverage), dataset -> verdict -> citation ->
#: transformer-family. The single audit surface F-10's closure and the N-9
#: gate check against (D-1, D-1a). Grouped below into the 20 R2-A-carried
#: pointer entries (I-9), then the 28-dataset B4 research population
#: (R3-RESEARCH.md Sec 1.1), in the research table's own order.
EVENT_WINDOW_CLASSIFICATION: dict[str, DatasetClassification] = {
    # --- R2-A carried (pointer citations, I-9) -- reason inline in
    # --- EVENT_WINDOW_FILTER_EXEMPT / on the class's own EVENT_WINDOW_FILTER ---
    "day_ahead_prices": DatasetClassification(
        doc_type="A44",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="DayAheadPricesTransformer",
    ),
    "actual_load": DatasetClassification(
        doc_type="A65/A16",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="ActualLoadTransformer",
    ),
    "load_forecast": DatasetClassification(
        doc_type="A65/A01",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="LoadForecastTransformer",
    ),
    "actual_generation": DatasetClassification(
        doc_type="A75/A16",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="ActualGenerationTransformer",
    ),
    "actual_generation_units": DatasetClassification(
        doc_type="A73/A16",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="ActualGenerationUnitsTransformer",
    ),
    "wind_solar_forecast": DatasetClassification(
        doc_type="A69/A01",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="WindSolarForecastTransformer",
    ),
    "generation_forecast": DatasetClassification(
        doc_type="A71/A01",
        classification=Classification.FILTER_SAFE,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="GenerationForecastTransformer",
    ),
    "load_forecast_weekly": DatasetClassification(
        doc_type="A65/A31",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="LoadForecastWeeklyTransformer",
    ),
    "load_forecast_monthly": DatasetClassification(
        doc_type="A65/A32",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="LoadForecastTransformer",
        transformer="LoadForecastMonthlyTransformer",
    ),
    "load_forecast_yearly": DatasetClassification(
        doc_type="A65/A33",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="LoadForecastTransformer",
        transformer="LoadForecastYearlyTransformer",
    ),
    "forecast_margin": DatasetClassification(
        doc_type="A70/A33",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="ForecastMarginTransformer",
    ),
    "installed_capacity": DatasetClassification(
        doc_type="A68/A33",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="InstalledCapacityTransformer",
    ),
    "installed_capacity_units": DatasetClassification(
        doc_type="A71/A33",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="InstalledCapacityUnitsTransformer",
    ),
    "water_reservoirs": DatasetClassification(
        doc_type="A72/A16",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="WaterReservoirsTransformer",
    ),
    "outages_generation": DatasetClassification(
        doc_type="A80",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="OutagesGenerationTransformer",
    ),
    "outages_consumption": DatasetClassification(
        doc_type="A76",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="_H7OutageTransformer",
        transformer="OutagesConsumptionTransformer",
    ),
    "outages_transmission": DatasetClassification(
        doc_type="A78",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="_H7OutageTransformer",
        transformer="OutagesTransmissionTransformer",
    ),
    "outages_offshore_grid": DatasetClassification(
        doc_type="A79",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="_H7OutageTransformer",
        transformer="OutagesOffshoreGridTransformer",
    ),
    "outages_production": DatasetClassification(
        doc_type="A77",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="_H7OutageTransformer",
        transformer="OutagesProductionTransformer",
    ),
    "generation_units_master_data": DatasetClassification(
        doc_type="A95",
        classification=Classification.EXEMPT,
        evidence="R2-A D-9; reason inline in EVENT_WINDOW_FILTER_EXEMPT",
        limitation="",
        probes=(),
        family="own",
        transformer="GenerationUnitsMasterDataTransformer",
    ),
    # --- B4 newly classified (R3-RESEARCH.md Sec 1.1, research table order) ---
    "cross_border_flows": DatasetClassification(
        doc_type="A11",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Live probe probes/entsoe_A11_cross_border_flows_FR_BE_20260801.xml: "
            "FR-BE 2026-08-01, period.timeInterval = [2026-08-01T00:00Z, "
            "2026-08-02T00:00Z), matching the request exactly (R3-RESEARCH.md:50)."
        ),
        limitation=(
            "Confirms one zone pair (FR-BE) and one day; other zone pairs and "
            "days are not independently observed."
        ),
        probes=("entsoe_A11_cross_border_flows_FR_BE_20260801.xml",),
        family="own",
        transformer="CrossBorderFlowsTransformer",
    ),
    "commercial_schedules": DatasetClassification(
        doc_type="A09",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/commercial_schedules.md, "
            "live GB-FR 2026-05-06: Publication_MarketDocument, 2 TimeSeries, hourly "
            "points (research verdict FILTER_SAFE, R3-RESEARCH.md:51)."
        ),
        limitation=(
            "Vault-recorded prior live validation, not re-probed this session; no "
            "local probe artifact exists, and the vault artifact records TimeSeries/"
            "point counts only - the response interval was never compared against "
            "the request bounds (Sec 5a)."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="CommercialSchedulesTransformer",
    ),
    "total_nominated_capacity": DatasetClassification(
        doc_type="A26/B08",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/total_nominated_capacity.md, "
            "live GB-FR 2026-05-06: 3 TimeSeries, period.timeInterval matches the "
            "request exactly, PT60M points (PASS, R3-RESEARCH.md:52)."
        ),
        limitation=(
            "Vault-recorded prior live validation, not re-probed this session; rests "
            "on a prior real live validation quoted in the vault page, not a fresh "
            "probe (Sec 5a)."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="TotalNominatedCapacityTransformer",
    ),
    "net_transfer_capacity": DatasetClassification(
        doc_type="A61",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/net_transfer_capacity.md, "
            "live GB-FR: 1 TimeSeries, window-bounded single Point (curveType A03) "
            "(PASS, R3-RESEARCH.md:53)."
        ),
        limitation=(
            "Vault-recorded prior live validation, not re-probed this session; "
            "single-Point sample, other zone pairs/days unobserved (Sec 5a)."
        ),
        probes=(),
        family="own",
        transformer="NetTransferCapacityTransformer",
    ),
    "net_positions": DatasetClassification(
        doc_type="A25/B09",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Live probe probes/entsoe_A25_net_positions_FR_20260601.xml: FR "
            "single-zone 2026-06-01, period.timeInterval exactly the requested day, "
            "PT15M points with real quantities (R3-RESEARCH.md:54)."
        ),
        limitation="Confirms one zone (FR) and one day; other zones/days unobserved.",
        probes=("entsoe_A25_net_positions_FR_20260601.xml",),
        family="_H6QuantityTransformer",
        transformer="NetPositionsTransformer",
    ),
    "procured_balancing_capacity": DatasetClassification(
        doc_type="A15/A51",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Live probe probes/entsoe_A15_procured_balancing_capacity_DE_20260601.xml "
            "(ZIP-extraction companion: probes/entsoe_A15_extracted.xml): DE-LU "
            "2026-06-01, period.timeInterval = [2026-05-31T22:00Z, "
            "2026-06-01T02:00Z) -- crosses the lower request bound (starts 2h early, "
            "B-7), the same CET-boundary over-span shape as the already-proven A44 "
            "case (R3-RESEARCH.md:55)."
        ),
        limitation=(
            "The upper request-window relation is unobserved (B-7): the response "
            "ends about 22h short of the requested day's end, so only the "
            "over-span lower edge is confirmed, not a full-window comparison."
        ),
        probes=(
            "entsoe_A15_procured_balancing_capacity_DE_20260601.xml",
            "entsoe_A15_extracted.xml",
        ),
        family="_H8BalancingTransformer",
        transformer="ProcuredBalancingCapacityTransformer",
    ),
    "balancing_energy_bids": DatasetClassification(
        doc_type="A37/A47/B74",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Live probe probes/entsoe_A37_balancing_energy_bids_DE_20260601.xml "
            "(ZIP-extraction companion: probes/entsoe_A37_extracted.xml): DE-LU "
            "2026-06-01, real 144KB payload, reserveBid_Period.timeInterval = "
            "[2026-06-01T00:00Z, 2026-06-01T00:15Z) -- the lower edge coincides "
            "exactly with the request start (B-7), resolution PT15M "
            "(R3-RESEARCH.md:56)."
        ),
        limitation=(
            "N-21 fixed the envelope handling: parse_timeseries_xml now accepts "
            "ReserveBid_MarketDocument/Bid_TimeSeries (root-scoped, "
            "_SERIES_TAGS_BY_DOC_ROOT) rather than matching only the literal "
            "Balancing_MarketDocument/TimeSeries shape, so the live A37 payload no "
            "longer parses to zero rows. The filter has now been exercised against "
            "real A37 rows: 100 rows at 2026-06-01T00:00Z, all retained at the "
            "recorded window's lower edge under HALF_OPEN (N-21 F-5). The upper "
            "request-window relation remains untested: the response ends far short "
            "of the request end (B-7), so only the lower boundary is confirmed."
        ),
        probes=(
            "entsoe_A37_balancing_energy_bids_DE_20260601.xml",
            "entsoe_A37_extracted.xml",
        ),
        family="_H8BalancingTransformer",
        transformer="BalancingEnergyBidsTransformer",
    ),
    "dc_link_intraday_transfer_limits": DatasetClassification(
        doc_type="A93",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "dc_link_intraday_transfer_limits.md, live GB-FR: real EMPTY (Reason "
            "999, no revision published), never observed populated. Structural "
            "analogy: same _H6QuantityTransformer code path and zone_pair shape as "
            "total_nominated_capacity/total_capacity_allocated, both directly "
            "verified day-bound (R3-RESEARCH.md:57)."
        ),
        limitation=(
            "EMPTY probe only -- no populated payload observed for this dataset "
            "itself; the verdict rides the structural analogy to a directly-"
            "verified sibling, not an own-payload window comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="DcLinkIntradayTransferLimitsTransformer",
    ),
    "redispatching_cross_border": DatasetClassification(
        doc_type="A63/A46",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "redispatching_cross_border.md, live GB-FR and NL-DE sanity check, both "
            "real EMPTY (event-driven, rare). Same _H6QuantityTransformer code path "
            "as the directly-verified quantity family (R3-RESEARCH.md:58)."
        ),
        limitation=(
            "EMPTY probes only; the verdict rides structural analogy, not an own "
            "populated-payload comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="RedispatchingCrossBorderTransformer",
    ),
    "redispatching_internal": DatasetClassification(
        doc_type="A63/A85",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/redispatching_internal.md, "
            "live: real EMPTY. Same code path as redispatching_cross_border; sister "
            "dataset differing only by businessType (R3-RESEARCH.md:59)."
        ),
        limitation=(
            "EMPTY probe only; the verdict rides structural analogy to its sister "
            "dataset and the quantity family, not an own populated-payload "
            "comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="RedispatchingInternalTransformer",
    ),
    "countertrading": DatasetClassification(
        doc_type="A91",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/countertrading.md, live: "
            "real EMPTY. Same _H6QuantityTransformer code path (R3-RESEARCH.md:60)."
        ),
        limitation=(
            "EMPTY probe only; the verdict rides structural analogy, not an own "
            "populated-payload comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="CountertradingTransformer",
    ),
    "offered_transfer_capacity_continuous": DatasetClassification(
        doc_type="A31",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "offered_transfer_capacity_continuous.md, live: real EMPTY (daily + "
            "30-day retry). Same code path/schema family as net_transfer_capacity "
            "(A61, directly verified day-bound) (R3-RESEARCH.md:61)."
        ),
        limitation=(
            "EMPTY probes only across a daily retry and a 30-day retry; the "
            "verdict rides structural analogy, not an own populated-payload "
            "comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="OfferedTransferCapacityContinuousTransformer",
    ),
    "offered_transfer_capacity_implicit": DatasetClassification(
        doc_type="A31",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault (live 2026-05-08) plus this session's live probe "
            "probes/entsoe_A31_offered_implicit_FR_BE_20260601.xml, FR-BE, also "
            "real EMPTY. Never observed populated across 3 independent probes; "
            "same structural family as A61/A11 (R3-RESEARCH.md:62)."
        ),
        limitation=(
            "EMPTY across all 3 probes; the verdict rides structural analogy, not "
            "an own populated-payload comparison."
        ),
        probes=("entsoe_A31_offered_implicit_FR_BE_20260601.xml",),
        family="_H6QuantityTransformer",
        transformer="OfferedTransferCapacityImplicitTransformer",
    ),
    "offered_transfer_capacity_explicit": DatasetClassification(
        doc_type="A31",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "offered_transfer_capacity_explicit.md, live: real EMPTY. Same family "
            "(R3-RESEARCH.md:63)."
        ),
        limitation=(
            "EMPTY probe only; the verdict rides structural analogy, not an own "
            "populated-payload comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="OfferedTransferCapacityExplicitTransformer",
    ),
    "transfer_capacity_use": DatasetClassification(
        doc_type="A25/B05",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault (live 2026-05-08) plus this session's live probes "
            "probes/entsoe_A25_transfer_capacity_use_FR_BE_20260601.xml and "
            "probes/entsoe_A25_transfer_capacity_use_NO1_SE1_20180115.xml (2 "
            "pairs/eras, both real EMPTY). MW-quantity schema "
            "(EntsoeTransmissionMarketQuantity, _H6QuantityTransformer), distinct "
            "from the EUR/amount family proved monthly (Sec 1.4); deliberately "
            "re-probed given that discovery, still resolves to the quantity family "
            "(R3-RESEARCH.md:64)."
        ),
        limitation=(
            "EMPTY across both probes; the verdict rides structural analogy to the "
            "quantity family, not an own populated-payload comparison."
        ),
        probes=(
            "entsoe_A25_transfer_capacity_use_FR_BE_20260601.xml",
            "entsoe_A25_transfer_capacity_use_NO1_SE1_20180115.xml",
        ),
        family="_H6QuantityTransformer",
        transformer="TransferCapacityUseTransformer",
    ),
    "total_capacity_allocated": DatasetClassification(
        doc_type="A26/A29",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "total_capacity_allocated.md, live: real EMPTY. Sister to "
            "total_nominated_capacity (A26/B08, directly verified day-bound with "
            "real populated data); same document type, same _H6QuantityTransformer "
            "code path (R3-RESEARCH.md:65)."
        ),
        limitation=(
            "EMPTY probe only; the verdict rides structural analogy to its sister "
            "dataset, not an own populated-payload comparison."
        ),
        probes=(),
        family="_H6QuantityTransformer",
        transformer="TotalCapacityAllocatedTransformer",
    ),
    "activated_balancing_prices": DatasetClassification(
        doc_type="A84/A16/A96",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "activated_balancing_prices.md, live NL 2025-04-01: real populated "
            "Balancing_MarketDocument, 2 TimeSeries, processType=A16 (Realised, "
            "not forecast) (R3-RESEARCH.md:66)."
        ),
        limitation=(
            "Window-bound comparison not shown in the abridged vault text -- "
            "populated real data observed, but not compared against the request "
            "window."
        ),
        probes=(),
        family="own",
        transformer="ActivatedBalancingPricesTransformer",
    ),
    "imbalance_prices": DatasetClassification(
        doc_type="A85",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/imbalance_prices.md, "
            "live FR 2025-04-01: real populated data, PT15M (R3-RESEARCH.md:67)."
        ),
        limitation=(
            "Window-bound comparison not shown in the abridged vault text -- "
            "populated real data observed, but not compared against the request "
            "window."
        ),
        probes=(),
        family="own",
        transformer="ImbalancePricesTransformer",
    ),
    "imbalance_volume": DatasetClassification(
        doc_type="A86/A19",
        classification=Classification.FILTER_SAFE,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/imbalance_volume.md, "
            "live FR 2025-04-01: real populated data, PT15M (R3-RESEARCH.md:68)."
        ),
        limitation=(
            "Same caveat as imbalance_prices: window-bound comparison not shown "
            "in the abridged vault text."
        ),
        probes=(),
        family="own",
        transformer="ImbalanceVolumeTransformer",
    ),
    "congestion_management_costs": DatasetClassification(
        doc_type="A92",
        classification=Classification.EXEMPT,
        evidence=(
            "Live probe probes/entsoe_A92_congestion_costs_FR_20260601.xml: FR "
            "single-zone 2026-06-01, real populated TransmissionNetwork_"
            "MarketDocument, resolution P1M, Period.timeInterval = "
            "[2026-05-31T22:00Z, 2026-06-30T22:00Z) -- the whole month of June, "
            "not the requested day. A day-exact trim would delete this row on "
            "every day except the one containing the period start "
            "(R3-RESEARCH.md:69)."
        ),
        limitation=(
            "Confirms one zone (FR) and one month; other zones/months not "
            "independently observed, though the P1M resolution is a stable "
            "document property, not a per-instance one."
        ),
        probes=("entsoe_A92_congestion_costs_FR_20260601.xml",),
        family="_H6AmountTransformer",
        transformer="CongestionManagementCostsTransformer",
    ),
    "balancing_financial_expenses_income": DatasetClassification(
        doc_type="A87",
        classification=Classification.EXEMPT,
        evidence=(
            "Live probe probes/entsoe_A87_balancing_financial_FR_20260601.xml "
            "(ZIP-extraction companion: probes/entsoe_A87_extracted_001-"
            "FINANCIAL_EXPENSES_AND_INCOME_FOR_BALANCING_R3202605312200-"
            "202606302200.xml): FR control area 2026-06-01, real populated ZIP -> "
            "Balancing_MarketDocument, resolution P1M, Period.timeInterval = "
            "[2026-05-31T22:00Z, 2026-06-30T22:00Z), Financial_Price.amount at "
            "position 1 only (one point per month). Independently confirms the "
            "same monthly-aggregate pattern as A92 from a structurally "
            "independent code family (h8_balancing.py, not h6_market.py) "
            "(R3-RESEARCH.md:70)."
        ),
        limitation=(
            "Confirms one control area (FR) and one month; other areas/months "
            "not independently observed."
        ),
        probes=(
            "entsoe_A87_balancing_financial_FR_20260601.xml",
            "entsoe_A87_extracted_001-FINANCIAL_EXPENSES_AND_INCOME_FOR_BALANCING_"
            "R3202605312200-202606302200.xml",
        ),
        family="_H8BalancingTransformer",
        transformer="BalancingFinancialExpensesIncomeTransformer",
    ),
    "auction_revenue": DatasetClassification(
        doc_type="A25/B07",
        classification=Classification.EXEMPT,
        evidence=(
            "Shares the literal _H6AmountTransformer class (h6_market.py) and "
            "EntsoeTransmissionMarketAmount schema with the directly-probed, "
            "confirmed-monthly congestion_management_costs (A92, same class). "
            "The verdict rides the 12.1.A/12.1.E article-class sibling inference "
            "(A92/A87 P1M live-confirmed, B-4) -- A92/A87 are TSO "
            "financial-reporting articles that settle over accounting periods, "
            "not delivery days. No populated live payload was obtained for B07 "
            "itself despite 2 probe attempts this session "
            "(probes/entsoe_A25_auction_revenue_FR_BE_20260601.xml, "
            "probes/entsoe_A25_auction_revenue_NO1_SE1_20180115.xml -- both real "
            "EMPTY, a modern SDAC pair and a pre-coupling Nordic pair) "
            "(R3-RESEARCH.md:71)."
        ),
        limitation=(
            "Own probes are real EMPTY, not populated -- the EXEMPT verdict "
            "rides the 12.1.A/12.1.E article-class sibling inference (B-4), not "
            "an own populated payload. Flagged for Bobbo: inferred, not "
            "independently confirmed."
        ),
        probes=(
            "entsoe_A25_auction_revenue_FR_BE_20260601.xml",
            "entsoe_A25_auction_revenue_NO1_SE1_20180115.xml",
        ),
        family="_H6AmountTransformer",
        transformer="AuctionRevenueTransformer",
    ),
    "congestion_income": DatasetClassification(
        doc_type="A25/B10",
        classification=Classification.EXEMPT,
        evidence=(
            "Same _H6AmountTransformer/EntsoeTransmissionMarketAmount reasoning "
            "as auction_revenue, riding the same 12.1.A/12.1.E article-class "
            "sibling inference (A92/A87 P1M live-confirmed, B-4). 1 probe "
            "attempt this session (probes/entsoe_A25_congestion_income_FR_BE_"
            "20260601.xml, real EMPTY) (R3-RESEARCH.md:72)."
        ),
        limitation=(
            "Own probe is real EMPTY, not populated -- the EXEMPT verdict rides "
            "the 12.1.A/12.1.E article-class sibling inference (B-4), not an own "
            "populated payload. Flagged for Bobbo: inferred, not independently "
            "confirmed."
        ),
        probes=("entsoe_A25_congestion_income_FR_BE_20260601.xml",),
        family="_H6AmountTransformer",
        transformer="CongestionIncomeTransformer",
    ),
    "contracted_reserves": DatasetClassification(
        doc_type="A81/A52/B95",
        classification=Classification.UNKNOWN,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/contracted_reserves.md "
            "records a live FR Balancing_MarketDocument from an older window "
            "(2025-04-01) -- populated, request-shape validation only, never "
            "window-compared (contracted_reserves.md:198-202). This session's "
            "live probe probes/entsoe_A81_contracted_reserves_DE_20260601.xml "
            "(DE-LU) returned real EMPTY, and GB is EMPTY per vault. The bronze "
            "fixture sample (tests/fixtures/entsoe/contracted_reserves_gb.xml) "
            "is hand-built, not vendor-observed (R3-RESEARCH.md:73)."
        ),
        limitation=(
            "Two-sided record: an older populated FR observation never compared "
            "against a request window, plus this session's real-EMPTY DE probe. "
            "Together they still support no window verdict -- never observed "
            "populated AND window-compared in the same sample."
        ),
        probes=("entsoe_A81_contracted_reserves_DE_20260601.xml",),
        family="own",
        transformer="ContractedReservesTransformer",
    ),
    "current_balancing_state": DatasetClassification(
        doc_type="A86/B33",
        classification=Classification.UNKNOWN,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "current_balancing_state.md cites tests/fixtures/entsoe/"
            "current_balancing_state_gb.xml (fixture only, not vendor-observed) "
            "as its bronze evidence. This session's live probe "
            "probes/entsoe_A86_current_balancing_state_DE_20260601.xml (DE-LU) "
            "returned real EMPTY, and GB is EMPTY per vault (R3-RESEARCH.md:74)."
        ),
        limitation=(
            "Never observed populated; fixture-only prior evidence is not "
            "trusted alone (a sibling fixture assumption, A87, was directly "
            "falsified this session, Sec 1.4/5). The record supports no window "
            "verdict."
        ),
        probes=("entsoe_A86_current_balancing_state_DE_20260601.xml",),
        family="_H8BalancingTransformer",
        transformer="CurrentBalancingStateTransformer",
    ),
    "aggregated_balancing_energy_bids": DatasetClassification(
        doc_type="A24/A51",
        classification=Classification.UNKNOWN,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "aggregated_balancing_energy_bids.md cites tests/fixtures/entsoe/"
            "aggregated_balancing_energy_bids_gb.xml (fixture only). This "
            "session's live probe probes/entsoe_A24_aggregated_balancing_"
            "energy_bids_DE_20260601.xml (DE-LU) returned real EMPTY, and GB is "
            "EMPTY per vault (R3-RESEARCH.md:75). N-21 (Sec 0 correction) "
            "confirmed the A24 probe is a 967-byte Acknowledgement_MarketDocument, "
            'Reason/code 999 ("No matching data found"), zero TimeSeries-like '
            "elements of any name -- a real-EMPTY response, not an envelope "
            "mismatch like the A37 sibling dataset (N-21)."
        ),
        limitation=(
            "Never observed populated; fixture-only prior evidence is not "
            "trusted alone (Sec 1.4/5). The record supports no window verdict. "
            "The concrete class subclasses BalancingEnergyBidsTransformer (C-5) "
            "-- EVENT_WINDOW_FILTER is explicitly False on this class, not "
            "inherited (I-6), so this UNKNOWN verdict cannot leak in via the "
            "parent's True."
        ),
        probes=("entsoe_A24_aggregated_balancing_energy_bids_DE_20260601.xml",),
        family="BalancingEnergyBidsTransformer",
        transformer="AggregatedBalancingEnergyBidsTransformer",
    ),
    "cross_zonal_balancing_capacity": DatasetClassification(
        doc_type="A38/A51",
        classification=Classification.UNKNOWN,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "cross_zonal_balancing_capacity.md cites tests/fixtures/entsoe/"
            "cross_zonal_balancing_capacity_gb_fr.xml (fixture only). This "
            "session's live probe probes/entsoe_A38_cross_zonal_balancing_"
            "capacity_DE_FR_20260601.xml (DE-FR) returned real EMPTY, and GB-FR "
            "is EMPTY per vault (R3-RESEARCH.md:76)."
        ),
        limitation=(
            "Never observed populated; fixture-only prior evidence is not "
            "trusted alone (Sec 1.4/5). The record supports no window verdict."
        ),
        probes=("entsoe_A38_cross_zonal_balancing_capacity_DE_FR_20260601.xml",),
        family="_H8BalancingTransformer",
        transformer="CrossZonalBalancingCapacityTransformer",
    ),
    "activated_balancing_qty": DatasetClassification(
        doc_type="A83/A16",
        classification=Classification.UNKNOWN,
        evidence=(
            "Vault quant-vault/30-vendors/entsoe/datasets/"
            "activated_balancing_qty.md is the only reference; not wired into "
            "connectors/entsoe/endpoints.py::DOC_TYPES or config/sources.yaml "
            "(confirmed absent from both by grep this session) -- no live fetch "
            "path exists at all for this dataset. The only reference shape is "
            "tests/fixtures/entsoe/activated_balancing_qty_gb.xml (fixture "
            "only) (R3-RESEARCH.md:77)."
        ),
        limitation=(
            "Unwired (N-22): no live probe was possible for this dataset "
            "regardless of classification; the record supports no window "
            "verdict. Its unreachability is not fixed, wired, or removed in B4 "
            "(out of scope)."
        ),
        probes=(),
        family="own",
        transformer="ActivatedBalancingQtyTransformer",
    ),
}
