"""ENTSO-E event-window filter scope and exemptions (R2-A Task 4 / B4 N-9).

Unlike Elexon's source-scoped publication-window filter
(``silver/elexon/_publication_window.py``, D-6), the ENTSO-E event-window
filter is opted in PER TRANSFORMER via ``BaseSilverTransformer.EVENT_WINDOW_
FILTER`` (a plain ``ClassVar[bool]``, default ``False``) -- there is no
per-dataset lookup table here that gates enforcement at runtime.
:data:`EVENT_WINDOW_FILTER_EXEMPT` records a reason for every registered
ENTSO-E dataset that is NOT opted in, so
``test_every_entsoe_transformer_is_classified`` can assert every registered
dataset is either opted in (``EVENT_WINDOW_FILTER = True`` on its own class)
or exempt with a recorded reason here -- never silently unclassified.

**Scope (B4, N-9): the opt-in set is the union of R2-A's original 7 datasets
and B4's evidence-classified FILTER_SAFE set -- see ADR-026 Appendix A for the
evidence behind each verdict (R3 research, 2026-08-03/04). Every
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
   gate**. ADR-026 Appendix A records why each one stays open.
"""

from __future__ import annotations

#: Every ENTSO-E dataset NOT opted into the event-window filter, with a
#: recorded reason (A-6 / A-11-equivalent completeness). Reasons are
#: verbatim from the plan's Task 4 classification (S3) -- a reviewer can
#: audit each exemption against ``connectors/entsoe/endpoints.py`` directly.
#: Entries added by B4 (the previously TODO-classified population) have
#: their full evidence in ADR-026 Appendix A -- this dict's reasons for those
#: entries are short exemption labels, not the complete citation.
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
        "unavailability of generation units -- open-ended revision horizon; the transformer "
        "clips each outage step to the partition day itself (silver/entsoe/_outages.py)"
    ),
    "outages_consumption": (
        "aggregated unavailability of consumption units -- open-ended revision horizon; "
        "steps clipped to the partition day by the transformer"
    ),
    "outages_transmission": (
        "unavailability of transmission infrastructure -- open-ended revision horizon; "
        "steps clipped to the partition day by the transformer"
    ),
    "outages_offshore_grid": (
        "unavailability of offshore grid infrastructure -- open-ended revision horizon; "
        "steps clipped to the partition day by the transformer"
    ),
    "outages_production": (
        "unavailability of production units -- open-ended revision horizon; the transformer "
        "clips each outage step to the partition day itself (silver/entsoe/_outages.py)"
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
