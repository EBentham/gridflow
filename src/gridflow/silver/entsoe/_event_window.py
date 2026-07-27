"""ENTSO-E event-window filter scope and exemptions (R2-A Task 4).

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

**Scope closure (D-9): R2-A closes F-10 for the 7 opted-in datasets only —
NOT repo-wide.** The opt-in set is deliberately narrow and probe-proven:

- ``day_ahead_prices`` (A44) -- the measured over-span case (plan S1.4).
- ``actual_load``, ``load_forecast``, ``actual_generation``,
  ``actual_generation_units``, ``wind_solar_forecast``, ``generation_forecast``
  -- share the same ``timestamp_utc`` / ``periodStart``-``periodEnd`` shape
  and the same day-exact bronze partitioning (``_EXACT_PARTITION_ONLY_
  SOURCES``), so the identical trim mechanism applies without further proof.

Everything else falls into one of two exemption classes below:

1. **Named horizon exemptions** -- datasets whose window covers a forecast
   horizon (week/month/year-ahead, "PT7D" weekly snapshots) or an
   open-ended revision stream (outages), where trimming to a single UTC
   delivery day would delete nearly the whole dataset, plus
   ``generation_units_master_data`` which has no ``periodStart``/
   ``periodEnd`` at all (``date_param`` snapshot query).
2. **Unclassified** -- the remaining registered datasets carry a literal
   ``"TODO: unclassified"`` reason: this plan deliberately does NOT infer
   their window semantics (CLAUDE.md: "Do not invent... write a TODO and
   stop"). Tracked as **N-9, a v0.18 milestone gate** (R2-A-PLAN.md OQ-3).
   ``cross_border_flows`` is named as the leading candidate for that future
   classification pass (D-9).
"""

from __future__ import annotations

#: Every ENTSO-E dataset NOT opted into the event-window filter, with a
#: recorded reason (A-6 / A-11-equivalent completeness). Reasons are
#: verbatim from the plan's Task 4 classification (S3) -- a reviewer can
#: audit each exemption against ``connectors/entsoe/endpoints.py`` directly.
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
    # --- unclassified: N-9, a v0.18 milestone gate. Do not infer semantics ---
    "activated_balancing_prices": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "activated_balancing_qty": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "contracted_reserves": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "cross_border_flows": (
        "TODO: unclassified -- the LEADING CANDIDATE for the next classification "
        "pass (D-9) -- see N-9 (v0.18 milestone gate)"
    ),
    "dc_link_intraday_transfer_limits": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "commercial_schedules": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "redispatching_cross_border": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "redispatching_internal": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "countertrading": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "offered_transfer_capacity_continuous": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "offered_transfer_capacity_implicit": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "offered_transfer_capacity_explicit": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "transfer_capacity_use": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "total_nominated_capacity": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "total_capacity_allocated": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "net_positions": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "congestion_management_costs": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "auction_revenue": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "congestion_income": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "current_balancing_state": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "balancing_energy_bids": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "aggregated_balancing_energy_bids": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "procured_balancing_capacity": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "cross_zonal_balancing_capacity": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "balancing_financial_expenses_income": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "imbalance_prices": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "imbalance_volume": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
    "net_transfer_capacity": "TODO: unclassified -- see N-9 (v0.18 milestone gate)",
}
