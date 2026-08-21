"""Pydantic v2 silver contracts for the NESO Open Data Portal (D-24).

Distinct from ``gridflow.schemas.neso``, which is the Carbon Intensity API
(D-01). Nothing here touches that source.

The contracts declare exactly what each transformer emits, so the schema
manifest's ``columns`` (derived from ``model_fields``) describes the real
silver relation rather than an aspiration. ``BaseSchema`` carries
``strict=True``: an ``int`` where a ``float`` is declared, or a naive datetime,
is a validation failure rather than a silent coercion — which is the whole
point of casting explicitly in the transformer (D-19) instead of letting
inference decide.
"""

from __future__ import annotations

from datetime import date, datetime  # noqa: TC003 - Pydantic needs these at runtime.

from pydantic import Field, model_validator

from gridflow.schemas.common import BaseSchema, SettlementPeriodMixin, TimestampMixin
from gridflow.utils.time import settlement_periods_in_day


def is_valid_settlement_period(settlement_date: date, settlement_period: int) -> bool:
    """Does ``settlement_period`` exist on ``settlement_date``? (D-27)

    **One predicate, two callers.** The schema validator on
    :class:`NesoEmbeddedWindSolarForecast` calls this, and so does that
    dataset's transformer filter; neither restates the comparison. Two
    hand-written copies of a two-sided bound is how one of them ends up
    upper-only again, and an upper-only filter beside a two-sided validator
    would let SP0 through to a wrong-day ``event_time`` while the schema called
    it invalid.

    The bound is **two-sided**, and both sides are load-bearing because
    ``settlement_period_to_utc`` bound-checks nothing:

    * **upper** — SP49 on an ordinary 48-period day, or SP47 on a 46-period
      spring day, lands in the *next* settlement day;
    * **lower** — SP0 lands in the *previous* one (measured:
      ``settlement_period_to_utc(2026-08-16, 0)`` is ``2026-08-15 22:30Z``,
      half an hour before that day's SP1), and a negative period walks further
      back still.

    The day's length comes from :func:`~gridflow.utils.time.settlement_periods_in_day`,
    which derives 46/48/50 from the DST machinery rather than a hardcoded table.

    **Scope: NESO-local, deliberately.** The shared
    :class:`~gridflow.schemas.common.SettlementPeriodMixin` declares no bound at
    all — not an insufficient one, none — and adding one there would change
    validation for every settlement-based schema in the repo. That needs its own
    blast-radius review (TODO-12) and is not smuggled in here.

    Args:
        settlement_date: The UK settlement date the period is stated against.
        settlement_period: The vendor's settlement period, unmodified.

    Returns:
        ``True`` when ``1 <= settlement_period <=
        settlement_periods_in_day(settlement_date)``.
    """
    return 1 <= settlement_period <= settlement_periods_in_day(settlement_date)


class _NesoDataPortalBase(BaseSchema):
    """Common provider stamp for every NESO Data Portal silver contract.

    Mirrors the ``schemas/neso.py`` ``_NesoBase`` convention rather than
    inventing a second one: one private base per source, carrying the
    ``data_provider`` default and nothing else.
    """

    data_provider: str = Field(default="neso_data_portal")


class NesoDailyWindAvailability(_NesoDataPortalBase, TimestampMixin):
    """Silver schema for ``daily_wind_availability`` (D-24).

    ``timestamp_utc`` comes from :class:`~gridflow.schemas.common.TimestampMixin`
    — the shared tz-aware enforcement, reused rather than re-implemented as a
    fourth copy of the same validator. It is a DERIVED instant (D-25:
    ``settlement_period_to_utc(availability_date, 1)``, the start of that GB
    availability day in UTC); ``availability_date`` is the authoritative
    user-facing field and the ADR-024 designated date column.

    ``published_at`` is REQUIRED, not nullable, and that is a real claim rather
    than an oversight: the provenance reader (D-23) returns an empty frame when
    the bronze sidecar cannot supply ``ckan_last_modified``, so a row that
    reaches this contract without a vendor publication instant cannot exist.
    Declaring it optional would quietly accept the fabricated-vintage case
    D-23 exists to refuse.
    """

    bmu_id: str
    availability_date: date
    availability_mw: float
    published_at: datetime


class NesoHistoricGenerationMix(_NesoDataPortalBase, TimestampMixin):
    """Silver schema for ``historic_generation_mix`` (D-24).

    ``timestamp_utc`` is the vendor's own ``DATETIME``, read as UTC. That
    reading is DOCUMENTED rather than inferred: the UTC statement lives only in
    the ``datastore_search`` field metadata
    (``_probe/datastore_historic-generation-mix.json``,
    ``DATETIME.info.description``), which a plain CSV download never exposes.
    The transformer refuses any ``DATETIME`` that carries an offset instead of
    re-reading it under the same assumption.

    Every fuel/metric column is a plain ``float`` and every ``_pct`` column is
    the vendor's OWN published percentage, carried rather than recomputed: a
    recomputation would disagree with NESO's own figures at whatever rounding
    NESO applied, and the vendor's number is the one a consumer can reconcile
    against the portal.

    ``published_at`` is REQUIRED for the same reason it is on
    :class:`NesoDailyWindAvailability`: D-23 refuses to transform a body whose
    vintage cannot be established, so a row without one cannot exist.
    """

    gas: float
    coal: float
    nuclear: float
    wind: float
    wind_emb: float
    hydro: float
    imports: float
    biomass: float
    other: float
    solar: float
    storage: float
    generation: float
    carbon_intensity: float
    low_carbon: float
    zero_carbon: float
    renewable: float
    fossil: float
    gas_pct: float
    coal_pct: float
    nuclear_pct: float
    wind_pct: float
    wind_emb_pct: float
    hydro_pct: float
    imports_pct: float
    biomass_pct: float
    other_pct: float
    solar_pct: float
    storage_pct: float
    generation_pct: float
    low_carbon_pct: float
    zero_carbon_pct: float
    renewable_pct: float
    fossil_pct: float
    published_at: datetime


class NesoEmbeddedWindSolarForecast(_NesoDataPortalBase, SettlementPeriodMixin):
    """Silver schema for ``embedded_wind_solar_forecast`` (D-24).

    **No ``timestamp_utc``** — and its absence is load-bearing, not tidiness
    (D-26). ``BaseSilverTransformer._event_time_expr`` PREFERS a
    ``timestamp_utc`` column over the settlement pair, and only the pair branch
    calls the DST-fold-safe ``settlement_period_to_utc``. Emitting an instant
    here would silently take ``event_time`` off the safe path on exactly the 46-
    and 50-period days where it matters.

    ``time_gmt_raw`` carries the vendor's ``TIME_GMT`` **unparsed**: its
    start-vs-end convention is undocumented by NESO, so no code path may depend
    on it. ``DATE_GMT``, its calendar half, is not emitted at all — the
    authoritative pair is ``settlement_date``/``settlement_period``, and bronze
    retains the bytes, so the decision is reversible by re-transform.

    ``issue_time`` is REQUIRED and is part of the entity key: it comes from the
    12-digit token in the vendor's own resource filename (D-15/D-23), and a body
    whose filename carries no token is declined rather than stamped from the
    fetch clock (FM-05).

    The ``model_validator`` below is D-27's schema half. It is the ONLY place
    the settlement-period constraint is stated for this dataset, because the
    shared :class:`~gridflow.schemas.common.SettlementPeriodMixin` declares
    none. See :func:`is_valid_settlement_period` for why the bound is two-sided
    and why the transformer enforces the same predicate rather than trusting
    this validator alone (``_validate_against_schema`` is documented fail-soft:
    it counts an invalid row and still writes it).
    """

    issue_time: datetime
    time_gmt_raw: str
    embedded_wind_forecast: float
    embedded_wind_capacity: float
    embedded_solar_forecast: float
    embedded_solar_capacity: float
    published_at: datetime

    @model_validator(mode="after")
    def settlement_period_must_exist_on_its_date(self) -> NesoEmbeddedWindSolarForecast:
        """Reject a settlement period that does not exist on its own date.

        Returns:
            The validated model.

        Raises:
            ValueError: The period is outside ``1..settlement_periods_in_day``.
                The message names the period AND the day's real length, because
                a bound violation a reader cannot act on is barely better than
                none — 49 is wrong on a 48-period day and right on a 50-period
                one.
        """
        if not is_valid_settlement_period(self.settlement_date, self.settlement_period):
            raise ValueError(
                f"settlement_period {self.settlement_period} does not exist on "
                f"{self.settlement_date.isoformat()}, which has "
                f"{settlement_periods_in_day(self.settlement_date)} settlement periods"
            )
        return self
