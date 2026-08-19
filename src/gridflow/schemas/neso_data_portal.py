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

from pydantic import Field

from gridflow.schemas.common import BaseSchema, TimestampMixin


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
