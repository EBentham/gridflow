"""The point-in-time serving contracts of the NESO forecast-versus-outturn gold views (ADR-041).

Each :class:`GoldViewContract` names one gold relation, its NESO input families,
its designated date column and the :class:`~gridflow.silver.latest_views.LatestViewSpec`
that :data:`POINT_IN_TIME_SELECTOR` applies over the view's all-vintage rows
(RULINGS 466: the view is static, the as-of bound is a query parameter of the
selector, never a view or a macro).

**Held representation (ADR-041): unregistered until eligible.** A contract whose
:func:`hold_reasons` is non-empty keeps its SQL under :data:`HELD_DIR`, which the
default gold registration never globs, and has no serving-alias row, so no
consumer path presents it. The hold reasons are read from the registry at call
time (the inputs' ``Held`` questions verbatim) plus the contract's own pairing
hold, so the hold text lives in exactly one place each.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import Held
from gridflow.silver.latest_views import LatestViewSpec
from gridflow.silver.neso_data_portal import generic

if TYPE_CHECKING:
    from gridflow.connectors.neso_data_portal.registry import Registry
    from gridflow.silver.date_columns import DateColSqlType

__all__ = [
    "GOLD_VIEW_CONTRACTS",
    "HELD_DIR",
    "POINT_IN_TIME_SELECTOR",
    "VIEWS_DIR",
    "GoldViewContract",
    "HoldReason",
    "contract_for",
    "hold_reasons",
    "is_published",
    "sql_path",
]

POINT_IN_TIME_SELECTOR: Final = "gridflow.silver.latest_views.select_latest_vintage"
"""The one point-in-time path over every G view (G3, RULINGS 466)."""

VIEWS_DIR: Final = Path(__file__).resolve().parent / "views"
"""The directory the default gold registration executes (top level only)."""

HELD_DIR: Final = VIEWS_DIR / "held"
"""Where a held view's SQL lives: never globbed, so never registered by default."""


@dataclass(frozen=True)
class HoldReason:
    """One reason a gold view is unpublished.

    Attributes:
        subject: The input family key whose output is held, or ``"pairing"``
            for a hold on the forecast/outturn comparison itself.
        question: The open question, verbatim.
        unit: The research unit that owns the question.
    """

    subject: str
    question: str
    unit: str


@dataclass(frozen=True)
class GoldViewContract:
    """The serving contract of one NESO forecast-versus-outturn gold view.

    Attributes:
        relation_name: The DuckDB relation the view's SQL creates.
        inputs: The NESO input family keys, in contract order.
        designated_date_col: The column to use for date-range filtering.
        date_col_sql_type: The SQL type family of ``designated_date_col``.
        point_in_time: The ``key_latest`` spec the selector applies over the
            view's all-vintage rows.
        pairing_hold: A hold on the comparison itself, when research has not
            shown the two sides are like with like.
    """

    relation_name: str
    inputs: tuple[str, ...]
    designated_date_col: str
    date_col_sql_type: DateColSqlType
    point_in_time: LatestViewSpec
    pairing_hold: HoldReason | None = None

    @property
    def sql_stem(self) -> str:
        """The SQL file stem: the relation name without its ``gold_`` prefix."""
        return self.relation_name.removeprefix("gold_")


# RESEARCH (b)'s fleet TODO, verbatim (bold markers removed).
_Q_FLEET = (
    "TODO: Obtain a primary NESO definition equating the monthly output’s contributing fleet "
    "with the national day-ahead forecast fleet, including licence-exempt sites and "
    "commissioning/decommissioning changes. The published monthly aggregates contain no "
    "membership identifiers with which to prove that equivalence locally."
)

_IC_INPUTS: tuple[str, ...] = (
    "eleclink",
    "ifa_itl",
    "ifa2_ifa_itl",
    "nemolink_ntc",
    "nsl",
    "viking_link_ntc",
    "brit_ned",
)

GOLD_VIEW_CONTRACTS: Final[tuple[GoldViewContract, ...]] = (
    GoldViewContract(
        relation_name="gold_gb_wind_forecast_vs_outturn",
        inputs=("da_wind_forecast_day_ahead", "metered_wind_output_monthly"),
        designated_date_col="settlement_date",
        date_col_sql_type="DATE",
        point_in_time=LatestViewSpec(
            key_columns=("timestamp_utc",),
            order_columns=("available_at",),
            tiebreak_columns=generic._TIEBREAK,
            mode="key_latest",
        ),
        pairing_hold=HoldReason("pairing", _Q_FLEET, "G-R"),
    ),
    GoldViewContract(
        relation_name="gold_gb_interconnector_limits",
        inputs=_IC_INPUTS,
        designated_date_col="operational_period_start_gmt",
        date_col_sql_type="TIMESTAMPTZ",
        point_in_time=LatestViewSpec(
            key_columns=(
                "family",
                "resource_id",
                "auction_type",
                "operational_period_start_gmt",
                "operational_date",
                "hourly_time_period",
                "operational_date_and_hour",
            ),
            order_columns=("issue_time", "available_at"),
            tiebreak_columns=generic._TIEBREAK,
            mode="key_latest",
        ),
    ),
)


def hold_reasons(
    contract: GoldViewContract, registry: Registry | None = None
) -> tuple[HoldReason, ...]:
    """Return why ``contract``'s view is unpublished; empty when it may publish.

    Args:
        contract: The view's contract.
        registry: The registry to judge the inputs by; ``None`` loads it
            through ``registry.load_registry`` at call time (the test seam).

    Returns:
        One reason per held input, in input order, with the registry's
        question and unit verbatim; then the pairing hold, when set.

    Raises:
        KeyError: An input is not a registry family.
        ValueError: An input has no silver record (an ingest-only family
            would otherwise read as eligible).
    """
    from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility

    loaded = registry if registry is not None else registry_module.load_registry()
    reasons: list[HoldReason] = []
    for key in contract.inputs:
        package, family = loaded.families[key]
        if family.record is None:
            raise ValueError(f"{key} has no silver record")
        eligibility = effective_eligibility(package, family)
        if isinstance(eligibility, Held):
            reasons.append(HoldReason(key, eligibility.question, eligibility.unit))
    if contract.pairing_hold is not None:
        reasons.append(contract.pairing_hold)
    return tuple(reasons)


def is_published(contract: GoldViewContract, registry: Registry | None = None) -> bool:
    """Whether every input is eligible and no pairing hold applies.

    Args:
        contract: The view's contract.
        registry: As :func:`hold_reasons`.

    Returns:
        ``True`` exactly when :func:`hold_reasons` is empty.
    """
    return not hold_reasons(contract, registry)


def sql_path(contract: GoldViewContract, registry: Registry | None = None) -> Path:
    """Where the view's SQL must live: :data:`VIEWS_DIR` if published, else :data:`HELD_DIR`.

    Args:
        contract: The view's contract.
        registry: As :func:`hold_reasons`.

    Returns:
        The SQL file path for the view's current publication state.
    """
    directory = VIEWS_DIR if is_published(contract, registry) else HELD_DIR
    return directory / f"{contract.sql_stem}.sql"


def contract_for(relation_name: str) -> GoldViewContract:
    """Return the contract of ``relation_name``.

    Args:
        relation_name: A gold relation name.

    Returns:
        Its contract.

    Raises:
        KeyError: No contract names the relation.
    """
    for contract in GOLD_VIEW_CONTRACTS:
        if contract.relation_name == relation_name:
            return contract
    known = ", ".join(contract.relation_name for contract in GOLD_VIEW_CONTRACTS)
    raise KeyError(f"no gold view contract for {relation_name!r}; known: {known}")
