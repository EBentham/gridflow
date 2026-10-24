"""Rows observed before their target start (ADR-044): policy file, selectors, report.

The core fixture is one family over five captures on two resources: an incomplete
capture ``c0`` (row count disagrees with the ledger), three complete captures
``c1``..``c3`` on resource ``R1``, and ``c2b`` on ``R2`` at the same instant as
``c2``. Each lettered row exercises one case: append, in-place revision,
deletion, a revision that reverts (A->B->A), a target equal to first sight, null
values, an incomplete capture, and the resource as part of a row version.
Lineage columns differ on every capture, so a version split by lineage, or a
lineage leaked from the wrong capture, shows up.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from _neso_registry_support import column, epoch, family, package, record, resource
from _neso_registry_support import write_registry as _write_registry

from gridflow.connectors.neso_data_portal import eligibility, skeleton
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import (
    Held,
    ObservedBeforeTarget,
    RegistryError,
    load_observed_before_target,
    observed_before_target_problems,
)
from gridflow.connectors.neso_data_portal.registry.record import RESERVED
from gridflow.silver.neso_data_portal.completion import COMPLETION_SCHEMA
from gridflow.silver.neso_data_portal.first_seen import (
    FIRST_SEEN_AT,
    OBSERVED_BEFORE_TARGET,
    NotObservedBeforeTargetError,
    observed_archive,
    observed_snapshot,
    version_columns,
    with_first_seen,
)

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.registry import Registry

TS = pl.Datetime("us", "UTC")
R1 = "res-1"
R2 = "res-2"
T0 = datetime(2026, 10, 7, 0, 0, tzinfo=UTC)
T1 = datetime(2026, 10, 8, 10, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 12, 10, 0, tzinfo=UTC)
T3 = datetime(2026, 10, 16, 10, 0, tzinfo=UTC)
T4 = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
FUTURE = datetime(2026, 11, 1, tzinfo=UTC)
T_C = datetime(2026, 10, 10, tzinfo=UTC)
T_E = datetime(2026, 10, 14, tzinfo=UTC)
T_I = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)

CAPTURES: dict[str, tuple[datetime, str]] = {
    "c0": (T0, R1),
    "c0z": (T1, R1),
    "c1": (T1, R1),
    "c2": (T2, R1),
    "c3": (T3, R1),
    "c2b": (T2, R2),
}

# (row id, k, v, target, captures)
ROWS: tuple[tuple[str, str, float | None, datetime, tuple[str, ...]], ...] = (
    ("A", "A", 1.0, FUTURE, ("c1", "c2", "c3")),
    ("B", "B", 1.0, FUTURE, ("c2", "c3")),
    ("C1", "C", 1.0, T_C, ("c1",)),
    ("C2", "C", 2.0, T_C, ("c2", "c3")),
    ("D", "D", 1.0, FUTURE, ("c1",)),
    ("E1", "E", 1.0, T_E, ("c1", "c3")),
    ("E2", "E", 2.0, T_E, ("c2",)),
    ("F", "F", 1.0, T1, ("c1",)),
    ("G", "G", None, FUTURE, ("c1", "c2")),
    ("N", "N", None, FUTURE, ("c3",)),
    ("H", "H", 1.0, FUTURE, ("c0",)),
    ("I", "I", 1.0, T_I, ("c0", "c2")),
    ("J", "J", 1.0, FUTURE, ("c0", "c1")),
    ("K", "K", 1.0, FUTURE, ("c0z", "c1")),
    ("AR2", "A", 1.0, FUTURE, ("c2b",)),
)

EXPECTED_FIRST_SEEN: dict[str, tuple[datetime | None, bool]] = {
    "A": (T1, True),
    "B": (T2, True),
    "C1": (T1, True),
    "C2": (T2, False),
    "D": (T1, True),
    "E1": (T1, True),
    "E2": (T2, True),
    "F": (T1, False),
    "G": (T1, True),
    "N": (T3, True),
    "H": (None, False),
    "I": (T2, False),
    "J": (T1, True),
    "K": (T1, True),
    "AR2": (T2, True),
}

ROW_OF: dict[tuple[str, float | None, str], str] = {
    (k, v, capture): row_id for row_id, k, v, _target, captures in ROWS for capture in captures
}
"""(k, v, capture) -> row id; a row id column on the base would itself be a version column."""

INCOMPLETE = frozenset({"c0", "c0z"})
"""Captures whose ledger row count disagrees with their rows; ``c0z`` ties ``c1``'s instant."""

FAMILY = "obs_series"
PARTED = "obs_parted"
UNLISTED = "obs_unlisted"


def _row_id(row: dict[str, Any]) -> str:
    return ROW_OF[(row["k"], row["v"], row["bronze_capture_id"])]


def _base(captures: tuple[str, ...] = tuple(CAPTURES)) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, k, v, target, held_by in ROWS:
        for capture in held_by:
            if capture not in captures:
                continue
            written, _resource = CAPTURES[capture]
            rows.append(
                {
                    "k": k,
                    "v": v,
                    "timestamp_utc": target,
                    "bronze_capture_id": capture,
                    "capture_written_at": written,
                    "available_at": written,
                    "published_at": written - timedelta(hours=1),
                    "dataset_version": f"dv-{capture}",
                    "source_run_id": f"run-{capture}",
                }
            )
    schema = {
        "k": pl.Utf8,
        "v": pl.Float64,
        "timestamp_utc": TS,
        "bronze_capture_id": pl.Utf8,
        "capture_written_at": TS,
        "available_at": TS,
        "published_at": TS,
        "dataset_version": pl.Utf8,
        "source_run_id": pl.Utf8,
    }
    return pl.DataFrame(rows, schema=schema)


def _completion(
    family_key: str,
    capture: str,
    written: datetime,
    resource_id: str,
    row_count: int,
    outcome: str = "populated",
) -> dict[str, Any]:
    return {
        "family": family_key,
        "bronze_capture_id": capture,
        "source_key": "neso_data_portal",
        "partition_date": written.date(),
        "resource_id": resource_id,
        "body_sha256": "0" * 64,
        "capture_written_at": written,
        "published_at": None,
        "available_at": written,
        "outcome": outcome,
        "row_count": row_count,
        "rows_excluded": 0,
        "output_path": f"silver/{capture}.parquet",
        "children": [],
        "record_version": "1",
        "engine_version": "1",
    }


def _completions(
    base: pl.DataFrame, family_key: str = FAMILY, *, valid_empty_at: datetime | None = None
) -> pl.DataFrame:
    counts = dict(base.group_by("bronze_capture_id").len().iter_rows())
    rows = []
    for capture in counts:
        written, resource_id = CAPTURES[capture]
        n = counts[capture] + (5 if capture in INCOMPLETE else 0)
        rows.append(_completion(family_key, capture, written, resource_id, n))
    if valid_empty_at is not None:
        rows.append(_completion(family_key, "c4", valid_empty_at, R1, 0, "valid_empty"))
    rows.append(_completion("other_family", "c1", T1, R2, 999))
    return pl.DataFrame(rows, schema=COMPLETION_SCHEMA)


def _ids(frame: pl.DataFrame) -> set[tuple[str, str]]:
    return {(_row_id(row), row["bronze_capture_id"]) for row in frame.iter_rows(named=True)}


# --------------------------------------------------------------------------- #
# A synthetic registry carrying the policy
# --------------------------------------------------------------------------- #

PKG = "eeeeeeee-0000-4000-8000-000000000000"
HOLD = {"status": "held", "question": "Q", "unit": "U"}


def _entry(key: str) -> dict[str, str]:
    return {"family": key, "rationale": "held only on availability", "evidence": "ADR-044"}


def _rid(n: int) -> str:
    return f"eeeeeeee-0000-4000-8000-{n:012d}"


def _parted_record() -> dict[str, Any]:
    rec = record(
        latest="whole_capture",
        entity_key=("settlement_date", "settlement_period", "unit", "resource_id"),
    )
    return dict(rec, latest_partition="resource_id", eligibility=HOLD)


def _policy_registry(
    tmp_path: Path,
    *,
    extra: list[tuple[str, dict[str, Any]]] | None = None,
    listed: list[str] | None = None,
    package_hold: bool = False,
) -> Path:
    families = [
        (FAMILY, dict(record(), eligibility=HOLD)),
        (PARTED, _parted_record()),
        (UNLISTED, dict(record(), eligibility=HOLD)),
        *(extra or []),
    ]
    document = package(
        "pkg-observed",
        PKG,
        [family(key, record=rec) for key, rec in families],
        [resource(_rid(i + 1), f"R{i}", key) for i, (key, _rec) in enumerate(families)],
    )
    if package_hold:
        document["eligibility"] = HOLD
    entries = [_entry(key) for key in (listed if listed is not None else [FAMILY, PARTED])]
    return _write_registry(tmp_path / "registry", [document], observed_before_target=entries)


@pytest.fixture
def policy(tmp_path: Path) -> Registry:
    return registry_module.load_registry(_policy_registry(tmp_path))


def _snapshot(
    registry: Registry,
    as_of: datetime,
    *,
    captures: tuple[str, ...] = ("c0", "c0z", "c1", "c2", "c3"),
    family_key: str = FAMILY,
    valid_empty_at: datetime | None = None,
) -> pl.DataFrame:
    base = _base(captures)
    completions = _completions(base, family_key, valid_empty_at=valid_empty_at)
    return observed_snapshot(
        family_key, base.lazy(), completions.lazy(), as_of, registry=registry
    ).collect()


def _archive(
    registry: Registry,
    as_of: datetime | None = None,
    *,
    captures: tuple[str, ...] = tuple(CAPTURES),
) -> pl.DataFrame:
    base = _base(captures)
    return observed_archive(
        FAMILY, base.lazy(), _completions(base).lazy(), as_of, registry=registry
    ).collect()


# --------------------------------------------------------------------------- #
# with_first_seen: the annotation
# --------------------------------------------------------------------------- #


def _annotated() -> pl.DataFrame:
    base = _base()
    out = with_first_seen(base.lazy(), _completions(base).lazy(), family=FAMILY).collect()
    identity = ["k", "v", "bronze_capture_id"]
    assert out.select(identity).equals(base.select(identity))
    return out


class TestWithFirstSeen:
    def test_every_row_keeps_its_place_and_gains_two_typed_columns(self) -> None:
        """Detects rows dropped or duplicated (a ledger join fanning out on another family's
        completion with the same capture id) or a non-UTC ``first_seen_at``."""
        out = _annotated()
        assert out.height == _base().height
        assert out.schema[FIRST_SEEN_AT] == TS
        assert out.schema[OBSERVED_BEFORE_TARGET] == pl.Boolean
        assert set(out.columns) - set(_base().columns) == {FIRST_SEEN_AT, OBSERVED_BEFORE_TARGET}

    @pytest.mark.parametrize("row_id", sorted(EXPECTED_FIRST_SEEN))
    def test_first_seen_and_observed_per_case(self, row_id: str) -> None:
        """Detects, per case: A lineage splitting a version (its c2/c3 rows would restart);
        B an append dated earlier than its capture; C1/C2 a revision inheriting the old
        version's first sight; D a deletion losing its row; E1 a reverted value re-dated to
        its return; F a target equal to first sight counted as observed before it (``>=``);
        G/N nulls merged or split; H/I/J/K an incomplete capture counted as a sighting; AR2
        the resource left out of the version."""
        seen = {
            (row[FIRST_SEEN_AT], row[OBSERVED_BEFORE_TARGET])
            for row in _annotated().iter_rows(named=True)
            if _row_id(row) == row_id
        }
        assert seen == {EXPECTED_FIRST_SEEN[row_id]}

    def test_version_columns_are_everything_but_the_reserved_set(self) -> None:
        """Detects a hand-copied lineage list drifting from the record ``RESERVED`` set."""
        assert version_columns(_base().columns) == ("k", "v")
        assert version_columns(sorted(RESERVED)) == ()

    def test_a_base_already_carrying_an_output_name_is_refused(self) -> None:
        """Detects a silver column silently overwritten by the derived one."""
        base = _base()
        clashing = base.with_columns(pl.lit(True).alias(OBSERVED_BEFORE_TARGET)).lazy()
        with pytest.raises(ValueError, match="already carries"):
            with_first_seen(clashing, _completions(base).lazy(), family=FAMILY)

    def test_a_naive_capture_clock_is_refused(self) -> None:
        """Detects a comparison of a naive capture clock against a UTC target."""
        base = _base()
        naive = base.with_columns(pl.col("capture_written_at").dt.replace_time_zone(None))
        with pytest.raises(ValueError, match="UTC datetime"):
            with_first_seen(naive.lazy(), _completions(base).lazy(), family=FAMILY)


# --------------------------------------------------------------------------- #
# observed_snapshot
# --------------------------------------------------------------------------- #

AT_C1 = {
    ("A", "c1"),
    ("C1", "c1"),
    ("D", "c1"),
    ("E1", "c1"),
    ("G", "c1"),
    ("J", "c1"),
    ("K", "c1"),
}
AT_C2 = {("A", "c2"), ("B", "c2"), ("E2", "c2"), ("G", "c2")}
AT_C3 = {("A", "c3"), ("B", "c3"), ("E1", "c3"), ("N", "c3")}


class TestObservedSnapshot:
    @pytest.mark.parametrize(
        ("as_of", "expected"),
        [
            pytest.param(T1 - timedelta(microseconds=1), set(), id="only-the-incomplete-c0"),
            pytest.param(T1, AT_C1, id="at-c1"),
            pytest.param(T1 + timedelta(days=1), AT_C1, id="between-c1-and-c2"),
            pytest.param(T2 + timedelta(days=1), AT_C2, id="between-c2-and-c3"),
            pytest.param(T3 + timedelta(days=1), AT_C3, id="after-c3"),
        ],
    )
    def test_the_newest_complete_capture_then_its_observed_rows(
        self, policy: Registry, as_of: datetime, expected: set[tuple[str, str]]
    ) -> None:
        """Detects rows of more than one capture (vintages mixed), an incomplete capture
        chosen or served (c0, J's c0 row), a strict capture bound, a deleted row (D) or a
        past-at-first-sight version (C2, I) surviving, and a target equal to first sight
        (F) served."""
        assert _ids(_snapshot(policy, as_of)) == expected

    def test_a_later_capture_does_not_change_an_earlier_read(self, policy: Registry) -> None:
        """Detects a snapshot that reads anything written after ``as_of``."""
        as_of = T2 + timedelta(days=1)
        before = _snapshot(policy, as_of, captures=("c0", "c0z", "c1", "c2"))
        after = _snapshot(policy, as_of)
        assert before.sort("k", "v").equals(after.sort("k", "v"))

    def test_a_valid_empty_newest_capture_clears_the_snapshot(self, policy: Registry) -> None:
        """Detects a valid-empty capture skipped, resurrecting the previous capture's rows."""
        assert _snapshot(policy, T4, valid_empty_at=T4).height == 0
        assert _ids(_snapshot(policy, T4 - timedelta(seconds=1), valid_empty_at=T4)) == AT_C3

    def test_a_reverted_value_keeps_first_sight_but_takes_the_selected_lineage(
        self, policy: Registry
    ) -> None:
        """Detects, for A->B->A, first sight re-dated to the return, or lineage (capture id,
        clocks, dataset version) taken from the first capture instead of the selected one."""
        out = _snapshot(policy, T3 + timedelta(days=1))
        (row,) = (r for r in out.iter_rows(named=True) if _row_id(r) == "E1")
        assert row[FIRST_SEEN_AT] == T1
        assert (row["bronze_capture_id"], row["capture_written_at"]) == ("c3", T3)
        assert (row["dataset_version"], row["available_at"]) == ("dv-c3", T3)

    def test_a_partitioned_record_takes_one_capture_per_resource(self, policy: Registry) -> None:
        """Detects a per-resource record collapsed to one capture (R2's c2b would win the tie
        and hide R1), or an unpartitioned record split per resource."""
        as_of = T2 + timedelta(days=1)
        every = (*CAPTURES,)
        parted = _snapshot(policy, as_of, captures=every, family_key=PARTED)
        assert _ids(parted) == AT_C2 | {("AR2", "c2b")}
        whole = _snapshot(policy, as_of, captures=every)
        assert _ids(whole) == {("AR2", "c2b")}

    def test_an_as_of_in_another_zone_is_the_same_instant(self, policy: Registry) -> None:
        """Detects an as-of compared by wall clock instead of by instant."""
        london = (T2 + timedelta(days=1)).astimezone(ZoneInfo("Europe/London"))
        assert _ids(_snapshot(policy, london)) == AT_C2

    def test_a_naive_as_of_is_refused(self, policy: Registry) -> None:
        """Detects a naive as-of being read in some local zone."""
        with pytest.raises(ValueError, match="timezone-aware"):
            _snapshot(policy, datetime(2026, 10, 13))  # noqa: DTZ001


# --------------------------------------------------------------------------- #
# observed_archive
# --------------------------------------------------------------------------- #

ARCHIVE_FIRSTS = {
    ("A", "c1"),
    ("B", "c2"),
    ("C1", "c1"),
    ("D", "c1"),
    ("E1", "c1"),
    ("E2", "c2"),
    ("G", "c1"),
    ("N", "c3"),
    ("J", "c1"),
    ("K", "c1"),
    ("AR2", "c2b"),
}


class TestObservedArchive:
    def test_one_row_per_observed_version_from_its_first_capture(self, policy: Registry) -> None:
        """Detects a version repeated per vintage, a representative taken from a later
        capture, a past-at-first-sight version (C2, F, I) kept, or an incomplete capture's
        row (H, J at c0, K at c0z, which ties c1's instant) served."""
        out = _archive(policy)
        assert _ids(out) == ARCHIVE_FIRSTS
        assert out.height == len(ARCHIVE_FIRSTS)

    def test_lineage_is_the_first_captures(self, policy: Registry) -> None:
        """Detects any lineage column from a later capture leaking onto the representative."""
        for row in _archive(policy).iter_rows(named=True):
            capture = row["bronze_capture_id"]
            assert row["capture_written_at"] == row[FIRST_SEEN_AT] == CAPTURES[capture][0]
            assert (row["dataset_version"], row["source_run_id"]) == (
                f"dv-{capture}",
                f"run-{capture}",
            )

    @pytest.mark.parametrize(
        ("as_of", "expected"),
        [
            pytest.param(T1 - timedelta(microseconds=1), set(), id="before-c1"),
            pytest.param(T1, {r for r in ARCHIVE_FIRSTS if r[1] == "c1"}, id="at-c1"),
            pytest.param(
                T2 + timedelta(days=1),
                {r for r in ARCHIVE_FIRSTS if r[1] != "c3"},
                id="between-c2-and-c3",
            ),
        ],
    )
    def test_as_of_bounds_first_sight(
        self, policy: Registry, as_of: datetime, expected: set[tuple[str, str]]
    ) -> None:
        """Detects a version served before gridflow first saw it, or a strict bound that
        loses the capture instant itself."""
        out = _archive(policy, as_of)
        assert _ids(out) == expected
        assert all(t <= as_of for t in out[FIRST_SEEN_AT])

    def test_a_later_capture_does_not_change_an_earlier_read(self, policy: Registry) -> None:
        """Detects an archive that reads anything written after ``as_of``."""
        as_of = T2 + timedelta(days=1)
        before = _archive(policy, as_of, captures=("c0", "c0z", "c1", "c2", "c2b"))
        after = _archive(policy, as_of)
        assert before.sort("k", "v", "bronze_capture_id").equals(
            after.sort("k", "v", "bronze_capture_id")
        )


# --------------------------------------------------------------------------- #
# Opt-in enforcement
# --------------------------------------------------------------------------- #

LIVE_REFUSED = {
    "upcoming_trades": "class B (unit question)",
    "regional_ci_forecast": "class B, temporal none",
    "dc_forecast_4d_history": "EFA label anchor",
    "ic_requirement_auction": "requirements mixed with outcomes",
    "eac_br_mock_results_summary": "package held",
}


class TestOptIn:
    def test_an_unlisted_family_is_refused_by_both_selectors(self, policy: Registry) -> None:
        """Detects a selector serving a held family that never opted in."""
        base = _base()
        lf, comp = base.lazy(), _completions(base, UNLISTED).lazy()
        with pytest.raises(NotObservedBeforeTargetError):
            observed_snapshot(UNLISTED, lf, comp, T4, registry=policy)
        with pytest.raises(NotObservedBeforeTargetError):
            observed_archive(UNLISTED, lf, comp, registry=policy)

    @pytest.mark.parametrize("key", sorted(LIVE_REFUSED))
    def test_committed_families_off_the_policy_are_refused(self, key: str) -> None:
        """Detects the committed policy admitting a class-B, temporal-none, EFA-anchored,
        requirement/outcome or package-held family."""
        base = _base()
        lf, comp = base.lazy(), _completions(base, key).lazy()
        with pytest.raises(NotObservedBeforeTargetError):
            observed_snapshot(key, lf, comp, T4)
        with pytest.raises(NotObservedBeforeTargetError):
            observed_archive(key, lf, comp)


# --------------------------------------------------------------------------- #
# The policy file's load rules
# --------------------------------------------------------------------------- #


def _anchored(date_column: str) -> dict[str, Any]:
    columns = [
        column("Day", date_column, "date", nullable=False),
        column("Unit", "unit", nullable=False),
        column("Value", "value", "float64"),
    ]
    rec = record(
        epochs=[epoch(columns)],
        temporal={"kind": "date_sp1", "date_column": date_column},
        entity_key=(date_column, "unit"),
    )
    return dict(rec, eligibility=HOLD)


class TestPolicyLoad:
    def test_the_synthetic_policy_loads(self, tmp_path: Path) -> None:
        """Positive control for the refusals below; detects a rule refusing a valid entry."""
        directory = _policy_registry(tmp_path)
        entries = load_observed_before_target(directory)
        assert [entry.family for entry in entries] == [FAMILY, PARTED]

    @pytest.mark.parametrize(
        ("rec", "message"),
        [
            pytest.param(record(), "the record is not held", id="eligible-record"),
            pytest.param(
                dict(record(temporal={"kind": "none"}), eligibility=HOLD),
                "temporal none",
                id="temporal-none",
            ),
            pytest.param(_anchored("forecast_target_date"), "label anchor", id="efa-label-anchor"),
        ],
    )
    def test_an_inadmissible_family_is_refused(
        self, tmp_path: Path, rec: dict[str, Any], message: str
    ) -> None:
        """Detects the policy admitting an eligible record (nothing to lift), a record whose
        target is its capture time, or a date label whose first block starts before SP1."""
        directory = _policy_registry(tmp_path, extra=[("obs_bad", rec)], listed=["obs_bad"])
        with pytest.raises(RegistryError, match=message):
            load_observed_before_target(directory)

    def test_a_date_sp1_start_anchor_is_admitted(self, tmp_path: Path) -> None:
        """Control for the label-anchor refusal: detects every ``date_sp1`` being refused."""
        rec = _anchored("fixed_tariff_start_date")
        directory = _policy_registry(tmp_path, extra=[("obs_day", rec)], listed=["obs_day"])
        assert [entry.family for entry in load_observed_before_target(directory)] == ["obs_day"]

    def test_a_held_package_is_refused(self, tmp_path: Path) -> None:
        """Detects the policy reaching into a package whose hold covers every output."""
        directory = _policy_registry(tmp_path, package_hold=True)
        with pytest.raises(RegistryError, match="is held"):
            load_observed_before_target(directory)

    def test_an_unknown_family_and_a_repeat_are_refused(self, tmp_path: Path) -> None:
        """Detects a typo'd key loading silently, or one family listed twice."""
        with pytest.raises(RegistryError, match="has no record"):
            load_observed_before_target(_policy_registry(tmp_path / "a", listed=["obs_typo"]))
        with pytest.raises(RegistryError, match="repeats"):
            load_observed_before_target(_policy_registry(tmp_path / "b", listed=[FAMILY, FAMILY]))

    def test_a_missing_file_is_never_read_as_empty(self, tmp_path: Path) -> None:
        """Detects an absent policy file loading as an empty list (silent loss of the rule)."""
        directory = _policy_registry(tmp_path)
        (directory / registry_module.OBSERVED_BEFORE_TARGET_FILE).unlink()
        with pytest.raises(RegistryError):
            load_observed_before_target(directory)

    def test_rationale_and_evidence_are_one_line(self) -> None:
        """Detects a multi-line rationale breaking the one-line report and ledger form."""
        with pytest.raises(ValueError, match="one non-empty line"):
            ObservedBeforeTarget.model_validate(
                {"family": FAMILY, "rationale": "a\nb", "evidence": "ADR-044"}
            )

    @pytest.mark.parametrize(
        ("key", "message"),
        [
            ("dc_forecast_4d_history", "label anchor"),
            ("regional_ci_forecast", "temporal none"),
            ("dc_dm_dr_long_term_fc", "temporal month"),
            ("as_industry_notifications_balancing", "temporal local_instant"),
            ("eac_br_mock_results_summary", "is held"),
        ],
    )
    def test_committed_families_the_rule_cannot_reach(self, key: str, message: str) -> None:
        """Detects the referential check admitting a committed EFA-anchored, temporal none,
        month, local-instant or package-held family."""
        entry = ObservedBeforeTarget(family=key, rationale="r", evidence="e")
        problems = observed_before_target_problems(registry_module.load_registry(), [entry])
        assert any(message in problem for problem in problems), problems


# --------------------------------------------------------------------------- #
# The committed policy
# --------------------------------------------------------------------------- #

OBSERVED = frozenset(
    {
        "bsuos_fixed_tariffs",
        "country_ci_forecast",
        "national_ci_forecast",
        "wind_forecast_14d_day_ahead_bmu",
        "wind_forecast_14d_days_ahead",
        "daily_opmr",
        "ifa_itl",
        "ifa2_ifa_itl",
        "viking_link_ntc",
        "system_operating_plan",
    }
)
"""Research class A less DC 4-day history (EFA label anchor) and IC requirement/auction
(requirements mixed with outcomes), REVIEW-REC-1 findings 2 and 3."""


class TestCommittedPolicy:
    def test_the_committed_policy_is_exactly_the_ten_families(self) -> None:
        """Detects a family added to or dropped from the policy without a decision, the two
        excluded class A families included."""
        assert {entry.family for entry in load_observed_before_target()} == OBSERVED

    def test_every_listed_family_stays_held(self) -> None:
        """Detects the policy turning a held output into an eligible one for any consumer
        (gold contracts would leave ``views/held/``)."""
        registry = registry_module.load_registry()
        for key in OBSERVED:
            pkg, fam = registry.families[key]
            assert isinstance(eligibility.effective_eligibility(pkg, fam), Held), key


# --------------------------------------------------------------------------- #
# The eligibility report and the vault skeleton
# --------------------------------------------------------------------------- #


class TestReport:
    def test_a_listed_hold_reads_held_with_the_observed_clause(self, policy: Registry) -> None:
        """Detects the clause missing from a listed hold, or added to an unlisted one."""
        rendered = eligibility.render(policy)
        listed = next(line for line in rendered.splitlines() if f"| `{FAMILY}` |" in line)
        assert f"held: Q (unit U); {eligibility.OBSERVED_BEFORE_TARGET_TEXT}" in listed
        unlisted = next(line for line in rendered.splitlines() if f"| `{UNLISTED}` |" in line)
        assert "held: Q (unit U) |" in unlisted
        assert eligibility.OBSERVED_BEFORE_TARGET_TEXT not in unlisted

    def test_the_clause_names_the_selectors_and_not_a_forecast(self) -> None:
        """Detects wording that certifies forecasts or omits the required selectors."""
        text = eligibility.OBSERVED_BEFORE_TARGET_TEXT
        assert "observed before their target start" in text
        assert "`observed_snapshot`" in text and "`observed_archive`" in text
        assert "forecast" not in text

    def test_totals_count_listed_outputs_and_keep_them_held(self, policy: Registry) -> None:
        """Detects a listed output counted as eligible, or the totals line missing."""
        rendered = eligibility.render(policy)
        assert "- Silver outputs held: 3" in rendered
        assert (
            "- Held outputs with rows readable as observed before their target start (ADR-044): 2"
            in rendered
        )

    def test_the_skeleton_holds_carry_the_same_clause(self, policy: Registry) -> None:
        """Detects the vault page and the report disagreeing about a listed hold."""
        (pkg,) = policy.packages
        lines = skeleton._holds(pkg, eligibility.observed_families(policy))
        listed = next(line for line in lines if f"`{FAMILY}`" in line)
        assert eligibility.OBSERVED_BEFORE_TARGET_TEXT in listed
        unlisted = next(line for line in lines if f"`{UNLISTED}`" in line)
        assert eligibility.OBSERVED_BEFORE_TARGET_TEXT not in unlisted


def test_the_fixture_dates_are_utc_and_ordered() -> None:
    """Guards the fixture: the cases assume T0 < T_I < T1 < T_C < T2 < T_E < T3 < T4."""
    order = [T0, T_I, T1, T_C, T2, T_E, T3, T4, FUTURE]
    assert order == sorted(order)
    assert all(t.tzinfo is UTC for t in order)
