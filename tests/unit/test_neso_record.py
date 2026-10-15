"""Frozen schema records and their load-time rules (ADR-034 P-1; T-B1-1..3, T-B2-2, T-B4-1).

Every negative starts from the valid default record (the positive control) and
breaks exactly one rule, and every error must name that rule, so each V-n is
shown to be load-bearing on its own.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest
from _neso_registry_support import (
    column,
    epoch,
    family,
    package,
    record,
    resource,
    sp_columns,
    write_registry,
)

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import (
    Eligible,
    Held,
    RegistryError,
    SchemaRecord,
)
from gridflow.connectors.neso_data_portal.registry.record import (
    RESERVED,
    ColumnSpec,
    validate_record,
)

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.registry import Registry

PKG = "dddddddd-0000-4000-8000-000000000000"
R1 = "dddddddd-0000-4000-8000-000000000001"
R2 = "dddddddd-0000-4000-8000-000000000002"


def _load(
    tmp_path: Path,
    rec: dict[str, Any] | None,
    *,
    extra_families: list[dict[str, Any]] | None = None,
    resources: list[dict[str, Any]] | None = None,
    family_kwargs: dict[str, Any] | None = None,
) -> Registry:
    families = [family("gen_series", record=rec, **(family_kwargs or {}))]
    families.extend(extra_families or [])
    document = package(
        "pkg-gen",
        PKG,
        families,
        resources if resources is not None else [resource(R1, "Series", "gen_series")],
    )
    return registry_module.load_registry(write_registry(tmp_path / "registry", [document]))


def _refused(tmp_path: Path, rule: str, rec: dict[str, Any] | None, **kwargs: Any) -> str:
    with pytest.raises(RegistryError) as info:
        _load(tmp_path, rec, **kwargs)
    message = str(info.value)
    assert f"{rule}:" in message, message
    assert "pkg-gen.json" in message
    return message


def _with_columns(columns: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    return record(epochs=[epoch(columns)], **kwargs)


class TestPositiveControl:
    def test_the_default_record_loads(self, tmp_path: Path) -> None:
        """Detects a rule that rejects a valid record (every negative builds on it)."""
        loaded = _load(tmp_path, record())
        _package, entry = loaded.families["gen_series"]
        assert isinstance(entry.record, SchemaRecord)
        assert entry.record.entity_key == ("settlement_date", "settlement_period", "unit")

    def test_eligibility_models_are_re_exported_unchanged(self) -> None:
        """Detects the move to record.py breaking the package's public names."""
        assert Eligible(status="eligible").status == "eligible"
        assert Held(status="held", question="q", unit="u").unit == "u"


class TestRules:
    """T-B1-1: one rejection per rule, each error naming the rule."""

    def test_v1_one_epoch_maps_two_vendor_columns_to_one_name(self, tmp_path: Path) -> None:
        cols = [*sp_columns(), column("Other", "value", "float64")]
        _refused(tmp_path, "V-1", _with_columns(cols))

    def test_v1_one_name_with_two_dtypes_across_epochs(self, tmp_path: Path) -> None:
        second = [*sp_columns()[:3], column("Value", "value", "string")]
        _refused(tmp_path, "V-1", record(epochs=[epoch(sp_columns()), epoch(second)]))

    @pytest.mark.parametrize("name", sorted(RESERVED - {"child_id"}))
    def test_v2_reserved_name(self, tmp_path: Path, name: str) -> None:
        cols = [*sp_columns(), column("Extra", name, "string")]
        _refused(tmp_path, "V-2", _with_columns(cols))

    def test_v3_temporal_column_absent_from_an_epoch(self, tmp_path: Path) -> None:
        second = [c for c in sp_columns() if c["name"] != "settlement_period"]
        _refused(tmp_path, "V-3", record(epochs=[epoch(sp_columns()), epoch(second)]))

    def test_v3_nullable_temporal_input(self, tmp_path: Path) -> None:
        cols = sp_columns()
        cols[1] = column("SettlementPeriod", "settlement_period", "int64", nullable=True)
        _refused(tmp_path, "V-3", _with_columns(cols))

    def test_v3_temporal_column_of_the_wrong_dtype(self, tmp_path: Path) -> None:
        cols = sp_columns()
        cols[1] = column("SettlementPeriod", "settlement_period", "string", nullable=False)
        _refused(tmp_path, "V-3", _with_columns(cols))

    def test_v3_issue_data_column_must_be_a_datetime(self, tmp_path: Path) -> None:
        rec = record(
            epochs=[epoch(sp_columns(), issue={"kind": "data_column", "column": "unit"})],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
        )
        _refused(tmp_path, "V-3", rec)

    def test_v4_entity_key_outside_the_outputs(self, tmp_path: Path) -> None:
        rec = record(entity_key=("settlement_date", "settlement_period", "nowhere"))
        _refused(tmp_path, "V-4", rec)

    def test_v4_issue_time_in_key_without_an_issue_recipe(self, tmp_path: Path) -> None:
        rec = record(entity_key=("settlement_date", "settlement_period", "unit", "issue_time"))
        _refused(tmp_path, "V-4", rec)

    def test_v4_issue_recipe_without_issue_time_in_key(self, tmp_path: Path) -> None:
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        _refused(tmp_path, "V-4", record(epochs=[epoch(sp_columns(), issue=issue)]))

    def test_v5_run_type_column_not_in_key(self, tmp_path: Path) -> None:
        cols = [*sp_columns(), column("Run", "run_type", nullable=False)]
        _refused(tmp_path, "V-5", _with_columns(cols, run_type_column="run_type"))

    def test_v5_run_type_column_not_a_string(self, tmp_path: Path) -> None:
        cols = [*sp_columns(), column("Run", "run_type", "int64", nullable=False)]
        rec = _with_columns(
            cols,
            run_type_column="run_type",
            entity_key=("settlement_date", "settlement_period", "unit", "run_type"),
        )
        _refused(tmp_path, "V-5", rec)

    def test_v7_evidence_on_a_vintage_that_takes_none(self, tmp_path: Path) -> None:
        _refused(tmp_path, "V-7", record(vintage_evidence="because"))

    def test_v7_issue_time_evidenced_with_an_epoch_without_issue(self, tmp_path: Path) -> None:
        issue = {"kind": "data_column", "column": "issued"}
        first = [
            *sp_columns(),
            column(
                "Issued",
                "issued",
                "datetime",
                nullable=False,
                format="%Y-%m-%dT%H:%M:%S",
                zone="UTC",
            ),
        ]
        second = [
            *sp_columns(),
            column(
                "Issued",
                "issued",
                "datetime",
                nullable=False,
                format="%Y-%m-%dT%H:%M:%S",
                zone="UTC",
            ),
            column("X", "x"),
        ]
        rec = record(
            epochs=[epoch(first, issue=issue), epoch(second)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
            vintage="issue_time_evidenced",
            vintage_evidence="issue column is the vendor publication instant",
        )
        _refused(tmp_path, "V-7", rec)

    def test_v8_datastore_resource_under_ckan_last_modified(self, tmp_path: Path) -> None:
        resources = [resource(R1, "Series", "gen_series", url_type="datastore")]
        _refused(tmp_path, "V-8", record(), resources=resources)

    def test_v9_record_on_a_files_family(self, tmp_path: Path) -> None:
        resources = [resource(R1, "Series", "gen_series", fmt="PDF")]
        _refused(tmp_path, "V-9", record(), resources=resources, family_kwargs={"kind": "files"})

    def test_v9_record_on_a_legacy_family(self, tmp_path: Path) -> None:
        """A legacy key carries no record (C-5). Driven directly: a legacy family
        must also be one of the three named keys, which the loader checks first."""
        with pytest.raises(ValueError, match="V-9:"):
            validate_record(
                SchemaRecord.model_validate(record()),
                key="daily_wind_availability",
                kind="tabular",
                legacy=True,
                package_families={"daily_wind_availability": True},
                family_url_types=frozenset({"upload"}),
            )

    def test_v19_a_key_column_cannot_be_trimmed(self, tmp_path: Path) -> None:
        """Detects a trimmed key column: trimming would merge entity keys that
        differ only by padding (BAL-2aH). Positive control: the non-key value."""
        cols = sp_columns()
        cols[1] = column(
            "SettlementPeriod", "settlement_period", "int64", nullable=False, trim="ascii"
        )
        _refused(tmp_path, "V-19", _with_columns(cols))
        control = sp_columns()
        control[3] = column("Value", "value", "float64", trim="ascii")
        loaded = _load(tmp_path / "control", _with_columns(control))
        entry = loaded.families["gen_series"][1].record
        assert entry is not None
        assert entry.epochs[0].columns[3].trim == "ascii"

    def test_v10_sibling_outside_the_package(self, tmp_path: Path) -> None:
        _refused(tmp_path, "V-10", record(siblings=("elsewhere",)))

    def test_v10_sibling_carrying_its_own_record(self, tmp_path: Path) -> None:
        sibling = family("gen_sibling", record=record())
        resources = [
            resource(R1, "Series", "gen_series"),
            resource(R2, "Sibling", "gen_sibling"),
        ]
        _refused(
            tmp_path,
            "V-10",
            record(siblings=("gen_sibling",)),
            extra_families=[sibling],
            resources=resources,
        )

    def test_v11_silver_under_a_family_that_does_not_list_the_sibling(self, tmp_path: Path) -> None:
        resources = [
            resource(R1, "Series", "gen_series"),
            resource(
                R2, "Container", "gen_box", disposition={"kind": "SILVER", "key": "gen_series"}
            ),
        ]
        _refused(
            tmp_path,
            "V-11",
            record(),
            extra_families=[family("gen_box")],
            resources=resources,
        )

    def test_v11_child_silver_under_a_family_that_does_not_list_the_sibling(
        self, tmp_path: Path
    ) -> None:
        children = [{"child": "sheet1", "disposition": {"kind": "SILVER", "key": "gen_series"}}]
        resources = [
            resource(R1, "Series", "gen_series"),
            resource(R2, "Box", "gen_box", fmt="XLSX", children=children),
        ]
        _refused(
            tmp_path,
            "V-11",
            record(),
            extra_families=[family("gen_box", kind="files")],
            resources=resources,
        )

    def test_v11_sibling_read_is_accepted_when_listed(self, tmp_path: Path) -> None:
        """Positive control for V-11: the sibling read the plan allows.

        The record reads sheets (``xlsx``) and the family's own CSV resource is held,
        so the fixture also satisfies V-15 (ADR-037: a child needs a container reader).
        """
        children = [{"child": "sheet1", "disposition": {"kind": "SILVER", "key": "gen_series"}}]
        held = {"kind": "HOLD", "reason": "not this reader", "unit": "T"}
        resources = [
            resource(R1, "Series", "gen_series", disposition=held),
            resource(R2, "Box", "gen_box", fmt="XLSX", children=children),
        ]
        loaded = _load(
            tmp_path,
            record(siblings=("gen_box",), reader="xlsx", xlsx={"header_row": 1, "columns": "A:D"}),
            extra_families=[family("gen_box", kind="files")],
            resources=resources,
        )
        _pkg, owner = loaded.resources[R2]
        assert owner.children[0].child == "sheet1"

    def test_v12_covered_grant_without_evidence(self, tmp_path: Path) -> None:
        resources = [
            resource(R1, "Series", "gen_series"),
            resource(R2, "Copy", "gen_series", disposition={"kind": "COVERED", "by": R1}),
        ]
        _refused(tmp_path, "V-12", record(), resources=resources)

    def test_v12_covered_grant_with_evidence_loads(self, tmp_path: Path) -> None:
        evidence = {"fingerprint": "0" * 64, "components": {"harness": "0" * 64}}
        resources = [
            resource(R1, "Series", "gen_series"),
            resource(
                R2,
                "Copy",
                "gen_series",
                disposition={
                    "kind": "COVERED",
                    "by": R1,
                    "key": "gen_series",
                    "evidence": evidence,
                },
            ),
        ]
        assert _load(tmp_path, record(), resources=resources).resources[R2]

    def test_v13_date_sp1_on_a_timestamptz_designated_name(self, tmp_path: Path) -> None:
        cols = [column("Day", "ingested_at", "date", nullable=False), column("Unit", "unit")]
        rec = _with_columns(
            cols,
            temporal={"kind": "date_sp1", "date_column": "ingested_at"},
            entity_key=("ingested_at", "unit"),
        )
        _refused(tmp_path, "V-13", rec)

    def test_v13_sp_pair_on_a_timestamptz_designated_name(self, tmp_path: Path) -> None:
        cols = sp_columns()
        cols[0] = column("SettlementDate", "implementation_datetime_utc", "date", nullable=False)
        rec = _with_columns(
            cols,
            temporal={
                "kind": "sp_pair",
                "date_column": "implementation_datetime_utc",
                "period_column": "settlement_period",
            },
            entity_key=("implementation_datetime_utc", "settlement_period", "unit"),
        )
        _refused(tmp_path, "V-13", rec)

    def test_v13_accepts_settlement_date_and_an_unmapped_name(self, tmp_path: Path) -> None:
        cols = [column("Day", "trading_day", "date", nullable=False), column("Unit", "unit")]
        rec = _with_columns(
            cols,
            temporal={"kind": "date_sp1", "date_column": "trading_day"},
            entity_key=("trading_day", "unit"),
        )
        assert _load(tmp_path, rec).families["gen_series"]
        assert _load(tmp_path / "b", record()).families["gen_series"]


class TestSpPairKeyContainment:
    """T-B1-2 (V-6): a key equal to (date, period) would dedup on the pair alone."""

    def test_key_equal_to_the_pair_is_rejected(self, tmp_path: Path) -> None:
        rec = record(entity_key=("settlement_date", "settlement_period"))
        _refused(tmp_path, "V-6", rec)

    def test_the_same_key_plus_run_type_is_accepted(self, tmp_path: Path) -> None:
        cols = [*sp_columns()[:2], column("Run", "run_type", nullable=False)]
        rec = _with_columns(
            cols,
            entity_key=("settlement_date", "settlement_period", "run_type"),
            run_type_column="run_type",
        )
        assert _load(tmp_path, rec).families["gen_series"][1].record is not None


class TestVintageRules:
    def test_t_b2_2_issue_time_evidenced_without_evidence(self, tmp_path: Path) -> None:
        """T-B2-2 (V-7): a vendor clock is never asserted without evidence."""
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        rec = record(
            epochs=[epoch(sp_columns(), issue=issue)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
            vintage="issue_time_evidenced",
        )
        _refused(tmp_path, "V-7", rec)

    def test_t_b4_1_whole_capture_with_issue_time_evidenced(self, tmp_path: Path) -> None:
        """T-B4-1 (V-8): whole-capture selection orders by capture, not by row."""
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        rec = record(
            epochs=[epoch(sp_columns(), issue=issue)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
            vintage="issue_time_evidenced",
            vintage_evidence="filename token is the issue instant",
            latest="whole_capture",
        )
        _refused(tmp_path, "V-8", rec)


class TestColumnShapes:
    """The models' own shape checks (surface as a RegistryError naming the file)."""

    @pytest.mark.parametrize(
        "spec",
        [
            {"source": "D", "name": "d", "dtype": "date", "nullable": False},
            {"source": "N", "name": "Bad", "dtype": "string", "nullable": True},
            {"source": "S", "name": "s", "dtype": "string", "nullable": True, "min": 0},
            {
                "source": "T",
                "name": "t",
                "dtype": "datetime",
                "nullable": False,
                "format": "%Y-%m-%d %H:%M",
            },
            {
                "source": "T",
                "name": "t",
                "dtype": "datetime",
                "nullable": False,
                "format": "%Y-%m-%d %H:%M",
                "zone": "Europe/London",
            },
            {
                "source": "T",
                "name": "t",
                "dtype": "datetime",
                "nullable": False,
                "format": "%Y-%m-%d %H:%M",
                "zone": "Not/AZone",
                "zone_evidence": "x",
                "ambiguous": "raise",
            },
            {"source": "S", "name": "s", "dtype": "string", "nullable": True, "trim": "ascii"},
            {
                "source": "D",
                "name": "d",
                "dtype": "date",
                "nullable": True,
                "format": "%Y-%m-%d",
                "trim": "ascii",
            },
            {
                "source": "T",
                "name": "t",
                "dtype": "datetime",
                "nullable": True,
                "format": "%Y-%m-%d %H:%M",
                "zone": "UTC",
                "trim": "ascii",
            },
            {"source": "V", "name": "v", "dtype": "float64", "nullable": True, "trim": "unicode"},
            {"source": "V", "name": "v", "dtype": "float64", "nullable": True, "trim": True},
            {
                "source": "V",
                "name": "v",
                "dtype": "float64",
                "nullable": True,
                "trim": "ascii",
                "null_tokens": [" - "],
            },
        ],
    )
    def test_malformed_column_specs_are_rejected(self, spec: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            ColumnSpec.model_validate(spec)

    @pytest.mark.parametrize("dtype", ["float64", "int64"])
    def test_a_trimmed_numeric_spec_is_accepted(self, dtype: str) -> None:
        """Detects the opt-in edge trim being refused on a numeric column (BAL-2aH)."""
        spec = ColumnSpec.model_validate(
            {
                "source": "V",
                "name": "v",
                "dtype": dtype,
                "nullable": True,
                "null_tokens": ["-"],
                "trim": "ascii",
            }
        )
        assert spec.trim == "ascii"

    def test_the_default_dump_carries_no_trim(self) -> None:
        """T-DUMP. Detects a ``trim`` key entering every record's dump, which would
        change every equivalence fingerprint and golden pin."""
        dumped = SchemaRecord.model_validate(record()).model_dump(mode="json", exclude_none=True)
        for each in dumped["epochs"]:
            for spec in each["columns"]:
                assert "trim" not in spec, spec

    def test_an_opted_in_trim_round_trips(self) -> None:
        """T-DUMP. Detects the opt-in being lost through a dump and reload."""
        cols = sp_columns()
        cols[3] = column("Value", "value", "float64", trim="ascii")
        loaded = SchemaRecord.model_validate(_with_columns(cols))
        again = SchemaRecord.model_validate(loaded.model_dump(mode="json"))
        assert again == loaded
        assert again.epochs[0].columns[3].trim == "ascii"

    def test_an_iana_zone_with_evidence_and_a_rule_is_accepted(self) -> None:
        spec = ColumnSpec.model_validate(
            {
                "source": "T",
                "name": "t",
                "dtype": "datetime",
                "nullable": False,
                "format": "%Y-%m-%d %H:%M",
                "zone": "Europe/London",
                "zone_evidence": "vendor docs state UK local time",
                "ambiguous": "earliest",
            }
        )
        assert spec.is_local

    def test_unknown_encoding_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegistryError, match="unknown encoding"):
            _load(tmp_path, record(encoding="no-such-codec"))


PILOT_RECORDED = {
    "tec_register",
    "interconnector_register",
    "embedded_register",
    "demand_forecast_2_52w",
    "da_demand_fc_performance",
    "constraint_cost_fc_24m",
}
"""The families v0.22-E's pilot records (ADR-036)."""

X_RECORDED = {"current_bsuos_cap_adjustments", "ffr_phase2_result_summary_archive"}
"""The two activation families unit X records (ADR-037 P-12); every other family is
record-free."""

PILOT_PACKAGE_FILES = {
    "transmission-entry-capacity-tec-register.json",
    "interconnector-register.json",
    "embedded-register.json",
    "long-term-2-52-weeks-ahead-national-demand-forecast.json",
    "day-ahead-half-hourly-demand-forecast-performance.json",
    "24-months-ahead-constraint-cost-forecast.json",
}

X_PACKAGE_FILES = {
    "current-balancing-services-use-of-system-bsuos-data.json",
    "phase-2-ffr-auction-results-summary.json",
}

EF_RECORDED = {"embedded_forecast_archive_upload", "embedded_forecast_archive_dump"}
"""The two sibling-fed embedded-archive owners unit EF records (ADR-038)."""

EF_PACKAGE_FILES = {"embedded-wind-and-solar-forecasts.json"}

CI_RECORDED = {
    "national_ci_forecast",
    "regional_ci_forecast",
    "country_ci_forecast",
    "ci_balancing_actions",
    "portal_known_issues",
    "capacity_market_auction_cost",
    "capacity_market_auction_static",
    "capacity_market_de_rating_factors",
    "capacity_market_unit_cmu",
    "capacity_market_unit_cmu_history",
    "capacity_market_component_history",
    "capacity_market_component_history_pre",
    "capacity_market_components",
}
"""The thirteen carbon-intensity and capacity-market families unit K-CI records."""

CI_PACKAGE_FILES = {
    "national-carbon-intensity-forecast.json",
    "regional-carbon-intensity-forecast.json",
    "country-carbon-intensity-forecast.json",
    "carbon-intensity-of-balancing-actions.json",
    "data-portal-planned-changes-known-issues.json",
    "capacity-market-register.json",
}

DEM1_RECORDED = {
    "demand_forecast_1d_day_ahead_national",
    "demand_forecast_1d_historic_day_ahead",
    "demand_forecast_2d_day_ahead",
    "demand_forecast_2d_historic_day_ahead",
    "national_forecast_7d_day_ahead_demand",
    "national_forecast_7d_historic_day_ahead",
    "national_demand_fc_2_14d_days_ahead",
    "national_demand_fc_2_14d_days_ahead_half",
    "national_demand_fc_2_14d_historic_day",
    "daily_demand_update",
}
"""The ten demand-forecast families unit K-DEM-1 records."""

DEM1_PACKAGE_FILES = {
    "1-day-ahead-demand-forecast.json",
    "2-day-ahead-demand-forecast.json",
    "7-day-ahead-national-forecast.json",
    "2-14-days-ahead-national-demand-forecast.json",
    "daily-demand-update.json",
}

DEM1H_RECORDED = {"historic_demand"}
"""The multi-resource family unit DEM-1H records (per-resource selection, ADR-039)."""

DEM1H_PACKAGE_FILES = {"historic-demand-data.json"}

DEM2_RECORDED = {
    "national_demand_bmus",
    "school_holiday_percentages",
    "transmission_losses_main",
    "transmission_losses_financial_year",
}
"""The four demand-reference families unit K-DEM-2 records (``demand_profile_dates`` stays
ingest-only: its body repeats identical rows, RULINGS 538)."""

DEM2_PACKAGE_FILES = {
    "national-demand-balancing-mechanism-units.json",
    "school-holiday-percentages.json",
    "transmission-losses.json",
}

GEN1_RECORDED = {
    "metered_wind_forecast_14d",
    "metered_wind_forecast_14d_windfarm",
    "wind_forecast_14d_days_ahead",
    "wind_forecast_14d_day_ahead_bmu",
    "da_wind_forecast_day_ahead",
    "da_wind_forecast_day_ahead_bmu",
    "da_wind_forecast_historic_day_ahead",
    "da_wind_forecast_historic_day_ahead_bmu",
    "nrapm_forecast_national_daily_days",
    "nrapm_forecast_scotland_daily_days",
    "nrapm_forecast_weekly_week_national",
    "nrapm_forecast_weekly_week_scotland",
    "daily_opmr",
    "weekly_opmr",
}
"""The fourteen wind / margin forecast families unit K-GEN-1 records."""

GEN2_RECORDED = {"weekly_wind_availability"}
"""The one weekly wind availability family unit K-GEN-2 records (held)."""

GEN1_PACKAGE_FILES = {
    "14-days-ahead-operational-metered-wind-forecasts.json",
    "14-days-ahead-wind-forecasts.json",
    "day-ahead-wind-forecast.json",
    "negative-reserve-active-power-margin-nrapm-forecast.json",
    "daily-opmr.json",
    "weekly-opmr.json",
}

GEN2_PACKAGE_FILES = {"weekly-wind-availability.json"}

GEN2H_RECORDED = {"metered_wind_output_monthly", "wind_bmu_boa_volumes"}
"""The metered wind output (eligible) and wind BOA volume (held) families unit GEN-2H records."""

GEN2H_PACKAGE_FILES = {"monthly-operational-metered-wind-output.json", "wind-bmu-boa-volumes.json"}

IC1_RECORDED = {
    "eleclink",
    "ifa_itl",
    "ifa_da_id_weekly_itls",
    "ifa2_ifa_itl",
    "ifa2_ifa_da_id_weekly_itls",
    "nemolink_ntc",
    "nemolink_intraday",
    "nemolink_nemo_da_id_weekly_ntcs",
    "nsl",
    "viking_ntc",
    "viking_link_ntc",
}
"""The eleven interconnector limit families unit K-IC-1 records (all held)."""

IC1_PACKAGE_FILES = {
    "eleclink.json",
    "ifa.json",
    "ifa2.json",
    "nemolink.json",
    "nsl.json",
    "viking.json",
}

IC2_RECORDED = {"brit_ned", "nordpool_da_prices", "system_operating_plan"}
"""The three families unit K-IC-2 records (BritNed and the SOP held, Nord Pool eligible)."""

IC2_PACKAGE_FILES = {
    "brit-ned.json",
    "day-ahead-power-exchange-prices-nordpool.json",
    "system-operating-plan-sop.json",
}

SC_RECORDED = {"fes_ed1_electricity_demand"}
"""The FES ED1 pilot unit SC records through unpivot and edition (held, ADR-042)."""

SC_PACKAGE_FILES = {"fes-electricity-demand-summary-data-table-ed1.json"}
"""The one package file unit SC's pilot record lives in."""

SCN1A_RECORDED = {
    "tresp_demand_pathways_gsp",
    "tresp_demand_pathways_la_england",
    "tresp_demand_pathways_la_scotland",
    "tresp_demand_pathways_la_wales",
    "tresp_demand_pathways_resp_region",
    "tresp_generation_pathways_gsp",
    "tresp_generation_pathways_la_england",
    "tresp_generation_pathways_la_scotland",
    "tresp_generation_pathways_la_wales",
    "tresp_generation_pathways_resp_region",
    "tresp_building_block_definitions",
    "tresp_gsp_area_names",
}
"""The ten tRESP pathway CSV families (demand held, generation eligible) and the two
workbook reference families unit K-SCN-1a records."""

SCN1A_PACKAGE_FILES = {"tresp-demand-pathways.json", "tresp-generation-pathways.json"}

SCN1B_RECORDED = {
    "fes_regional_demand_active_power",
    "fes_regional_dg_gt_1mw",
    "fes_regional_dg_lt_1mw",
    "fes_regional_dsr",
    "fes_regional_gsp_info",
    "fes_regional_storage_gt_1mw",
    "fes_regional_storage_gt_1mw_pre2023",
    "fes_regional_storage_lt_1mw",
    "fes_regional_storage_lt_1mw_pre2023",
}
"""The nine regional-breakdown-of-FES families (five eligible, four held) unit K-SCN-1b records."""

SCN1B_PACKAGE_FILES = {"regional-breakdown-of-fes-data-electricity.json"}

SCN1C_RECORDED = {
    "fes_building_blocks_main",
    "fes_building_blocks_block_definitions",
    "fes_building_blocks_block_licence_area",
}
"""The three FES building block families (two eligible, the unpivoted main held) unit K-SCN-1c
records."""

SCN1C_PACKAGE_FILES = {"future-energy-scenario-fes-building-block-data.json"}

SCN1D_RECORDED = {"fes_es1_electricity_supply"}
"""The FES ES1 electricity supply family (held, three of its seven resources HOLD) unit K-SCN-1d
records."""

SCN1D_PACKAGE_FILES = {"future-energy-scenario-electricity-supply-data-table-es1.json"}

CON_RECORDED = {
    "constraint_limits_24m",
    "cmis_intertrip",
    "da_constraint_flows_limits",
    "otf_network_congestion",
    "thermal_constraint_costs",
    "thermal_constraint_costs_xlsx",
    "voltage_requirement",
}
"""The seven constraint-management families unit K-CON records (five held, the thermal CSV and
workbook child eligible, the day-ahead capture adjudicated failed, the 2021-22 thermal resource
HOLD)."""

CON_PACKAGE_FILES = {
    "24-months-ahead-constraint-limits.json",
    "constraint-management-intertrip-service-information-cmis.json",
    "day-ahead-constraint-flows-and-limits.json",
    "operational-transparency-forum-network-congestion-data.json",
    "thermal-constraint-costs.json",
    "voltage-requirement.json",
}

SYS1_RECORDED = {
    "voltage_units_utilisation",
    "stability_midterm_y1",
    "stability_pathfinder_utilisation_report",
    "stability_pathfinder_availability_report",
    "system_inertia",
    "system_inertia_cost",
    "outturn_voltage_costs_historical",
    "outturn_voltage_costs_main",
}
"""The eight system families unit K-SYS-1 records (six eligible, the two Pathfinder reports held,
six Pathfinder captures adjudicated failed, the 2026 inertia cost resource HOLD)."""

SYS1_PACKAGE_FILES = {
    "monthly-utilisation-data-of-voltage-contracted-units.json",
    "outturn-voltage-costs.json",
    "stability-midterm-y-1-utilisation-report.json",
    "stability-pathfinder-service-information.json",
    "system-inertia-cost.json",
    "system-inertia.json",
}

BAL1_RECORDED = {
    "current_bsuos_ii",
    "current_bsuos_sf",
    "current_bsuos_rf",
    "current_bsuos_historic_ii",
    "current_bsuos_historic_sf",
    "current_bsuos_historic_rf",
    "constraint_breakdown",
    "bsuos_fixed_tariffs",
    "inertia_bid_offer_costs",
}
"""The nine balancing-costs families unit K-BAL-1 records (seven eligible, tariffs and inertia held,
the Historic II overlap adjudicated; the two daily balancing families stay without a record)."""

BAL1_PACKAGE_FILES = {
    "bsuos-fixed-tariffs.json",
    "constraint-breakdown.json",
    "current-balancing-services-use-of-system-bsuos-data.json",
    "gb-system-inertia-bid-and-offer-costs.json",
}

BAL2A_RECORDED = {"bsuos_monthly_forecast_fc_summary"}
"""The BSUoS monthly forecast summary family unit K-BAL-2a records (held; the eight other BSUoS
families have no record: every newest body is invalid UTF-8 or carries no run)."""

BAL2A_PACKAGE_FILES = {"bsuos-monthly-forecast.json"}

BAL2B_RECORDED = {
    "skip_rates_summary",
    "skip_rates_stage5_psa_technology",
    "skip_rates_behind_constraints_bmu",
    "skip_rates_behind_constraints_cons",
    "skip_rates_in_merit_all_bm",
    "skip_rates_in_merit_psa",
    "skip_rates_exclusion_reasons",
    "skip_rates_stage6_acceptance",
    "skip_rates_stage6_psa",
}
"""The nine skip-rate families unit K-BAL-2b records (two eligible, seven held; the two DSF
families stay record-less: their bodies repeat whole rows)."""

BAL2B_PACKAGE_FILES = {"skip-rates.json"}

BAL2C_RECORDED = {
    "aahedc_tariffs",
    "tnuos_tariffs_embedded_export",
    "tnuos_tariffs_half_hourly_hh_demand",
    "tnuos_tariffs_non_half_hourly_nhh_demand",
    "tnuos_tariffs_onshore_generator",
    "tnuos_tariffs_local_offshore_generator",
    "tnuos_tariffs_local_substation_tariff",
    "tnuos_tariffs_transmission_demand",
    "tnuos_tariffs_onshore_local_circuit",
}
"""The AAHEDC family and the eight TNUoS tariff families unit K-BAL-2c records (all nine held)."""

BAL2C_PACKAGE_FILES = {
    "aahedc-tariffs.json",
    "transmission-network-use-of-system-tnuos-tariffs.json",
}

TRD_RECORDED = {
    "bs_contract_enactment_balancing",
    "bs_contract_enactment_balancing_service",
    "gtma_trades_data",
    "gtma_trades_data_pre",
    "gtma_trades_fy",
    "index_linked_contract_volume",
    "ic_requirement_auction",
    "ssel_contract_enactment",
    "upcoming_trades",
}
"""The nine trading families unit K-TRD records (two eligible, seven held)."""

TRD_PACKAGE_FILES = {
    "balancing-services-contract-enactment.json",
    "historic-gtma-grid-trade-master-agreement-trades-data.json",
    "index-linked-contract-volume.json",
    "interconnector-requirement-and-auction-summary-data.json",
    "super-stable-export-limit-contract-enactment.json",
    "upcoming-trades.json",
}

AS1_RECORDED = {
    "br_auction_requirement_fc_da",
    "br_auction_requirement_fc_da_archive",
    "br_auction_requirement_fc_medium_term",
    "dc_forecast_4d",
    "dc_forecast_4d_history",
    "dm_requirements",
    "dr_requirements",
    "dc_dm_dr_long_term_fc",
    "qr_auction_requirement_fc",
}
"""The nine ancillary-services families unit K-AS-1 records (four eligible, five held)."""

AS2_RECORDED = {
    "as_industry_notifications_balancing",
    "as_industry_notifications_dc_dm_dr",
    "as_industry_notifications_quick_reserve",
    "as_industry_notifications_sffr_important",
    "as_industry_notifications_slow_reserve",
    "as_industry_notifications_stor_important",
    "slow_reserve_requirement_fc",
    "sffr_auction_results_main",
    "sffr_auction_results_buy_orders",
    "sffr_requirement",
    "stor_windows",
}
"""The eleven ancillary-services families unit K-AS-2 records (nine eligible, two held)."""

AS3A_RECORDED = {
    "eac_results_by_unit",
    "eac_results_by_unit_fy",
    "eac_results_daily_by_unit",
    "eac_results_buy_orders",
    "eac_results_buy_orders_fy",
    "eac_results_daily_buy_orders",
    "eac_results_summary",
    "eac_results_summary_fy",
    "eac_results_daily_summary",
    "eac_results_sell_orders",
    "eac_results_daily_sell_orders",
}
"""The eleven EAC auction-result families unit K-AS-3a records (all eligible)."""

AS3B_RECORDED = {
    "eac_mock_results_by_unit",
    "eac_mock_results_buy_orders",
    "eac_mock_results_summary",
    "eac_mock_results_sell_orders",
    "orps_utilisation_historic_reactive",
    "orps_utilisation_reactive",
    "orps_utilisation_reactive_default",
    "obp_physical_notifications",
    "obp_reserve_instructions",
    "obp_reserve_avail_price",
}
"""The ten mock EAC, ORPS and OBP families unit K-AS-3b records (three eligible, seven held)."""

AS4A_RECORDED = {
    "aggregated_bsad",
    "bsad_forward_contracts",
    "dynamic_containment_masterdata",
    "dynamic_containment_dc_dr_dm_results_sum",
    "dynamic_containment_dc_dr_dm_results",
    "dynamic_containment_dc_dr_dm_linear",
    "dynamic_containment_dc_dr_dm_block",
}
"""The seven BSAD and legacy Dynamic Containment families unit K-AS-4a records (five eligible, two
held)."""

AS4B_RECORDED = {
    "eac_br_results_by_unit",
    "eac_br_results_buy_orders",
    "eac_br_results_summary",
    "eac_br_results_sell_orders",
    "eac_br_mock_results_by_unit",
    "eac_br_mock_results_buy_orders",
    "eac_br_mock_results_sell_orders",
    "eac_br_mock_results_summary",
}
"""The eight EAC-BR archive and mock families unit K-AS-4b records (four eligible, four held)."""

AS5A_RECORDED = {
    "ffr_post_tender_reports_csv_ext",
    "ffr_phase2_auction_phase_block_order",
    "ffr_phase2_auction_phase_blockorders",
    "ffr_phase2_auction_phase_linear_orders",
    "ffr_phase2_auction_phase_linearorders",
    "ffr_phase2_auction_phase_resultsbyunit",
    "ffr_phase2_auction_phase_resultsum",
}
"""The seven FFR post-tender and Phase-2 FFR auction families unit K-AS-5a records (five eligible,
two held)."""

AS5B_RECORDED = {
    "asdp_instructions",
    "asdp_window_prices",
    "odfm_market_information_instructed",
    "odfm_market_information_load_factors",
    "stor_da_auction_results",
}
"""The five ASDP, ODFM and STOR day-ahead families unit K-AS-5b records (three eligible, two
held)."""

AS5C_RECORDED = {
    "ffr_post_tender_reports_workbook",
    "ffr_phase2_blockorders_archive",
    "ffr_phase2_linearorders_archive",
    "ffr_phase2_results_by_unit_archive",
}
"""The FFR post-tender workbook family and the three Phase-2 FFR ZIP-archive families unit
K-AS-5c records (one eligible, three held)."""

DFSA_RECORDED = {
    "demand_flexibility_service_requirement",
    "demand_flexibility_industry_notification",
    "demand_flexibility_utilisation_report",
    "demand_flexibility_utilisation_summary",
    "stor_da_buy_curve",
}
"""The four Demand Flexibility Service families and the STOR day-ahead buy curve unit K-DFS-a
records (four eligible, one held)."""

DFSB_RECORDED = {
    "dfs_industry_notification",
    "dfs_service_requirements",
    "dfs_utilisation_report",
    "dfs_utilisation_report_sum",
    "dfs_live_events_service_requirement",
    "dfs_live_events_service_update_industry",
    "dfs_live_events_utilisation_report",
    "dfs_live_events_utilisation_report_sum",
    "dfs_test_events_service_requirement",
    "dfs_test_events_service_update_industry",
    "dfs_test_events_utilisation_report",
    "dfs_test_events_utilisation_report_sum",
}
"""The twelve frozen Demand Flexibility Service families (2023/24 service, 2022/23 live events and
test events) unit K-DFS-b records (ten eligible, two held)."""

SCN2A_RECORDED = {
    "fes_es2_european_supply",
    "fes_flx1_flexibility",
    "fes_ed5_road_transport",
    "fes_ed6_road_transport_notes",
}
"""The FES ES2, FLX1, ED5 and ED6 families unit K-SCN-2a records (three held, ED6 eligible; two
resources HOLD)."""

SCN2B_RECORDED = {
    "fes_ed3_heat_demand_natural_gas",
    "fes_ed3_heat_demand_natural_gas_sum_ed",
    "fes_ed3_heat_demand_sum_ed",
    "fes_ed4_gas_demand_defs",
}
"""The FES ED3 combined, ED3 gas summary, ED7 and ED4 families unit K-SCN-2b records (three held,
ED4 eligible)."""

SCN3_RECORDED = {
    "building_heat_model_example_output_half",
    "fes_ws1_gas_supply",
    "fes_ws2_gas_emissions",
    "fes_la_heat_model_domestic",
    "fes_la_heat_model_residential",
    "resource_adequacy_2030s_portfolio",
    "resource_adequacy_2030s_peak_demand",
    "resource_adequacy_2030s_results",
}
"""The Building Heat Model example output, WS1, WS2, local-authority domestic and residential heat
model and three Resource Adequacy 2030s families unit K-SCN-3 records (five held, Resource Adequacy
eligible; one WS1 resource HOLD, one WS1 ADR-040 failed entry)."""

FILE_RECORDED = {"gis_grid_supply_points"}
"""The GSP / Gnode / Direct Connect / Region lookup family unit K-FILE records (eligible)."""

SCN2A_PACKAGE_FILES = {
    "fes-european-electricity-supply-data-table-es2.json",
    "fes-flexibility-data-table-data-table-flx1.json",
    "fes-road-transport-summary-data-table-ed5.json",
    "fes-road-transport-notes-data-table-ed6.json",
}

SCN2B_PACKAGE_FILES = {
    "fes-natural-gas-demand-definitions-ed4.json",
    "fes-natural-gas-residential-and-non-domestic-i-c-heat-demand-summary-data-table-ed3.json",
}

SCN3_PACKAGE_FILES = {
    "building-heat-model.json",
    "fes-whole-system-gas-supply-data-table-ws1.json",
    "fes-whole-system-gas-supply-emissions-data-table-ws2.json",
    "local-authority-level-spatial-heat-model-outputs-fes.json",
    "resource-adequacy-in-2030s.json",
}

FILE_PACKAGE_FILES = {"gis-boundaries-for-gb-grid-supply-points.json"}

AS1_PACKAGE_FILES = {
    "balancing-reserve-auction-requirement-forecast.json",
    "dynamic-containment-4-day-forecast.json",
    "dynamic-moderation-requirements.json",
    "dynamic-regulation-requirements.json",
    "long-term-forecasts-for-dc-dm-dr-requirements.json",
    "quick-reserve-auction-requirement-forecast.json",
}

AS3A_PACKAGE_FILES = {
    "eac-auction-results.json",
}

AS4A_PACKAGE_FILES = {
    "aggregated-bsad.json",
    "balancing-services-adjustment-data-forward-contracts.json",
    "dynamic-containment-data.json",
}

AS4B_PACKAGE_FILES = {
    "eac-br-auction-results.json",
    "eac-br-mock-auction-results.json",
}

AS5A_PACKAGE_FILES = {
    "firm-frequency-response-post-tender-reports.json",
    "phase-2-ffr-auction-results-summary.json",
}

AS5B_PACKAGE_FILES = {
    "non-bm-ancillary-service-dispatch-platform-asdp-instructions.json",
    "non-bm-ancillary-service-dispatch-platform-asdp-window-prices.json",
    "optional-downward-flexibility-management-odfm-market-information.json",
    "short-term-operating-reserve-stor-day-ahead-auction-results.json",
}

DFSA_PACKAGE_FILES = {
    "demand-flexibility.json",
    "short-term-operating-reserve-stor-day-ahead-buy-curve.json",
}

DFSB_PACKAGE_FILES = {
    "demand-flexibility-service.json",
    "demand-flexibility-service-live-events.json",
    "demand-flexibility-service-test-events.json",
}

AS3B_PACKAGE_FILES = {
    "eac-mock-auction-results.json",
    "obligatory-reactive-power-service-orps-utilisation.json",
    "obp-non-bm-physical-notifications.json",
    "obp-non-bm-reserve-instructions.json",
    "obp-reserve-availability-utilisation-price.json",
}

AS2_PACKAGE_FILES = {
    "ancillary-services-important-industry-notifications.json",
    "slow-reserve-requirement-forecast.json",
    "static-firm-frequency-response-auction-results.json",
    "static-firm-frequency-response-requirement.json",
    "stor-windows.json",
}

X_RESOURCES_WITH_CHILDREN = 4 + 1 + 39 + 73 + 1
"""Unit X's committed inventories (ADR-037 P-8): CMP workbooks, the ResultSummary ZIP,
frequency ZIPs, held data containers and the GSP archive with a CSV member."""


class TestSeededRegistry:
    """T-B1-3: the defaults keep every seeded package valid; only the pilot and X record."""

    def test_every_seeded_package_loads_with_no_record(self) -> None:
        loaded = registry_module.load_registry()
        entries = [entry for _package, entry in loaded.families.values()]
        assert len(entries) == 321
        assert sum(entry.legacy for entry in entries) == 3
        assert sum(entry.kind == "files" for entry in entries) == 35
        recorded = {entry.key for entry in entries if entry.record is not None}
        assert recorded == (
            PILOT_RECORDED
            | X_RECORDED
            | EF_RECORDED
            | CI_RECORDED
            | DEM1_RECORDED
            | DEM1H_RECORDED
            | DEM2_RECORDED
            | GEN1_RECORDED
            | GEN2_RECORDED
            | GEN2H_RECORDED
            | IC1_RECORDED
            | IC2_RECORDED
            | SC_RECORDED
            | SCN1A_RECORDED
            | SCN1B_RECORDED
            | SCN1C_RECORDED
            | SCN1D_RECORDED
            | CON_RECORDED
            | SYS1_RECORDED
            | BAL1_RECORDED
            | BAL2A_RECORDED
            | BAL2B_RECORDED
            | BAL2C_RECORDED
            | TRD_RECORDED
            | AS1_RECORDED
            | AS2_RECORDED
            | AS3A_RECORDED
            | AS3B_RECORDED
            | AS4A_RECORDED
            | AS4B_RECORDED
            | AS5A_RECORDED
            | AS5B_RECORDED
            | AS5C_RECORDED
            | DFSA_RECORDED
            | DFSB_RECORDED
            | SCN2A_RECORDED
            | SCN2B_RECORDED
            | SCN3_RECORDED
            | FILE_RECORDED
        )
        with_children = [res for _package, res in loaded.resources.values() if res.children]
        assert len(with_children) == X_RESOURCES_WITH_CHILDREN

    def test_no_seeded_file_carries_a_b_field(self) -> None:
        """Only the pilot's and X's package files name ``record``."""
        from importlib import resources as importlib_resources

        root = importlib_resources.files(registry_module.__name__)
        with_record: set[str] = set()
        for item in root.iterdir():
            if not item.name.endswith(".json") or item.name.startswith("_"):
                continue
            document = json.loads(item.read_text(encoding="utf-8"))
            if any("record" in fam for fam in document["families"]):
                with_record.add(item.name)
        assert with_record == (
            PILOT_PACKAGE_FILES
            | X_PACKAGE_FILES
            | EF_PACKAGE_FILES
            | CI_PACKAGE_FILES
            | DEM1_PACKAGE_FILES
            | DEM1H_PACKAGE_FILES
            | DEM2_PACKAGE_FILES
            | GEN1_PACKAGE_FILES
            | GEN2_PACKAGE_FILES
            | GEN2H_PACKAGE_FILES
            | IC1_PACKAGE_FILES
            | IC2_PACKAGE_FILES
            | SC_PACKAGE_FILES
            | SCN1A_PACKAGE_FILES
            | SCN1B_PACKAGE_FILES
            | SCN1C_PACKAGE_FILES
            | SCN1D_PACKAGE_FILES
            | CON_PACKAGE_FILES
            | SYS1_PACKAGE_FILES
            | BAL1_PACKAGE_FILES
            | BAL2A_PACKAGE_FILES
            | BAL2B_PACKAGE_FILES
            | BAL2C_PACKAGE_FILES
            | TRD_PACKAGE_FILES
            | AS1_PACKAGE_FILES
            | AS2_PACKAGE_FILES
            | AS3A_PACKAGE_FILES
            | AS3B_PACKAGE_FILES
            | AS4A_PACKAGE_FILES
            | AS4B_PACKAGE_FILES
            | AS5A_PACKAGE_FILES
            | AS5B_PACKAGE_FILES
            | DFSA_PACKAGE_FILES
            | DFSB_PACKAGE_FILES
            | SCN2A_PACKAGE_FILES
            | SCN2B_PACKAGE_FILES
            | SCN3_PACKAGE_FILES
            | FILE_PACKAGE_FILES
        )


class TestDumpVintageRule:
    """T-D3-3 (ADR-035 P-10): a family holding a dump takes ``capture_fallback`` only."""

    @staticmethod
    def _evidenced() -> dict[str, Any]:
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        return record(
            epochs=[epoch(sp_columns(), issue=issue)],
            entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
            vintage="issue_time_evidenced",
            vintage_evidence="the filename token is the vendor issue instant",
        )

    def test_d3_3_datastore_family_with_issue_time_evidenced_is_refused(
        self, tmp_path: Path
    ) -> None:
        """Detects a dump family loading with a vendor clock nothing evidences (red on master)."""
        resources = [resource(R1, "Series", "gen_series", url_type="datastore")]
        message = _refused(tmp_path, "V-8", self._evidenced(), resources=resources)
        assert "capture_fallback" in message and "ADR-035" in message

    def test_d3_3_controls_load(self, tmp_path: Path) -> None:
        """The same record loads on an upload family; ``capture_fallback`` loads on a dump."""
        assert _load(tmp_path / "a", self._evidenced()).families["gen_series"][1].record
        resources = [resource(R1, "Series", "gen_series", url_type="datastore")]
        loaded = _load(tmp_path / "b", record(vintage="capture_fallback"), resources=resources)
        assert loaded.families["gen_series"][1].record is not None


class TestReaderSpecs:
    """V-14 (ADR-037 P-2): the reader specs match the reader; E's records load unchanged."""

    XLSX = {"header_row": 8, "columns": "A:J"}

    HELD = [resource(R1, "Series", "gen_series", disposition={"kind": "DOC"})]
    """A container record's family holds no childless SILVER resource (V-15)."""

    def test_controls_load(self, tmp_path: Path) -> None:
        loaded = _load(tmp_path / "a", record(reader="xlsx", xlsx=self.XLSX), resources=self.HELD)
        rec = loaded.families["gen_series"][1].record
        assert rec is not None and rec.xlsx is not None
        assert rec.xlsx.bounds == (1, 10)
        csv_member = {"member_pattern": r"[^/]+\.csv", "inner": "csv"}
        rec1 = record(reader="zip_member", zip_member=csv_member)
        assert _load(tmp_path / "b", rec1, resources=self.HELD)
        xlsx_member = {"member_pattern": ".+", "inner": "xlsx"}
        rec2 = record(reader="zip_member", zip_member=xlsx_member, xlsx=self.XLSX)
        assert _load(tmp_path / "c", rec2, resources=self.HELD)
        last = {"header_row": 2, "columns": "A:AA", "last_row": 3}
        assert _load(tmp_path / "d", record(reader="xlsx", xlsx=last), resources=self.HELD)

    @pytest.mark.parametrize(
        ("kwargs", "label"),
        [
            ({"reader": "csv", "xlsx": {"header_row": 1, "columns": "A:B"}}, "csv+xlsx"),
            (
                {"reader": "csv", "zip_member": {"member_pattern": ".+", "inner": "csv"}},
                "csv+zip",
            ),
            ({"reader": "xlsx"}, "xlsx without spec"),
            (
                {
                    "reader": "xlsx",
                    "xlsx": {"header_row": 1, "columns": "A:B"},
                    "zip_member": {"member_pattern": ".+", "inner": "xlsx"},
                },
                "xlsx+zip",
            ),
            (
                {
                    "reader": "zip_member",
                    "zip_member": {"member_pattern": ".+", "inner": "xlsx"},
                },
                "inner xlsx without xlsx",
            ),
            (
                {
                    "reader": "zip_member",
                    "zip_member": {"member_pattern": ".+", "inner": "csv"},
                    "xlsx": {"header_row": 1, "columns": "A:B"},
                },
                "inner csv with xlsx",
            ),
            (
                {"reader": "xlsx", "xlsx": {"header_row": 3, "columns": "A:B", "last_row": 3}},
                "last_row not after header_row",
            ),
        ],
    )
    def test_v14_refusals(self, tmp_path: Path, kwargs: dict[str, Any], label: str) -> None:
        _refused(tmp_path, "V-14", record(**kwargs))

    def test_zip_member_without_spec_is_refused(self, tmp_path: Path) -> None:
        rec = record(reader="zip_member")
        del rec["zip_member"]
        _refused(tmp_path, "V-14", rec)

    @pytest.mark.parametrize(
        "spec",
        [
            {"header_row": 0, "columns": "A:B"},
            {"header_row": 1, "columns": "a:b"},
            {"header_row": 1, "columns": "A"},
            {"header_row": 1, "columns": "AAAA:B"},
            {"header_row": 1, "columns": "C:B"},
        ],
    )
    def test_malformed_xlsx_specs_are_rejected(self, tmp_path: Path, spec: dict[str, Any]) -> None:
        with pytest.raises(RegistryError, match="xlsx"):
            _load(tmp_path, record(reader="xlsx", xlsx=spec), resources=self.HELD)

    def test_member_pattern_must_compile(self, tmp_path: Path) -> None:
        spec = {"member_pattern": "(", "inner": "csv"}
        with pytest.raises(RegistryError, match="member_pattern does not compile"):
            _load(tmp_path, record(reader="zip_member", zip_member=spec))


class TestXlsxBlocks:
    """V-20 / V-14 (ADR-037 amendment 1, AS-5c): composite blocks on reader xlsx only (A-12)."""

    BLOCK = {
        "resource_id": R1,
        "sheet": "TR121 ",
        "header_first": 2,
        "header_last": 4,
        "data_first": 7,
        "data_last": 121,
        "columns": "A:AP",
    }

    HELD = TestReaderSpecs.HELD

    def _blocks(self, *blocks: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        rec = record(reader=kwargs.pop("reader", "xlsx"), **kwargs)
        rec["xlsx_blocks"] = list(blocks)
        return rec

    def test_controls_load(self, tmp_path: Path) -> None:
        """Detects V-20 or V-14 refusing a valid block map: one or two blocks on reader xlsx load,
        the sheet name keeps its trailing space, and bounds parse like ``XlsxSpec``."""
        second = {**self.BLOCK, "resource_id": R2, "sheet": "Sheet1"}
        loaded = _load(tmp_path, self._blocks(self.BLOCK, second), resources=self.HELD)
        rec = loaded.families["gen_series"][1].record
        assert rec is not None and rec.xlsx is None and rec.xlsx_blocks is not None
        assert [b.resource_id for b in rec.xlsx_blocks] == [R1, R2]
        assert rec.xlsx_blocks[0].sheet == "TR121 "
        assert rec.xlsx_blocks[0].bounds == (1, 42)
        single = {**self.BLOCK, "header_last": 2, "data_first": 3, "data_last": 3}
        assert _load(tmp_path / "b", self._blocks(single), resources=self.HELD)

    def test_absent_blocks_are_dropped_from_the_dump(self) -> None:
        """Detects ``xlsx_blocks`` leaking into an existing record's ``exclude_none`` dump (the
        equivalence fingerprint and the byte-unchanged pins read it)."""
        rec = SchemaRecord.model_validate(record(reader="xlsx", xlsx=TestReaderSpecs.XLSX))
        assert rec.xlsx_blocks is None
        assert "xlsx_blocks" not in rec.model_dump(mode="json", exclude_none=True)

    @pytest.mark.parametrize(
        ("kwargs", "label"),
        [
            ({"reader": "csv"}, "blocks on csv"),
            (
                {"reader": "zip_member", "zip_member": {"member_pattern": ".+", "inner": "csv"}},
                "blocks on zip_member csv",
            ),
            (
                {
                    "reader": "zip_member",
                    "zip_member": {"member_pattern": ".+", "inner": "xlsx"},
                    "xlsx": {"header_row": 1, "columns": "A:B"},
                },
                "blocks on zip_member xlsx",
            ),
            ({"xlsx": {"header_row": 1, "columns": "A:B"}}, "blocks with xlsx"),
        ],
    )
    def test_v20_refusals(self, tmp_path: Path, kwargs: dict[str, Any], label: str) -> None:
        """Detects a block map accepted beside another reader spec or on another reader."""
        _refused(tmp_path, "V-20", self._blocks(self.BLOCK, **kwargs), resources=self.HELD)

    def test_v20_empty_map_is_refused(self, tmp_path: Path) -> None:
        """Detects an empty block map standing in for 'no blocks' (every capture would fail)."""
        _refused(tmp_path, "V-20", self._blocks(), resources=self.HELD)

    def test_v20_repeated_resource_is_refused(self, tmp_path: Path) -> None:
        """Detects two blocks for one resource (the reader would pick one silently)."""
        twice = {**self.BLOCK, "sheet": "Other"}
        _refused(tmp_path, "V-20", self._blocks(self.BLOCK, twice), resources=self.HELD)

    def test_reader_xlsx_with_neither_spec_is_v14(self, tmp_path: Path) -> None:
        """Detects V-14 accepting reader xlsx with neither an xlsx spec nor a block map."""
        message = _refused(tmp_path, "V-14", record(reader="xlsx"), resources=self.HELD)
        assert "xlsx_blocks" in message

    @pytest.mark.parametrize(
        "change",
        [
            {"header_last": 1},
            {"data_first": 4},
            {"data_last": 6},
            {"header_first": 0, "header_last": 0},
            {"columns": "a:b"},
            {"columns": "C:B"},
            {"columns": "A"},
            {"sheet": ""},
            {"resource_id": ""},
        ],
    )
    def test_malformed_blocks_are_rejected(self, tmp_path: Path, change: dict[str, Any]) -> None:
        """Detects a block whose rows are out of order, whose column range does not parse or
        whose resource or sheet is empty passing model validation."""
        with pytest.raises(RegistryError, match="xlsx"):
            _load(tmp_path, self._blocks({**self.BLOCK, **change}), resources=self.HELD)
