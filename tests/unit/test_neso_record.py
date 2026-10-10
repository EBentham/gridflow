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
        ],
    )
    def test_malformed_column_specs_are_rejected(self, spec: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            ColumnSpec.model_validate(spec)

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

X_RESOURCES_WITH_CHILDREN = 4 + 1 + 39 + 73 + 1
"""Unit X's committed inventories (ADR-037 P-8): CMP workbooks, the ResultSummary ZIP,
frequency ZIPs, held data containers and the GSP archive with a CSV member."""


class TestSeededRegistry:
    """T-B1-3: the defaults keep every seeded package valid; only the pilot and X record."""

    def test_every_seeded_package_loads_with_no_record(self) -> None:
        loaded = registry_module.load_registry()
        entries = [entry for _package, entry in loaded.families.values()]
        assert len(entries) == 314
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
