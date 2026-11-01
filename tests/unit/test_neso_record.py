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

    def test_v9_accepts_a_record_on_a_legacy_family(self) -> None:
        """Detects V-9 still refusing a legacy key's record (ADR-034 P-8 superseded):
        ``legacy`` now governs fetch-time admission only."""
        validate_record(
            SchemaRecord.model_validate(record()),
            key="daily_wind_availability",
            kind="tabular",
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


LEGACY_FAMILIES = {
    "daily_wind_availability",
    "embedded_wind_solar_forecast",
    "historic_generation_mix",
}
"""The three keys onboarded before the registry, on frozen records since ADR-034 P-8 was
superseded. A closed set: no family is onboarded as legacy any more."""


class TestSeededRegistry:
    """T-B1-3, read from the committed registry rather than typed out per batch: every
    seeded package loads, a family carries a record exactly when its package file names
    one, a recorded family is a silver dataset and every other family is ingest-only."""

    def test_a_family_has_a_record_exactly_when_its_package_file_names_one(self) -> None:
        from importlib import resources as importlib_resources

        loaded = registry_module.load_registry()
        root = importlib_resources.files(registry_module.__name__)
        named: dict[str, bool] = {}
        for item in root.iterdir():
            if not item.name.endswith(".json") or item.name.startswith("_"):
                continue
            document = json.loads(item.read_text(encoding="utf-8"))
            named.update({fam["key"]: "record" in fam for fam in document["families"]})
        assert named.keys() == loaded.families.keys()
        recorded = {key for key, (_pkg, fam) in loaded.families.items() if fam.record is not None}
        assert recorded == {key for key, has_record in named.items() if has_record}

    def test_recorded_families_are_silver_datasets_and_the_rest_are_ingest_only(self) -> None:
        """Fails on a registry family with no silver class and no ingest-only reason, and
        on a silver class registered under the source with no family behind it."""
        import gridflow.silver.neso_data_portal  # noqa: F401  (registers the generated set)
        from gridflow.silver.latest_views import LATEST_VIEW_SPECS
        from gridflow.silver.registry import (
            get_transformer_class,
            ingest_only_reason,
            list_transformers,
        )

        source = "neso_data_portal"
        families = registry_module.load_registry().families
        recorded = {key for key, (_pkg, fam) in families.items() if fam.record is not None}
        assert {dataset for _source, dataset in list_transformers(source)} == recorded
        for key in sorted(families):
            if key in recorded:
                transformer_cls = get_transformer_class(source, key)
                assert transformer_cls is not None and transformer_cls.APPEND_ONLY, key
                assert (source, key) in LATEST_VIEW_SPECS, key
                assert ingest_only_reason(source, key) is None, key
            else:
                assert ingest_only_reason(source, key) is not None, key

    def test_spot_checks(self) -> None:
        """Named recorded families from three batches, an ingest-only tabular family and a
        catalogue-only files family."""
        families = registry_module.load_registry().families
        assert {key for key, (_pkg, fam) in families.items() if fam.legacy} == LEGACY_FAMILIES
        for key in ("tec_register", "current_bsuos_cap_adjustments", "system_frequency_gmt_csv"):
            assert families[key][1].record is not None, key
        assert families["bsuos_daily_forecast"][1].record is None
        assert families["bs_contract_enactment_files"][1].kind == "files"
        assert families["bs_contract_enactment_files"][1].record is None


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
