"""Completion records for the three bespoke NESO transformers (ADR-034 P-8; T-B8-5..7).

The bespoke modules stay byte-unchanged; a post-run hook records each output
that passes the one validity predicate, and the drain's per-capture step adopts
a valid output or re-transforms the body exactly as the per-file branch does.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import assert_same_output, install_generated, snapshot, write_capture
from _neso_registry_support import (
    family,
    ingest_context,
    package,
    record,
    resource,
)

from gridflow.pipeline import runner as pipeline_runner
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    bespoke_versions,
    is_valid,
    read_completion,
    read_failure,
    record_bespoke_completions,
    run_bespoke_capture,
)
from gridflow.silver.neso_data_portal.daily_wind_availability import (
    DailyWindAvailabilityTransformer,
)
from gridflow.silver.neso_data_portal.embedded_wind_solar_forecast import (
    EmbeddedWindSolarForecastTransformer,
)
from gridflow.silver.neso_data_portal.historic_generation_mix import (
    HistoricGenerationMixTransformer,
)
from gridflow.silver.registry import get_transformer_class, list_transformers

if TYPE_CHECKING:
    from gridflow.silver.base import BaseSilverTransformer

DWA = "daily_wind_availability"
DWA_PKG = "3758a0ed-6c96-4e36-88d0-107f5020ddf3"
DWA_RESOURCE = "7aa508eb-36f5-4298-820f-2fa6745ae2e7"
FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "neso_data_portal"
    / "daily_wind_availability.csv"
)
DAY = date(2026, 8, 16)


@pytest.fixture
def data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("b")


def _t(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 16, hour, minute, tzinfo=UTC)


def _dwa(
    data: Path, written: datetime, body: bytes | None = None, *, rid: str = DWA_RESOURCE
) -> Path:
    path, _sidecar = write_capture(
        data,
        DWA,
        package_slug="daily-wind-availability",
        package_id=DWA_PKG,
        resource_id=rid,
        resource_name="Daily Wind Availability",
        body=body if body is not None else FIXTURE.read_bytes(),
        written_at=written,
        ckan_last_modified=written.replace(tzinfo=None).isoformat(),
        partition=DAY,
    )
    return path


def _outputs(data: Path) -> list[Path]:
    return sorted((data / "silver" / "neso_data_portal" / DWA).rglob("[!.]*.parquet"))


def _capture_id(data: Path, body: Path) -> str:
    return body.relative_to(data).as_posix()


_WIRING_SCRIPT = """
import json
from gridflow.pipeline import runner
from gridflow.silver.registry import get_transformer_class, post_run_hooks

runner.import_transformers()
out = {}
for key in (
    "daily_wind_availability", "historic_generation_mix", "embedded_wind_solar_forecast"
):
    cls = get_transformer_class("neso_data_portal", key)
    hooks = post_run_hooks("neso_data_portal", key)
    out[key] = {
        "cls": None if cls is None else f"{cls.__module__}.{cls.__qualname__}",
        "hooks": [f"{h.__module__}.{h.__qualname__}" for h in hooks],
    }
print(json.dumps(out))
"""


def _qualified(obj: Any) -> str:
    return f"{obj.__module__}.{obj.__qualname__}"


class TestIdentityPins:
    def test_c_5_the_three_keep_their_bespoke_classes_and_gain_a_hook(self) -> None:
        """C-5: no wrapper class; completion arrives through a hook only.

        Detects the runner's bootstrap no longer registering the NESO
        transformers or their hooks. Runs in a fresh interpreter, because this
        module's own imports already register both and would mask a broken
        ``import_transformers`` (REVIEW-DIFF-1 tests #2)."""
        result = subprocess.run(
            [sys.executable, "-c", _WIRING_SCRIPT],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        wiring = json.loads(result.stdout.strip().splitlines()[-1])
        hook = _qualified(record_bespoke_completions)
        for key, cls in (
            (DWA, DailyWindAvailabilityTransformer),
            ("historic_generation_mix", HistoricGenerationMixTransformer),
            ("embedded_wind_solar_forecast", EmbeddedWindSolarForecastTransformer),
        ):
            assert wiring[key] == {"cls": _qualified(cls), "hooks": [hook]}, key


class TestHook:
    """T-B8-5: the bespoke hook on the DWA fixture."""

    def test_run_transform_records_one_populated_completion(
        self, data: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = _dwa(data, _t(18, 25))
        pipeline_runner.import_transformers()
        day = datetime(2026, 8, 16, tzinfo=UTC)
        with ingest_context(data, monkeypatch) as ctx:
            (result,) = pipeline_runner.run_transform(ctx, "neso_data_portal", [DWA], day, day)
        assert result.status == "success", result.error
        ledger = read_completion(data, DWA, _capture_id(data, body))
        assert ledger is not None
        assert ledger["outcome"] == "populated" and ledger["engine_version"] == "bespoke"
        assert ledger["row_count"] == 6
        assert ledger["output_path"] == _outputs(data)[0].relative_to(data).as_posix()

    def test_no_record_when_the_output_is_absent(self, data: Path) -> None:
        body = _dwa(data, _t(18, 25))
        record_bespoke_completions(DailyWindAvailabilityTransformer(data), DAY)
        assert read_completion(data, DWA, _capture_id(data, body)) is None

    def test_neither_capture_on_a_stamp_collision(self, data: Path) -> None:
        """FM-11: two captures with one ``written_at`` share one bespoke path."""
        first = _dwa(data, _t(18, 25))
        second = _dwa(
            data,
            _t(18, 25),
            FIXTURE.read_bytes().replace(b"120.5", b"121.5"),
            rid="7aa508eb-36f5-4298-820f-2fa6745ae2e8",
        )
        transformer = DailyWindAvailabilityTransformer(data)
        transformer.run(DAY, run_id="r")
        assert len(_outputs(data)) == 1
        record_bespoke_completions(transformer, DAY)
        assert read_completion(data, DWA, _capture_id(data, first)) is None
        assert read_completion(data, DWA, _capture_id(data, second)) is None


class TestDrainStep:
    """T-B8-6: FM-16, FM-9 and FM-15 through ``run_bespoke_capture``."""

    def test_adopts_current_outputs_and_transforms_absent_ones(self, data: Path) -> None:
        """FM-16: the bespoke run wrote both bodies, then raised before its hook."""
        first = _dwa(data, _t(9))
        second = _dwa(data, _t(18, 25), FIXTURE.read_bytes().replace(b"120.5", b"121.5"))
        transformer = DailyWindAvailabilityTransformer(data)
        transformer.run(DAY, run_id="r")
        kept, lost = _outputs(data)
        mtime = kept.stat().st_mtime_ns
        lost.unlink()
        run_bespoke_capture(DailyWindAvailabilityTransformer(data), first, "drain-1")
        run_bespoke_capture(DailyWindAvailabilityTransformer(data), second, "drain-1")
        assert kept.stat().st_mtime_ns == mtime  # adopted, not rewritten
        assert lost.is_file()  # transformed
        versions = bespoke_versions(DailyWindAvailabilityTransformer)
        for body in (first, second):
            ledger = read_completion(data, DWA, _capture_id(data, body))
            assert ledger is not None and is_valid(ledger, data, versions)

    def test_an_older_dataset_version_is_rewritten_then_recorded(self, data: Path) -> None:
        """FM-9: a planted output with an old ``dataset_version`` is never adopted."""
        body = _dwa(data, _t(18, 25))
        transformer = DailyWindAvailabilityTransformer(data)
        transformer.run(DAY, run_id="r")
        (output,) = _outputs(data)
        stale = pl.read_parquet(output, hive_partitioning=False).with_columns(
            pl.lit("0.9.0").alias("dataset_version")
        )
        stale.write_parquet(output)
        before = output.stat().st_mtime_ns
        run_bespoke_capture(DailyWindAvailabilityTransformer(data), body, "drain-1")
        assert output.stat().st_mtime_ns != before
        stored = pl.read_parquet(output, hive_partitioning=False)["dataset_version"].unique()
        assert stored.to_list() == [DailyWindAvailabilityTransformer.DATASET_VERSION]
        assert read_completion(data, DWA, _capture_id(data, body)) is not None

    def test_a_raising_capture_fails_alone(self, data: Path) -> None:
        """FM-15 at the capture: a failure record, NesoCaptureFailedError, and the
        other capture of the date is still written."""
        bad = _dwa(data, _t(9), b"BMU_ID,Date,MW\nX,not-a-date,1\n")
        good = _dwa(data, _t(18, 25))
        with pytest.raises(NesoCaptureFailedError):
            run_bespoke_capture(DailyWindAvailabilityTransformer(data), bad, "drain-1")
        run_bespoke_capture(DailyWindAvailabilityTransformer(data), good, "drain-1")
        assert read_failure(data, DWA, _capture_id(data, bad)) is not None
        assert read_completion(data, DWA, _capture_id(data, bad)) is None
        assert read_completion(data, DWA, _capture_id(data, good)) is not None

    def test_b7_the_drain_step_equals_base_run(self, data: Path) -> None:
        """B7: P-8's per-capture transform equals ``base.run()`` for that body."""
        on_time = data / "on_time"
        late = data / "late"
        body = _dwa(on_time, _t(18, 25))
        shutil.copytree(on_time / "bronze", late / "bronze")
        transformer = DailyWindAvailabilityTransformer(on_time)
        transformer.run(DAY, run_id="on-time")
        record_bespoke_completions(transformer, DAY)
        late_body = late / body.relative_to(on_time)
        run_bespoke_capture(DailyWindAvailabilityTransformer(late), late_body, "drain-late")
        assert_same_output(
            snapshot(on_time, DWA),
            snapshot(late, DWA),
            DailyWindAvailabilityTransformer.ENTITY_KEY_COLUMNS,
        )


class TestNoPartitionColumn:
    def test_t_b8_7_no_neso_class_sets_partition_date_column(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-B8-7: neither the real registry's classes nor a generated family."""
        pipeline_runner.import_transformers()
        document = package(
            "pkg-gen",
            "dddddddd-0000-4000-8000-000000000000",
            [family("gen", record=record())],
            [resource("dddddddd-0000-4000-8000-000000000001", "Series", "gen")],
        )
        _registry, generated = install_generated(monkeypatch, tmp_path / "reg", [document])
        classes: list[type[BaseSilverTransformer]] = [
            cls
            for key in list_transformers("neso_data_portal")
            if (cls := get_transformer_class(*key)) is not None
        ]
        assert generated.transformers["gen"] in classes
        assert all(cls.PARTITION_DATE_COLUMN is None for cls in classes)
