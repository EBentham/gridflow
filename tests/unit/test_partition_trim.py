"""Regression coverage for dataset-declared partition ownership covering sets."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl
import pytest

import gridflow.silver.elexon  # noqa: F401 - populate the registry
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.registry import get_transformer_class, list_transformers
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path


DESTINATION = date(2024, 1, 15)
PREDECESSOR = DESTINATION - timedelta(days=1)
SUCCESSOR = DESTINATION + timedelta(days=1)
FIXED_NOW = datetime(2026, 9, 7, 12, tzinfo=UTC)


class _Clock(datetime):
    @classmethod
    def now(cls, tz: object = None) -> datetime:
        return FIXED_NOW


def _mid_row(
    owner: date,
    period: int,
    provider: str,
    price: float,
) -> dict[str, object]:
    return {
        "settlementDate": owner.isoformat(),
        "settlementPeriod": period,
        "dataProvider": provider,
        "price": price,
        "volume": 1.0,
    }


def _mid_inputs() -> dict[date, pl.DataFrame]:
    return {
        PREDECESSOR: pl.DataFrame(
            [
                _mid_row(PREDECESSOR, 1, "P", 10.0),
                _mid_row(DESTINATION, 1, "P", 11.0),
                _mid_row(DESTINATION, 1, "Q", 12.0),
            ]
        ),
        DESTINATION: pl.DataFrame(
            [
                _mid_row(DESTINATION, 1, "P", 21.0),
                _mid_row(DESTINATION, 2, "P", 22.0),
            ]
        ),
    }


def _fixed_mid(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    inputs: dict[date, pl.DataFrame],
) -> MIDTransformer:
    monkeypatch.setattr("gridflow.silver.base.datetime", _Clock)
    monkeypatch.setattr("gridflow.silver.elexon.mid.datetime", _Clock)
    transformer = MIDTransformer(root)
    monkeypatch.setattr(
        transformer,
        "read_bronze",
        lambda source_date: inputs.get(source_date, pl.DataFrame()),
    )
    monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)
    return transformer


@pytest.mark.parametrize("order", [(PREDECESSOR, DESTINATION), (DESTINATION, PREDECESSOR)])
def test_p_t01_p_t07_order_independent_complete_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    order: tuple[date, date],
) -> None:
    """D wins one overlap while providers survive and destination order is irrelevant."""
    root = tmp_path / f"root-{order[0]}"
    transformer = _fixed_mid(root, monkeypatch, _mid_inputs())

    for destination in order:
        transformer.run(destination, run_id=f"run-{destination}")

    paths = PathBuilder(root)
    predecessor_bytes = paths.silver_file("elexon", "mid", PREDECESSOR).read_bytes()
    destination_path = paths.silver_file("elexon", "mid", DESTINATION)
    destination_bytes = destination_path.read_bytes()
    frame = pl.read_parquet(destination_path)
    assert frame.select("settlement_period", "data_provider_id").rows() == [
        (1, "P"),
        (1, "Q"),
        (2, "P"),
    ]
    assert (
        frame.filter((pl.col("settlement_period") == 1) & (pl.col("data_provider_id") == "P"))[
            "market_index_price"
        ].item()
        == 21.0
    )

    parity_root = tmp_path / f"parity-{order[0]}"
    parity = _fixed_mid(parity_root, monkeypatch, _mid_inputs())
    for destination in reversed(order):
        parity.run(destination, run_id=f"run-{destination}")
    parity_paths = PathBuilder(parity_root)
    assert parity_paths.silver_file("elexon", "mid", PREDECESSOR).read_bytes() == predecessor_bytes
    assert parity_paths.silver_file("elexon", "mid", DESTINATION).read_bytes() == destination_bytes


def test_p_t07_fuelhh_successor_is_lowest_collision_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concat [D+1,D-1,D] makes D win, then D-1, with D+1 recovery-only."""
    monkeypatch.setattr("gridflow.silver.base.datetime", _Clock)
    monkeypatch.setattr("gridflow.silver.elexon.fuelhh.datetime", _Clock)

    def row(generation: float) -> dict[str, object]:
        return {
            "settlementDate": DESTINATION.isoformat(),
            "settlementPeriod": 1,
            "startTime": "2024-01-15T00:00:00Z",
            "publishTime": "2024-01-15T00:30:00Z",
            "fuelType": "CCGT",
            "generation": generation,
        }

    for label, values, expected in (
        ("all", {SUCCESSOR: 1.0, PREDECESSOR: 2.0, DESTINATION: 3.0}, 3.0),
        ("no-own", {SUCCESSOR: 1.0, PREDECESSOR: 2.0}, 2.0),
        ("successor-only", {SUCCESSOR: 1.0}, 1.0),
    ):
        transformer = FuelHHTransformer(tmp_path / label)
        monkeypatch.setattr(
            transformer,
            "read_bronze",
            lambda source_date, values=values: (
                pl.DataFrame([row(values[source_date])])
                if source_date in values
                else pl.DataFrame()
            ),
        )
        monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)
        assert transformer.run(DESTINATION, run_id="collision") == 1
        persisted = pl.read_parquet(
            PathBuilder(tmp_path / label).silver_file("elexon", "fuelhh", DESTINATION)
        )
        assert persisted["generation_mw"].item() == expected


@pytest.mark.parametrize("own", [None, pl.DataFrame()])
def test_p_t03_missing_or_empty_own_recovers_neighbour(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    own: pl.DataFrame | None,
) -> None:
    inputs = {PREDECESSOR: pl.DataFrame([_mid_row(DESTINATION, 1, "P", 10.0)])}
    if own is not None:
        inputs[DESTINATION] = own
    transformer = _fixed_mid(tmp_path, monkeypatch, inputs)

    assert transformer.run(DESTINATION, run_id="recover") == 1
    persisted = pl.read_parquet(PathBuilder(tmp_path).silver_file("elexon", "mid", DESTINATION))
    assert persisted["settlement_date"].to_list() == [DESTINATION]
    assert (persisted["available_at"] >= persisted["event_time"]).all()


def test_p_t04_p_t05_missing_predecessor_and_both_empty_reset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[date] = []
    inputs = {DESTINATION: pl.DataFrame([_mid_row(DESTINATION, 1, "P", 10.0)])}
    transformer = _fixed_mid(tmp_path, monkeypatch, inputs)
    monkeypatch.setattr(
        transformer,
        "read_bronze",
        lambda source_date: calls.append(source_date) or inputs.get(source_date, pl.DataFrame()),
    )
    assert transformer.run(DESTINATION, run_id="own") == 1
    assert calls == [PREDECESSOR, DESTINATION]
    assert transformer.last_partition_filter_unresolved_count == 0

    transformer.last_partition_trimmed_count = 9
    calls.clear()
    inputs.clear()
    monkeypatch.setattr(
        transformer,
        "_source_window_plan",
        lambda _source_date: pytest.fail("empty inputs must not resolve windows"),
    )
    with caplog.at_level("WARNING", logger="gridflow.silver.base"):
        assert transformer.run(DESTINATION, run_id="empty") == 0
    assert calls == [PREDECESSOR, DESTINATION]
    assert f"No bronze data for elexon/mid on {DESTINATION}" in caplog.messages
    assert transformer.last_partition_trimmed_count == 0
    assert transformer.last_partition_filter_unresolved_count == 0


def test_p_t08_declared_source_sets_are_exact_and_ordered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fuel_dates: list[date] = []
    fuelhh = FuelHHTransformer(tmp_path / "fuelhh")
    monkeypatch.setattr(
        fuelhh,
        "read_bronze",
        lambda source_date: fuel_dates.append(source_date) or pl.DataFrame(),
    )
    assert fuelhh.run(DESTINATION, run_id="fuel-bound") == 0
    assert fuel_dates == [SUCCESSOR, PREDECESSOR, DESTINATION]

    system_price_dates: list[date] = []
    original_bronze_date_dir = PathBuilder.bronze_date_dir

    def record_system_price_date(
        paths: PathBuilder,
        source: str,
        dataset: str,
        target_date: date,
        *,
        dataset_dir: Path | None = None,
    ) -> Path:
        if (source, dataset) == ("elexon", "system_prices"):
            system_price_dates.append(target_date)
        return original_bronze_date_dir(
            paths,
            source,
            dataset,
            target_date,
            dataset_dir=dataset_dir,
        )

    monkeypatch.setattr(PathBuilder, "bronze_date_dir", record_system_price_date)
    system_prices = SystemPriceTransformer(tmp_path / "system-prices")
    assert system_prices.run(DESTINATION, run_id="prices-bound") == 0
    assert system_price_dates == [DESTINATION]
    assert MIDTransformer.PARTITION_SOURCE_OFFSETS == (-1, 0)
    assert FuelHHTransformer.PARTITION_SOURCE_OFFSETS == (1, -1, 0)
    assert SystemPriceTransformer.PARTITION_SOURCE_OFFSETS == (0,)


def _write_sidecar(root: Path, source_date: date, stamp: object) -> None:
    directory = PathBuilder(root).bronze_date_dir("elexon", "mid", source_date)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "raw_probe.meta.json").write_text(json.dumps({"written_at": stamp}))


def test_p_t11_combined_availability_and_sidecar_less_own_rule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _write_sidecar(tmp_path, PREDECESSOR, "2024-01-15T02:00:00+00:00")
    inputs = {
        PREDECESSOR: pl.DataFrame([_mid_row(DESTINATION, 1, "P", 10.0)]),
        DESTINATION: pl.DataFrame([_mid_row(DESTINATION, 2, "P", 20.0)]),
    }
    transformer = _fixed_mid(tmp_path, monkeypatch, inputs)

    with caplog.at_level("WARNING"):
        transformer.run(DESTINATION, run_id="fallback", reingest=True)
    frame = pl.read_parquet(PathBuilder(tmp_path).silver_file("elexon", "mid", DESTINATION))
    assert "sidecar-less-own fallback" in caplog.text
    assert frame["vintage_policy"].unique().to_list() == ["elexon-mid/vp-2026-09b"]
    assert (frame["available_at"] >= frame["event_time"]).all()

    _write_sidecar(tmp_path, DESTINATION, "2024-01-15T03:00:00+00:00")
    caplog.clear()
    transformer.run(DESTINATION, run_id="combined", reingest=True)
    assert "sidecar-less-own fallback" not in caplog.text


def test_p_t06_empty_overwrite_is_bounded_by_destination_existence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = {DESTINATION: pl.DataFrame([_mid_row(PREDECESSOR, 1, "P", 10.0)])}
    transformer = _fixed_mid(tmp_path, monkeypatch, inputs)
    destination = PathBuilder(tmp_path).silver_file("elexon", "mid", DESTINATION)

    assert transformer.run(DESTINATION, run_id="absent") == 0
    assert not destination.exists()

    destination.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"stale": [1]}).write_parquet(destination)
    assert transformer.run(DESTINATION, run_id="replace") == 0
    empty = pl.read_parquet(destination)
    assert empty.is_empty()
    assert "settlement_date" in empty.columns


def test_p_t08_registry_opt_in_is_exact() -> None:
    declarations = {
        key
        for key in list_transformers()
        if get_transformer_class(*key).PARTITION_DATE_COLUMN is not None
    }
    assert declarations == {
        ("elexon", "mid"),
        ("elexon", "fuelhh"),
        ("elexon", "system_prices"),
    }
    assert MIDTransformer.DATASET_VERSION == "1.1.0"


def test_p_t09_source_window_plans_are_source_relative(tmp_path: Path) -> None:
    """Each FUELHH source resolves its own sidecar window, independent of destination."""
    transformer = FuelHHTransformer(tmp_path)
    expected: dict[date, tuple[datetime, datetime]] = {}
    for index, source_date in enumerate((SUCCESSOR, PREDECESSOR, DESTINATION)):
        start = datetime.combine(source_date, datetime.min.time(), tzinfo=UTC) + timedelta(
            hours=index
        )
        end = start + timedelta(days=1)
        partition = PathBuilder(tmp_path).bronze_date_dir("elexon", "fuelhh", source_date)
        partition.mkdir(parents=True, exist_ok=True)
        (partition / "raw_window.json").write_text("{}")
        (partition / "raw_window.meta.json").write_text(
            json.dumps(
                {
                    "source": "elexon",
                    "dataset": "fuelhh",
                    "data_date": source_date.isoformat(),
                    "request_params": {
                        "publishDateTimeFrom": start.isoformat(),
                        "publishDateTimeTo": end.isoformat(),
                    },
                    "page": 1,
                    "total_pages": 1,
                }
            )
        )
        expected[source_date] = (start, end)

    for destination_order in ((DESTINATION, SUCCESSOR), (SUCCESSOR, DESTINATION)):
        for _destination in destination_order:
            for source_date in transformer._partition_source_dates(DESTINATION):
                plan = transformer._source_window_plan(source_date)
                assert plan is not None
                assert (plan.window.start, plan.window.end) == expected[source_date]


def test_p_t13_defective_successor_does_not_fail_healthy_own_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = {
        "settlementDate": DESTINATION.isoformat(),
        "settlementPeriod": 1,
        "startTime": "2024-01-15T00:00:00Z",
        "publishTime": "2024-01-15T00:30:00Z",
        "fuelType": "CCGT",
        "generation": 1.0,
    }
    transformer = FuelHHTransformer(tmp_path)
    monkeypatch.setattr(
        transformer,
        "read_bronze",
        lambda source_date: (
            pl.DataFrame({"defective": [1]})
            if source_date == SUCCESSOR
            else pl.DataFrame([good])
            if source_date == DESTINATION
            else pl.DataFrame()
        ),
    )
    monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)

    assert transformer.run(DESTINATION, run_id="healthy-own") == 1
    assert transformer.last_unaccounted_empty_frames == 1
    assert transformer.last_total_unaccounted_exclusion is False


def test_p_t14_ledger_counts_recoverable_unsafe_and_unclassifiable(tmp_path: Path) -> None:
    transformer = MIDTransformer(tmp_path)
    source_date = PREDECESSOR
    frame = pl.DataFrame(
        {
            "settlement_date": [
                DESTINATION,
                PREDECESSOR,
                PREDECESSOR - timedelta(days=1),
                None,
            ]
        },
        schema={"settlement_date": pl.Date},
    )
    owned = transformer._record_partition_ownership(frame, source_date, DESTINATION)
    assert owned.height == 1
    assert transformer.last_partition_trimmed_count == 3
    assert transformer.last_partition_trim_unrecoverable_count == 2

    with pytest.raises(ValueError, match="settlement_date.*missing"):
        transformer._record_partition_ownership(
            pl.DataFrame({"other": [1]}), source_date, DESTINATION
        )


@pytest.mark.parametrize(
    ("transformer_type", "source_date", "recoverable", "unsafe"),
    [
        (MIDTransformer, DESTINATION, (DESTINATION, SUCCESSOR), PREDECESSOR),
        (
            FuelHHTransformer,
            DESTINATION,
            (PREDECESSOR, DESTINATION, SUCCESSOR),
            SUCCESSOR + timedelta(days=1),
        ),
        (SystemPriceTransformer, DESTINATION, (DESTINATION,), SUCCESSOR),
    ],
)
def test_p_t14_dataset_specific_recoverability(
    tmp_path: Path,
    transformer_type: type[BaseSilverTransformer],
    source_date: date,
    recoverable: tuple[date, ...],
    unsafe: date,
) -> None:
    transformer = transformer_type(tmp_path)
    owners = (*recoverable, unsafe, None)
    frame = pl.DataFrame({"settlement_date": owners}, schema={"settlement_date": pl.Date})

    transformer._record_partition_ownership(frame, source_date, source_date, trim=False)

    expected_trimmed = sum(owner != source_date for owner in owners)
    assert transformer.last_partition_trimmed_count == expected_trimmed
    assert transformer.last_partition_trim_unrecoverable_count == 2


def test_partition_owner_missing_column_message_is_identical_at_both_call_sites(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transformer = MIDTransformer(tmp_path)
    frame = pl.DataFrame({"other": [1]})
    expected = (
        "elexon/mid: declared partition ownership column "
        "'settlement_date' is missing from the prepared frame"
    )
    helper_calls = 0

    def missing_owner_column(_frame: pl.DataFrame) -> str:
        nonlocal helper_calls
        helper_calls += 1
        raise ValueError(expected)

    monkeypatch.setattr(transformer, "_partition_owner_column", missing_owner_column)

    for select in (
        lambda: transformer._record_partition_ownership(
            frame, PREDECESSOR, DESTINATION, trim=False
        ),
        lambda: transformer._select_partition_owner(frame, DESTINATION),
    ):
        with pytest.raises(ValueError) as exc_info:
            select()
        assert str(exc_info.value) == expected
    assert helper_calls == 2


def test_timestamp_from_sidecar_doc_names_declaring_path_exception() -> None:
    doc = BaseSilverTransformer._timestamp_from_sidecar.__doc__ or ""
    assert "Declaring availability discovery is the deliberate" in doc
    assert "when ``source_dates`` is supplied" in doc
    assert "call this for EVERY source" not in doc


class _PartitionOwnedLockstepTransformer(BaseSilverTransformer):
    source = "test"
    dataset = "invalid_partition_lockstep"
    PARTITION_DATE_COLUMN = "owner_date"
    LOCKSTEP_BRONZE_READ = True

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df


def test_partition_ownership_and_lockstep_read_are_mutually_exclusive(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match="cannot set both PARTITION_DATE_COLUMN and LOCKSTEP_BRONZE_READ",
    ):
        _PartitionOwnedLockstepTransformer(tmp_path)


@pytest.mark.parametrize("sidecar_text", ["{not-json", "[]"])
def test_declaring_reingest_logs_corrupt_neighbour_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    sidecar_text: str,
) -> None:
    predecessor_dir = PathBuilder(tmp_path).bronze_date_dir("elexon", "mid", PREDECESSOR)
    predecessor_dir.mkdir(parents=True, exist_ok=True)
    corrupt_sidecar = predecessor_dir / "raw_corrupt.meta.json"
    corrupt_sidecar.write_text(sidecar_text)
    _write_sidecar(tmp_path, DESTINATION, "2024-01-15T03:00:00+00:00")
    transformer = _fixed_mid(
        tmp_path,
        monkeypatch,
        {
            PREDECESSOR: pl.DataFrame([_mid_row(DESTINATION, 1, "P", 10.0)]),
            DESTINATION: pl.DataFrame([_mid_row(DESTINATION, 2, "P", 20.0)]),
        },
    )

    with caplog.at_level("WARNING", logger="gridflow.silver.base"):
        assert transformer.run(DESTINATION, run_id="corrupt-neighbour", reingest=True) == 2

    assert any(
        record.levelname == "WARNING"
        and "Failed to parse bronze sidecar" in record.getMessage()
        and str(corrupt_sidecar) in record.getMessage()
        for record in caplog.records
    )


def test_per_body_declaring_run_warns_when_both_partitions_are_absent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    transformer = SystemPriceTransformer(tmp_path)

    with caplog.at_level("WARNING", logger="gridflow.silver.base"):
        assert transformer.run(DESTINATION, run_id="empty-per-body") == 0

    assert f"No bronze data for elexon/system_prices on {DESTINATION}" in caplog.messages


def test_p_t16_repeated_source_fallback_occurrences(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fallback_rows = [
        {
            "settlementDate": PREDECESSOR.isoformat(),
            "settlementPeriod": period,
            "fuelType": "CCGT",
            "generation": float(period),
        }
        for period in range(1, 6)
    ]
    own_rows = [
        {
            **row,
            "settlementDate": DESTINATION.isoformat(),
            "startTime": f"2024-01-15T{(int(row['settlementPeriod']) - 1) // 2:02d}:"
            f"{'30' if int(row['settlementPeriod']) % 2 == 0 else '00'}:00Z",
        }
        for row in fallback_rows
    ]
    inputs = {
        PREDECESSOR: pl.DataFrame(fallback_rows),
        DESTINATION: pl.DataFrame(own_rows),
    }
    transformer = FuelHHTransformer(tmp_path)
    monkeypatch.setattr(
        transformer,
        "read_bronze",
        lambda source_date: inputs.get(source_date, pl.DataFrame()),
    )
    monkeypatch.setattr(transformer, "_source_window_plan", lambda _source_date: None)

    transformer.run(PREDECESSOR, run_id="first")
    assert transformer.last_start_time_fallback_count == 5
    transformer.run(DESTINATION, run_id="second")
    assert transformer.last_start_time_fallback_count == 5


def _write_system_price_body(
    root: Path,
    source_date: date,
    owner: date,
    stamp: datetime,
) -> None:
    partition = PathBuilder(root).bronze_date_dir("elexon", "system_prices", source_date)
    partition.mkdir(parents=True, exist_ok=True)
    body = partition / "raw_capture.json"
    body.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "settlementDate": owner.isoformat(),
                        "settlementPeriod": 1,
                        "systemSellPrice": 40.0,
                        "systemBuyPrice": 41.0,
                        "netImbalanceVolume": -1.0,
                        "settlementRunType": "SF",
                    }
                ]
            }
        )
    )
    body.with_suffix(".meta.json").write_text(json.dumps({"written_at": stamp.isoformat()}))


def test_p_t12_system_prices_reads_only_own_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("gridflow.silver.elexon.system_prices.datetime", _Clock)
    own_stamp = datetime(2024, 1, 15, 12, tzinfo=UTC)
    baseline_root = tmp_path / "baseline"
    candidate_root = tmp_path / "candidate"
    _write_system_price_body(baseline_root, DESTINATION, DESTINATION, own_stamp)
    _write_system_price_body(
        candidate_root, PREDECESSOR, PREDECESSOR, own_stamp - timedelta(days=1)
    )
    _write_system_price_body(candidate_root, DESTINATION, DESTINATION, own_stamp)

    baseline = SystemPriceTransformer(baseline_root)
    candidate = SystemPriceTransformer(candidate_root)
    assert baseline.run(DESTINATION, run_id="fixed") == 1
    assert candidate.run(DESTINATION, run_id="fixed") == 1
    baseline_files = {
        path.name: path.read_bytes()
        for path in PathBuilder(baseline_root)
        .silver_partition_dir("elexon", "system_prices", DESTINATION)
        .glob("*.parquet")
    }
    candidate_files = {
        path.name: path.read_bytes()
        for path in PathBuilder(candidate_root)
        .silver_partition_dir("elexon", "system_prices", DESTINATION)
        .glob("*.parquet")
    }
    assert candidate_files == baseline_files
    assert candidate.last_partition_trimmed_count == 0
    assert candidate.last_partition_trim_unrecoverable_count == 0


def test_p_t19_patched_silver_dir_controls_existence_and_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    production = tmp_path / "production"
    output = tmp_path / "output" / "silver" / "elexon" / "mid"
    inputs = {DESTINATION: pl.DataFrame([_mid_row(PREDECESSOR, 1, "P", 10.0)])}
    transformer = _fixed_mid(production, monkeypatch, inputs)
    transformer.silver_dir = output

    production_path = PathBuilder(production).silver_file("elexon", "mid", DESTINATION)
    production_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"production": [1]}).write_parquet(production_path)
    assert transformer.run(DESTINATION, run_id="no-output") == 0
    assert not transformer._silver_destination(DESTINATION).exists()
    assert production_path.exists()

    output_path = transformer._silver_destination(DESTINATION)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"stale": [1]}).write_parquet(output_path)
    transformer.run(DESTINATION, run_id="replace-output")
    assert pl.read_parquet(output_path).is_empty()
    assert pl.read_parquet(production_path).height == 1
