"""R2-g Task 1: the bronze-vouching primitive (ADR-028).

A bronze body whose OWN sidecar does not yield a parseable tz-aware UTC
timestamp is UNVOUCHED: it is excluded from the read set, counted with its
reason, and never repaired or deleted (the owner's exclude-until-vouched
ruling, 2026-08-02). These tests pin the primitive that decides that, plus the
D-7 guarantee that the pre-existing logging wrapper's behaviour is unchanged.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import polars as pl
import pytest

from gridflow.silver.base import (
    BaseSilverTransformer,
    BronzeReadSelection,
    BronzeVouchReason,
)

if TYPE_CHECKING:
    from pathlib import Path


class _StubTransformer(BaseSilverTransformer):
    source = "test_source"
    dataset = "test_dataset"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame()


def _body(directory: Path, name: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text("{}")
    return path


def _vouch(body: Path, **meta: object) -> Path:
    sidecar = body.with_suffix(".meta.json")
    sidecar.write_text(json.dumps(meta))
    return sidecar


def _stub(tmp_path: Path) -> _StubTransformer:
    return _StubTransformer(tmp_path)


class TestResolveVouchedBronzeSet:
    def test_orphan_body_is_excluded_with_no_sidecar_reason(self, tmp_path: Path) -> None:
        """T1-a."""
        good = _body(tmp_path, "raw_0900_aaaa.json")
        _vouch(good, written_at="2026-08-01T09:15:00Z")
        orphan = _body(tmp_path, "raw_1000_bbbb.json")

        result = _stub(tmp_path)._resolve_vouched_bronze_set(
            [good, orphan], BronzeReadSelection.ALL
        )

        assert result.entries == ((good, datetime(2026, 8, 1, 9, 15, tzinfo=UTC)),)
        assert result.unvouched == ((orphan, BronzeVouchReason.NO_SIDECAR),)
        assert result.examined == 2
        assert result.paths == (good,)

    def test_malformed_json_sidecar_is_unreadable_and_silent(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T1-b: the resolver emits nothing above DEBUG -- aggregation is the caller's job."""
        body = _body(tmp_path, "raw_0900_aaaa.json")
        body.with_suffix(".meta.json").write_text("{not json")

        with caplog.at_level(logging.DEBUG, logger="gridflow.silver.base"):
            result = _stub(tmp_path)._resolve_vouched_bronze_set([body], BronzeReadSelection.ALL)

        assert result.entries == ()
        assert result.unvouched == ((body, BronzeVouchReason.UNREADABLE_SIDECAR),)
        above_debug = [r for r in caplog.records if r.levelno > logging.DEBUG]
        assert above_debug == [], f"resolver must be silent above DEBUG: {above_debug}"

    def test_sidecar_without_any_timestamp_key(self, tmp_path: Path) -> None:
        """T1-c."""
        body = _body(tmp_path, "raw_0900_aaaa.json")
        _vouch(body, source="entsog", dataset="physical_flows")

        result = _stub(tmp_path)._resolve_vouched_bronze_set([body], BronzeReadSelection.ALL)

        assert result.unvouched == ((body, BronzeVouchReason.NO_TIMESTAMP_KEY),)

    def test_sidecar_with_unparseable_timestamp(self, tmp_path: Path) -> None:
        """T1-d."""
        body = _body(tmp_path, "raw_0900_aaaa.json")
        _vouch(body, written_at="not-a-date")

        result = _stub(tmp_path)._resolve_vouched_bronze_set([body], BronzeReadSelection.ALL)

        assert result.unvouched == ((body, BronzeVouchReason.UNPARSEABLE_TIMESTAMP),)

    def test_newest_vouched_steps_over_unvouched_and_stops(self, tmp_path: Path) -> None:
        """T1-e: the two stepped-over bodies are counted; the walk stops at the 3rd."""
        newest = _body(tmp_path, "raw_1200_cccc.json")
        middle = _body(tmp_path, "raw_1100_bbbb.json")
        oldest = _body(tmp_path, "raw_1000_aaaa.json")
        _vouch(oldest, written_at="2026-08-01T10:05:00Z")

        result = _stub(tmp_path)._resolve_vouched_bronze_set(
            [newest, middle, oldest], BronzeReadSelection.NEWEST_VOUCHED
        )

        assert result.entries == ((oldest, datetime(2026, 8, 1, 10, 5, tzinfo=UTC)),)
        assert result.unvouched == (
            (newest, BronzeVouchReason.NO_SIDECAR),
            (middle, BronzeVouchReason.NO_SIDECAR),
        )
        assert result.examined == 3

    def test_newest_vouched_stops_probing_after_the_first_vouched_file(
        self, tmp_path: Path
    ) -> None:
        """The trailing candidate behind the selected file is never probed.

        Without this, a resolver that eagerly walked the whole list would
        satisfy every other assertion while counting irrelevant OLDER orphans
        into a false non-success run result.
        """
        newest = _body(tmp_path, "raw_1200_cccc.json")
        _vouch(newest, written_at="2026-08-01T12:05:00Z")
        older_orphan = _body(tmp_path, "raw_1000_aaaa.json")

        result = _stub(tmp_path)._resolve_vouched_bronze_set(
            [newest, older_orphan], BronzeReadSelection.NEWEST_VOUCHED
        )

        assert result.entries == ((newest, datetime(2026, 8, 1, 12, 5, tzinfo=UTC)),)
        assert result.unvouched == ()
        assert result.examined == 1

    def test_newest_vouched_with_nothing_vouchable_is_empty(self, tmp_path: Path) -> None:
        """T1-f: no available_at() aggregate exists to fall back on (S-22)."""
        bodies = [_body(tmp_path, f"raw_{h}00_x.json") for h in (12, 11, 10)]

        result = _stub(tmp_path)._resolve_vouched_bronze_set(
            bodies, BronzeReadSelection.NEWEST_VOUCHED
        )

        assert result.entries == ()
        assert len(result.unvouched) == 3
        assert result.examined == 3
        assert not hasattr(result, "available_at")

    def test_every_entry_maps_a_path_to_its_own_utc_stamp(self, tmp_path: Path) -> None:
        """T1-g: mixed offsets, per-path mapping -- never an aggregate."""
        a = _body(tmp_path, "raw_a.json")
        b = _body(tmp_path, "raw_b.json")
        c = _body(tmp_path, "raw_c.json")
        _vouch(a, written_at="2026-08-01T09:00:00+02:00")
        _vouch(b, written_at="2026-08-01T09:00:00Z")
        _vouch(c, written_at="2026-08-01T09:00:00")

        result = _stub(tmp_path)._resolve_vouched_bronze_set([a, b, c], BronzeReadSelection.ALL)

        mapping = dict(result.entries)
        assert mapping[a] == datetime(2026, 8, 1, 7, 0, tzinfo=UTC)
        assert mapping[b] == datetime(2026, 8, 1, 9, 0, tzinfo=UTC)
        assert mapping[c] == datetime(2026, 8, 1, 9, 0, tzinfo=UTC)
        assert all(stamp.tzinfo == UTC for _, stamp in result.entries)

    def test_resolver_never_globs(self, tmp_path: Path) -> None:
        """T1-i: only the supplied candidates are examined."""
        listed = _body(tmp_path, "raw_listed.json")
        _vouch(listed, written_at="2026-08-01T09:00:00Z")
        unlisted = _body(tmp_path, "raw_unlisted.json")
        _body(tmp_path, ".tmp_raw_partial.json")

        result = _stub(tmp_path)._resolve_vouched_bronze_set([listed], BronzeReadSelection.ALL)

        seen = {path for path, _ in result.entries} | {path for path, _ in result.unvouched}
        assert seen == {listed}
        assert unlisted not in seen
        assert result.examined == 1

    @pytest.mark.parametrize(
        "meta,expected",
        [
            ({"fetched_at": "2026-08-01T09:00:00Z"}, datetime(2026, 8, 1, 9, 0, tzinfo=UTC)),
            (
                {"available_at": "2026-08-01T08:00:00Z", "written_at": "2026-08-01T09:00:00Z"},
                datetime(2026, 8, 1, 8, 0, tzinfo=UTC),
            ),
            ({"written_at": "2026-08-01T09:00:00"}, datetime(2026, 8, 1, 9, 0, tzinfo=UTC)),
        ],
        ids=["fetched_at-only", "available_at-wins", "naive-gets-utc"],
    )
    def test_legacy_sidecar_shapes_still_vouch(
        self, tmp_path: Path, meta: dict[str, str], expected: datetime
    ) -> None:
        """T1-j: cases 5, 6 and 7 of the failure-mode enumeration."""
        body = _body(tmp_path, "raw_a.json")
        _vouch(body, **meta)

        result = _stub(tmp_path)._resolve_vouched_bronze_set([body], BronzeReadSelection.ALL)

        assert result.entries == ((body, expected),)


class TestKeyPreferenceFallThrough:
    """T1-k: an invalid key must NOT end the search -- master falls through."""

    def test_invalid_written_at_falls_through_to_valid_fetched_at(self, tmp_path: Path) -> None:
        body = _body(tmp_path, "raw_a.json")
        sidecar = _vouch(body, written_at="not-a-date", fetched_at="2026-08-01T09:00:00Z")
        expected = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)

        pure = BaseSilverTransformer._read_sidecar_timestamp(sidecar)
        assert pure.timestamp == expected
        assert pure.reason is None

        assert BaseSilverTransformer._timestamp_from_sidecar(sidecar) == expected

        result = _stub(tmp_path)._resolve_vouched_bronze_set([body], BronzeReadSelection.ALL)
        assert result.entries == ((body, expected),)
        assert result.unvouched == ()

    def test_unparseable_reported_only_when_every_present_key_fails(self, tmp_path: Path) -> None:
        body = _body(tmp_path, "raw_a.json")
        sidecar = _vouch(body, written_at="not-a-date", fetched_at="also-not-a-date")

        pure = BaseSilverTransformer._read_sidecar_timestamp(sidecar)
        assert pure.timestamp is None
        assert pure.reason is BronzeVouchReason.UNPARSEABLE_TIMESTAMP
        assert [d.key for d in pure.diagnostics] == ["written_at", "fetched_at"]


class TestWrapperContractIsUnchanged:
    """T1-h / T1-m / T1-l: the D-7 refactor must not shift the wrapper."""

    def test_returns_none_and_warns_once_for_malformed_json(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        sidecar = tmp_path / "raw_a.meta.json"
        sidecar.write_text("{not json")

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert BaseSilverTransformer._timestamp_from_sidecar(sidecar) is None

        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "Failed to parse bronze sidecar" in warnings[0].getMessage()

    def test_returns_none_and_warns_once_for_unparseable_timestamp(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        sidecar = tmp_path / "raw_a.meta.json"
        sidecar.write_text(json.dumps({"written_at": "not-a-date"}))

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert BaseSilverTransformer._timestamp_from_sidecar(sidecar) is None

        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "Could not parse bronze sidecar timestamp" in warnings[0].getMessage()

    def test_returns_none_without_warning_for_missing_keys(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        sidecar = tmp_path / "raw_a.meta.json"
        sidecar.write_text(json.dumps({"source": "entsog"}))

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert BaseSilverTransformer._timestamp_from_sidecar(sidecar) is None

        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_returns_none_and_warns_once_when_sidecar_is_absent(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert (
                BaseSilverTransformer._timestamp_from_sidecar(tmp_path / "nope.meta.json") is None
            )

        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1

    def test_replays_a_failed_key_warning_and_still_returns_the_later_value(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T1-m: the wrapper replays EVERY diagnostic, not just the last one.

        A single-``reason`` design would swallow the first warning silently,
        so this asserts the COUNT, not merely the return value.
        """
        sidecar = tmp_path / "raw_a.meta.json"
        sidecar.write_text(
            json.dumps({"written_at": "not-a-date", "fetched_at": "2026-08-01T09:00:00Z"})
        )

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            result = BaseSilverTransformer._timestamp_from_sidecar(sidecar)

        assert result == datetime(2026, 8, 1, 9, 0, tzinfo=UTC)
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "not-a-date" in warnings[0].getMessage()

    def test_non_string_timestamp_value_is_silent_on_both_paths(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Master's `_parse_timestamp` returns None silently for an int."""
        sidecar = tmp_path / "raw_a.meta.json"
        sidecar.write_text(json.dumps({"written_at": 12345}))

        with caplog.at_level(logging.WARNING, logger="gridflow.silver.base"):
            assert BaseSilverTransformer._timestamp_from_sidecar(sidecar) is None

        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        assert (
            BaseSilverTransformer._read_sidecar_timestamp(sidecar).reason
            is BronzeVouchReason.UNPARSEABLE_TIMESTAMP
        )

    @pytest.mark.parametrize("payload", ["[1, 2]", '"text"', "null", "3"], ids=list("labi"))
    def test_non_object_json_classifies_but_the_wrapper_still_raises(
        self, tmp_path: Path, payload: str
    ) -> None:
        """T1-l (N-18): the classifier is hardened; the wrapper is NOT.

        Softening the wrapper would convert a loud fail-closed crash into a
        fail-open ``None`` for `_available_at_from_bronze` and the
        `VINTAGE_PER_BRONZE_FILE` branch -- i.e. into a SIBLING file's stamp,
        a now() fallback, or silently skipped rows, for every source.
        """
        body = _body(tmp_path, "raw_a.json")
        sidecar = body.with_suffix(".meta.json")
        sidecar.write_text(payload)

        pure = BaseSilverTransformer._read_sidecar_timestamp(sidecar)
        assert pure.timestamp is None
        assert pure.reason is BronzeVouchReason.UNREADABLE_SIDECAR

        with pytest.raises(AttributeError):
            BaseSilverTransformer._timestamp_from_sidecar(sidecar)

        result = _stub(tmp_path)._resolve_vouched_bronze_set([body], BronzeReadSelection.ALL)
        assert result.unvouched == ((body, BronzeVouchReason.UNREADABLE_SIDECAR),)
