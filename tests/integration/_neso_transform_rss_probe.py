"""Peak-RSS probe for the NESO transform memory gate (ADR-034 P-16, B8).

Run in a FRESH interpreter by ``test_neso_transform_memory_gate.py``; never
collected by pytest (no ``test_`` prefix)::

    python tests/integration/_neso_transform_rss_probe.py --members N \\
        --engine {generic,perfile} --data-dir <tmp> [--warmup W] \\
        [--reader {csv,zip_member}]

It writes N CSV captures of ~75 MiB each into one bronze date partition (one
header ``row_id,value,label``; ``row_id`` a monotonically increasing counter, so
every row is distinct and castable), each with a valid unit-A sidecar, then
runs one transform over that date and prints one JSON line::

    {"peak_rss": <bytes>, "outputs": <silver files>, "rows": <rows written>,
     "rows_written_to_bronze": <rows generated>}

**Warm-up.** With ``--warmup W`` the same engine first transforms W captures
of a separate partition date (another month, so its outputs land in another
silver directory) before the measured N. The allocator's cache ramps over the
first several captures of a process (measured: the working set after each
capture is flat, the process PEAK climbs for ~6-9 captures, then plateaus), so
without a warm-up a 5-capture and a 20-capture run sample that ramp at
different points and their peak ratio is noise. ``outputs``, ``rows`` and
``rows_written_to_bronze`` count the measured partition only.

``generic`` runs the generic engine over a tmp-registry family whose record is
reader ``csv``, ``row_id`` int64 / ``value`` float64 / ``label`` string (all
non-nullable), temporal ``none``, key ``(row_id,)``, ``key_latest``,
``ckan_last_modified``. ``perfile`` runs a synthetic ``VINTAGE_PER_BRONZE_FILE``
subclass over the same bodies (the B8 red control).

**Reader.** ``--reader zip_member`` (generic engine only; v0.22-H, ADR-043) writes
each body as a deflated ZIP holding one member ``m.csv`` with the same ~75 MiB
rows, streamed into the archive, and gives the family reader ``zip_member``
(``member_pattern`` ``m\\.csv``, inner ``csv``) with each resource carrying the
child ``m.csv``. The default ``csv`` is the original gate, unchanged.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, ClassVar

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "unit"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _neso_generic_support import write_capture  # noqa: E402
from _neso_rss_probe import peak_rss_bytes  # noqa: E402

if TYPE_CHECKING:
    import polars as pl

MIB = 1024 * 1024
BODY_BYTES = 75 * MIB
HEADER = ("row_id", "value", "label")
FAMILY = "rss_probe_generic"
MEMBER = "m.csv"
PKG = "eeeeeeee-1111-4000-8000-000000000000"
PARTITION = date(2026, 10, 7)
WARMUP_PARTITION = date(2026, 9, 1)
_CHUNK_ROWS = 100_000


def _rid(n: int) -> str:
    return f"eeeeeeee-1111-4000-8000-{n:012d}"


def _body_writer(member: int, counts: list[int]) -> Any:
    offset = member * 100_000_000

    def _write(handle: BinaryIO) -> None:
        handle.write((",".join(HEADER) + "\n").encode())
        written = 0
        i = 0
        while written < BODY_BYTES:
            chunk = "".join(
                f"{offset + j},{j * 0.5},L{j % 97}\n" for j in range(i, i + _CHUNK_ROWS)
            ).encode()
            handle.write(chunk)
            written += len(chunk)
            i += _CHUNK_ROWS
        counts.append(i)

    return _write


def _zip_writer(member: int, counts: list[int]) -> Any:
    """The same rows as :func:`_body_writer`, streamed into a deflated one-member ZIP."""
    import zipfile

    inner = _body_writer(member, counts)

    def _write(handle: BinaryIO) -> None:
        with (
            zipfile.ZipFile(handle, "w", zipfile.ZIP_DEFLATED) as archive,
            archive.open(MEMBER, "w") as stream,
        ):
            inner(stream)  # type: ignore[arg-type]

    return _write


def _write_bronze(
    data_dir: Path,
    key: str,
    members: range,
    partition: date = PARTITION,
    reader: str = "csv",
) -> int:
    counts: list[int] = []
    base = datetime(partition.year, partition.month, partition.day, 6, tzinfo=UTC)
    stamp = partition.isoformat()
    writer = _zip_writer if reader == "zip_member" else _body_writer
    for member in members:
        write_capture(
            data_dir,
            key,
            package_slug="rss-probe-generic",
            package_id=PKG,
            resource_id=_rid(member),
            resource_name=f"Member {member}",
            body_writer=writer(member, counts),
            written_at=base + timedelta(minutes=member),
            ckan_last_modified=f"{stamp}T05:{member:02d}:00.000001",
            partition=partition,
            extension="zip" if reader == "zip_member" else "csv",
        )
    return sum(counts)


def _registry_documents(members: int, reader: str = "csv") -> dict[str, Any]:
    columns = [
        {"source": "row_id", "name": "row_id", "dtype": "int64", "nullable": False},
        {"source": "value", "name": "value", "dtype": "float64", "nullable": False},
        {"source": "label", "name": "label", "dtype": "string", "nullable": False},
    ]
    rec: dict[str, Any] = {
        "version": "1",
        "reader": reader,
        "encoding": "utf-8",
        "epochs": [{"header": list(HEADER), "columns": columns, "issue": {"kind": "none"}}],
        "temporal": {"kind": "none"},
        "entity_key": ["row_id"],
        "latest": "key_latest",
        "vintage": "ckan_last_modified",
    }
    disposition = {"kind": "SILVER", "key": FAMILY}
    extra: dict[str, Any] = {}
    if reader == "zip_member":
        rec["zip_member"] = {"member_pattern": "m\\.csv", "inner": "csv"}
        extra = {"children": [{"child": MEMBER, "disposition": disposition}]}
    return {
        "package": "rss-probe-generic",
        "package_id": PKG,
        "group": "synthetic",
        "archetype": "SER",
        "refresh": "daily",
        "eligibility": {"status": "eligible"},
        "families": [
            {
                "key": FAMILY,
                "kind": "tabular",
                "legacy": False,
                "archetype": "SER",
                "refresh": "daily",
                "empty_allowed": False,
                "max_download_bytes": 200 * MIB,
                "name_regex": r"^Member \d+$",
                "transformer": None,
                "record": rec,
            }
        ],
        "resources": [
            {
                "id": _rid(member),
                "name": f"Member {member}",
                "format": "CSV",
                "url_type": "upload",
                "family": FAMILY,
                "disposition": disposition,
                **extra,
            }
            for member in range(members)
        ],
    }


def _run_generic(data_dir: Path, members: int, warmup: int, reader: str = "csv") -> int:
    from gridflow.connectors.neso_data_portal import registry as registry_module
    from gridflow.silver.neso_data_portal import generic

    directory = data_dir / "_registry"
    directory.mkdir()
    (directory / "rss-probe-generic.json").write_text(
        registry_module.dump_json(_registry_documents(warmup + members, reader)), encoding="utf-8"
    )
    (directory / "_frozen_keys.json").write_text("[]", encoding="utf-8")
    (directory / "_adjudications.json").write_text("[]", encoding="utf-8")
    loaded = registry_module.load_registry(directory)

    def _load(path: Path | None = None) -> registry_module.Registry:
        return loaded if path is None else registry_module.load_registry(path)

    registry_module.load_registry = _load  # type: ignore[assignment]
    generated = generic.generated_registrations(loaded)
    transformer = generated.transformers[FAMILY](data_dir)
    if warmup:
        transformer.run(WARMUP_PARTITION, run_id="rss-probe-warmup")
    return transformer.run(PARTITION, run_id="rss-probe")


def _run_perfile(data_dir: Path, warmup: int) -> int:
    import polars as pl

    from gridflow.silver.base import BaseSilverTransformer
    from gridflow.silver.csv_bronze import read_csv_bronze_body

    class _PerFile(BaseSilverTransformer):
        source = "neso_data_portal"
        dataset = FAMILY
        APPEND_ONLY: ClassVar[bool] = True
        VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True
        BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.csv"

        def read_bronze(self, target_date: date) -> pl.DataFrame:
            return pl.DataFrame()

        def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
            return read_csv_bronze_body(
                raw_path.read_bytes(), expected_columns=HEADER, source_label=str(raw_path)
            )

        def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
            return raw_df.with_columns(
                pl.col("row_id").cast(pl.Int64, strict=True),
                pl.col("value").cast(pl.Float64, strict=True),
            ).with_columns(
                pl.lit(datetime(2026, 10, 7, tzinfo=UTC))
                .cast(pl.Datetime("us", "UTC"))
                .alias("timestamp_utc")
            )

    if warmup:
        _PerFile(data_dir).run(WARMUP_PARTITION, run_id="rss-probe-warmup")
    return _PerFile(data_dir).run(PARTITION, run_id="rss-probe")


def main() -> None:
    """Write the bronze, run one engine, print the JSON result."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--members", type=int, required=True)
    parser.add_argument("--engine", choices=("generic", "perfile"), required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--reader", choices=("csv", "zip_member"), default="csv")
    args = parser.parse_args()
    data_dir: Path = args.data_dir
    warmup: int = args.warmup
    reader: str = args.reader
    if reader != "csv" and args.engine != "generic":
        parser.error("--reader zip_member needs --engine generic")
    if warmup:
        _write_bronze(data_dir, FAMILY, range(warmup), WARMUP_PARTITION, reader)
    generated_rows = _write_bronze(
        data_dir, FAMILY, range(warmup, warmup + args.members), reader=reader
    )
    rows = (
        _run_generic(data_dir, args.members, warmup, reader)
        if args.engine == "generic"
        else _run_perfile(data_dir, warmup)
    )
    measured = f"year={PARTITION.year}/month={PARTITION.month:02d}"
    outputs = len(list((data_dir / "silver").rglob(f"{measured}/*.parquet")))
    print(
        json.dumps(
            {
                "peak_rss": peak_rss_bytes(),
                "outputs": outputs,
                "rows": rows,
                "rows_written_to_bronze": generated_rows,
            }
        )
    )


if __name__ == "__main__":
    main()
