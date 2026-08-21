"""Extract chart-ready CSVs for every dataset in the gridflow catalogue.

Run: ingest -> transform -> (gridflow init) -> export via DuckDB.

Provenance: written 2026-08-16 during the gridflow-front-end real-chart-data
session (front-end PRs #36/#37); rescued into this repo 2026-08-21 (T-23,
`neso_data_portal` front-end registration) after living only in a volatile
session scratchpad. `site/hifi/data/chart-series.json` records
`extract_generated_by: "extract_all.py"`, so this filename is load-bearing
provenance — do not rename it.

Writes to `C:\\data_to_seed_gridflow_charts` (`OUT_ROOT`, not part of this
repo): per-dataset CSVs plus a `manifest.json` the front-end build's
`--refresh-chart-data` step reads.

KNOWN DEFECTS -- read before trusting a fresh run
-------------------------------------------------
Cross-model review (Codex GPT-5.6 Sol, 2026-08-21) raised these against the
rescued script. NONE of them corrupted the 2026-08-21 `neso_data_portal`
extract -- that run was verified row-for-row against silver (628/628 and
3589/3589, zero rows dropped, dedup ratio 1.000) -- but every one is live for
the NEXT run, especially over the wider catalogue. Filed, not fixed.

1. `settlement_period` is in `METADATA_COLUMNS`, so it is excluded from the
   dedup subset. Rows differing ONLY by settlement period collapse. This
   contravenes the repo rule that settlement data is never deduplicated on
   `(date, period)` alone. Fixing it means editing a frozenset that gates
   every one of the ~163 datasets -- enumerate the callers first.
2. A missing `_latest` relation falls back to the raw silver relation without
   checking the dataset's update strategy, so APPEND_ONLY datasets can export
   duplicate vintages while still reporting `chartable=true`. Same class as
   the `export-csv` defect in the extract's own FINDINGS.md section 2.
3. Every fetch checkpoint counts as completed regardless of status, so
   `FETCH_ERROR` and vendor-parked datasets are never retried and a later run
   can export stale pre-existing silver as fresh chart data.
4. CSV and `manifest.json` writes go straight to their final paths. An
   interrupted run can leave a truncated CSV referenced by the old manifest,
   or a partially written manifest. The repo convention is `os.replace()`.
5. `--datasets` without `--source` is silently treated as an unscoped run,
   which fetches the whole catalogue and wholesale-overwrites the manifest --
   the exact clobbering the scoping flags were added to prevent.
6. Checkout, interpreter and DuckDB paths are hard-coded rather than resolved
   through `PathBuilder`, so another checkout fails or, worse, reads an
   unrelated database.
7. `--full-history` materialises the whole relation in memory, then builds a
   second frame to deduplicate it.
8. A `--full-history` export still stamps the fixed 1-5 Aug window into
   `manifest.json` and `SUMMARY.txt`, contradicting the series it just wrote.
   (Observed: the 2026-08-21 manifest declares that window while holding
   `historic_generation_mix` rows from 2009.) The front-end distiller reads
   per-series start/end, not this window, so the rendered pages are correct.

Design constraints this encodes, and why:

* NOTHING fails the whole run. Each dataset gets a status record and the loop
  continues. Over 160 datasets the first reference table with no time column
  would otherwise abort everything.
* Datasets that CANNOT be backfilled are classified from config up front rather
  than attempted and mis-reported: `max_query_days == 0` is a snapshot endpoint.
* Duplicate natural keys are resolved ONLY when the duplicate rows are identical
  in every non-metadata column. That case is provably lossless. When rows with
  the same key carry genuinely DIFFERENT values, they are kept and the dataset is
  flagged DIVERGENT -- resolving those would need vendor semantics this script
  does not have, and guessing is the silent-data-bug class.
* Value columns are not hand-picked. Chartability is mechanical: a recognised
  time column AND at least one non-metadata numeric column.
* N-17: three ENTSO-G reference families stamp event_time = the run date. They
  are exported but marked chartable=false with a machine-readable reason so
  unit 2 cannot accidentally plot fabricated timestamps.

Scoping (added at rescue time, T-23): the original script always walked the
FULL catalogue for both fetch and export, which (a) would trigger live
`gridflow ingest` calls for any not-yet-fetched dataset and (b) would
overwrite `manifest.json` wholesale on every run, silently reshuffling
already-shipped vendors' chart inputs. `--source` / `--datasets` scope both
phases to a subset. When scoped, `export` MERGES its results into any
existing `manifest.json` (replacing only the scoped entries) instead of
overwriting it, so an unrelated vendor's extract is never touched by a
targeted re-run. Unscoped runs keep the original overwrite-the-whole-file
behaviour.

Writes only to OUT_ROOT. Uses read-only DuckDB connections for export.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import yaml

REPO = Path(r"C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow")
PY = REPO / ".venv" / "Scripts" / "python.exe"
OUT_ROOT = Path(r"C:\data_to_seed_gridflow_charts")
DB_PATH = Path(r"C:\gridflow-data\gridflow.duckdb")
STATE = OUT_ROOT / "_run_state.jsonl"

WINDOW_START = date(2026, 8, 1)
WINDOW_END = date(2026, 8, 5)
# Half-open vendors (ENTSO-E `[periodStart, periodEnd)`) drop the final day
# unless the request end is exclusive. Verified empirically for entsoe/entsog.
TOPUP_END = date(2026, 8, 6)

INGEST_TIMEOUT = 420
TRANSFORM_TIMEOUT = 420
# Park a vendor after this many consecutive failures so one throttled source
# cannot stall the other six.
VENDOR_FAILURE_LIMIT = 3

TIME_COLUMNS = ("settlement_date", "event_time", "timestamp_utc", "gas_day")

# Present on every silver frame; carries provenance, not signal. Excluded from
# both the duplicate comparison and the numeric-column scan.
METADATA_COLUMNS = frozenset(
    {
        "ingested_at",
        "available_at",
        "source_run_id",
        "dataset_version",
        "year",
        "month",
        "data_provider",
        "settlement_period",
    }
)

# N-17: proven live 2026-08-03 to carry zero _TIMESTAMP_PRIORITY fields, so
# every row gets event_time = target_date (the run date).
N17_UNCHARTABLE = {
    ("entsog", "connection_points"),
    ("entsog", "balancing_zones"),
    ("entsog", "aggregate_interconnections"),
}

# N-22: absent from both DOC_TYPES and sources.yaml, so no fetch path exists.
UNWIRED = {("entsoe", "activated_balancing_qty")}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_catalogue(
    scope: set[tuple[str, str]] | None = None,
) -> list[tuple[str, str, dict[str, Any]]]:
    """Load the (source, dataset, config) catalogue, optionally scoped.

    Args:
        scope: if given, only (source, dataset) pairs present in this set
            are returned. ``None`` (the default) returns the full catalogue,
            preserving the original unscoped behaviour.
    """
    cfg = yaml.safe_load((REPO / "config" / "sources.yaml").read_text(encoding="utf-8"))
    out = []
    for source, scfg in (cfg.get("sources") or cfg).items():
        for ds, dcfg in ((scfg or {}).get("datasets") or {}).items():
            if scope is not None and (source, ds) not in scope:
                continue
            out.append((source, ds, dcfg or {}))
    return out


def run_cli(args: list[str], timeout: int) -> tuple[bool, str]:
    env = dict(os.environ, GRIDFLOW_ALLOW_INGEST="1", PYTHONIOENCODING="utf-8")
    try:
        p = subprocess.run(
            [str(PY), "-m", "gridflow", *args],
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, f"timeout after {timeout}s"
    tail = ((p.stdout or "") + (p.stderr or "")).strip().splitlines()
    return p.returncode == 0, " | ".join(tail[-2:])[:300]


def checkpoint(rec: dict[str, Any]) -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    with STATE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, default=str) + "\n")


def already_done() -> set[tuple[str, str]]:
    if not STATE.exists():
        return set()
    done = set()
    for line in STATE.read_text(encoding="utf-8").splitlines():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("phase") == "fetch":
            done.add((r["source"], r["dataset"]))
    return done


# ----------------------------------------------------------------- phase 1


def fetch_all(scope: set[tuple[str, str]] | None = None) -> None:
    catalogue = load_catalogue(scope)
    done = already_done()
    log(f"catalogue: {len(catalogue)} datasets | {len(done)} already fetched")

    by_source: dict[str, list[tuple[str, str, dict]]] = {}
    for s, d, c in catalogue:
        by_source.setdefault(s, []).append((s, d, c))

    for source, items in by_source.items():
        consecutive = 0
        parked = False
        for s, ds, cfg in items:
            if (s, ds) in done:
                continue
            if parked:
                checkpoint(
                    {
                        "phase": "fetch",
                        "source": s,
                        "dataset": ds,
                        "status": "SKIPPED_VENDOR_PARKED",
                    }
                )
                continue
            if (s, ds) in UNWIRED:
                checkpoint(
                    {
                        "phase": "fetch",
                        "source": s,
                        "dataset": ds,
                        "status": "UNWIRED",
                        "detail": "N-22: no fetch path in DOC_TYPES or sources.yaml",
                    }
                )
                continue
            if cfg.get("max_query_days", 1) == 0:
                checkpoint(
                    {
                        "phase": "fetch",
                        "source": s,
                        "dataset": ds,
                        "status": "SNAPSHOT_ONLY",
                        "detail": "max_query_days=0: current-value endpoint, no history",
                    }
                )
                continue

            ok_i, msg_i = run_cli(
                [
                    "ingest",
                    s,
                    ds,
                    "--start",
                    WINDOW_START.isoformat(),
                    "--end",
                    TOPUP_END.isoformat(),
                ],
                INGEST_TIMEOUT,
            )
            ok_t, msg_t = (False, "not attempted")
            if ok_i:
                ok_t, msg_t = run_cli(
                    [
                        "transform",
                        s,
                        ds,
                        "--start",
                        WINDOW_START.isoformat(),
                        "--end",
                        WINDOW_END.isoformat(),
                        "--reingest",
                    ],
                    TRANSFORM_TIMEOUT,
                )

            status = "FETCHED" if (ok_i and ok_t) else "FETCH_ERROR"
            checkpoint(
                {
                    "phase": "fetch",
                    "source": s,
                    "dataset": ds,
                    "status": status,
                    "ingest": msg_i,
                    "transform": msg_t,
                }
            )
            log(f"  {status:12s} {s}/{ds}")

            consecutive = 0 if status == "FETCHED" else consecutive + 1
            if consecutive >= VENDOR_FAILURE_LIMIT:
                parked = True
                log(f"!! parking vendor {source} after {consecutive} consecutive failures")


# ----------------------------------------------------------------- phase 2


def pick_time_column(cols: list[str]) -> str | None:
    for c in TIME_COLUMNS:
        if c in cols:
            return c
    return None


def numeric_columns(df: pl.DataFrame) -> list[str]:
    return [
        c
        for c, t in zip(df.columns, df.dtypes, strict=True)
        if t.is_numeric() and c not in METADATA_COLUMNS
    ]


def export_all(
    scope: set[tuple[str, str]] | None = None, full_history: bool = False
) -> list[dict[str, Any]]:
    """Export chartable CSVs for the (scoped) catalogue.

    Args:
        scope: see ``load_catalogue``.
        full_history: if True, skip the WINDOW_START..WINDOW_END filter and
            export each relation's entire history instead. Added at T-23:
            the fixed 1-5 Aug 2026 window was calibrated against the
            2026-08-16 extract's vendors and does not overlap datasets whose
            own history sits elsewhere (e.g. a forward-looking forecast
            ingested on a later date, or a long archive). Only meaningful
            combined with ``scope`` — a full-catalogue full-history run would
            re-export years of Elexon/ENTSO-E data.
    """
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("SET TimeZone='UTC'")
    available = {
        r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    }

    results: list[dict[str, Any]] = []
    for source, ds, _cfg in load_catalogue(scope):
        rec: dict[str, Any] = {"source": source, "dataset": ds}
        try:
            base = f"silver_{source}_{ds}"
            latest = f"{base}_latest"
            relation = latest if latest in available else base
            if relation not in available:
                rec |= {"status": "NO_RELATION", "chartable": False}
                results.append(rec)
                continue
            rec["relation"] = relation
            rec["vintage_resolved"] = relation == latest

            cols = [r[1] for r in con.execute(f'PRAGMA table_info("{relation}")').fetchall()]
            tcol = pick_time_column(cols)
            if tcol is None:
                rec |= {
                    "status": "NOT_CHARTABLE",
                    "reason": "no recognised time column (reference/lookup table)",
                    "chartable": False,
                }
                results.append(rec)
                continue
            rec["time_column"] = tcol

            if full_history:
                df = con.execute(f'SELECT * FROM "{relation}" ORDER BY "{tcol}"').pl()
            else:
                df = con.execute(
                    f'SELECT * FROM "{relation}" '
                    f'WHERE CAST("{tcol}" AS DATE) BETWEEN ? AND ? ORDER BY "{tcol}"',
                    [WINDOW_START, WINDOW_END],
                ).pl()

            if df.is_empty():
                rec |= {"status": "EMPTY", "rows": 0, "chartable": False}
                results.append(rec)
                continue

            nums = numeric_columns(df)
            rec["value_columns"] = nums

            # Duplicate resolution. Compare on every non-metadata column: if the
            # duplicates are identical there, collapsing them is lossless. If
            # they differ, the disagreement is real and this script must not
            # pick a winner.
            signal = [c for c in df.columns if c not in METADATA_COLUMNS]
            before = df.height
            deduped = df.unique(subset=signal, keep="first", maintain_order=True)
            removed = before - deduped.height

            key = [tcol] + [
                c for c in signal if c not in nums and c != tcol and df[c].n_unique() > 1
            ]
            residual = deduped.height - deduped.select(key).n_unique() if key else 0

            if residual > 0:
                rec |= {
                    "status": "DIVERGENT",
                    "rows": before,
                    "duplicate_rows_removed": removed,
                    "residual_conflicting_keys": residual,
                    "chartable": False,
                    "reason": (
                        "rows share a key but carry different values; resolving "
                        "needs vendor semantics this run does not have"
                    ),
                }
                out_df = df
            else:
                rec |= {
                    "status": "OK_DEDUPED" if removed else "OK",
                    "rows": deduped.height,
                    "duplicate_rows_removed": removed,
                    "chartable": bool(nums),
                }
                out_df = deduped

            if (source, ds) in N17_UNCHARTABLE:
                rec["chartable"] = False
                rec["reason"] = (
                    "N-17: reference family carries no vendor timestamp; "
                    "event_time is the transform run date, not a real event time"
                )
            if not nums:
                rec["chartable"] = False
                rec.setdefault("reason", "no non-metadata numeric column to plot")

            # Dup-ratio: fraction of rows retained after lossless dedup. 1.000
            # means no duplication. Computed for every OK/OK_DEDUPED dataset so
            # a caller can gate on it without re-deriving rows/duplicate_rows_removed.
            if before:
                rec["dup_ratio"] = round(deduped.height / before, 6)

            out_dir = OUT_ROOT / source
            out_dir.mkdir(parents=True, exist_ok=True)
            path = out_dir / f"{ds}.csv"
            out_df.write_csv(path)
            rec["csv"] = str(path)
            tv = out_df[tcol]
            rec["coverage"] = {"min": str(tv.min()), "max": str(tv.max())}
            rec["event_days"] = out_df.select(pl.col(tcol).cast(pl.Date).n_unique()).item()

        except Exception as exc:  # noqa: BLE001 - status record, never abort the run
            rec |= {"status": "ERROR", "detail": f"{type(exc).__name__}: {exc}"[:300]}
            rec["chartable"] = False

        results.append(rec)
        checkpoint({"phase": "export", **rec})

    return results


def _build_manifest(results: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for r in results:
        counts[r.get("status", "?")] = counts.get(r.get("status", "?"), 0) + 1
    chartable = [r for r in results if r.get("chartable")]
    total_dupes = sum(r.get("duplicate_rows_removed", 0) or 0 for r in results)

    return {
        "generated_by": "extract_all.py",
        "window": {"start": WINDOW_START.isoformat(), "end": WINDOW_END.isoformat()},
        "catalogue_size": len(results),
        "chartable_count": len(chartable),
        "duplicate_rows_removed_total": total_dupes,
        "status_counts": counts,
        "series": results,
    }


def _write_summary_txt(results: list[dict[str, Any]]) -> None:
    counts: dict[str, int] = {}
    for r in results:
        counts[r.get("status", "?")] = counts.get(r.get("status", "?"), 0) + 1
    chartable = [r for r in results if r.get("chartable")]
    total_dupes = sum(r.get("duplicate_rows_removed", 0) or 0 for r in results)

    lines = [
        "GRIDFLOW CHART DATA EXTRACT",
        f"window {WINDOW_START} .. {WINDOW_END}",
        f"catalogue: {len(results)} datasets",
        f"chartable: {len(chartable)}",
        f"duplicate rows removed (lossless): {total_dupes}",
        "",
        "status counts:",
    ]
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        lines.append(f"  {k:24s} {v}")
    lines += ["", "chartable series:"]
    for r in sorted(chartable, key=lambda x: (x["source"], x["dataset"])):
        lines.append(
            f"  {r['source']}/{r['dataset']:<34s} {r.get('rows', 0):>7} rows  "
            f"{len(r.get('value_columns', []))} numeric col(s)"
        )
    notable = [r for r in results if r.get("status") in {"DIVERGENT", "ERROR"}]
    if notable:
        lines += ["", "needs a human look:"]
        for r in notable:
            lines.append(
                f"  {r['source']}/{r['dataset']}: {r['status']} - "
                f"{r.get('reason') or r.get('detail', '')}"
            )
    (OUT_ROOT / "SUMMARY.txt").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


def write_summary(results: list[dict[str, Any]], scoped: bool) -> None:
    """Write manifest.json + SUMMARY.txt.

    Args:
        results: freshly computed per-dataset records from this run.
        scoped: if True, MERGE ``results`` into the existing manifest.json
            (replacing only the scoped (source, dataset) entries) instead of
            overwriting the whole file. An unscoped run keeps the original
            overwrite-the-whole-catalogue behaviour.
    """
    manifest_path = OUT_ROOT / "manifest.json"
    if scoped and manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        by_key = {(r["source"], r["dataset"]): r for r in existing.get("series", [])}
        for r in results:
            by_key[(r["source"], r["dataset"])] = r
        merged = list(by_key.values())
    else:
        merged = results

    manifest = _build_manifest(merged)
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    _write_summary_txt(merged if scoped else results)


def _parse_scope(source: str | None, datasets: str | None) -> set[tuple[str, str]] | None:
    if not source:
        return None
    if not datasets:
        raise SystemExit("--datasets is required when --source is given")
    return {(source, d.strip()) for d in datasets.split(",") if d.strip()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("phase", nargs="?", default="all", choices=["all", "fetch", "export"])
    parser.add_argument("--source", default=None, help="scope to one source (requires --datasets)")
    parser.add_argument(
        "--datasets", default=None, help="comma-separated dataset names under --source"
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help=(
            "export each relation's full history instead of the fixed "
            "1-5 Aug 2026 window (requires --source; see export_all docstring)"
        ),
    )
    args = parser.parse_args()
    scope = _parse_scope(args.source, args.datasets)
    if args.full_history and scope is None:
        raise SystemExit("--full-history requires --source/--datasets")

    if args.phase in ("all", "fetch"):
        log("=== phase 1: ingest + transform ===")
        fetch_all(scope)
    if args.phase in ("all", "export"):
        log("=== phase 2: register views ===")
        run_cli(["init"], 300)
        log("=== phase 3: export ===")
        write_summary(export_all(scope, full_history=args.full_history), scoped=scope is not None)
    log("done")


if __name__ == "__main__":
    main()
