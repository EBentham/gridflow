# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: title,-all
#     notebook_metadata_filter: -kernelspec
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
# ---

# %% [markdown]
# # Cross-source overview: the gridflow data layer
#
# Start here. This notebook covers what spans every source: what is configured,
# what the catalogue holds and how fresh it is, ad hoc SQL, and the commands
# that refresh or wipe the store. `01_sources.py` walks a single source.
#
# The notebooks only read. `GridflowClient` opens the DuckDB catalogue
# read-only, and every step that would fetch or delete is a markdown cell
# showing the `gridflow` command to run in a terminal instead.

# %% [Bootstrap]
# ruff: noqa: B018
# B018: a bare trailing frame is the jupytext idiom for rendering a table.
import os
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import polars as pl

from gridflow.config.settings import load_settings
from gridflow.serving.client import GridflowClient

settings = load_settings()
gf = GridflowClient()  # read-only; raises until `gridflow init` has created the catalogue
TABLES = set(gf.get_tables())
print(f"GridflowClient OK: {len(TABLES)} tables and views")

# %% [Parameters]
# The window the cells read: the last 7 complete UTC days, unless GRIDFLOW_NB_START
# and GRIDFLOW_NB_END (inclusive, YYYY-MM-DD) are set before Jupyter starts.
_last = datetime.now(UTC).date() - timedelta(days=1)  # the last complete UTC day
_first = _last - timedelta(days=6)
START = date.fromisoformat(os.environ.get("GRIDFLOW_NB_START") or _first.isoformat())
END = date.fromisoformat(os.environ.get("GRIDFLOW_NB_END") or _last.isoformat())
print(f"window: {START} to {END}")

# %% [markdown]
# ## Reading a view that may not be there
#
# A fresh store holds nothing, and two sources need an API key to ingest. So
# every read goes through `show(view, query)`. When `view` is in the catalogue
# it runs `query`, which is SQL or a client reader such as
# `lambda: gf.get_system_prices(START, END)`, and returns the frame. When it is
# not, it prints the command that ingests it and the key that command needs,
# once per view, and returns `None`.

# %%
_GOLD_INPUTS = {"gold_eu_gas_storage": ("gie_agsi", "storage")}
_skipped: set[str] = set()


def show(view: str, query: str | Callable[[], pl.DataFrame]) -> pl.DataFrame | None:
    """The frame `query` returns when `view` is in the catalogue, else a skip line."""
    if view in TABLES:
        frame = gf.query(query) if isinstance(query, str) else query()
        print(f"{view}: {frame.height} rows")
        return frame
    if view not in _skipped:
        _skipped.add(view)
        source, dataset = _GOLD_INPUTS.get(view) or _source_dataset(view)
        key = settings.sources[source].api_key_env
        needs = f"; needs {key}" if key else ""
        command = f"gridflow backfill {source} {dataset} --start {START} --end {END}"
        print(f"skipped: {view} not ingested ({command}{needs})")
    return None


def _source_dataset(view: str) -> tuple[str, str]:
    """Split a silver view name on the longest configured source name it starts with."""
    name = view.removeprefix("silver_").removesuffix("_latest")
    source = max((s for s in settings.sources if name.startswith(f"{s}_")), key=len)
    return source, name.removeprefix(f"{source}_")


# %% [markdown]
# ## 1. What is configured, what is stored, how fresh it is
#
# - `settings.sources`: every source gridflow can ingest, with its dataset count
#   and the environment variable holding its API key (blank when it needs none);
# - `gf.get_tables()`: every table and view in the catalogue;
# - `pipeline_runs`: one row per ingest or transform run, which is where
#   `gridflow status` reads its history.

# %%
sources = pl.DataFrame(
    [
        {"source": name, "datasets": len(cfg.datasets), "api_key_env": cfg.api_key_env}
        for name, cfg in settings.sources.items()
    ]
)
print(f"sources configured: {sources.height}")
sources

# %% [markdown]
# The catalogue has one view per silver dataset on disk, named
# `silver_<source>_<dataset>`; a `_latest` view for each dataset that keeps
# several vintages; and the gold views that join sources. The NESO data-portal
# families register an empty view at `gridflow init`, so a view can exist
# before any data does.

# %%
views = sorted(t for t in TABLES if t.startswith(("silver_", "gold_")))
print(f"catalogue: {len(views)} views, {len(TABLES) - len(views)} other tables")
pl.DataFrame({"view": views}).head(10)

# %% [markdown]
# Freshness per dataset: the last run's status, when a run last succeeded and
# how many hours ago. Every source in one query, so it stays quick on a full
# store.

# %%
freshness = gf.query("""
    SELECT source, dataset,
           arg_max(status, started_at) AS last_status,
           max(completed_at) FILTER (WHERE status IN ('success', 'completed_with_warnings'))
               AS last_ok,
           date_diff('hour', last_ok, now()) AS hours_since_ok,
           count(*) AS runs
    FROM pipeline_runs
    GROUP BY source, dataset
    ORDER BY source, dataset
""")
if freshness.is_empty():
    print("no pipeline runs yet: the skip lines below name the commands that ingest")
freshness

# %% [markdown]
# ## 2. Refreshing the store (in a terminal)
#
# Notebooks never write. Each of these fetches from the provider into bronze,
# transforms to silver and refreshes the catalogue views:
#
# ```bash
# gridflow pipeline elexon system_prices --last 1d   # one dataset, the latest day
# gridflow pipeline elexon --all --last 1d           # every dataset on the source
# gridflow backfill elexon system_prices --start 2026-10-01 --end 2026-10-07
# ```
#
# `--all` fetches every dataset configured for the source (the `datasets`
# column above), so it can take a while.
#
# DuckDB lets one process write the catalogue at a time, and this notebook holds
# it open. Run `gf.close()` before a command that writes, then re-run the
# Bootstrap cell so `TABLES` sees the new views.

# %% [markdown]
# ## 3. Ad hoc SQL with `gf.query(...)`
#
# `gf.query(sql)` runs read-only SQL against the catalogue and returns a Polars
# frame. It is the escape hatch for joins and aggregations the named readers
# (`get_system_prices`, `get_fuel_generation`, `get_weather`, `get_gas_storage`)
# don't cover; `01_sources.py` shows those readers source by source.

# %%
show("silver_elexon_system_prices", "SELECT * FROM silver_elexon_system_prices LIMIT 5")

# %%
show(
    "silver_elexon_system_prices",
    "SELECT COUNT(*) AS row_count, MIN(timestamp_utc) AS earliest, MAX(timestamp_utc) AS latest "
    "FROM silver_elexon_system_prices",
)

# %% [markdown]
# ## 4. Wiping the store (in a terminal)
#
# `gridflow reset` deletes bronze, silver and gold and recreates the catalogue.
# It asks before deleting unless you pass `--yes`, and `--dry-run` lists what it
# would delete. A source, a dataset and a layer flag narrow it:
#
# ```bash
# gridflow reset --dry-run                                 # preview a full wipe
# gridflow reset elexon system_prices --silver --dry-run   # one dataset, one layer
# gridflow reset elexon system_prices --silver --yes
# ```
#
# Bronze holds the raw provider responses, so wiping it means fetching again.
# Silver can be rebuilt from bronze with `gridflow transform`.

# %% [markdown]
# ## Next steps
#
# - [`01_sources.py`](./01_sources.py): the lifecycle on one source, then what
#   differs per source (API keys, Open-Meteo's weather roles, GIE's two registers
#   and storage alias).
# - [`README.md`](./README.md): setup, and copying `_TEMPLATE.py` for a new
#   notebook.
