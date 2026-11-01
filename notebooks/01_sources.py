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
# # Data sources: one walkthrough for every source
#
# Every source lands in the catalogue the same way: one view per dataset, named
# `silver_<source>_<dataset>`. So the same few steps read any of them, and
# nothing per dataset needs learning beyond its name and its date column.
#
# The walkthrough runs the lifecycle **discover, refresh, query, inspect,
# reset** on one dataset. Change the strings in the "Pick a dataset" cell to
# walk another. The notes at the end cover what differs between sources: API
# keys, Open-Meteo's weather roles, and GIE's two registers and storage alias.

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
# ## Helpers
#
# `show(view, query)` runs `query` (SQL, or a client reader) when `view` is in
# the catalogue and returns the frame. Otherwise it prints, once per view, the
# command that ingests the view and the key it needs, and returns `None`: a
# source you haven't ingested skips instead of failing. `window(view, column)`
# is the SQL for the view's rows in the window's UTC days, dated by `column`.

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


def window(view: str, column: str) -> str:
    """SQL for `view`'s rows dated START to END by a date column or `timestamp_utc`."""
    if column == "timestamp_utc":
        # Explicit UTC offsets: a bare date literal would take the session's time zone.
        after = END + timedelta(days=1)
        where = f"{column} >= '{START} 00:00:00+00' AND {column} < '{after} 00:00:00+00'"
    else:
        where = f"{column} BETWEEN DATE '{START}' AND DATE '{END}'"
    return f"SELECT * FROM {view} WHERE {where} ORDER BY {column}"


# %% [markdown]
# ## Pick a dataset
#
# The source, the dataset, and the column that dates its rows (each dataset's
# is listed in `gridflow.silver.schema_manifest.DESIGNATED_DATE_COLS`).

# %%
SOURCE, DATASET, DATE_COLUMN = "elexon", "system_prices", "settlement_date"
VIEW = f"silver_{SOURCE}_{DATASET}"

# %% [markdown]
# ## 1. Discover
#
# `settings.sources[SOURCE].datasets` lists every dataset configured for the
# source in gridflow's `sources.yaml`, with how often it publishes and the
# longest window one request may ask for. `ingested` says whether the catalogue
# has its view yet.

# %%
datasets = pl.DataFrame(
    [
        {
            "dataset": name,
            "schedule": cfg.schedule,
            "max_query_days": cfg.max_query_days,
            "ingested": f"silver_{SOURCE}_{name}" in TABLES,
        }
        for name, cfg in settings.sources[SOURCE].datasets.items()
    ]
)
print(f"{SOURCE}: {datasets.height} datasets, {datasets['ingested'].sum()} ingested")
datasets

# %% [markdown]
# ## 2. Refresh (in a terminal)
#
# Three ways to fetch from the provider into the local store. Each ingests to
# bronze, transforms to silver and refreshes the catalogue views:
#
# ```bash
# gridflow backfill elexon system_prices --start 2026-10-01 --end 2026-10-07  # a window
# gridflow pipeline elexon system_prices --last 1d   # the most recent day
# gridflow pipeline elexon --all --last 1d           # every dataset on the source
# ```
#
# DuckDB lets one process write the catalogue at a time. Run `gf.close()` before
# a command that writes, then re-run the Bootstrap cell.

# %% [markdown]
# ## 3. Query
#
# Reading never calls the provider: it reads only what is already stored.

# %%
show(VIEW, window(VIEW, DATE_COLUMN))

# %% [markdown]
# ## 4. Inspect
#
# - `DESCRIBE`: the view's columns and their types;
# - coverage: the runs that wrote the source's datasets, from `pipeline_runs`;
# - tail: the newest stored rows.

# %%
show(VIEW, f"DESCRIBE {VIEW}")

# %%
gf.query(f"""
    SELECT dataset, arg_max(status, started_at) AS last_status,
           max(completed_at) AS last_run, sum(rows_out)::BIGINT AS rows_written
    FROM pipeline_runs WHERE source = '{SOURCE}'
    GROUP BY dataset ORDER BY dataset
""")

# %%
show(VIEW, f"SELECT * FROM {VIEW} ORDER BY {DATE_COLUMN} DESC LIMIT 5")

# %% [markdown]
# ## 5. Reset (in a terminal)
#
# `gridflow reset <source> <dataset>` deletes one dataset's data, and a layer
# flag narrows it to bronze, silver or gold. `--dry-run` lists what it would
# delete, and without `--yes` it asks first:
#
# ```bash
# gridflow reset elexon system_prices --silver --dry-run
# gridflow reset elexon system_prices --silver --yes
# gridflow transform elexon system_prices --start 2026-10-01 --end 2026-10-07  # rebuild silver
# ```
#
# Bronze holds the raw provider responses, so wiping it means fetching again.

# %% [markdown]
# ## Per-source notes
#
# ### Elexon
#
# GB balancing-market data, no API key. `system_prices` is the imbalance price
# walked above. Its silver view keeps every settlement run (II, SF, R1 and so
# on) as its own vintage; `silver_elexon_system_prices_latest` keeps the latest
# run per settlement period and is what `gf.get_system_prices` reads. `fuelhh`
# is half-hourly generation by fuel type, read by `gf.get_fuel_generation`.
# Others include `indo` (initial demand outturn) and `mid` (the market index
# price).

# %%
show("silver_elexon_system_prices_latest", lambda: gf.get_system_prices(START, END))

# %%
show("silver_elexon_fuelhh", lambda: gf.get_fuel_generation(START, END))

# %% [markdown]
# ### ENTSO-E
#
# European transparency-platform data. Ingest needs an API key in
# `ENTSOE_API_KEY` (register at
# https://transparency.entsoe.eu/usrm/user/myAccountSettings); reading what you
# have stored needs none. gridflow sends the key as the `securityToken` query
# parameter. ENTSO-E's GB day-ahead prices stop at Brexit, so for a GB
# day-ahead price gridflow uses Elexon's market index
# (`gold_gb_day_ahead_benchmark`).

# %%
show("silver_entsoe_day_ahead_prices", window("silver_entsoe_day_ahead_prices", "timestamp_utc"))

# %% [markdown]
# ### NESO
#
# National Energy System Operator data, no API key. `carbon_intensity` is GB
# carbon intensity per half hour, forecast and actual; the `regional_*`
# datasets break it down by region. NESO's open-data portal is a separate
# source, `neso_data_portal`.

# %%
show("silver_neso_carbon_intensity", window("silver_neso_carbon_intensity", "timestamp_utc"))

# %% [markdown]
# ### Open-Meteo
#
# Weather reanalysis, no API key, split by role. `historical_demand` covers 7
# UK population centres and is what `gf.get_weather` reads; `historical_wind`
# covers 12 GB wind sites and `historical_solar` 6 GB solar sites, each with
# its own variables. Each has a `forecast_*` twin. The sites differ by role, so
# read the one that matches what you are modelling. The reanalysis archive
# carries wind at 10 m and 100 m only, so the wind view's 80, 120 and 180 m
# columns are empty in history.

# %%
wind, solar = "silver_open_meteo_historical_wind", "silver_open_meteo_historical_solar"
weather = {
    "demand": show("silver_open_meteo_historical_demand", lambda: gf.get_weather(START, END)),
    "wind": show(wind, window(wind, "timestamp_utc")),
    "solar": show(solar, window(solar, "timestamp_utc")),
}
pl.DataFrame(
    [
        {"role": role, "rows": frame.height, "sites": frame["location"].n_unique()}
        for role, frame in weather.items()
        if frame is not None
    ],
    schema={"role": pl.String, "rows": pl.Int64, "sites": pl.Int64},
)

# %% [markdown]
# ### ENTSO-G
#
# European gas transmission data, no API key. `physical_flows` is the gas flow
# at each interconnection point.

# %%
show("silver_entsog_physical_flows", window("silver_entsog_physical_flows", "timestamp_utc"))

# %% [markdown]
# ### GIE (AGSI and ALSI)
#
# Gas Infrastructure Europe publishes two registers behind one API key,
# `GIE_API_KEY` (register at https://agsi.gie.eu/account), needed for ingest
# only; gridflow sends it in the `x-key` header. gridflow treats them as two
# sources: `gie_agsi` for gas storage and `gie_alsi` for LNG terminals.
#
# AGSI's physical dataset is `storage`. For reading there is also
# `gold_eu_gas_storage`, a gold view over it with fewer columns, which
# `gf.get_gas_storage` reads (serving alias `gas_storage`). Both return the same
# rows. Ingest and reset take the physical name, `storage`; the gold view
# follows when the catalogue refreshes.

# %%
show("silver_gie_agsi_storage", window("silver_gie_agsi_storage", "gas_day"))
show("gold_eu_gas_storage", lambda: gf.get_gas_storage(START, END))

# %%
show("silver_gie_alsi_lng", window("silver_gie_alsi_lng", "gas_day"))

# %% [markdown]
# ## Next steps
#
# - [`00_overview.py`](./00_overview.py): what spans every source (the
#   catalogue, freshness, ad hoc SQL, refresh and wipe).
# - [`README.md`](./README.md): setup, and copying `_TEMPLATE.py` for a new
#   notebook.
