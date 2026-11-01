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
# # New data-layer notebook
#
# Copy this file to `notebooks/NN_<topic>.py` (a two-digit prefix), rename the
# title and keep the three code cells below: they open the catalogue read-only,
# set the window, and define the `show` and `window` helpers that
# `01_sources.py` explains.

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
        source, dataset = _GOLD_INPUTS.get(view) or source_dataset(view)
        key = settings.sources[source].api_key_env
        needs = f"; needs {key}" if key else ""
        command = f"gridflow backfill {source} {dataset} --start {START} --end {END}"
        print(f"skipped: {view} not ingested ({command}{needs})")
    return None


def source_dataset(view: str) -> tuple[str, str]:
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
# ## Your cells
#
# Read every view through `show(...)`, so a source that isn't ingested skips.
# Notebooks only read: put a command that fetches or deletes in a markdown cell.
# To chart, `plotly.express` (in the `notebooks` group) takes a Polars frame as
# it is:
#
# ```python
# import plotly.express as px
#
# prices = show("silver_elexon_system_prices_latest", lambda: gf.get_system_prices(START, END))
# px.line(prices, x="timestamp_utc", y="system_sell_price") if prices is not None else None
# ```

# %%
