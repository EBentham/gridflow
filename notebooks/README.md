# gridflow notebooks

Read-only tours of the data layer, written as jupytext `py:percent` files. The
`.py` files are the source; `.ipynb` files are local renders, gitignored, and CI
fails on a tracked one.

- `00_overview.py`: start here. What is configured, what the catalogue holds and
  how fresh it is, ad hoc SQL, and the commands that refresh or wipe the store.
- `01_sources.py`: the lifecycle on one source (discover, refresh, query,
  inspect, reset), then what differs per source: the ENTSO-E and GIE API keys,
  Open-Meteo's weather roles, and GIE's two registers and storage alias.
- `_TEMPLATE.py`: the first cells to copy for a new notebook.

The model notebooks (forecasts, the merit-order stack, the SMP backtest) live in
gridflow-models.

## Setup

```bash
uv sync --group notebooks                        # jupyterlab, jupytext, ipykernel, plotly
uv run gridflow init                             # create the catalogue if you haven't
uv run --group notebooks jupyter lab notebooks/
```

JupyterLab opens a `.py` file as plain text. Right-click it and choose
**Open With → Notebook**. The default Python 3 kernel is the project's
environment, so `import gridflow` works without registering a kernel.

The notebooks open the catalogue that gridflow's settings point at, the same one
the `gridflow` CLI uses. A notebook also runs as a script, which is a quick check
that it still works top to bottom:

```bash
uv run --group notebooks python notebooks/01_sources.py
```

Cells read a window that defaults to the last 7 complete UTC days. Set
`GRIDFLOW_NB_START` and `GRIDFLOW_NB_END` (inclusive, `YYYY-MM-DD`) to read
another.

## Reading, never writing

`GridflowClient` opens the catalogue read-only. The steps that fetch or delete
(`gridflow backfill`, `gridflow pipeline`, `gridflow reset`) are markdown cells
showing the command to run in a terminal.

DuckDB lets one process write the catalogue at a time, and an open notebook holds
it. Run `gf.close()` before a command that writes, then re-run the notebook's
Bootstrap cell.

## Conventions

- **Read through `show(view, query)`.** When the view isn't in the catalogue it
  prints `skipped: <view> not ingested (gridflow backfill ...)`, naming the API
  key the command needs, instead of failing. A fresh clone runs every notebook
  to the end.
- **Polars only, never pandas.** plotly takes Polars frames as they are.
- **No pipeline logic in notebooks.** A function a notebook needs goes into
  `src/` with tests first.
- **Run cells top to bottom.** Later cells use what earlier cells defined.

`tests/notebooks/test_notebook_static.py` checks that every notebook here
compiles, imports neither pandas nor the gridflow-models package, and names only
views the catalogue registers. `tests/notebooks/test_notebooks_run.py` runs every
`NN_*.py` notebook top to bottom against a sample store, a temp data root built
from the test fixtures, which cover 15 January 2024. It fails on an uncaught
error, on a `NameError`, `UnboundLocalError` or `AttributeError` in either output
stream, or when `01_sources.py` shows fewer than 8 of its 11 datasets.

## Adding a notebook

Copy `_TEMPLATE.py` to `notebooks/NN_<topic>.py`, then run it top to bottom once.
