# Quickstart

From a fresh clone to notebooks with data in them. The
[README](../README.md#quickstart) has the commands; this page explains each step.

## 1. Install

```bash
git clone https://github.com/EBentham/gridflow.git && cd gridflow
uv sync --group notebooks
```

`uv sync` installs gridflow and its `gridflow` command into `.venv`, fetching
Python 3.12 if needed (gridflow supports 3.11 and later). `--group notebooks`
adds JupyterLab, jupytext and plotly. To run the tests as well, add
`--extra dev` for pytest, ruff and mypy. `uv run gridflow --version` prints the
installed version.

## 2. The data root

```bash
uv run gridflow init
```

`init` creates the data root and its DuckDB catalogue, and prints the
catalogue's path. Gold views register later, once a backfill fetches their
inputs. By default, gridflow
stores data in `~/gridflow-data`, with the catalogue at
`~/gridflow-data/gridflow.duckdb`, whichever folder you run it from. To move
the data root, set one variable in your shell or in an untracked `.env` in
(or above) the folder you run from:

```env
GRIDFLOW_DATA_DIR=<absolute local data root>
```

The catalogue and logs follow it; `GRIDFLOW_DUCKDB_PATH` and
`GRIDFLOW_LOG_DIR` are optional overrides. Relative paths resolve against the
current folder. Precedence is the environment (including `.env`), then
`config/settings.yaml` in the current folder, then built-in defaults.
Keep cloud-synced folders such as OneDrive or Google Drive out of the data
root; sync clients can lock the DuckDB file and make reads or writes fail.

## 3. API keys

```bash
cp .env.example .env
```

Only ENTSO-E (`ENTSOE_API_KEY`) and GIE (`GIE_API_KEY`, one key for AGSI and
ALSI) need a key. Both are free; the README's
[key table](../README.md#what-needs-a-key) links the registration pages, and
`.env.example` says what each key unlocks. gridflow reads keys from the
environment, filled from the nearest `.env` at or above the folder you run
from, and a variable already set in the shell wins over `.env`.

A key is needed to fetch, never to read what is stored. Without one, a preset
skips the source with a line naming the variable, and the notebooks print a
skip line for its views instead of failing. Each vendor's terms and the
attribution they ask for are in [DATA-SOURCES.md](../DATA-SOURCES.md).

## 4. First ingest: presets

```bash
uv run gridflow backfill --preset tour
```

A preset is a list of datasets and one window. `gridflow backfill --preset`
takes a packaged preset's name or the path of a `.yaml` file, skips each source
whose API key is unset, and backfills the rest through the same path as
`gridflow backfill <source>`: bronze, then silver, then the catalogue views,
one day at a time unless an Open-Meteo entry's `chunk_days` or `--chunk-days N`
says otherwise.

| Preset | Datasets | Window | Requests | Time |
|---|---|---|---|---|
| `tour` | 10: the views `00_overview` and `01_sources` read | last 7 complete UTC days | 53 without keys | not re-measured |

Without keys the tour fetches 7 of its 10 datasets: Elexon system prices and
generation by fuel (14 requests, one per day each), Open-Meteo demand, wind and
solar weather (25, one per weather site for the whole week, as the tour sets
`chunk_days: 7` on them), ENTSO-G physical flows (7, one per day) and NESO
carbon intensity (7). The time is not re-measured. A run on 2 November 2026
with no keys set, when Open-Meteo still took one-day chunks (175 of 203
requests), took just under 4 minutes. Most of that was spent waiting on
Open-Meteo: 30 of its connections failed after about 10 seconds and succeeded
when retried. With keys the tour also fetches ENTSO-E day-ahead prices and GIE
storage and LNG, and gridflow sends both vendors one request a second.

The run prints WARNING lines along the way, for example one per day for Elexon
generation by fuel, which one-day windows raise by design, or one per retried
request. Afterwards `01_sources.py` shows every dataset the tour fetched.

Your own preset is a YAML file with two keys:

```yaml
datasets:
  - {source: elexon, dataset: indo}
  - {source: open_meteo, dataset: historical_demand, chunk_days: 31}
window: {start: 2024-01-01, end: 2024-01-31}   # or {last_days: 30}
```

`last_days: N` is the N complete UTC days before today. `start` and `end` read
as `--start` and `--end` do, so a bare end date is included. Every source and
dataset must be one gridflow configures (the `sources` of
`gridflow.config.settings.load_settings()`), listed once, or the preset fails
before any request.

`chunk_days` is optional and only for Open-Meteo: a whole number from 1 to 35,
the days each request covers (one request per weather site per chunk). Without
it an entry backfills one day at a time. On any other source, or outside 1 to
35, the preset fails before any request. Other sources keep one-day chunks
because NESO files a multi-day response under its first day, so a rerun a day
later would write those days twice. `--chunk-days N` replaces every entry's
chunk size, `chunk_days` included. gridflow-models keeps its own preset, for the model notebooks, and runs
it the same way: `gridflow backfill --preset configs/ingest/models.yaml`.

## 5. Notebooks

```bash
uv run --group notebooks python notebooks/01_sources.py   # run one as a script
uv run --group notebooks jupyter lab notebooks/            # or open them
```

The notebooks are jupytext `.py` files: in JupyterLab, right-click one and
choose **Open With → Notebook**. They read the catalogue the CLI writes, over
the last 7 complete UTC days, the window the tour fetches, and print a skip
line for a view not ingested yet. Run as a script after the tour without keys,
`01_sources.py` shows everything but the ENTSO-E and GIE sections.
[notebooks/README.md](../notebooks/README.md) covers the rest: another window,
closing the read-only connection before a command that writes, and adding a
notebook.

From your own Python, read the same catalogue with the client the notebooks use:

```python
from gridflow.serving.client import GridflowClient

client = GridflowClient()
df = client.query("SELECT * FROM silver_elexon_system_prices LIMIT 100")
```

## 6. After the tour

```bash
uv run gridflow pipeline elexon --all --last 24h     # every Elexon dataset, last day
uv run gridflow backfill entsoe day_ahead_prices --start 2024-01-01 --end 2024-12-31
uv run gridflow status                               # run history and watermarks
```

`pipeline` fetches and transforms one window; `backfill` walks a long window in
chunks (`--chunk-days`, default 1). [CLI_CHEAT_SHEET.md](CLI_CHEAT_SHEET.md)
lists every command.
