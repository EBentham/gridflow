---
slug: neso-settlement-period-iteration
status: complete
completed: 2026-05-04
---

# Summary

Updated the NESO connector so `intensity_period` expands one date into every
valid GB settlement period instead of only requesting period 1. Normal dates now
produce 48 requests, spring DST transition dates produce 46, and autumn DST
transition dates produce 50.

## Verification

- `python -m ruff check src\gridflow\connectors\neso tests\integration\test_neso_mocked_e2e.py tests\unit\test_neso_endpoints.py`
- `python -m pytest tests\unit\test_neso.py tests\unit\test_neso_endpoints.py tests\integration\test_neso_mocked_e2e.py -q`
- `python -m pytest tests\integration\test_neso_live_e2e.py tests\integration\test_neso_cli_live_smoke.py tests\endpoints\test_endpoint_live.py::TestNesoLive -q -m live`
- `python -m pytest -q -m "not live"`
- Live `python -m gridflow pipeline neso intensity_period --start 2026-04-22 --end 2026-04-22` wrote 48 bronze payloads, 48 bronze sidecars, and 48 silver rows.
