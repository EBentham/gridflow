---
status: complete
phase: L4
updated: 2026-05-04
---

# L4 User Setup - GIE AGSI Live Gates

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `GIE_API_KEY` | Yes for `-m live` | Sent to GIE AGSI as the lowercase `x-key` header. |
| `GRIDFLOW_DATA_DIR` | Recommended for manual CLI runs | Keeps live bronze/silver outputs outside normal project data. |
| `GRIDFLOW_DUCKDB_PATH` | Recommended for manual CLI runs | Keeps live catalogue state isolated. |
| `GRIDFLOW_LOG_DIR` | Recommended for manual CLI runs | Keeps live command logs isolated. |

## Verification

Automated pytest CLI smoke tests set isolated `GRIDFLOW_*` paths themselves.
Manual command examples are documented in `L4-LIVE-COMMANDS.md`.
