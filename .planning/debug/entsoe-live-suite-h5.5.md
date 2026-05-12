# Debug Session - ENTSO-E Live Suite H5.5

## Command

```powershell
uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -v -rs
```

## Baseline

- Result: 12 failed, 46 passed, 6 deselected.
- Hard API failure: `activated_balancing_qty` returned HTTP 400 for
  `documentType=A83`, `processType=A16`, `businessType=A95`.
- Follow-up A83 probing for GB also rejected `businessType=A96` without
  `processType`; active UK-centric all-dataset ingestion should not include A83.
- Data availability failures: several fixed-date GB balancing/unit endpoints
  returned HTTP 200 acknowledgement XML with `No matching data found`.
- Payload-shape failure: `outages_generation` returned `application/zip`.

## Hypotheses

- A83 request metadata is wrong, not merely unavailable.
- `No matching data found` must be modeled as a live availability skip for the
  transform test, not as a transformer failure.
- Zip expansion belongs in the connector so bronze and silver keep receiving XML.
- Generic time-series parsing needs live flat unit tag support.
- Outage parsing needs live `Available_Period` and
  `production_RegisteredResource.*` support.
