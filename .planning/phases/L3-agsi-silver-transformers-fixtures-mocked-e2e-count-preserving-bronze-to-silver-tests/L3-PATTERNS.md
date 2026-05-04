---
phase: L3
slug: agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests
status: complete
mapped: 2026-05-04
---

# Phase L3: Pattern Map

## File Classification

| New/Modified File | Role | Closest Analog | Match Quality |
|-------------------|------|----------------|---------------|
| `src/gridflow/silver/gie/agsi.py` | AGSI storage/reference/news/unavailability silver transforms | `src/gridflow/silver/entsog/generic.py`, `src/gridflow/silver/neso/carbon_intensity.py`, current AGSI transformer | strong |
| `src/gridflow/silver/gie/__init__.py` | Import registrations for all GIE transformers | Existing source package `__init__.py` modules | exact |
| `src/gridflow/schemas/gie.py` | Optional expanded AGSI schema contract | Current `GasStorage` schema | strong |
| `tests/unit/test_gie.py` | Focused AGSI transformer unit tests | Existing AGSI and ALSI tests in same file | exact |
| `tests/integration/test_gie_agsi_mocked_e2e.py` | Fixture-backed bronze-to-silver E2E tests | `tests/integration/test_entsog_mocked_e2e.py`, `tests/integration/test_neso_mocked_e2e.py` | exact |
| `tests/fixtures/gie/*.json` | Sanitized AGSI payload fixtures | Existing `tests/fixtures/gie/` payloads | exact |
| `docs/gie_agsi_endpoint_catalog.yaml` | Catalog-backed transform/defer status if needed | Existing L1/L2 catalog contract | exact |

## Patterns To Reuse

### Registered Transformer Integration

Source: `tests/integration/test_entsog_mocked_e2e.py`

Import the source silver package for registration, write a `RawResponse` through
`BronzeWriter`, call `get_transformer(source, dataset, tmp_data_dir)`, run the
transformer, and read the parquet file with Polars.

### Dataset Family Fixtures

Source: `tests/integration/test_neso_mocked_e2e.py`

Use a small `_body_for(dataset)` helper that returns representative payloads for
each parser family. For AGSI, keep payloads compact but shaped like the live API:
top-level `last_page`, `total`, `gas_day`, and `data` for storage; `data` rows
for listing/news/unavailability.

### Reference And Time-Series Output Paths

Source: `src/gridflow/silver/base.py`

Default `BaseSilverTransformer.run()` writes date-partitioned parquet. If a new
AGSI reference transformer intentionally writes a non-partitioned reference file,
follow the existing Elexon/NESO reference-data tests by using a helper that
asserts the correct path. If no custom reference writer exists, keep the default
date-partitioned path and document that choice in tests.

### Generic JSON Normalisation

Source: `src/gridflow/silver/entsog/generic.py`

For non-storage AGSI families, prefer deterministic field normalisation with
snake_case columns and tolerant datetime/numeric parsing over bespoke fragile
row-by-row logic.

## Anti-Patterns To Avoid

- Do not call the live AGSI API in L3 tests.
- Do not rely on the legacy `storage` dataset id only; `storage_reports` must
  be registered and tested.
- Do not deduplicate storage rows only by `gas_day` and `country_code`.
- Do not transform only direct `pl.DataFrame` inputs and skip the
  `BronzeWriter`/registry path.
- Do not introduce broad silver abstractions unless the GIE families actually
  share enough parser behavior.
- Do not weaken L2 request-shape and pagination tests while adding silver
  coverage.

## Verification Commands

```powershell
uv run --extra dev ruff check src/gridflow/silver/gie tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
uv run --extra dev pytest -m "not live" -q
```
