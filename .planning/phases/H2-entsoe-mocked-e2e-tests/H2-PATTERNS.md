---
phase: H2
slug: entsoe-mocked-e2e-tests
status: complete
mapped: 2026-05-02
---

# Phase H2: Pattern Map

## File Classification

| New/Modified File | Role | Closest Analog | Match Quality |
|-------------------|------|----------------|---------------|
| `pyproject.toml` | dependency metadata | existing project dependency lists | exact |
| `tests/integration/test_entsoe_mocked_e2e.py` | integration test | `tests/integration/test_entsoe_connector.py` + `tests/integration/test_bronze_to_silver.py` | exact |

## Patterns To Reuse

### Mocked HTTP With respx

Source: `tests/integration/test_entsoe_connector.py`

Use:

```python
@respx.mock
@pytest.mark.asyncio
async def test_...(entsoe_config):
    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )
    async with EntsoeConnector(entsoe_config) as connector:
        await connector.fetch(...)
    params = dict(route.calls[0].request.url.params)
```

Apply to H2 URL tests, but parametrize over all 16 datasets and inspect every call for
domain/query-shape correctness.

### Bronze-To-Silver Pipeline Test

Source: `tests/integration/test_bronze_to_silver.py`

Use:

```python
writer = BronzeWriter(tmp_data_dir)
bronze_path = writer.write(raw_response)
rows = transformer.run(date(2024, 1, 15))
df = pl.read_parquet(silver_path)
```

Apply to ENTSO-E XML fixtures by constructing `RawResponse` with:

- `source="entsoe"`
- `dataset=<dataset>`
- `content_type="text/xml"`
- `data_date=date(2024, 1, 15)`
- request metadata matching the connector params where practical

### Transformer Imports

Import concrete transformer modules directly:

- `gridflow.silver.entsoe.day_ahead_prices.DayAheadPricesTransformer`
- `gridflow.silver.entsoe.actual_load.ActualLoadTransformer`
- `gridflow.silver.entsoe.cross_border_flows.CrossBorderFlowsTransformer`
- `gridflow.silver.entsoe.imbalance_prices.ImbalancePricesTransformer`

Do not import broad `gridflow.silver.elexon` package modules in H2 tests. The known
Elexon package import blocker is unrelated to ENTSO-E mocked E2E coverage.

## H2 Test Design

### URL Shape Test

Create `TestEntsoeUrlConstructionAllDatasets` with:

- `test_config_and_doc_types_cover_same_16_datasets`
- `test_url_shape_for_every_dataset`, parametrized over `sorted(DOC_TYPES)`

Expected assertions per call:

- URL path is `/api`
- `securityToken == "test-token"`
- `documentType == DOC_TYPES[dataset].document_type`
- `periodStart == "202401150000"`
- `periodEnd == "202401160000"`
- `processType` is present only when expected
- control-area datasets use `controlArea_Domain.mRID` and no `in_Domain.mRID`
- zone and zone-pair datasets use `in_Domain.mRID` and `out_Domain.mRID`

### Bronze-To-Silver Test

Create `TestEntsoeBronzeToSilverPipeline` parametrized over:

| Dataset | Fixture | Transformer | Required Columns |
|---------|---------|-------------|------------------|
| `day_ahead_prices` | `day_ahead_prices_gb.xml` | `DayAheadPricesTransformer` | `timestamp_utc`, `area_code`, `price_eur_mwh` |
| `actual_load` | `actual_load_gb.xml` | `ActualLoadTransformer` | `timestamp_utc`, `area_code`, `load_mw` |
| `cross_border_flows` | `cross_border_flows_gb_fr.xml` | `CrossBorderFlowsTransformer` | `timestamp_utc`, `in_area_code`, `out_area_code`, `flow_mw` |
| `imbalance_prices` | `imbalance_prices_gb.xml` | `ImbalancePricesTransformer` | `timestamp_utc`, `area_code`, `direction`, `price_eur_mwh` |

Helper for silver path:

```python
def _silver_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    return (
        data_dir
        / "silver"
        / "entsoe"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )
```

## Anti-Patterns To Avoid

- Do not call the live ENTSO-E API.
- Do not use simplified inline XML when realistic fixtures already exist.
- Do not manually parse XML in integration tests; let real transformers parse fixture bytes.
- Do not broaden H2 into Elexon package cleanup unless the user explicitly changes scope.
- Do not assert exact call counts for zone-pair internals unless importing the connector's
  private `_FLOW_PAIRS`; query-shape assertions are enough for H2.

## Verification Commands

```powershell
uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q
uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q
uv run --extra dev pytest -x -q
```
