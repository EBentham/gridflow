# Testing Patterns

**Analysis Date:** 2026-05-02

## Test Framework

**Runner:**
- pytest >=8.0
- Config: `pyproject.toml`
- Async support: `pytest-asyncio>=0.23` with `asyncio_mode = "auto"` in `pyproject.toml`
- HTTP mocking: `respx>=0.21` for async `httpx` connector tests

**Assertion Library:**
- Native `assert` statements.
- `pytest.raises` for exception assertions.
- Pydantic `ValidationError` is asserted directly in schema tests.

**Run Commands:**
```bash
make test              # Run all tests: pytest tests/ -v --tb=short
make test-unit         # Run unit tests only: pytest tests/unit/ -v --tb=short
make test-integration  # Run integration tests only: pytest tests/integration/ -v --tb=short
pytest tests/contracts/ -v --tb=short  # Run contract tests
make lint              # Ruff lint on src/ and tests/
make typecheck         # Strict mypy on src/gridflow/
```

## Test File Organization

**Location:**
- Shared fixtures live in `tests/conftest.py`.
- Unit tests live in `tests/unit/`.
- Integration tests live in `tests/integration/`.
- Data contract tests live in `tests/contracts/`.
- Static fixture payloads live in `tests/fixtures/{source}/`.

**Naming:**
- Test files use `test_*.py`: `tests/unit/test_time_utils.py`, `tests/unit/test_entsoe.py`, `tests/integration/test_elexon_connector.py`.
- Test classes use `Test{Subject}`: `TestSettlementPeriodToUTC` in `tests/unit/test_time_utils.py`, `TestSystemPriceTransformer` in `tests/unit/test_silver_transforms.py`.
- Test methods and functions use `test_*` with behavior-oriented names: `test_fetch_unknown_dataset_raises()` in `tests/integration/test_elexon_connector.py`.
- Shared local helpers in tests use leading underscores: `_make_transformer()` and `_load_fixture_records()` in `tests/unit/test_gie.py`.

**Structure:**
```text
tests/
├── conftest.py
├── unit/
│   ├── test_time_utils.py
│   ├── test_schemas.py
│   ├── test_silver_transforms.py
│   └── test_entsoe.py
├── integration/
│   ├── test_elexon_connector.py
│   └── test_entsoe_connector.py
├── contracts/
│   ├── test_bronze_silver_contract.py
│   └── test_silver_gold_contract.py
└── fixtures/
    ├── elexon/
    ├── entsoe/
    ├── entsog/
    ├── gie/
    ├── neso/
    └── openmeteo/
```

## Test Structure

**Suite Organization:**
```python
class TestSettlementPeriodToUTC:
    def test_sp1_winter(self):
        result = settlement_period_to_utc(date(2024, 1, 15), 1)
        assert result == datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
```

Use this class-per-subject style for unit tests, following `tests/unit/test_time_utils.py`, `tests/unit/test_schemas.py`, and `tests/unit/test_silver_transforms.py`.

**Patterns:**
- Arrange test input inline for small records, then call the unit under test, then assert concrete fields. See `TestElexonSystemPrice.test_valid_record()` in `tests/unit/test_schemas.py`.
- Use source fixture files for realistic payloads instead of embedding large API bodies. See `FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"` in `tests/unit/test_silver_transforms.py`.
- For filesystem pipeline tests, write through production helpers into `tmp_path`, then read the generated Parquet output. See `tests/contracts/test_bronze_silver_contract.py`.
- For transformer methods that only need `transform()`, instantiate with `__new__` and assign `data_dir`, `bronze_dir`, and `silver_dir` manually, as in `tests/unit/test_silver_transforms.py`.

## Mocking

**Framework:** `respx` for HTTPX request mocking; pytest fixtures for object/data setup.

**Patterns:**
```python
@respx.mock
@pytest.mark.asyncio
async def test_fetch_system_prices(elexon_config: SourceConfig):
    fixture = (FIXTURES / "system_prices_response.json").read_text()
    respx.get("https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/2024-01-15").mock(
        return_value=httpx.Response(200, text=fixture)
    )

    async with ElexonConnector(elexon_config) as connector:
        responses = await connector.fetch(
            dataset="system_prices",
            start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )

    assert responses[0].http_status == 200
```

This pattern is used in `tests/integration/test_elexon_connector.py` and `tests/integration/test_entsoe_connector.py`.

**What to Mock:**
- External HTTP calls from connectors with `respx`.
- API response bodies with files under `tests/fixtures/{source}/`.
- Filesystem roots with `tmp_path` or `tmp_data_dir` from `tests/conftest.py`.
- Source configuration objects with `SourceConfig` and `DatasetConfig` fixtures, not environment variables.

**What NOT to Mock:**
- Pydantic schema validation in `src/gridflow/schemas/`; instantiate real schemas and assert `ValidationError`.
- Polars transformations in `src/gridflow/silver/`; pass real `pl.DataFrame` inputs and assert output columns/values.
- Bronze/silver/gold file contracts; use real `BronzeWriter`, real transformer `run()`, and real `pl.read_parquet()` in contract tests.

## Fixtures and Factories

**Test Data:**
```python
@pytest.fixture
def sample_raw_response(sample_elexon_response_data: dict) -> RawResponse:
    body = json.dumps(sample_elexon_response_data).encode()
    return RawResponse(
        body=body,
        content_type="application/json",
        source="elexon",
        dataset="system_prices",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        request_url="https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices",
        request_params={"settlementDate": "2024-01-15"},
        api_version="v1",
        page=1,
        total_pages=1,
        http_status=200,
    )
```

**Location:**
- Shared object fixtures: `tests/conftest.py`.
- JSON/XML payload fixtures: `tests/fixtures/elexon/`, `tests/fixtures/entsoe/`, `tests/fixtures/gie/`, `tests/fixtures/entsog/`, `tests/fixtures/neso/`, `tests/fixtures/openmeteo/`.
- Per-test factory helpers: inside the relevant test module, for example `_make_raw_df()` in `tests/unit/test_silver_transforms.py` and `_make_entsoe_transformer()` in `tests/unit/test_entsoe.py`.

## Coverage

**Requirements:** No coverage threshold is configured in `pyproject.toml`; `pytest-cov` is not listed in project dev dependencies.

**View Coverage:**
```bash
# Not configured by the project.
# Add pytest-cov before introducing a required coverage command.
```

## Test Types

**Unit Tests:**
- Scope: pure utilities, schema validation, endpoint parameter construction, XML/JSON parsing, and individual transformer logic.
- Location: `tests/unit/`.
- Pattern: instantiate real classes/functions with small inline records or fixture files, then assert exact fields and validation behavior.

**Integration Tests:**
- Scope: connector behavior against mocked HTTP transports and API request construction.
- Location: `tests/integration/`.
- Pattern: use `@respx.mock` plus `@pytest.mark.asyncio`; assert request params through `route.calls` or `respx.calls`.
- Network behavior: the registered `live` marker in `pyproject.toml` is reserved for real API tests, but current integration tests use mocked HTTP.

**Contract Tests:**
- Scope: cross-layer guarantees between bronze, silver, and gold outputs.
- Location: `tests/contracts/`.
- Pattern: write realistic bronze payloads, run production transform/build code, read generated Parquet, and validate output schemas/columns.

**E2E Tests:**
- Not used. There is no dedicated `tests/e2e/` directory or browser/UI test framework.

## Common Patterns

**Async Testing:**
```python
@respx.mock
@pytest.mark.asyncio
async def test_fetch_control_area_uses_correct_query_param(
    entsoe_config: SourceConfig,
) -> None:
    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )
    async with EntsoeConnector(entsoe_config) as connector:
        await connector.fetch(dataset="imbalance_prices", start=start, end=end)
    sent_params = dict(route.calls[0].request.url.params)
    assert "controlArea_Domain.mRID" in sent_params
```

Use this style for connector tests under `tests/integration/`.

**Error Testing:**
```python
def test_invalid_unit(self):
    with pytest.raises(ValueError):
        parse_lookback("10x")
```

Use `pytest.raises(ExpectedError, match="...")` when checking user-facing error text, as in `tests/integration/test_elexon_connector.py`.

**Schema Testing:**
```python
with pytest.raises(ValidationError):
    ElexonSystemPrice(
        settlement_date=date(2024, 1, 15),
        settlement_period=0,
        timestamp_utc=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
        system_sell_price=45.50,
        system_buy_price=55.00,
        net_imbalance_volume=0,
        run_type="SF",
    )
```

Use this direct schema instantiation pattern for new Pydantic models under `src/gridflow/schemas/`.

---

*Testing analysis: 2026-05-02*
