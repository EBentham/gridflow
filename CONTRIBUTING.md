# Contributing to gridflow

## Connector architecture (bronze layer)

New connectors live under `src/gridflow/connectors/<source>/`. Read `base.py`
and an existing vendor (`elexon/` or `entsoe/`) before writing a new one.

### BaseConnector ABC (`base.py`)

```python
class BaseConnector(ABC):
    source_name: str                        # set on the subclass
    def __init__(self, config: SourceConfig): ...
    async def __aenter__(self) -> BaseConnector: ...   # builds httpx.AsyncClient + Semaphore
    async def __aexit__(self, *exc) -> None: ...
    def _auth_headers(self) -> dict[str, str]: ...     # override for non-header auth
    @abstractmethod
    async def fetch(self, dataset: str, start: datetime, end: datetime,
                    **params: Any) -> list[RawResponse]: ...
    @abstractmethod
    def list_datasets(self) -> list[str]: ...
```

Subclass contract:
- Set `source_name = "<source>"` as a class attribute.
- Implement `fetch()` and `list_datasets()`.
- Always use the connector via `async with`. `__aenter__` initialises `_client`
  and `_semaphore`; bare instantiation leaves them `None`.

### Registration

```python
from gridflow.connectors.registry import register_connector
register_connector("<source>", <Source>Connector)   # at module bottom
```

`get_connector(source, config)` looks up the class and instantiates it. The
CLI's `_import_connectors()` imports each `gridflow.connectors.<source>` to
trigger registration — add new sources there.

### Rate limiting + retries

- **Rate limit:** `asyncio.Semaphore(self.config.rate_limit_per_second)` is
  built in `__aenter__`. Wrap every HTTP call: `async with self._semaphore: ...`.
- **Retry:** decorate the request method with `@RETRY_POLICY` from
  `gridflow.utils.retry`. It's `stop_after_attempt(5)`,
  `wait_exponential(min=2, max=60)`, retries on `httpx.TimeoutException` and
  `httpx.HTTPStatusError`, `reraise=True`.
- A `CircuitBreaker` exists in `utils/retry.py` but is not applied automatically.
  Wire it in only if a vendor needs it.

### Auth patterns

- **Header auth (default):** `_auth_headers()` reads `api_key` + `api_key_header`
  from `SourceConfig`. Used by `gie` (`x-key`).
- **Query-param auth:** override both `_auth_headers()` (return `{}`) and
  `__aenter__` (build the client without auth headers), then inject the token
  into each request's params. See `entsoe/client.py` (`securityToken`).
- **No auth:** `_auth_headers()` returns `{}` automatically when
  `api_key`/`api_key_header` are blank in YAML. Used by elexon, openmeteo,
  entsog, neso.

### RawResponse fields

```
body: bytes                       # raw response body — never decoded here
content_type: str                 # "application/json" / "text/xml" / "text/csv"
source: str                       # "elexon"
dataset: str                      # "system_prices"
fetched_at: datetime              # default: now(UTC)
request_url: str
request_params: dict[str, Any]
api_version: str = ""
page: int = 1
total_pages: int = 1
http_status: int = 200
data_date: date | None = None     # set this so bronze partitions by data date,
                                  # not fetch date — important for backfills
```

Always set `data_date` when the API call is for a specific calendar date.

### File structure of a vendor connector

```
connectors/<source>/
├── __init__.py        Imports `client` so register_connector(...) runs at import time
├── endpoints.py       Endpoint table, param-style enum / doc-type table, helpers
├── client.py          <Source>Connector subclass + register_connector(...) at bottom
└── parsers.py         (optional) JSON/XML helpers
```

### Vendor-specific gotchas

- **Elexon settlement-period endpoints:** iterate periods 1–**50**, not 1–48.
  Spring forward: 46 periods. Autumn back: 50. Stop early on HTTP error or
  HTTP 200 with empty `data: []`.
- **Elexon pagination:** every page increments `page`; stop when
  `current_page >= total_pages`.
- **ENTSO-E auth:** API key goes in **query params (`securityToken`)**, not a
  header. Override `_auth_headers()` and `__aenter__` together.
- **ENTSO-E XML namespaces:** parse with the namespace-agnostic helper in
  `connectors/entsoe/parsers.py`. Don't hardcode `{urn:...}` prefixes.
- **GIE auth:** `x-key` header. Handled by the default `_auth_headers()` when
  `api_key_header: "x-key"` is set in YAML.
- **Open-Meteo:** two base URLs (`api.open-meteo.com` for forecast,
  `archive-api.open-meteo.com` for historical). Pick the right one per dataset
  in `endpoints.py`.
- **Settlement-date params:** Elexon expects ISO `YYYY-MM-DD` in the local UK
  date sense — convert UTC datetimes via `utils/time` before building params.
