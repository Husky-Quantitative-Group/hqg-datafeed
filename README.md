# hqg-datafeed

`hqg-datafeed` is a small, stateless Python library for fetching daily
securities data and native-frequency alternative data.

## Installation

Requires Python 3.10 or newer. The distribution name is `hqg-datafeed`;
the import package is `datafeed`.

```shell
python -m pip install hqg-datafeed
```

For development, run from the repository root:

```shell
python -m pip install -e ".[test,release]"
python -m pytest
```

## Configuration

Set the `FRED_API_KEY` environment variable for FRED requests, or pass it
explicitly using `Config(fred_api_key="...")`. Securities and Kalshi requests
do not require this key.

When running from a source checkout, `Config()` also reads `.env` in the
repository root as a fallback. The `.env` file is excluded from distributions;
use an environment variable or explicit configuration after installing from PyPI.

## Example
```python
from datetime import datetime

from datafeed import Config, DataFeed

feed = DataFeed(Config())

prices = feed.get_securities(
    ["SPY", "TLT"],
    datetime(2020, 1, 1),
    datetime(2024, 12, 31),
)

macro = feed.get_alt_data(
    ["FRED.GDP"],
    datetime(2020, 1, 1),
    datetime(2024, 12, 31),
)
gdp = macro["FRED.GDP"]

market_data = feed.get_data(
    securities=["SPY"],
    alt_data=["FRED.GDP", "FRED.DGS10"],
    start=datetime(2020, 1, 1),
    end=datetime(2024, 12, 31),
)
```

## Securities Data

`get_securities(symbols, start, end)` downloads daily OHLCV data from
yfinance in one batched call for all deduplicated symbols. The public range
rule is exact: returned data is sliced to `[start, end]` inclusive.

The returned frame has:

```text
index:   DatetimeIndex named "date"
columns: MultiIndex (symbol, field)
fields:  open, high, low, close, volume
```

Example columns:

```text
("SPY", "open")
("SPY", "high")
("SPY", "low")
("SPY", "close")
("SPY", "volume")
```

Symbols are normalized to uppercase before download, deduplication, and output.

## Alternative Data

`get_alt_data(ids, start, end)` fetches provider-prefixed series IDs such as
`FRED.GDP`. IDs are split on the first dot, so IDs like
`VENDOR.DATASET.FIELD` are valid. Provider prefixes, series IDs, and output
keys are uppercased.

The returned value is a dict keyed by normalized full series ID:

```text
{
    "FRED.GDP": pd.DataFrame(...),
    "FRED.DGS10": pd.DataFrame(...),
}
```

Each per-series frame has a `DatetimeIndex` named `"date"` and columns returned
by the provider, normally `"value"` and `"available_at"`. Series are not 
aligned to each other, and duplicate dates are valid within a frame when a
series has one row per vintage.

Important backtesting note: alt data is indexed by observation date, not by the
date the value became public, and values can be revised later. Use
`available_at` to filter point-in-time snapshots. Providers own the semantics
and normalization of their `available_at` fields.

During alt-data preparation, rows with missing or invalid observation dates are
removed and logged if they make up at most 25% of the original series frame.
If more than 25% have invalid dates, the data call raises an error. Rows with
missing `value` fields are also removed and logged.
For alt-data slicing, `start` and `end` are interpreted as whole observation
dates: `start` is floored to the start of its day, and `end` includes the full
end date. The cleaned per-series frames must include `"value"` and
`"available_at"`, and `(date, available_at)` pairs must be unique within each
series.

`raw_alt_data_fetch(ids, start, end)` returns the raw provider payloads keyed
by normalized full series ID. It does not normalize, slice, reshape, or clean
the provider response.

## Providers

Provider implementations live in `datafeed/altdata/providers/`, and the base
interface remains in `datafeed/altdata/base.py`. The provider registry is defined
in `datafeed/altdata/registry.py`. Callers cannot mutate the registry through
`Config` or `DataFeed`. Import providers from `datafeed.altdata` or their modules under
`datafeed.altdata.providers`.

To add a new built-in provider:

1. Implement `AltDataProvider` in `datafeed/altdata/providers/`.
2. Import the implementation and register its factory in
   `datafeed/altdata/registry.py`.
3. Add required credentials to `_ENV_KEYS` in `datafeed/config.py`.
   This mapping supplies environment loading, constructor overrides, and secret
   discovery without a separate dataclass field declaration. For example,
   `"vendor_api_key": "VENDOR_API_KEY"` enables `Config(vendor_api_key="...")`.

For example, extend the existing factory mapping in `altdata/registry.py`:

```python
from types import MappingProxyType

from datafeed.altdata.providers.fred import FredProvider
from datafeed.altdata.providers.kalshi import KalshiProvider
from datafeed.altdata.providers.vendor import VendorProvider

_PROVIDER_FACTORIES = MappingProxyType(
    {
        "FRED": lambda config: FredProvider(config.fred_api_key),
        "KALSHI": lambda config: KalshiProvider(),
        "VENDOR": lambda config: VendorProvider(config.vendor_api_key),
    }
)
```

The registry's `build_providers(config)` constructs fresh provider instances and
returns an immutable mapping. `Config.providers` delegates to it, preserving the
existing `Config` and `DataFeed` API without sharing provider instances globally.
The example assumes `vendor_api_key` has been added to `Config`. The new provider
can then be requested as `VENDOR.MY_SERIES` through `DataFeed.get_alt_data()`.

Providers implement raw fetch and normalization separately:

```python
fetch_raw(series: list[str], start: datetime, end: datetime) -> dict[str, object]
normalize(raw_data: dict[str, object], start: datetime, end: datetime) -> dict[str, pd.DataFrame]
```

The base `fetch()` implementation calls `fetch_raw()` and then `normalize()`.
Each normalized DataFrame must contain both value and available_at.

Shared helpers in `datafeed/altdata/common.py` handle HTTP retries, exponential
backoff, `Retry-After` parsing, raw response storage, JSON object validation,
and bounded error text. Providers supply their retry statuses, delay policy,
and error context, and retain their own HTTP error interpretation and data
transformations. FRED accepts numeric retry delays; Kalshi also accepts HTTP
dates and retries all 5xx responses.

The client uses the same module for case-insensitive response lookup and frame
preparation, whole-date slicing, required-column validation, and duplicate
observation/availability detection. Preparation preserves row order, extra
columns, and provider-defined `available_at` values without changing the input
frame. Providers retain their own sorting and timestamp conventions.

The FRED provider first fetches `series/tags` and reads the tag with
`group_id == "freq"`. Daily series are requested without FRED realtime
parameters and use `date + 1 US federal business day` as `available_at`; less
frequent series are requested with full realtime history. FRED normalizes
`available_at` to a conservative end-of-day timestamp (`YYYY-MM-DD 23:59:59`)
on the release date.

### Kalshi prediction markets

Request an exact market ticker using `KALSHI.<MARKET_TICKER>`:

```python
prediction = feed.get_alt_data(
    ["KALSHI.YOUR-MARKET-TICKER"],  # Replace with an actual Kalshi market ticker.
    datetime(2024, 1, 1),
    datetime(2024, 12, 31),
)
```

The provider uses public REST endpoints without credentials and fetches daily
candles (`period_interval=1440`). It resolves live markets through their event
metadata and automatically uses historical endpoints for archived markets,
including when a live candle request returns HTTP 404 during archival.

`value` is the YES trade close in dollars on a 0–1 scale (for example, `0.62`).
The `date` index and `available_at` both contain the exact candle end timestamp,
converted from Unix seconds to timezone-naive UTC. Naive request dates mean UTC;
aware bounds are converted to UTC before selecting whole inclusive dates.
Missing trade closes and candles ending after fetch time are excluded. There
is no forward-filling or substitution of settlement values. Requests with no
usable observations raise `DataFeedError`.

Candle end is an assumed availability time, not a measured publication time;
historical candles do not establish publication latency or revision history.
The backtester only exposes a candle when `available_at < simulation clock`.
Contract discovery, recurring-contract selection, trading, streaming, and other
metrics are outside this provider. `raw_alt_data_fetch()` preserves metadata
and candle response text and HTTP status codes alongside the fetch cutoff and
the selected live/historical candle format.

API references: [live candlesticks](https://docs.kalshi.com/api-reference/market/get-market-candlesticks),
[historical candlesticks](https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks),
and [historical data routing](https://docs.kalshi.com/getting_started/historical_data).

## Errors and logging

All library error categories inherit from `DataFeedError`, so existing
`except DataFeedError` handlers continue to work. Import the more specific
classes from `datafeed` or `datafeed.errors` when different recovery is useful:

| Exception | Meaning |
| --- | --- |
| `DataFeedRequestError` | Transport failures or unsuccessful HTTP requests |
| `InvalidProviderResponseError` | Malformed JSON, invalid response shapes, or invalid/duplicate observations |
| `InvalidSeriesIdError` | Malformed series IDs or unknown provider prefixes |
| `DataUnavailableError` | Missing series, HTTP 404 resources, or no usable data in the requested range |

Missing configuration and other uncategorized failures continue to use
`DataFeedError`. Provider failures retain their category when `DataFeed` adds
provider and requested-series context. Existing messages are retained except
when credential details require redaction. Transport errors report exception
types without including authenticated request URLs; redacted errors suppress
unsafe exception chains. Raw payloads remain unchanged for explicit inspection.

Credential discovery is centralized in `errors.py`: client operations use the
feed's `Config`, and authenticated provider entry points use `@redact_errors`
to cover direct calls too. Config declares every credential in `_ENV_KEYS` as
secret automatically, including fields with unconventional names. For direct
provider calls, credential-like attribute names or explicit `secret_fields`
declarations identify values to redact.
Redaction is scoped to each operation and isolated between concurrent feeds;
providers do not pass lists of secret values. Custom code can also use
`with error_redaction(config):` when creating errors outside a feed operation.

Library diagnostics use module loggers without configuring application logging.
Retry attempts are logged at DEBUG with provider/series context, attempt counts,
status or exception type, and delay. Dropped-row warnings include the series ID.
Applications choose log handlers and levels; request headers and raw response
bodies are not included in retry diagnostics.

## Deliberate Non-Features (For Now)

This library deliberately does not do:

- Disk caching or cache directories
- Resampling or frequency conversion
- Forward-filling
- Calendar alignment between securities or alt series
- Row dropping after joins
- History floors or automatic range widening

The consuming application owns those policies.

## Known Issues 
- FRED may rate limit output for vintage dates that have many revisions
- Add async calling
