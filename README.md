# hqg-datafeed

`hqg-datafeed` is a small, stateless Python library for fetching daily
securities data and native-frequency alternative data.

The import package is `datafeed`:

### Example
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

`Config()` reads `FRED_API_KEY` from `.env` in the `hqg-datafeed` project
folder. Passing `Config(fred_api_key="...")` still overrides the file value.

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

Rows with missing observation dates or missing `value` fields are removed
during alt-data preparation. Dropped rows are logged with warning-level counts.
For alt-data slicing, `start` and `end` are interpreted as whole observation
dates: `start` is floored to the start of its day, and `end` includes the full
end date. The cleaned per-series frames must include `"value"` and
`"available_at"`, and `(date, available_at)` pairs must be unique within each
series.

`raw_alt_data_fetch(ids, start, end)` returns the raw provider payloads keyed
by normalized full series ID. It does not normalize, slice, reshape, or clean
the provider response.

## Providers

Providers are defined in `datafeed/config.py`. Callers cannot mutate the
provider registry through `Config` or `DataFeed`.

To add a new built-in provider, implement `AltDataProvider` and add a factory
to the provider registry in `config.py`:

```python
from types import MappingProxyType

_PROVIDER_FACTORIES = MappingProxyType(
    {
        "FRED": lambda config: FredProvider(config.fred_api_key),
        "VENDOR": lambda config: VendorProvider(...),
    }
)

data = DataFeed(Config()).get_alt_data(["VENDOR.MY_SERIES"], start, end)
```

Providers implement raw fetch and normalization separately:

```python
fetch_raw(series: list[str], start: datetime, end: datetime) -> dict[str, object]
normalize(raw_data: dict[str, object], start: datetime, end: datetime) -> dict[str, pd.DataFrame]
```

The base `fetch()` implementation calls `fetch_raw()` and then `normalize()`.
Each normalized DataFrame must have a `DatetimeIndex` and one or more columns.

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

## Deliberate Non-Features (For Now)

This library deliberately does not do:

- Disk caching or cache directories
- Resampling or frequency conversion
- Forward-filling
- Calendar alignment between securities or alt series
- Row dropping after joins
- History floors or automatic range widening
- Trading-day or holiday logic
- Environment-variable configuration

The consuming application owns those policies.

## Known Issues 
- FRED may rate limit output for vintage dates that have many revisions
- Add async calling
