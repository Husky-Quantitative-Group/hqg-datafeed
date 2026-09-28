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
column labels are uppercased.

The returned frame has:

```text
index:   DatetimeIndex named "date"
columns: MultiIndex (series_id, field)
```

The default alt-data field is `"value"`, although providers may return
multiple fields per series.

Important backtesting warning: alt data is indexed by observation date, not by
the date the value became public, and values can be revised later. It is not
safe to use alt data in a backtest without applying a publication lag. This
library does not apply that lag.

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

Providers implement:

```python
fetch(series: list[str], start: datetime, end: datetime) -> dict[str, pd.DataFrame]
```

Each returned DataFrame must have a `DatetimeIndex` and one or more columns.

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
