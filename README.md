# hqg-datafeed
Centralized data service that normalizes, caches, and serves historical and live market data, economic data (e.g., FRED), and alternative datasets. 

Called datafeed because hqg-databroker looks too similar to hqg-dashboard at first glance lol (lowk dyslexic).

## What Is This

Every service we run (backtester, execution engine, dashboard) currently manages its own connection to external data. The backtester talks to Yahoo Finance directly, the engine polls Alpaca directly, the dashboard will need its own data source as we scale functionality. This doesn't scale, and it's already a pain.

The hqg-datafeed is a standalone service that owns all data retrieval, caching, and delivery. No consumer needs to know where data comes from or how it's fetched/aggregated.

Currently planning out just two modes:
- **Historical**: "Give me SPY and TLT, daily bars, 2015-2024." -> returns a DataFrame.
- **Live**: "Give me a 'stream' of AAPL bars at a 1-hour cadence." -> returns bars as they're built.

## Customers

**Backtester** needs historical price data (Yahoo Finance currently, Databento eventually), alternative data (Carbon Arc), macro data (FRED), and whatever else we plug in over time.

**Execution Engine** needs live market data. Currently polls Alpaca's IEX endpoint via REST, aggregates quotes into OHLC bars, and passes each bar to the strategy's `on_data`. The broker should own that aggregation: _engine asks for a slice, gets a slice, passes it through_. It may need historic data down the line as well, but integrating would be trivial with hqg-datafeed.

**Dashboard** needs historical data for benchmarks (S&P 500, Bloomberg Bond Aggregate) and research tools (universe explorer: covariance/correlation matrices over user-defined windows). Also needs live data for a price carousel showing current prices + daily changes.

## Some Design Ideas

**"Ask for a slice, get a slice" is preferable to firehose.** The engine doesn't need a raw quote stream, as we're trading at hourly cadence max for the foreseeable future. The broker handles aggregation internally (poll quotes, build OHLC bars at the requested cadence), and the engine just receives finished bars. This keeps the engine simple and keeps data logic centralized.

**Parquet for cold(ish) storage, LRU for hot.** Historical data lives in parquet files (one per symbol, daily and hourly variants). On startuo 8am daily, we refresh parquets with up to yesterday's data and seed the in-memory LRU cache with the most recently used symbols + a baseline universe (large ETFs, S&P 500 constituents). On a cache miss, we check parquet (if 2x miss: fetch, write to parquet), then promote to cache.

**Look-ahead bias prevention.** Assume any data with a timestamp of `t` is only available at `t+1` and beyond. This is critical for alternative data (FRED, Carbon Arc) where publication time is not equal timestamp. I.e., some information provided by FRED yesterday may have only been available after market close! Philosophy: make it very difficult for quant researchers to introduce data leakage beyond their inherent biases (universe selection, etc.). This is also why we should not allow users to import their own data or learned weights via CSV-- it's too easy to leak future information.

**Data catalog / registry.** We need to know and surface to users what data we have: which tickers, which datasets, which date ranges, at which granularities. This powers the dashboard's universe explorer, gives clear error messages ("Carbon Arc data for XYZ is only available from 2018 onward at daily granularity"), and drives cache logic. Should be a maintained list rather than derived from parquet files at runtime (crash resilience).

## Versioning / Implementation Roadmap

### V1 - Market Data (YF & Alpaca Only)

Drop-in replacement for current data interactions. No breaking changes to hqg-algorithms (besides adding hourly granularity support). Backtester and engine swap out their data providers for the datafeed and experience no quality changes.

**V0.0 - Historical tickers.** Migrate all historical data fetching from the backtester to the broker. Endpoint: given ticker names, granularity, and date range, return OHLCV data. Current flow: check parquet coverage -> collect cache misses -> batch-fetch from YF back to 2000 -> write/extend parquets -> return requested slice.

**V0.1 - Live tickers.** Migrate all live data fetching from the engine to the broker. Alpaca integration: poll IEX quotes via REST, aggregate at requested cadence, deliver finished bars.

**V1.0 - Publish.** Package / ship. Backtester, engine, and dashboard can now consume from the broker. The rest of V1 is for speed improvements + building out our own datasets of hourly/daily data (this is important because (1) we may eventually want to have the logic to support this when we have a better market data source, like Databento, or alternative data sources, like CarbonArc (2) yahoo finance only provides about 2 years of hourly data-- we must start storing now to have some proprietary data we can use down the line).

**V1.1 - Centralized caching.** Centralized cache: parquet cold storage + in-memory LRU. Request coalescing so multiple consumers asking for the same symbol don't trigger duplicate fetches, etc.

**V1.2 - Seeding & daily refresh.** Cron job at 8am: update all parquets (daily + hourly) with yesterday's data, warm the LRU with MRU tickers. Maintain a persisted ticker list of static universe of common ETFs/stocks & all tickers ever requested for these updates.

### V2 - Free External Data + Subscription Infra

Introduces yf data. Requires changes to hqg-algorithms - `Slice` (or a new structure) needs to carry heterogeneous data alongside price data.

We can implement FRED first: macro data (rates, inflation, employment, etc.). Need to make data availability apparent in the frontend and in error responses - users should know what's accessible and what isn't.

Set up the infra so adding a new dataset is plug-and-play: implement the provider interface, register in the catalog, done. Implement at the historical endpoint first, then live.

**Look-ahead rules enforced at the provider level**.

____
## Note
The below versions require support/consideration of automated payments for refreshing data for live trading. Integrating in historic endpoint using the data we have collected is relatively easy, but pointless if we cannot use it live in a manner that is safe.
_____
### V3 - Carbon Arc (Paid Alternative Data)

Requires solving automated payments for data refresh in live trading.

**V3.0 - Historical Carbon Arc.** Serve data we've already collected.

**V3.1 - Live Carbon Arc + purchases.** Two strategies (possibly both): (a) on-demand - if today's data is needed, purchase from CA and extend the local dataset; (b) scheduled - daily script purchases what we expect to need based on active strategies.

### V4 - Databento

Higher precision market data. Could replace YF entirely (Databento is more accurate), or could be reserved for L1/L3 experimentation and research use cases. Decision deferred.

## Known Complications / Annoyances

**Corporate actions.** Stock splits, dividends, symbol changes. YF auto-adjusts historical data, but if we're caching parquets long-term and layering other sources on top, retroactive changes to historical data will cause drift.

**Alternative data coverage gaps.** Not every dataset covers every ticker, date range, or granularity. FRED is monthly; Carbon Arc does not have data for many tickers. We can forward-fill where appropriate (e.g., daily value repeated in every hourly bar), but need clear error messages: "data not supported for this ticker," "date range not available," "granularity not supported for this combo" + guidance on what *is* accessible.

**Calendar alignment.** Different data sources have different trading calendars. BTC trades 24/7, equities are M-F minus holidays, FRED publishes monthly with variable lag. Joining these into a single bar timeline may be non-trivial.


_______
# v1 API Sketch

## Historical Data

Retrieve OHLCV bars for one or more symbols over a date range at a given granularity. Backtester, dashboard benchmarks, and universe explorer all hit this.

**`GET /data/historical`**

Request:
```
{
  "symbols": ["SPY", "TLT", "AAPL"],
  "start_date": "2015-01-01",
  "end_date": "2024-01-01",
  "bar_size": "daily"          // daily | hourly | weekly | monthly | quarterly
}
```

Response:
```
{
  "bars": {
    "SPY": [
      {
        "date": "2015-01-02",
        "open": 206.38,
        "high": 206.88,
        "low": 204.18,
        "close": 205.43,
        "volume": 121465900
      },
      ...
    ],
    "TLT": [...],
    "AAPL": [...]
  },
  "metadata": {
    "bar_size": "daily",
    "start_date": "2015-01-02",    // actual first bar (market was closed 1/1)
    "end_date": "2024-01-01",
    "symbols_returned": ["SPY", "TLT", "AAPL"]
  }
}
```

Errors:
- Symbol not found -> 404 with `{ "error": "unknown_symbol", "symbols": ["FAKE"] }`
- Partial coverage -> 206 with data we have + `"warnings": [{ "symbol": "XYZ", "available_from": "2018-03-15" }]`

---

## Live Data

Subscribe to a live bar 'stream' for one or more symbols at a given cadence. The broker polls quotes (Alpaca IEX), aggregates them into OHLC bars internally, and delivers finished bars.

### Start a stream

**`POST /data/live/subscribe`**

Request:
```
{
  "symbols": ["AAPL", "MSFT"],
  "cadence": "1m"               // 1m | 5m | 15m | 1h
}
```

Response:
```
{
  "stream_id": "abc-123",
  "symbols": ["AAPL", "MSFT"],
  "cadence": "1m",
  "status": "active"
}
```

### Get next bar(s)

**`GET /data/live/{stream_id}`**

Long-poll. Returns when the next bar is ready or after a timeout. Engine calls this in a loop.

Response:
```
{
  "stream_id": "abc-123",
  "bars": [
    {
      "symbol": "AAPL",
      "open": 189.50,
      "high": 189.95,
      "low": 189.32,
      "close": 189.78,
      "volume": 34200,
      "bar_start": "2024-06-10T10:30:00Z",
      "bar_end": "2024-06-10T10:31:00Z"
    },
    {
      "symbol": "MSFT",
      ...
    }
  ]
}
```

If no bar is ready within timeout -> `204 No Content`. Client retries.

### Teardown

**`DELETE /data/live/{stream_id}`**

Response: `{ "stream_id": "abc-123", "status": "stopped" }`

### Pause / Resume
Maybe...
**`POST /data/live/{stream_id}/pause`**
**`POST /data/live/{stream_id}/resume`**

Response: `{ "stream_id": "abc-123", "status": "paused" | "active" }`

---

## Catalog

What data do we have? Especially useful for figuring out what alternative data we have. Frontend can query it to show users what's available before they build a strategy (and on error).

**`GET /data/catalog`**

Request
```
?symbols=AAPL,SPY          // optional filter
&dataset=price              // optional: price | fred | carbonarc | ...
```

Response:
```
{
  "symbols": {
    "AAPL": {
      "datasets": {
        "price": {
          "source": "yfinance",
          "available_from": "2000-01-03",
          "available_to": "2024-06-10",
          "granularities": ["daily", "hourly"],
          "last_refreshed": "2024-06-10T08:00:00Z"
        }
      }
    },
    "SPY": {
      "datasets": {
        "price": { ... }
      }
    }
  }
}
```

---

## Latest Quotes

Simple endpoint for the dashboard price carousel. No aggregation, just the most recent quote per symbol.

**`GET /data/quotes`**

Request:
```
?symbols=AAPL,SPY,TLT
```

Response:
```
{
  "quotes": {
    "AAPL": {
      "price": 189.78,
      "bid": 189.75,
      "ask": 189.80,
      "change": 1.23,
      "change_pct": 0.65,
      "timestamp": "2024-06-10T14:32:00Z"
    },
    ...
  }
}
```

---

## How This Changes With Alternative Data

The above API is built around price data. Every bar is OHLCV, keyed by symbol and date. This works cleanly for V1. When we introduce alternative data (FRED macro series, Carbon Arc, etc.) in V2+, the shape shifts.

Core question: **how to we make it easy for QRs to access alternative data?**. There exists a future where using the data is hidden behind obscurity (how to subscribe to a specific dataset at a per-ticker basis, eg, CarbonArc.consumer_credit.AAPL to add this to Slice, accessible similarly?) and confusion over what is even available.

Something like self.subscribe(data.CarbonArc.consumer_credit.AAPL) might be clear? But AST parsing could fall apart on renamed imports... Recall, we'd like to fetch data _before_ passing strategy to container (with initilization overhead & no write/internet access).


To maintain AST-parseablility...
```python3
from hqg_algorithms import (
    Strategy, Cadence, BarSize, Slice, PortfolioView,
    Signal, TargetWeights, Hold,
    FRED, CarbonArc,    # alt data sources
)

class MomentumWithMacro(Strategy):
    universe = ["AAPL", "BND"]                    # market data

    datasets = [                                   # everything else
        FRED("DGS10"),
        FRED("UNRATE"),
        CarbonArc.consumer_credit("AAPL"),
        CarbonArc.food_traffic("MCD"),        # automatically add "MCD" to universe too & get market data
    ]

    cadence = Cadence(bar_size=BarSize.DAILY)

    def on_data(self, data: Slice, portfolio: PortfolioView) -> Signal:
        aapl = data.close("AAPL")
        ten_yr = data.fred("DGS10")
        mcd = data.open("MCD")    # added to universe, so can access market data (return warning)
        credit = data.carbonarc.consumer_credit("AAPL")
        ...
```
