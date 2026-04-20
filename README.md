# hqg-datafeed
Centralized data service that normalizes, caches, and serves historical and live market data, economic data (e.g., FRED), and alternative datasets. 

## What Is This

Every service we run (backtester, execution engine, dashboard) currently manages its own connection to external data. The backtester talks to Yahoo Finance directly, the engine polls Alpaca directly, the dashboard will need its own data source as we scale functionality. This doesn't scale, and it's already a pain.

The hqg-datafeed is a standalone service that owns all data retrieval, caching, and delivery. No consumer needs to know where data comes from or how it's fetched/aggregated.

Currently planning out just two modes:
- **Historical**: "Give me SPY and TLT, daily bars, 2015-2024." -> returns a DataFrame.
- **Live**: "Give me a 'stream' of AAPL bars at a 1-hour cadence." -> returns bars as they're built.
