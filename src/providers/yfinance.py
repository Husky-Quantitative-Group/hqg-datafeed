import logging
import threading
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import yfinance as yf

from hqg_algorithms import BarSize

from src.storage.parquet_store import ParquetStore, DEFAULT_HISTORY_START

logger = logging.getLogger(__name__)

# yf.download() is not thread-safe: concurrent calls corrupt shared internal
_yfinance_lock = threading.Lock()

_RESAMPLE_RULES = {
    BarSize.DAILY: None,
    BarSize.WEEKLY: "W-FRI",
    BarSize.MONTHLY: "M",
    BarSize.QUARTERLY: "Q",
}

# TODO: currently only supports DAILY as lowest granularity. 
#   Need to also add HOURLY store.
class YFinanceProvider:
    """
    Implements HistoricalProvider.

    - Always fetches daily bars from YF, going back to 2000 so the
      cache is broad and future requests are almost always hits.
    - Delegates all persistence to ParquetStore.
    - Resamples to the requested bar_size on-the-fly after reading from cache.
    """

    def __init__(self, store: ParquetStore):
        self._store = store

    def _fetch_from_yf(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        """
        Download daily data for symbols between start and end (inclusive)
        and return a dict mapping each symbol to its flat OHLCV DF.
        """
        logger.info(f"yfinance download: {symbols}  {start.date()} -> {end.date()}")

        with _yfinance_lock:
            raw = yf.download(
                tickers=symbols,
                start=start,
                end=end + timedelta(days=1),
                interval="1d",
                progress=False,
                group_by="ticker",
                auto_adjust=True,
            )

        if raw.empty:
            raise ValueError(f"yfinance returned no data for {symbols}")

        result: dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            result[symbol] = self._extract_symbol(raw, symbol)
        return result

    def _extract_symbol(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Extract a flat per-symbol OHLCV DataFrame from a yf.download() result.
        Handles both single-symbol (flat columns) and multi-symbol (MultiIndex).
        """
        fields = ["Open", "High", "Low", "Close", "Volume"]

        if isinstance(data.columns, pd.MultiIndex):
            level_values = data.columns.get_level_values(0)
            if symbol in level_values:
                sub = data[symbol]
            elif symbol.upper() in level_values:
                sub = data[symbol.upper()]
            else:
                raise ValueError(f"Symbol {symbol} not in download result")
            cols = {f.lower(): sub[f] for f in fields if f in sub.columns}
        else:
            cols = {f.lower(): data[f] for f in fields if f in data.columns}

        df = pd.DataFrame(cols, index=data.index)
        df.index.name = "date"
        return df.dropna(how="all")


    def _resample(self, df: pd.DataFrame, bar_size: BarSize) -> pd.DataFrame:
        """
        Resample daily bars to a coarser frequency.

        Uses manual grouping instead of pd.resample() so the output
        index contains the last actual trading date in each period,
        not the calendar period-end.
        """
        rule = _RESAMPLE_RULES.get(bar_size)
        if rule is None:
            return df

        agg = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
        agg = {k: v for k, v in agg.items() if k in df.columns}

        # assign each daily row to a calendar period bucket & aggregate
        periods = df.index.to_period(rule)
        grouped = df.groupby(periods)
        resampled = grouped.agg(agg).dropna(how="all")

        # replace period-end dates with the last real trading date per group
        real_dates = grouped.nth(-1).index
        resampled.index = pd.DatetimeIndex(real_dates, name=df.index.name)

        return resampled


    def _last_trading_day(self) -> datetime:
        """
        Approximate last trading day (no holiday calendar).
        So we don't re-fetch "missing" data if today is a weekend.
        """
        # TODO: if today is a holiday, we will always refetch, as 
        #   cache_end is last trading day, which is not today. merry christmas.
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if today.weekday() == 5:        # Saturday
            return today - timedelta(days=1)
        elif today.weekday() == 6:      # Sunday
            return today - timedelta(days=2)
        #NOTE: when implementing holiday check, precompute & cache
        #    NYSE calendar (it's slow).
        return today


    def get_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        bar_size: BarSize = BarSize.DAILY,
    ) -> pd.DataFrame:
        """
        Return a MultiIndex (symbol, field) DataFrame for the requested
        symbols and date range.

        Data is always cached/fetched as daily bars and resampled to
        bar_size on the fly.
        """
        # widen the fetch window so the cache is useful for future requests
        fetch_start = min(start_date, DEFAULT_HISTORY_START)
        fetch_end = self._last_trading_day()

        # lockless pre-scan
        probable_misses = self._store.find_misses(symbols, fetch_start, fetch_end)

        # lock + double-check + fetch
        if probable_misses:
            sorted_misses = sorted(set(probable_misses))
            locks = self._store.acquire_locks(sorted_misses)

            try:
                confirmed_misses = [
                    s for s in sorted_misses
                    if not self._store.covers(s, fetch_start, fetch_end)
                ]

                if confirmed_misses:
                    new_data = self._fetch_from_yf(confirmed_misses, 
                                                   fetch_start, fetch_end)

                    for symbol in confirmed_misses:
                        if symbol not in new_data:
                            raise ValueError(f"No data returned for {symbol}")
                        self._store.merge_and_write(symbol, new_data[symbol])
            finally:
                self._store.release_locks(locks)

        # build result from cache
        frames: dict[tuple[str, str], pd.Series] = {}

        for symbol in symbols:
            sym_df = self._store.read(symbol)
            if sym_df is None:
                raise ValueError(f"Cache miss after fetch for {symbol}")

            # slice to requested window
            sym_df = sym_df.loc[
                (sym_df.index >= pd.Timestamp(start_date))
                & (sym_df.index <= pd.Timestamp(end_date))
            ]

            # resample if needed
            sym_df = self._resample(sym_df, bar_size)

            for field in ("open", "high", "low", "close", "volume"):
                if field in sym_df.columns:
                    frames[(symbol, field)] = sym_df[field]

        if not frames:
            raise ValueError(f"No data available for {symbols}")

        result = pd.DataFrame(frames)
        result.columns = pd.MultiIndex.from_tuples(result.columns)

        # mixed calendars (e.g., BTC-USD + equities) can create rows
        # with missing closes. drop those rows, they break portfolio
        # valuation downstream.
        close_cols = [col for col in result.columns if col[1] == "close"]
        if close_cols:
            result = result.dropna(subset=close_cols, how="any")

        logger.info(
            f"Returning {len(result)} bars for {symbols} "
            f"({start_date.date()} to {end_date.date()}, bar_size={bar_size})"
        )
        return result