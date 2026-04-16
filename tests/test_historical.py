"""
Component tests for YFinanceProvider + ParquetStore.

Use a temp ParquetStore per test, so the real cache on disk is never touched.

Marked tests:
  -m network      : hits real yfinance (slower, requires internet)
  -m slow         : benchmark / long-running
  (unmarked)      : fast, fully offline via monkeypatched yf.download
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

import pandas as pd
import pandas_market_calendars as mcal
import pytest

from src.providers import yfinance as yf_module
from src.providers.base import HistoricalProvider
from src.storage.parquet_store import DEFAULT_HISTORY_START
from hqg_algorithms import BarSize


@pytest.mark.network
def test_historical_spy_aapl_real_network(provider: HistoricalProvider):
    """
    E2E: fetch SPY + AAPL from real yfinance into an empty store;
    verify the returned frame covers the requested window and contains
    only NYSE trading days.
    """
    start = datetime(2023, 1, 1)
    end = datetime(2023, 3, 31)         # regular trade day
    trade_start = datetime(2023, 1, 3)  # first trading day of 2023

    df = provider.get_data(
        symbols=["SPY", "AAPL"],
        start_date=start,
        end_date=end,
        bar_size=BarSize.DAILY,
    )

    # structure
    assert isinstance(df.columns, pd.MultiIndex)
    assert {"SPY", "AAPL"} <= set(df.columns.get_level_values(0))
    for sym in ("SPY", "AAPL"):
        for field in ("open", "high", "low", "close", "volume"):
            assert (sym, field) in df.columns, f"missing ({sym},{field})"

    # correct window bounds
    assert df.index.min() >= pd.Timestamp(trade_start)
    assert df.index.max() <= pd.Timestamp(end)

    # all returned dates are days NYSE is open
    nyse = mcal.get_calendar("NYSE")
    expected_sessions = nyse.valid_days(start_date=start, end_date=end)
    expected_dates = {d.date() for d in expected_sessions}
    returned_dates = {d.date() for d in df.index}

    non_trading = returned_dates - expected_dates
    assert not non_trading, f"returned non-trading dates: {sorted(non_trading)}"

    missing = expected_dates - returned_dates
    assert not missing, f"missing trading dates: {sorted(missing)}"

    # close prices exist, are positive
    for sym in ("SPY", "AAPL"):
        closes = df[(sym, "close")]
        assert closes.notna().all()
        assert (closes > 0).all()



@pytest.mark.network
def test_cache_widens_to_default_history_start(provider, tmp_store):
    """
    Even if the caller asks for a narrow recent window, the provider
    should fetch & cache all the way back to DEFAULT_HISTORY_START so
    future requests are cheap.
    """
    # request a narrow recent window
    provider.get_data(
        symbols=["SPY"],
        start_date=datetime(2023, 6, 1),
        end_date=datetime(2023, 6, 30),
        bar_size=BarSize.DAILY,
    )

    cached = tmp_store.read("SPY")
    assert cached is not None
    cache_min = cached.index.min()

    # SPY inception is 1993; yfinance should give us data back to
    # at least DEFAULT_HISTORY_START. We allow a small slack for
    # exchange holidays at the very start of January 2000.
    assert cache_min <= pd.Timestamp(DEFAULT_HISTORY_START) + pd.Timedelta(days=10), (
        f"cache only goes back to {cache_min.date()}, "
        f"expected <= ~{DEFAULT_HISTORY_START.date()}"
    )


# 2nd call for another ticker does NOT re-hit yf
class _YFSpy:
    """Wraps yf.download to count calls and forward."""
    def __init__(self, real_download):
        self._real = real_download
        self.calls = 0
        self._lock = Lock()

    def __call__(self, *args, **kwargs):
        with self._lock:
            self.calls += 1
        return self._real(*args, **kwargs)


@pytest.mark.network
def test_second_call_hits_cache_not_yfinance(provider, monkeypatch):
    """
    After a first call warms the cache, a second call for the same ticker
    must NOT invoke yf.download again.
    """
    spy = _YFSpy(yf_module.yf.download)
    monkeypatch.setattr(yf_module.yf, "download", spy)

    args1 = dict(
        symbols=["SPY", "IEF"],
        start_date=datetime(2023, 1, 3),
        end_date=datetime(2023, 3, 31),
        bar_size=BarSize.DAILY,
    )

    # first call: cache is empty, must fetch
    provider.get_data(**args1)
    assert spy.calls == 1, "first call should trigger exactly one download"

    # second call: cache should be filled since within DEFAULT_HISTORY_START;
    # should NOT re-fetch from yf
    args2 = dict(
        symbols=["SPY", "IEF"],
        start_date=datetime(2022, 1, 3),
        end_date=datetime(2022, 3, 31),
        bar_size=BarSize.DAILY,
    )

    provider.get_data(**args2)
    assert spy.calls == 1, f"second call triggered {spy.calls - 1} extra download(s)"

    # third call: only SPY; still should be cached
    provider.get_data(
        symbols=["SPY"],
        start_date=datetime(2025, 2, 1),
        end_date=datetime(2025, 2, 28),
        bar_size=BarSize.DAILY,
    )
    assert spy.calls == 1



def test_concurrent_calls_dedup_to_single_fetch(provider, monkeypatch, fake_daily_frame):
    """
    Fire 50 concurrent get_data() calls for the same uncached symbol.
    The lock + double-check pattern in YFinanceProvider should collapse
    them into a single yf.download call.

    Uses a fake yf.download (no network) so we can assert exact counts.
    """
    call_count = 0
    count_lock = Lock()

    def fake_download(tickers, start, end, interval, progress, group_by, auto_adjust):
        nonlocal call_count
        with count_lock:
            call_count += 1
        # Simulate a slow network so threads overlap inside lock acquisition window
        time.sleep(0.2)

        # Build a frame in the shape yf.download returns for a single
        # ticker (flat columns, title-case).
        df = fake_daily_frame("2000-01-03", "2024-12-31")
        df.columns = [c.title() for c in df.columns]  # Open/High/Low/Close/Volume
        return df

    monkeypatch.setattr(yf_module.yf, "download", fake_download)

    req = dict(
        symbols=["FAKE1"],
        start_date=datetime(2023, 1, 3),
        end_date=datetime(2023, 3, 31),
        bar_size=BarSize.DAILY,
    )

    N = 50
    with ThreadPoolExecutor(max_workers=N) as ex:
        futures = [ex.submit(provider.get_data, **req) for _ in range(N)]
        results = [f.result() for f in as_completed(futures)]

    assert len(results) == N
    assert call_count == 1, (
        f"expected exactly 1 yfinance call for 50 concurrent requests, "
        f"got {call_count}"
    )

    # every caller got a valid frame
    for df in results:
        assert ("FAKE1", "close") in df.columns
        assert not df.empty


# benchmark
@pytest.mark.slow
def test_cached_read_benchmark(provider, monkeypatch, fake_daily_frame):
    """
    Measure average latency of a cached get_data() call.

    This is the SLOWEST the historical system will ever be for a cached
    hit (no network, no write). We assert a loose ceiling so regressions
    that make the cache path dramatically slower get caught.
    """
    # Use a fake yf.download for the one-time warmup so this test is
    # offline-friendly and deterministic.
    def fake_download(tickers, start, end, interval, progress, group_by, auto_adjust):
        df = fake_daily_frame("2000-01-03", "2024-12-31")
        df.columns = [c.title() for c in df.columns]
        return df

    monkeypatch.setattr(yf_module.yf, "download", fake_download)

    req = dict(
        symbols=["BENCH"],
        start_date=datetime(2023, 1, 3),
        end_date=datetime(2023, 12, 31),
        bar_size=BarSize.DAILY,
    )

    # warm cache
    provider.get_data(**req)

    # time cached reads
    N = 50
    t0 = time.perf_counter()
    for _ in range(N):
        provider.get_data(**req)
    elapsed = time.perf_counter() - t0
    avg_ms = (elapsed / N) * 1000

    print(f"\n[benchmark] cached read avg: {avg_ms:.2f} ms over {N} calls")

    # On my cheeks laptop, avg was 160ms.
    MS_CEIL = 200
    assert avg_ms < MS_CEIL, f"cached read too slow: {avg_ms:.2f} ms avg"



def test_per_symbol_cache_independence(provider, monkeypatch, fake_daily_frame):
    """
    Requesting [A, B] should fetch both, but a follow-up for just [A]
    or just [B] should be fully cached.
    """
    calls = []

    def fake_download(tickers, start, end, interval, progress, group_by, auto_adjust):
        calls.append(list(tickers) if isinstance(tickers, list) else [tickers])
        # yf returns a MultiIndex when given multiple tickers
        frames = {}
        tickers_list = list(tickers) if isinstance(tickers, list) else [tickers]
        for t in tickers_list:
            df = fake_daily_frame("2000-01-03", "2024-12-31", seed=100.0 + len(t))
            df.columns = [c.title() for c in df.columns]
            frames[t] = df
        if len(tickers_list) == 1:
            return frames[tickers_list[0]]
        return pd.concat(frames, axis=1)

    monkeypatch.setattr(yf_module.yf, "download", fake_download)

    req = dict(
        start_date=datetime(2023, 1, 3),
        end_date=datetime(2023, 3, 31),
        bar_size=BarSize.DAILY,
    )

    provider.get_data(symbols=["AAA", "BBB"], **req)
    assert len(calls) == 1

    # follow-ups should not trigger any more downloads
    provider.get_data(symbols=["AAA"], **req)
    provider.get_data(symbols=["BBB"], **req)
    provider.get_data(symbols=["BBB", "AAA"], **req)
    assert len(calls) == 1, f"expected 1 download total, got {len(calls)}: {calls}"