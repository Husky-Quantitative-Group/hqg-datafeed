"""
Shared fixtures.

Every fixture that creates a cache uses pytest's `tmp_path`,
so tests never touch the real parquet store on disk.
"""
from __future__ import annotations

import pandas as pd
import pytest

from src.storage.parquet_store import ParquetStore
from src.providers.yfinance import YFinanceProvider


@pytest.fixture
def tmp_store(tmp_path) -> ParquetStore:
    """Fresh, empty ParquetStore rooted in a per-test temp dir."""
    return ParquetStore(cache_dir=str(tmp_path / "cache"))


@pytest.fixture
def provider(tmp_store) -> YFinanceProvider:
    """YFinanceProvider backed by the temp store."""
    return YFinanceProvider(store=tmp_store)

def _fake_daily_frame(start: str, end: str, seed: float = 100.0) -> pd.DataFrame:
    """
    Deterministic fake OHLCV frame with a business-day index.
    Shape matches what _extract_symbol would produce.
    """
    idx = pd.bdate_range(start=start, end=end, name="date")
    n = len(idx)
    # simple deterministic walk so the numbers look plausible
    closes = [seed + i * 0.5 for i in range(n)]
    opens = [c - 0.25 for c in closes]
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    vols = [1_000_000 + i * 1000 for i in range(n)]
    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": vols,
        },
        index=idx,
    )


@pytest.fixture
def fake_daily_frame():
    """Factory exposed to tests that need fake OHLCV data."""
    return _fake_daily_frame