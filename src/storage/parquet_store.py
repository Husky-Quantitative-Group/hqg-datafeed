import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Earliest date we'll ever request. Keeps cache broad so future requests
# for the same symbol are almost always a hit.
DEFAULT_HISTORY_START = datetime(2000, 1, 1)


class ParquetStore:
    """
    Thread-safe, symbol-level parquet cache.
    """

    def __init__(self, cache_dir: str):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # one lock per symbol; protects both the file write and the
        # double-check read. shared across all callers.
        self._locks: dict[str, threading.Lock] = {}
        self._locks_mutex = threading.Lock()

    def _get_lock(self, symbol: str) -> threading.Lock:
        with self._locks_mutex:
            if symbol not in self._locks:
                self._locks[symbol] = threading.Lock()
            return self._locks[symbol]

    def _path(self, symbol: str) -> Path:
        return self._cache_dir / f"{symbol}.parquet"

    def read(self, symbol: str) -> Optional[pd.DataFrame]:
        """Read cached data for a symbol. Returns None if missing or corrupt."""
        path = self._path(symbol)
        if not path.exists():
            return None

        try:
            df = pd.read_parquet(path)
            if not df.empty:
                return df
        except Exception:
            logger.warning(f"Corrupt cache for {symbol}, deleting")
            with self._get_lock(symbol):
                path.unlink(missing_ok=True)

        return None

    def write(self, symbol: str, df: pd.DataFrame) -> None:
        """Atomic write via tmp + os.replace()."""
        path = self._path(symbol)
        tmp = path.with_suffix(".parquet.tmp")
        df.to_parquet(tmp)
        os.replace(tmp, path)

    def merge_and_write(self, symbol: str, new_data: pd.DataFrame) -> None:
        """
        Merge new data with existing cache and write. If no existing
        cache, just writes the new data. Deduplicates on index,
        keeping the latest row on conflict.
        """
        existing = self.read(symbol)
        if existing is not None:
            merged = pd.concat([existing, new_data])
            merged = merged[~merged.index.duplicated(keep="last")]
            merged = merged.sort_index()
        else:
            merged = new_data
        self.write(symbol, merged)

    def covers(
        self,
        symbol: str,
        fetch_start: datetime,
        fetch_end: datetime,
    ) -> bool:
        """ Is cache full enough to skip a fetch?"""
        cached = self.read(symbol)
        if cached is None:
            return False

        cache_min = cached.index.min().date()
        cache_max = cached.index.max().date()

        # if requesting our default range or narrower, a previous fetch
        # already requested that far back, so start is covered!
        if fetch_start >= DEFAULT_HISTORY_START:
            start_covered = True
        else:
            # incl big buffer for non-trading days
            start_covered = cache_min <= (fetch_start + timedelta(days=30)).date()  

        end_covered = cache_max >= fetch_end.date()

        covers = end_covered and start_covered
        logger.debug(
            f"covers({symbol}): cache={cache_min}..{cache_max}, "
            f"requested={fetch_start.date()}..{fetch_end.date()}, covers={covers}"
        )
        return covers


    def find_misses(
        self,
        symbols: list[str],
        fetch_start: datetime,
        fetch_end: datetime,
    ) -> list[str]:
        """
        Lockless pre-scan: return symbols whose cache doesn't cover
        the requested range. Caller should double-check under lock
        before actually fetching.
        """
        return [s for s in symbols if not self.covers(s, fetch_start, fetch_end)]

    def acquire_locks(self, symbols: list[str]) -> dict[str, threading.Lock]:
        """
        Acquire per-symbol locks in sorted order (prevents deadlock).
        Returns the lock dict so the caller can release in reverse order.
        """
        sorted_symbols = sorted(set(symbols))
        locks = {s: self._get_lock(s) for s in sorted_symbols}
        for s in sorted_symbols:
            locks[s].acquire()
        return locks

    @staticmethod
    def release_locks(locks: dict[str, threading.Lock]) -> None:
        """Release locks in reverse sorted order."""
        for s in reversed(sorted(locks.keys())):
            locks[s].release()