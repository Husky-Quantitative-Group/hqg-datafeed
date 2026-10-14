from __future__ import annotations

from datetime import datetime, timedelta
import logging
import threading

import pandas as pd

from datafeed.errors import DataFeedError

try:
    import yfinance as yf
except ImportError:  
    yf = None

logger = logging.getLogger(__name__)

_YFINANCE_LOCK = threading.Lock()
_FIELDS = ("open", "high", "low", "close", "volume")
_YF_FIELDS = ("Open", "High", "Low", "Close", "Volume")


def empty_securities_frame() -> pd.DataFrame:
    return pd.DataFrame(
        index=pd.DatetimeIndex([], name="date"),
        columns=pd.MultiIndex.from_tuples([], names=["symbol", "field"]),
    )


def get_securities_frame(
    symbols: list[str],
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Download daily OHLCV for symbols over exactly [start, end]."""
    unique_symbols = _dedupe_symbols(symbols)
    if not unique_symbols:
        return empty_securities_frame()

    if yf is None:
        raise DataFeedError("yfinance is required for securities data")

    logger.info("Downloading securities data for %s from %s to %s", unique_symbols, start, end)
    with _YFINANCE_LOCK:
        raw = yf.download(
            tickers=unique_symbols,
            start=start,
            end=end + timedelta(days=1),
            interval="1d",
            progress=False,
            group_by="ticker",
            auto_adjust=True,
        )

    if raw.empty:
        raise DataFeedError(
            f"yfinance returned no data for requested symbols: {', '.join(unique_symbols)}"
        )

    frames: dict[tuple[str, str], pd.Series] = {}
    missing_symbols: list[str] = []

    for symbol in unique_symbols:
        try:
            symbol_frame = _extract_symbol(raw, symbol)
        except DataFeedError:
            missing_symbols.append(symbol)
            continue

        symbol_frame = _slice_frame(symbol_frame, start, end)
        if symbol_frame.empty:
            missing_symbols.append(symbol)
            continue

        for field in _FIELDS:
            if field in symbol_frame.columns:
                frames[(symbol, field)] = symbol_frame[field]

        if not any((symbol, field) in frames for field in _FIELDS):
            missing_symbols.append(symbol)

    if missing_symbols:
        raise DataFeedError(
            "No securities data available in requested range for: "
            + ", ".join(missing_symbols)
        )

    result = pd.DataFrame(frames)
    result.index = pd.to_datetime(result.index)
    result.index.name = "date"
    result.columns = pd.MultiIndex.from_tuples(result.columns, names=["symbol", "field"])
    return _slice_frame(result, start, end)


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _slice_frame(frame: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    index = pd.to_datetime(frame.index)
    sliced = frame.loc[(index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))]
    sliced.index = pd.to_datetime(sliced.index)
    sliced.index.name = "date"
    return sliced


def _extract_symbol(data: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Extract one symbol from yfinance output and drop fully empty rows."""
    if isinstance(data.columns, pd.MultiIndex):
        level_values = list(data.columns.get_level_values(0).unique())
        lookup = {str(value).lower(): value for value in level_values}
        matched_symbol = lookup.get(symbol.lower())
        if matched_symbol is None:
            raise DataFeedError(f"Symbol {symbol} not in download result")
        source = data[matched_symbol]
    else:
        source = data

    columns = {}
    source_columns = {str(column).lower(): column for column in source.columns}
    for yf_field, field in zip(_YF_FIELDS, _FIELDS):
        source_column = source_columns.get(yf_field.lower())
        if source_column is not None:
            columns[field] = source[source_column]

    frame = pd.DataFrame(columns, index=source.index)
    frame.index = pd.to_datetime(frame.index)
    frame.index.name = "date"
    return frame.dropna(how="all")
