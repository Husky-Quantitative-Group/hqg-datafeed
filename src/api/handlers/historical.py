import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd

from ...providers.base import HistoricalProvider
from ..models import (
    HistoricalMetadata,
    HistoricalRequest,
    HistoricalResponse,
    SymbolWarning,
    DataUnavailableError,
)

logger = logging.getLogger(__name__)


async def handle_historical(body: HistoricalRequest, historical_provider: HistoricalProvider) -> HistoricalResponse:
    start = datetime.strptime(body.start_date, "%Y-%m-%d")
    end = datetime.strptime(body.end_date, "%Y-%m-%d")
    if start >= end:
        raise ValueError("start_date must be before end_date")

    try:
        df = await asyncio.to_thread(
            historical_provider.get_data, body.symbols, start, end, body.bar_size.value
        )

    except ValueError as e:
        raise DataUnavailableError(str(e), symbols=body.symbols)

    bars_out, warnings = _serialize_dataframe(df, body.symbols)

    if not bars_out:
        raise DataUnavailableError("No data for any requested symbol", symbols=body.symbols)

    all_dates = [b["date"] for bars in bars_out.values() for b in bars]

    return HistoricalResponse(
        bars=bars_out,
        metadata=HistoricalMetadata(
            bar_size=body.bar_size,
            start_date=min(all_dates),
            end_date=max(all_dates),
            symbols_returned=list(bars_out.keys()),
        ),
        warnings=warnings if warnings else None,
    )


def _serialize_dataframe(df: pd.DataFrame, symbols: List[str]) -> Tuple[Dict, List[SymbolWarning]]:
    """
    Convert a MultiIndex (symbol, field) DataFrame into the response shape. 
    """
    bars_out: Dict[str, list] = {}
    warnings: List[SymbolWarning] = []

    symbols_in_result = set(col[0] for col in df.columns)

    for symbol in symbols:
        if symbol not in symbols_in_result:
            warnings.append(SymbolWarning(symbol=symbol, message="No data returned"))
            continue

        sym_df = df[symbol].copy()
        sym_df = sym_df.dropna(how="all")

        if sym_df.empty:
            warnings.append(SymbolWarning(symbol=symbol, message="No bars in range"))
            continue

        sym_df.index = sym_df.index.strftime("%Y-%m-%d")
        records = sym_df.reset_index().rename(columns={"index": "date"}).to_dict("records")

        for r in records:
            r["volume"] = int(r.get("volume", 0))

        bars_out[symbol] = records

    return bars_out, warnings
