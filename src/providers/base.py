from datetime import datetime
from typing import List, Protocol

import pandas as pd

from hqg_algorithms import BarSize


class HistoricalProvider(Protocol):
    """
    Retrieves historical OHLCV data for a set of symbols.

    Returns a MultiIndex (symbol, field) DataFrame aligned across 
    all requested symbols.
    """

    def get_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        bar_size: BarSize = BarSize.DAILY,
    ) -> pd.DataFrame: ...

# TODO: LiveProvider