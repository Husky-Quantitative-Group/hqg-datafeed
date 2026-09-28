from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class MarketData:
    """Securities and alternative-data frames"""

    securities: pd.DataFrame
    alt_data: pd.DataFrame
