from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd


class AltDataProvider(ABC):
    """Interface for batched alternative-data providers."""

    def fetch(
        self,
        series: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        """Fetch each requested series over exactly [start, end] inclusive of end."""
        return self.normalize(self.fetch_raw(series, start, end), start, end)

    @abstractmethod
    def fetch_raw(
        self,
        series: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        """Fetch unnormalized provider payloads for each requested series. Used for debugging/inspection."""
        raise NotImplementedError

    @abstractmethod
    def normalize(
        self,
        raw_data: dict[str, Any],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        """Normalize raw provider payloads into DataFrames."""
        raise NotImplementedError
