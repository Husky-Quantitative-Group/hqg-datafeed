from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


class AltDataProvider(ABC):
    """Interface for batched alternative-data providers."""

    @abstractmethod
    def fetch(
        self,
        series: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        """Fetch each requested series over exactly [start, end]."""
        raise NotImplementedError
