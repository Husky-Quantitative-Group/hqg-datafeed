from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
from hqg_algorithms import BarSize

from pydantic import BaseModel, Field




class StreamStatus(str, Enum):
    ACTIVE = "active"
    STOPPED = "stopped"


class Bar(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class LiveBar(Bar):
    symbol: str
    bar_start: datetime
    bar_end: datetime


class SymbolWarning(BaseModel):
    symbol: str
    message: str
    available_from: Optional[str] = None
    available_to: Optional[str] = None


# historical
class HistoricalRequest(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    start_date: str = Field(..., examples=["2015-01-01"])
    end_date: str = Field(..., examples=["2024-01-01"])
    bar_size: BarSize = BarSize.DAILY


class HistoricalMetadata(BaseModel):
    bar_size: BarSize
    start_date: str          # actual first bar returned
    end_date: str            # actual last bar returned
    symbols_returned: List[str]


class HistoricalResponse(BaseModel):
    bars: Dict[str, List[Bar]]
    metadata: HistoricalMetadata
    warnings: Optional[List[SymbolWarning]] = None


# error
class ErrorDetail(BaseModel):
    error: str
    message: str
    symbols: Optional[List[str]] = None


class DataUnavailableError(Exception):
    def __init__(self, message: str, symbols: Optional[List[str]] = None):
        self.message = message
        self.symbols = symbols
