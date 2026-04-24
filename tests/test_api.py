"""
API tests for POST /data/historical.

These mock the HistoricalProvider at the seam, so the API layer is
tested independently of yfinance and the parquet cache. The goal is
to verify:
  - request validation
  - handler error -> HTTP code mapping
  - response shape matches HistoricalResponse
  - warnings surface correctly for partial data
"""
from __future__ import annotations

from datetime import datetime
from typing import List
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes import router
from src.providers.base import HistoricalProvider
from hqg_algorithms import BarSize



def _build_frame(symbols: List[str], start: str, end: str) -> pd.DataFrame:
    """Build a MultiIndex (symbol, field) frame, the shape get_data returns."""
    idx = pd.bdate_range(start=start, end=end, name="date")
    frames = {}
    for i, sym in enumerate(symbols):
        base = 100.0 + i * 10
        frames[(sym, "open")] = [base + j * 0.1 for j in range(len(idx))]
        frames[(sym, "high")] = [base + 1 + j * 0.1 for j in range(len(idx))]
        frames[(sym, "low")] = [base - 1 + j * 0.1 for j in range(len(idx))]
        frames[(sym, "close")] = [base + 0.5 + j * 0.1 for j in range(len(idx))]
        frames[(sym, "volume")] = [1_000_000 + j for j in range(len(idx))]
    df = pd.DataFrame(frames, index=idx)
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    return df


@pytest.fixture
def mock_provider():
    """A MagicMock conforming to the HistoricalProvider interface."""
    m = MagicMock(spec=HistoricalProvider)
    return m


@pytest.fixture
def client(mock_provider):
    """TestClient with a mock provider wired into app.state."""
    app = FastAPI()
    app.include_router(router)
    app.state.historical_provider = mock_provider
    return TestClient(app)



def test_historical_happy_path(client, mock_provider):
    mock_provider.get_data.return_value = _build_frame(
        ["SPY", "AAPL"], "2023-01-03", "2023-01-10"
    )

    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY", "AAPL"],
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": "1d",
        },
    )

    assert resp.status_code == 200
    body = resp.json()

    # top-level shape
    assert set(body["bars"].keys()) == {"SPY", "AAPL"}
    assert body["metadata"]["bar_size"] == "1d"
    assert body["metadata"]["symbols_returned"] == ["SPY", "AAPL"]
    assert body["warnings"] is None

    # per-bar shape
    for sym in ("SPY", "AAPL"):
        bars = body["bars"][sym]
        assert len(bars) > 0
        b0 = bars[0]
        assert set(b0.keys()) == {"date", "open", "high", "low", "close", "volume"}
        assert isinstance(b0["volume"], int)
        # date format
        datetime.strptime(b0["date"], "%Y-%m-%d")

    # provider was called with parsed datetimes + mapped BarSize
    args, kwargs = mock_provider.get_data.call_args
    # handler calls positionally: (symbols, start, end, bar_size)
    symbols, start, end, bar_size = args
    assert symbols == ["SPY", "AAPL"]
    assert start == datetime(2023, 1, 3)
    assert end == datetime(2023, 1, 10)
    assert bar_size == BarSize.DAILY


# validation errors
def test_empty_symbols_list_rejected(client):
    resp = client.post(
        "/data/historical",
        json={
            "symbols": [],      # emppty
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": "1d",
        },
    )
    assert resp.status_code == 422


def test_bad_bar_size_rejected(client):
    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY"],
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": "1y",   # not in enum
        },
    )
    assert resp.status_code == 422


def test_start_after_end_rejected(client, mock_provider):
    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY"],
            "start_date": "2023-06-01",
            "end_date": "2023-01-01",
            "bar_size": "1d",
        },
    )
    assert resp.status_code == 400
    assert "start_date must be before end_date" in resp.text
    mock_provider.get_data.assert_not_called()


def test_bad_date_format_rejected(client):
    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY"],
            "start_date": "01/03/2023",     # wrong format
            "end_date": "2023-01-10",
            "bar_size": "1d",
        },
    )
    # handler's datetime.strptime raises ValueError -> 400
    assert resp.status_code == 400


# ticker unavailable is error
def test_provider_raises_value_error_maps_to_404(client, mock_provider):
    mock_provider.get_data.side_effect = ValueError("yfinance returned no data for ['ZZZZ']")

    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["ZZZZ"],
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": "1d",
        },
    )
    assert resp.status_code == 404
    body = resp.json()
    # FastAPI wraps our ErrorDetail under "detail"
    assert body["detail"]["error"] == "data_unavailable"
    assert body["detail"]["symbols"] == ["ZZZZ"]


def test_empty_frame_returns_404(client, mock_provider):
    """Provider returns an empty frame; handler raises DataUnavailableError."""
    mock_provider.get_data.return_value = pd.DataFrame(
        columns=pd.MultiIndex.from_tuples([], names=[None, None])
    )

    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY"],
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": "1d",
        },
    )
    assert resp.status_code == 404


# warnings
def test_missing_symbol_produces_warning(client, mock_provider):
    """
    Request [SPY, LEBRON_THE_GOAT], provider returns only SPY; response includes
    a warning for LEBRON_THE_GOAT but still 200s with SPY bars.
    """
    mock_provider.get_data.return_value = _build_frame(
        ["SPY"], "2023-01-03", "2023-01-10"
    )

    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY", "LEBRON_THE_GOAT"],
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": "1d",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["bars"].keys()) == {"SPY"}
    assert body["warnings"] is not None
    warned_symbols = {w["symbol"] for w in body["warnings"]}
    assert "LEBRON_THE_GOAT" in warned_symbols


# bar size mapping
@pytest.mark.parametrize(
    "api_value, hqg_value",
    [
        ("1d", BarSize.DAILY),
        ("1w", BarSize.WEEKLY),
        ("1m", BarSize.MONTHLY),
        ("1q", BarSize.QUARTERLY),
    ],
)
def test_bar_size_mapping(client, mock_provider, api_value, hqg_value):
    mock_provider.get_data.return_value = _build_frame(
        ["SPY"], "2023-01-03", "2023-01-10"
    )
    resp = client.post(
        "/data/historical",
        json={
            "symbols": ["SPY"],
            "start_date": "2023-01-03",
            "end_date": "2023-01-10",
            "bar_size": api_value,
        },
    )
    assert resp.status_code == 200
    args, _ = mock_provider.get_data.call_args
    assert args[3] == hqg_value