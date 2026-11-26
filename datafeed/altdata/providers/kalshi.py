from __future__ import annotations

from datetime import datetime
from functools import partial
import math
from time import sleep
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.common import (
    date_bounds,
    parse_json_object,
    plain_error_message,
    request_with_retries,
    retry_delay,
    store_response,
)
from datafeed.errors import (
    DataFeedRequestError,
    DataUnavailableError,
    InvalidProviderResponseError,
)

_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
_MAX_ATTEMPTS = 4
_MAX_RETRY_AFTER_SECONDS = 60.0


class KalshiProvider(AltDataProvider):
    """Daily YES trade closes for explicitly named Kalshi markets."""

    def fetch_raw(
        self, series: list[str], start: datetime, end: datetime
    ) -> dict[str, Any]:
        start_bound, end_bound = date_bounds(
            start, end, utc=True,
            invalid_range_message="Kalshi requires a valid start date on or before end date",
        )
        fetched_at = pd.Timestamp.now(tz="UTC")
        params = {
            "start_ts": start_bound.value // 1_000_000_000,
            "end_ts": min(end_bound, fetched_at).value // 1_000_000_000,
            "period_interval": 1440,
        }
        result = {}
        for ticker in series:
            ticker = ticker.upper()
            result[ticker] = self._fetch_raw_series(ticker, params, fetched_at)
        return result

    def _fetch_raw_series(
        self, ticker: str, params: dict[str, Any], fetched_at: pd.Timestamp
    ) -> dict[str, Any]:
        """Assemble one market's raw payload using the batch-wide fetch cutoff."""
        payload: dict[str, Any] = {"fetched_at": fetched_at.isoformat()}
        metadata, historical = self._fetch_market_metadata(ticker, payload)
        candle_path = self._resolve_candle_path(ticker, metadata, historical, payload)
        self._fetch_candlesticks(ticker, candle_path, params, historical, payload)
        return payload

    def _fetch_market_metadata(
        self, ticker: str, payload: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """Resolve live or archived market metadata and preserve both responses."""
        market_path = f"markets/{quote(ticker, safe='')}"
        market = self._request(market_path, ticker)
        store_response(payload, "live_market", market)
        historical = market.status_code == 404
        if historical:
            market = self._request(f"historical/{market_path}", ticker)
        store_response(payload, "market", market)
        metadata = _checked_json(payload, "market", ticker).get("market")
        if not isinstance(metadata, dict):
            raise InvalidProviderResponseError(f"Kalshi returned invalid market metadata for '{ticker}'")
        return metadata, historical

    def _resolve_candle_path(
        self,
        ticker: str,
        metadata: dict[str, Any],
        historical: bool,
        payload: dict[str, Any],
    ) -> str:
        """Choose the archived endpoint or resolve a live market's parent series."""
        market_path = f"markets/{quote(ticker, safe='')}"
        if historical:
            return f"historical/{market_path}/candlesticks"

        event_ticker = metadata.get("event_ticker")
        if not isinstance(event_ticker, str) or not event_ticker:
            raise InvalidProviderResponseError(f"Kalshi returned no event ticker for '{ticker}'")
        event = self._request(f"events/{quote(event_ticker, safe='')}", ticker)
        store_response(payload, "event", event)
        event_metadata = _checked_json(payload, "event", ticker).get("event")
        series_ticker = (
            event_metadata.get("series_ticker") if isinstance(event_metadata, dict) else None
        )
        if not isinstance(series_ticker, str) or not series_ticker:
            raise InvalidProviderResponseError(f"Kalshi returned no series ticker for '{ticker}'")
        return f"series/{quote(series_ticker, safe='')}/{market_path}/candlesticks"

    def _fetch_candlesticks(
        self,
        ticker: str,
        path: str,
        params: dict[str, Any],
        historical: bool,
        payload: dict[str, Any],
    ) -> None:
        """Store candles, falling back to history if a live market was just archived."""
        candles = self._request(path, ticker, params=params)
        if not historical and candles.status_code == 404:
            store_response(payload, "live_candlesticks", candles)
            candles = self._request(
                f"historical/markets/{quote(ticker, safe='')}/candlesticks", ticker, params=params
            )
            historical = True
        store_response(payload, "candlesticks", candles)
        payload["historical"] = historical

    def normalize(
        self, raw_data: dict[str, Any], start: datetime, end: datetime
    ) -> dict[str, pd.DataFrame]:
        start_bound, end_bound = date_bounds(
            start, end, utc=True,
            invalid_range_message="Kalshi requires a valid start date on or before end date",
        )
        result = {}
        for ticker, payload in raw_data.items():
            result[ticker.upper()] = self._normalize_series(
                ticker,
                payload,
                start_bound,
                end_bound,
                range_description=f"between {start.date()} and {end.date()}",
            )
        return result

    def _normalize_series(
        self,
        ticker: str,
        payload: Any,
        start_bound: pd.Timestamp,
        end_bound: pd.Timestamp,
        *,
        range_description: str,
    ) -> pd.DataFrame:
        """Validate a raw series and assemble sorted, unique candle observations."""
        if not isinstance(payload, dict):
            raise InvalidProviderResponseError(f"Kalshi raw payload for '{ticker}' must be a dict")
        _checked_json(payload, "market", ticker)
        if "event" in payload:
            _checked_json(payload, "event", ticker)
        candles = _checked_json(payload, "candlesticks", ticker).get("candlesticks")
        if not isinstance(candles, list):
            raise InvalidProviderResponseError(f"Kalshi returned invalid candlesticks for '{ticker}'")
        fetched_at = pd.to_datetime(payload.get("fetched_at"), utc=True, errors="coerce")
        if fetched_at is None or pd.isna(fetched_at):
            raise InvalidProviderResponseError(f"Kalshi raw payload for '{ticker}' has invalid fetched_at")
        close_field = "close" if payload.get("historical") else "close_dollars"
        cutoff = min(end_bound, fetched_at)
        rows = []
        for candle in candles:
            row = self._normalize_candle(ticker, candle, close_field, start_bound, cutoff)
            if row is not None:
                rows.append(row)
        if not rows:
            raise DataUnavailableError(
                f"Kalshi returned no usable observations for '{ticker}' {range_description}"
            )
        frame = pd.DataFrame(rows).sort_values("date").set_index("date")
        if frame.index.duplicated().any():
            raise InvalidProviderResponseError(f"Kalshi returned duplicate candle timestamps for '{ticker}'")
        return frame[["value", "available_at"]]

    def _normalize_candle(
        self,
        ticker: str,
        candle: Any,
        close_field: str,
        start_bound: pd.Timestamp,
        cutoff: pd.Timestamp,
    ) -> dict[str, Any] | None:
        """Convert a candle to a row, skipping missing closes and excluded timestamps."""
        if not isinstance(candle, dict) or not isinstance(candle.get("price"), dict):
            raise InvalidProviderResponseError(f"Kalshi returned invalid candle for '{ticker}'")
        raw_close = candle["price"].get(close_field)
        if raw_close is None:
            return None
        try:
            value = float(raw_close)
            timestamp = float(candle["end_period_ts"])
            date = pd.to_datetime(timestamp, unit="s", utc=True, errors="coerce")
        except (TypeError, ValueError, KeyError, OverflowError):
            raise InvalidProviderResponseError(f"Kalshi returned invalid candle for '{ticker}'") from None
        if not math.isfinite(value) or not 0 <= value <= 1 or pd.isna(date):
            raise InvalidProviderResponseError(f"Kalshi returned invalid candle for '{ticker}'")
        if start_bound <= date <= cutoff:
            date = date.tz_localize(None)
            return {"date": date, "value": value, "available_at": date}
        return None

    def _request(
        self, path: str, ticker: str, *, params: dict[str, Any] | None = None
    ) -> requests.Response:
        return request_with_retries(
            lambda: requests.get(f"{_BASE_URL}/{path}", params=params, timeout=30),
            max_attempts=_MAX_ATTEMPTS,
            retry_status=lambda status: status == 429 or 500 <= status <= 599,
            retry_delay=partial(
                retry_delay,
                max_seconds=_MAX_RETRY_AFTER_SECONDS,
                allow_http_date=True,
            ),
            sleep=sleep,
            context=f"Kalshi request failed for '{ticker}'",
        )


def _checked_json(payload: dict[str, Any], name: str, ticker: str) -> dict[str, Any]:
    raw_text = payload.get(name)
    status = payload.get(f"{name}_status_code")
    if not isinstance(raw_text, str) or not isinstance(status, int):
        raise InvalidProviderResponseError(f"Kalshi returned no {name} response for '{ticker}'")
    if status >= 400:
        message = plain_error_message(raw_text)
        error_class = DataUnavailableError if status == 404 else DataFeedRequestError
        raise error_class(f"Kalshi {name} fetch failed for '{ticker}' (HTTP {status}): {message}")
    return parse_json_object(
        raw_text, provider="Kalshi", context=f"{name} response for '{ticker}'"
    )
