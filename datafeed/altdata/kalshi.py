from __future__ import annotations

from datetime import datetime
from email.utils import parsedate_to_datetime
import json
import math
from time import sleep
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests

from datafeed.altdata.base import AltDataProvider
from datafeed.errors import DataFeedError

_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
_MAX_ATTEMPTS = 4
_MAX_RETRY_AFTER_SECONDS = 60.0


class KalshiProvider(AltDataProvider):
    """Daily YES trade closes for explicitly named Kalshi markets."""

    def fetch_raw(
        self, series: list[str], start: datetime, end: datetime
    ) -> dict[str, Any]:
        start_bound, end_bound = _date_bounds(start, end)
        fetched_at = pd.Timestamp.now(tz="UTC")
        params = {
            "start_ts": start_bound.value // 1_000_000_000,
            "end_ts": min(end_bound, fetched_at).value // 1_000_000_000,
            "period_interval": 1440,
        }
        result = {}
        for ticker in series:
            ticker = ticker.upper()
            market_path = f"markets/{quote(ticker, safe='')}"
            payload: dict[str, Any] = {"fetched_at": fetched_at.isoformat()}
            market = self._request(market_path, ticker)
            _store_response(payload, "live_market", market)
            historical = market.status_code == 404
            if historical:
                market = self._request(f"historical/{market_path}", ticker)
            _store_response(payload, "market", market)
            metadata = _checked_json(payload, "market", ticker).get("market")
            if not isinstance(metadata, dict):
                raise DataFeedError(f"Kalshi returned invalid market metadata for '{ticker}'")

            if historical:
                candle_path = f"historical/{market_path}/candlesticks"
            else:
                event_ticker = metadata.get("event_ticker")
                if not isinstance(event_ticker, str) or not event_ticker:
                    raise DataFeedError(f"Kalshi returned no event ticker for '{ticker}'")
                event = self._request(f"events/{quote(event_ticker, safe='')}", ticker)
                _store_response(payload, "event", event)
                event_metadata = _checked_json(payload, "event", ticker).get("event")
                series_ticker = (
                    event_metadata.get("series_ticker") if isinstance(event_metadata, dict) else None
                )
                if not isinstance(series_ticker, str) or not series_ticker:
                    raise DataFeedError(f"Kalshi returned no series ticker for '{ticker}'")
                candle_path = f"series/{quote(series_ticker, safe='')}/{market_path}/candlesticks"

            candles = self._request(candle_path, ticker, params=params)
            if not historical and candles.status_code == 404:
                _store_response(payload, "live_candlesticks", candles)
                candles = self._request(
                    f"historical/{market_path}/candlesticks", ticker, params=params
                )
                historical = True
            _store_response(payload, "candlesticks", candles)
            payload["historical"] = historical
            result[ticker] = payload
        return result

    def normalize(
        self, raw_data: dict[str, Any], start: datetime, end: datetime
    ) -> dict[str, pd.DataFrame]:
        start_bound, end_bound = _date_bounds(start, end)
        result = {}
        for ticker, payload in raw_data.items():
            if not isinstance(payload, dict):
                raise DataFeedError(f"Kalshi raw payload for '{ticker}' must be a dict")
            _checked_json(payload, "market", ticker)
            if "event" in payload:
                _checked_json(payload, "event", ticker)
            candles = _checked_json(payload, "candlesticks", ticker).get("candlesticks")
            if not isinstance(candles, list):
                raise DataFeedError(f"Kalshi returned invalid candlesticks for '{ticker}'")
            fetched_at = pd.to_datetime(payload.get("fetched_at"), utc=True, errors="coerce")
            if fetched_at is None or pd.isna(fetched_at):
                raise DataFeedError(f"Kalshi raw payload for '{ticker}' has invalid fetched_at")
            close_field = "close" if payload.get("historical") else "close_dollars"
            rows = []
            for candle in candles:
                if not isinstance(candle, dict) or not isinstance(candle.get("price"), dict):
                    raise DataFeedError(f"Kalshi returned invalid candle for '{ticker}'")
                raw_close = candle["price"].get(close_field)
                if raw_close is None:
                    continue
                try:
                    value = float(raw_close)
                    timestamp = float(candle["end_period_ts"])
                    date = pd.to_datetime(timestamp, unit="s", utc=True, errors="coerce")
                except (TypeError, ValueError, KeyError, OverflowError):
                    raise DataFeedError(f"Kalshi returned invalid candle for '{ticker}'") from None
                if not math.isfinite(value) or not 0 <= value <= 1 or pd.isna(date):
                    raise DataFeedError(f"Kalshi returned invalid candle for '{ticker}'")
                if start_bound <= date <= min(end_bound, fetched_at):
                    date = date.tz_localize(None)
                    rows.append({"date": date, "value": value, "available_at": date})
            if not rows:
                raise DataFeedError(
                    f"Kalshi returned no usable observations for '{ticker}' "
                    f"between {start.date()} and {end.date()}"
                )
            frame = pd.DataFrame(rows).sort_values("date").set_index("date")
            if frame.index.duplicated().any():
                raise DataFeedError(f"Kalshi returned duplicate candle timestamps for '{ticker}'")
            result[ticker.upper()] = frame[["value", "available_at"]]
        return result

    def _request(
        self, path: str, ticker: str, *, params: dict[str, Any] | None = None
    ) -> requests.Response:
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                response = requests.get(f"{_BASE_URL}/{path}", params=params, timeout=30)
            except requests.RequestException as exc:
                if attempt == _MAX_ATTEMPTS:
                    raise DataFeedError(
                        f"Kalshi request failed for '{ticker}' after {_MAX_ATTEMPTS} "
                        f"attempts ({type(exc).__name__})"
                    ) from None
                sleep(2 ** (attempt - 1))
                continue
            if response.status_code != 429 and not 500 <= response.status_code <= 599:
                return response
            if attempt == _MAX_ATTEMPTS:
                return response
            sleep(_retry_delay(response, attempt))
        raise AssertionError("Unreachable retry state")


def _date_bounds(start: datetime, end: datetime) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_bound = pd.Timestamp(start)
    end_bound = pd.Timestamp(end)
    start_bound = (
        start_bound.tz_localize("UTC")
        if start_bound.tzinfo is None else start_bound.tz_convert("UTC")
    )
    end_bound = (
        end_bound.tz_localize("UTC")
        if end_bound.tzinfo is None else end_bound.tz_convert("UTC")
    )
    start_bound = start_bound.normalize()
    end_bound = end_bound.normalize() + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    if pd.isna(start_bound) or pd.isna(end_bound) or start_bound > end_bound:
        raise DataFeedError("Kalshi requires a valid start date on or before end date")
    return start_bound, end_bound


def _store_response(payload: dict[str, Any], name: str, response: requests.Response) -> None:
    payload[name] = response.text
    payload[f"{name}_status_code"] = response.status_code


def _checked_json(payload: dict[str, Any], name: str, ticker: str) -> dict[str, Any]:
    raw_text = payload.get(name)
    status = payload.get(f"{name}_status_code")
    if not isinstance(raw_text, str) or not isinstance(status, int):
        raise DataFeedError(f"Kalshi returned no {name} response for '{ticker}'")
    if status >= 400:
        message = " ".join(raw_text.split())[:200]
        raise DataFeedError(f"Kalshi {name} fetch failed for '{ticker}' (HTTP {status}): {message}")
    try:
        parsed = json.loads(raw_text)
    except ValueError:
        raise DataFeedError(f"Kalshi returned non-JSON {name} response for '{ticker}'") from None
    if not isinstance(parsed, dict):
        raise DataFeedError(f"Kalshi returned invalid {name} response for '{ticker}'")
    return parsed


def _retry_delay(response: requests.Response, attempt: int) -> float:
    raw_value = response.headers.get("Retry-After")
    try:
        delay = float(raw_value)
    except (TypeError, ValueError):
        try:
            retry_at = pd.Timestamp(parsedate_to_datetime(raw_value))
            delay = (retry_at - pd.Timestamp.now(tz="UTC")).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return float(2 ** (attempt - 1))
    if not math.isfinite(delay):
        return float(2 ** (attempt - 1))
    return min(max(delay, 0.0), _MAX_RETRY_AFTER_SECONDS)
