from __future__ import annotations

from datetime import datetime
from functools import partial
from time import sleep
from typing import Any

import pandas as pd
import requests

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.common import (
    add_federal_business_day,
    error_message,
    http_error_message,
    parse_json_object,
    plain_error_message,
    request_with_retries,
    retry_delay,
    store_response,
    to_end_of_day,
)
from datafeed.errors import (
    DataFeedError,
    DataFeedRequestError,
    DataUnavailableError,
    InvalidProviderResponseError,
    redact_errors,
)

_FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TAGS_URL = "https://api.stlouisfed.org/fred/series/tags"
_COLUMNS = ["value", "available_at"]
_DAILY_FREQUENCY = "daily"
_FRED_MAX_ATTEMPTS = 4
_FRED_BACKOFF_SECONDS = 1.0
_FRED_MAX_RETRY_AFTER_SECONDS = 60.0
_RETRY_STATUS_CODES = {423, 429, 500}
_HANDLED_STATUS_MESSAGES = {
    400: "Bad request",
    404: "Not found",
    423: "Locked",
    429: "Too many requests; FRED allows up to 120 requests per minute",
    500: "Internal server error",
}


class FredProvider(AltDataProvider):
    """FRED provider that returns each series at its native publication shape."""

    def __init__(self, api_key: str | None):
        self.api_key = api_key

    @redact_errors
    def fetch_raw(
        self,
        series: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, dict[str, object]]:
        if not self.api_key:
            raise DataFeedError("FRED API key is required for fred.* series")

        result: dict[str, dict[str, object]] = {}

        for series_id in series:
            normalized_series_id = series_id.upper()
            try:
                result[normalized_series_id] = self._fetch_raw_series(
                    normalized_series_id, start, end
                )
            except DataFeedError:
                raise
            except Exception as exc:
                raise DataFeedError(
                    f"FRED fetch failed for series '{normalized_series_id}' ({type(exc).__name__})"
                ) from None

        return result

    def _fetch_raw_series(
        self, series_id: str, start: datetime, end: datetime
    ) -> dict[str, object]:
        """Fetch tags and frequency-dependent observations, preserving raw responses."""
        tags_response = self._request_tags(series_id)
        raw_payload: dict[str, Any] = {}
        store_response(raw_payload, "tags", tags_response)

        if tags_response.status_code < 400:
            tags_payload = self._json_from_text(
                tags_response.text,
                tags_response.status_code,
                series_id,
                "tags",
            )
            frequency = _frequency_from_tags(tags_payload)
            observations_response = self._request_observations(
                series_id,
                start,
                end,
                include_realtime_params=frequency != _DAILY_FREQUENCY,
            )
            store_response(raw_payload, "observations", observations_response)

        return raw_payload

    @redact_errors
    def normalize(
        self,
        raw_data: dict[str, Any],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}

        for series_id, payload in raw_data.items():
            if not isinstance(payload, dict):
                raise InvalidProviderResponseError(f"FRED raw payload for series '{series_id}' must be a dict")

            tags_payload = self._checked_json_payload(payload, series_id, "tags")
            frequency = _frequency_from_tags(tags_payload)
            observations_payload = self._checked_json_payload(payload, series_id, "observations")
            result[series_id.upper()] = self._normalize_observations(
                series_id.upper(),
                observations_payload,
                frequency=frequency,
                start=start,
                end=end,
            )

        return result

    def _request_tags(self, series_id: str) -> requests.Response:
        return self._request_with_retries(
            _FRED_TAGS_URL,
            params={
                "api_key": self.api_key,
                "file_type": "json",
                "series_id": series_id.upper(),
            },
            series_id=series_id,
            payload_name="tags",
        )

    def _request_observations(
        self,
        series_id: str,
        start: datetime,
        end: datetime,
        *,
        include_realtime_params: bool,
    ) -> requests.Response:
        params = {
            "api_key": self.api_key,
            "file_type": "json",
            "series_id": series_id.upper(),
            "observation_start": start.date().isoformat(),
            "observation_end": end.date().isoformat(),
        }
        if include_realtime_params:
            params.update(
                {
                    "realtime_start": "1776-07-04",
                    "realtime_end": "9999-12-31",
                }
            )

        return self._request_with_retries(
            _FRED_OBSERVATIONS_URL,
            params=params,
            series_id=series_id,
            payload_name="observations",
        )

    def _request_with_retries(
        self,
        url: str,
        *,
        params: dict[str, object],
        series_id: str,
        payload_name: str,
    ) -> requests.Response:
        return request_with_retries(
            lambda: requests.get(url, params=params, timeout=30),
            max_attempts=_FRED_MAX_ATTEMPTS,
            retry_status=lambda status: status in _RETRY_STATUS_CODES,
            retry_delay=partial(
                retry_delay,
                base_seconds=_FRED_BACKOFF_SECONDS,
                max_seconds=_FRED_MAX_RETRY_AFTER_SECONDS,
                require_finite=False,
            ),
            sleep=sleep,
            context=f"FRED {payload_name} request failed for series '{series_id}'",
        )

    @redact_errors
    def _checked_json_payload(
        self,
        payload: dict[str, Any],
        series_id: str,
        payload_name: str,
    ) -> dict[str, Any]:
        raw_text = payload.get(payload_name)
        status_code = int(payload.get(f"{payload_name}_status_code", 0))

        parsed = self._json_from_text(raw_text, status_code, series_id, payload_name)
        if status_code < 400:
            return parsed

        message = http_error_message(parsed, status_code, _HANDLED_STATUS_MESSAGES)
        if (
            payload_name == "tags"
            and status_code == 400
            and "series does not exist" in message.lower()
        ):
            raise DataUnavailableError(f"FRED series not found: {series_id}")

        error_class = DataUnavailableError if status_code == 404 else DataFeedRequestError
        raise error_class(
            f"FRED {payload_name} fetch failed for series '{series_id}' "
            f"(HTTP {status_code}): {message}"
        )

    def _json_from_text(
        self,
        raw_text: object,
        status_code: int,
        series_id: str,
        payload_name: str,
    ) -> dict[str, Any]:
        fallback = None
        if status_code >= 400 and isinstance(raw_text, str):
            fallback = {"message": plain_error_message(raw_text, suffix="...")}
        return parse_json_object(
            raw_text,
            provider="FRED",
            context=f"{payload_name} response for series '{series_id}'",
            non_json_suffix=f" (HTTP {status_code})",
            non_json_fallback=fallback,
        )

    def _normalize_observations(
        self,
        series_id: str,
        payload: dict[str, Any],
        *,
        frequency: str | None,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        observations = payload.get("observations")
        if observations is None:
            message = error_message(payload) or "missing observations"
            raise InvalidProviderResponseError(f"FRED fetch failed for series '{series_id}': {message}")
        if not isinstance(observations, list):
            raise InvalidProviderResponseError(f"FRED returned invalid observations for series '{series_id}'")

        rows = []
        is_daily = frequency == _DAILY_FREQUENCY
        for observation in observations:
            if not isinstance(observation, dict):
                continue

            raw_date = observation.get("date")
            raw_value = observation.get("value")
            rows.append(
                {
                    "date": raw_date,
                    "value": None if raw_value == "." else raw_value,
                    "available_at": (
                        add_federal_business_day(raw_date)
                        if is_daily and raw_date
                        else observation.get("realtime_start")
                    ),
                }
            )

        frame = pd.DataFrame(rows, columns=["date", "value", "available_at"])
        if frame.empty:
            raise DataUnavailableError(
                f"FRED returned no observations for series '{series_id}' "
                f"between {start.date()} and {end.date()}"
            )

        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["available_at"] = to_end_of_day(frame["available_at"])
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["date", "value", "available_at"])
        if frame.empty:
            raise DataUnavailableError(
                f"FRED returned no usable observations for series '{series_id}' "
                f"between {start.date()} and {end.date()}"
            )

        return frame.sort_values(["date", "available_at"]).set_index("date")[_COLUMNS]


def _frequency_from_tags(payload: dict[str, Any]) -> str | None:
    tags = payload.get("tags")
    if not isinstance(tags, list):
        return None

    for tag in tags:
        if not isinstance(tag, dict):
            continue
        if str(tag.get("group_id", "")).lower() == "freq":
            return str(tag.get("name", "")).strip().lower() or None
    return None
