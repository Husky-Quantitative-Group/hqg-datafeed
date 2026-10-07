from __future__ import annotations

from datetime import datetime
import json
from time import sleep
from typing import Any

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
import requests

from datafeed.altdata.base import AltDataProvider
from datafeed.errors import DataFeedError

_FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
_FRED_TAGS_URL = "https://api.stlouisfed.org/fred/series/tags"
_COLUMNS = ["value", "available_at"]
_DAILY_FREQUENCY = "daily"
_FRED_MAX_ATTEMPTS = 4
_FRED_BACKOFF_SECONDS = 1.0
_FRED_MAX_RETRY_AFTER_SECONDS = 60.0
_FRED_DAILY_OFFSET = pd.offsets.CustomBusinessDay(calendar=USFederalHolidayCalendar())
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
                tags_response = self._request_tags(normalized_series_id)
                raw_payload: dict[str, object] = {
                    "tags": tags_response.text,
                    "tags_status_code": tags_response.status_code,
                }

                if tags_response.status_code < 400:
                    tags_payload = self._json_from_text(
                        tags_response.text,
                        tags_response.status_code,
                        normalized_series_id,
                        "tags",
                    )
                    frequency = _frequency_from_tags(tags_payload)
                    observations_response = self._request_observations(
                        normalized_series_id,
                        start,
                        end,
                        include_realtime_params=frequency != _DAILY_FREQUENCY,
                    )
                    raw_payload.update(
                        {
                            "observations": observations_response.text,
                            "observations_status_code": observations_response.status_code,
                        }
                    )

                result[normalized_series_id] = raw_payload
            except DataFeedError:
                raise
            except Exception as exc:
                raise DataFeedError(
                    f"FRED fetch failed for series '{normalized_series_id}' ({type(exc).__name__})"
                ) from None

        return result

    def normalize(
        self,
        raw_data: dict[str, Any],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}

        for series_id, payload in raw_data.items():
            if not isinstance(payload, dict):
                raise DataFeedError(f"FRED raw payload for series '{series_id}' must be a dict")

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
        last_exception: requests.RequestException | None = None

        for attempt in range(1, _FRED_MAX_ATTEMPTS + 1):
            try:
                response = requests.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                last_exception = exc
                if attempt == _FRED_MAX_ATTEMPTS:
                    break
                _sleep_before_retry(attempt)
                continue

            if response.status_code not in _RETRY_STATUS_CODES:
                return response
            if attempt == _FRED_MAX_ATTEMPTS:
                return response

            _sleep_before_retry(attempt, response=response)

        assert last_exception is not None
        raise DataFeedError(
            f"FRED {payload_name} request failed for series '{series_id}' "
            f"after {_FRED_MAX_ATTEMPTS} attempts ({type(last_exception).__name__})"
        ) from None

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

        message = _http_error_message(parsed, status_code)
        if (
            payload_name == "tags"
            and status_code == 400
            and "series does not exist" in message.lower()
        ):
            raise DataFeedError(f"FRED series not found: {series_id}")

        raise DataFeedError(
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
        if not isinstance(raw_text, str):
            raise DataFeedError(f"FRED returned no {payload_name} response for series '{series_id}'")

        try:
            parsed = json.loads(raw_text)
        except ValueError:
            if status_code >= 400:
                return {"message": _plain_error_message(raw_text)}

            raise DataFeedError(
                f"FRED returned non-JSON {payload_name} response for series '{series_id}' "
                f"(HTTP {status_code})"
            ) from None

        if not isinstance(parsed, dict):
            raise DataFeedError(
                f"FRED returned invalid {payload_name} response for series '{series_id}'"
            )
        return parsed

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
            message = _error_message(payload) or "missing observations"
            raise DataFeedError(f"FRED fetch failed for series '{series_id}': {message}")
        if not isinstance(observations, list):
            raise DataFeedError(f"FRED returned invalid observations for series '{series_id}'")

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
                        pd.Timestamp(raw_date) + _FRED_DAILY_OFFSET
                        if is_daily and raw_date
                        else observation.get("realtime_start")
                    ),
                }
            )

        frame = pd.DataFrame(rows, columns=["date", "value", "available_at"])
        if frame.empty:
            raise DataFeedError(
                f"FRED returned no observations for series '{series_id}' "
                f"between {start.date()} and {end.date()}"
            )

        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["available_at"] = (
            pd.to_datetime(frame["available_at"], errors="coerce", format="mixed").dt.normalize()
            + pd.Timedelta(hours=23, minutes=59, seconds=59)
        )
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["date", "value", "available_at"])
        if frame.empty:
            raise DataFeedError(
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


def _sleep_before_retry(attempt: int, *, response: requests.Response | None = None) -> None:
    retry_after = _retry_after_seconds(response)
    if retry_after is not None:
        sleep(retry_after)
        return

    sleep(_FRED_BACKOFF_SECONDS * (2 ** (attempt - 1)))


def _retry_after_seconds(response: requests.Response | None) -> float | None:
    if response is None:
        return None

    raw_value = response.headers.get("Retry-After")
    if raw_value is None:
        return None

    try:
        retry_after = float(raw_value)
    except ValueError:
        return None

    return min(max(0.0, retry_after), _FRED_MAX_RETRY_AFTER_SECONDS)


def _http_error_message(payload: dict[str, Any], status_code: int) -> str:
    provider_message = _error_message(payload)
    status_message = _HANDLED_STATUS_MESSAGES.get(status_code)
    if status_message and provider_message:
        return f"{status_message}: {provider_message}"
    if status_message:
        return status_message
    return provider_message


def _plain_error_message(raw_text: str) -> str:
    stripped = " ".join(raw_text.split())
    if len(stripped) > 200:
        return f"{stripped[:200]}..."
    return stripped


def _error_message(payload: dict[str, Any]) -> str:
    return str(
        payload.get("error_message")
        or payload.get("message")
        or payload.get("error")
        or ""
    )
