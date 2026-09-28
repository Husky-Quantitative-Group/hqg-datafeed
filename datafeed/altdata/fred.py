from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

from datafeed.altdata.base import AltDataProvider
from datafeed.errors import DataFeedError

_FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
_COLUMNS = ["value", "available_at"]


class FredProvider(AltDataProvider):
    """FRED provider that returns every revision of each observation.

    Output per series: DatetimeIndex named "date" (the period the value describes, NOT unique: one row per revision) and columns:
      - value:        the figure as published in that revision
      - available_at: first date that revision was the official figure (FRED's realtime_start)

    To get what was known on a given day, keep rows with
    available_at <= that day and take the latest per date.
    """

    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def fetch(
        self,
        series: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        if not self.api_key:
            raise DataFeedError("FRED API key is required for fred.* series")

        result: dict[str, pd.DataFrame] = {}

        for series_id in series:
            normalized_series_id = series_id.upper()
            try:
                result[normalized_series_id] = self._fetch_series(normalized_series_id, start, end)
            except DataFeedError:
                raise
            except Exception as exc:
                raise DataFeedError(
                    f"FRED fetch failed for series '{normalized_series_id}' ({type(exc).__name__})"
                ) from None

        return result

    def _fetch_series(
        self,
        series_id: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        try:
            response = requests.get(
                _FRED_OBSERVATIONS_URL,
                params={
                    "api_key": self.api_key,
                    "file_type": "json",
                    "series_id": series_id.upper(),
                    "observation_start": start.date().isoformat(),
                    "observation_end": end.date().isoformat(),
                    "realtime_start": "1776-07-04",
                    "realtime_end": "9999-12-31",
                },
                timeout=30,
            )
        except requests.RequestException as exc:
            raise DataFeedError(
                f"FRED request failed for series '{series_id}' ({type(exc).__name__})"
            ) from None

        try:
            payload = response.json()
        except ValueError:
            raise DataFeedError(
                f"FRED returned non-JSON response for series '{series_id}' "
                f"(HTTP {response.status_code})"
            ) from None

        if response.status_code >= 400:
            message = payload.get("error_message") or payload.get("message") or ""
            raise DataFeedError(
                f"FRED fetch failed for series '{series_id}' "
                f"(HTTP {response.status_code}): {message}"
            )

        observations = payload.get("observations")
        if observations is None:
            message = payload.get("error_message") or "missing observations"
            raise DataFeedError(f"FRED fetch failed for series '{series_id}': {message}")

        rows = []
        for observation in observations:
            raw_value = observation.get("value")
            rows.append(
                {
                    "date": observation.get("date"),
                    "value": None if raw_value == "." else raw_value,
                    "available_at": observation.get("realtime_start"),
                }
            )

        frame = pd.DataFrame(rows, columns=["date", "value", "available_at"])
        if frame.empty:
            raise DataFeedError(
                f"FRED returned no observations for series '{series_id}' "
                f"between {start.date()} and {end.date()}"
            )

        frame["date"] = pd.to_datetime(frame["date"])
        frame["available_at"] = pd.to_datetime(frame["available_at"])
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")

        frame = frame.dropna(subset=["value"])
        if frame.empty:
            raise DataFeedError(
                f"FRED returned no usable observations for series '{series_id}' "
                f"between {start.date()} and {end.date()}"
            )

        return frame.sort_values(["date", "available_at"]).set_index("date")[_COLUMNS]
