"""Copy and adapt this example when adding a provider; it is not registered.

Assumed response: column-oriented JSON, with equal-length arrays under "data":
    {
        "data": {
            "observation_date": ["2024-01-01", "2024-02-01"],
            "reading": ["1.5", "1.7"],
            "published_at": ["2024-01-15T08:30:00Z", "2024-02-15T08:30:00Z"]
        }
    }

Availability defaults to 23:59:59 on the publication's UTC date. Use the more accurate 
datetime when available. If only date is provided, default to EOD to avoid lookahead bias. 

To add a built-in provider:
1. Copy this implementation into altdata/providers/ and replace the example URL,
   authentication, parameters and response fields with the actual API contract.
2. Import the class in altdata/registry.py and add its factory to
   _PROVIDER_FACTORIES, for example:
       "VENDOR": lambda config: NewProvider(config.vendor_api_key)
3. Add credentials to _ENV_KEYS in datafeed/config.py, for example:
       "vendor_api_key": "VENDOR_API_KEY"
   Config then supports Config(vendor_api_key="...") and automatic environment
   loading and error redaction. No dataclass field declaration is needed.
Export the class from providers/__init__.py and altdata/__init__.py if desired.
Callers can then request "VENDOR.MY_SERIES" through DataFeed.

Before registering, test raw payload preservation, malformed/empty responses,
HTTP/transport errors, retry limits, date boundaries and revision history.
"""

from __future__ import annotations

from datetime import datetime
from functools import partial
from time import sleep
from typing import Any

import pandas as pd
import requests

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.common import (
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

_PROVIDER = "VENDOR"
_OBSERVATIONS_URL = "https://api.example/observations"
_COLUMN_MAPPING = {
    "observation_date": "date",
    "reading": "value",
    "published_at": "available_at",
}
_STATUS_MESSAGES = {
    401: "Invalid or missing API key",
    403: "Series access denied",
    404: "Series not found",
    429: "Rate limit exceeded",
}


class NewProvider(AltDataProvider):
    """Example provider with API-key auth and column-oriented observations."""

    def __init__(self, api_key: str | None):
        # Let Config resolve credentials. Do not embed secrets in source or logs.
        # Validate at fetch time so an unused provider can lack credentials.
        self.api_key = api_key

    # DataFeed scopes redaction using Config. This decorator also protects
    # direct provider calls by discovering credential attributes on self.
    @redact_errors
    def fetch_raw(
        self, series: list[str], start: datetime, end: datetime
    ) -> dict[str, Any]:
        if not self.api_key:
            raise DataFeedError(f"{_PROVIDER} API key is required")

        result = {}
        for series_id in series:
            # DataFeed supplies uppercase IDs. Check whether the vendor needs a
            # case-sensitive identifier or an explicit mapping to its native ID.
            series_id = series_id.upper() # Adapt to vendor contract
            result[series_id] = self._fetch_raw_series(series_id, start, end)
        return result

    def _fetch_raw_series(
        self, series_id: str, start: datetime, end: datetime
    ) -> dict[str, Any]:
        # Confirm parameter names, date timezone, and inclusive/exclusive bounds.
        # This example assumes inclusive calendar dates and a single response.
        # Implement pagination/batching if the real API limits returned history.
        params = {
            "series_id": series_id,
            "start": start.date().isoformat(),
            "end": end.date().isoformat(),
        }
        response = request_with_retries(
            lambda: requests.get(
                _OBSERVATIONS_URL,
                params=params,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=30,
            ),
            max_attempts=4,
            # Choose retryable statuses from the vendor's documented policy.
            # Avoid retrying permanent errors such as 401/403/404 by default.
            retry_status=lambda status: status == 429 or 500 <= status <= 599,
            retry_delay=partial(retry_delay, max_seconds=60.0, allow_http_date=True),
            sleep=sleep,
            context=f"{_PROVIDER} observations request failed for series '{series_id}'",
        )
        # Keep raw text and status for inspection; do not reshape or clean here.
        # Review whether provider responses themselves contain sensitive fields.
        payload: dict[str, Any] = {}
        store_response(payload, "observations", response)
        return payload

    @redact_errors
    def normalize(
        self, raw_data: dict[str, Any], start: datetime, end: datetime
    ) -> dict[str, pd.DataFrame]:
        # Inherited fetch() calls fetch_raw() then normalize(). Normalization
        # should also work on saved payloads without credentials or network I/O.
        result = {}
        for series_id, payload in raw_data.items():
            series_id = series_id.upper()
            parsed = self._checked_json(payload, series_id)
            result[series_id] = self._normalize_series(parsed, series_id, start, end)
        return result

    @redact_errors
    def _checked_json(self, payload: Any, series_id: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise InvalidProviderResponseError(f"{_PROVIDER} raw payload for '{series_id}' must be a dict")
        text = payload.get("observations")
        status = payload.get("observations_status_code")
        if not isinstance(text, str) or not isinstance(status, int):
            raise InvalidProviderResponseError(f"{_PROVIDER} returned no observations response for '{series_id}'")

        # This example extracts JSON error messages, falling back to bounded
        # plain text for HTML/non-JSON errors. Adapt to the vendor's error shape;
        # some APIs return error objects even with HTTP 200.
        parsed = parse_json_object(
            text,
            provider=_PROVIDER,
            context=f"observations response for series '{series_id}'",
            non_json_fallback={"message": plain_error_message(text)} if status >= 400 else None,
        )
        if status >= 400:
            message = http_error_message(parsed, status, _STATUS_MESSAGES)
            error_class = DataUnavailableError if status == 404 else DataFeedRequestError
            raise error_class(
                f"{_PROVIDER} observations fetch failed for series '{series_id}' "
                f"(HTTP {status}): {message}"
            )
        return parsed

    def _normalize_series(
        self, parsed: dict[str, Any], series_id: str, start: datetime, end: datetime
    ) -> pd.DataFrame:
        columns = parsed.get("data")
        # Validate shape before DataFrame construction: arrays must have equal
        # lengths. For record-oriented JSON, validate a list of row dictionaries
        # instead, then apply the same explicit column mapping below.
        if (
            not isinstance(columns, dict)
            or any(not isinstance(columns.get(name), list) for name in _COLUMN_MAPPING)
            or len({len(columns[name]) for name in _COLUMN_MAPPING}) != 1
        ):
            raise InvalidProviderResponseError(f"{_PROVIDER} returned invalid data columns for '{series_id}'")
        frame = pd.DataFrame({name: columns[name] for name in _COLUMN_MAPPING})
        frame = frame.rename(columns=_COLUMN_MAPPING)

        # This example assumes naive timestamps mean UTC; convert aware inputs
        # to UTC too. Use the vendor's actual timezone and epoch units if needed.
        for column in ("date", "available_at"):
            frame[column] = pd.to_datetime(
                frame[column], errors="coerce", format="mixed", utc=True
            ).dt.tz_localize(None)
        # Should preserve exact publication timestamps when provided
        # Here we use to_end_of_day() b/c we assume the source only provides
        # date-only publication information or the data contract
        # explicitly requires conservative EOD availability.
        # Ensure lookahead bias is not present
        frame["available_at"] = to_end_of_day(frame["available_at"])
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")

        # Define missing-value markers and numeric bounds per provider. This
        # example rejects invalid availability and infinite values; shared
        # preparation in DataFeed drops missing values and up to 25% invalid
        # observation dates with warning counts; a higher fraction is rejected.
        if (
            frame["available_at"].isna().any()
            or frame["value"].isin([float("inf"), -float("inf")]).any()
        ):
            raise InvalidProviderResponseError(f"{_PROVIDER} returned invalid records for '{series_id}'")

        # available_at must describe when the value became public, not its
        # observation date. Only add a federal business day if justified by the
        # provider's publication policy. EOD conversion can collapse intraday
        # releases into duplicate pairs; define a policy if the API has these.
        # Preserve vintages: duplicate dates are allowed with distinct releases
        frame = frame.sort_values(["date", "available_at"]).set_index("date")
        if frame.empty:
            raise DataUnavailableError(f"{_PROVIDER} returned no usable observations for '{series_id}'")
        # DataFeed handles cleaning, date-range slicing, required-column checks,
        # and duplicate (date, available_at) validation at its public boundary. Avoid
        # alignment, forward-filling, resampling, or discarding revision history.
        return frame[["value", "available_at"]]
