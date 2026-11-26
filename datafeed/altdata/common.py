"""Shared alternative-data mechanics; providers retain their data and HTTP policies.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from email.utils import parsedate_to_datetime
import json
import logging
import math
from typing import Any

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
import requests

from datafeed.errors import (
    DataFeedError,
    DataFeedRequestError,
    InvalidProviderResponseError,
    redact_sensitive_text,
)

logger = logging.getLogger(__name__)

DEFAULT_DATE_FORMAT = "ISO8601"
_MIXED_OFFSETS_MESSAGE = "Dates must have a consistent timezone; mixed offsets are unsupported"
# Rough approximation of market holidays/weekends that would cause delays
_FEDERAL_BUSINESS_DAY = pd.offsets.CustomBusinessDay(calendar=USFederalHolidayCalendar())


def add_federal_business_day(date: str | datetime | pd.Timestamp) -> pd.Timestamp:
    """Advance a date by one business day, skipping weekends and US federal holidays."""
    return pd.Timestamp(date) + _FEDERAL_BUSINESS_DAY


def to_end_of_day(dates: pd.Series, *, date_format: str = DEFAULT_DATE_FORMAT) -> pd.Series:
    """Parse dates, normalize to the calendar day and return its last second.

    The result is next local midnight minus one second, so it stays correct on
    DST days. Entries that do not match ``date_format`` become
    NaT. Returns a new Series without modifying the input or converting
    timezones. Mixed timezone offsets are rejected because their local dates
    are ambiguous.
    """
    try:
        parsed = pd.to_datetime(dates, errors="coerce", format=date_format)
    except ValueError:
        raise DataFeedError(_MIXED_OFFSETS_MESSAGE) from None
    if not pd.api.types.is_datetime64_any_dtype(parsed.dtype):
        raise DataFeedError(_MIXED_OFFSETS_MESSAGE)
    return parsed.dt.normalize() + pd.DateOffset(days=1) - pd.Timedelta(seconds=1)


def request_with_retries(
    request: Callable[[], requests.Response],
    *,
    max_attempts: int,
    retry_status: Callable[[int], bool],
    retry_delay: Callable[[int, requests.Response | None], float],
    sleep: Callable[[float], None],
    context: str,
) -> requests.Response:
    """Retry transport failures and selected statuses, returning the final HTTP response.

    ``context`` describes the provider request and series without credentials.
    HTTP errors are interpreted later by the provider, including archival routing.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    context = redact_sensitive_text(context)
    for attempt in range(1, max_attempts + 1):
        try:
            response = request()
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise DataFeedRequestError(
                    f"{context} after {max_attempts} attempts ({type(exc).__name__})"
                ) from None
            delay = retry_delay(attempt, None)
            logger.debug(
                "%s; attempt %s/%s (%s), retrying in %ss",
                context, attempt, max_attempts, type(exc).__name__, delay,
            )
            sleep(delay)
            continue
        if not retry_status(response.status_code) or attempt == max_attempts:
            return response
        delay = retry_delay(attempt, response)
        logger.debug(
            "%s; attempt %s/%s (HTTP %s), retrying in %ss",
            context, attempt, max_attempts, response.status_code, delay,
        )
        sleep(delay)
    raise AssertionError("Unreachable retry state")


def exponential_backoff(attempt: int, base_seconds: float = 1.0) -> float:
    """Delay before retrying a failed attempt, numbered from one."""
    return base_seconds * (2 ** (attempt - 1))


def retry_after_seconds(
    response: requests.Response | None,
    *,
    max_seconds: float = 60.0,
    allow_http_date: bool = False,
    require_finite: bool = True,
) -> float | None:
    """Read a capped Retry-After delay, or return None for unsupported headers.

    Providers opt into HTTP dates. ``require_finite=False`` retains FRED's
    legacy numeric clamping, including its handling of NaN and infinity.
    """
    if response is None:
        return None
    raw_value = response.headers.get("Retry-After")
    if raw_value is None:
        return None
    try:
        delay = float(raw_value)
    except (TypeError, ValueError):
        if not allow_http_date:
            return None
        try:
            retry_at = pd.Timestamp(parsedate_to_datetime(raw_value))
            delay = (retry_at - pd.Timestamp.now(tz="UTC")).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    if require_finite and not math.isfinite(delay):
        return None
    return min(max(0.0, delay), max_seconds)


def retry_delay(
    attempt: int,
    response: requests.Response | None = None,
    *,
    base_seconds: float = 1.0,
    max_seconds: float = 60.0,
    allow_http_date: bool = False,
    require_finite: bool = True,
) -> float:
    """Prefer a supported Retry-After header, otherwise use exponential backoff."""
    delay = retry_after_seconds(
        response,
        max_seconds=max_seconds,
        allow_http_date=allow_http_date,
        require_finite=require_finite,
    )
    return delay if delay is not None else exponential_backoff(attempt, base_seconds)


def store_response(payload: dict[str, Any], name: str, response: requests.Response) -> None:
    """Preserve response text and status without parsing or changing the raw body."""
    payload[name] = response.text
    payload[f"{name}_status_code"] = response.status_code


def error_message(payload: Mapping[str, Any]) -> str:
    """Read the first nonempty message from common JSON error fields."""
    return redact_sensitive_text(
        str(payload.get("error_message") or payload.get("message") or payload.get("error") or "")
    )


def http_error_message(
    payload: Mapping[str, Any],
    status_code: int,
    status_messages: Mapping[int, str] | None = None,
) -> str:
    """Combine an optional provider status description with its parsed error message."""
    provider_message = error_message(payload)
    status_message = (status_messages or {}).get(status_code)
    if status_message and provider_message:
        return f"{status_message}: {provider_message}"
    return status_message or provider_message


def plain_error_message(raw_text: str, *, limit: int = 200, suffix: str = "") -> str:
    """Collapse whitespace and bound response text included in an error."""
    stripped = " ".join(redact_sensitive_text(raw_text).split())
    return stripped[:limit] + suffix if len(stripped) > limit else stripped


def parse_json_object(
    raw_text: object,
    *,
    provider: str,
    context: str,
    non_json_suffix: str = "",
    non_json_fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Parse a JSON object with contextual errors and an optional non-JSON fallback.

    Providers check HTTP status themselves, deciding whether to parse an error
    body or report its raw text. A fallback applies only to invalid JSON syntax.
    """
    if not isinstance(raw_text, str):
        raise InvalidProviderResponseError(f"{provider} returned no {context}")
    try:
        parsed = json.loads(raw_text)
    except ValueError:
        if non_json_fallback is not None:
            return non_json_fallback
        raise InvalidProviderResponseError(f"{provider} returned non-JSON {context}{non_json_suffix}") from None
    if not isinstance(parsed, dict):
        raise InvalidProviderResponseError(f"{provider} returned invalid {context}")
    return parsed


def date_bounds(
    start: datetime,
    end: datetime,
    *,
    utc: bool = False,
    naive: bool = False,
    timezone: Any | None = None,
    invalid_range_message: str | None = None,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return inclusive whole-date bounds.

    Only the calendar date of each bound matters. Aware inputs are first
    converted to the target zone (``timezone``, else UTC when ``utc`` is set,
    else their own zone); naive inputs keep their wall-clock date and are
    interpreted in the target zone. Time-of-day is discarded *before* a zone is
    attached, so DST gaps and overlaps in the input cannot raise. Local
    midnights that do not exist are shifted forward. The end bound is the last
    nanosecond before the next local midnight, which is correct on DST days.

    ``naive`` returns timezone-free bounds. Set ``invalid_range_message`` to
    reject missing or reversed bounds with a contextual DataFeedError. By
    default, no range validation is performed.
    """
    target = timezone if timezone is not None else ("UTC" if utc else None)

    walls: list[pd.Timestamp] = []
    zones: list[Any] = []
    for value in (start, end):
        ts = pd.Timestamp(value)
        zone = ts.tzinfo
        if target is not None:
            if ts.tzinfo is not None:
                ts = ts.tz_convert(target)
            zone = target
        walls.append(ts.tz_localize(None).normalize())
        zones.append(zone)

    def localize(wall: pd.Timestamp, zone: Any) -> pd.Timestamp:
        if naive or zone is None or pd.isna(wall):
            return wall
        return wall.tz_localize(zone, nonexistent="shift_forward", ambiguous=True)

    start_bound = localize(walls[0], zones[0])
    end_bound = localize(walls[1] + pd.Timedelta(days=1), zones[1]) - pd.Timedelta(nanoseconds=1)

    if invalid_range_message is not None:
        if pd.isna(start_bound) or pd.isna(end_bound) or start_bound > end_bound:
            raise DataFeedError(invalid_range_message)
    return start_bound, end_bound


def find_provider_payload(
    provider_result: Mapping[str, Any],
    series_id: str,
) -> Any | None:
    """Find a series by exact key, falling back to case-insensitive matching."""
    if series_id in provider_result:
        return provider_result[series_id]

    lookup = {str(key).lower(): key for key in provider_result}
    matched_key = lookup.get(series_id.lower())
    if matched_key is None:
        return None
    return provider_result[matched_key]


def prepare_alt_frame(
    frame: pd.DataFrame,
    start: datetime,
    end: datetime,
    *,
    logger: logging.Logger | None = None,
    series_id: str | None = None,
    date_format: str = DEFAULT_DATE_FORMAT,
    max_invalid_date_fraction: float = 0.25,
) -> pd.DataFrame:
    """Copy, clean and slice a frame without altering provider availability values.

    Rows whose dates are missing or do not match ``date_format`` are dropped
    with a warning, but only up to ``max_invalid_date_fraction`` of the frame.
    Beyond that the response is rejected, since a high failure rate usually
    means the provider changed its format. By default, up to 25% of rows may
    have invalid dates; a larger fraction is rejected.
    """
    if not isinstance(frame, pd.DataFrame):
        raise InvalidProviderResponseError("Alt data providers must return pandas DataFrames")
    if not 0.0 <= max_invalid_date_fraction <= 1.0:
        raise ValueError("max_invalid_date_fraction must be between 0 and 1")

    logger = logger if logger is not None else logging.getLogger(__name__)
    prepared = frame.copy()
    context = f" for series {series_id}" if series_id else ""
    original_len = len(prepared)
    prepared.index = _parse_alt_index(prepared.index, errors="coerce", date_format=date_format)
    prepared.index.name = "date"
    prepared = prepared.loc[prepared.index.notna()]
    dropped_bad_dates = original_len - len(prepared)
    if dropped_bad_dates:
        if dropped_bad_dates / original_len > max_invalid_date_fraction:
            raise InvalidProviderResponseError(
                f"{dropped_bad_dates} of {original_len} alt-data row(s) have missing or "
                f"invalid dates{context} (allowed fraction: {max_invalid_date_fraction})"
            )
        logger.warning("Dropped %s alt-data row(s) with missing or invalid dates%s", dropped_bad_dates, context)

    if "value" in prepared.columns:
        before_value_drop = len(prepared)
        prepared = prepared.dropna(subset=["value"])
        dropped_missing_values = before_value_drop - len(prepared)
        if dropped_missing_values:
            logger.warning("Dropped %s alt-data row(s) with missing values%s", dropped_missing_values, context)
    return slice_alt_frame(prepared, start, end, date_format=date_format)


def validate_alt_frame(
    frame: pd.DataFrame,
    series_id: str,
    provider_prefix: str,
    *,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> None:
    """Require the alt-data schema and unique observation/availability pairs."""
    required_columns = ["value", "available_at"]
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise InvalidProviderResponseError(
            f"Provider '{provider_prefix}' returned invalid data for series {series_id}: "
            f"missing required column(s): {', '.join(missing_columns)}"
        )

    pairs = pd.DataFrame(
        {
            "date": _parse_alt_index(frame.index, date_format=date_format),
            "available_at": frame["available_at"].to_numpy(),
        }
    )
    if pairs.duplicated().any():
        raise InvalidProviderResponseError(
            f"Provider '{provider_prefix}' returned duplicate (date, available_at) "
            f"rows for series {series_id}"
        )


def _parse_alt_index(
    index: pd.Index,
    *,
    errors: str = "raise",
    date_format: str = DEFAULT_DATE_FORMAT,
) -> pd.DatetimeIndex:
    """Parse an index with an explicit format; reject unparseable or mixed-offset input."""
    try:
        parsed = pd.to_datetime(index, errors=errors, format=date_format)
    except ValueError:
        offsets = set()
        for value in index:
            try:
                item = pd.to_datetime(value, errors="coerce", format=date_format)
                if not pd.isna(item):
                    offsets.add(item.utcoffset() if item.tzinfo is not None else None)
            except (TypeError, ValueError, AttributeError):
                continue
        if len(offsets) > 1:
            raise InvalidProviderResponseError(_MIXED_OFFSETS_MESSAGE) from None
        raise InvalidProviderResponseError(
            f"Alt data index could not be parsed with date format {date_format!r}"
        ) from None
    if not isinstance(parsed, pd.DatetimeIndex):
        raise InvalidProviderResponseError(_MIXED_OFFSETS_MESSAGE)
    return parsed


def slice_alt_frame(
    frame: pd.DataFrame,
    start: datetime,
    end: datetime,
    *,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    """Slice whole observation dates in the index timezone without resampling.

    Naive bounds use the index timezone; mixed-offset indexes are rejected.
    Rows with a missing index value are excluded.
    """
    index = _parse_alt_index(frame.index, date_format=date_format)
    start_bound, end_bound = date_bounds(
        start, end, utc=index.tz is None, naive=index.tz is None, timezone=index.tz
    )
    mask = (index >= start_bound) & (index <= end_bound)
    sliced = frame.loc[mask].copy()
    sliced.index = index[mask]
    sliced.index.name = "date"
    return sliced
