from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import logging
from typing import Any, Mapping

import pandas as pd

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.ids import AltDataId, normalize_alt_data_ids
from datafeed.config import Config
from datafeed.errors import DataFeedError
from datafeed.models import MarketData
from datafeed.securities import get_securities_frame

logger = logging.getLogger(__name__)


class DataFeed:
    """Stateless datafeed client."""

    def __init__(self, config: Config):
        self.config = config
        self.providers: Mapping[str, AltDataProvider] = config.providers

    def get_securities(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        
        """Return daily securities OHLCV over exactly [start, end]."""
        return get_securities_frame(symbols, start, end)

    def get_alt_data(
        self,
        ids: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        """
        Return native-frequency alternative data keyed by full series id.

        Each frame is indexed by observation date over exactly [start, end].
        The index may contain duplicate dates, one row per vintage. Series are
        never aligned to each other. Providers own available_at semantics.
        """
        requested = self._normalize_alt_data_ids(ids)
        if not requested:
            return {}

        grouped = _group_normalized_alt_data_ids(requested)
        series_frames: dict[str, pd.DataFrame] = {}

        # Obtain output by provider and associate with requested id
        for provider_prefix, requested_ids in grouped.items():
            provider = self.providers[provider_prefix]
            series = [item.series for item in requested_ids]
            full_ids = [item.output_id for item in requested_ids]

            try:
                provider_result = provider.fetch(series, start, end)
            except Exception as exc:
                raise DataFeedError(
                    f"Provider '{provider_prefix}' failed for series "
                    f"{', '.join(full_ids)}: {exc}"
                ) from exc

            for item in requested_ids:
                frame = _find_provider_frame(provider_result, item.series)
                if frame is None:
                    raise DataFeedError(
                        f"Provider '{provider_prefix}' returned no data for series {item.output_id}"
                    )

                clean_frame = _prepare_alt_frame(frame, start, end)
                if clean_frame.empty:
                    raise DataFeedError(
                        f"Provider '{provider_prefix}' returned empty data for series {item.output_id}"
                    )
                _validate_alt_frame(clean_frame, item.output_id, provider_prefix)

                series_frames[item.output_id] = clean_frame

        return {item.output_id: series_frames[item.output_id] for item in requested}

    def raw_alt_data_fetch(
        self,
        ids: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        """Return raw provider payloads keyed by normalized full series id."""
        grouped = self._group_alt_data_ids(ids)
        if not grouped:
            return {}

        result: dict[str, Any] = {}
        for provider_prefix, requested_ids in grouped.items():
            provider = self.providers[provider_prefix]
            series = [item.series for item in requested_ids]
            full_ids = [item.output_id for item in requested_ids]

            try:
                provider_result = provider.fetch_raw(series, start, end)
            except Exception as exc:
                raise DataFeedError(
                    f"Provider '{provider_prefix}' failed for raw series "
                    f"{', '.join(full_ids)}: {exc}"
                ) from exc

            for item in requested_ids:
                payload = _find_provider_payload(provider_result, item.series)
                if payload is None:
                    raise DataFeedError(
                        f"Provider '{provider_prefix}' returned no raw data for series {item.output_id}"
                    )
                result[item.output_id] = payload

        return result

    def get_data(
        self,
        securities: list[str],
        alt_data: list[str],
        start: datetime,
        end: datetime,
    ) -> MarketData:
        """
        Return securities and alt data for the same requested range.
        """
        return MarketData(
            securities=self.get_securities(securities, start, end),
            alt_data=self.get_alt_data(alt_data, start, end),
        )

    def _validate_providers(self, ids: list[AltDataId]) -> None:
        unknown = sorted({item.provider for item in ids if item.provider not in self.providers})
        if not unknown:
            return

        raise DataFeedError(
            f"Unknown alt data provider(s): {', '.join(unknown)}. "
            f"Registered providers: {self._registered_providers()}"
        )

    def _registered_providers(self) -> str:
        return ", ".join(sorted(self.providers)) or "none"

    def _group_alt_data_ids(self, ids: list[str]) -> dict[str, list[AltDataId]]:
        return _group_normalized_alt_data_ids(self._normalize_alt_data_ids(ids))

    def _normalize_alt_data_ids(self, ids: list[str]) -> list[AltDataId]:
        try:
            normalized = normalize_alt_data_ids(ids)
        except DataFeedError as exc:
            raise DataFeedError(f"{exc}. Registered providers: {self._registered_providers()}") from exc

        self._validate_providers(normalized)
        return normalized


def _group_normalized_alt_data_ids(ids: list[AltDataId]) -> dict[str, list[AltDataId]]:
    grouped: dict[str, list[AltDataId]] = defaultdict(list)
    for parsed in ids:
        grouped[parsed.provider].append(parsed)
    return grouped


def _find_provider_frame(
    provider_result: dict[str, pd.DataFrame],
    series_id: str,
) -> pd.DataFrame | None:
    return _find_provider_payload(provider_result, series_id)


def _find_provider_payload(
    provider_result: Mapping[str, Any],
    series_id: str,
) -> Any | None:
    if series_id in provider_result:
        return provider_result[series_id]

    lookup = {str(key).lower(): key for key in provider_result}
    matched_key = lookup.get(series_id.lower())
    if matched_key is None:
        return None
    return provider_result[matched_key]


def _prepare_alt_frame(
    frame: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise DataFeedError("Alt data providers must return pandas DataFrames")

    prepared = frame.copy()
    original_len = len(prepared)
    prepared.index = pd.to_datetime(prepared.index, errors="coerce")
    prepared.index.name = "date"
    prepared = prepared.loc[prepared.index.notna()]
    dropped_bad_dates = original_len - len(prepared)
    if dropped_bad_dates:
        logger.warning("Dropped %s alt-data row(s) with missing or invalid dates", dropped_bad_dates)

    if "value" in prepared.columns:
        before_value_drop = len(prepared)
        prepared = prepared.dropna(subset=["value"])
        dropped_missing_values = before_value_drop - len(prepared)
        if dropped_missing_values:
            logger.warning("Dropped %s alt-data row(s) with missing values", dropped_missing_values)
    return _slice_alt_frame(prepared, start, end)


def _validate_alt_frame(
    frame: pd.DataFrame,
    series_id: str,
    provider_prefix: str,
) -> None:
    required_columns = ["value", "available_at"]
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise DataFeedError(
            f"Provider '{provider_prefix}' returned invalid data for series {series_id}: "
            f"missing required column(s): {', '.join(missing_columns)}"
        )

    pairs = pd.DataFrame(
        {
            "date": pd.to_datetime(frame.index),
            "available_at": frame["available_at"].to_numpy(),
        }
    )
    if pairs.duplicated().any():
        raise DataFeedError(
            f"Provider '{provider_prefix}' returned duplicate (date, available_at) "
            f"rows for series {series_id}"
        )


def _slice_alt_frame(frame: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    index = pd.to_datetime(frame.index)
    start_bound = pd.Timestamp(start).normalize()
    end_bound = pd.Timestamp(end).normalize() + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    sliced = frame.loc[(index >= start_bound) & (index <= end_bound)]
    sliced.index = pd.to_datetime(sliced.index)
    sliced.index.name = "date"
    return sliced
