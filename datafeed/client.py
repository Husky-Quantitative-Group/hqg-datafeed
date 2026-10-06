from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time
import logging
from typing import Mapping

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
    ) -> pd.DataFrame:
        """
        Return native-frequency alternative data over exactly [start, end].

        Alt data is indexed by observation date, not by the date the value
        became public, and values can be revised later. If providers include
        an available_at column, values are conservatively shifted to end-of-day
        on their release date.
        """
        try:
            normalized = normalize_alt_data_ids(ids)
        except DataFeedError as exc:
            raise DataFeedError(f"{exc}. Registered providers: {self._registered_providers()}") from exc
        if not normalized:
            return _empty_alt_frame()

        self._validate_providers(normalized)

        grouped: dict[str, list[AltDataId]] = defaultdict(list)
        for parsed in normalized:
            grouped[parsed.provider].append(parsed)

        series_frames: dict[tuple[str, str], pd.Series] = {}

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

                for field in clean_frame.columns:
                    series_frames[(item.output_id, str(field))] = clean_frame[field]

        if not series_frames:
            return _empty_alt_frame()

        result = pd.DataFrame(series_frames)
        result.index = pd.to_datetime(result.index)
        result.index.name = "date"
        result.columns = pd.MultiIndex.from_tuples(
            result.columns,
            names=["series_id", "field"],
        )

        # Redundant slice to ensure only date in range is returned
        return _slice_alt_frame(result, start, end)

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


def _empty_alt_frame() -> pd.DataFrame:
    return pd.DataFrame(
        index=pd.DatetimeIndex([], name="date"),
        columns=pd.MultiIndex.from_tuples([], names=["series_id", "field"]),
    )


def _find_provider_frame(
    provider_result: dict[str, pd.DataFrame],
    series_id: str,
) -> pd.DataFrame | None:
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
    prepared.index = pd.to_datetime(prepared.index)
    prepared.index.name = "date"
    if "available_at" in prepared.columns:
        prepared["available_at"] = prepared["available_at"].map(_parse_available_at)
    return _slice_alt_frame(prepared, start, end)


def _slice_alt_frame(frame: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    index = pd.to_datetime(frame.index)
    sliced = frame.loc[(index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))]
    sliced.index = pd.to_datetime(sliced.index)
    sliced.index.name = "date"
    return sliced


def _parse_available_at(raw_value) -> pd.Timestamp:
    """Parse provider release metadata into a conservative end-of-day timestamp."""
    timestamp = pd.Timestamp(raw_value)
    if pd.isna(timestamp):
        return timestamp

    return pd.Timestamp.combine(timestamp.date(), time(23, 59, 59))
