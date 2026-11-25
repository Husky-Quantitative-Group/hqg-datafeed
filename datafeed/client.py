from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import logging
from typing import Any, Mapping

import pandas as pd

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.common import find_provider_payload, prepare_alt_frame, validate_alt_frame
from datafeed.altdata.ids import AltDataId, normalize_alt_data_ids
from datafeed.config import Config
from datafeed.errors import (
    DataFeedError,
    DataUnavailableError,
    InvalidSeriesIdError,
    contextualize_error,
    redact_errors,
)
from datafeed.models import MarketData
from datafeed.securities import get_securities_frame

logger = logging.getLogger(__name__)


class DataFeed:
    """Stateless datafeed client."""

    def __init__(self, config: Config):
        self.config = config
        self.providers: Mapping[str, AltDataProvider] = config.providers

    @redact_errors
    def get_securities(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        
        """Return daily securities OHLCV over exactly [start, end]."""
        return get_securities_frame(symbols, start, end)

    @redact_errors
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
                context = (
                    f"Provider '{provider_prefix}' failed for series {', '.join(full_ids)}"
                )
                wrapped = contextualize_error(exc, context)
                if (
                    str(wrapped) != f"{context}: {exc}"
                    or exc.__cause__ is not None
                    or (exc.__context__ is not None and not exc.__suppress_context__)
                ):
                    raise wrapped from None
                raise wrapped from exc

            for item in requested_ids:
                frame = find_provider_payload(provider_result, item.series)
                if frame is None:
                    raise DataUnavailableError(
                        f"Provider '{provider_prefix}' returned no data for series {item.output_id}"
                    )

                clean_frame = prepare_alt_frame(
                    frame, start, end, logger=logger, series_id=item.output_id
                )
                if clean_frame.empty:
                    raise DataUnavailableError(
                        f"Provider '{provider_prefix}' returned empty data for series {item.output_id}"
                    )
                validate_alt_frame(clean_frame, item.output_id, provider_prefix)

                series_frames[item.output_id] = clean_frame

        return {item.output_id: series_frames[item.output_id] for item in requested}

    @redact_errors
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
                context = (
                    f"Provider '{provider_prefix}' failed for raw series {', '.join(full_ids)}"
                )
                wrapped = contextualize_error(exc, context)
                if (
                    str(wrapped) != f"{context}: {exc}"
                    or exc.__cause__ is not None
                    or (exc.__context__ is not None and not exc.__suppress_context__)
                ):
                    raise wrapped from None
                raise wrapped from exc

            for item in requested_ids:
                payload = find_provider_payload(provider_result, item.series)
                if payload is None:
                    raise DataUnavailableError(
                        f"Provider '{provider_prefix}' returned no raw data for series {item.output_id}"
                    )
                result[item.output_id] = payload

        return result

    @redact_errors
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

        raise InvalidSeriesIdError(
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
            raise InvalidSeriesIdError(f"{exc}. Registered providers: {self._registered_providers()}") from exc

        self._validate_providers(normalized)
        return normalized


def _group_normalized_alt_data_ids(ids: list[AltDataId]) -> dict[str, list[AltDataId]]:
    grouped: dict[str, list[AltDataId]] = defaultdict(list)
    for parsed in ids:
        grouped[parsed.provider].append(parsed)
    return grouped
