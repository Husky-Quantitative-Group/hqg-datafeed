"""Immutable built-in provider factories and configuration-based construction."""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Mapping

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.providers.fred import FredProvider
from datafeed.altdata.providers.kalshi import KalshiProvider

if TYPE_CHECKING:
    from datafeed.config import Config

ProviderFactory = Callable[["Config"], AltDataProvider]

_PROVIDER_FACTORIES: Mapping[str, ProviderFactory] = MappingProxyType(
    {
        "FRED": lambda config: FredProvider(config.fred_api_key),
        "KALSHI": lambda config: KalshiProvider(),
    }
)


def build_providers(config: Config) -> Mapping[str, AltDataProvider]:
    """Build a fresh immutable mapping of provider instances for this configuration."""
    return MappingProxyType(
        {prefix: factory(config) for prefix, factory in _PROVIDER_FACTORIES.items()}
    )
