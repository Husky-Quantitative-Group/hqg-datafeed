from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping

from dotenv import dotenv_values

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.fred import FredProvider

ProviderFactory = Callable[["Config"], AltDataProvider]
_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
_FRED_API_KEY_ENV = "FRED_API_KEY"
_PLACEHOLDER_VALUES = {"", "INSERT_API_KEY_HERE"}

_PROVIDER_FACTORIES: Mapping[str, ProviderFactory] = MappingProxyType(
    {
        "FRED": lambda config: FredProvider(config.fred_api_key),
    }
)


@dataclass
class Config:
    fred_api_key: str | None = None

    def __post_init__(self) -> None:
        if self.fred_api_key is None:
            self.fred_api_key = _read_env_value(_FRED_API_KEY_ENV)

    @property
    def providers(self) -> Mapping[str, AltDataProvider]:
        """Immutable provider registry built from config."""
        providers = {
            prefix: factory(self)
            for prefix, factory in _PROVIDER_FACTORIES.items()
        }
        return MappingProxyType(providers)


def _read_env_value(key: str) -> str | None:
    """Read real environment var with .env as local fallback."""
    value = os.environ.get(key)
    if value is None:
        value = dotenv_values(_ENV_FILE).get(key)
    return value if value not in _PLACEHOLDER_VALUES else None
