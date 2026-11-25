from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values

from datafeed.altdata.base import AltDataProvider
from datafeed.altdata.registry import build_providers

_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
_PLACEHOLDER_VALUES = {None, "", "INSERT_API_KEY_HERE"}

# Config attribute name -> environment variable name.
# Adding an entry here to add a new credential
_ENV_KEYS: Mapping[str, str] = {
    "fred_api_key": "FRED_API_KEY",
}


class Config:
    def __init__(self, **overrides: str | None) -> None:
        unknown = set(overrides) - set(_ENV_KEYS)
        if unknown:
            raise TypeError(f"Unknown config option(s): {', '.join(sorted(unknown))}")
        self.secret_fields = tuple(_ENV_KEYS)
        for attr, env_name in _ENV_KEYS.items():
            setattr(self, attr, overrides.get(attr) or _read_env_value(env_name))

    @property
    def providers(self) -> Mapping[str, AltDataProvider]:
        """Immutable provider registry built from config."""
        return build_providers(self)


def _read_env_value(key: str) -> str | None:
    """Read real environment var with .env as local fallback."""
    value = os.environ.get(key)
    if value in _PLACEHOLDER_VALUES:
        value = dotenv_values(_ENV_FILE).get(key)
    return None if value in _PLACEHOLDER_VALUES else value
