from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import fields, is_dataclass
from functools import wraps
import re
from typing import Any, Callable, TypeVar, cast
from urllib.parse import quote, quote_plus

import requests

_ACTIVE_SECRETS: ContextVar[tuple[str, ...]] = ContextVar("datafeed_error_secrets", default=())
_Method = TypeVar("_Method", bound=Callable[..., Any])


def _config_secrets(config: object) -> tuple[str, ...]:
    """Find credential fields by explicit declaration, dataclass metadata, or name."""
    values = vars(config) if hasattr(config, "__dict__") else {}
    metadata = {field.name: field.metadata for field in fields(config)} if is_dataclass(config) else {}
    declared = set(getattr(config, "secret_fields", ()))
    secrets = []
    for name, value in values.items():
        heuristic = re.search(r"api_?key|token|secret|password|credential", name, re.IGNORECASE)
        sensitive = name in declared or bool(heuristic)
        if metadata.get(name, {}).get("secret", sensitive) and isinstance(value, str) and value:
            secrets.append(value)
    return tuple(secrets)


@contextmanager
def error_redaction(config: object) -> Iterator[None]:
    """Use this configuration's credentials for errors and diagnostics in this scope.

    Context-local storage isolates concurrent feeds and restores the previous
    values on exit. Provider instances can also supply directly held credentials
    when called outside DataFeed. No environment loading occurs here.
    """
    values = tuple(dict.fromkeys((*_ACTIVE_SECRETS.get(), *_config_secrets(config))))
    token = _ACTIVE_SECRETS.set(values)
    try:
        yield
    finally:
        _ACTIVE_SECRETS.reset(token)


def redact_errors(method: _Method) -> _Method:
    """Apply automatic credential discovery to a synchronous client/provider method."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with error_redaction(getattr(self, "config", self)):
            return method(self, *args, **kwargs)

    return cast(_Method, wrapped)


class DataFeedError(Exception):
    """Raised when datafeed cannot fetch or validate requested data."""

    def __init__(self, *args: object):
        super().__init__(*(redact_sensitive_text(arg) if isinstance(arg, str) else arg for arg in args))


class DataFeedRequestError(DataFeedError):
    """A provider request failed at the transport or HTTP layer."""


class InvalidProviderResponseError(DataFeedError):
    """A provider returned malformed payloads or invalid observations."""


class InvalidSeriesIdError(DataFeedError):
    """A requested series identifier or provider prefix is invalid."""


class DataUnavailableError(DataFeedError):
    """A requested series has no usable data in the requested range."""


def redact_sensitive_text(text: str) -> str:
    """Redact known secrets, bearer tokens, and explicit credential fields.

    Known secrets are replaced wherever they appear, including URL-encoded forms.
    Bearer tokens and a small set of unambiguous credential labels (api_key,
    access_token, refresh_token, client_secret, password) are also masked.
    This is for diagnostics only; raw provider payloads remain unmodified.
    """
    for secret in sorted(_ACTIVE_SECRETS.get(), key=len, reverse=True):
        if secret:
            values = {secret, quote(secret, safe=""), quote_plus(secret)}
            for value in sorted(values, key=len, reverse=True):
                text = text.replace(value, "[REDACTED]")
    text = re.sub(r"(?i)(\bBearer\s+)[\w.~+/=-]+", r"\1[REDACTED]", text)
    return re.sub(
        r'''(?ix)(\b(?:[a-z0-9]+[_-])?(?:api[_-]?key|access[_-]?token|
        refresh[_-]?token|client[_-]?secret|password)
        \b["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s&<>,;]+)''',
        r"\1[REDACTED]",
        text,
    )


def contextualize_error(error: Exception, context: str) -> DataFeedError:
    """Add request context, preserving standard categories and redacting credentials."""
    error_class: type[DataFeedError] = DataFeedError
    if isinstance(error, requests.RequestException):
        error_class = DataFeedRequestError
        detail = type(error).__name__
    else:
        for category in (
            DataFeedRequestError,
            InvalidProviderResponseError,
            InvalidSeriesIdError,
            DataUnavailableError,
        ):
            if isinstance(error, category):
                error_class = category
                break
        detail = str(error)
    return error_class(f"{context}: {detail}")
