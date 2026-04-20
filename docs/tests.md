# Tests

## Layout

- `conftest.py` - shared fixtures. `tmp_store` / `provider` always use
  a pytest `tmp_path`, so the real parquet cache is never touched.
- `test_historical.py` - component tests for `YFinanceProvider` +
  `ParquetStore` (no API layer).
- `test_api.py` - API tests for `POST /data/historical` using
  `TestClient` + a mocked `HistoricalProvider`.

## Running

Fast tests only (offline, no yfinance):

    pytest -m "not network and not slow"

Everything except the benchmark:

    pytest -m "not slow"

Full suite (requires internet):

    pytest

Just the benchmark:

    pytest -m slow -s        # -s so the timing print shows

## Markers

- `@pytest.mark.network` - hits real yfinance.
- `@pytest.mark.slow` - benchmark / long-running.
