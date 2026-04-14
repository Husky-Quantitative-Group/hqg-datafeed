import os


class Settings:
    @property
    def DATA_CACHE_DIR(self) -> str:
        return os.environ.get("DATA_CACHE_DIR", "/data/cache")

    @property
    def ALPACA_API_KEY(self) -> str:
        return os.environ.get("ALPACA_API_KEY", "")

    @property
    def ALPACA_SECRET_KEY(self) -> str:
        return os.environ.get("ALPACA_SECRET_KEY", "")

    @property
    def ALPACA_POLL_INTERVAL(self) -> float:
        return float(os.environ.get("ALPACA_POLL_INTERVAL", "30.0"))


settings = Settings()