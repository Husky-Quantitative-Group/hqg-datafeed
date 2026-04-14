import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .middleware import RequestLoggingMiddleware
from .routes import router
from src.config.config import settings
from src.storage.parquet_store import ParquetStore
from src.providers.yfinance import YFinanceProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    at Startup: init providers and attach them to app state so route handlers can access them.
    
    at Shutdown: clean up any active streams and provider connections.
    """
    logger.info("Starting datafeed")

    store = ParquetStore(settings.DATA_CACHE_DIR)
    app.state.historical_provider = YFinanceProvider(store)

    # TODO
    #app.state.live_provider = None  # placeholder
    # dict of stream_id : { symbols, cadence, status, queue, task } ??
    # app.state.streams = {}

    logger.info("Datafeed ready")
    yield       # app is running, serving requests

    # shutdown
    logger.info("Shutting down datafeed")
    # TODO: cancel all active streams & live provider
    logger.info("Datafeed stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="HQG Datafeed",
        version="0.0.1",
        lifespan=lifespan,
    )

    app.add_middleware(RequestLoggingMiddleware)
    app.include_router(router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


# uvicorn src.api.app:app
app = create_app()