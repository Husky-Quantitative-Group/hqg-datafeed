from fastapi import APIRouter, HTTPException, Request

from .models import (
    ErrorDetail,
    HistoricalRequest,
    HistoricalResponse,
    DataUnavailableError,
)
from .handlers import handle_historical

router = APIRouter(prefix="/data")

# get historical data (has side effects: cache state change)
@router.post("/historical", response_model=HistoricalResponse)
async def get_historical(body: HistoricalRequest, request: Request):
    provider = request.app.state.historical_provider

    try:
        return await handle_historical(body, provider)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DataUnavailableError as e:
        raise HTTPException(
            status_code=404,
            detail=ErrorDetail(
                error="data_unavailable",
                message=e.message,
                symbols=e.symbols,
            ).model_dump(),
        )

# TODO:
# POST: initalize live data subscription, return stream id
# GET: poll stream
# DELET: delete stream