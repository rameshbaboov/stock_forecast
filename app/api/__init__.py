from fastapi import FastAPI

from .router_universe import router as universe_router
from .router_import import router as import_router
from .router_forecast import router as forecast_router
from .router_reports import router as reports_router


def register_api_routes(app: FastAPI) -> None:
    """
    Attach all API routers to the FastAPI application.
    """
    app.include_router(universe_router, prefix="/universe", tags=["universe"])
    app.include_router(import_router, prefix="/import", tags=["import"])
    app.include_router(forecast_router, prefix="/forecast", tags=["forecast"])
    app.include_router(reports_router, prefix="/reports", tags=["reports"])
