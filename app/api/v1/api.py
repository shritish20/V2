"""
API Router Aggregation
"""
from fastapi import APIRouter
from app.api.v1.endpoints import analysis, trading, health

api_router = APIRouter()
api_router.include_router(analysis.router, prefix="/analysis", tags=["analysis"])
api_router.include_router(trading.router, prefix="/trading", tags=["trading"])
api_router.include_router(health.router, prefix="", tags=["health"])
