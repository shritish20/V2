"""
Analysis Endpoints - Production Fixed
Uses run_in_threadpool for CPU-intensive analytics
"""
from fastapi import APIRouter, Depends, Request
from starlette.concurrency import run_in_threadpool
from typing import Dict, Any

from app.services.analytics.engine import AnalyticsEngine
from app.services.analytics.regime import RegimeEngineV33
from app.core.config import settings

router = APIRouter()

@router.post("/run")
async def run_analysis(request: Request):
    """Run market analysis and return regime mandates"""
    # Run CPU-intensive analytics in threadpool to avoid blocking event loop
    engine = AnalyticsEngine()
    config = {'access_token': settings.UPSTOX_ACCESS_TOKEN}
    
    result = await run_in_threadpool(engine.run_sync, config)
    
    if result.get('status') != 'success':
        return {"status": "error", "message": result.get('message', 'Unknown error')}
    
    regime_engine = RegimeEngineV33()
    
    weekly_mandate = regime_engine.generate_mandate(
        regime_engine.calculate_scores(
            result['vol_metrics'],
            result['struct_metrics_weekly'],
            result['edge_metrics'],
            result['external_metrics'],
            "WEEKLY",
            result['time_metrics'].dte_weekly
        ),
        result['vol_metrics'],
        result['struct_metrics_weekly'],
        result['edge_metrics'],
        result['external_metrics'],
        result['time_metrics'],
        "WEEKLY",
        result['time_metrics'].weekly_exp,
        result['time_metrics'].dte_weekly
    )
    
    monthly_mandate = regime_engine.generate_mandate(
        regime_engine.calculate_scores(
            result['vol_metrics'],
            result['struct_metrics_monthly'],
            result['edge_metrics'],
            result['external_metrics'],
            "MONTHLY",
            result['time_metrics'].dte_monthly
        ),
        result['vol_metrics'],
        result['struct_metrics_monthly'],
        result['edge_metrics'],
        result['external_metrics'],
        result['time_metrics'],
        "MONTHLY",
        result['time_metrics'].monthly_exp,
        result['time_metrics'].dte_monthly
    )
    
    return {
        "status": "success",
        "timestamp": result['timestamp'],
        "spot": result['vol_metrics'].spot,
        "vix": result['vol_metrics'].vix,
        "weekly_mandate": {
            "regime": weekly_mandate.regime_name,
            "structure": weekly_mandate.suggested_structure,
            "score": weekly_mandate.score.composite,
            "allocation": weekly_mandate.allocation_pct,
            "max_lots": weekly_mandate.max_lots,
            "allowed": weekly_mandate.is_trade_allowed,
            "warnings": weekly_mandate.warnings
        },
        "monthly_mandate": {
            "regime": monthly_mandate.regime_name,
            "structure": monthly_mandate.suggested_structure,
            "score": monthly_mandate.score.composite,
            "allocation": monthly_mandate.allocation_pct,
            "max_lots": monthly_mandate.max_lots,
            "allowed": monthly_mandate.is_trade_allowed,
            "warnings": monthly_mandate.warnings
        }
    }
