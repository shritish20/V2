"""
Trading Endpoints
"""
from fastapi import APIRouter, Depends, Request, BackgroundTasks
from typing import List, Dict
import threading

from app.services.trading.strategy import StrategyFactory
from app.services.trading.execution import ExecutionEngine
from app.services.analytics.engine import AnalyticsEngine
from app.services.analytics.regime import RegimeEngineV33
from app.services.risk.manager import RiskManager
from app.api.deps import get_upstox_client, get_db_writer, get_circuit_breaker, get_greeks_manager
from app.core.config import settings
from app.infrastructure.metrics import record_trade_open

router = APIRouter()

@router.post("/deploy")
async def deploy_strategy(request: Request, background_tasks: BackgroundTasks):
    """Deploy best strategy based on current market analysis"""
    from multiprocessing import Queue
    
    result_queue = Queue()
    config = {'access_token': settings.UPSTOX_ACCESS_TOKEN}
    
    analytics_engine = AnalyticsEngine(result_queue)
    analytics_engine.run(config)
    
    status, result = result_queue.get()
    
    if status != 'success':
        return {"status": "error", "message": result}
    
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
    
    mandate = weekly_mandate if weekly_mandate.score.composite > monthly_mandate.score.composite else monthly_mandate
    chain = result['weekly_chain'] if mandate == weekly_mandate else result['monthly_chain']
    
    if not mandate.is_trade_allowed or mandate.max_lots == 0:
        return {
            "status": "rejected",
            "reason": mandate.veto_reasons,
            "warnings": mandate.warnings
        }
    
    client = get_upstox_client(request)
    db_writer = get_db_writer(request)
    circuit_breaker = get_circuit_breaker(request)
    greeks_mgr = get_greeks_manager(request)
    
    strategy_factory = StrategyFactory(client)
    execution_engine = ExecutionEngine(client, db_writer, circuit_breaker)
    
    legs, max_risk = strategy_factory.generate(
        mandate, chain, result['lot_size'], result['vol_metrics'], 
        result['vol_metrics'].spot, result['struct_metrics_weekly']
    )
    
    if not legs:
        return {"status": "error", "message": "Failed to generate strategy legs"}
    
    filled_legs = execution_engine.execute_strategy(legs)
    
    if not filled_legs:
        return {"status": "error", "message": "Execution failed"}
    
    trade_id = f"VG33_{'PAPER' if settings.DRY_RUN_MODE else 'LIVE'}_{int(__import__('time').time())}"
    
    entry_premium = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'SELL')
    entry_debit = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'BUY')
    net_premium = entry_premium - entry_debit
    
    db_writer.save_trade(trade_id, mandate.strategy_type, mandate.expiry_date, filled_legs, net_premium, max_risk)
    record_trade_open(mandate.strategy_type, mandate.expiry_type, trade_id)
    
    gtt_ids = []
    if not settings.DRY_RUN_MODE:
        short_legs = [l for l in filled_legs if l['side'] == 'SELL']
        if short_legs:
            for leg in short_legs:
                gtt_id = execution_engine.place_gtt_order(
                    leg['key'], leg['filled_qty'], 'BUY', 
                    round(leg['entry_price'] * 2.0, 1), 
                    round(leg['entry_price'] * 0.30, 1)
                )
                if gtt_id:
                    gtt_ids.append(gtt_id)
    
    risk_manager = RiskManager(
        client, filled_legs, mandate.expiry_date, trade_id, 
        gtt_ids, db_writer, circuit_breaker, greeks_mgr
    )
    
    risk_thread = threading.Thread(target=risk_manager.monitor, daemon=True)
    risk_thread.start()
    
    request.app.state.active_risk_managers[trade_id] = risk_manager
    
    return {
        "status": "success",
        "trade_id": trade_id,
        "structure": mandate.suggested_structure,
        "net_premium": net_premium,
        "max_risk": max_risk,
        "legs": len(filled_legs),
        "gtt_orders": len(gtt_ids)
    }

@router.post("/flatten/{trade_id}")
async def flatten_position(trade_id: str, request: Request):
    """Manually flatten a position"""
    if trade_id in request.app.state.active_risk_managers:
        risk_manager = request.app.state.active_risk_managers[trade_id]
        risk_manager.flatten_all("MANUAL_REQUEST")
        return {"status": "success", "message": f"Flattening {trade_id}"}
    return {"status": "error", "message": "Trade not found"}

@router.get("/status")
async def get_status(request: Request):
    """Get system trading status"""
    circuit_breaker = get_circuit_breaker(request)
    return {
        "circuit_breaker_active": circuit_breaker.is_active(),
        "dry_run": settings.DRY_RUN_MODE,
        "capital": settings.BASE_CAPITAL
    }
