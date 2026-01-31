"""
FastAPI Main Application Entry Point - Production Fixed
"""
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime

import upstox_client
from fastapi import FastAPI

from app.api.v1.api import api_router
from app.core.config import settings
from app.infrastructure.database import DatabaseWriter
from app.infrastructure.messaging import TelegramAlerter
from app.infrastructure.circuit_breaker import CircuitBreaker
from app.infrastructure.metrics import start_metrics_server, sys_uptime
from app.integrations.upstox.websockets import LiveGreeksManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("VOLGUARD")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("=" * 80)
    logger.info("VOLGUARD 3.4 – PRODUCTION STABILIZED")
    logger.info("FastAPI Modular Deployment")
    if settings.DRY_RUN_MODE:
        logger.warning("🎯 DRY RUN MODE ENABLED - NO REAL TRADES")
    logger.info("=" * 80)
    
    # Initialize services
    app.state.db_writer = DatabaseWriter(settings.DB_PATH)
    app.state.telegram = TelegramAlerter()
    app.state.circuit_breaker = CircuitBreaker(app.state.db_writer)
    app.state.upstox_client = upstox_client.ApiClient()
    app.state.upstox_client.configuration.access_token = settings.UPSTOX_ACCESS_TOKEN
    app.state.greeks_manager = LiveGreeksManager(app.state.upstox_client)
    
    # FIX: Initialize active_risk_managers as dict (Blocker 4 fix)
    app.state.active_risk_managers = {}
    
    # Start services
    app.state.greeks_manager.start()
    sys_uptime.set_to_current_time()
    
    # Start metrics server in background if not running
    try:
        import threading
        metrics_thread = threading.Thread(
            target=start_metrics_server, 
            args=(8000,), 
            daemon=True,
            name="Metrics-Server"
        )
        metrics_thread.start()
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
    
    if settings.TELEGRAM_BOT_TOKEN:
        app.state.telegram.send(
            f"🚀 System Startup v3.4\n"
            f"Production Stabilized\n"
            f"Environment: {settings.ENVIRONMENT}\n"
            f"{'📄 DRY RUN' if settings.DRY_RUN_MODE else '💰 LIVE TRADING'}",
            "SUCCESS"
        )
    
    logger.info("✅ Application startup complete - Production Ready")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    app.state.greeks_manager.stop()
    
    # Cancel all running risk managers gracefully
    if hasattr(app.state, 'active_risk_managers'):
        for trade_id, rm in list(app.state.active_risk_managers.items()):
            if rm.running:
                logger.info(f"Shutting down risk manager for {trade_id}")
                import asyncio
                asyncio.create_task(rm.flatten_all_async("SYSTEM_SHUTDOWN"))
    
    app.state.db_writer.shutdown()
    app.state.telegram.send("System shutdown complete", "SYSTEM")
    logger.info("✅ Application shutdown complete")

app = FastAPI(
    title="VOLGUARD 3.4 API",
    description="Professional Option Selling System - Production Stabilized",
    version="3.4.0",
    lifespan=lifespan
)

app.include_router(api_router, prefix="/api/v1")

@app.get("/")
def root():
    return {
        "status": "VOLGUARD 3.4 Production Ready",
        "mode": "dry_run" if settings.DRY_RUN_MODE else "live",
        "timestamp": datetime.now().isoformat(),
        "features": [
            "Async Analytics",
            "Threadpool Isolation", 
            "Async Risk Monitoring",
            "Circular Import Safe"
        ]
    }
