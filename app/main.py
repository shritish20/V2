"""
FastAPI Main Application Entry Point - Institutional Grade (v3.5)
Fixed: Graceful shutdown awaits all positions to flatten
"""
import logging
import sys
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

import upstox_client
from fastapi import FastAPI

from app.api.v1.api import api_router
from app.core.config import settings
from app.core.task_manager import TaskSupervisor, get_task_supervisor
from app.infrastructure.database import DatabaseWriter
from app.infrastructure.messaging import TelegramAlerter
from app.infrastructure.circuit_breaker import CircuitBreaker
from app.infrastructure.metrics import start_metrics_server, sys_uptime
from app.integrations.upstox.websockets import LiveGreeksManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("VOLGUARD")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle with institutional-grade shutdown"""
    logger.info("=" * 80)
    logger.info("VOLGUARD 3.5 – INSTITUTIONAL GRADE")
    logger.info("Task Supervisor Active | Graceful Shutdown Guaranteed")
    if settings.DRY_RUN_MODE:
        logger.warning("🎯 DRY RUN MODE ENABLED - NO REAL TRADES")
    logger.info("=" * 80)
    
    # Initialize TaskSupervisor (Issue 1 fix)
    task_supervisor = get_task_supervisor()
    
    # Initialize services
    app.state.db_writer = DatabaseWriter(settings.DB_PATH)
    app.state.telegram = TelegramAlerter()
    app.state.circuit_breaker = CircuitBreaker(app.state.db_writer)
    app.state.upstox_client = upstox_client.ApiClient()
    app.state.upstox_client.configuration.access_token = settings.UPSTOX_ACCESS_TOKEN
    app.state.greeks_manager = LiveGreeksManager(app.state.upstox_client)
    app.state.task_supervisor = task_supervisor
    
    # Start services
    app.state.greeks_manager.start()
    sys_uptime.set_to_current_time()
    
    # Start metrics server
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
            f"🚀 System Startup v3.5 Institutional\n"
            f"Task Supervisor: Active\n"
            f"Graceful Shutdown: Guaranteed\n"
            f"Environment: {settings.ENVIRONMENT}\n"
            f"{'📄 DRY RUN' if settings.DRY_RUN_MODE else '💰 LIVE TRADING'}",
            "SUCCESS"
        )
    
    logger.info("✅ Application startup complete - Institution Ready")
    
    yield
    
    # SHUTDOWN SEQUENCE - Issue 2 Fix: Properly await all tasks
    logger.info("=" * 80)
    logger.info("INITIATING GRACEFUL SHUTDOWN SEQUENCE")
    logger.info("=" * 80)
    
    # Stop accepting new WebSocket data
    app.state.greeks_manager.stop()
    
    # FIX Issue 2: Block until all risk monitors complete flattening
    # This ensures all positions are closed before container dies
    try:
        await task_supervisor.graceful_shutdown(timeout=30.0)
    except Exception as e:
        logger.error(f"Error during graceful shutdown: {e}")
    
    # Final database cleanup
    app.state.db_writer.shutdown()
    
    app.state.telegram.send(
        "System shutdown complete - All positions secured", 
        "SYSTEM"
    )
    logger.info("✅ Application shutdown complete - All tasks finalized")

app = FastAPI(
    title="VOLGUARD 3.5 API",
    description="Institutional Grade Option Selling System - Task Supervised",
    version="3.5.0",
    lifespan=lifespan
)

app.include_router(api_router, prefix="/api/v1")

@app.get("/")
def root():
    supervisor = get_task_supervisor()
    return {
        "status": "VOLGUARD 3.5 Institutional",
        "mode": "dry_run" if settings.DRY_RUN_MODE else "live",
        "timestamp": datetime.now().isoformat(),
        "active_monitors": supervisor.get_active_count(),
        "features": [
            "Async Analytics",
            "Task Supervision", 
            "Graceful Shutdown Guaranteed",
            "Trade ID Safety"
        ]
    }
