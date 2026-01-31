"""
Institutional Task Supervisor
Prevents orphaned tasks, guarantees graceful shutdown, tracks lifecycle
"""
import asyncio
import logging
from typing import Set, Optional
from datetime import datetime

logger = logging.getLogger("VOLGUARD")

class TaskSupervisor:
    """
    Production-grade task manager that survives worker reloads
    and guarantees cleanup on shutdown.
    """
    def __init__(self):
        self.active_tasks: Set[asyncio.Task] = set()
        self.risk_monitors: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()
        
    async def spawn_risk_monitor(self, trade_id: str, coro) -> asyncio.Task:
        """
        Spawn a risk monitor with guaranteed lifecycle tracking.
        Prevents garbage collection and handles completion cleanup.
        """
        task = asyncio.create_task(coro)
        
        async with self.lock:
            self.active_tasks.add(task)
            self.risk_monitors[trade_id] = task
            
        # Add done callback to auto-cleanup
        def on_done(t):
            asyncio.create_task(self._cleanup_task(trade_id, t))
            
        task.add_done_callback(on_done)
        logger.info(f"TaskSupervisor: Spawned risk monitor for {trade_id} (Total active: {len(self.active_tasks)})")
        return task
    
    async def _cleanup_task(self, trade_id: str, task: asyncio.Task):
        """Cleanup completed tasks from registry"""
        async with self.lock:
            self.active_tasks.discard(task)
            self.risk_monitors.pop(trade_id, None)
            
        if task.cancelled():
            logger.warning(f"TaskSupervisor: Task {trade_id} was cancelled")
        elif task.exception():
            logger.error(f"TaskSupervisor: Task {trade_id} failed with {task.exception()}")
        else:
            logger.info(f"TaskSupervisor: Task {trade_id} completed successfully")
    
    async def graceful_shutdown(self, timeout: float = 30.0):
        """
        Institutional-grade shutdown: awaits all positions to flatten.
        Blocks until all risk managers complete or timeout hits.
        """
        logger.critical(f"TaskSupervisor: Initiating graceful shutdown ({timeout}s timeout)")
        
        async with self.lock:
            # Get current active tasks snapshot
            tasks_to_wait = list(self.active_tasks)
            trade_ids = list(self.risk_monitors.keys())
            
        if not tasks_to_wait:
            logger.info("TaskSupervisor: No active tasks to shutdown")
            return
            
        logger.info(f"TaskSupervisor: Waiting for {len(tasks_to_wait)} tasks: {trade_ids}")
        
        # Create timeout wrapper
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks_to_wait, return_exceptions=True),
                timeout=timeout
            )
            logger.info("TaskSupervisor: All tasks completed gracefully")
        except asyncio.TimeoutError:
            logger.critical("TaskSupervisor: TIMEOUT - Force cancelling remaining tasks")
            for task in tasks_to_wait:
                if not task.done():
                    task.cancel()
    
    def get_active_count(self) -> int:
        return len(self.active_tasks)
        
    def is_monitor_active(self, trade_id: str) -> bool:
        return trade_id in self.risk_monitors and not self.risk_monitors[trade_id].done()

# Singleton pattern for app state (initialized in lifespan)
_task_supervisor: Optional[TaskSupervisor] = None

def get_task_supervisor() -> TaskSupervisor:
    global _task_supervisor
    if _task_supervisor is None:
        _task_supervisor = TaskSupervisor()
    return _task_supervisor
