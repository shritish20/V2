"""
Circuit Breaker - Preserved exactly with dependency injection fix
"""
import os
import logging
from datetime import date, datetime, timedelta
from app.infrastructure.metrics import circuit_breaker_active

logger = logging.getLogger("VOLGUARD")

class CircuitBreaker:
    def __init__(self, db_writer):
        self.db_writer = db_writer
        self.consecutive_losses = self._load_consecutive_losses()
        self.breaker_triggered = False
        self.breaker_until = None
        self.daily_slippage_events = 0
        self.last_reset_date = date.today()
        self.peak_capital = self._load_peak_capital()
        self.current_capital = 1000000  # Will be updated from settings

    def _load_consecutive_losses(self) -> int:
        try:
            value = self.db_writer.get_state("consecutive_losses")
            return int(value) if value else 0
        except Exception as e:
            logger.error(f"Failed to load consecutive losses: {e}")
            return 0

    def _load_peak_capital(self) -> float:
        try:
            value = self.db_writer.get_state("peak_capital")
            return float(value) if value else 1000000
        except Exception as e:
            logger.error(f"Failed to load peak capital: {e}")
            return 1000000

    def _save_consecutive_losses(self):
        self.db_writer.set_state("consecutive_losses", str(self.consecutive_losses))

    def _save_peak_capital(self):
        self.db_writer.set_state("peak_capital", str(self.peak_capital))

    def _check_daily_reset(self):
        if date.today() > self.last_reset_date:
            self.daily_slippage_events = 0
            self.last_reset_date = date.today()
            logger.info("Circuit breaker daily counters reset")

    def update_capital(self, new_capital: float):
        from app.core.config import settings
        self.current_capital = new_capital
        if new_capital > self.peak_capital:
            self.peak_capital = new_capital
            self._save_peak_capital()
        drawdown = (self.peak_capital - new_capital) / self.peak_capital
        if drawdown >= settings.MAX_DRAWDOWN_PCT:
            self.trigger_breaker("MAX_DRAWDOWN", f"Drawdown: {drawdown*100:.1f}%")
            return False
        return True

    def check_daily_trade_limit(self) -> bool:
        stats = self.db_writer.get_daily_stats()
        if stats and stats['trades_executed'] >= 3:  # MAX_TRADES_PER_DAY
            logger.warning(f"Daily trade limit reached: {stats['trades_executed']}/3")
            return False
        return True

    def check_daily_loss_limit(self, current_pnl: float) -> bool:
        from app.core.config import settings
        loss_pct = abs(current_pnl) / settings.BASE_CAPITAL
        if current_pnl < 0 and loss_pct >= settings.DAILY_LOSS_LIMIT:
            self.trigger_breaker("DAILY_LOSS_LIMIT", f"Loss: ₹{current_pnl:,.2f} ({loss_pct*100:.1f}%)")
            return False
        return True

    def record_slippage_event(self, slippage_pct: float) -> bool:
        self._check_daily_reset()
        self.daily_slippage_events += 1
        if self.daily_slippage_events >= 5:  # MAX_SLIPPAGE_EVENTS_PER_DAY
            self.trigger_breaker("EXCESSIVE_SLIPPAGE", f"{self.daily_slippage_events} events today")
            return False
        return True

    def record_trade_result(self, pnl: float) -> bool:
        from app.infrastructure.messaging import TelegramAlerter
        telegram = TelegramAlerter()
        
        if pnl < 0:
            self.consecutive_losses += 1
            self._save_consecutive_losses()
            if self.consecutive_losses >= 3:  # MAX_CONSECUTIVE_LOSSES
                self.trigger_breaker("CONSECUTIVE_LOSSES", f"{self.consecutive_losses} losses")
                return False
        else:
            if self.consecutive_losses > 0:
                logger.info(f"Winning trade after {self.consecutive_losses} losses - resetting counter")
                self.consecutive_losses = 0
                self._save_consecutive_losses()
        return True

    def trigger_breaker(self, reason: str, details: str):
        from app.infrastructure.messaging import TelegramAlerter
        telegram = TelegramAlerter()
        
        self.breaker_triggered = True
        self.breaker_until = datetime.now() + timedelta(seconds=86400)  # COOL_DOWN_PERIOD
        telegram.send(f"🔴 *CIRCUIT BREAKER*\n{reason}: {details}\nCooldown until: {self.breaker_until.strftime('%H:%M:%S')}", "CRITICAL")
        self.db_writer.log_risk_event("CIRCUIT_BREAKER", "CRITICAL", reason, details)
        logger.critical(f"CIRCUIT BREAKER: {reason} - {details}")
        circuit_breaker_active.set(1)

    def is_active(self) -> bool:
        from app.core.config import settings
        if os.path.exists(settings.KILL_SWITCH_FILE):
            logger.critical(f"KILL SWITCH DETECTED: {settings.KILL_SWITCH_FILE}")
            self.trigger_breaker("KILL_SWITCH", "Manual emergency stop")
            return True
        if self.breaker_triggered and self.breaker_until:
            if datetime.now() > self.breaker_until:
                logger.info("Circuit breaker cooldown expired - resetting")
                self.breaker_triggered = False
                self.breaker_until = None
                from app.infrastructure.messaging import TelegramAlerter
                TelegramAlerter().send("Circuit breaker cooldown expired - system ready", "SYSTEM")
                circuit_breaker_active.set(0)
                return False
        return self.breaker_triggered
