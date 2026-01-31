"""
Risk Manager - Preserved
"""
import time
import logging
import traceback
from datetime import date
from typing import List, Dict

import upstox_client
from upstox_client.api.market_quote_api import MarketQuoteV3Api
from upstox_client.api.portfolio_api import PortfolioApi

from app.core.config import settings
from app.infrastructure.database import DatabaseWriter
from app.infrastructure.circuit_breaker import CircuitBreaker
from app.infrastructure.messaging import TelegramAlerter
from app.infrastructure.metrics import trade_pnl
from app.integrations.upstox.websockets import LiveGreeksManager

logger = logging.getLogger("VOLGUARD")

class RiskManager:
    def __init__(self, api_client: upstox_client.ApiClient, legs: List[Dict], expiry_date: date, trade_id: str, gtt_ids: List[str], db_writer: DatabaseWriter, circuit_breaker: CircuitBreaker, greeks_mgr: LiveGreeksManager):
        self.api_client = api_client
        self.legs = legs
        self.expiry = expiry_date
        self.trade_id = trade_id
        self.gtt_ids = gtt_ids or []
        self.running = True
        self.last_price_update = time.time()
        self.db_writer = db_writer
        self.circuit_breaker = circuit_breaker
        self.telegram = TelegramAlerter()
        self.greeks_mgr = greeks_mgr
        
        credit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'SELL')
        debit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'BUY')
        self.net_premium = credit - debit
        
        structure = legs[0].get('structure', 'UNKNOWN') if legs else 'UNKNOWN'
        qty = legs[0]['filled_qty'] if legs else 0
        
        if structure in ['IRON_FLY', 'IRON_CONDOR']:
            call_strikes = sorted([l['strike'] for l in legs if l['type'] == 'CE'])
            put_strikes = sorted([l['strike'] for l in legs if l['type'] == 'PE'])
            
            call_width = (call_strikes[-1] - call_strikes[0]) if len(call_strikes) >= 2 else 0
            put_width = (put_strikes[-1] - put_strikes[0]) if len(put_strikes) >= 2 else 0
            
            max_spread_width = max(call_width, put_width)
            total_width_value = max_spread_width * qty
            
            self.max_spread_loss = max(0, total_width_value - self.net_premium)
        else:
            self.max_spread_loss = self.net_premium * 2

        logger.info(f"Risk Manager Init: Trade={trade_id} | Premium=₹{self.net_premium:,.2f} | Max Risk=₹{self.max_spread_loss:,.2f}")

    def monitor(self):
        from app.services.trading.execution import ExecutionEngine
        
        market_api = MarketQuoteV3Api(self.api_client)
        consecutive_errors = 0
        max_consecutive_errors = 10
        logger.info(f"🔍 Risk monitoring started for {self.trade_id}")
        
        while self.running:
            try:
                current_time = time.time()
                if int(current_time) % 5 == 0 and self.greeks_mgr.subscribed_keys:
                    warnings = self.greeks_mgr.check_risk_limits(self.legs, self.trade_id)
                    for warning in warnings:
                        if 'CRITICAL' in warning:
                            logger.critical(warning)
                            if 'θ/ν Ratio' in warning and 'CRITICAL' in warning:
                                logger.critical("Auto-flattening due to extreme Theta/Vega ratio")
                                self.flatten_all("THETA_VEGA_RATIO_CRITICAL")
                                return
                        else:
                            logger.warning(warning)
                    
                    if int(current_time) % 30 == 0:
                        port = self.greeks_mgr.get_portfolio_greeks(self.legs, self.trade_id)
                        logger.info(
                            f"📊 Live Portfolio Greeks | "
                            f"Δ:{port['delta']:.1f} | "
                            f"θ:{port['theta']/1000:.1f}k | "
                            f"ν:{port['vega']:.3f} | "
                            f"Γ:{port['gamma']:.1f} | "
                            f"θ/ν:{port['theta_vega_ratio']:.2f}"
                        )
                
                days_to_expiry = (self.expiry - date.today()).days
                if days_to_expiry <= settings.EXIT_DTE:
                    logger.info(f"DTE exit trigger: {days_to_expiry} days remaining")
                    self.flatten_all("DTE_EXIT")
                    return

                keys = [l['key'] for l in self.legs]
                price_response = None
                for attempt in range(3):
                    try:
                        price_response = market_api.get_ltp(instrument_key=','.join(keys))
                        if price_response and price_response.status == 'success':
                            break
                    except Exception as e:
                        logger.warning(f"Price fetch attempt {attempt+1} failed: {e}")
                        time.sleep(0.5)
                
                if not price_response or price_response.status != 'success':
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.critical(f"Price feed failed {consecutive_errors} times - flattening for safety")
                        self.flatten_all("PRICE_FEED_FAILURE")
                        return
                    time.sleep(settings.POLL_INTERVAL)
                    continue
                
                consecutive_errors = 0
                prices = price_response.data
                self.last_price_update = time.time()

                current_pnl = self._calculate_pnl(prices)
                
                if self.max_spread_loss > 0 and current_pnl < -(self.max_spread_loss * 0.80):
                    logger.critical(f"Max risk breached: P&L={current_pnl:.2f}, Limit={self.max_spread_loss:.2f}")
                    self.flatten_all("STOP_LOSS_MAX_RISK")
                    return
                
                stop_threshold = -(self.net_premium * settings.STOP_LOSS_PCT)
                if self.net_premium > 0 and current_pnl < stop_threshold:
                    logger.critical(f"Stop loss hit: P&L={current_pnl:.2f}, Threshold={stop_threshold:.2f}")
                    self.flatten_all("STOP_LOSS_PREMIUM")
                    return
                
                target_pnl = self.net_premium * settings.TARGET_PROFIT_PCT
                if self.net_premium > 0 and (current_pnl >= target_pnl - 0.1):
                    logger.info(f"Target profit reached: P&L={current_pnl:.2f}, Target={target_pnl:.2f}")
                    self.flatten_all("TARGET_PROFIT")
                    return

                self._update_dashboard_state(current_pnl)
                time.sleep(settings.POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Risk monitor interrupted by user")
                self.running = False
                return
            except Exception as e:
                logger.error(f"Risk monitor error: {e}")
                traceback.print_exc()
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("Too many errors in risk monitor - emergency exit")
                    self.flatten_all("MONITOR_ERROR")
                    return
                time.sleep(5)

    def _calculate_pnl(self, prices) -> float:
        pnl = 0.0
        missing_data_count = 0
        
        for leg in self.legs:
            current_price = 0.0
            
            if leg['key'] in prices and hasattr(prices[leg['key']], 'last_price'):
                current_price = prices[leg['key']].last_price
                leg['last_known_ltp'] = current_price
            elif 'last_known_ltp' in leg:
                current_price = leg['last_known_ltp']
                logger.warning(f"⚠️ Using stale price for {leg['key']}: {current_price}")
            else:
                missing_data_count += 1
                logger.error(f"❌ NO DATA for {leg['key']}")
                continue 

            if leg['side'] == 'SELL':
                leg_pnl = (leg['entry_price'] - current_price) * leg['filled_qty']
            else:
                leg_pnl = (current_price - leg['entry_price']) * leg['filled_qty']
            
            pnl += leg_pnl

        if missing_data_count > 0:
            logger.critical(f"Incomplete P&L calc due to missing data on {missing_data_count} legs")
            if missing_data_count > len(self.legs) / 2:
                self.flatten_all("DATA_FEED_LOSS")
        
        return pnl

    def _update_dashboard_state(self, current_pnl: float):
        try:
            pnl_pct = (current_pnl / self.net_premium * 100) if self.net_premium > 0 else 0
            trade_pnl.labels(trade_id=self.trade_id, strategy="LIVE").set(current_pnl)
        except Exception:
            pass

    def flatten_all(self, reason="SIGNAL"):
        from app.services.trading.execution import ExecutionEngine
        
        logger.critical(f"🚨 FLATTEN TRIGGERED: {reason}")
        self.telegram.send(f"🚨 Position Exit: {reason}", "CRITICAL")
        
        if self.gtt_ids:
            logger.info(f"Cancelling {len(self.gtt_ids)} GTT orders...")
            executor = ExecutionEngine(self.api_client, self.db_writer, self.circuit_breaker)
            
            for gtt_id in self.gtt_ids:
                cancelled = False
                for _ in range(3):
                    if executor.cancel_gtt_order(gtt_id):
                        cancelled = True
                        break
                    time.sleep(0.5)
                
                if not cancelled:
                    msg = f"❌ FATAL: Could not cancel GTT {gtt_id}. Manual intervention required."
                    logger.critical(msg)
                    self.telegram.send(msg, "CRITICAL")
        
        executor = ExecutionEngine(self.api_client, self.db_writer, self.circuit_breaker)
        atomic_success = False
        for attempt in range(2):
            logger.info(f"Atomic exit attempt {attempt+1}...")
            if executor.exit_all_positions(tag="VG30"):
                atomic_success = True
                logger.info("✅ Atomic exit successful")
                break
            time.sleep(2)
            
        if not atomic_success:
            logger.critical("Atomic exit failed - falling back to leg-by-leg")
            self.telegram.send("Atomic exit failed - manual closure initiated", "CRITICAL")
            executor._flatten_legs(self.legs)
            
        final_pnl = self._get_final_pnl()
        self.db_writer.update_trade_exit(self.trade_id, reason, final_pnl)
        self.db_writer.log_risk_event("POSITION_EXIT", "INFO", reason, f"P&L: ₹{final_pnl:.2f}")
        self.circuit_breaker.record_trade_result(final_pnl)
        
        self.telegram.send(
            f"Position Closed\nReason: {reason}\nFinal P&L: ₹{final_pnl:,.2f}",
            "SUCCESS" if final_pnl > 0 else "WARNING"
        )
        self.running = False
        logger.info(f"Risk monitor shutdown complete for {self.trade_id}")

    def _get_final_pnl(self) -> float:
        try:
            portfolio_api = PortfolioApi(self.api_client)
            response = portfolio_api.get_positions(api_version="2.0")
            if response.status == 'success' and response.data:
                return sum(float(p.pnl) for p in response.data)
            return 0.0
        except Exception:
            return 0.0
