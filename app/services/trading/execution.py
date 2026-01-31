"""
Execution Engine - Preserved
"""
import os
import time
import threading
import traceback
import concurrent.futures
import logging
from typing import Optional, Dict, List

import upstox_client
from upstox_client.rest import ApiException
from upstox_client import OrderApiV3
from upstox_client.api.charge_api import ChargeApi
from upstox_client.api.order_api import OrderApi
from upstox_client.api.portfolio_api import PortfolioApi

from app.core.config import settings
from app.infrastructure.database import DatabaseWriter
from app.infrastructure.circuit_breaker import CircuitBreaker
from app.infrastructure.messaging import TelegramAlerter
from app.infrastructure.metrics import (
    record_order_fill, record_slippage, update_margin_pct, 
    order_timeout_counter
)
from app.services.trading.validation import InstrumentValidator
from app.services.trading.paper import PaperTradingEngine

logger = logging.getLogger("VOLGUARD")

class ExecutionEngine:
    def __init__(self, api_client: upstox_client.ApiClient, db_writer: DatabaseWriter, circuit_breaker: CircuitBreaker):
        self.api_client = api_client
        self.order_updates = {}
        self.update_lock = threading.Lock()
        self.price_cache = {}
        self.price_cache_lock = threading.Lock()
        self.websocket_connected = False
        self.validator = InstrumentValidator(api_client)
        self.db_writer = db_writer
        self.circuit_breaker = circuit_breaker
        self.telegram = TelegramAlerter()
        
        if not settings.DRY_RUN_MODE:
            self._setup_portfolio_stream()
        else:
            logger.info("📄 Dry run mode - skipping WebSocket setup")
            self.paper_engine = PaperTradingEngine()

    def _setup_portfolio_stream(self):
        try:
            self.portfolio_streamer = upstox_client.PortfolioDataStreamer(
                self.api_client,
                order_update=True,
                position_update=True,
                holding_update=False,
                gtt_update=True
            )

            def on_message(message):
                with self.update_lock:
                    if 'order_updates' in message:
                        for update in message['order_updates']:
                            order_id = update.get('order_id')
                            if order_id:
                                self.order_updates[order_id] = update
                                logger.debug(f"WebSocket order update: {order_id} -> {update.get('status')}")

            def on_open():
                self.websocket_connected = True
                logger.info("✅ Portfolio Stream Connected")

            def on_error(error):
                self.websocket_connected = False
                logger.error(f"Portfolio Stream Error: {error}")

            def on_close():
                self.websocket_connected = False
                logger.warning("Portfolio Stream Closed")

            self.portfolio_streamer.on("message", on_message)
            self.portfolio_streamer.on("open", on_open)
            self.portfolio_streamer.on("error", on_error)
            self.portfolio_streamer.on("close", on_close)
            self.portfolio_streamer.auto_reconnect(True, 10, 5)
            threading.Thread(target=self.portfolio_streamer.connect, daemon=True, name="Portfolio-WS").start()
            time.sleep(2)
        except Exception as e:
            logger.error(f"Failed to setup portfolio stream: {e}")
            self.websocket_connected = False

    def check_margin_requirement(self, legs: List[Dict]) -> float:
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                charge_api = ChargeApi(self.api_client)
                instruments = []
                for leg in legs:
                    instruments.append(upstox_client.Instrument(
                        instrument_key=leg['key'],
                        quantity=int(leg['qty']),
                        transaction_type=leg['side'],
                        product="D"
                    ))
                margin_request = upstox_client.MarginRequest(instruments=instruments)
                response = charge_api.post_margin(margin_request)
                if response.status == 'success' and hasattr(response.data, 'required_margin'):
                    margin = float(response.data.required_margin)
                    logger.info(f"Margin requirement: ₹{margin:,.2f}")
                    return margin
                logger.warning(f"Margin check attempt {attempt+1} failed: {response}")
            except Exception as e:
                logger.error(f"Margin check error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(2 ** attempt)
        logger.error("Margin check failed after all retries")
        return float('inf')

    def get_funds(self) -> float:
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                user_api = upstox_client.UserApi(self.api_client)
                response = user_api.get_user_fund_margin(api_version="2.0")
                if response.status != 'success' or not response.data:
                    logger.error("Funds API returned no data")
                    continue
                data = response.data
                if hasattr(data, 'equity') and hasattr(data.equity, 'available_margin'):
                    funds = float(data.equity.available_margin)
                    logger.info(f"Available funds: ₹{funds:,.2f}")
                    return funds
            except Exception as e:
                logger.error(f"Funds fetch error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(2 ** attempt)
        logger.error("Funds fetch failed after all retries")
        return 0.0

    def place_order(self, instrument_key: str, qty: int, side: str, order_type: str = "LIMIT", price: float = 0.0) -> Optional[str]:
        if qty <= 0 or price < 0:
            logger.error(f"Invalid order parameters: qty={qty}, price={price}")
            return None
        if settings.DRY_RUN_MODE:
            return self.paper_engine.place_order(instrument_key, qty, side, order_type, price)
        if not self.validator.validate_contract_exists(instrument_key):
            logger.error(f"Contract validation failed: {instrument_key}")
            return None
        if self.validator.is_instrument_banned(instrument_key):
            logger.error(f"Instrument is banned: {instrument_key}")
            self.telegram.send(f"⛔ Attempted trade on banned instrument: {instrument_key}", "ERROR")
            return None
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                body = upstox_client.PlaceOrderV3Request(
                    quantity=int(qty),
                    product="D",
                    validity="DAY",
                    price=float(price),
                    tag="VG30",
                    instrument_token=instrument_key,
                    order_type=order_type,
                    transaction_type=side,
                    disclosed_quantity=0,
                    trigger_price=0.0,
                    is_amo=False,
                    slice=True
                )
                response = order_api.place_order(body)
                if response.status == 'success' and response.data and hasattr(response.data, 'order_ids') and response.data.order_ids:
                    order_id = response.data.order_ids[0]
                    logger.info(f"ORDER PLACED: {side} {qty}x {instrument_key} @ {price} | ID={order_id}")
                    self.db_writer.log_order(order_id, instrument_key, side, qty, price, "PLACED")
                    return order_id
                else:
                    logger.warning(f"Order placement attempt {attempt+1} failed: {response}")
            except Exception as e:
                logger.error(f"Order placement error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(1)
        logger.error(f"Order placement failed after {settings.MAX_API_RETRIES} attempts")
        self.db_writer.log_order("FAILED", instrument_key, side, qty, price, "FAILED", message="All retries exhausted")
        return None

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        if settings.DRY_RUN_MODE:
            return self.paper_engine.get_order_status(order_id)
        with self.update_lock:
            if order_id in self.order_updates:
                update = self.order_updates[order_id]
                return {
                    'status': update.get('status', '').lower(),
                    'avg_price': float(update.get('average_price', 0)),
                    'filled_qty': int(update.get('filled_quantity', 0))
                }
        try:
            order_api = OrderApi(self.api_client)
            response = order_api.get_order_details(api_version="2.0", order_id=order_id)
            if response.status != 'success' or not response.data:
                return None
            order_data = response.data
            return {
                'status': order_data.status.lower() if hasattr(order_data, 'status') else 'unknown',
                'avg_price': float(order_data.average_price) if hasattr(order_data, 'average_price') and order_data.average_price else 0.0,
                'filled_qty': int(order_data.filled_quantity) if hasattr(order_data, 'filled_quantity') and order_data.filled_quantity else 0
            }
        except Exception as e:
            logger.error(f"Order status check failed: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        if settings.DRY_RUN_MODE:
            return self.paper_engine.cancel_order(order_id)
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                order_api.cancel_order(order_id=order_id)
                logger.info(f"ORDER CANCELLED: {order_id}")
                self.db_writer.log_order(order_id, "", "", 0, 0, "CANCELLED")
                return True
            except Exception as e:
                logger.error(f"Cancel order error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(0.5)
        return False

    def place_gtt_order(self, instrument_key: str, qty: int, side: str, stop_loss_price: float, target_price: float) -> Optional[str]:
        if qty <= 0 or stop_loss_price <= 0 or target_price <= 0:
            logger.error(f"Invalid GTT parameters")
            return None
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                sl_trigger = "BELOW" if side == "BUY" else "ABOVE"
                rules = [
                    upstox_client.GttRule(strategy="STOPLOSS", trigger_type=sl_trigger, trigger_price=float(stop_loss_price)),
                    upstox_client.GttRule(strategy="TARGET", trigger_type="IMMEDIATE", trigger_price=float(target_price))
                ]
                body = upstox_client.GttPlaceOrderRequest(
                    type="MULTIPLE",
                    quantity=int(qty),
                    product="D",
                    rules=rules,
                    instrument_token=instrument_key,
                    transaction_type=side
                )
                response = order_api.place_gtt_order(body)
                if response.status == 'success' and response.data and hasattr(response.data, 'gtt_order_ids') and response.data.gtt_order_ids:
                    gtt_id = response.data.gtt_order_ids[0]
                    logger.info(f"GTT PLACED: {side} {qty}x {instrument_key} | SL={stop_loss_price} Target={target_price} | ID={gtt_id}")
                    return gtt_id
            except Exception as e:
                logger.error(f"GTT placement error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(1)
        logger.error("GTT placement failed after all retries")
        return None

    def get_gtt_order_details(self, gtt_id: str) -> Optional[str]:
        try:
            order_api = OrderApiV3(self.api_client)
            response = order_api.get_gtt_order_details(gtt_order_id=gtt_id)
            if response.status == 'success' and response.data:
                data = response.data[0] if isinstance(response.data, list) else response.data
                return data.status if hasattr(data, 'status') else None
            return None
        except Exception as e:
            logger.error(f"GTT check failed: {e}")
            return None

    def cancel_gtt_order(self, gtt_id: str) -> bool:
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                order_api.cancel_gtt_order(gtt_order_id=gtt_id)
                logger.info(f"GTT CANCELLED: {gtt_id}")
                return True
            except Exception as e:
                logger.error(f"GTT cancel error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(0.5)
        return False

    def get_brokerage_impact(self, legs: List[Dict]) -> float:
        try:
            charge_api = ChargeApi(self.api_client)
            total_brokerage = 0.0
            for leg in legs:
                response = charge_api.get_brokerage(
                    leg['key'],
                    leg['qty'],
                    'D',
                    leg['side'],
                    leg['ltp'],
                    "2.0"
                )
                if response.status == 'success' and response.data and hasattr(response.data, 'charges'):
                    total_brokerage += float(response.data.charges.total)
            logger.info(f"Estimated brokerage: ₹{total_brokerage:.2f}")
            return total_brokerage
        except Exception as e:
            logger.error(f"Brokerage calculation error: {e}")
            return 0.0

    def exit_all_positions(self, tag: Optional[str] = None) -> bool:
        for attempt in range(settings.MAX_API_RETRIES):
            try:
                order_api = OrderApi(self.api_client)
                response = order_api.exit_positions()
                if response.status == 'success':
                    logger.critical("🚨 ATOMIC EXIT EXECUTED")
                    self.telegram.send("🚨 Server-side atomic exit completed", "CRITICAL")
                    return True
                else:
                    logger.warning(f"Atomic exit attempt {attempt+1} failed: {response}")
            except Exception as e:
                logger.error(f"Atomic exit error (attempt {attempt+1}): {e}")
                if attempt < settings.MAX_API_RETRIES - 1:
                    time.sleep(1)
        logger.error("Atomic exit failed after all retries")
        return False

    def verify_gtt(self, gtt_ids: List[str]) -> bool:
        try:
            for gtt_id in gtt_ids:
                status = self.get_gtt_order_details(gtt_id)
                if status != 'active':
                    logger.warning(f"GTT {gtt_id} status: {status}")
                    self.telegram.send(f"GTT verification failed: {gtt_id} is {status}", "WARNING")
                    return False
            logger.info(f"✅ All GTTs verified: {len(gtt_ids)} orders active")
            return True
        except Exception as e:
            logger.error(f"GTT verification failed: {e}")
            return False

    def _execute_leg_atomic(self, leg):
        tolerance = 0.998 if leg['role'] == 'HEDGE' else (1.002 if leg['side'] == 'BUY' else 0.998)
        limit_price = round(leg['ltp'] * tolerance, 1)
        expected_price = leg['ltp']
        logger.info(f"PLACING {leg['side']} {leg['strike']} {leg['type']} @ {limit_price} (Role: {leg['role']})")
        order_id = self.place_order(leg['key'], leg['qty'], leg['side'], "LIMIT", limit_price)
        if not order_id:
            return None
        start = time.time()
        last_status = None
        while (time.time() - start) < settings.ORDER_TIMEOUT:
            status = self.get_order_status(order_id)
            if not status:
                time.sleep(0.2)
                continue
            if status['status'] != last_status:
                logger.debug(f"Order {order_id}: {status['status']}")
                last_status = status['status']
            if status['status'] == 'complete':
                fill_threshold = settings.HEDGE_FILL_TOLERANCE if leg['role'] == 'HEDGE' else settings.PARTIAL_FILL_TOLERANCE
                if status['filled_qty'] < leg['qty'] * fill_threshold:
                    logger.critical(f"PARTIAL FILL: {status['filled_qty']}/{leg['qty']} for {leg['role']}")
                    self.cancel_order(order_id)
                    self.db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "PARTIAL_REJECTED", 
                                           filled_qty=status['filled_qty'], message=f"Below {fill_threshold*100:.0f}% threshold")
                    return None
                actual_price = status['avg_price']
                slippage = abs(actual_price - expected_price) / expected_price if expected_price > 0 else 0
                if slippage > settings.SLIPPAGE_TOLERANCE:
                    logger.warning(f"SLIPPAGE: {slippage*100:.2f}% on {leg['key']}")
                    self.circuit_breaker.record_slippage_event(slippage)
                leg['entry_price'] = actual_price
                leg['filled_qty'] = status['filled_qty']
                leg['slippage'] = slippage
                self.db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "FILLED", 
                                       filled_qty=status['filled_qty'], avg_price=actual_price)
                record_order_fill(leg, time.time() - start)
                record_slippage(leg)
                logger.info(f"✅ FILLED: {leg['side']} {status['filled_qty']}x {leg['strike']} {leg['type']} @ {actual_price}")
                return leg
            elif status['status'] in ['rejected', 'cancelled']:
                logger.error(f"ORDER DEAD: {status['status']}")
                self.db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, status['status'].upper())
                return None
            time.sleep(0.2)
        logger.warning(f"TIMEOUT on {order_id}. Attempting cancel...")
        self.cancel_order(order_id)
        time.sleep(1)
        final_status = self.get_order_status(order_id)
        if final_status and final_status['status'] == 'complete':
            leg['entry_price'] = final_status['avg_price']
            leg['filled_qty'] = final_status['filled_qty']
            logger.info(f"Order filled during cancel: {final_status}")
            return leg
        order_timeout_counter.labels(side=leg['side'], role=leg['role']).inc()
        self.db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "TIMEOUT")
        return None

    def execute_strategy(self, legs: List[Dict]) -> List[Dict]:
        total_qty = sum(l['qty'] for l in legs)
        if total_qty > settings.MAX_CONTRACTS_PER_INSTRUMENT:
            logger.critical(f"Position size {total_qty} exceeds limit {settings.MAX_CONTRACTS_PER_INSTRUMENT}")
            self.telegram.send(f"Position size violation: {total_qty} contracts", "ERROR")
            return []
        if len(legs) >= 4:
            strikes = sorted([l['strike'] for l in legs])
            max_spread_width = max(strikes) - min(strikes)
            premium = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
            max_loss = (max_spread_width - premium) * legs[0]['qty']
            if max_loss > settings.MAX_LOSS_PER_TRADE:
                logger.critical(f"Max loss ₹{max_loss:,.0f} exceeds limit ₹{settings.MAX_LOSS_PER_TRADE:,.0f}")
                self.telegram.send(f"Max loss violation: ₹{max_loss:,.0f}", "ERROR")
                return []
        brokerage_cost = 0.0
        if not settings.DRY_RUN_MODE:
            required_margin = self.check_margin_requirement(legs)
            available_funds = self.get_funds()
            usable_funds = available_funds * (1 - settings.MARGIN_BUFFER)
            if required_margin > usable_funds:
                logger.critical(f"Margin ERROR: Need ₹{required_margin:,.2f}, Have ₹{usable_funds:,.2f} (with buffer)")
                self.telegram.send(f"Margin Shortfall: Need {required_margin/100000:.2f}L, Have {usable_funds/100000:.2f}L", "ERROR")
                return []
            projected_premium = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
            brokerage_cost = self.get_brokerage_impact(legs)
            if projected_premium > 0 and (projected_premium - brokerage_cost) < (projected_premium * 0.05):
                logger.critical(f"BROKERAGE TOO HIGH: Cost=₹{brokerage_cost:.2f}, Premium=₹{projected_premium:.2f}")
                self.telegram.send(f"Brokerage kills profit: ₹{brokerage_cost:.2f} on ₹{projected_premium:.2f} premium", "ERROR")
                return []
            update_margin_pct(required_margin, available_funds)
        else:
            logger.info("📄 Dry run - skipping margin and brokerage checks")
        if not self.validator.validate_lot_size(settings.NIFTY_KEY, legs[0]['qty'] // 25 if legs else 25):
            logger.error("Lot size validation failed - aborting")
            return []
        hedges = [l for l in legs if l['role'] == 'HEDGE']
        cores = [l for l in legs if l['role'] == 'CORE']
        logger.info(f"📋 Execution Plan: {len(hedges)} Hedges → {len(cores)} Cores {'[DRY RUN]' if settings.DRY_RUN_MODE else ''}")
        hedge_results = []
        if hedges:
            logger.info(f"Executing {len(hedges)} Hedges in Parallel...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(hedges), thread_name_prefix="Hedge-Exec") as executor:
                future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in hedges}
                for future in concurrent.futures.as_completed(future_to_leg):
                    result = future.result()
                    if result:
                        hedge_results.append(result)
                    else:
                        logger.critical("HEDGE EXECUTION FAILED - ABORTING STRATEGY")
                        if hedge_results:
                            logger.warning(f"Flattening {len(hedge_results)} filled hedges")
                            self._flatten_legs(hedge_results)
                        return []
            if len(hedge_results) != len(hedges):
                logger.critical(f"INCOMPLETE HEDGES: {len(hedge_results)}/{len(hedges)} - ABORTING")
                self._flatten_legs(hedge_results)
                return []
        logger.info(f"✅ All {len(hedge_results)} Hedges Filled Successfully")
        core_results = []
        if cores:
            logger.info(f"Executing {len(cores)} Cores in Parallel...")
                        with concurrent.futures.ThreadPoolExecutor(max_workers=len(cores), thread_name_prefix="Core-Exec") as executor:
                future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in cores}
                for future in concurrent.futures.as_completed(future_to_leg):
                    result = future.result()
                    if result:
                        core_results.append(result)
                    else:
                        logger.critical("CORE EXECUTION FAILED - FLATTENING ALL")
                        self._flatten_legs(hedge_results + core_results)
                        return []
            if len(core_results) != len(cores):
                logger.critical(f"INCOMPLETE CORES: {len(core_results)}/{len(cores)} - FLATTENING ALL")
                self._flatten_legs(hedge_results + core_results)
                return []
        executed = hedge_results + core_results
        structure = executed[0].get('structure', 'UNKNOWN') if executed else 'UNKNOWN'
        actual_premium = sum(l['entry_price'] * l['filled_qty'] for l in executed if l['side'] == 'SELL')
        actual_debit = sum(l['entry_price'] * l['filled_qty'] for l in executed if l['side'] == 'BUY')
        net_premium = actual_premium - actual_debit
        self.db_writer.update_daily_stats(trades=1)
        mode_indicator = "📄 PAPER" if settings.DRY_RUN_MODE else "💰 LIVE"
        logger.info(f"✅ {mode_indicator} STRATEGY DEPLOYED: {structure} | Net Premium: ₹{net_premium:,.2f}")
        self.telegram.send(
            f"{mode_indicator} Position Opened\n"
            f"Structure: {structure}\n"
            f"Legs: {len(executed)}\n"
            f"Net Premium: ₹{net_premium:,.2f}\n"
            f"Brokerage: ₹{brokerage_cost:.2f}",
            "TRADE"
        )
        return executed

    def _flatten_legs(self, legs: List[Dict]):
        if not legs:
            return
        logger.critical(f"🚨 EMERGENCY FLATTEN: {len(legs)} legs")
        self.telegram.send(f"Emergency flattening {len(legs)} legs", "CRITICAL")
        for leg in legs:
            if leg.get('filled_qty', 0) <= 0:
                continue
            exit_side = 'SELL' if leg['side'] == 'BUY' else 'BUY'
            success = False
            for attempt in range(2):
                try:
                    oid = self.place_order(leg['key'], leg['filled_qty'], exit_side, "MARKET", 0.0)
                    if oid:
                        time.sleep(1)
                        status = self.get_order_status(oid)
                        if status and status['status'] == 'complete':
                            logger.info(f"✅ Market exit: {leg['key']}")
                            success = True
                            break
                except Exception as e:
                    logger.error(f"Market exit failed: {e}")
            if success:
                continue
            for attempt in range(3):
                try:
                    exit_price = leg.get('current_ltp', leg['entry_price'])
                    exit_price = exit_price * 1.10 if exit_side == 'BUY' else exit_price * 0.90
                    oid = self.place_order(leg['key'], leg['filled_qty'], exit_side, "LIMIT", round(exit_price, 1))
                    if oid:
                        time.sleep(2)
                        status = self.get_order_status(oid)
                        if status and status['status'] == 'complete':
                            logger.info(f"✅ Limit exit: {leg['key']}")
                            success = True
                            break
                        elif status and status['status'] != 'complete':
                            self.cancel_order(oid)
                except Exception as e:
                    logger.error(f"Limit exit attempt {attempt+1} failed: {e}")
                time.sleep(1)
            if not success:
                msg = f"❌ CRITICAL: FAILED TO CLOSE {leg['key']} - MANUAL INTERVENTION REQUIRED"
                logger.critical(msg)
                self.telegram.send(msg, "CRITICAL")
                self.db_writer.log_risk_event("FAILED_EXIT", "CRITICAL", f"Could not close {leg['key']}", "MANUAL_ACTION_REQUIRED")
