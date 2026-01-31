"""
Live Greeks Manager - Converted from singleton to class-based
"""
import json
import time
import threading
import logging
from typing import Optional, Dict, List
from dataclasses import dataclass

import upstox_client
from app.infrastructure.metrics import PROM_REGISTRY, Gauge, update_greeks

logger = logging.getLogger("VOLGUARD")

@dataclass
class GreeksData:
    delta: float = 0.0
    theta: float = 0.0
    gamma: float = 0.0
    vega: float = 0.0
    rho: float = 0.0
    iv: float = 0.0
    ltp: float = 0.0
    oi: float = 0.0
    timestamp: float = 0.0

class LiveGreeksManager:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.ws = None
        self.subscribed_keys = set()
        self.greeks_cache = {}
        self.lock = threading.RLock()
        self.running = False
        self.thread = None
        self.connected = False
        self.message_count = 0
        self.last_message_time = time.time()
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        
        try:
            self.theta_vega_ratio_gauge = Gauge(
                "volguard_theta_vega_ratio_normalized", 
                "Portfolio Theta/Vega ratio (normalized by 1000)",
                ["trade_id"],
                registry=PROM_REGISTRY
            )
            self.ws_connection_status = Gauge(
                "volguard_greeks_ws_connected",
                "WebSocket connection status (1=connected)",
                registry=PROM_REGISTRY
            )
        except Exception as e:
            logger.warning(f"Prometheus metrics registration skipped: {e}")
            self.theta_vega_ratio_gauge = None
            self.ws_connection_status = None

    def start(self):
        if self.running:
            logger.debug("LiveGreeksManager already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._ws_loop, daemon=True, name="LiveGreeks-WS")
        self.thread.start()
        logger.info("📡 LiveGreeks Manager started")

    def stop(self):
        logger.info("Shutting down LiveGreeks Manager...")
        self.running = False
        
        if self.ws:
            try:
                self.ws.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
            
        if self.ws_connection_status:
            self.ws_connection_status.set(0)

    def _ws_loop(self):
        while self.running:
            try:
                self._connect()
                self.reconnect_delay = 1
                
                while self.running and self.connected:
                    time.sleep(5)
                    if time.time() - self.last_message_time > 30:
                        logger.warning("WebSocket stale (no data for 30s), forcing reconnect")
                        break
                        
            except Exception as e:
                logger.error(f"WebSocket loop error: {e}")
                self.connected = False
                if self.ws_connection_status:
                    self.ws_connection_status.set(0)
            
            if self.running:
                logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                time.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)

    def _connect(self):
        with self.lock:
            keys = list(self.subscribed_keys) if self.subscribed_keys else []
        
        if not keys:
            logger.warning("No instruments to subscribe, skipping connection")
            return
        
        logger.info(f"🔌 Connecting to Option Greeks WebSocket ({len(keys)} instruments)")
        
        try:
            self.ws = upstox_client.MarketDataStreamerV3(
                self.api_client, 
                keys, 
                "option_greeks"
            )
            
            self.ws.on("open", self._on_open)
            self.ws.on("message", self._on_message)
            self.ws.on("error", self._on_error)
            self.ws.on("close", self._on_close)
            
            self.ws.connect()
            
            while self.running and self.connected:
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self.connected = False
            if self.ws_connection_status:
                self.ws_connection_status.set(0)
            raise

    def _on_open(self):
        logger.info("✅ Live Greeks WebSocket connected")
        self.connected = True
        self.last_message_time = time.time()
        if self.ws_connection_status:
            self.ws_connection_status.set(1)

    def _on_message(self, msg):
        self.last_message_time = time.time()
        
        try:
            if isinstance(msg, str):
                data = json.loads(msg)
            else:
                data = msg
            
            if not isinstance(data, dict) or 'feeds' not in data:
                return
            
            self.message_count += 1
            
            feeds = data.get('feeds', {})
            for instrument_key, feed_data in feeds.items():
                self._process_instrument(instrument_key, feed_data)
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
        except Exception as e:
            logger.debug(f"Message processing error: {e}")

    def _process_instrument(self, instrument_key: str, feed_data: dict):
        try:
            first_level = feed_data.get('firstLevelWithGreeks', {})
            ltpc = first_level.get('ltpc', {})
            ltp = float(ltpc.get('ltp', 0) or 0)
            
            greeks_dict = first_level.get('optionGreeks', {})
            
            data = GreeksData()
            data.ltp = ltp
            data.delta = float(greeks_dict.get('delta', 0) or 0)
            data.theta = float(greeks_dict.get('theta', 0) or 0)
            data.gamma = float(greeks_dict.get('gamma', 0) or 0)
            data.vega = float(greeks_dict.get('vega', 0) or 0)
            data.rho = float(greeks_dict.get('rho', 0) or 0)
            data.iv = float(first_level.get('iv', 0) or 0)
            data.oi = float(first_level.get('oi', 0) or 0)
            data.timestamp = time.time()
            
            with self.lock:
                self.greeks_cache[instrument_key] = data
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Data conversion error for {instrument_key}: {e}")
        except Exception as e:
            logger.debug(f"Parse error for {instrument_key}: {e}")

    def _on_error(self, error):
        logger.error(f"WebSocket error: {error}")
        from app.infrastructure.messaging import TelegramAlerter
        TelegramAlerter().send(f"⚠️ Greeks WebSocket error: {str(error)[:100]}", "WARNING")
        self.connected = False
        if self.ws_connection_status:
            self.ws_connection_status.set(0)

    def _on_close(self):
        if self.connected:
            logger.warning("Live Greeks WebSocket disconnected")
        self.connected = False
        if self.ws_connection_status:
            self.ws_connection_status.set(0)

    def update_subscriptions(self, instrument_keys: List[str]):
        new_keys = set(instrument_keys)
        
        with self.lock:
            if new_keys == self.subscribed_keys:
                return
            self.subscribed_keys = new_keys
        
        logger.info(f"Updated Greek subscriptions: {len(new_keys)} instruments")
        
        if self.ws and self.connected:
            logger.debug("Forcing reconnect to update subscriptions")
            try:
                self.ws.disconnect()
            except Exception as e:
                logger.error(f"Error forcing disconnect: {e}")

    def get_position_greeks(self, instrument_key: str) -> Optional[GreeksData]:
        with self.lock:
            data = self.greeks_cache.get(instrument_key)
            if data and (time.time() - data.timestamp) < 60:
                return data
            return None

    def calculate_position_greeks(self, leg: Dict, trade_id: str) -> Dict[str, float]:
        live = self.get_position_greeks(leg['key'])
        if not live:
            return {}
        
        qty = leg.get('filled_qty', leg.get('qty', 0))
        side_mult = -1 if leg['side'] == 'SELL' else 1
        
        notional = {
            'delta': live.delta * qty * side_mult,
            'theta': live.theta * qty * side_mult,
            'gamma': live.gamma * qty * side_mult,
            'vega': live.vega * qty * side_mult,
            'iv': live.iv,
            'ltp': live.ltp,
            'oi': live.oi
        }
        
        if abs(notional['vega']) > 0.0001:
            raw_ratio = abs(notional['theta']) / abs(notional['vega'])
            notional['theta_vega_ratio'] = raw_ratio / 1000.0
        else:
            notional['theta_vega_ratio'] = 0.0
            
        return notional

    def get_portfolio_greeks(self, legs: List[Dict], trade_id: str) -> Dict[str, float]:
        portfolio = {
            'delta': 0.0,
            'theta': 0.0,
            'gamma': 0.0,
            'vega': 0.0,
            'theta_vega_ratio': 0.0,
            'short_vega_exposure': 0.0,
            'legs_count': len(legs),
            'stale_count': 0
        }
        
        for leg in legs:
            pos = self.calculate_position_greeks(leg, trade_id)
            if not pos:
                portfolio['stale_count'] += 1
                continue
            
            portfolio['delta'] += pos['delta']
            portfolio['theta'] += pos['theta']
            portfolio['gamma'] += pos['gamma']
            portfolio['vega'] += pos['vega']
            
            if leg['side'] == 'SELL' and pos['vega'] > 0:
                portfolio['short_vega_exposure'] += pos['vega']
        
        if abs(portfolio['vega']) > 0.0001:
            raw_ratio = abs(portfolio['theta']) / abs(portfolio['vega'])
            portfolio['theta_vega_ratio'] = raw_ratio / 1000.0
            
            if self.theta_vega_ratio_gauge:
                try:
                    self.theta_vega_ratio_gauge.labels(trade_id=trade_id).set(portfolio['theta_vega_ratio'])
                except Exception as e:
                    logger.debug(f"Prometheus update error: {e}")
        
        try:
            update_greeks(
                portfolio['delta'],
                portfolio['theta'],
                portfolio['gamma'],
                portfolio['vega']
            )
        except Exception as e:
            logger.debug(f"Global metrics update error: {e}")
        
        return portfolio

    def check_risk_limits(self, legs: List[Dict], trade_id: str) -> List[str]:
        warnings = []
        port = self.get_portfolio_greeks(legs, trade_id)
        
        from app.core.config import settings
        
        if port['stale_count'] > len(legs) / 2:
            warnings.append(f"⚠️ CRITICAL: {port['stale_count']}/{len(legs)} legs have stale Greeks data")
        
        ratio = port['theta_vega_ratio']
        if ratio > 0:
            if ratio < settings.THETA_VEGA_RATIO_CRITICAL:
                msg = f"🔴 CRITICAL θ/ν Ratio: {ratio:.2f} (Volatility risk exceeds time income)"
                warnings.append(msg)
                from app.infrastructure.messaging import TelegramAlerter
                TelegramAlerter().send(f"🚨 {trade_id}: {msg}", "CRITICAL")
            elif ratio < settings.THETA_VEGA_RATIO_WARNING:
                warnings.append(f"🟡 LOW θ/ν Ratio: {ratio:.2f} (Monitor volatility closely)")
            elif ratio > 5.0:
                warnings.append(f"🟢 STRONG θ/ν Ratio: {ratio:.2f} (Excellent time decay)")
            else:
                warnings.append(f"✅ NORMAL θ/ν Ratio: {ratio:.2f}")
        
        if abs(port['delta']) > settings.MAX_PORTFOLIO_DELTA:
            warnings.append(f"🔴 DELTA ALERT: {port['delta']:.1f} (Limit: ±{settings.MAX_PORTFOLIO_DELTA})")
        
        if legs:
            from datetime import date
            expiry = legs[0].get('expiry', date.today())
            if isinstance(expiry, str):
                expiry = __import__('datetime').datetime.strptime(expiry, "%Y-%m-%d").date()
            days_to_expiry = (expiry - date.today()).days
            
            if days_to_expiry <= 2 and abs(port['gamma']) > settings.MAX_POSITION_GAMMA:
                msg = f"🔴 GAMMA DANGER: {port['gamma']:.1f} with {days_to_expiry} DTE (Gamma Week)"
                warnings.append(msg)
                from app.infrastructure.messaging import TelegramAlerter
                TelegramAlerter().send(msg, "CRITICAL")
        
        return warnings
