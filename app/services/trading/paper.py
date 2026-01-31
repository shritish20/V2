"""
Paper Trading Engine - Preserved
"""
import time
import logging
import threading
import numpy as np
from typing import Optional, Dict, List

from app.core.config import settings

logger = logging.getLogger("VOLGUARD")

class PaperTradingEngine:
    def __init__(self):
        self.paper_positions = {}
        self.paper_orders = {}
        self.order_counter = 0
        self.lock = threading.Lock()

    def place_order(self, instrument_key: str, qty: int, side: str, order_type: str, price: float) -> Optional[str]:
        with self.lock:
            self.order_counter += 1
            order_id = f"PAPER_{int(time.time())}_{self.order_counter}"

            if np.random.random() > settings.DRY_RUN_FILL_PROBABILITY:
                logger.info(f"📄 PAPER ORDER REJECTED (simulated): {order_id}")
                self.paper_orders[order_id] = {
                    'status': 'rejected',
                    'filled_qty': 0,
                    'avg_price': 0
                }
                return order_id

            slippage = np.random.normal(
                settings.DRY_RUN_SLIPPAGE_MEAN,
                settings.DRY_RUN_SLIPPAGE_STD
            )
            fill_price = price * (1 + slippage) if side == 'BUY' else price * (1 - slippage)
            fill_price = round(fill_price, 1)

            self.paper_orders[order_id] = {
                'status': 'complete',
                'filled_qty': qty,
                'avg_price': fill_price,
                'instrument_key': instrument_key,
                'side': side
            }

            pos_key = f"{instrument_key}_{side}"
            if pos_key not in self.paper_positions:
                self.paper_positions[pos_key] = {
                    'qty': 0,
                    'avg_price': 0,
                    'instrument_key': instrument_key,
                    'side': side
                }

            pos = self.paper_positions[pos_key]
            pos['qty'] += qty
            pos['avg_price'] = fill_price

            logger.info(f"📄 PAPER ORDER FILLED: {side} {qty}x {instrument_key} @ {fill_price} (slippage: {slippage*100:.2f}%)")
            return order_id

    def get_order_status(self, order_id: str) -> Optional[Dict]:
        with self.lock:
            return self.paper_orders.get(order_id)

    def cancel_order(self, order_id: str) -> bool:
        with self.lock:
            if order_id in self.paper_orders:
                self.paper_orders[order_id]['status'] = 'cancelled'
                return True
            return False

    def get_positions(self) -> List[Dict]:
        with self.lock:
            return list(self.paper_positions.values())

    def clear_position(self, instrument_key: str, side: str):
        with self.lock:
            pos_key = f"{instrument_key}_{side}"
            if pos_key in self.paper_positions:
                del self.paper_positions[pos_key]
