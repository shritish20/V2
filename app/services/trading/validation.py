"""
Instrument Validator - Preserved
"""
import time
import logging
import requests
import upstox_client
from app.core.config import settings

logger = logging.getLogger("VOLGUARD")

class InstrumentValidator:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.ban_list_cache = set()
        self.cache_time = 0
        self.cache_ttl = 3600

    def is_instrument_banned(self, instrument_key: str) -> bool:
        if settings.DRY_RUN_MODE:
            return False
        try:
            if time.time() - self.cache_time > self.cache_ttl:
                self._refresh_ban_list()
            return instrument_key in self.ban_list_cache
        except Exception as e:
            logger.error(f"Ban list check failed: {e}")
            return False

    def _refresh_ban_list(self):
        try:
            url = "https://www.nseindia.com/api/fo-ban-securities"
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.ban_list_cache = set(data.get('data', []))
                self.cache_time = time.time()
                logger.info(f"Ban list refreshed: {len(self.ban_list_cache)} instruments")
        except Exception as e:
            logger.warning(f"Failed to refresh ban list: {e}")

    def validate_price(self, current_price: float, previous_price: float) -> bool:
        if previous_price <= 0:
            return True
        change_pct = abs(current_price - previous_price) / previous_price
        if change_pct > settings.PRICE_CHANGE_THRESHOLD:
            logger.error(f"Price changed {change_pct*100:.1f}% - exceeds threshold")
            return False
        return True

    def validate_lot_size(self, instrument_key: str, expected_lot_size: int) -> bool:
        if settings.DRY_RUN_MODE:
            return True
        try:
            options_api = upstox_client.OptionsApi(self.api_client)
            response = options_api.get_option_contracts(instrument_key=settings.NIFTY_KEY)
            if response.status == 'success' and response.data:
                actual_lot_size = next((int(c.lot_size) for c in response.data if hasattr(c, 'lot_size')), 0)
                if actual_lot_size != expected_lot_size:
                    logger.error(f"Lot size mismatch: Expected {expected_lot_size}, Got {actual_lot_size}")
                    from app.infrastructure.messaging import TelegramAlerter
                    TelegramAlerter().send(f"⚠️ Lot size changed: {expected_lot_size} → {actual_lot_size}", "WARNING")
                    return False
            return True
        except Exception as e:
            logger.error(f"Lot size validation failed: {e}")
            return True

    def validate_contract_exists(self, instrument_key: str) -> bool:
        if settings.DRY_RUN_MODE:
            return True
        try:
            market_api = upstox_client.MarketQuoteV3Api(self.api_client)
            response = market_api.get_ltp(instrument_key=instrument_key)
            return response.status == 'success' and response.data
        except Exception as e:
            logger.error(f"Contract validation failed: {e}")
            return False
