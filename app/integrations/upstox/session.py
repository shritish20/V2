"""
Session Manager - Preserved
"""
import time
import logging
import upstox_client
from app.core.config import settings

logger = logging.getLogger("VOLGUARD")

class SessionManager:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.login_api = upstox_client.LoginApi(self.api_client)
        self.last_validation = 0
        self.validation_interval = 3600

    def validate_session(self, force: bool = False) -> bool:
        if not force and (time.time() - self.last_validation) < self.validation_interval:
            return True
        try:
            user_api = upstox_client.UserApi(self.api_client)
            response = user_api.get_profile(api_version="2.0")
            if response.status == 'success':
                self.last_validation = time.time()
                logger.debug("Session validated successfully")
                return True
            elif response.status == 'error':
                logger.warning("Session invalid - attempting refresh")
                return self._refresh_token()
            return False
        except Exception as e:
            logger.error(f"Session validation error: {e}")
            return self._refresh_token()

    def _refresh_token(self) -> bool:
        if not settings.UPSTOX_REFRESH_TOKEN:
            logger.critical("No refresh token configured - cannot refresh session")
            return False
        try:
            token_request = upstox_client.TokenRequest(
                client_id=settings.UPSTOX_CLIENT_ID,
                client_secret=settings.UPSTOX_CLIENT_SECRET,
                redirect_uri=settings.UPSTOX_REDIRECT_URI,
                grant_type='refresh_token',
                code=settings.UPSTOX_REFRESH_TOKEN
            )
            response = self.login_api.token(token_request)
            if response.status == 'success' and response.data and hasattr(response.data, 'access_token'):
                new_token = response.data.access_token
                self.api_client.configuration.access_token = new_token
                self.last_validation = time.time()
                logger.info("✅ Token refreshed successfully")
                from app.infrastructure.messaging import TelegramAlerter
                TelegramAlerter().send("Access token refreshed", "SYSTEM")
                return True
            logger.critical(f"Token refresh failed: {response}")
            from app.infrastructure.messaging import TelegramAlerter
            TelegramAlerter().send("Token refresh failed - manual intervention required", "CRITICAL")
            return False
        except Exception as e:
            logger.critical(f"Token refresh exception: {e}")
            from app.infrastructure.messaging import TelegramAlerter
            TelegramAlerter().send(f"Token refresh error: {str(e)}", "CRITICAL")
            return False

    def check_market_status(self) -> bool:
        try:
            market_api = upstox_client.MarketHolidaysAndTimingsApi(self.api_client)
            status_response = market_api.get_market_status(exchange='NFO')
            if status_response.status == 'success' and status_response.data:
                market_status = status_response.data.status.upper() if hasattr(status_response.data, 'status') else 'UNKNOWN'
                if market_status == 'OPEN':
                    return True
                logger.info(f"Market status: {market_status}")
            
            from datetime import date
            today_str = date.today().strftime("%Y-%m-%d")
            holiday_response = market_api.get_holiday(today_str)
            if holiday_response.status == 'success' and holiday_response.data:
                holidays = holiday_response.data if isinstance(holiday_response.data, list) else [holiday_response.data]
                for holiday in holidays:
                    if hasattr(holiday, 'holiday_type') and holiday.holiday_type == 'TRADING_HOLIDAY':
                        logger.info(f"Market Closed: {getattr(holiday, 'description', 'Holiday')}")
                        return False
            
            from datetime import datetime
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 15:
                logger.warning("Could not determine market status - assuming open during market hours")
                return True
            return False
        except Exception as e:
            logger.error(f"Market status check failed: {e}")
            from datetime import datetime
            current_hour = datetime.now().hour
            return 9 <= current_hour <= 15
