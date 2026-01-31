"""
Telegram Alerter - Preserved exactly
"""
import time
import threading
import logging
import requests
from app.core.config import settings

logger = logging.getLogger("VOLGUARD")

class TelegramAlerter:
    def __init__(self):
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.rate_limit_lock = threading.Lock()
        self.last_send_time = 0
        self.min_interval = 1.0

    def send(self, message: str, level: str = "INFO", retry: int = 3):
        if not self.bot_token or not self.chat_id:
            return False
            
        emoji_map = {
            "CRITICAL": "🚨", "ERROR": "❌", "WARNING": "⚠️",
            "INFO": "ℹ️", "SUCCESS": "✅", "TRADE": "💰", "SYSTEM": "⚙️"
        }
        prefix = emoji_map.get(level, "📢")
        full_msg = f"{prefix} *VOLGUARD 3.3*\n{message}"

        with self.rate_limit_lock:
            elapsed = time.time() - self.last_send_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)

        for attempt in range(retry):
            try:
                response = requests.post(
                    f"{self.base_url}/sendMessage",
                    json={"chat_id": self.chat_id, "text": full_msg, "parse_mode": "Markdown"},
                    timeout=5
                )
                if response.status_code == 200:
                    with self.rate_limit_lock:
                        self.last_send_time = time.time()
                    return True
            except Exception as e:
                logger.error(f"Telegram send error (attempt {attempt+1}/{retry}): {e}")
                if attempt < retry - 1:
                    time.sleep(2 ** attempt)
        logger.error(f"Failed to send Telegram alert after {retry} attempts: {message}")
        return False
