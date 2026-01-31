"""
Core Configuration - Converted from ProductionConfig class
"""
import os
import pytz
from datetime import time as dtime
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # Environment
    ENVIRONMENT: str = Field(default="PRODUCTION", alias="VG_ENV")
    DRY_RUN_MODE: bool = Field(default=True, alias="VG_DRY_RUN")
    
    # Upstox Credentials
    UPSTOX_ACCESS_TOKEN: Optional[str] = None
    UPSTOX_REFRESH_TOKEN: Optional[str] = None
    UPSTOX_CLIENT_ID: Optional[str] = None
    UPSTOX_CLIENT_SECRET: Optional[str] = None
    UPSTOX_REDIRECT_URI: Optional[str] = None
    
    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None
    
    # AI
    GROQ_API_KEY: Optional[str] = None
    
    # Trading Parameters
    BASE_CAPITAL: int = Field(default=1000000, alias="VG_BASE_CAPITAL")
    MARGIN_SELL_BASE: int = 125000
    MARGIN_BUY_BASE: int = 30000
    MAX_CAPITAL_USAGE: float = 0.80
    DAILY_LOSS_LIMIT: float = 0.03
    MAX_POSITION_SIZE: float = 0.25
    MAX_LOSS_PER_TRADE: int = Field(default=50000, alias="VG_MAX_LOSS_PER_TRADE")
    MAX_CAPITAL_PER_TRADE: int = Field(default=300000, alias="VG_MAX_CAPITAL_PER_TRADE")
    MAX_TRADES_PER_DAY: int = Field(default=3, alias="MAX_TRADES_PER_DAY")
    MAX_DRAWDOWN_PCT: float = Field(default=0.15, alias="VG_MAX_DRAWDOWN_PCT")
    MAX_CONTRACTS_PER_INSTRUMENT: int = 1800
    PRICE_CHANGE_THRESHOLD: float = 0.10
    
    # Strategy Parameters
    IRON_FLY_MIN_WING_WIDTH: int = 100
    IRON_FLY_MAX_WING_WIDTH: int = 400
    IRON_FLY_WING_DELTA_TARGET: float = 0.10
    IRON_FLY_ATM_TOLERANCE: float = 0.02
    GAMMA_DANGER_DTE: int = 1
    GEX_STICKY_RATIO: float = 0.03
    HIGH_VOL_IVP: float = 75.0
    LOW_VOL_IVP: float = 25.0
    VOV_CRASH_ZSCORE: float = 2.5
    VOV_WARNING_ZSCORE: float = 2.0
    
    # Weights
    WEIGHT_VOL: float = 0.40
    WEIGHT_STRUCT: float = 0.30
    WEIGHT_EDGE: float = 0.20
    WEIGHT_RISK: float = 0.10
    
    # FII Thresholds
    FII_STRONG_LONG: int = 50000
    FII_STRONG_SHORT: int = -50000
    FII_MODERATE: int = 20000
    FII_VERY_HIGH_CONVICTION: int = 150000
    FII_HIGH_CONVICTION: int = 80000
    FII_MODERATE_CONVICTION: int = 40000
    
    # Exit Parameters
    TARGET_PROFIT_PCT: float = 0.50
    STOP_LOSS_PCT: float = 1.0
    MAX_SHORT_DELTA: float = 0.20
    EXIT_DTE: int = 1
    SLIPPAGE_TOLERANCE: float = 0.02
    PARTIAL_FILL_TOLERANCE: float = 0.95
    HEDGE_FILL_TOLERANCE: float = 0.98
    ORDER_TIMEOUT: int = 10
    MAX_BID_ASK_SPREAD: float = 0.05
    
    # Intervals
    POLL_INTERVAL: float = 0.5
    ANALYSIS_INTERVAL: int = 1800
    MAX_API_RETRIES: int = 3
    DASHBOARD_REFRESH_RATE: float = 1.0
    PRICE_STALENESS_THRESHOLD: int = 5
    
    # Paths
    DB_PATH: str = Field(default="/app/data/volguard.db", alias="VG_DB_PATH")
    LOG_DIR: str = Field(default="/app/logs", alias="VG_LOG_DIR")
    KILL_SWITCH_FILE: str = Field(default="/app/data/KILL_SWITCH", alias="VG_KILL_SWITCH_FILE")
    
    # Market Hours
    MARKET_OPEN: tuple = (9, 15)
    MARKET_CLOSE: tuple = (15, 30)
    SAFE_ENTRY_START: tuple = (9, 0)
    SAFE_EXIT_END: tuple = (15, 15)
    SQUARE_OFF_TIME_IST: dtime = dtime(14, 0)
    
    # Risk Management
    MAX_CONSECUTIVE_LOSSES: int = 3
    COOL_DOWN_PERIOD: int = 86400
    MAX_SLIPPAGE_EVENTS_PER_DAY: int = 5
    ANALYTICS_PROCESS_TIMEOUT: int = 300
    DB_WRITER_QUEUE_MAX_SIZE: int = 10000
    HEARTBEAT_INTERVAL: int = 30
    WEBSOCKET_RECONNECT_DELAY: int = 5
    MAX_ZOMBIE_PROCESSES: int = 3
    MARGIN_BUFFER: float = 0.20
    
    # Paper Trading
    DRY_RUN_SLIPPAGE_MEAN: float = 0.001
    DRY_RUN_SLIPPAGE_STD: float = 0.0005
    DRY_RUN_FILL_PROBABILITY: float = 0.95
    
    # Strike Selection
    DEFAULT_STRIKE_INTERVAL: int = 50
    MIN_STRIKE_OI: int = 1000
    WING_FACTOR_EXTREME_VOL: float = 1.4
    WING_FACTOR_HIGH_VOL: float = 1.1
    WING_FACTOR_LOW_VOL: float = 0.8
    WING_FACTOR_STANDARD: float = 1.0
    IVP_THRESHOLD_EXTREME: float = 80.0
    IVP_THRESHOLD_HIGH: float = 50.0
    IVP_THRESHOLD_LOW: float = 20.0
    MIN_WING_INTERVAL_MULTIPLIER: int = 2
    DELTA_SHORT_WEEKLY: float = 0.20
    DELTA_SHORT_MONTHLY: float = 0.16
    DELTA_LONG_HEDGE: float = 0.05
    DELTA_CREDIT_SHORT: float = 0.30
    DELTA_CREDIT_LONG: float = 0.10
    TREND_BULLISH_THRESHOLD: float = 0.0
    PCR_BULLISH_THRESHOLD: float = 1.0
    
    # Event Risk
    VETO_KEYWORDS: list = [
        "RBI Monetary Policy", "RBI Policy", "Reserve Bank of India",
        "Repo Rate Decision", "MPC Meeting",
        "FOMC", "Federal Reserve Meeting", "Fed Meeting",
        "Federal Funds Rate Decision"
    ]
    HIGH_IMPACT_KEYWORDS: list = [
        "GDP", "Gross Domestic Product",
        "NFP", "Non-Farm Payroll",
        "CPI", "Consumer Price Index",
        "Union Budget", "Budget Speech"
    ]
    MEDIUM_IMPACT_KEYWORDS: list = [
        "PMI", "Manufacturing PMI", "Services PMI",
        "Industrial Production",
        "Retail Sales"
    ]
    EVENT_RISK_DAYS_AHEAD: int = 7
    VIX_MOMENTUM_BREAKOUT: float = 5.0
    SKEW_CRASH_FEAR: float = 3.0
    SKEW_MELT_UP: float = -1.0
    
    # Live Greeks
    GREEKS_WS_RECONNECT_DELAY: int = 1
    GREEKS_WS_MAX_RECONNECT_DELAY: int = 60
    GREEKS_STALE_THRESHOLD: int = 60
    MAX_PORTFOLIO_DELTA: float = 50.0
    THETA_VEGA_RATIO_CRITICAL: float = 1.0
    THETA_VEGA_RATIO_WARNING: float = 2.0
    MAX_POSITION_GAMMA: float = 100.0
    
    # URLs
    ECONOMIC_CALENDAR_URL: str = "https://economic-calendar.tradingview.com/events"
    
    # Instrument Keys
    NIFTY_KEY: str = "NSE_INDEX|Nifty 50"
    VIX_KEY: str = "NSE_INDEX|India VIX"
    
    # Timezone
    IST: pytz.timezone = pytz.timezone('Asia/Kolkata')
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
