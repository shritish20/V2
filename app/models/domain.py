"""
All dataclasses from the original codebase
Preserved exactly as they were
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from datetime import date, datetime, time as dtime

@dataclass
class TimeMetrics:
    current_date: date
    weekly_exp: Optional[date]
    monthly_exp: Optional[date]
    next_weekly_exp: Optional[date]
    dte_weekly: int
    dte_monthly: int
    dte_next_weekly: int = 0
    is_expiry_day_weekly: bool = False
    is_expiry_day_monthly: bool = False
    is_past_square_off_time: bool = False
    is_gamma_week: bool = False
    is_gamma_month: bool = False
    days_to_next_weekly: int = 0

@dataclass
class VolMetrics:
    spot: float = 0.0
    vix: float = 0.0
    rv7: float = 0.0
    rv28: float = 0.0
    rv90: float = 0.0
    garch7: float = 0.0
    garch28: float = 0.0
    park7: float = 0.0
    park28: float = 0.0
    vov: float = 0.0
    vov_zscore: float = 0.0
    ivp_30d: float = 0.0
    ivp_90d: float = 0.0
    ivp_1yr: float = 0.0
    ma20: float = 0.0
    atr14: float = 0.0
    trend_strength: float = 0.0
    vol_regime: str = "UNKNOWN"
    is_fallback: bool = False
    vix_change_5d: float = 0.0
    vix_momentum: str = "UNKNOWN"

@dataclass
class StructMetrics:
    net_gex: float = 0.0
    gex_ratio: float = 0.0
    total_oi_value: float = 0.0
    gex_regime: str = "NEUTRAL"
    pcr: float = 0.0
    max_pain: float = 0.0
    skew_25d: float = 0.0
    oi_regime: str = "NEUTRAL"
    lot_size: int = 50
    pcr_atm: float = 1.0
    skew_regime: str = "UNKNOWN"
    gex_weighted: float = 5.0

@dataclass
class EdgeMetrics:
    iv_weekly: float = 0.0
    vrp_rv_weekly: float = 0.0
    vrp_garch_weekly: float = 0.0
    vrp_park_weekly: float = 0.0
    iv_monthly: float = 0.0
    vrp_rv_monthly: float = 0.0
    vrp_garch_monthly: float = 0.0
    vrp_park_monthly: float = 0.0
    iv_next_weekly: float = 0.0
    vrp_rv_next_weekly: float = 0.0
    vrp_garch_next_weekly: float = 0.0
    vrp_park_next_weekly: float = 0.0
    term_spread: float = 0.0
    term_regime: str = "FLAT"
    primary_edge: str = "NONE"
    weighted_vrp_weekly: float = 0.0
    weighted_vrp_monthly: float = 0.0
    weighted_vrp_next_weekly: float = 0.0

@dataclass
class ParticipantData:
    fut_long: float = 0.0
    fut_short: float = 0.0
    fut_net: float = 0.0
    call_long: float = 0.0
    call_short: float = 0.0
    call_net: float = 0.0
    put_long: float = 0.0
    put_short: float = 0.0
    put_net: float = 0.0
    stock_net: float = 0.0

@dataclass
class EconomicEvent:
    title: str = ""
    country: str = ""
    event_date: Optional[datetime] = None
    impact_level: str = "LOW"
    event_type: str = "LOW_IMPACT"
    forecast: str = ""
    previous: str = ""
    days_until: int = 0
    hours_until: float = 0.0
    is_veto_event: bool = False
    suggested_square_off_time: Optional[datetime] = None

@dataclass
class DynamicWeights:
    vol_weight: float = 0.4
    struct_weight: float = 0.3
    edge_weight: float = 0.3
    rationale: str = ""

@dataclass
class RegimeScore:
    vol_score: float = 0.0
    struct_score: float = 0.0
    edge_score: float = 0.0
    composite: float = 0.0
    confidence: str = "LOW"
    score_stability: float = 0.0
    weights_used: DynamicWeights = field(default_factory=DynamicWeights)
    score_drivers: List[str] = field(default_factory=list)

@dataclass
class TradingMandate:
    expiry_type: str = ""
    expiry_date: Optional[date] = None
    dte: int = 0
    regime_name: str = ""
    strategy_type: str = ""
    allocation_pct: float = 0.0
    deployment_amount: float = 0.0
    score: RegimeScore = field(default_factory=RegimeScore)
    rationale: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    veto_reasons: List[str] = field(default_factory=list)
    suggested_structure: str = ""
    directional_bias: str = "NEUTRAL"
    wing_protection: str = "STANDARD"
    is_trade_allowed: bool = True
    data_relevance: str = "FRESH"
    square_off_instruction: Optional[str] = None
    max_lots: int = 0
    risk_per_lot: float = 0.0

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

@dataclass
class ExternalMetrics:
    fii: Optional[ParticipantData] = None
    dii: Optional[ParticipantData] = None
    pro: Optional[ParticipantData] = None
    client: Optional[ParticipantData] = None
    fii_net_change: float = 0.0
    flow_regime: str = "NEUTRAL"
    flow_score: float = 0.0
    option_bias: float = 0.0
    data_date: str = ""
    is_fallback_data: bool = False
    fast_vol: bool = False
    fii_conviction: str = "NEUTRAL"
    fii_direction: str = "NEUTRAL"
    economic_events: List[EconomicEvent] = field(default_factory=list)
    veto_events: List[EconomicEvent] = field(default_factory=list)
    high_impact_events: List[EconomicEvent] = field(default_factory=list)
    veto_square_off_needed: bool = False
    veto_square_off_time: Optional[datetime] = None
    veto_hours_until: Optional[float] = None
    event_risk: str = "LOW"
    has_veto_event: bool = False
    veto_event_name: Optional[str] = None
    upcoming_high_impact: List[EconomicEvent] = field(default_factory=list)

@dataclass
class TradeRecord:
    trade_id: str = ""
    entry_date: str = ""
    entry_time: str = ""
    expiry_date: str = ""
    dte_at_entry: int = 0
    structure: str = ""
    strikes: Dict = field(default_factory=dict)
    lots: int = 0
    entry_premium: float = 0.0
    regime_name: str = ""
    regime_score: float = 0.0
    regime_confidence: str = ""
    vol_score: float = 0.0
    struct_score: float = 0.0
    edge_score: float = 0.0
    risk_score: float = 0.0
    spot_at_entry: float = 0.0
    vix_at_entry: float = 0.0
    ivp_at_entry: float = 0.0
    weighted_vrp_at_entry: float = 0.0
    gex_regime: str = ""
    skew_regime: str = ""
    fii_flow: str = ""

@dataclass
class MarketRegime:
    risk_score: int = 5
    nifty_view: str = "FLAT"
    strategy: str = "IRON_CONDOR"
    reasoning: str = ""
