"""
Strategy Factory - Preserved
"""
import logging
from typing import Optional, Dict, List, Tuple
import pandas as pd
import numpy as np

from app.core.config import settings
from app.models.domain import TradingMandate, VolMetrics, StructMetrics

logger = logging.getLogger("VOLGUARD")

class StrategyFactory:
    def __init__(self, api_client):
        self.api_client = api_client

    def _discover_strike_interval(self, df: pd.DataFrame) -> int:
        if df.empty or len(df) < 2:
            return settings.DEFAULT_STRIKE_INTERVAL
        strikes = sorted(df['strike'].unique())
        diffs = np.diff(strikes)
        valid_diffs = diffs[diffs > 0]
        if len(valid_diffs) == 0:
            return settings.DEFAULT_STRIKE_INTERVAL
        try:
            return int(pd.Series(valid_diffs).mode().iloc[0])
        except:
            return settings.DEFAULT_STRIKE_INTERVAL

    def _find_professional_atm(self, df: pd.DataFrame, spot: float) -> Optional[Dict]:
        interval = self._discover_strike_interval(df)
        closest = int(spot / interval + 0.5) * interval
        candidates = [closest, closest + interval, closest - interval]
        best_strike, min_skew, best_cost = None, float('inf'), 0.0
        for strike in candidates:
            ce = df[(df['strike'] == strike) & (df['ce_oi'] > settings.MIN_STRIKE_OI)]
            pe = df[(df['strike'] == strike) & (df['pe_oi'] > settings.MIN_STRIKE_OI)]
            if ce.empty or pe.empty: 
                continue
            ce_ltp, pe_ltp = ce.iloc[0]['ce_ltp'], pe.iloc[0]['pe_ltp']
            if ce_ltp <= 0.1 or pe_ltp <= 0.1: 
                continue
            skew = abs(ce_ltp - pe_ltp)
            if skew < min_skew:
                min_skew, best_strike, best_cost = skew, strike, ce_ltp + pe_ltp
        if not best_strike:
            logger.warning(f"Using Geometric ATM {closest} (Liquidity Low)")
            return {'strike': closest, 'straddle_cost': 0.0, 'interval': interval}
        logger.info(f"🎯 Pro ATM: {best_strike} (Skew: ₹{min_skew:.1f}) | Interval: {interval}")
        return {'strike': best_strike, 'straddle_cost': best_cost, 'interval': interval}

    def _calculate_pro_wing_width(self, straddle_cost: float, vol_metrics: VolMetrics, interval: int) -> int:
        if vol_metrics.ivp_1yr > settings.IVP_THRESHOLD_EXTREME:
            factor = settings.WING_FACTOR_EXTREME_VOL
        elif vol_metrics.ivp_1yr > settings.IVP_THRESHOLD_HIGH:
            factor = settings.WING_FACTOR_HIGH_VOL
        elif vol_metrics.ivp_1yr < settings.IVP_THRESHOLD_LOW:
            factor = settings.WING_FACTOR_LOW_VOL
        else:
            factor = settings.WING_FACTOR_STANDARD
        target = straddle_cost * factor
        rounded = int(target / interval + 0.5) * interval
        min_width = interval * settings.MIN_WING_INTERVAL_MULTIPLIER
        final = max(min_width, rounded)
        logger.info(f"📏 Wing Width: Target {target:.1f} -> Rounded {final} (Factor {factor})")
        return final

    def _get_leg_details(self, df: pd.DataFrame, strike: float, type_: str) -> Optional[Dict]:
        rows = df[(df['strike'] - strike).abs() < 0.1]
        if rows.empty: 
            return None
        row = rows.iloc[0]
        pref = type_.lower()
        ltp = row[f'{pref}_ltp']
        if ltp <= 0: 
            return None
        return {
            'key': row[f'{pref}_key'],
            'strike': row['strike'],
            'ltp': ltp,
            'delta': row[f'{pref}_delta'],
            'type': type_,
            'bid': row[f'{pref}_bid'],
            'ask': row[f'{pref}_ask']
        }

    def _find_leg_by_delta(self, df: pd.DataFrame, type_: str, target_delta: float) -> Optional[Dict]:
        target, col_delta = abs(target_delta), f"{type_.lower()}_delta"
        df = df.copy()
        df = df[(df[f'{type_.lower()}_oi'] > settings.MIN_STRIKE_OI) & (df[f'{type_.lower()}_ltp'] > 0.5)]
        df['delta_diff'] = (df[col_delta].abs() - target).abs()
        for _, row in df.sort_values('delta_diff').head(3).iterrows():
            bid, ask, ltp = row[f'{type_.lower()}_bid'], row[f'{type_.lower()}_ask'], row[f'{type_.lower()}_ltp']
            if ltp <= 0 or ask <= 0: 
                continue
            if (ask - bid) / ltp > settings.MAX_BID_ASK_SPREAD: 
                continue
            return self._get_leg_details(df, row['strike'], type_)
        return None

        def _calculate_defined_risk(self, legs: List[Dict], qty: int) -> float:
        if not legs: 
            return 0.0
        
        premiums = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
        debits = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'BUY')
        net_credit = premiums - debits
        
        ce_legs = sorted([l for l in legs if l['type'] == 'CE'], key=lambda x: x['strike'])
        pe_legs = sorted([l for l in legs if l['type'] == 'PE'], key=lambda x: x['strike'])
        
        call_risk = 0.0
        put_risk = 0.0
        
        if len(ce_legs) >= 2:
            shorts = [l for l in ce_legs if l['side'] == 'SELL']
            longs = [l for l in ce_legs if l['side'] == 'BUY']
            if shorts and longs:
                width = longs[-1]['strike'] - shorts[0]['strike']
                call_risk = width * qty

        if len(pe_legs) >= 2:
            shorts = [l for l in pe_legs if l['side'] == 'SELL']
            longs = [l for l in pe_legs if l['side'] == 'BUY']
            if shorts and longs:
                width = shorts[-1]['strike'] - longs[0]['strike']
                put_risk = width * qty
        
        max_structural_risk = max(call_risk, put_risk)
        max_loss = max(0, max_structural_risk - net_credit)
        
        logger.info(f"🧮 Risk Calc: CallRisk={call_risk:.0f}, PutRisk={put_risk:.0f}, Credit={net_credit:.0f} -> MaxLoss={max_loss:.0f}")
        return max_loss

    def generate(self, mandate: TradingMandate, chain: pd.DataFrame, lot_size: int, vol_metrics: VolMetrics, spot: float, struct_metrics: StructMetrics) -> Tuple[List[Dict], float]:
        if mandate.max_lots == 0 or chain.empty: 
            return [], 0.0
        qty = mandate.max_lots * lot_size
        legs = []

        # 1. IRON FLY
        if mandate.suggested_structure == "IRON_FLY":
            logger.info(f"🦅 Constructing Iron Fly | DTE={mandate.dte} | Spot={spot:.2f}")
            atm_data = self._find_professional_atm(chain, spot)
            if not atm_data: 
                return [], 0.0
            atm_strike, straddle_cost, interval = atm_data['strike'], atm_data['straddle_cost'], atm_data['interval']
            wing_width = self._calculate_pro_wing_width(straddle_cost, vol_metrics, interval)
            upper_wing, lower_wing = atm_strike + wing_width, atm_strike - wing_width
            atm_call = self._get_leg_details(chain, atm_strike, 'CE')
            atm_put = self._get_leg_details(chain, atm_strike, 'PE')
            wing_call = self._get_leg_details(chain, upper_wing, 'CE')
            wing_put = self._get_leg_details(chain, lower_wing, 'PE')
            if not all([atm_call, atm_put, wing_call, wing_put]):
                logger.error("Iron Fly incomplete: Missing liquid strikes")
                return [], 0.0
            legs = [
                {**atm_call, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'IRON_FLY'},
                {**atm_put, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'IRON_FLY'},
                {**wing_call,'side': 'BUY', 'role': 'HEDGE','qty': qty, 'structure': 'IRON_FLY'},
                {**wing_put, 'side': 'BUY', 'role': 'HEDGE','qty': qty, 'structure': 'IRON_FLY'}
            ]

        # 2. IRON CONDOR
        elif mandate.suggested_structure == "IRON_CONDOR":
            logger.info(f"🦅 Constructing Iron Condor | DTE={mandate.dte}")
            short_delta = settings.DELTA_SHORT_MONTHLY if mandate.expiry_type == "MONTHLY" else settings.DELTA_SHORT_WEEKLY
            legs = [
                self._find_leg_by_delta(chain, 'CE', short_delta),
                self._find_leg_by_delta(chain, 'PE', short_delta),
                self._find_leg_by_delta(chain, 'CE', settings.DELTA_LONG_HEDGE),
                self._find_leg_by_delta(chain, 'PE', settings.DELTA_LONG_HEDGE)
            ]
            if not all(legs): 
                logger.error("Iron Condor incomplete")
                return [], 0.0
            legs = [{**l, 'side': 'SELL' if idx < 2 else 'BUY', 'role': 'CORE' if idx < 2 else 'HEDGE', 'qty': qty, 'structure': 'IRON_CONDOR'} for idx, l in enumerate(legs)]

        # 3. DIRECTIONAL CREDIT SPREAD
        elif mandate.suggested_structure in ["CREDIT_SPREAD", "BULL_PUT_SPREAD", "BEAR_CALL_SPREAD"]:
            is_uptrend = vol_metrics.spot > vol_metrics.ma20 * (1 + settings.TREND_BULLISH_THRESHOLD/100)
            is_bullish_pcr = struct_metrics.pcr > settings.PCR_BULLISH_THRESHOLD
            
            if is_uptrend or mandate.directional_bias in ["BULLISH", "MILDLY_BULLISH"]:
                logger.info("📈 Direction: BULLISH. Deploying BULL PUT SPREAD.")
                short = self._find_leg_by_delta(chain, 'PE', settings.DELTA_CREDIT_SHORT)
                long = self._find_leg_by_delta(chain, 'PE', settings.DELTA_CREDIT_LONG)
                if not all([short, long]): 
                    return [], 0.0
                legs = [
                    {**short, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'BULL_PUT_SPREAD'},
                    {**long, 'side': 'BUY', 'role': 'HEDGE','qty': qty, 'structure': 'BULL_PUT_SPREAD'}
                ]
            else:
                logger.info("📉 Direction: BEARISH. Deploying BEAR CALL SPREAD.")
                short = self._find_leg_by_delta(chain, 'CE', settings.DELTA_CREDIT_SHORT)
                long = self._find_leg_by_delta(chain, 'CE', settings.DELTA_CREDIT_LONG)
                if not all([short, long]): 
                    return [], 0.0
                legs = [
                    {**short, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'BEAR_CALL_SPREAD'},
                    {**long, 'side': 'BUY', 'role': 'HEDGE','qty': qty, 'structure': 'BEAR_CALL_SPREAD'}
                ]

        if not legs: 
            return [], 0.0
        for leg in legs:
            if leg['ltp'] <= 0:
                logger.error(f"❌ Invalid Leg Price: {leg['strike']} = {leg['ltp']}")
                return [], 0.0

        max_risk = self._calculate_defined_risk(legs, qty)
        if max_risk > settings.MAX_LOSS_PER_TRADE:
            logger.critical(f"⛔ Trade Rejected: Max Risk ₹{max_risk:,.2f} > Limit ₹{settings.MAX_LOSS_PER_TRADE:,.2f}")
            from app.infrastructure.messaging import TelegramAlerter
            TelegramAlerter().send(f"Trade Rejected: Risk ₹{max_risk:,.0f} exceeds limit", "WARNING")
            return [], 0.0
        return legs, max_risk
