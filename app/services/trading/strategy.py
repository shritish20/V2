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
        pe_legs = sorted([l
