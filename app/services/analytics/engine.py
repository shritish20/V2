"""
Analytics Engine - Preserved
"""
import io
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple
from multiprocessing import Queue

import requests
import pandas as pd
import numpy as np
import pytz
from scipy.stats import norm
from arch import arch_model

import upstox_client
from upstox_client.api.history_v3_api import HistoryV3Api
from upstox_client.api.options_api import OptionsApi
from upstox_client.api.market_quote_api import MarketQuoteV3Api

from app.core.config import settings
from app.models.domain import (
    TimeMetrics, VolMetrics, StructMetrics, EdgeMetrics, 
    ExternalMetrics, ParticipantData
)
from app.services.intelligence.calendar import CalendarEngine

logger = logging.getLogger("VOLGUARD")

class AnalyticsEngine:
    def __init__(self, result_queue: Queue):
        self.result_queue = result_queue

    def run(self, config: Dict):
        try:
            api_client = upstox_client.ApiClient()
            api_client.configuration.access_token = config['access_token']
            history_api = HistoryV3Api(api_client)
            options_api = OptionsApi(api_client)
            
            to_date = date.today().strftime("%Y-%m-%d")
            from_date = (date.today() - timedelta(days=400)).strftime("%Y-%m-%d")
            
            nifty_response = history_api.get_historical_candle_data1(settings.NIFTY_KEY, "days", "1", to_date, from_date)
            vix_response = history_api.get_historical_candle_data1(settings.VIX_KEY, "days", "1", to_date, from_date)
            
            nifty_hist = self._parse_candle_response(nifty_response)
            vix_hist = self._parse_candle_response(vix_response)
            
            market_api = MarketQuoteV3Api(api_client)
            live_prices = market_api.get_ltp(instrument_key=f"{settings.NIFTY_KEY},{settings.VIX_KEY}")
            
            weekly, monthly, next_weekly, lot_size = self._get_expiries(options_api)
            weekly_chain = self._get_option_chain(options_api, weekly) if weekly else pd.DataFrame()
            monthly_chain = self._get_option_chain(options_api, monthly) if monthly else pd.DataFrame()
            next_weekly_chain = self._get_option_chain(options_api, next_weekly) if next_weekly else pd.DataFrame()
            
            calendar_engine = CalendarEngine()
            economic_events = calendar_engine.fetch_calendar(settings.EVENT_RISK_DAYS_AHEAD)
            
            participant_data, participant_yest, fii_net_change, data_date = self._fetch_participant_data()
            time_metrics = self.get_time_metrics(weekly, monthly, next_weekly)
            vol_metrics = self.get_vol_metrics(nifty_hist, vix_hist, live_prices)
            struct_metrics_weekly = self.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
            struct_metrics_monthly = self.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
            edge_metrics = self.get_edge_metrics(weekly_chain, monthly_chain, next_weekly_chain, vol_metrics.spot, vol_metrics, time_metrics)
            external_metrics = self.get_external_metrics(nifty_hist, participant_data, participant_yest, fii_net_change, data_date, economic_events)

            result = {
                'timestamp': datetime.now(),
                'time_metrics': time_metrics,
                'vol_metrics': vol_metrics,
                'weekly_chain': weekly_chain,
                'monthly_chain': monthly_chain,
                'next_weekly_chain': next_weekly_chain,
                'lot_size': lot_size,
                'participant_data': participant_data,
                'participant_yest': participant_yest,
                'fii_net_change': fii_net_change,
                'data_date': data_date,
                'external_metrics': external_metrics,
                'edge_metrics': edge_metrics,
                'struct_metrics_weekly': struct_metrics_weekly,
                'struct_metrics_monthly': struct_metrics_monthly
            }
            self.result_queue.put(('success', result))
        except Exception as e:
            logger.error(f"Analytics process error: {e}")
            import traceback
            traceback.print_exc()
            self.result_queue.put(('error', str(e)))

    def _parse_candle_response(self, response):
        if response.status != 'success':
            return pd.DataFrame()
        candles = response.data.candles if hasattr(response.data, 'candles') else []
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        return df.astype(float).sort_index()

    def _get_expiries(self, options_api: OptionsApi) -> Tuple[Optional[date], Optional[date], Optional[date], int]:
        try:
            response = options_api.get_option_contracts(instrument_key=settings.NIFTY_KEY)
            if response.status != 'success':
                return None, None, None, 0
            data = response.data
            if not data:
                return None, None, None, 0
            lot_size = next((int(c.lot_size) for c in data if hasattr(c, 'lot_size')), 0)
            expiry_dates = sorted(list(set([
                (c.expiry.date() if hasattr(c.expiry, 'date') else 
                 datetime.strptime(str(c.expiry).split('T')[0], "%Y-%m-%d").date())
                for c in data if hasattr(c, 'expiry') and c.expiry
            ])))
            valid_dates = [d for d in expiry_dates if d >= date.today()]
            if not valid_dates:
                return None, None, None, lot_size
            weekly = valid_dates[0]
            next_weekly = valid_dates[1] if len(valid_dates) > 1 else valid_dates[0]
            current_month = date.today().month
            current_year = date.today().year
            monthly_candidates = [d for d in valid_dates if d.month == current_month and d.year == current_year]
            if not monthly_candidates or monthly_candidates[-1] < date.today():
                next_month = current_month + 1 if current_month < 12 else 1
                next_year = current_year if current_month < 12 else current_year + 1
                monthly_candidates = [d for d in valid_dates if d.month == next_month and d.year == next_year]
            monthly = monthly_candidates[-1] if monthly_candidates else valid_dates[-1]
            return weekly, monthly, next_weekly, lot_size
        except Exception as e:
            logger.error(f"Expiries fetch error: {e}")
            return None, None, None, 0

    def _get_option_chain(self, options_api: OptionsApi, expiry_date: date) -> pd.DataFrame:
        try:
            response = options_api.get_put_call_option_chain(
                instrument_key=settings.NIFTY_KEY,
                expiry_date=expiry_date.strftime("%Y-%m-%d")
            )
            if response.status != 'success':
                return pd.DataFrame()
            data = response.data
            return pd.DataFrame([{
                'strike': x.strike_price,
                'ce_iv': x.call_options.option_greeks.iv,
                'pe_iv': x.put_options.option_greeks.iv,
                'ce_delta': x.call_options.option_greeks.delta,
                'pe_delta': x.put_options.option_greeks.delta,
                'ce_gamma': x.call_options.option_greeks.gamma,
                'pe_gamma': x.put_options.option_greeks.gamma,
                'ce_oi': x.call_options.market_data.oi,
                'pe_oi': x.put_options.market_data.oi,
                'ce_ltp': x.call_options.market_data.ltp,
                'pe_ltp': x.put_options.market_data.ltp,
                'ce_bid': getattr(x.call_options.market_data, 'bid_price', 0),
                'ce_ask': getattr(x.call_options.market_data, 'ask_price', 0),
                'pe_bid': getattr(x.put_options.market_data, 'bid_price', 0),
                'pe_ask': getattr(x.put_options.market_data, 'ask_price', 0),
                'ce_key': x.call_options.instrument_key,
                'pe_key': x.put_options.instrument_key
            } for x in data])
        except Exception as e:
            logger.error(f"Option chain fetch error: {e}")
            return pd.DataFrame()

    def _fetch_participant_data(self):
        tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(tz)
        dates = []
        candidate = now
        if candidate.hour < 18:
            candidate -= timedelta(days=1)
        while len(dates) < 2:
            if candidate.weekday() < 5:
                dates.append(candidate)
            candidate -= timedelta(days=1)
        today, yest = dates[0], dates[1]

        def fetch_oi_csv(date_obj):
            date_str = date_obj.strftime('%d%m%Y')
            url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    content = r.content.decode('utf-8')
                    lines = content.splitlines()
                    for idx, line in enumerate(lines[:20]):
                        if "Future Index Long" in line:
                            df = pd.read_csv(io.StringIO(content), skiprows=idx)
                            df.columns = df.columns.str.strip()
                            return df
            except:
                pass
            return None

        df_today = fetch_oi_csv(today)
        df_yest = fetch_oi_csv(yest)
        if df_today is None:
            return None, None, 0.0, today.strftime('%d-%b-%Y')
        today_data = self._process_participant_data(df_today)
        yest_data = self._process_participant_data(df_yest) if df_yest is not None else {}
        fii_net_change = 0.0
        if today_data.get('FII') and yest_data.get('FII'):
            fii_net_change = today_data['FII'].fut_net - yest_data['FII'].fut_net
        return today_data, yest_data, fii_net_change, today.strftime('%d-%b-%Y')

    def _process_participant_data(self, df) -> Dict[str, ParticipantData]:
        data = {}
        for p in ["FII", "DII", "Client", "Pro"]:
            try:
                row = df[df['Client Type'].astype(str).str.contains(p, case=False, na=False)].iloc[0]
                data[p] = ParticipantData(
                    fut_long=float(row['Future Index Long']),
                    fut_short=float(row['Future Index Short']),
                    fut_net=float(row['Future Index Long']) - float(row['Future Index Short']),
                    call_long=float(row['Option Index Call Long']),
                    call_short=float(row['Option Index Call Short']),
                    call_net=float(row['Option Index Call Long']) - float(row['Option Index Call Short']),
                    put_long=float(row['Option Index Put Long']),
                    put_short=float(row['Option Index Put Short']),
                    put_net=float(row['Option Index Put Long']) - float(row['Option Index Put Short']),
                    stock_net=float(row['Future Stock Long']) - float(row['Future Stock Short'])
                )
            except:
                data[p] = None
        return data

    def get_time_metrics(self, weekly, monthly, next_weekly) -> TimeMetrics:
        today = date.today()
        dte_w = (weekly - today).days if weekly else 0
        dte_m = (monthly - today).days if monthly else 0
        dte_nw = (next_weekly - today).days if next_weekly else 0
        
        now = datetime.now(settings.IST)
        is_expiry_day_weekly = (dte_w == 0)
        is_expiry_day_monthly = (dte_m == 0)
        is_past_square_off_time = now.time() >= settings.SQUARE_OFF_TIME_IST
        
        return TimeMetrics(
            current_date=today,
            weekly_exp=weekly,
            monthly_exp=monthly,
            next_weekly_exp=next_weekly,
            dte_weekly=dte_w,
            dte_monthly=dte_m,
            dte_next_weekly=dte_nw,
            is_expiry_day_weekly=is_expiry_day_weekly,
            is_expiry_day_monthly=is_expiry_day_monthly,
            is_past_square_off_time=is_past_square_off_time,
            is_gamma_week=dte_w <= settings.GAMMA_DANGER_DTE,
            is_gamma_month=dte_m <= settings.GAMMA_DANGER_DTE,
            days_to_next_weekly=dte_nw
        )

    def get_vol_metrics(self, nifty_hist, vix_hist, live_prices) -> VolMetrics:
        is_fallback = False
        nifty_live = vix_live = 0
        if hasattr(live_prices, 'data'):
            data = live_prices.data
            if settings.NIFTY_KEY in data:
                nifty_live = data[settings.NIFTY_KEY].last_price
            if settings.VIX_KEY in data:
                vix_live = data[settings.VIX_KEY].last_price
        spot = nifty_live if nifty_live > 0 else (nifty_hist.iloc[-1]['close'] if not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if not vix_hist.empty else 0)
        if nifty_live <= 0 or vix_live <= 0:
            is_fallback = True
            
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        rv7 = returns.rolling(7).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 7 else 0
        rv28 = returns.rolling(28).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 28 else 0
        rv90 = returns.rolling(90).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 90 else 0

        def fit_garch(horizon):
            try:
                if len(returns) < 100:
                    return 0
                model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='normal')
                result = model.fit(disp='off', show_warning=False)
                forecast = result.forecast(horizon=horizon, reindex=False)
                return np.sqrt(forecast.variance.values[-1, -1]) * np.sqrt(252)
            except:
                return 0

        garch7 = fit_garch(7) or rv7
        garch28 = fit_garch(28) or rv28
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100 if len(nifty_hist) >= 7 else 0
        park28 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100 if len(nifty_hist) >= 28 else 0
        
        vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
        vov = vix_returns.rolling(30).std().iloc[-1] * np.sqrt(252) * 100 if len(vix_returns) >= 30 else 0
        vov_rolling = vix_returns.rolling(30).std() * np.sqrt(252) * 100 if len(vix_returns) >= 30 else pd.Series()
        vov_mean = vov_rolling.rolling(60).mean().iloc[-1] if len(vov_rolling) >= 60 else 0
        vov_std = vov_rolling.rolling(60).std().iloc[-1] if len(vov_rolling) >= 60 else 0
        vov_zscore = (vov - vov_mean) / vov_std if vov_std > 0 else 0

        def calc_ivp(window):
            if len(vix_hist) < window:
                return 0.0
            history = vix_hist['close'].tail(window)
            return (history < vix).mean() * 100

        ivp_30d, ivp_90d, ivp_1yr = calc_ivp(30), calc_ivp(90), calc_ivp(252)
        ma20 = nifty_hist['close'].rolling(20).mean().iloc[-1] if len(nifty_hist) >= 20 else 0
        true_range = pd.concat([
            nifty_hist['high'] - nifty_hist['low'],
            (nifty_hist['high'] - nifty_hist['close'].shift(1)).abs(),
            (nifty_hist['low'] - nifty_hist['close'].shift(1)).abs()
        ], axis=1).max(axis=1)
        atr14 = true_range.rolling(14).mean().iloc[-1] if len(true_range) >= 14 else 0
        trend_strength = abs(spot - ma20) / atr14 if atr14 > 0 else 0
        
        vix_5d_ago = vix_hist['close'].iloc[-6] if len(vix_hist) >= 6 else vix
        vix_change_5d = vix - vix_5d_ago
        
        if vix_change_5d > settings.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "EXPLOSIVE_UP"
        elif vix_change_5d < -settings.VIX_MOMENTUM_BREAKOUT:
            vix_momentum = "COLLAPSING"
        elif vix_change_5d > 2.0:
            vix_momentum = "RISING"
        elif vix_change_5d < -2.0:
            vix_momentum = "FALLING"
        else:
            vix_momentum = "STABLE"
            
        vol_regime = "EXPLODING" if vov_zscore > settings.VOV_CRASH_ZSCORE else \
                    "RICH" if ivp_1yr > settings.HIGH_VOL_IVP else \
                    "CHEAP" if ivp_1yr < settings.LOW_VOL_IVP else "FAIR"
                    
        return VolMetrics(
            spot, vix, rv7, rv28, rv90, garch7, garch28,
            park7, park28, vov, vov_zscore,
            ivp_30d, ivp_90d, ivp_1yr,
            ma20, atr14, trend_strength, vol_regime, is_fallback,
            vix_change_5d, vix_momentum
        )

    def get_struct_metrics(self, chain, spot, lot_size) -> StructMetrics:
        if chain.empty or spot == 0:
            return StructMetrics(0, 0, 0, "NEUTRAL", 0, 0, 0, "NEUTRAL", lot_size, 1.0, "UNKNOWN", 5.0)
        subset = chain[(chain['strike'] > spot * 0.90) & (chain['strike'] < spot * 1.10)]
        net_gex = ((subset['ce_gamma'] * subset['ce_oi']).sum() - (subset['pe_gamma'] * subset['pe_oi']).sum()) * spot * lot_size
        total_oi_value = (chain['ce_oi'].sum() + chain['pe_oi'].sum()) * spot * lot_size
        gex_ratio = abs(net_gex) / total_oi_value if total_oi_value > 0 else 0
        gex_regime = "STICKY" if gex_ratio > settings.GEX_STICKY_RATIO else \
                    "SLIPPERY" if gex_ratio < settings.GEX_STICKY_RATIO * 0.5 else "NEUTRAL"
        pcr = chain['pe_oi'].sum() / chain['ce_oi'].sum() if chain['ce_oi'].sum() > 0 else 1.0
        
        atm_range = chain[(chain['strike'] >= spot * 0.98) & (chain['strike'] <= spot * 1.02)]
        pcr_atm = atm_range['pe_oi'].sum() / atm_range['ce_oi'].sum() if not atm_range.empty and atm_range['ce_oi'].sum() > 0 else pcr
        
        strikes = chain['strike'].values
        losses = [
            np.sum(np.maximum(0, s - strikes) * chain['ce_oi'].values) + \
            np.sum(np.maximum(0, strikes - s) * chain['pe_oi'].values)
            for s in strikes
        ]
        max_pain = strikes[np.argmin(losses)] if losses else 0
        
        try:
            ce_25d_idx = (chain['ce_delta'].abs() - 0.25).abs().argsort()[:1]
            pe_25d_idx = (chain['pe_delta'].abs() - 0.25).argsort()[:1]
            skew_25d = chain.iloc[pe_25d_idx]['pe_iv'].values[0] - chain.iloc[ce_25d_idx]['ce_iv'].values[0]
            
            if skew_25d > settings.SKEW_CRASH_FEAR:
                skew_regime = "CRASH_FEAR"
            elif skew_25d < settings.SKEW_MELT_UP:
                skew_regime = "MELT_UP"
            else:
                skew_regime = "BALANCED"
        except:
            skew_25d = 0
            skew_regime = "UNKNOWN"
            
        oi_regime = "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL"
        
        if gex_regime == "STICKY":
            gex_weighted = 8.0
        elif gex_regime == "SLIPPERY":
            gex_weighted = 3.0
        else:
            gex_weighted = 5.0
        
        return StructMetrics(
            net_gex, gex_ratio, total_oi_value, gex_regime,
            pcr, max_pain, skew_25d, oi_regime, lot_size,
            pcr_atm, skew_regime, gex_weighted
        )

    def get_edge_metrics(self, weekly_chain, monthly_chain, next_weekly_chain, spot, vol: VolMetrics, time: TimeMetrics) -> EdgeMetrics:
        def get_atm_iv(chain):
            if chain.empty or spot == 0:
                return 0
            atm_idx = (chain['strike'] - spot).abs().argsort()[:1]
            row = chain.iloc[atm_idx].iloc[0]
            return (row['ce_iv'] + row['pe_iv']) / 2
            
        iv_weekly = get_atm_iv(weekly_chain)
        iv_monthly = get_atm_iv(monthly_chain)
        iv_next_weekly = get_atm_iv(next_weekly_chain)
        
        vrp_rv_weekly = iv_weekly - vol.rv7
        vrp_garch_weekly = iv_weekly - vol.garch7
        vrp_park_weekly = iv_weekly - vol.park7
        weighted_vrp_weekly = (vrp_garch_weekly * 0.70) + (vrp_park_weekly * 0.15) + (vrp_rv_weekly * 0.15)
        
        vrp_rv_monthly = iv_monthly - vol.rv28
        vrp_garch_monthly = iv_monthly - vol.garch28
        vrp_park_monthly = iv_monthly - vol.garch28
        weighted_vrp_monthly = (vrp_garch_monthly * 0.70) + (vrp_park_monthly * 0.15) + (vrp_rv_monthly * 0.15)
        
        vrp_rv_next_weekly = iv_next_weekly - vol.rv7
        vrp_garch_next_weekly = iv_next_weekly - vol.garch7
        vrp_park_next_weekly = iv_next_weekly - vol.park7
        weighted_vrp_next_weekly = (vrp_garch_next_weekly * 0.70) + (vrp_park_next_weekly * 0.15) + (vrp_rv_next_weekly * 0.15)
        
        if time.is_expiry_day_weekly:
            short_term_iv = iv_next_weekly
            term_spread = iv_monthly - short_term_iv
        else:
            short_term_iv = iv_weekly
            term_spread = iv_monthly - short_term_iv
            
        term_regime = "BACKWARDATION" if term_spread < -1.0 else "CONTANGO" if term_spread > 1.0 else "FLAT"
        primary_edge = "LONG_VOL" if vol.ivp_1yr < settings.LOW_VOL_IVP else \
                      "SHORT_GAMMA" if weighted_vrp_weekly > 4.0 and vol.ivp_1yr > 50 else \
                      "SHORT_VEGA" if weighted_vrp_monthly > 3.0 and vol.ivp_1yr > 50 else \
                      "CALENDAR_SPREAD" if term_regime == "BACKWARDATION" and term_spread < -2.0 else \
                      "MEAN_REVERSION" if vol.ivp_1yr > settings.HIGH_VOL_IVP else "NONE"
                      
        return EdgeMetrics(
            iv_weekly, vrp_rv_weekly, vrp_garch_weekly, vrp_park_weekly,
            iv_monthly, vrp_rv_monthly, vrp_garch_monthly, vrp_park_monthly,
            iv_next_weekly, vrp_rv_next_weekly, vrp_garch_next_weekly, vrp_park_next_weekly,
            term_spread, term_regime, primary_edge,
            weighted_vrp_weekly, weighted_vrp_monthly, weighted_vrp_next_weekly
        )

    def get_external_metrics(self, nifty_hist, participant_data, participant_yest, fii_net_change, data_date, economic_events) -> ExternalMetrics:
        fast_vol = False
        if not nifty_hist.empty:
            last_bar = nifty_hist.iloc[-1]
            daily_range_pct = ((last_bar['high'] - last_bar['low']) / last_bar['open']) * 100
            fast_vol = daily_range_pct > 1.8
        
        calendar_engine = CalendarEngine()
        veto_events, high_impact, square_off_needed, square_off_time = calendar_engine.calculate_event_impact(economic_events)
        
        has_veto, veto_name, veto_square_off, hours_until = calendar_engine.analyze_veto_risk(economic_events, datetime.now(settings.IST))
        
        flow_regime = "NEUTRAL"
        fii_direction = "NEUTRAL"
        if participant_data and participant_data.get('FII'):
            fii_net = participant_data['FII'].fut_net
            if fii_net > settings.FII_STRONG_LONG:
                flow_regime = "STRONG_LONG"
                fii_direction = "BULLISH"
            elif fii_net < settings.FII_STRONG_SHORT:
                flow_regime = "STRONG_SHORT"
                fii_direction = "BEARISH"
            elif abs(fii_net) > settings.FII_MODERATE:
                flow_regime = "MODERATE_LONG" if fii_net > 0 else "MODERATE_SHORT"
                fii_direction = "MILDLY_BULLISH" if fii_net > 0 else "MILDLY_BEARISH"
        
        event_risk = "LOW"
        if has_veto and hours_until and hours_until <= 24:
            event_risk = "CRITICAL"
        elif has_veto and hours_until and hours_until <= 48:
            event_risk = "HIGH"
        elif len(high_impact) > 0 and high_impact[0].hours_until <= 48:
            event_risk = "MEDIUM"
        
        return ExternalMetrics(
            fii=participant_data.get('FII') if participant_data else None,
            dii=participant_data.get('DII') if participant_data else None,
            pro=participant_data.get('Pro') if participant_data else None,
            client=participant_data.get('Client') if participant_data else None,
            fii_net_change=fii_net_change,
            flow_regime=flow_regime,
            flow_score=0.0,
            option_bias=0.0,
            data_date=data_date,
            is_fallback_data=False,
            fast_vol=fast_vol,
            fii_conviction="NEUTRAL",
            fii_direction=fii_direction,
            economic_events=economic_events,
            veto_events=veto_events,
            high_impact_events=high_impact,
            veto_square_off_needed=square_off_needed,
            veto_square_off_time=square_off_time,
            veto_hours_until=hours_until,
            event_risk=event_risk,
            has_veto_event=has_veto,
            veto_event_name=veto_name,
            upcoming_high_impact=high_impact
        )
