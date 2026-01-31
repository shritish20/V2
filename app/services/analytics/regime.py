"""
Regime Engine - Preserved
"""
import logging
from datetime import date, timedelta
from typing import List
import numpy as np

from app.core.config import settings
from app.models.domain import (
    VolMetrics, StructMetrics, EdgeMetrics, ExternalMetrics, 
    TimeMetrics, DynamicWeights, RegimeScore, TradingMandate
)

logger = logging.getLogger("VOLGUARD")

class RegimeEngineV33:
    def calculate_dynamic_weights(self, vol: VolMetrics, external: ExternalMetrics, dte: int) -> DynamicWeights:
        base_vol = 0.40
        base_struct = 0.30
        base_edge = 0.30
        
        rationale = "Base: 40% Vol, 30% Struct, 30% Edge"
        
        if vol.vov_zscore > settings.VOV_WARNING_ZSCORE or vol.vix_momentum == "RISING":
            base_vol += 0.10
            base_struct -= 0.05
            base_edge -= 0.05
            rationale = "High Vol Environment: Vol↑ to 50%, Struct/Edge↓"
        elif vol.ivp_1yr < settings.LOW_VOL_IVP:
            base_edge += 0.10
            base_vol -= 0.05
            base_struct -= 0.05
            rationale = "Low Vol: Edge↑ to 40% (limited premium)"
        
        if dte <= 2:
            base_struct += 0.10
            base_edge -= 0.05
            base_vol -= 0.05
            rationale += " | Near expiry: Struct↑ (gamma important)"
        
        if external.veto_square_off_needed:
            base_vol = 0.60
            base_struct = 0.20
            base_edge = 0.20
            rationale = "VETO EVENT: Vol risk dominates (60/20/20)"
        
        total = base_vol + base_struct + base_edge
        vol_weight = base_vol / total
        struct_weight = base_struct / total
        edge_weight = base_edge / total
        
        return DynamicWeights(vol_weight, struct_weight, edge_weight, rationale)
    
    def calculate_scores(self, vol: VolMetrics, struct: StructMetrics, 
                        edge: EdgeMetrics, external: ExternalMetrics,
                        expiry_type: str, dte: int) -> RegimeScore:
        score_drivers = []
        
        if expiry_type == "WEEKLY":
            weighted_vrp = edge.weighted_vrp_weekly
        elif expiry_type == "NEXT_WEEKLY":
            weighted_vrp = edge.weighted_vrp_next_weekly
        else:
            weighted_vrp = edge.weighted_vrp_monthly
        
        edge_score = 5.0
        
        if weighted_vrp > 4.0:
            edge_score += 3.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Excellent) +3.0")
        elif weighted_vrp > 2.0:
            edge_score += 2.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Good) +2.0")
        elif weighted_vrp > 1.0:
            edge_score += 1.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Moderate) +1.0")
        elif weighted_vrp < 0:
            edge_score -= 3.0
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Negative) -3.0")
        else:
            score_drivers.append(f"Edge: VRP {weighted_vrp:.1f}% (Neutral)")
        
        if edge.term_regime == "BACKWARDATION" and edge.term_spread < -2.0:
            edge_score += 1.0
            score_drivers.append(f"Edge: Steep Backwardation ({edge.term_spread:.1f}%) +1.0")
        elif edge.term_regime == "CONTANGO":
            edge_score += 0.5
            score_drivers.append("Edge: Contango +0.5")
        
        edge_score = max(0, min(10, edge_score))
        
        vol_score = 5.0
        
        if vol.vov_zscore > settings.VOV_CRASH_ZSCORE:
            vol_score = 0.0
            score_drivers.append(f"Vol: VOV Crash ({vol.vov_zscore:.1f}σ) → ZERO")
        elif vol.vov_zscore > settings.VOV_WARNING_ZSCORE:
            vol_score -= 3.0
            score_drivers.append(f"Vol: High VOV ({vol.vov_zscore:.1f}σ) -3.0")
        elif vol.vov_zscore < 1.5:
            vol_score += 1.5
            score_drivers.append(f"Vol: Stable VOV ({vol.vov_zscore:.1f}σ) +1.5")
        
        if vol.ivp_1yr > settings.HIGH_VOL_IVP:
            if vol.vix_momentum == "FALLING":
                vol_score += 1.5
                score_drivers.append(f"Vol: Rich IVP ({vol.ivp_1yr:.0f}%) + Falling VIX +1.5")
            elif vol.vix_momentum == "RISING":
                vol_score -= 1.0
                score_drivers.append(f"Vol: Rich IVP + Rising VIX -1.0")
            else:
                vol_score += 0.5
                score_drivers.append(f"Vol: Rich IVP ({vol.ivp_1yr:.0f}%) +0.5")
        elif vol.ivp_1yr < settings.LOW_VOL_IVP:
            vol_score -= 2.5
            score_drivers.append(f"Vol: Cheap IVP ({vol.ivp_1yr:.0f}%) -2.5")
        else:
            vol_score += 1.0
            score_drivers.append(f"Vol: Fair IVP ({vol.ivp_1yr:.0f}%) +1.0")
        
        if vol.vix_momentum == "EXPLOSIVE_UP":
            vol_score -= 2.0
            score_drivers.append(f"Vol: VIX explosive ({vol.vix_change_5d:+.1f}) -2.0")
        elif vol.vix_momentum == "COLLAPSING":
            vol_score += 1.0
            score_drivers.append(f"Vol: VIX collapsing ({vol.vix_change_5d:+.1f}) +1.0")
        
        vol_score = max(0, min(10, vol_score))
        
        struct_score = 5.0
        
        if struct.gex_regime == "STICKY":
            struct_score += 2.5
            score_drivers.append(f"Struct: Sticky GEX ({struct.gex_ratio:.3%}) +2.5")
        elif struct.gex_regime == "SLIPPERY":
            struct_score -= 1.0
            score_drivers.append("Struct: Slippery GEX -1.0")
        
        if 0.9 < struct.pcr_atm < 1.1:
            struct_score += 1.5
            score_drivers.append(f"Struct: Balanced PCR ATM ({struct.pcr_atm:.2f}) +1.5")
        elif struct.pcr_atm > 1.3 or struct.pcr_atm < 0.7:
            struct_score -= 0.5
            score_drivers.append(f"Struct: Extreme PCR ATM ({struct.pcr_atm:.2f}) -0.5")
        
        if struct.skew_regime == "CRASH_FEAR":
            struct_score -= 1.0
            score_drivers.append(f"Struct: Crash Fear Skew ({struct.skew_25d:+.1f}%) -1.0")
        elif struct.skew_regime == "MELT_UP":
            struct_score -= 0.5
            score_drivers.append("Struct: Melt-Up Skew -0.5")
        else:
            struct_score += 0.5
            score_drivers.append("Struct: Balanced Skew +0.5")
        
        struct_score = max(0, min(10, struct_score))
        
        weights = self.calculate_dynamic_weights(vol, external, dte)
        
        composite = (
            vol_score * weights.vol_weight +
            struct_score * weights.struct_weight +
            edge_score * weights.edge_weight
        )
        
        alt_weights = [(0.30, 0.35, 0.35), (0.50, 0.25, 0.25), (0.35, 0.30, 0.35)]
        alt_scores = [
            vol_score * wv + struct_score * ws + edge_score * we 
            for wv, ws, we in alt_weights
        ]
        score_stability = 1.0 - (np.std(alt_scores) / np.mean(alt_scores)) if np.mean(alt_scores) > 0 else 0.5
        
        confidence = "VERY_HIGH" if composite >= 8.0 and score_stability > 0.85 else \
                    "HIGH" if composite >= 6.5 and score_stability > 0.75 else \
                    "MODERATE" if composite >= 4.0 else "LOW"
        
        score_drivers.append(
            f"Composite: {composite:.2f}/10 "
            f"[V:{vol_score:.1f}×{weights.vol_weight:.0%} "
            f"S:{struct_score:.1f}×{weights.struct_weight:.0%} "
            f"E:{edge_score:.1f}×{weights.edge_weight:.0%}]"
        )
        
        return RegimeScore(
            vol_score, struct_score, edge_score, composite, confidence,
            score_stability, weights, score_drivers
        )
    
    def generate_mandate(self, score: RegimeScore, vol: VolMetrics, 
                        struct: StructMetrics, edge: EdgeMetrics,
                        external: ExternalMetrics, time: TimeMetrics,
                        expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        rationale = []
        warnings = []
        veto_reasons = []
        is_trade_allowed = True
        square_off_instruction = None
        data_relevance = "CURRENT"
        directional_bias = "NEUTRAL"
        
        if expiry_type == "WEEKLY":
            weighted_vrp = edge.weighted_vrp_weekly
        elif expiry_type == "NEXT_WEEKLY":
            weighted_vrp = edge.weighted_vrp_next_weekly
        else:
            weighted_vrp = edge.weighted_vrp_monthly
        
        if external.veto_square_off_needed:
            return TradingMandate(
                expiry_type=expiry_type,
                expiry_date=expiry_date,
                dte=dte,
                regime_name="VETO_EVENT",
                strategy_type="CASH",
                allocation_pct=0.0,
                deployment_amount=0.0,
                score=score,
                rationale=[f"VETO EVENT: {external.veto_event_name}"],
                warnings=[f"⛔ RBI/Fed event in {external.veto_hours_until:.1f}h - ALL TRADES BLOCKED"],
                veto_reasons=[f"Veto: {external.veto_event_name}"],
                suggested_structure="NONE",
                directional_bias="NEUTRAL",
                wing_protection="NONE",
                is_trade_allowed=False,
                square_off_instruction=f"Square off all positions before {external.veto_event_name}",
                data_relevance="VETO_ACTIVE"
            )
        
        if time.is_expiry_day_weekly and expiry_type == "WEEKLY":
            warnings.append("⚠️ EXPIRY DAY - Data may be erratic")
            data_relevance = "STALE_EXPIRY_DAY"
        elif time.is_expiry_day_monthly and expiry_type == "MONTHLY":
            warnings.append("⚠️ EXPIRY DAY - Data may be erratic")
            data_relevance = "STALE_EXPIRY_DAY"
        
        if time.is_past_square_off_time:
            warnings.append("⚠️ Past 2 PM - No new trades")
            is_trade_allowed = False
            veto_reasons.append("Past square-off time (2:00 PM IST)")
        
        if struct.pcr_atm > 1.3:
            directional_bias = "BULLISH"
        elif struct.pcr_atm < 0.7:
            directional_bias = "BEARISH"
        elif external.fii_direction == "BULLISH":
            directional_bias = "MILDLY_BULLISH"
        elif external.fii_direction == "BEARISH":
            directional_bias = "MILDLY_BEARISH"
        
        if score.composite >= 7.5 and score.confidence in ["HIGH", "VERY_HIGH"]:
            if dte > 2:
                regime_name = "PREMIUM_HARVEST"
                strategy = "AGGRESSIVE_SHORT"
                suggested = "IRON_CONDOR"
                wing_protection = "Standard (100-200 pts)"
                base_allocation = 60.0
                rationale.append(f"Very High Confidence ({score.confidence}): VRP {weighted_vrp:.2f}%")
                rationale.append(f"Dynamic Weights: {score.weights_used.rationale}")
            else:
                regime_name = "GAMMA_HARVEST"
                strategy = "AGGRESSIVE_SHORT"
                suggested = "IRON_FLY"
                wing_protection = "Tight (50-100 pts)"
                base_allocation = 50.0
                rationale.append(f"High VRP ({weighted_vrp:.2f}%) + Near expiry")
                rationale.append(f"Gamma collapse setup - tight iron fly for max theta")
                warnings.append("⚠️ GAMMA RISK - Monitor closely")
        
        elif score.composite >= 6.0 and score.confidence in ["HIGH", "VERY_HIGH"]:
            if dte > 1:
                regime_name = "MODERATE_HARVEST"
                strategy = "MODERATE_SHORT"
                suggested = "IRON_CONDOR"
                wing_protection = "Standard (100-150 pts)"
                base_allocation = 40.0
                rationale.append(f"Moderate Confidence: VRP {weighted_vrp:.2f}%")
                rationale.append(f"Weights: {score.weights_used.rationale}")
            else:
                regime_name = "LATE_GAMMA_HARVEST"
                strategy = "MODERATE_SHORT"
                suggested = "IRON_FLY"
                wing_protection = "Standard (75-100 pts)"
                base_allocation = 35.0
                rationale.append(f"Moderate VRP near expiry")
                rationale.append(f"Last day theta capture - standard iron fly")
                warnings.append("⚠️ EXPIRY RISK - Monitor gamma")
        
        elif score.composite >= 4.0:
            if struct.pcr > 1.3 and vol.trend_strength > 0.5:
                directional_bias = "BULLISH"
                suggested = "BULL_PUT_SPREAD"
            elif struct.pcr < 0.7 and vol.trend_strength > 0.5:
                directional_bias = "BEARISH"
                suggested = "BEAR_CALL_SPREAD"
            else:
                directional_bias = "NEUTRAL"
                suggested = "CREDIT_SPREAD"
            
            regime_name = "DEFENSIVE"
            strategy = "DEFENSIVE"
            base_allocation = 20.0
            wing_protection = "Wide (150-250 pts)"
            rationale.append("Defensive Posture - lower conviction")
            rationale.append(f"Directional bias: {directional_bias}")
            warnings.append("⚠️ LOWER CONVICTION - Reduce size")
        
        else:
            regime_name = "CASH"
            strategy = "CASH"
            suggested = "NONE"
            wing_protection = "N/A"
            base_allocation = 0.0
            is_trade_allowed = False
            rationale.append("Regime Unfavorable: Cash is a position")
            rationale.append(f"Composite score too low: {score.composite:.2f}/10")
            veto_reasons.append("Low composite score")
        
        allocation = base_allocation
        
        if vol.vov_zscore > settings.VOV_WARNING_ZSCORE:
            warnings.append(f"⚠️ HIGH VOL-OF-VOL ({vol.vov_zscore:.2f}σ) - Size reduced 30%")
            allocation *= 0.7
        
        if vol.vix_momentum == "EXPLOSIVE_UP":
            warnings.append(f"⚠️ VIX EXPLOSIVE ({vol.vix:.1f}) - Size reduced 40%")
            allocation *= 0.6
        
        if score.score_stability < 0.75:
            warnings.append(f"⚠️ LOW SCORE STABILITY ({score.score_stability:.2f}) - Size reduced 20%")
            allocation *= 0.8
        
        if external.high_impact_events:
            high_impact_count = len(external.high_impact_events)
            warnings.append(f"⚠️ {high_impact_count} HIGH IMPACT EVENT(S) THIS WEEK")
            allocation *= 0.85
        
        try:
            daily_regime = external.db_writer.get_state("daily_risk_regime") if hasattr(external, 'db_writer') else None
            if daily_regime:
                import json
                ai_data = json.loads(daily_regime)
                
                if ai_data.get("date") == str(date.today()):
                    risk_score = ai_data.get("risk_score", 5)
                    
                    if risk_score >= 8:
                        strategy = "DEFENSIVE"
                        suggested = "CASH_ONLY"
                        allocation = 0.0
                        is_trade_allowed = False
                        warnings.append(f"⛔ AI BRAKE: Extreme Risk ({risk_score}/10) - HALTED")
                        rationale.append(f"AI Reason: {ai_data.get('reason', 'High Risk')}")
                        veto_reasons.append("AI Morning Brief: Extreme market risk detected")
                    
                    elif risk_score >= 6:
                        allocation = allocation * 0.5
                        warnings.append(f"⚠️ AI CAUTION: Risk ({risk_score}/10) - Sizing Halved")
                        rationale.append("AI Brief: Elevated risk - reduced sizing")
        except Exception as e:
            logger.error(f"AI Override Failed: {e}")
        
        allocation = max(0, min(100, allocation))
        deployment_amount = settings.BASE_CAPITAL * (allocation / 100.0)
        
        if deployment_amount > settings.MAX_CAPITAL_PER_TRADE:
            deployment_amount = settings.MAX_CAPITAL_PER_TRADE
            warnings.append(f"⚠️ Capital capped at ₹{settings.MAX_CAPITAL_PER_TRADE:,.0f}")
        
        if strategy == "AGGRESSIVE_SHORT":
            risk_per_lot = settings.MARGIN_SELL_BASE
        elif strategy == "MODERATE_SHORT":
            risk_per_lot = settings.MARGIN_SELL_BASE * 0.8
        elif strategy == "DEFENSIVE":
            risk_per_lot = settings.MARGIN_SELL_BASE * 0.6
        else:
            risk_per_lot = 0
            
        max_lots = int(deployment_amount / risk_per_lot) if risk_per_lot > 0 else 0
        
        square_off_instruction = None
        if external.has_veto_event and external.veto_hours_until:
            if external.veto_hours_until <= 48:
                square_off_instruction = (
                    f"Square off by {external.veto_hours_until:.1f}h before "
                    f"{external.veto_event_name}"
                )
        
        return TradingMandate(
            expiry_type=expiry_type,
            expiry_date=expiry_date,
            dte=dte,
            regime_name=regime_name,
            strategy_type=strategy,
            allocation_pct=allocation,
            deployment_amount=deployment_amount,
            score=score,
            rationale=rationale,
            warnings=warnings,
            veto_reasons=veto_reasons,
            suggested_structure=suggested,
            directional_bias=directional_bias,
            wing_protection=wing_protection,
            is_trade_allowed=is_trade_allowed,
            data_relevance=data_relevance,
            square_off_instruction=square_off_instruction,
            max_lots=max_lots,
            risk_per_lot=risk_per_lot
        )
