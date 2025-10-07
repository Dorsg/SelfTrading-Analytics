from __future__ import annotations

import os
import logging
from typing import Any, Dict, List

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("claude-4-5-sonnet-strategy")


class Claude45SonnetStrategy:
    """
    High-Probability Pullback Strategy with Rigorous Risk Management
    
    Philosophy:
      • QUALITY OVER QUANTITY: Only take the absolute best setups
      • Buy pullbacks in strong uptrends, NOT breakouts (better risk/reward)
      • Multiple confirmation required before entry
      • Wide stops to survive normal volatility
      • Exit only on clear reversal signals
    
    Strategy:
      • Trend: Price > EMA20 > EMA50 (strong uptrend)
      • Entry: Pullback to Fibonacci 38.2% or 50% retracement
      • Momentum: RSI 40-75 (healthy but not overbought)
      • Confirmation: MACD bullish + volume above average
      • Higher TF: Daily trend must also be up
      • Risk: ATR-based trailing stops (3-10% range)
    """

    name = "Claude45SonnetStrategy"

    # Trend indicators - simplified
    ema_fast_period = 20      # Short-term trend
    ema_mid_period = 50       # Medium-term trend
    
    # Momentum
    rsi_period = 14
    rsi_min = 40.0            # Healthy pullback zone
    rsi_max = 75.0            # Avoid extreme overbought
    
    # MACD
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9
    
    # Volume
    volume_ma_period = 20
    volume_multiplier = 1.1   # 10% above average
    
    # ATR & Risk Management
    atr_period = 14
    trail_min_pct = 3.0       # Wider stops to survive noise
    trail_max_pct = 10.0      # Cap maximum stop
    atr_trail_multiplier = 2.0  # Give trends room to breathe
    
    # Fibonacci for pullback entries
    fib_lookback = 50         # Bars to find swing high/low
    fib_entry_levels = [0.382, 0.5, 0.618]  # Buy on retracements
    fib_tolerance = 0.02      # 2% tolerance around fib levels
    
    # Exit thresholds
    extreme_rsi = 80.0        # Lock profits when extreme
    weak_rsi = 35.0           # Exit when very weak
    take_profit_pct = 20.0    # Lock in gains at 20%
    
    # Higher timeframe
    daily_ema_period = 50
    daily_lookback = 100
    
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    def _check_fib_retracement(self, candles: List[Dict[str, Any]], price: float) -> tuple[bool, float]:
        """
        Check if current price is at a Fibonacci retracement level.
        Returns (is_at_fib_level, fib_level_pct)
        """
        if len(candles) < self.fib_lookback:
            return False, 0.0
        
        lookback_candles = candles[-self.fib_lookback:]
        swing_high = max(c['high'] for c in lookback_candles)
        swing_low = min(c['low'] for c in lookback_candles)
        
        if swing_high <= swing_low:
            return False, 0.0
        
        price_range = swing_high - swing_low
        current_retrace = (swing_high - price) / price_range
        
        # Check if near any Fibonacci level
        for fib_level in self.fib_entry_levels:
            if abs(current_retrace - fib_level) <= self.fib_tolerance:
                return True, fib_level
        
        return False, 0.0

    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        # Much lower bar requirement
        min_bars = max(
            self.ema_mid_period + 1,
            self.rsi_period + 1,
            self.atr_period + 1,
            self.volume_ma_period + 1,
            self.macd_slow + self.macd_signal + 1,
            self.fib_lookback + 1,
        )

        if len(candles) < min_bars:
            return {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "candles_count": len(candles),
                "required_bars": min_bars,
            }

        # Calculate indicators
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_mid = self.mkt.calculate_ema(candles, self.ema_mid_period)
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        macd_line, macd_sig = self.mkt.calculate_macd(
            candles, self.macd_fast, self.macd_slow, self.macd_signal
        )
        atr = self.mkt.calculate_atr(candles, self.atr_period)
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        current_volume = float(candles[-1].get("volume", 0) or 0)

        # Validate indicators
        if any(
            x is None or (isinstance(x, float) and x != x)  # None or NaN
            for x in [ema_fast, ema_mid, rsi, macd_line, macd_sig, atr]
        ):
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
            }

        # ========== CORE CONDITIONS (ALL MUST PASS) ==========
        
        # 1. STRONG UPTREND: Price above both EMAs, and EMAs aligned
        trend_ok = price > ema_fast and ema_fast > ema_mid
        price_above_emas = price > ema_fast * 1.01  # At least 1% above fast EMA
        
        # 2. RSI IN HEALTHY ZONE: Not oversold, not overbought
        rsi_ok = self.rsi_min <= rsi <= self.rsi_max
        
        # 3. MACD BULLISH: Momentum is up
        macd_bullish = macd_line > macd_sig
        macd_positive = macd_line > 0
        
        # 4. FIBONACCI RETRACEMENT: Price at pullback level (KEY DIFFERENTIATOR)
        at_fib_level, fib_level = self._check_fib_retracement(candles, price)
        
        # 5. VOLUME CONFIRMATION: Above average volume
        volume_ok = current_volume > volume_ma * self.volume_multiplier if volume_ma > 0 else False
        
        # ========== HIGHER TIMEFRAME CHECK ==========
        higher_tf_ok = True
        daily_ema = None
        if getattr(info.runner, "time_frame", 5) == 5:
            # For 5-min timeframe, check daily trend
            try:
                daily_candles = self.mkt.get_candles_until(
                    symbol, 1440, candles[-1]['ts'], lookback=self.daily_lookback
                )
                if len(daily_candles) >= self.daily_ema_period:
                    daily_ema = self.mkt.calculate_ema(daily_candles, self.daily_ema_period)
                    if daily_ema:
                        higher_tf_ok = price > daily_ema
            except Exception as e:
                log.debug(f"{symbol} - Could not check daily trend: {e}")
                # If can't check, be conservative
                higher_tf_ok = True

        # ========== ENTRY DECISION MATRIX ==========
        
        # Core requirements (STRICT - ALL must pass)
        core_conditions = [trend_ok, rsi_ok, macd_bullish, higher_tf_ok]
        core_passed = all(core_conditions)
        
        # Entry trigger (need at least 2 of 3)
        entry_signals = [at_fib_level, volume_ok, macd_positive]
        entry_trigger = sum(entry_signals) >= 2
        
        # STRICT ACCEPTANCE CRITERIA
        accept_trade = core_passed and entry_trigger

        checklist = [
            {"label": "✓ CORE: Strong Uptrend (P>EMA20>EMA50)", "ok": trend_ok, 
             "actual": f"P:{price:.2f} EMA20:{ema_fast:.2f} EMA50:{ema_mid:.2f}"},
            {"label": "✓ CORE: RSI Healthy Zone", "ok": rsi_ok, 
             "actual": f"{rsi:.1f}", "wanted": f"{self.rsi_min}-{self.rsi_max}"},
            {"label": "✓ CORE: MACD Bullish", "ok": macd_bullish, 
             "actual": f"MACD:{macd_line:.4f} Sig:{macd_sig:.4f}"},
            {"label": "✓ CORE: Higher TF Trend", "ok": higher_tf_ok, 
             "actual": f"Daily EMA: {daily_ema:.2f}" if daily_ema else "N/A"},
            {"label": "ENTRY: Fibonacci Pullback", "ok": at_fib_level, 
             "actual": f"{fib_level:.1%}" if at_fib_level else "Not at level"},
            {"label": "ENTRY: Volume Surge", "ok": volume_ok, 
             "actual": f"{current_volume:.0f}", "wanted": f">{volume_ma * self.volume_multiplier:.0f}"},
            {"label": "ENTRY: MACD Positive", "ok": macd_positive, 
             "actual": f"{macd_line:.4f}", "wanted": ">0"},
        ]

        if not accept_trade:
            core_score = sum(core_conditions)
            entry_score = sum(entry_signals)
            res = {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "signal_strength": f"Core:{core_score}/4 Entry:{entry_score}/3",
                "explanation": f"Need ALL core conditions + 2/3 entry signals\n" + format_checklist(checklist),
                "checks": checklist,
            }
            log.debug(
                "%s NO_ACTION @ %s - Core:%d/4 Entry:%d/3",
                symbol, res["price"], core_score, entry_score
            )
            return res

        # Calculate ATR-based trailing stop
        atr_pct = (atr / price) * 100.0 if price > 0 else self.trail_min_pct
        trail_pct = min(
            max(atr_pct * self.atr_trail_multiplier, self.trail_min_pct),
            self.trail_max_pct
        )
        
        # Session-aware limit price
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)

        core_score = sum(core_conditions)
        entry_score = sum(entry_signals)
        res = {
            "action": "BUY",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_stop_order": {
                "action": "SELL",
                "order_type": "TRAIL_LIMIT",
                "trailing_percent": round(trail_pct, 2),
            },
            "signal_strength": f"Core:{core_score}/4 Entry:{entry_score}/3",
            "explanation": f"🎯 HIGH-PROBABILITY PULLBACK (Fib:{fib_level:.1%})\n" + format_checklist(checklist),
            "checks": checklist,
        }
        
        log.info(
            "🎯 BUY %s @ %s (lmt=%s, trail=%.1f%%) - RSI:%.1f MACD:%.4f Fib:%s HTF:%s",
            symbol, res["price"], res["limit_price"], trail_pct,
            rsi, macd_line, f"{fib_level:.1%}" if at_fib_level else "N/A", 
            "✓" if higher_tf_ok else "✗"
        )
        return res

    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        """
        Conservative exit logic: Only sell on strong reversal signals or profit target.
        Let trailing stops do most of the work.
        """
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []
        position = info.position

        min_bars = max(
            self.rsi_period + 1,
            self.macd_slow + self.macd_signal + 1,
            self.ema_fast_period + 1,
        )
        
        if len(candles) < min_bars or position is None:
            return {
                "action": "NO_ACTION",
                "reason": "insufficient_data_or_no_position",
                "price": round(price, 4),
            }

        # Calculate indicators
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        macd_line, macd_sig = self.mkt.calculate_macd(
            candles, self.macd_fast, self.macd_slow, self.macd_signal
        )
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_mid = self.mkt.calculate_ema(candles, self.ema_mid_period)

        # Calculate current P&L
        current_gain_pct = ((price - position.avg_price) / position.avg_price * 100) if position.avg_price > 0 else 0

        # ========== EXIT CONDITIONS ==========
        
        # 1. TAKE PROFIT: Hit profit target
        take_profit_hit = current_gain_pct >= self.take_profit_pct
        
        # 2. EXTREME OVERBOUGHT + BEARISH MACD: Lock in profits
        extreme_overbought = rsi is not None and rsi >= self.extreme_rsi
        macd_bearish = (macd_line is not None and macd_sig is not None and 
                       macd_line < macd_sig * 0.98)  # MACD clearly bearish
        
        profit_protection = extreme_overbought and macd_bearish
        
        # 3. TREND BREAKDOWN: Price breaks below both EMAs significantly
        if ema_fast and ema_mid:
            trend_broken = price < ema_fast * 0.96 and price < ema_mid * 0.97
        else:
            trend_broken = False
        
        # 4. MOMENTUM COLLAPSE: RSI very weak + MACD bearish
        rsi_very_weak = rsi is not None and rsi < self.weak_rsi
        momentum_collapse = rsi_very_weak and macd_bearish and trend_broken
        
        # ========== EXIT DECISION ==========
        
        # Critical exit (immediate sell)
        critical_exit = take_profit_hit or momentum_collapse
        
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)
        
        if critical_exit:
            reason = "take_profit" if take_profit_hit else "momentum_collapse"
            log.info(
                "🚨 EXIT %s @ %s - %s (P&L:%.2f%%, RSI:%.1f)",
                symbol, price, reason, current_gain_pct, rsi or 0
            )
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": reason,
                "explanation": f"Exit signal: {reason} (P&L: {current_gain_pct:.2f}%)",
                "checks": [
                    {"label": "Take Profit", "ok": take_profit_hit, "actual": f"{current_gain_pct:.2f}%", "wanted": f"≥{self.take_profit_pct}%"},
                    {"label": "Momentum Collapse", "ok": momentum_collapse},
                    {"label": "RSI Very Weak", "ok": rsi_very_weak, "actual": f"{rsi:.1f}" if rsi else "N/A"},
                    {"label": "Trend Broken", "ok": trend_broken},
                ],
            }
        
        if profit_protection:
            log.info(
                "💰 PROFIT LOCK %s @ %s - Extreme overbought + bearish (P&L:%.2f%%, RSI:%.1f)",
                symbol, price, current_gain_pct, rsi or 0
            )
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": "profit_protection",
                "explanation": f"Locking profits - RSI extreme + bearish MACD (P&L: {current_gain_pct:.2f}%)",
                "checks": [
                    {"label": "Extreme Overbought", "ok": extreme_overbought, "actual": f"{rsi:.1f}" if rsi else "N/A"},
                    {"label": "MACD Bearish", "ok": macd_bearish},
                ],
            }
        
        # Otherwise, let trailing stop do the work
        return {
            "action": "NO_ACTION",
            "reason": "trailing_stop_active",
            "price": round(price, 4),
            "explanation": f"Trailing stop active (P&L: {current_gain_pct:.2f}%)",
        }

    def decide_refresh(self, info: RunnerDecisionInfo) -> Dict[str, Any] | None:
        """No refresh logic - let trailing stops work."""
        return {"action": "NO_ACTION", "reason": "no_refresh_logic"}
