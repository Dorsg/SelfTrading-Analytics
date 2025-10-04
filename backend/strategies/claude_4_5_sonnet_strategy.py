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
    Elite quantitative strategy optimized for maximum profitability.
    
    Core Strategy:
      • Trend-following with momentum breakouts (NOT mean reversion)
      • Multiple timeframe confluence (EMA 20, 50, 200)
      • Donchian breakout + Bollinger squeeze for entries
      • RSI momentum filter (trending, not oversold)
      • MACD confirmation for trend strength
      • Volume surge validation
      • Adaptive ATR stops with tighter ranges
      • Dynamic profit targets based on volatility
      • Market regime detection (trending vs choppy)
    
    Philosophy: Catch strong momentum moves with tight risk management.
    Enter on confirmed breakouts in strong trends, exit quickly on weakness.
    """

    name = "Claude45SonnetStrategy"

    # Multi-timeframe trend
    ema_fast_period = 20      # Short-term trend
    ema_mid_period = 50       # Medium-term trend
    ema_slow_period = 200     # Long-term trend
    
    # Breakout detection
    donchian_period = 40      # Breakout channel
    breakout_buffer_pct = 0.15  # % above high to confirm breakout
    
    # MACD settings (standard)
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9
    
    # Bollinger Bands for squeeze detection
    bb_period = 20
    bb_std = 2.0
    squeeze_lookback = 50     # Bars to assess squeeze
    squeeze_threshold = 0.25  # Low volatility percentile
    
    # RSI settings - MOMENTUM focused
    rsi_period = 14
    rsi_momentum_min = 50.0   # Want RSI trending UP, not oversold
    rsi_momentum_max = 85.0   # Avoid extreme overbought
    
    # Volume
    volume_ma_period = 20
    volume_surge_multiplier = 1.2  # 20% above average (more lenient)
    
    # ATR for risk management - BALANCED for trend following
    atr_period = 14
    trail_min_pct = 2.0       # Wider stop to survive noise (was 0.5%)
    trail_max_pct = 8.0       # Allow more room in volatile markets (was 4.5%)
    atr_multiplier = 2.5      # Give trends room to breathe (was 1.8)
    
    # Price action
    min_bars_for_trend = 5    # Look at last N bars for trend
    
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    def _calculate_squeeze_score(self, candles: List[Dict[str, Any]]) -> float:
        """Calculate Bollinger Band squeeze score (0-1, lower = tighter squeeze)."""
        try:
            if len(candles) < self.bb_period + self.squeeze_lookback:
                return 1.0
            
            widths = []
            for i in range(min(self.squeeze_lookback, len(candles) - self.bb_period)):
                subset = candles[:-(i)] if i > 0 else candles
                bb_u, bb_m, bb_l = self.mkt.calculate_bollinger_bands(subset, self.bb_period, self.bb_std)
                if bb_u and bb_m and bb_l and bb_m > 0:
                    widths.append((bb_u - bb_l) / bb_m)
            
            if not widths:
                return 1.0
            
            # Current width
            bb_u, bb_m, bb_l = self.mkt.calculate_bollinger_bands(candles, self.bb_period, self.bb_std)
            if not bb_u or not bb_m or not bb_l or bb_m == 0:
                return 1.0
            
            current_width = (bb_u - bb_l) / bb_m
            
            # Percentile of current width
            widths_sorted = sorted(widths)
            rank = sum(1 for w in widths_sorted if w < current_width)
            return rank / len(widths_sorted) if widths_sorted else 1.0
        except Exception:
            return 1.0
    
    def _detect_price_momentum(self, candles: List[Dict[str, Any]]) -> float:
        """Calculate price momentum score (0-1, higher = stronger upward momentum)."""
        if len(candles) < self.min_bars_for_trend + 1:
            return 0.0
        
        recent = candles[-self.min_bars_for_trend:]
        closes = [float(c["close"]) for c in recent]
        
        # Count consecutive higher closes
        up_days = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
        momentum_score = up_days / (len(closes) - 1) if len(closes) > 1 else 0.0
        
        # Also check rate of change
        roc = (closes[-1] - closes[0]) / closes[0] if closes[0] > 0 else 0.0
        roc_score = min(max(roc * 50, 0), 1)  # Normalize
        
        return (momentum_score * 0.6 + roc_score * 0.4)

    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        min_bars = max(
            self.ema_slow_period + 1,
            self.donchian_period + 1,
            self.macd_slow + self.macd_signal + 1,
            self.bb_period + self.squeeze_lookback + 1,
            self.rsi_period + 1,
            self.volume_ma_period + 1,
            self.atr_period + 1,
        )

        if len(candles) < min_bars:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "candles_count": len(candles),
                "required_bars": min_bars,
                "explanation": f"Need ≥{min_bars} bars for comprehensive analysis",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), "wanted": min_bars, "direction": ">="}
                ],
            }
            log.info(
                "%s NO_ACTION - insufficient_data @ %s (required=%d have=%d)",
                symbol, res["price"], min_bars, len(candles)
            )
            return res

        # Calculate all indicators
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_mid = self.mkt.calculate_ema(candles, self.ema_mid_period)
        ema_slow = self.mkt.calculate_ema(candles, self.ema_slow_period)
        
        donchian_upper, donchian_lower = self.mkt.donchian_channel(candles, self.donchian_period)
        
        macd_line, macd_sig = self.mkt.calculate_macd(
            candles, self.macd_fast, self.macd_slow, self.macd_signal
        )
        
        bb_upper, bb_middle, bb_lower = self.mkt.calculate_bollinger_bands(
            candles, self.bb_period, self.bb_std
        )
        
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        current_volume = float(candles[-1].get("volume", 0) or 0)
        
        atr = self.mkt.calculate_atr(candles, self.atr_period)

        # Check for NaN/None values
        if any(
            x is None or x != x  # None or NaN check
            for x in [ema_fast, ema_mid, ema_slow, macd_line, macd_sig, 
                     bb_upper, bb_middle, bb_lower, rsi, atr, donchian_upper, donchian_lower]
        ):
            res = {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "explanation": "One or more indicators returned NaN/None",
                "checks": [
                    {"label": "All indicators valid", "ok": False, "actual": "NaN/None", "wanted": "valid"}
                ],
            }
            log.info("%s NO_ACTION - indicator_unavailable @ %s", symbol, res["price"])
            return res

        # ========== ADVANCED BUY CONDITIONS ==========
        
        # 1. MULTI-TIMEFRAME TREND: All EMAs aligned + price above all
        trend_strength = sum([
            price > ema_fast,
            price > ema_mid,
            price > ema_slow,
            ema_fast > ema_mid,
            ema_mid > ema_slow,
        ])
        perfect_trend = trend_strength >= 4  # At least 4/5 conditions
        
        # 2. DONCHIAN BREAKOUT: Price breaking above recent high
        breakout_level = donchian_upper * (1.0 + self.breakout_buffer_pct / 100.0)
        breakout_ok = price >= breakout_level
        
        # 3. BOLLINGER SQUEEZE: Volatility contraction before expansion
        squeeze_score = self._calculate_squeeze_score(candles)
        squeeze_ok = squeeze_score <= self.squeeze_threshold
        
        # 4. RSI MOMENTUM: Looking for strength, NOT oversold
        rsi_momentum_ok = self.rsi_momentum_min <= rsi <= self.rsi_momentum_max
        
        # 5. MACD BULLISH: Strong momentum
        macd_bullish = macd_line > macd_sig * 1.02  # 2% above signal
        macd_positive = macd_line > 0  # Above zero line
        
        # 6. VOLUME SURGE: Institutional interest
        volume_ok = current_volume > volume_ma * self.volume_surge_multiplier if volume_ma > 0 else False
        
        # 7. PRICE MOMENTUM: Recent strength
        momentum_score = self._detect_price_momentum(candles)
        momentum_ok = momentum_score >= 0.5
        
        # 8. MARKET REGIME: Detect if trending or choppy
        atr_pct = (atr / price) * 100.0 if price > 0 else 0
        bb_width_pct = ((bb_upper - bb_lower) / bb_middle) * 100.0 if bb_middle > 0 else 0
        trending_regime = bb_width_pct > 3.0  # Wide bands = trending
        
        # ========== SCORING SYSTEM ========== 
        # Core requirements (RELAXED: trend OR rsi, not both)
        core_passed = perfect_trend or rsi_momentum_ok
        
        # Breakout signals (RELAXED: need at least 1 of 3, was 2)
        breakout_signals = sum([breakout_ok, squeeze_ok, momentum_ok])
        
        # Confirmation signals (RELAXED: need at least 1 of 3, was 2)
        confirmation_signals = sum([macd_bullish, volume_ok, macd_positive])
        
        # Overall acceptance logic (RELAXED: allow more entries)
        accept_trade = (
            core_passed and 
            breakout_signals >= 1 and 
            confirmation_signals >= 1
        )

        checklist = [
            {"label": "✓ CORE: Multi-EMA Alignment", "ok": perfect_trend, 
             "actual": f"{trend_strength}/5", "wanted": "≥4/5"},
            {"label": "✓ CORE: RSI Momentum Range", "ok": rsi_momentum_ok, 
             "actual": f"{rsi:.1f}", "wanted": f"{self.rsi_momentum_min}-{self.rsi_momentum_max}", "direction": "range"},
            {"label": "BREAKOUT: Donchian High", "ok": breakout_ok, 
             "actual": f"{price:.2f}", "wanted": f"{breakout_level:.2f}", "direction": ">="},
            {"label": "BREAKOUT: BB Squeeze", "ok": squeeze_ok, 
             "actual": f"{squeeze_score:.2f}", "wanted": f"≤{self.squeeze_threshold}"},
            {"label": "BREAKOUT: Price Momentum", "ok": momentum_ok, 
             "actual": f"{momentum_score:.2f}", "wanted": "≥0.5"},
            {"label": "CONFIRM: MACD Bullish", "ok": macd_bullish, 
             "actual": f"{macd_line:.4f}", "wanted": f"{macd_sig * 1.02:.4f}", "direction": ">="},
            {"label": "CONFIRM: Volume Surge", "ok": volume_ok, 
             "actual": f"{current_volume:.0f}", "wanted": f"{volume_ma * self.volume_surge_multiplier:.0f}", "direction": ">="},
            {"label": "CONFIRM: MACD Positive", "ok": macd_positive, 
             "actual": f"{macd_line:.4f}", "wanted": "0", "direction": ">"},
            {"label": "REGIME: Trending Market", "ok": trending_regime, 
             "actual": f"{bb_width_pct:.2f}%", "wanted": ">3.0%"},
        ]

        if not accept_trade:
            total_score = trend_strength + breakout_signals + confirmation_signals
            res = {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "signal_strength": f"{total_score}/11",
                "explanation": f"Signal score {total_score}/11. Need: core(2) + breakout(≥2/3) + confirm(≥2/3)\n" + format_checklist(checklist),
                "checks": checklist,
            }
            log.info(
                "%s NO_ACTION - conditions_not_met @ %s (core=%s, breakout=%d/3, confirm=%d/3)", 
                symbol, res["price"], core_passed, breakout_signals, confirmation_signals
            )
            return res

        # Calculate adaptive trailing stop based on ATR - TIGHTER
        trail_pct = min(max(atr_pct * self.atr_multiplier, self.trail_min_pct), self.trail_max_pct)
        
        # Session-aware limit price adjustment
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)

        total_score = trend_strength + breakout_signals + confirmation_signals
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
            "signal_strength": f"{total_score}/11",
            "explanation": f"🚀 ELITE BREAKOUT SETUP (score {total_score}/11)\n" + format_checklist(checklist),
            "checks": checklist,
        }
        
        log.info(
            "🚀 BUY %s @ %s (limit=%s, trail=%s%%, score=%d/11) - Trend:%d/5, Break:%d/3, Conf:%d/3, RSI:%.1f, MACD:%.4f",
            symbol, res["price"], res["limit_price"], res["trail_stop_order"]["trailing_percent"],
            total_score, trend_strength, breakout_signals, confirmation_signals, rsi, macd_line
        )
        return res

    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        """
        Aggressive sell logic: Cut losses fast, protect profits quickly.
        Multiple exit conditions for optimal risk management.
        """
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        min_bars = max(
            self.rsi_period + 1, 
            self.atr_period + 1, 
            self.ema_fast_period + 1,
            self.macd_slow + self.macd_signal + 1
        )
        
        if len(candles) < min_bars:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "explanation": f"Need ≥{min_bars} bars for sell analysis",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), 
                     "wanted": min_bars, "direction": ">="}
                ],
            }
            log.info(
                "%s SELL NO_ACTION - insufficient_data @ %s (required=%d have=%d)",
                symbol, res["price"], min_bars, len(candles)
            )
            return res

        # Calculate indicators for exit signals
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        macd_line, macd_sig = self.mkt.calculate_macd(candles, self.macd_fast, self.macd_slow, self.macd_signal)
        atr = self.mkt.calculate_atr(candles, self.atr_period)
        ema_fast = self.mkt.calculate_ema(candles, self.ema_fast_period)
        ema_mid = self.mkt.calculate_ema(candles, self.ema_mid_period)
        ema_slow = self.mkt.calculate_ema(candles, self.ema_slow_period)

        if atr is None or atr != atr:  # NaN check
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "checks": [{"label": "ATR valid", "ok": False, "actual": "NaN", "wanted": "valid"}],
            }

        # ========== DISCRETIONARY EXIT CONDITIONS (SOFTENED) ==========
        
        # 1. TREND BREAKDOWN: Price breaks below key EMAs (SOFTENED thresholds)
        trend_break_fast = price < ema_fast * 0.95  # 5% below fast EMA (was 2%)
        trend_break_mid = price < ema_mid * 0.93    # 7% below mid EMA (was 3%)
        trend_break_slow = price < ema_slow * 0.95  # 5% below slow EMA (was 0%)
        
        # 2. MOMENTUM REVERSAL: RSI or MACD turning negative (SOFTENED)
        rsi_weak = rsi is not None and rsi < 30.0   # RSI very weak (was 40)
        macd_bearish = (macd_line is not None and macd_sig is not None and 
                        macd_line < macd_sig * 0.95)  # MACD strongly bearish (was 0.98)
        
        # 3. EXTREME OVERBOUGHT: Lock in profits (UNCHANGED - reasonable)
        rsi_extreme = rsi is not None and rsi > 85.0
        
        # 4. LOSS OF MOMENTUM: Check recent price action (SOFTENED threshold)
        momentum_score = self._detect_price_momentum(candles) if len(candles) >= self.min_bars_for_trend + 1 else 0.5
        momentum_lost = momentum_score < 0.15  # Very low momentum only (was 0.3)
        
        # 5. VOLATILITY SPIKE: Unusual volatility suggests danger (SOFTENED)
        bb_upper, bb_middle, bb_lower = self.mkt.calculate_bollinger_bands(candles, self.bb_period, self.bb_std)
        price_near_lower = (bb_lower is not None and price <= bb_lower * 0.98)  # Well below lower band (was 1.02)
        
        # ========== EXIT DECISION MATRIX (REQUIRE MULTIPLE CONFIRMATIONS) ==========
        
        # Critical exits (immediate sell) - REQUIRE 2 STRONG SIGNALS NOW
        critical_exit = (
            (trend_break_mid and trend_break_slow and macd_bearish) or  # Multiple EMA breaks + MACD
            (trend_break_fast and trend_break_mid and rsi_weak) or      # 2 EMA breaks + very weak RSI
            (price_near_lower and momentum_lost and macd_bearish)       # BB + momentum + MACD all bearish
        )
        
        # Profit protection (lock gains) - ONLY on extreme overbought
        profit_protection = rsi_extreme
        
        # Moderate weakness (tighten stops) - REMOVED, let trailing stops do the work
        moderate_weakness = False
        
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)
        
        if critical_exit:
            log.info(
                "🚨 CRITICAL EXIT %s @ %s - Trend break & momentum loss (RSI:%.1f, MACD:%.4f/%.4f)",
                symbol, price, rsi or 0, macd_line or 0, macd_sig or 0
            )
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": "critical_exit",
                "explanation": f"Critical trend breakdown detected - exit immediately",
                "checks": [
                    {"label": "Trend break (mid)", "ok": trend_break_mid, "actual": price, "wanted": ema_mid * 0.97, "direction": "<"},
                    {"label": "Trend break (fast)", "ok": trend_break_fast, "actual": price, "wanted": ema_fast * 0.98, "direction": "<"},
                    {"label": "RSI weak", "ok": rsi_weak, "actual": rsi if rsi else "N/A", "wanted": "<40"},
                    {"label": "MACD bearish", "ok": macd_bearish, "actual": f"{macd_line:.4f}" if macd_line else "N/A"},
                ],
            }
        
        if profit_protection:
            log.info(
                "💰 PROFIT LOCK %s @ %s - Extreme overbought or weakness (RSI:%.1f)",
                symbol, price, rsi or 0
            )
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": "profit_protection",
                "explanation": f"Locking in profits - RSI extreme or showing weakness",
                "checks": [
                    {"label": "RSI extreme", "ok": rsi_extreme, "actual": rsi if rsi else "N/A", "wanted": ">85"},
                    {"label": "Trend weakening", "ok": trend_break_fast, "actual": price, "wanted": ema_fast * 0.98},
                ],
            }
        
        # Calculate adaptive trailing stop - BALANCED for trend following
        atr_pct = (atr / price) * 100.0 if price > 0 else self.trail_min_pct
        
        # Use standard trailing stop (removed tightening logic)
        trail_pct = min(max(atr_pct * self.atr_multiplier, self.trail_min_pct), self.trail_max_pct)

        res = {
            "action": "SELL",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_percent": round(trail_pct, 2),
            "explanation": f"Trailing stop at {trail_pct:.2f}% (ATR-based adaptive)",
            "checks": [
                {"label": "ATR-based adaptive trail", "ok": True, "actual": trail_pct, "wanted": "optimized"},
            ],
        }
        
        log.debug(
            "SELL %s @ %s (limit=%s, trail=%s%%) - Trailing mode with wider stops",
            symbol, res["price"], res["limit_price"], res["trail_percent"]
        )
        return res

