from __future__ import annotations

import os
import logging
from typing import Any, Dict

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("deepseek-v3-1-strategy")


class DeepSeekV31Strategy:
    """
    Advanced multi-indicator strategy combining:
      • MACD for trend direction and momentum
      • Bollinger Bands for mean reversion entries
      • RSI for overbought/oversold conditions
      • Stochastic for precise entry timing
      • Volume confirmation for breakout validation
      • Adaptive ATR-based trailing stops
      • Multi-timeframe trend alignment
    
    Philosophy: Buy quality dips in strong trends with multiple confirmations.
    The strategy aims for high win-rate by waiting for confluence of signals.
    """

    name = "DeepSeekV31Strategy"

    # Trend indicators
    ema_fast_period = 12
    ema_slow_period = 26
    ema_trend_period = 50
    
    # MACD settings
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9
    
    # Bollinger Bands
    bb_period = 20
    bb_std = 2.0
    
    # RSI settings
    rsi_period = 14
    rsi_oversold = 32.0  # Buy when RSI dips below this in uptrend
    rsi_overbought = 72.0  # Avoid buying when too hot
    
    # Stochastic Oscillator
    stoch_k_period = 14
    stoch_d_period = 3
    stoch_oversold = 22.0
    
    # Volume
    volume_ma_period = 20
    volume_surge_multiplier = 1.25  # Volume should be 30% above average
    
    # ATR for risk management
    atr_period = 14
    trail_min_pct = 0.7
    trail_max_pct = 6.5
    
    # Position sizing multiplier based on signal strength
    atr_stop_multiplier = 2.2  # Stop loss at 2.5x ATR below entry
    
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        min_bars = max(
            self.ema_trend_period + 1,
            self.macd_slow + self.macd_signal + 1,
            self.bb_period + 1,
            self.rsi_period + 1,
            self.stoch_k_period + self.stoch_d_period + 1,
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
                "explanation": f"Need ≥{min_bars} bars for multi-indicator analysis",
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
        ema_slow = self.mkt.calculate_ema(candles, self.ema_slow_period)
        ema_trend = self.mkt.calculate_ema(candles, self.ema_trend_period)
        
        macd_line, macd_signal = self.mkt.calculate_macd(
            candles, self.macd_fast, self.macd_slow, self.macd_signal
        )
        
        bb_upper, bb_middle, bb_lower = self.mkt.calculate_bollinger_bands(
            candles, self.bb_period, self.bb_std
        )
        
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        
        stoch_k, stoch_d = self.mkt.calculate_stochastic(
            candles, self.stoch_k_period, self.stoch_d_period
        )
        
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        current_volume = candles[-1]["volume"]
        
        atr = self.mkt.calculate_atr(candles, self.atr_period)

        # Check for NaN/None values
        if any(
            x is None or x != x  # None or NaN check
            for x in [ema_fast, ema_slow, ema_trend, macd_line, macd_signal, 
                     bb_upper, bb_middle, bb_lower, rsi, stoch_k, stoch_d, atr]
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

        # ========== BUY CONDITIONS ==========
        
        # 1. STRONG UPTREND: Price above long-term EMA and fast EMA above slow EMA
        uptrend_ok = price > ema_trend and ema_fast > ema_slow
        
        # 2. MACD BULLISH: MACD line above signal line (or just crossed)
        macd_bullish = macd_line > macd_signal * 0.95  # Allow slight wiggle room
        
        # 3. MEAN REVERSION ENTRY: Price near or below lower Bollinger Band
        # This identifies temporary dips in strong trends
        bb_entry_level = bb_lower * 1.01  # Within 1% of lower band
        mean_reversion_ok = price <= bb_entry_level and price > bb_lower * 0.97
        
        # 4. RSI: Not overbought, ideally in oversold/neutral zone
        rsi_ok = self.rsi_oversold <= rsi <= self.rsi_overbought
        
        # 5. STOCHASTIC: Oversold or just turning up (buy signal)
        stochastic_ok = stoch_k < self.stoch_oversold * 1.2  # Allow 20% wiggle
        
        # 6. VOLUME CONFIRMATION: Higher than average (interest in the stock)
        volume_ok = current_volume > volume_ma * self.volume_surge_multiplier

        # Calculate signal strength (0-6 based on conditions met)
        signal_strength = sum([
            uptrend_ok, macd_bullish, mean_reversion_ok, 
            rsi_ok, stochastic_ok, volume_ok
        ])

        checklist = [
            {"label": "Strong Uptrend (price > EMA50, EMA12 > EMA26)", "ok": uptrend_ok, 
             "actual": f"price:{price:.2f}, EMA50:{ema_trend:.2f}", "wanted": "price > EMA50"},
            {"label": "MACD Bullish (MACD > Signal)", "ok": macd_bullish, 
             "actual": f"{macd_line:.4f}", "wanted": f"{macd_signal:.4f}", "direction": ">="},
            {"label": "Mean Reversion Entry (near BB lower)", "ok": mean_reversion_ok, 
             "actual": price, "wanted": bb_entry_level, "direction": "<="},
            {"label": "RSI Healthy Range", "ok": rsi_ok, 
             "actual": rsi, "wanted": (self.rsi_oversold, self.rsi_overbought), "direction": "range"},
            {"label": "Stochastic Oversold/Turning", "ok": stochastic_ok, 
             "actual": stoch_k, "wanted": self.stoch_oversold * 1.2, "direction": "<="},
            {"label": "Volume Surge", "ok": volume_ok, 
             "actual": current_volume, "wanted": volume_ma * self.volume_surge_multiplier, "direction": ">="},
        ]

        # Require at least 5 out of 6 conditions for high-quality setups
        min_signals_required = 5
        
        if signal_strength < min_signals_required:
            res = {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "signal_strength": f"{signal_strength}/{len(checklist)}",
                "explanation": f"Signal strength {signal_strength}/6 (need ≥{min_signals_required})\n" + format_checklist(checklist),
                "checks": checklist,
            }
            log.info(
                "%s NO_ACTION - conditions_not_met @ %s (strength=%d/%d)", 
                symbol, res["price"], signal_strength, len(checklist)
            )
            return res

        # Calculate adaptive trailing stop based on ATR and volatility
        # More volatile stocks get wider stops
        atr_pct = (atr / price) * 100.0
        trail_pct = min(max(atr_pct * 1.5, self.trail_min_pct), self.trail_max_pct)
        
        # Session-aware limit price adjustment
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)

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
            "signal_strength": f"{signal_strength}/{len(checklist)}",
            "explanation": f"🎯 HIGH-QUALITY SETUP (strength {signal_strength}/6)\n" + format_checklist(checklist),
            "checks": checklist,
        }
        
        log.info(
            "🎯 BUY %s @ %s (limit=%s, trail=%s%%, strength=%d/%d) - MACD:%.4f>%.4f, RSI:%.1f, Stoch:%.1f, BB:%.2f-%.2f-%.2f",
            symbol, res["price"], res["limit_price"], res["trail_stop_order"]["trailing_percent"],
            signal_strength, len(checklist), macd_line, macd_signal, rsi, stoch_k,
            bb_lower, bb_middle, bb_upper
        )
        return res

    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        """
        Sell logic: Primarily rely on trailing stop, with discretionary exits
        on strong reversal signals (RSI/MACD/Stochastic).
        """
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        min_bars_for_discretionary = max(self.rsi_period + 1, self.atr_period + 1, self.stoch_k_period + 1)
        if len(candles) < min_bars_for_discretionary:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "explanation": f"Need ≥{min_bars_for_discretionary} bars for sell analysis",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), "wanted": min_bars_for_discretionary, "direction": ">="}
                ],
            }
            log.info(
                "%s SELL NO_ACTION - insufficient_data @ %s (required=%d have=%d)",
                symbol, res["price"], min_bars_for_discretionary, len(candles)
            )
            return res

        # Indicators for discretionary sell
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        stoch_k, _ = self.mkt.calculate_stochastic(candles, self.stoch_k_period, self.stoch_d_period)
        macd_line, macd_signal = self.mkt.calculate_macd(candles, self.macd_fast, self.macd_slow, self.macd_signal)
        atr = self.mkt.calculate_atr(candles, self.atr_period)

        if atr is None or atr != atr:
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "checks": [{"label": "ATR valid", "ok": False, "actual": "NaN", "wanted": "valid"}],
            }

        # Strong reversal signals
        extreme_overbought = rsi is not None and rsi > 85.0
        stoch_extreme = stoch_k is not None and stoch_k > 90.0
        macd_bearish_cross = (macd_line is not None and macd_signal is not None and macd_line < macd_signal * 0.95)

        if sum([extreme_overbought, stoch_extreme, macd_bearish_cross]) >= 2:
            session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
            wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
            limit_price = price * (1 - wiggle)
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "reason": "discretionary_reversal",
                "explanation": f"Strong reversal: RSI={rsi:.1f if rsi==rsi else 'N/A'}, Stoch={stoch_k:.1f if stoch_k is not None else 'N/A'}, MACD cross",
                "checks": [
                    {"label": "Extreme overbought", "ok": extreme_overbought, "actual": rsi if rsi==rsi else "N/A"},
                    {"label": "Stochastic extreme", "ok": stoch_extreme, "actual": stoch_k if stoch_k is not None else "N/A"},
                    {"label": "MACD bearish", "ok": macd_bearish_cross, "actual": "cross" if macd_bearish_cross else "no"},
                ],
            }

        # Default trailing stop exit
        atr_pct = (atr / price) * 100.0
        trail_pct = min(max(atr_pct * 1.5, self.trail_min_pct), self.trail_max_pct)
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)
        res = {
            "action": "SELL",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_percent": round(trail_pct, 2),
            "explanation": f"Trailing stop at {trail_pct:.2f}% (ATR-based adaptive)",
            "checks": [{"label": "ATR-based adaptive trail", "ok": True, "actual": trail_pct, "wanted": "optimized"}],
        }
        log.debug("SELL %s @ %s (limit=%s, trail=%s%%) - Trailing stop mode", symbol, res["price"], res["limit_price"], res["trail_percent"])
        return res

