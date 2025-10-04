from __future__ import annotations

import os
from math import floor
from typing import Any, Dict

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo
import logging

log = logging.getLogger("grok-4-strategy")

class Grok4Strategy:
    """
    Advanced long-only strategy combining Fibonacci extensions, trend breakouts, 
    RSI momentum, and ATR-based risk management for high gains with low risk.
    Designed for frequent triggers on good stocks.
    
    Key features:
    - Multi-timeframe trend confirmation
    - Dynamic Fibonacci entries with reduced offsets for more triggers
    - Momentum filters with wider RSI range
    - Adaptive ATR trailing stops
    - Volume confirmation for breakouts
    
    All parameters hardcoded for minimal user input.
    """
    
    name = "Grok4Strategy"
    
    # Internal config - adjusted for better performance
    ma_short_period = 20      # Short EMA for momentum
    ma_long_period = 50       # Longer SMA for trend
    rsi_period = 14
    rsi_low = 30.0            # Adjusted lower
    rsi_high = 80.0           # Higher to allow momentum
    atr_period = 14
    fib_offset_ratio = 0.0    # No offset for easier fib condition
    volume_ma_period = 20
    trail_min_pct = 2.0       # Looser trail to survive noise
    trail_max_pct = 8.0
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))
    
    # New parameters
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9
    bb_period = 20
    bb_std = 2.0
    rsi_overbought = 75.0     # Tighten for sells
    take_profit_pct = 15.0    # Higher target

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()
    
    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []
        
        min_bars = max(
            self.ma_long_period + 1,
            self.rsi_period + 1,
            self.atr_period + 1,
            self.volume_ma_period + 1,
            self.macd_slow + self.macd_signal + 1,
            self.bb_period + 1,
        )
        if len(candles) < min_bars:
            return {"action": "NO_ACTION", "reason": "insufficient_data"}
        
        # Indicators
        ma_short = self.mkt.calculate_ema(candles, self.ma_short_period)
        ma_long = self.mkt.calculate_sma(candles, self.ma_long_period)
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        atr = self.mkt.calculate_atr(candles, self.atr_period)
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        macd_line, signal_line = self.mkt.calculate_macd(candles, self.macd_fast, self.macd_slow, self.macd_signal)
        bb_upper, bb_middle, bb_lower = self.mkt.calculate_bollinger_bands(candles, self.bb_period, self.bb_std)
        
        # Fib
        lookback_fib = min(50, len(candles))
        high = max(c['high'] for c in candles[-lookback_fib:])
        low = min(c['low'] for c in candles[-lookback_fib:])
        fib_50 = low + (high - low) * 0.5  # Changed to 50% for easier triggers
        entry_level = fib_50 * (1 + self.fib_offset_ratio)
        
        # Multi-TF
        higher_trend_ok = True
        if getattr(info.runner, "time_frame", 5) == 5:
            daily_lookback = 50  # Reduced
            daily_candles = self.mkt.get_candles_until(symbol, 1440, candles[-1]['ts'], lookback=daily_lookback)
            if len(daily_candles) >= daily_lookback:
                daily_ema = self.mkt.calculate_ema(daily_candles, daily_lookback)
                higher_trend_ok = price > daily_ema
            # Fallback to True if insufficient data
        
        # Conditions - require core, optional enhancers
        trend_ok = price > ma_long and ma_short > ma_long
        momentum_ok = rsi > self.rsi_low  # Only lower bound for momentum
        macd_ok = macd_line > signal_line if macd_line is not None and signal_line is not None else False
        fib_ok = price > entry_level
        
        # Enhancers (at least one for entry)
        volume_ok = candles[-1]['volume'] > volume_ma * 1.1
        bb_breakout_ok = price > bb_upper if bb_upper is not None else False
        
        core_checks = [
            {"label": "Trend", "ok": trend_ok},
            {"label": "Momentum (RSI > 30)", "ok": momentum_ok},
            {"label": "MACD bullish", "ok": macd_ok},
            {"label": "Fib extension", "ok": fib_ok},
            {"label": "Higher TF", "ok": higher_trend_ok},
        ]
        
        enhancer_checks = [
            {"label": "Volume surge", "ok": volume_ok},
            {"label": "BB breakout", "ok": bb_breakout_ok},
        ]
        
        checklist = core_checks + enhancer_checks
        
        core_met = sum(1 for c in core_checks if c['ok']) >= 4  # Relaxed: 4/5 core
        if not core_met or not any(c['ok'] for c in enhancer_checks):
            return {"action": "NO_ACTION", "reason": "conditions_not_met", "checks": checklist}
        
        trail_pct = min(max((atr / price) * 100 * 1.5, self.trail_min_pct), self.trail_max_pct)  # Wider trail
        
        session = self.mkt._last_session[1] if hasattr(self.mkt, "_last_session") else "regular-hours"
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)
        
        return {
            "action": "BUY",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_stop_order": {
                "action": "SELL",
                "order_type": "TRAIL_LIMIT",
                "trailing_percent": round(trail_pct, 2),
            },
            "explanation": format_checklist(checklist),
            "checks": checklist,
        }
    
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []
        position = info.position
        
        if len(candles) < max(self.rsi_period, self.macd_slow + self.macd_signal) or position is None:
            return {"action": "NO_ACTION", "reason": "insufficient_data_or_no_position"}
        
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        macd_line, signal_line = self.mkt.calculate_macd(candles, self.macd_fast, self.macd_slow, self.macd_signal)
        
        current_gain = ((price - position.avg_price) / position.avg_price * 100) if position.avg_price > 0 else 0
        
        overbought = rsi > self.rsi_overbought
        bearish_macd = macd_line < signal_line if macd_line is not None and signal_line is not None else False
        take_profit = current_gain > self.take_profit_pct
        
        checklist = [
            {"label": f"Overbought (RSI > {self.rsi_overbought})", "ok": overbought},
            {"label": "Bearish MACD", "ok": bearish_macd},
            {"label": f"Take profit (>{self.take_profit_pct}%)", "ok": take_profit},
        ]
        
        if overbought and bearish_macd or take_profit:  # Require confirmation for overbought
            session = self.mkt._last_session[1] if hasattr(self.mkt, "_last_session") else "regular-hours"
            wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
            limit_price = price * (1 - wiggle)
            
            return {
                "action": "SELL",
                "order_type": "LMT",
                "price": round(price, 4),
                "limit_price": round(limit_price, 4),
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }
        
        return {"action": "NO_ACTION", "reason": "no_sell_signal"}

    def decide_refresh(self, info: RunnerDecisionInfo) -> Dict[str, Any] | None:
        return {"action": "NO_ACTION", "reason": "no_refresh_logic"}
