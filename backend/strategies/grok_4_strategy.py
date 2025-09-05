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
    
    # Internal config - shorter periods for more frequent signals
    ma_short_period = 20      # Short MA for momentum
    ma_long_period = 20       # Long MA for trend (easier data requirement)
    rsi_period = 14
    rsi_low = 40.0            # Lower threshold for more entries
    rsi_high = 75.0           # Avoid extreme overbought
    atr_period = 14
    fib_offset_ratio = 0.10   # Reduced for more triggers
    volume_ma_period = 20     # Volume confirmation
    trail_min_pct = 0.5
    trail_max_pct = 6.0
    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))
    
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
        )
        if len(candles) < min_bars:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "explanation": f"Need at least {min_bars} bars",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), "wanted": min_bars, "direction": ">="}
                ],
            }
            log.debug("Grok4Strategy.decide_buy insufficient data symbol=%s required=%d have=%d", symbol, min_bars, len(candles))
            return res
        
        # Calculate indicators
        ma_short = self.mkt.calculate_ema(candles, self.ma_short_period)
        ma_long = self.mkt.calculate_sma(candles, self.ma_long_period)
        rsi = self.mkt.calculate_rsi(candles, self.rsi_period)
        atr = self.mkt.calculate_atr(candles, self.atr_period)
        volume_ma = self.mkt.average_volume(candles, self.volume_ma_period)
        
        # Simple fib calculation - adapt from fibonacci strategy
        high = max(c['high'] for c in candles[-50:])
        low = min(c['low'] for c in candles[-50:])
        fib_618 = high - (high - low) * 0.618
        entry_level = fib_618 * (1 + self.fib_offset_ratio)
        
        # Conditions - looser for more triggers
        trend_ok = price > ma_long and ma_short > ma_long
        momentum_ok = self.rsi_low < rsi < self.rsi_high
        volume_ok = candles[-1]['volume'] > volume_ma * 1.2  # 20% above MA
        fib_ok = price > entry_level
        
        checklist = [
            {"label": "Trend (price > MA long)", "ok": trend_ok, "actual": price, "wanted": ma_long},
            {"label": "Momentum (RSI in range)", "ok": momentum_ok, "actual": rsi, "wanted": (self.rsi_low, self.rsi_high), "direction": "range"},
            {"label": "Volume breakout", "ok": volume_ok, "actual": candles[-1]['volume'], "wanted": volume_ma * 1.2},
            {"label": "Fib entry", "ok": fib_ok, "actual": price, "wanted": entry_level},
        ]
        
        if not all(item['ok'] for item in checklist):
            res = {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }
            log.debug("Grok4Strategy.decide_buy conditions not met symbol=%s checklist=%s", symbol, checklist)
            return res
        
        # Calculate order details
        trail_pct = min(max((atr / price) * 100, self.trail_min_pct), self.trail_max_pct)
        session  = self.mkt._last_session[1] if getattr(self.mkt, "_last_session", None) else None
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
            "explanation": format_checklist(checklist),
            "checks": checklist,
        }
        log.debug("Grok4Strategy.decide_buy BUY symbol=%s price=%s limit=%s trail_pct=%s", symbol, res["price"], res["limit_price"], res["trail_stop_order"]["trailing_percent"])
        return res
    
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []
        
        if len(candles) < self.atr_period + 1:
            return {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "explanation": f"Need at least {self.atr_period + 1} bars for ATR",
                "checks": [
                    {"label": "Minimum bars for ATR", "ok": False, "actual": len(candles), "wanted": self.atr_period + 1, "direction": ">="}
                ],
            }
        
        atr = self.mkt.calculate_atr(candles, self.atr_period)
        if atr is None:
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "explanation": "ATR indicator unavailable",
                "checks": [{"label": "ATR valid", "ok": False, "actual": "None", "wanted": "valid"}],
            }
        
        trail_pct = min(max((atr / price) * 100, self.trail_min_pct), self.trail_max_pct)
        session  = self.mkt._last_session[1] if getattr(self.mkt, "_last_session", None) else None
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)
        
        return {
            "action": "SELL",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_percent": round(trail_pct, 2),
            "explanation": f"SELL SIGNAL - Trailing stop at {trail_pct:.2f}% below current price",
            "checks": [{"label": "ATR-based trail", "ok": True, "actual": trail_pct, "wanted": "within min/max"}],
        }

    def decide_refresh(self, info: RunnerDecisionInfo) -> Dict[str, Any] | None:
        return {"action": "NO_ACTION", "reason": "no_refresh_logic"}
