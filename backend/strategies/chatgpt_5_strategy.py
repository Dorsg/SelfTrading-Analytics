from __future__ import annotations

import os
from math import floor
from typing import Any, Dict, Optional

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_actual_vs_wanted, format_checklist
from strategies.runner_decision_info import RunnerDecisionInfo
import logging

log = logging.getLogger("chatgpt-5-strategy")

class ChatGPT5Strategy:
    """
    Long-only trend breakout with:
      • Donchian breakout trigger + long MA trend filter
      • RSI momentum window (internal config)
      • ATR-based TRAIL_LIMIT attached on BUY
      • Optional user take-profit accelerator via runner.parameters["take_profit"]
      • Global guards can still use runner.parameters["stop_loss"] externally

    Notes:
      - All indicator periods and thresholds live inside this class (no UI needed).
      - User parameters expected from the UI:
          • take_profit  (number, percent > 0)    -> optional accelerator
          • stop_loss    (negative percent, e.g., -5) -> consumed by global guards
    """

    name = "ChatGPT5Strategy"

    # ── Internal configuration (self-contained) ───────────────────────────────
    breakout_lookback = 20        # Donchian upper channel lookback
    long_ma_period    = 20        # long MA for trend filter (reduced for better data availability)
    atr_period        = 14        # ATR lookback
    rsi_period        = 14        # RSI lookback
    rsi_min           = 50.0      # require momentum ≥ 50
    rsi_max           = 80.0      # avoid buying into extreme overbought

    buy_buffer_pct    = 0.10      # trigger requires price ≥ (upper * (1 + 0.10%))

    # clamp the resulting trail% (derived from ATR)
    trail_min_pct     = 0.75
    trail_max_pct     = 8.0

    # limit price "wiggle" vs last, depends on session
    limit_wiggle_rth  = 0.0005    # 0.05%
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))  # 2%

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    # ─────────────────────────── BUY ────────────────────────────
    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol  = (getattr(info.runner, "stock", None) or "").upper()
        price   = float(info.current_price)
        candles = info.candles or []

        # need sufficient bars for all indicators
        min_bars = max(
            self.breakout_lookback + 1,
            self.long_ma_period + 1,
            self.atr_period + 1,
            self.rsi_period + 1,
        )
        if len(candles) < min_bars:
            result = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "candles_count": len(candles),
                "required_bars": min_bars,
                "explanation": f"NO BUY SIGNAL - Insufficient data: {len(candles)} price bars available, need ≥{min_bars} bars for trend analysis",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # indicators
        upper, _ = self.mkt.donchian_channel(candles, self.breakout_lookback)
        long_ma  = self.mkt.calculate_sma(candles, self.long_ma_period)
        atr_val  = self.mkt.calculate_atr(candles, period=self.atr_period)
        rsi_val  = self.mkt.calculate_rsi(candles, period=self.rsi_period)

        if upper is None or long_ma is None or atr_val is None or rsi_val is None:
            missing_indicators = []
            if upper is None:
                missing_indicators.append("Donchian Channel")
            if long_ma is None:
                missing_indicators.append("Long MA")
            if atr_val is None:
                missing_indicators.append("ATR")
            if rsi_val is None:
                missing_indicators.append("RSI")
            
            result = {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "missing_indicators": missing_indicators,
                "explanation": f"NO BUY SIGNAL - Missing indicators: {', '.join(missing_indicators)}. Need all indicators to evaluate breakout.",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # ── BUY CONDITIONS ────────────────────────────────────────────────────
        # 1. Price must be above long MA (trend filter)
        trend_ok = price > long_ma

        # 2. Price must break above Donchian upper with buffer
        breakout_level = upper * (1 + self.buy_buffer_pct)
        breakout_ok = price >= breakout_level

        # 3. RSI must be in momentum range
        momentum_ok = self.rsi_min <= rsi_val <= self.rsi_max

        # ── DECISION ──────────────────────────────────────────────────────────
        checklist = [
            {"label": "Trend (price > MA long)", "ok": trend_ok, "actual": price, "wanted": long_ma},
            {"label": "Breakout (price ≥ upper + buffer)", "ok": breakout_ok, "actual": price, "wanted": breakout_level},
            {"label": "Momentum (RSI in range)", "ok": momentum_ok, "actual": rsi_val, "wanted": (self.rsi_min, self.rsi_max)},
        ]

        if not all(item["ok"] for item in checklist):
            result = {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "explanation": format_checklist(checklist),
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # ── BUY SIGNAL ────────────────────────────────────────────────────────
        # Calculate ATR-based trailing stop
        trail_pct = min(max((atr_val / price) * 100, self.trail_min_pct), self.trail_max_pct)

        # Determine limit wiggle based on session (sync: consult last known session)
        session  = self.mkt._last_session[1] if getattr(self.mkt, "_last_session", None) else None
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 + wiggle)

        result = {
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
        }

        self._log_decision(logging.INFO, symbol, result)
        return result

    # ─────────────────────────── SELL ───────────────────────────
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        # need sufficient bars for ATR
        if len(candles) < self.atr_period + 1:
            result = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "candles_count": len(candles),
                "required_bars": self.atr_period + 1,
                "explanation": f"NO SELL SIGNAL - Insufficient data: {len(candles)} price bars available, need ≥{self.atr_period + 1} bars for ATR calculation",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # Calculate ATR for trailing stop
        atr_val = self.mkt.calculate_atr(candles, period=self.atr_period)
        if atr_val is None:
            result = {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "explanation": "NO SELL SIGNAL - ATR indicator unavailable for trailing stop calculation",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # Calculate trailing stop percentage
        trail_pct = min(max((atr_val / price) * 100, self.trail_min_pct), self.trail_max_pct)

        # Determine limit wiggle based on session (sync: consult last known session)
        session  = self.mkt._last_session[1] if getattr(self.mkt, "_last_session", None) else None
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)

        result = {
            "action": "SELL",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_percent": round(trail_pct, 2),
            "explanation": f"SELL SIGNAL - Trailing stop at {trail_pct:.2f}% below current price",
        }

        self._log_decision(logging.INFO, symbol, result)
        return result

    def _log_decision(self, level: int, symbol: str, result: Dict[str, Any]) -> None:
        """Log decision with consistent format"""
        action = result.get("action", "UNKNOWN")
        reason = result.get("reason", "")
        price = result.get("price", 0)
        
        if reason:
            log.log(level, f"{symbol} {action} - {reason} @ {price}")
        else:
            log.log(level, f"{symbol} {action} @ {price}")



