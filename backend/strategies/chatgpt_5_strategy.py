from __future__ import annotations

import os
import logging
from typing import Any, Dict

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from backend.strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("chatgpt-5-strategy")


class ChatGPT5Strategy:
    """
    Long-only trend breakout with:
      • Donchian breakout trigger + long MA trend filter
      • RSI momentum window
      • ATR-based trailing stop attached on BUY (analytics-friendly)
    """

    name = "ChatGPT5Strategy"

    breakout_lookback = 20
    long_ma_period = 20
    atr_period = 14
    rsi_period = 14
    rsi_min = 50.0
    rsi_max = 80.0

    # Expressed in PERCENT (e.g., 0.10 -> 0.10%)
    buy_buffer_pct = 0.10

    trail_min_pct = 0.75
    trail_max_pct = 8.0

    limit_wiggle_rth = 0.0005
    limit_wiggle_xrth = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    # ─────────────────────────── BUY ────────────────────────────
    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        min_bars = max(
            self.breakout_lookback + 1,
            self.long_ma_period + 1,
            self.atr_period + 1,
            self.rsi_period + 1,
        )
        if len(candles) < min_bars:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "candles_count": len(candles),
                "required_bars": min_bars,
                "explanation": f"Need ≥{min_bars} bars",
                "checks": [
                    {"label": "Minimum bars", "ok": False, "actual": len(candles), "wanted": min_bars, "direction": ">="}
                ],
            }
            log.info(
                "%s NO_ACTION - insufficient_data @ %s (required=%d have=%d)",
                symbol, res["price"], min_bars, len(candles)
            )
            return res

        upper, lower = self.mkt.donchian_channel(candles, self.breakout_lookback)
        long_ma = self.mkt.calculate_sma(candles, self.long_ma_period)
        atr_val = self.mkt.calculate_atr(candles, period=self.atr_period)
        rsi_val = self.mkt.calculate_rsi(candles, period=self.rsi_period)

        if upper is None or long_ma != long_ma or atr_val != atr_val or rsi_val != rsi_val:
            res = {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "explanation": "Missing/NaN indicators",
                "checks": [
                    {"label": "Indicators available", "ok": False, "actual": "NaN/missing", "wanted": "valid"}
                ],
            }
            log.info("%s NO_ACTION - indicator_unavailable @ %s", symbol, res["price"])
            return res

        # percent-based buffer (e.g., 0.10 -> 0.10%)
        breakout_level = upper * (1.0 + (self.buy_buffer_pct / 100.0))
        trend_ok = price > long_ma
        breakout_ok = price >= breakout_level
        momentum_ok = self.rsi_min <= rsi_val <= self.rsi_max

        checklist = [
            {"label": "Trend (price > MA long)", "ok": trend_ok, "actual": price, "wanted": long_ma},
            {"label": "Breakout (price ≥ Donchian+buf)", "ok": breakout_ok, "actual": price, "wanted": breakout_level},
            {"label": "Momentum (RSI range)", "ok": momentum_ok, "actual": rsi_val, "wanted": (self.rsi_min, self.rsi_max), "direction": "range"},
        ]

        if not all(i["ok"] for i in checklist):
            res = {
                "action": "NO_ACTION",
                "reason": "conditions_not_met",
                "price": round(price, 4),
                "explanation": format_checklist(checklist),
                "checks": checklist,
            }
            log.info("%s NO_ACTION - conditions_not_met @ %s", symbol, res["price"])
            return res

        trail_pct = min(max((atr_val / price) * 100.0, self.trail_min_pct), self.trail_max_pct)
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
            "explanation": format_checklist(checklist),
            "checks": checklist,
        }
        log.debug(
            "BUY %s price=%s limit=%s trail_pct=%s",
            symbol,
            res["price"],
            res["limit_price"],
            res["trail_stop_order"]["trailing_percent"],
        )
        return res

    # ─────────────────────────── SELL ───────────────────────────
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        candles = info.candles or []

        if len(candles) < self.atr_period + 1:
            res = {
                "action": "NO_ACTION",
                "reason": "insufficient_data",
                "price": round(price, 4),
                "explanation": f"Need ≥{self.atr_period + 1} bars for ATR",
                "checks": [
                    {"label": "Minimum bars for ATR", "ok": False, "actual": len(candles), "wanted": self.atr_period + 1, "direction": ">="}
                ],
            }
            log.info(
                "%s NO_ACTION - insufficient_data @ %s (required=%d have=%d)",
                symbol, res["price"], self.atr_period + 1, len(candles)
            )
            return res

        atr_val = self.mkt.calculate_atr(candles, period=self.atr_period)
        if atr_val != atr_val:  # NaN
            return {
                "action": "NO_ACTION",
                "reason": "indicator_unavailable",
                "price": round(price, 4),
                "checks": [{"label": "ATR valid", "ok": False, "actual": "NaN", "wanted": "valid"}],
            }

        trail_pct = min(max((atr_val / price) * 100.0, self.trail_min_pct), self.trail_max_pct)
        session = getattr(self.mkt, "_last_session", (None, "regular-hours"))[1]
        wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth
        limit_price = price * (1 - wiggle)

        res = {
            "action": "SELL",
            "order_type": "LMT",
            "price": round(price, 4),
            "limit_price": round(limit_price, 4),
            "trail_percent": round(trail_pct, 2),
            "explanation": f"Trailing stop at {trail_pct:.2f}%",
            "checks": [{"label": "ATR-based trail", "ok": True, "actual": trail_pct, "wanted": "within min/max"}],
        }
        log.debug("SELL %s price=%s limit=%s trail_pct=%s", symbol, res["price"], res["limit_price"], res["trail_percent"])
        return res
