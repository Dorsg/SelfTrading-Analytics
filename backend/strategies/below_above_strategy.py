from __future__ import annotations

from datetime import datetime, timezone
import logging
from math import floor
import os
from typing import Any, Dict, Optional

from backend.ib_manager.market_data_manager import MarketDataManager
from strategies.runner_decision_info import RunnerDecisionInfo
from backend.strategies.explain import format_actual_vs_wanted

log = logging.getLogger("below-above-strategy")


class BelowAboveStrategy:
    """
    Buy ABOVE → Sell BELOW/TP strategy with ATR-based TRAIL-LIMIT protection.

    • Each decision (BUY/SELL/NO_ACTION) is ALWAYS accompanied by a rich
      `details` dictionary so the UI can surface *exactly* **why** something
      happened (price, trigger, budget, ATR, …).  This eliminates all the
      “why did it skip?” guess‑work our users rightfully complained about.

      The timeline component now simply renders `details.explanation` for any
      status ≠ order_placed / trade_executed.
    """

    name = "BelowAboveStrategy"

    # trigger buffers
    above_buffer = 0.0015  # +0.15 %
    below_buffer = 0.0015  # –0.15 %

    limit_wiggle_rth   = 0.0005                    # 0 .05  %
    limit_wiggle_xrth  = float(os.getenv("XRTH_LIMIT_WIGGLE", "0.02"))   # 2 %

    # how tight we set the limit vs. last price (0.05 % default)
    limit_wiggle = 0.0005

    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()

    # ─────────────────────────── BUY ────────────────────────────
    def decide_buy(self, info: RunnerDecisionInfo) -> Optional[Dict[str, Any]]:
        """
        BUY when price breaks **above** the configured trigger.

        Each NO_ACTION return now contains:
            • current `price`
            • calculated `trigger_price`
            • human‑friendly `explanation`
        """
        symbol   = info.runner.stock.upper()
        price    = float(info.current_price)
        params   = info.runner.parameters or {}
        above_buy = params.get("above_buy")

        if above_buy is None:
            result = {
                "action": "NO_ACTION",
                "reason": "missing_params",
                "explanation": "'above_buy' parameter missing"
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result

        session  = self.mkt._last_session[1] if getattr(self.mkt, "_last_session", None) else None
        limit_wiggle = self.limit_wiggle_xrth if session == "extended-hours" else self.limit_wiggle_rth

        trigger_price = above_buy * (1 + self.above_buffer)
        if price < trigger_price:
            explanation_core = format_actual_vs_wanted([
                {
                    "actual_label": "price",
                    "actual": price,
                    "wanted_label": "breakout trigger",
                    "wanted": trigger_price,
                    "direction": ">=",
                }
            ])
            result = {
                "action": "NO_ACTION",
                "reason": "price_below_trigger",
                "price": price,
                "trigger_price": round(trigger_price, 4),
                "above_buy": above_buy,
                "above_buffer": self.above_buffer,
                "explanation": f"NO BUY SIGNAL - {explanation_core}"
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # --- limit price FIRST --------------
        limit_price = round(price * (1 + limit_wiggle), 2)

        # --- qty uses limit_price ----------
        qty = floor(info.runner.current_budget / limit_price)

        # clamp until we *fit* inside the budget (handles rounding issues)
        while qty > 0 and qty * limit_price > info.runner.current_budget:
            qty -= 1

        if qty <= 0:
            result = {
                "action": "NO_ACTION",
                "reason": "funds<1share",
                "price": price,
                "limit_price": limit_price,
                "budget": info.runner.current_budget,
                "explanation": f"NO BUY SIGNAL - Insufficient funds: budget=${info.runner.current_budget:.2f}, limit_price=${limit_price:.2f}. Need budget ≥ limit_price to buy at least 1 share"
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # --- ATR for trail ------------------
        atr_val = self.mkt.calculate_atr(info.candles, period=14)
        if atr_val is None:
            result = {
                "action": "NO_ACTION",
                "reason": "atr_unavailable",
                "price": price,
                "explanation": "NO BUY SIGNAL - ATR data unavailable ― cannot set trailing stop. Need sufficient price history for ATR calculation"
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result

        atr_percent = round((atr_val / price) * 100, 2)

        result = {
            "action":      "BUY",
            "quantity":    qty,
            "order_type":  "LMT",
            "limit_price": limit_price,
            "reason":      "price_above_trigger",
            "price":       price,
            "trigger_price": round(trigger_price, 4),
            "trail_stop_order": {
                "action": "SELL",
                "order_type": "TRAIL_LIMIT",
                "trailing_percent": atr_percent,
            },
            "explanation": (
                f"Break‑out: price {price:.2f} ≥ trigger {trigger_price:.2f}; "
                f"buy {qty} @≤{limit_price:.2f} with ATR {atr_percent:.2f}% trail"
            )
        }
        self._log_decision(logging.INFO, symbol, result)
        return result

    # ─────────────────────────── SELL ───────────────────────────
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        """
        SELL according to a configured trigger (supports both `sell_trigger` and `below_sell`)
        + optional take-profit.
        Always returns a detailed dict (also for NO_ACTION).
        """
        symbol = info.runner.stock.upper()
        price  = float(info.current_price)
        qty    = info.position.quantity
        params = info.runner.parameters or {}

        # Support both names
        sell_trigger   = params.get("sell_trigger")
        below_sell     = params.get("below_sell")
        stop_trigger   = sell_trigger if sell_trigger is not None else below_sell

        take_profit_pct = params.get("take_profit")

        # time-based exit wins first (if inside loop we are still not expired)
        if info.distance_from_time_limit is not None and info.distance_from_time_limit <= 0:
            limit_price = round(price * (1 - self.limit_wiggle), 2)
            result = {
                "action": "SELL",
                "quantity": qty,
                "order_type": "LMT",
                "limit_price": limit_price,
                "reason": "time_exit",
                "price": price,
                "explanation": "Runner expiry window reached – flattening position",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        if stop_trigger is None:
            result = {
                "action": "NO_ACTION",
                "reason": "missing_params",
                "price": price,
                "explanation": "sell_trigger/below_sell parameter missing"
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result

        below_trigger = stop_trigger * (1 - self.below_buffer)

        # 1) stop-loss / sell-trigger
        if price <= below_trigger:
            limit_price = round(price * (1 - self.limit_wiggle), 2)
            result = {
                "action": "SELL",
                "quantity": qty,
                "order_type": "LMT",
                "limit_price": limit_price,
                "reason": "stop_loss_triggered",
                "price": price,
                "stop_trigger": stop_trigger,
                "below_trigger": below_trigger,
                "explanation": f"Price {price:.2f} ≤ sell-trigger {below_trigger:.2f}",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result

        # 2) take-profit
        if take_profit_pct and take_profit_pct > 0:
            entry_price = float(info.position.avg_price)
            tp_level = entry_price * (1 + take_profit_pct / 100)
            if price >= tp_level:
                limit_price = round(price * (1 - self.limit_wiggle), 2)
                result = {
                    "action": "SELL",
                    "quantity": qty,
                    "order_type": "LMT",
                    "limit_price": limit_price,
                    "reason": "take_profit_triggered",
                    "price": price,
                    "tp_level": tp_level,
                    "explanation": f"Price {price:.2f} ≥ take‑profit {tp_level:.2f}",
                }
                self._log_decision(logging.INFO, symbol, result)
                return result

        # 3) hold
        explanation_core = format_actual_vs_wanted([
            {
                "actual_label": "price",
                "actual": price,
                "wanted_label": "sell trigger",
                "wanted": below_trigger,
                "direction": "<=",
            }
        ])
        result = {
            "action": "NO_ACTION",
            "reason": "price_above_threshold",
            "price": price,
            "stop_trigger": stop_trigger,
            "below_trigger": below_trigger,
            "below_buffer": self.below_buffer,
            "explanation": f"NO SELL SIGNAL - {explanation_core}",
        }
        self._log_decision(logging.INFO, symbol, result)
        return result

    def _log_decision(self, level: int, symbol: str, result: Dict[str, Any]) -> None:
        """
        Emit a single structured log line for a decision result.
        Keeps logs compact but useful for correlating with the UI.
        """
        try:
            payload = {
                "strategy": self.name,
                "symbol": symbol,
                "action": result.get("action"),
                "reason": result.get("reason"),
            }
            # Add all relevant fields for detailed analysis
            for k in (
                "price", "trigger_price", "limit_price", "quantity", "budget",
                "above_buy", "above_buffer", "stop_trigger", "below_trigger",
                "tp_level", "explanation"
            ):
                if k in result:
                    payload[k] = result[k]
            log.log(level, "%s", payload)
        except Exception:
            # Never let logging break the strategy
            log.exception("below-above: failed to emit decision log")
