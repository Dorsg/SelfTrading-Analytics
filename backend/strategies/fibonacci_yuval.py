from __future__ import annotations

import logging
from math import floor
from typing import Any, Dict, Optional

from backend.ib_manager.market_data_manager import MarketDataManager
from backend.strategies.explain import format_checklist
from strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("fibonacci-yuval-strategy")


class FibonacciYuvalStrategy:
    """
    Fibonacci-based trading strategy with static stop losses.
    
    This strategy uses Fibonacci retracement levels (38.2%, 50%, 61.8%) to identify
    entry and exit points. It maintains position awareness to manage exits differently
    based on the entry level.
    
    Parameters from UI:
    - fib_high: The high price for Fibonacci calculation
    - fib_low: The low price for Fibonacci calculation
    - stop_loss: Global stop loss percentage (handled by guards)
    - take_profit: Optional take profit percentage (handled by guards)
    
    Strategy rules:
    - BUY when price crosses above specific offset levels
    - SELL based on position and price crossing below offset levels
    - Uses 25% of ladder window as offset for trigger levels
    - Static stop losses at Fibonacci offset levels (no trailing)
    """
    
    name = "FibonacciYuvalStrategy"
    
    # Fibonacci retracement levels
    FIB_382 = 0.382  # 38.2%
    FIB_500 = 0.500  # 50.0%
    FIB_618 = 0.618  # 61.8%
    
    # Offset percentage of ladder window
    OFFSET_PCT = 0.25  # 25% of ladder window
    
    # Position state markers
    POSITION_50 = "fib_position_50"
    POSITION_618 = "fib_position_618"
    
    def __init__(self, market_data: MarketDataManager | None = None) -> None:
        self.mkt = market_data or MarketDataManager()
    
    def _calculate_fibonacci_levels(self, fib_high: float, fib_low: float) -> Dict[str, float]:
        """Calculate Fibonacci retracement levels."""
        diff = fib_high - fib_low
        
        levels = {
            "high": fib_high,
            "low": fib_low,
            "382": fib_high - diff * self.FIB_382,  # Retracement from high
            "500": fib_high - diff * self.FIB_500,  # Retracement from high
            "618": fib_high - diff * self.FIB_618,  # Retracement from high
            "diff": diff,
        }
        
        # Calculate ladder window (distance between consecutive levels)
        # Using the distance between 38.2% and 50% levels as the standard window
        # Since levels are now in descending order, 38.2% is above 50%
        ladder_window = levels["382"] - levels["500"]
        levels["ladder_window"] = ladder_window
        
        # Calculate offsets (25% of ladder window)
        offset = ladder_window * self.OFFSET_PCT
        levels["offset"] = offset
        
        # Calculate trigger levels
        levels["offset_above_50"] = levels["500"] + offset
        levels["offset_below_50"] = levels["500"] - offset
        levels["offset_above_618"] = levels["618"] + offset
        levels["offset_below_618"] = levels["618"] - offset
        
        return levels
    
    def _get_price_zone(self, price: float, levels: Dict[str, float]) -> str:
        """Determine which Fibonacci zone the price is in."""
        if price < levels["382"]:
            return "below_382"
        elif price < levels["500"]:
            return "382_to_500"
        elif price < levels["618"]:
            return "500_to_618"
        else:
            return "above_618"
    
    # ─────────────────────────── BUY ────────────────────────────
    def decide_buy(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        params = info.runner.parameters or {}
        
        # Get Fibonacci parameters
        fib_high = params.get("fib_high")
        fib_low = params.get("fib_low")
        
        # Validate parameters
        if fib_high is None or fib_low is None:
            result = {
                "action": "NO_ACTION",
                "reason": "missing_parameters",
                "price": round(price, 4),
                "explanation": "NO BUY SIGNAL - Missing Fibonacci parameters. Need fib_high and fib_low to calculate levels.",
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result
        
        try:
            fib_high = float(fib_high)
            fib_low = float(fib_low)
        except (ValueError, TypeError):
            result = {
                "action": "NO_ACTION",
                "reason": "invalid_parameters",
                "price": round(price, 4),
                "explanation": "NO BUY SIGNAL - Invalid Fibonacci parameters. fib_high and fib_low must be numbers.",
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result
        
        if fib_high <= fib_low:
            result = {
                "action": "NO_ACTION",
                "reason": "invalid_parameters",
                "price": round(price, 4),
                "explanation": "NO BUY SIGNAL - Invalid Fibonacci range. fib_high must be greater than fib_low.",
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result
        
        # Calculate Fibonacci levels
        levels = self._calculate_fibonacci_levels(fib_high, fib_low)
        zone = self._get_price_zone(price, levels)
        
        # Check BUY conditions based on zone
        should_buy = False
        buy_reason = ""
        stop_loss_level = 0.0
        position_marker = ""
        
        # Check 50% level conditions (when price is in 38.2% - 50% zone)
        if zone == "382_to_500":
            if price >= levels["offset_above_50"]:
                should_buy = True
                buy_reason = "crossed_above_50_offset"
                stop_loss_level = levels["offset_below_50"]
                position_marker = self.POSITION_50
        
        # Check 61.8% level conditions (when price is in 50% - 61.8% zone)
        elif zone == "500_to_618":
            if price >= levels["offset_above_618"]:
                should_buy = True
                buy_reason = "crossed_above_618_offset"
                stop_loss_level = levels["offset_below_618"]
                position_marker = self.POSITION_618
        
        if not should_buy:
            # Build checklist for why we didn't buy
            checklist_items = []
            
            # Add current window information
            checklist_items.append({
                "label": f"Current window: {zone}",
                "ok": True,
                "actual": f"Price {price:.2f} in {zone} zone",
                "wanted": f"38.2%: {levels['382']:.2f}, 50%: {levels['500']:.2f}, 61.8%: {levels['618']:.2f}",
                "direction": "info",
                "wanted_label": "Fibonacci levels",
            })
            
            if zone == "382_to_500":
                checklist_items.append({
                    "label": "Cross above 50% offset",
                    "ok": price >= levels["offset_above_50"],
                    "actual": price,
                    "wanted": levels["offset_above_50"],
                    "direction": ">=",
                    "wanted_label": "50% offset",
                })
                checklist_items.append({
                    "label": "Buy trigger in current window",
                    "ok": False,
                    "actual": f"Need price >= {levels['offset_above_50']:.2f}",
                    "wanted": f"50% offset above trigger",
                    "direction": "info",
                    "wanted_label": "trigger",
                })
            elif zone == "500_to_618":
                checklist_items.append({
                    "label": "Cross above 61.8% offset",
                    "ok": price >= levels["offset_above_618"],
                    "actual": price,
                    "wanted": levels["offset_above_618"],
                    "direction": ">=",
                    "wanted_label": "61.8% offset",
                })
                checklist_items.append({
                    "label": "Buy trigger in current window",
                    "ok": False,
                    "actual": f"Need price >= {levels['offset_above_618']:.2f}",
                    "wanted": f"61.8% offset above trigger",
                    "direction": "info",
                    "wanted_label": "trigger",
                })
            else:
                checklist_items.append({
                    "label": f"Price in buy zone (38.2%-61.8%)",
                    "ok": False,
                    "actual": price,
                    "wanted": (levels["382"], levels["618"]),
                    "direction": "range",
                    "wanted_label": "buy zone",
                })
            
            explanation = format_checklist(checklist_items)
            
            result = {
                "action": "NO_ACTION",
                "reason": "no_buy_signal",
                "price": round(price, 4),
                "zone": zone,
                "fib_levels": {
                    "high": round(fib_high, 4),
                    "low": round(fib_low, 4),
                    "382": round(levels["382"], 4),
                    "500": round(levels["500"], 4),
                    "618": round(levels["618"], 4),
                },
                "ladder_window": round(levels["ladder_window"], 4),
                "offset": round(levels["offset"], 4),
                "offset_above_50": round(levels["offset_above_50"], 4),
                "offset_above_618": round(levels["offset_above_618"], 4),
                "explanation": explanation,
            }
            self._log_decision(logging.INFO, symbol, result)
            return result
        
        # Calculate position size
        budget = float(getattr(info.runner, "current_budget", 0.0) or 0.0)
        limit_price = round(price * 1.0005, 2)  # 0.05% wiggle room
        qty = floor(budget / max(limit_price, 0.01))
        
        while qty > 0 and qty * limit_price > budget:
            qty -= 1
        
        if qty <= 0:
            result = {
                "action": "NO_ACTION",
                "reason": "insufficient_funds",
                "price": round(price, 4),
                "limit_price": limit_price,
                "budget": budget,
                "explanation": f"NO BUY SIGNAL - Insufficient funds: budget=${budget:.2f}, limit_price=${limit_price:.2f}",
            }
            self._log_decision(logging.INFO, symbol, result)
            return result
        
        # Mark position in parameters for tracking
        # Clear any existing position markers first
        params.pop(self.POSITION_50, None)
        params.pop(self.POSITION_618, None)
        # Set the current position marker
        params[position_marker] = True
        
        result = {
            "action": "BUY",
            "quantity": qty,
            "order_type": "LMT",
            "limit_price": limit_price,
            "reason": buy_reason,
            "price": round(price, 4),
            "zone": zone,
            "position_marker": position_marker,
            "stop_loss_level": round(stop_loss_level, 4),
            "fib_levels": {
                "high": round(fib_high, 4),
                "low": round(fib_low, 4),
                "382": round(levels["382"], 4),
                "500": round(levels["500"], 4),
                "618": round(levels["618"], 4),
            },
            "ladder_window": round(levels["ladder_window"], 4),
            "offset": round(levels["offset"], 4),
            "offset_above_50": round(levels["offset_above_50"], 4),
            "offset_above_618": round(levels["offset_above_618"], 4),
            # Static stop loss order
            "static_stop_order": {
                "action": "SELL",
                "order_type": "STOP_LIMIT",
                "stop_price": stop_loss_level,
                "limit_price": round(stop_loss_level * 0.999, 2),  # Slightly below stop for fill
            },
            "explanation": (
                f"Fibonacci BUY signal: {buy_reason}. "
                f"Price {price:.2f} crossed above trigger level. "
                f"Buy {qty} @≤{limit_price:.2f}; static stop loss at {stop_loss_level:.2f}."
            ),
        }
        self._log_decision(logging.INFO, symbol, result)
        return result
    
    # ─────────────────────────── SELL ───────────────────────────
    def decide_sell(self, info: RunnerDecisionInfo) -> Dict[str, Any]:
        symbol = (getattr(info.runner, "stock", None) or "").upper()
        price = float(info.current_price)
        qty = int(info.position.quantity)
        params = info.runner.parameters or {}
        
        # Get Fibonacci parameters
        fib_high = params.get("fib_high")
        fib_low = params.get("fib_low")
        
        if fib_high is None or fib_low is None:
            # If we lost parameters somehow, just hold
            result = {
                "action": "NO_ACTION",
                "reason": "missing_parameters",
                "price": round(price, 4),
                "explanation": "HOLD - Missing Fibonacci parameters. Holding position, managed by static stop loss.",
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result
        
        try:
            fib_high = float(fib_high)
            fib_low = float(fib_low)
        except (ValueError, TypeError):
            result = {
                "action": "NO_ACTION",
                "reason": "invalid_parameters",
                "price": round(price, 4),
                "explanation": "HOLD - Invalid Fibonacci parameters. Holding position, managed by static stop loss.",
            }
            self._log_decision(logging.WARNING, symbol, result)
            return result
        
        # Calculate Fibonacci levels
        levels = self._calculate_fibonacci_levels(fib_high, fib_low)
        
        # Check position marker to determine exit rules
        in_50_position = params.get(self.POSITION_50, False)
        in_618_position = params.get(self.POSITION_618, False)
        
        should_sell = False
        sell_reason = ""
        
        # Time-based exit (if configured)
        if info.distance_from_time_limit is not None and info.distance_from_time_limit <= 0:
            should_sell = True
            sell_reason = "time_exit"
        
        # Position-specific exit rules
        elif in_50_position:
            # Sell if price down-crossed offset below 50%
            if price <= levels["offset_below_50"]:
                should_sell = True
                sell_reason = "crossed_below_50_offset"
        
        elif in_618_position:
            # Sell if price down-crossed offset below 61.8%
            if price <= levels["offset_below_618"]:
                should_sell = True
                sell_reason = "crossed_below_618_offset"
            # Also sell if price up-crossed offset below 50% (profit target)
            elif price >= levels["offset_below_50"]:
                should_sell = True
                sell_reason = "profit_target_50_reached"
        
        if should_sell:
            limit_price = round(price * 0.9995, 2)  # 0.05% wiggle room
            
            result = {
                "action": "SELL",
                "quantity": qty,
                "order_type": "LMT",
                "limit_price": limit_price,
                "reason": sell_reason,
                "price": round(price, 4),
                "position_type": "50%" if in_50_position else "61.8%" if in_618_position else "unknown",
                "explanation": f"Fibonacci SELL: {sell_reason}. Selling {qty} @≥{limit_price:.2f}",
            }
            
            # Clear position marker on sell
            params.pop(self.POSITION_50, None)
            params.pop(self.POSITION_618, None)
            
            self._log_decision(logging.INFO, symbol, result)
            return result
        
        # Build checklist for why we're not selling
        checklist_items = []
        
        if in_50_position:
            checklist_items.append({
                "label": "Price crossed below 50% offset",
                "ok": False,
                "actual": price,
                "wanted": levels["offset_below_50"],
                "direction": "<=",
                "wanted_label": "50% stop",
            })
        elif in_618_position:
            checklist_items.append({
                "label": "Price crossed below 61.8% offset",
                "ok": False,
                "actual": price,
                "wanted": levels["offset_below_618"],
                "direction": "<=",
                "wanted_label": "61.8% stop",
            })
            checklist_items.append({
                "label": "Price crossed above 50% profit target",
                "ok": price >= levels["offset_below_50"],
                "actual": price,
                "wanted": levels["offset_below_50"],
                "direction": ">=",
                "wanted_label": "50% target",
            })
        
        if info.distance_from_time_limit is not None:
            checklist_items.append({
                "label": "Time expiry reached",
                "ok": False,
            })
        
        explanation = format_checklist(checklist_items) if checklist_items else "Holding position; managed by Fibonacci levels and static stop loss."
        
        result = {
            "action": "NO_ACTION",
            "reason": "hold_position",
            "price": round(price, 4),
            "position_type": "50%" if in_50_position else "61.8%" if in_618_position else "unknown",
            "fib_levels": {
                "offset_below_50": round(levels["offset_below_50"], 4),
                "offset_below_618": round(levels["offset_below_618"], 4),
            },
            "explanation": explanation,
        }
        self._log_decision(logging.INFO, symbol, result)
        return result
    
    def decide_refresh(self, info: RunnerDecisionInfo) -> Dict[str, Any] | None:
        """
        Refresh method for compatibility with Strategy interface.
        Currently not used in this strategy.
        """
        return {"action": "NO_ACTION", "reason": "refresh_not_implemented"}
    
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
                "price", "zone", "position_type", "position_marker",
                "stop_loss_level", "limit_price", "quantity",
                "fib_levels", "explanation"
            ):
                if k in result:
                    payload[k] = result[k]
            log.log(level, "%s", payload)
        except Exception:
            # Never let logging break the strategy
            log.exception("fibonacci-yuval: failed to emit decision log")
