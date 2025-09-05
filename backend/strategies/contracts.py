from __future__ import annotations
from typing import Protocol, Literal, TypedDict, Any, Dict

# ───────── Strategy decision shapes ─────────
ActionT = Literal["BUY", "SELL", "NO_ACTION"]

class TrailStopSpec(TypedDict, total=False):
    trailing_percent: float | None
    trailing_amount: float | None
    limit_offset: float | None

class StaticStopSpec(TypedDict, total=False):
    action: str
    order_type: str
    stop_price: float | None
    limit_price: float | None

class StrategyDecision(TypedDict, total=False):
    action: ActionT
    reason: str
    order_type: str
    quantity: int
    limit_price: float | None
    stop_price: float | None
    trail_stop_order: TrailStopSpec  # Trailing stop for BUY
    static_stop_order: StaticStopSpec  # Static stop for BUY (alternative to trail)

class Strategy(Protocol):
    name: str
    def decide_buy(self, info: "RunnerDecisionInfo") -> StrategyDecision: ...
    def decide_sell(self, info: "RunnerDecisionInfo") -> StrategyDecision: ...

# ───────── Validation ─────────
class StrategyDecisionError(ValueError):
    pass

def validate_decision(decision: Dict[str, Any] | None, *, is_exit: bool) -> StrategyDecision | None:
    if not decision or decision.get("action") in (None, "", "NO_ACTION"):
        # Preserve all fields from the original decision, not just action and reason
        if decision:
            return dict(decision)  # Return the full decision with all details
        return {"action": "NO_ACTION", "reason": "no_signal"}

    out: Dict[str, Any] = dict(decision)
    action = str(out.get("action", "")).upper()
    out["action"] = action

    if action not in {"BUY", "SELL", "NO_ACTION"}:
        raise StrategyDecisionError(f"Invalid action '{action}'")

    if action == "NO_ACTION":
        # Preserve all fields from the original decision, not just action and reason
        return out  # Return the full decision with all details

    # quantity (when provided)
    if "quantity" in out:
        try:
            q = int(out["quantity"])
            if q <= 0:
                raise StrategyDecisionError("quantity must be > 0 when provided")
            out["quantity"] = q
        except Exception as e:
            raise StrategyDecisionError("quantity must be an integer > 0") from e

    # LMT requires limit_price
    ot = out.get("order_type")
    if ot is not None and str(ot).upper() == "LMT":
        lp = out.get("limit_price")
        try:
            if lp is None or float(lp) <= 0:
                raise StrategyDecisionError("LMT orders require a positive limit_price")
        except Exception as e:
            raise StrategyDecisionError("limit_price must be a positive number for LMT") from e

    # BUY must include either trailing stop or static stop in live mode
    if action == "BUY":
        ts = out.get("trail_stop_order")
        ss = out.get("static_stop_order")

        # In analytics-only mode we allow BUY without stop specs to keep simulations simple.
        # Be tolerant: if RUNNING_ENV is missing, default to analytics to avoid accidental strict blocking
        import os
        running_env = os.getenv("RUNNING_ENV", "analytics").lower()
        if running_env == "analytics":
            # nothing to enforce here for analytics
            pass
        else:
            if not isinstance(ts, dict) and not isinstance(ss, dict):
                raise StrategyDecisionError("BUY decision must include either 'trail_stop_order' or 'static_stop_order' dict")
        
        # Validate trailing stop if provided
        if isinstance(ts, dict):
            tp = ts.get("trailing_percent")
            ta = ts.get("trailing_amount")
            if (tp is None or float(tp) <= 0) and (ta is None or float(ta) <= 0):
                raise StrategyDecisionError(
                    "trail_stop_order must include positive trailing_percent or trailing_amount"
                )
        
        # Validate static stop if provided
        if isinstance(ss, dict):
            stop_price = ss.get("stop_price")
            order_type = ss.get("order_type", "").upper()
            if stop_price is None or float(stop_price) <= 0:
                raise StrategyDecisionError("static_stop_order must include positive stop_price")
            if order_type not in {"STOP", "STOP_LIMIT"}:
                raise StrategyDecisionError("static_stop_order order_type must be 'STOP' or 'STOP_LIMIT'")
            if order_type == "STOP_LIMIT":
                limit_price = ss.get("limit_price")
                if limit_price is None or float(limit_price) <= 0:
                    raise StrategyDecisionError("STOP_LIMIT static_stop_order must include positive limit_price")

    return out  # type: ignore[return-value]
