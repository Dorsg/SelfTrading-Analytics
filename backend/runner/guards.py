from __future__ import annotations
import logging
from strategies.runner_decision_info import RunnerDecisionInfo

log = logging.getLogger("runner-guards")

def global_sl_tp_decision(
    info: RunnerDecisionInfo,
    *,
    commission_buffer_pct: float,
    limit_wiggle_pct: float,
) -> dict | None:
    """
    Global protective SELL decisions based on runner parameters:
      • stop_loss:   percent relative to entry (e.g., -3 for -3%)
      • take_profit: percent relative to entry (e.g., 5 for +5%)

    Semantics preserved:
      trigger levels = entry * (1 + pct/100).
      stop triggers when price <= level*(1+commission_buffer_pct).
      take-profit triggers when price >= level*(1-commission_buffer_pct).
    """
    if not info.position:
        return None

    params = info.runner.parameters or {}
    stop_loss_pct   = params.get("stop_loss")
    take_profit_pct = params.get("take_profit")

    if stop_loss_pct is None and take_profit_pct is None:
        return None

    try:
        stop_loss_pct   = float(stop_loss_pct)   if stop_loss_pct   is not None else None
        take_profit_pct = float(take_profit_pct) if take_profit_pct is not None else None
    except Exception:
        log.exception(
            "Invalid stop_loss/take_profit values for runner %s(%d)",
            info.runner.name, info.runner.id
        )
        return None

    entry = float(info.position.avg_price)
    price = float(info.current_price)
    qty   = int(info.position.quantity)

    # ── Stop-loss (sell if price falls to/through trigger) ──
    if stop_loss_pct is not None:
        stop_level   = entry * (1.0 + stop_loss_pct / 100.0)
        stop_trigger = stop_level * (1.0 + commission_buffer_pct)
        if price <= stop_trigger:
            limit_price = round(max(0.01, price * (1.0 - limit_wiggle_pct)), 2)
            return {
                "action":       "SELL",
                "quantity":     qty,
                "order_type":   "LMT",
                "limit_price":  limit_price,
                "reason":       "global_stop_loss_triggered",
                "entry_price":  round(entry, 6),
                "price":        round(price, 6),
                "stop_loss_pct":   stop_loss_pct,
                "stop_level":      round(stop_level, 6),
                "stop_trigger":    round(stop_trigger, 6),
                "buffer_applied":  commission_buffer_pct,
            }

    # ── Take-profit (sell if price rises to/through trigger) ──
    if take_profit_pct is not None:
        tp_level   = entry * (1.0 + take_profit_pct / 100.0)
        tp_trigger = tp_level * (1.0 - commission_buffer_pct)
        if price >= tp_trigger:
            limit_price = round(price * (1.0 + limit_wiggle_pct), 2)
            return {
                "action":        "SELL",
                "quantity":      qty,
                "order_type":    "LMT",
                "limit_price":   limit_price,
                "reason":        "global_take_profit_triggered",
                "entry_price":   round(entry, 6),
                "price":         round(price, 6),
                "take_profit_pct": take_profit_pct,
                "tp_level":        round(tp_level, 6),
                "tp_trigger":      round(tp_trigger, 6),
                "buffer_applied":  commission_buffer_pct,
            }

    return None
