from __future__ import annotations
import logging
from datetime import datetime, timezone

log = logging.getLogger("trades")

def _ts(dt) -> str:
    if isinstance(dt, datetime):
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).isoformat()
    return str(dt)

def log_buy(*, user_id: int, runner_id: int, symbol: str, qty: float, price: float, as_of, reason: str = "") -> None:
    """
    Write a BUY fill line to trades.log
    """
    log.info(
        "BUY user=%s runner=%s symbol=%s qty=%.4f fill=%.4f as_of=%s%s",
        user_id,
        runner_id,
        symbol,
        qty,
        price,
        _ts(as_of),
        f" reason={reason}" if reason else "",
    )

def log_sell(*, user_id: int, runner_id: int, symbol: str, qty: float, avg_price: float, price: float, as_of, reason: str = "") -> None:
    """
    Write a SELL fill line to trades.log including P&L and P&L%.
    """
    pnl = (price - avg_price) * qty
    pnl_pct = 0.0 if avg_price == 0 else ((price / avg_price) - 1.0) * 100.0
    log.info(
        "SELL user=%s runner=%s symbol=%s qty=%.4f fill=%.4f avg=%.4f pnl=%.2f pnl_pct=%.2f%% as_of=%s%s",
        user_id,
        runner_id,
        symbol,
        qty,
        price,
        avg_price,
        pnl,
        pnl_pct,
        _ts(as_of),
        f" reason={reason}" if reason else "",
    )
