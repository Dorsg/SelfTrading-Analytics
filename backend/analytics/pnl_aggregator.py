from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import func
from datetime import timedelta

from database.db_manager import DBManager
from database.models import ExecutedTrade


def compute_final_pnl_for_runner(*, runner_id: int) -> tuple[float, float, int, float | None, float | None]:
    """
    Return (final_pnl_amount, final_pnl_percent, trades_count) using ExecutedTrade.
    Percent is realized-only vs. total buy cost basis; unrealized is ignored in backtest end.
    """
    with DBManager() as db:
        realised = (
            db.db.query(func.coalesce(func.sum(ExecutedTrade.pnl_amount), 0.0))
            .filter(ExecutedTrade.runner_id == runner_id, ExecutedTrade.pnl_amount.isnot(None))
            .scalar()
            or 0.0
        )
        trades_count = (
            db.db.query(func.count(ExecutedTrade.id))
            .filter(ExecutedTrade.runner_id == runner_id)
            .scalar()
            or 0
        )
        # Approximate percent vs. sum of absolute sell proceeds, to avoid tracking initial capital here
        proceeds = (
            db.db.query(func.coalesce(func.sum(ExecutedTrade.price * ExecutedTrade.quantity), 0.0))
            .filter(ExecutedTrade.runner_id == runner_id, ExecutedTrade.action == "SELL")
            .scalar()
            or 0.0
        )
        pct = (realised / proceeds * 100.0) if proceeds > 0 else 0.0

        # Average P&L per trade (realized) and average trade duration
        sells = (
            db.db.query(ExecutedTrade)
            .filter(ExecutedTrade.runner_id == runner_id, ExecutedTrade.action == "SELL")
            .order_by(ExecutedTrade.fill_time.asc())
            .all()
        )
        avg_pnl_per_trade = (realised / len(sells)) if sells else 0.0

        # Duration approximated by SELL time minus prior BUY time for same perm stream
        # Since we aggregate per perm_id, we approximate by average distance between BUY and SELL stamps
        buys = (
            db.db.query(ExecutedTrade)
            .filter(ExecutedTrade.runner_id == runner_id, ExecutedTrade.action == "BUY")
            .order_by(ExecutedTrade.fill_time.asc())
            .all()
        )
        i = j = 0
        durations: list[float] = []
        while i < len(buys) and j < len(sells):
            if sells[j].fill_time and buys[i].fill_time and sells[j].fill_time > buys[i].fill_time:
                durations.append((sells[j].fill_time - buys[i].fill_time).total_seconds())
                i += 1
                j += 1
            else:
                j += 1
        avg_trade_duration_sec = (sum(durations) / len(durations)) if durations else None

        return (
            round(realised, 2),
            round(pct, 4),
            int(trades_count),
            round(avg_pnl_per_trade, 4),
            avg_trade_duration_sec,
        )


