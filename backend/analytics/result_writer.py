from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert

from database.db_core import engine
from database.models import AnalyticsResult


def upsert_result(
    *,
    symbol: str,
    strategy: str,
    timeframe: str,
    start_ts: datetime | None,
    end_ts: datetime | None,
    final_pnl_amount: float | None,
    final_pnl_percent: float | None,
    trades_count: int | None,
    max_drawdown: float | None,
    avg_pnl_per_trade: float | None = None,
    avg_trade_duration_sec: float | None = None,
) -> None:
    with engine.begin() as conn:
        row = {
            "symbol": symbol.upper(),
            "strategy": strategy,
            "timeframe": timeframe,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "final_pnl_amount": final_pnl_amount,
            "final_pnl_percent": final_pnl_percent,
            "trades_count": trades_count,
            "max_drawdown": max_drawdown,
            "details": None,
            "updated_at": datetime.now(timezone.utc),
        }
        # stash averages in details json in future (column not defined). For now ignore.
        ins = insert(AnalyticsResult).values(row)
        update_cols = {k: getattr(ins.excluded, k) for k in row.keys() if k not in ("created_at",)}
        conn.execute(
            ins.on_conflict_do_update(
                index_elements=["symbol", "strategy", "timeframe"],
                set_=update_cols,
            )
        )


