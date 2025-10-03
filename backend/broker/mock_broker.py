from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from database.db_manager import DBManager
from database.models import OpenPosition, Order, ExecutedTrade
from backend.trades_logger import log_buy, log_sell

log = logging.getLogger("mock-broker")


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _quantize(p: float, tick: float = 0.01) -> float:
    try:
        steps = round(p / tick)
        return round(steps * tick, 6)
    except Exception:
        return float(p)


@dataclass(slots=True)
class _RunnerLite:
    id: int
    user_id: int
    stock: str
    strategy: str
    time_frame: int
    parameters: dict


class MockBroker:
    """
    Lightweight broker simulator with:
      • Single active trailing controller bound to the OpenPosition row (runner_id unique).
      • Tick-aware trailing/static stop enforcement (exit on cross; never "skip").
      • Emits ExecutedTrade rows and trade logs.
    """

    def __init__(self, *, tick_size: float = 0.01) -> None:
        self._tick = float(tick_size)

    # ─────────────────────────────────────────────────────────────────────────────

    def buy(
        self,
        *,
        user_id: int,
        runner: Any,
        symbol: str,
        price: float,
        quantity: int,
        decision: Optional[Dict[str, Any]] = None,
        at: datetime,
    ) -> bool:
        """Open (or replace) a position for this runner."""
        at = _utc(at)
        r = _RunnerLite(
            id=int(getattr(runner, "id")),
            user_id=user_id,
            stock=(getattr(runner, "stock", symbol) or symbol).upper(),
            strategy=str(getattr(runner, "strategy", "unknown")),
            time_frame=int(getattr(runner, "time_frame", 5) or 5),
            parameters=dict(getattr(runner, "parameters", {}) or {}),
        )

        q = max(int(quantity or 0), 0)
        if q <= 0 or price is None or price <= 0:
            return False

        with DBManager() as db:
            # Replace any existing position for this runner (runner_id is unique in table)
            pos = db.get_open_position(r.id)
            if pos:
                # For safety, close existing before opening a new one at same tick
                self._force_close_without_trade(db, pos)

            pos = OpenPosition(
                user_id=r.user_id,
                runner_id=r.id,
                symbol=r.stock,
                account="mock",
                quantity=q,
                avg_price=float(price),
                created_at=at,
                stop_price=None,
                trail_percent=None,
                highest_price=None,
            )
            # Apply static stop if provided in decision
            ss = (decision or {}).get("static_stop_order")
            if isinstance(ss, dict):
                try:
                    sp = float(ss.get("stop_price"))
                    if sp > 0:
                        pos.stop_price = _quantize(sp, self._tick)
                except Exception:
                    pass

            db.db.add(pos)

            # Record synthetic order
            ord_buy = Order(
                user_id=r.user_id,
                runner_id=r.id,
                symbol=r.stock,
                side="BUY",
                order_type=str((decision or {}).get("order_type", "MKT")).upper(),
                quantity=q,
                limit_price=float((decision or {}).get("limit_price") or 0) or None,
                stop_price=pos.stop_price,
                status="filled",
                created_at=at,
                filled_at=at,
                details=None,
            )
            db.db.add(ord_buy)
            db.db.commit()

            log_buy(user_id=r.user_id, runner_id=r.id, symbol=r.stock, qty=q, price=float(price), as_of=at, reason="strategy_buy")
        return True

    # ─────────────────────────────────────────────────────────────────────────────

    def sell_all(
        self,
        *,
        user_id: int,
        runner: Any,
        symbol: str,
        price: float,
        decision: Optional[Dict[str, Any]] = None,
        at: datetime,
        reason_override: Optional[str] = None,
    ) -> bool:
        """Close the open position (if any) and emit a trade."""
        at = _utc(at)
        rid = int(getattr(runner, "id"))
        with DBManager() as db:
            pos = db.get_open_position(rid)
            if not pos:
                return False

            q = float(pos.quantity or 0)
            avg = float(pos.avg_price or 0)
            if q <= 0 or avg <= 0:
                self._force_close_without_trade(db, pos)
                return False

            # Record SELL order (synthetic)
            ord_sell = Order(
                user_id=user_id,
                runner_id=rid,
                symbol=pos.symbol,
                side="SELL",
                order_type=str((decision or {}).get("order_type", "MKT")).upper(),
                quantity=int(q),
                limit_price=float((decision or {}).get("limit_price") or 0) or None,
                stop_price=float((decision or {}).get("stop_price") or 0) or None,
                status="filled",
                created_at=at,
                filled_at=at,
                details=(None if decision is None else str(decision)),
            )
            db.db.add(ord_sell)

            # ExecutedTrade roll-up
            pnl_amt = (float(price) - avg) * q
            pnl_pct = 0.0 if avg == 0 else (float(price) / avg - 1.0) * 100.0
            trade = ExecutedTrade(
                user_id=user_id,
                runner_id=rid,
                symbol=pos.symbol,
                buy_ts=pos.created_at,
                sell_ts=at,
                buy_price=avg,
                sell_price=float(price),
                quantity=q,
                pnl_amount=pnl_amt,
                pnl_percent=pnl_pct,
                strategy=str(getattr(runner, "strategy", "unknown")),
                timeframe='1d' if str(int(getattr(runner, "time_frame", 5) or 5)) == '1440' else '5m',
            )
            db.db.add(trade)

            # Remove position
            db.db.delete(pos)
            db.db.commit()

            log_sell(
                user_id=user_id,
                runner_id=rid,
                symbol=ord_sell.symbol,
                qty=q,
                avg_price=avg,
                price=float(price),
                as_of=at,
                reason=(reason_override or (decision or {}).get("reason") or ""),
            )
        return True

    # ─────────────────────────────────────────────────────────────────────────────

    def arm_trailing_stop_once(
        self,
        *,
        user_id: int,
        runner: Any,
        entry_price: float,
        trail_pct: float,
        at: datetime,
    ) -> None:
        """
        Ensure exactly ONE active trailing controller bound to the DB position.
        If already armed, we do nothing (idempotent).
        """
        at = _utc(at)
        rid = int(getattr(runner, "id"))
        with DBManager() as db:
            pos = db.get_open_position(rid)
            if not pos:
                return
            # If already armed, do nothing (single controller per position)
            try:
                if float(pos.trail_percent or 0) > 0:
                    return
            except Exception:
                pass

            pos.trail_percent = float(trail_pct)
            pos.highest_price = float(entry_price)
            db.db.commit()
            log.debug("Trailing armed for runner=%s symbol=%s trail_pct=%.3f%%", rid, pos.symbol, float(trail_pct))

    # ─────────────────────────────────────────────────────────────────────────────

    def on_tick(self, *, user_id: int, runner: Any, price: float, at: datetime) -> Dict[str, int]:
        """
        Tick-aware trailing/static stop evaluation.
        Exit when price <= stop (with epsilon at tick size). Never 'skip'.
        Returns counters for KPI aggregation: {"stop_cross_exits": N}
        """
        at = _utc(at)
        rid = int(getattr(runner, "id"))
        out = {"stop_cross_exits": 0}

        with DBManager() as db:
            pos = db.get_open_position(rid)
            if not pos:
                return out

            # Update trailing state
            try:
                trail_pct = float(pos.trail_percent or 0.0)
            except Exception:
                trail_pct = 0.0

            top = float(pos.highest_price or 0.0)
            px = float(price)

            if trail_pct > 0.0:
                if px > top:
                    pos.highest_price = px
                    db.db.commit()
                    top = px

                trail_stop = top * (1.0 - trail_pct / 100.0)
                # tick-aware compare: allow a tiny epsilon up to one tick
                if px <= (trail_stop + self._tick * 1e-9):
                    # Close position now
                    out["stop_cross_exits"] += 1
                    self.sell_all(
                        user_id=user_id,
                        runner=runner,
                        symbol=pos.symbol,
                        price=_quantize(px, self._tick),
                        decision={"reason": "trailing_stop_hit", "order_type": "MKT"},
                        at=at,
                        reason_override="trailing_stop_hit",
                    )
                    return out

            # Static stop (if any)
            try:
                sp = float(pos.stop_price or 0.0)
            except Exception:
                sp = 0.0

            if sp > 0.0 and px <= (sp + self._tick * 1e-9):
                out["stop_cross_exits"] += 1
                self.sell_all(
                    user_id=user_id,
                    runner=runner,
                    symbol=pos.symbol,
                    price=_quantize(px, self._tick),
                    decision={"reason": "static_stop_hit", "order_type": "MKT"},
                    at=at,
                    reason_override="static_stop_hit",
                )
                return out

        return out

    # ─────────────────────────────────────────────────────────────────────────────

    def mark_to_market_all(self, *, user_id: int, at: datetime) -> None:
        """
        Placeholder for equity/cash MTM bookkeeping in mock mode.
        Currently a no-op; extend as needed.
        """
        _ = user_id, at  # intentionally unused

    # ─────────────────────────────────────────────────────────────────────────────
    # internals
    # ─────────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _force_close_without_trade(db: DBManager, pos: OpenPosition) -> None:
        try:
            db.db.delete(pos)
            db.db.commit()
        except Exception:
            db.db.rollback()
