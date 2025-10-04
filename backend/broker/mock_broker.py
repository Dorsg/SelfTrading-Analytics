from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sqlalchemy import inspect

from database.db_manager import DBManager
from database.models import OpenPosition, Order, ExecutedTrade
from backend.trades_logger import log_buy, log_sell

log = logging.getLogger("mock-broker")

# Simulation realism parameters from environment variables
_SIM_COMMISSION_PER_TRADE = float(os.environ.get("SIM_COMMISSION_PER_TRADE", 1.00))  # e.g., $1 per trade
_SIM_BID_ASK_SPREAD = float(os.environ.get("SIM_BID_ASK_SPREAD", 0.01))  # e.g., 1 cent spread
_SIM_SLIPPAGE_PERCENT = float(os.environ.get("SIM_SLIPPAGE_PERCENT", 0.0005))  # e.g., 0.05% slippage


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
        self.commission = _SIM_COMMISSION_PER_TRADE
        self.spread = _SIM_BID_ASK_SPREAD
        self.slippage = _SIM_SLIPPAGE_PERCENT
        log.info(f"MockBroker initialized with realism params: commission=${self.commission}, spread=${self.spread}, slippage={self.slippage * 100:.4f}%")

    # ─────────────────────────────────────────────────────────────────────────────

    def _apply_realism_costs(self, price: float, side: str) -> float:
        """Applies spread and slippage to the execution price."""
        # 1. Apply bid-ask spread
        if side == "BUY":
            price += self.spread / 2
        else:  # SELL
            price -= self.spread / 2

        # 2. Apply slippage
        slippage_amount = price * self.slippage * (1 if side == "BUY" else -1)
        price += slippage_amount

        return _quantize(price, self._tick)

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
        # Check for limit order condition
        order_type = str((decision or {}).get("order_type", "MKT")).upper()
        if order_type == "LMT":
            limit_price = float((decision or {}).get("limit_price") or 0)
            if limit_price > 0 and price > limit_price:
                # For a BUY LIMIT, current price is too high, don't fill.
                return False
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

        # Apply realism to execution price
        exec_price = self._apply_realism_costs(price, "BUY")

        with DBManager() as db:
            # Replace any existing position for this runner (runner_id is unique in table)
            pos = db.get_open_position(r.id)
            if pos:
                # Bug fix: Properly sell the existing position to record the trade, instead of just deleting it.
                log.warning(f"Runner {r.id} is buying while already in a position. Closing existing position first.")
                self.sell_all(
                    user_id=user_id,
                    runner=r,
                    symbol=pos.symbol,
                    price=price, # Use the current market price for the sell
                    decision={"reason": "strategy_override_buy"},
                    at=at,
                    reason_override="strategy_override_buy"
                )

            pos = OpenPosition(
                user_id=r.user_id,
                runner_id=r.id,
                symbol=r.stock,
                account="mock",
                quantity=q,
                avg_price=exec_price,
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
                order_type=order_type,
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

            log_buy(user_id=r.user_id, runner_id=r.id, symbol=r.stock, qty=q, price=exec_price, as_of=at, reason="strategy_buy")
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
    ) -> Optional[float]:
        """Close the open position (if any) and emit a trade. Returns P&L of the trade."""
        at = _utc(at)
        # Check for limit order condition
        order_type = str((decision or {}).get("order_type", "MKT")).upper()
        if order_type == "LMT":
            limit_price = float((decision or {}).get("limit_price") or 0)
            if limit_price > 0 and price < limit_price:
                # For a SELL LIMIT, current price is too low, don't fill.
                return None
        rid = int(getattr(runner, "id"))
        with DBManager() as db:
            pos = db.get_open_position(rid)
            if not pos:
                return None

            q = float(pos.quantity or 0)
            avg = float(pos.avg_price or 0)
            if q <= 0 or avg <= 0:
                self._force_close_without_trade(db, pos)
                return None

            # Apply realism to execution price
            exec_price = self._apply_realism_costs(price, "SELL")

            # Record SELL order (synthetic)
            ord_sell = Order(
                user_id=user_id,
                runner_id=rid,
                symbol=pos.symbol,
                side="SELL",
                order_type=order_type,
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
            pnl_amt = (exec_price - avg) * q - (self.commission * 2) # Commission on buy and sell
            cost_basis = avg * q
            pnl_pct = (pnl_amt / cost_basis) * 100.0 if cost_basis > 0 else 0.0

            trade = ExecutedTrade(
                user_id=user_id,
                runner_id=rid,
                symbol=pos.symbol,
                buy_ts=pos.created_at,
                sell_ts=at,
                buy_price=avg,
                sell_price=exec_price,
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
                price=exec_price,
                as_of=at,
                reason=(reason_override or (decision or {}).get("reason") or ""),
            )
        return pnl_amt

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

    def on_bar(self, *, user_id: int, runner: Any, o: float, h: float, l: float, c: float, at: datetime) -> Dict[str, int]:
        """
        OHLC-aware trailing/static stop evaluation.
        Exit when low <= stop. Assumes exit price is the stop price.
        Returns counters for KPI aggregation.
        """
        at = _utc(at)
        rid = int(getattr(runner, "id"))
        out = {"stop_cross_exits": 0}

        with DBManager() as db:
            pos = db.get_open_position(rid)
            if not pos:
                return out

            exit_price = None
            exit_reason = None

            # 1. Check static stop
            try:
                sp = float(pos.stop_price or 0.0)
                if sp > 0.0 and l <= (sp + self._tick * 1e-9):
                    exit_price = sp  # Exit at the stop price
                    exit_reason = "static_stop_hit"
            except Exception:
                pass

            # 2. Check trailing stop (only if static stop not hit)
            if not exit_price:
                try:
                    trail_pct = float(pos.trail_percent or 0.0)
                    if trail_pct > 0.0:
                        top = float(pos.highest_price or 0.0)
                        # First, update highest price based on the bar's high
                        if h > top:
                            pos.highest_price = h
                            top = h

                        trail_stop = top * (1.0 - trail_pct / 100.0)
                        if l <= (trail_stop + self._tick * 1e-9):
                            exit_price = trail_stop  # Exit at the stop price
                            exit_reason = "trailing_stop_hit"
                except Exception:
                    pass
            
            # If an exit was triggered, sell the position
            if exit_price and exit_reason:
                out["stop_cross_exits"] += 1
                self.sell_all(
                    user_id=user_id,
                    runner=runner,
                    symbol=pos.symbol,
                    price=_quantize(exit_price, self._tick),
                    decision={"reason": exit_reason, "order_type": "MKT"},
                    at=at,
                    reason_override=exit_reason,
                )
                # Commit highest price change if trail was active
                db.db.commit()

            # If no exit, but trailing is active, still commit highest price update
            elif pos in inspect(pos).session.dirty:
                 db.db.commit()


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
