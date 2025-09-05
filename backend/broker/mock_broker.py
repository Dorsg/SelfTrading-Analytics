from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from database.db_manager import DBManager
from database.models import Account, OpenPosition, Order, ExecutedTrade, Runner
from trades_logger import log_buy, log_sell
from backend.ib_manager.market_data_manager import MarketDataManager

log = logging.getLogger("mock-broker")


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@dataclass(slots=True)
class _RunnerLite:
    id: int
    user_id: int
    stock: str
    parameters: dict


class MockBroker:
    """
    Lightweight fill-at-mid mock broker with:
      • BUY/SELL market/limit fills (assumed immediate at provided `price`)
      • Trailing and static stop management (evaluated on each tick)
      • Mark-to-market account equity (cash + sum(position_value))
    """

    def __init__(self) -> None:
        self.mkt = MarketDataManager()

    # ───────────────────────── helpers ─────────────────────────

    def _runner_lite(self, runner: Any) -> _RunnerLite:
        return _RunnerLite(
            id=int(getattr(runner, "id", 0) or 0),
            user_id=int(getattr(runner, "user_id", 0) or 0),
            stock=str(getattr(runner, "stock", "UNKNOWN") or "UNKNOWN").upper(),
            parameters=dict(getattr(runner, "parameters", {}) or {}),
        )

    def _get_acct(self, db: DBManager, user_id: int) -> Account:
        # Ensure an account exists; RunnerService also ensures, but be defensive.
        return db.ensure_account(user_id=user_id, name="mock")

    # ───────────────────────── public API ─────────────────────────

    def on_tick(self, *, user_id: int, runner: Runner | _RunnerLite, price: float, at: datetime) -> None:
        """
        Update trailing/static stops for the position and execute if hit.
        """
        at = _utc(at)
        rl = self._runner_lite(runner)

        with DBManager() as db:
            pos: Optional[OpenPosition] = (
                db.db.query(OpenPosition)
                .filter(OpenPosition.user_id == user_id, OpenPosition.runner_id == rl.id)
                .first()
            )
            if not pos:
                return

            # Update highest_price for trailing stops
            if (pos.trail_percent or 0) > 0:
                hp = float(pos.highest_price or pos.avg_price or price)
                if price > hp:
                    pos.highest_price = price
                    db.db.commit()

                trail_pct = float(pos.trail_percent or 0.0)
                stop_trail = float((pos.highest_price or hp) * (1.0 - trail_pct / 100.0))
                if price <= stop_trail and pos.quantity > 0:
                    log.info("Trailing stop hit for runner=%s %s at %.4f <= stop %.4f", rl.id, rl.stock, price, stop_trail)
                    self.sell_all(
                        user_id=user_id,
                        runner=runner,
                        symbol=rl.stock,
                        price=price,
                        decision={"reason": "trailing_stop_hit"},
                        at=at,
                        reason_override="trailing_stop_hit",
                    )
                    return  # Position closed; nothing else to check

            # Static stop
            if (pos.stop_price or 0) > 0 and pos.quantity > 0:
                if price <= float(pos.stop_price):
                    log.info("Static stop hit for runner=%s %s at %.4f <= stop %.4f", rl.id, rl.stock, price, float(pos.stop_price))
                    self.sell_all(
                        user_id=user_id,
                        runner=runner,
                        symbol=rl.stock,
                        price=price,
                        decision={"reason": "static_stop_hit"},
                        at=at,
                        reason_override="static_stop_hit",
                    )

    def arm_trailing_stop_once(self, *, user_id: int, runner: Runner | _RunnerLite, entry_price: float, trail_pct: float, at: datetime) -> None:
        """
        If a position exists and has no trailing stop, arm one.
        """
        at = _utc(at)
        rl = self._runner_lite(runner)
        with DBManager() as db:
            pos: Optional[OpenPosition] = (
                db.db.query(OpenPosition)
                .filter(OpenPosition.user_id == user_id, OpenPosition.runner_id == rl.id)
                .first()
            )
            if not pos:
                return
            if (pos.trail_percent or 0) > 0:
                return  # already armed
            pos.trail_percent = float(trail_pct)
            pos.highest_price = float(entry_price)
            db.db.commit()
            log.info("Armed trailing stop %.2f%% for runner=%s %s", float(trail_pct), rl.id, rl.stock)

    def buy(
        self,
        *,
        user_id: int,
        runner: Runner | _RunnerLite,
        symbol: str,
        price: float,
        quantity: int,
        decision: Dict[str, Any] | None,
        at: datetime,
    ) -> bool:
        at = _utc(at)
        rl = self._runner_lite(runner)
        symbol = (symbol or rl.stock).upper()
        if quantity <= 0 or price <= 0:
            return False

        with DBManager() as db:
            acct = self._get_acct(db, user_id)
            cost = float(price) * int(quantity)
            try:
                if float(acct.cash or 0.0) < cost:
                    log.info("BUY rejected (insufficient cash) user=%s runner=%s need=%.2f have=%.2f", user_id, rl.id, cost, float(acct.cash or 0.0))
                    return False
            except Exception:
                return False

            # Upsert position (1 position per runner)
            pos: Optional[OpenPosition] = (
                db.db.query(OpenPosition)
                .filter(OpenPosition.user_id == user_id, OpenPosition.runner_id == rl.id)
                .first()
            )
            if pos:
                # Aggregate into existing position with new avg price
                new_qty = int((pos.quantity or 0) + quantity)
                if new_qty <= 0:
                    return False
                new_cost = (float(pos.avg_price) * int(pos.quantity)) + cost
                pos.quantity = new_qty
                pos.avg_price = new_cost / new_qty
                # Reset stops to the most conservative values from decision (if provided)
            else:
                pos = OpenPosition(
                    user_id=user_id,
                    runner_id=rl.id,
                    symbol=symbol,
                    account="mock",
                    quantity=int(quantity),
                    avg_price=float(price),
                    created_at=at,
                    stop_price=None,
                    trail_percent=None,
                    highest_price=None,
                )
                db.db.add(pos)

            # Apply stop specs from decision
            if isinstance(decision, dict):
                ss = decision.get("static_stop_order") or {}
                tp = decision.get("trail_stop_order") or {}
                try:
                    sp = ss.get("stop_price")
                    if sp is not None and float(sp) > 0:
                        pos.stop_price = float(sp)
                except Exception:
                    pass
                try:
                    trailing_percent = tp.get("trailing_percent")
                    if trailing_percent is not None and float(trailing_percent) > 0:
                        pos.trail_percent = float(trailing_percent)
                        pos.highest_price = max(float(pos.highest_price or 0.0), float(price))
                except Exception:
                    pass

            # Create an order record (filled)
            ord = Order(
                user_id=user_id,
                runner_id=rl.id,
                symbol=symbol,
                side="BUY",
                order_type=(str(decision.get("order_type")) if decision else "MKT"),
                quantity=int(quantity),
                limit_price=decision.get("limit_price") if decision else None,
                stop_price=(decision.get("stop_price") or (decision.get("static_stop_order") or {}).get("stop_price")) if decision else None,
                status="filled",
                created_at=at,
                filled_at=at,
                details=None,
            )
            db.db.add(ord)

            # Deduct cash; equity recalculated in mark_to_market
            acct.cash = float(acct.cash or 0.0) - cost
            db.db.commit()

            log_buy(
                user_id=user_id,
                runner_id=rl.id,
                symbol=symbol,
                qty=float(quantity),
                price=float(price),
                as_of=at,
                reason=str((decision or {}).get("reason") or (decision or {}).get("explanation") or ""),
            )
            log.info("BUY filled user=%s runner=%s %s x%d @ %.4f", user_id, rl.id, symbol, int(quantity), float(price))
            return True

    def sell_all(
        self,
        *,
        user_id: int,
        runner: Runner | _RunnerLite,
        symbol: str,
        price: float,
        decision: Dict[str, Any] | None,
        at: datetime,
        reason_override: Optional[str] = None,
    ) -> bool:
        at = _utc(at)
        rl = self._runner_lite(runner)
        symbol = (symbol or rl.stock).upper()

        with DBManager() as db:
            pos: Optional[OpenPosition] = (
                db.db.query(OpenPosition)
                .filter(OpenPosition.user_id == user_id, OpenPosition.runner_id == rl.id)
                .first()
            )
            if not pos or int(pos.quantity or 0) <= 0:
                return False

            qty = int(pos.quantity)
            avg_price = float(pos.avg_price or 0.0)

            # Create order record (filled)
            ord = Order(
                user_id=user_id,
                runner_id=rl.id,
                symbol=symbol,
                side="SELL",
                order_type=(str(decision.get("order_type")) if decision else "MKT"),
                quantity=qty,
                limit_price=decision.get("limit_price") if decision else None,
                stop_price=decision.get("stop_price") if decision else None,
                status="filled",
                created_at=at,
                filled_at=at,
                details=None,
            )
            db.db.add(ord)

            # Credit cash for the sale
            acct = self._get_acct(db, user_id)
            proceeds = float(price) * qty
            acct.cash = float(acct.cash or 0.0) + proceeds

            # Record execution summary (optional in analytics)
            try:
                db.db.add(
                    ExecutedTrade(
                        user_id=user_id,
                        runner_id=rl.id,
                        symbol=symbol,
                        buy_ts=None,
                        sell_ts=at,
                        buy_price=avg_price,
                        sell_price=float(price),
                        quantity=float(qty),
                        pnl_amount=(float(price) - avg_price) * float(qty),
                        pnl_percent=(0.0 if avg_price == 0 else ((float(price) / avg_price) - 1.0) * 100.0),
                        strategy=None,
                        timeframe=None,
                    )
                )
            except Exception:
                pass

            # Delete the open position
            try:
                db.db.delete(pos)
            except Exception:
                # In rare cases (constraints), zero it out instead
                pos.quantity = 0
            db.db.commit()

            reason = reason_override or str((decision or {}).get("reason") or (decision or {}).get("explanation") or "strategy_sell")
            log_sell(
                user_id=user_id,
                runner_id=rl.id,
                symbol=symbol,
                qty=float(qty),
                avg_price=avg_price,
                price=float(price),
                as_of=at,
                reason=reason,
            )
            log.info("SELL filled user=%s runner=%s %s x%d @ %.4f (%s)", user_id, rl.id, symbol, qty, float(price), reason)
            return True

    def mark_to_market_all(self, *, user_id: int, at: datetime) -> None:
        """
        Refresh account equity and trailing-stop anchors based on last available prices.
        Missing prices are skipped (WARN once); the routine must never raise.
        """
        at = _utc(at)
        with DBManager() as db:
            positions: List[OpenPosition] = (
                db.db.query(OpenPosition)
                .filter(OpenPosition.user_id == user_id)
                .all()
            )
            if not positions:
                # Keep equity == cash if flat
                try:
                    acct = self._get_acct(db, user_id)
                    acct.equity = float(acct.cash or 0.0)
                    db.db.commit()
                except Exception:
                    pass
                return

            syms = [p.symbol.upper() for p in positions]
            try:
                # Use 5-minute prices by default for intraday MTM
                last_px = self.mkt.get_last_close_for_symbols(syms, 5, at, regular_hours_only=True)
            except Exception:
                log.exception("mark_to_market_all: price fetch failed")
                last_px = {}

            # Revalue positions, update trailing anchors, recompute equity
            total_mkt_value = 0.0
            for p in positions:
                px = float(last_px.get(p.symbol.upper(), 0.0) or 0.0)
                if px > 0:
                    total_mkt_value += px * int(p.quantity or 0)

                    # Bump highest_price for trailing stops
                    if (p.trail_percent or 0) > 0:
                        hp = float(p.highest_price or p.avg_price or px)
                        if px > hp:
                            p.highest_price = px
                else:
                    # No price available — skip; don't error
                    pass

            try:
                acct = self._get_acct(db, user_id)
                acct.equity = float(acct.cash or 0.0) + total_mkt_value
                db.db.commit()
            except Exception:
                log.exception("mark_to_market_all: failed to update account equity")
