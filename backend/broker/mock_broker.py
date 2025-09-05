from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, Literal

from database.db_manager import DBManager
from database.models import OpenPosition, Order, ExecutedTrade, Account
from backend.ib_manager.market_data_manager import MarketDataManager
from backend.trades_logger import log_buy, log_sell

log = logging.getLogger("mock-broker")

SideT = Literal["LONG", "SHORT"]


def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _round4(x: float) -> float:
    try:
        return round(float(x), 4)
    except Exception:
        return x


@dataclass(slots=True)
class _TrailMeta:
    # Do not evaluate the trailing stop until this timestamp (activation delay)
    do_not_trigger_before: datetime
    # Track best price since entry (highest for long, lowest for short)
    best_price: float


class MockBroker:
    """
    Lightweight simulation broker with trailing stop logic that mirrors live-broker behavior:

      • For LONGS: stop starts BELOW price and trails up:  stop = max(prev_stop, best_price * (1 - pct))
      • For SHORTS: stop starts ABOVE price and trails down: stop = min(prev_stop, best_price * (1 + pct))
      • Guardrails ensure a stop never arms on the wrong side of price and never triggers on the entry tick.
      • One-bar activation delay: the trailing stop is not evaluated until the *next* completed bar.

    The broker stores trailing state with open positions and uses an in-memory meta table for per-position timing.
    """

    def __init__(self) -> None:
        self._trail_meta: Dict[int, _TrailMeta] = {}
        self._mkt = MarketDataManager()

        # Safety epsilon to ensure "valid side" comparisons never equal on float edges
        self._eps = float(os.getenv("TRAIL_STOP_EPS", "1e-6"))

    # ─────────────────────────── Public API ───────────────────────────

    def buy(
        self,
        *,
        user_id: int,
        runner,
        symbol: str,
        price: float,
        quantity: int,
        decision: Dict[str, Any] | None,
        at: datetime,
    ) -> bool:
        at = _utc(at)
        symbol = (symbol or "").upper()
        if quantity <= 0 or price <= 0:
            log.warning("BUY rejected: invalid qty/price (qty=%s price=%s) %s", quantity, price, symbol)
            return False

        with DBManager() as db:
            # One open position per runner; skip if exists
            pos = db.get_open_position(runner_id=int(getattr(runner, "id")))
            if pos:
                log.info("BUY ignored: position already open for runner_id=%s symbol=%s", runner.id, symbol)
                return False

            # Basic cash guard
            acct: Account = db.ensure_account(user_id=user_id, name="mock")
            notional = float(quantity) * float(price)
            if float(acct.cash or 0.0) < notional:
                log.info("BUY rejected: insufficient cash (need=%.2f have=%.2f)", notional, float(acct.cash or 0.0))
                return False

            # Create position
            pos = OpenPosition(
                user_id=user_id,
                runner_id=int(getattr(runner, "id")),
                symbol=symbol,
                account="mock",
                quantity=int(quantity),
                avg_price=float(price),
                created_at=at,
                stop_price=None,
                trail_percent=None,
                highest_price=float(price),  # for LONGs; shorts will maintain best in meta
            )
            db.db.add(pos)

            # Account movement and a BUY order record
            acct.cash = float(acct.cash or 0.0) - notional
            acct.equity = float(acct.equity or 0.0) + notional  # mark-to-market will keep refining this

            ord = Order(
                user_id=user_id,
                runner_id=int(getattr(runner, "id")),
                symbol=symbol,
                side="BUY",
                order_type=str((decision or {}).get("order_type", "MKT")).upper(),
                quantity=int(quantity),
                limit_price=float((decision or {}).get("limit_price") or 0) or None,
                stop_price=None,
                status="filled",
                created_at=at,
                filled_at=at,
                details=None,
            )
            db.db.add(ord)
            db.db.commit()

            log_buy(
                user_id=user_id,
                runner_id=int(getattr(runner, "id")),
                symbol=symbol,
                qty=float(quantity),
                price=float(price),
                as_of=at,
                reason=(decision or {}).get("reason", "") or "strategy_buy",
            )
            return True

    def arm_trailing_stop_once(
        self,
        *,
        user_id: int,
        runner,
        entry_price: float,
        trail_pct: float,
        at: datetime,
        interval_min: Optional[int] = None,
    ) -> None:
        """
        Initialize trailing stop fields and arm the 1-bar activation delay.

        LONG:  initial stop = entry * (1 - pct)
        SHORT: initial stop = entry * (1 + pct)

        Guardrail: if computed stop is on the wrong side of price, clamp to the valid side and DO NOT trigger on this bar.
        """
        at = _utc(at)
        rid = int(getattr(runner, "id"))
        tf_min = int(interval_min or int(os.getenv("SIM_STEP_SECONDS", "300")) // 60 or 5)
        delay = timedelta(minutes=tf_min)

        with DBManager() as db:
            pos = db.get_open_position(runner_id=rid)
            if not pos:
                return  # nothing to arm

            # If already armed, do not rearm
            if (pos.trail_percent or 0) > 0 and pos.stop_price is not None:
                # Refresh delay meta if missing (idempotent safety)
                self._trail_meta.setdefault(
                    rid,
                    _TrailMeta(do_not_trigger_before=pos.created_at + delay, best_price=float(pos.highest_price or pos.avg_price)),
                )
                return

            side = self._infer_side(pos)

            pct = float(trail_pct) / 100.0
            if pct <= 0:
                return

            if side == "LONG":
                init_stop = entry_price * (1.0 - pct)
                # Guardrail: ensure strictly below price
                if init_stop >= entry_price:
                    log.error(
                        "Arming trail: invalid LONG stop computed >= price (stop=%.6f price=%.6f). Clamping.",
                        init_stop,
                        entry_price,
                    )
                    init_stop = entry_price - max(entry_price * pct, self._eps)

                pos.highest_price = float(entry_price)
                pos.stop_price = _round4(init_stop)
                pos.trail_percent = float(trail_pct)
                db.db.commit()

                self._trail_meta[rid] = _TrailMeta(
                    do_not_trigger_before=pos.created_at + delay,
                    best_price=float(entry_price),
                )
                log.info(
                    "Trailing armed LONG: runner=%s sym=%s entry=%.4f stop=%.4f pct=%.2f delay_until=%s",
                    rid,
                    pos.symbol,
                    entry_price,
                    pos.stop_price,
                    trail_pct,
                    (pos.created_at + delay).isoformat(),
                )
                return

            # SHORT (supported for compute parity; sim engine is long-only today)
            init_stop = entry_price * (1.0 + pct)
            if init_stop <= entry_price:
                log.error(
                    "Arming trail: invalid SHORT stop computed <= price (stop=%.6f price=%.6f). Clamping.",
                    init_stop,
                    entry_price,
                )
                init_stop = entry_price + max(entry_price * pct, self._eps)

            # We store best_price in meta; DB only has highest_price column—leave it as entry for observability
            pos.highest_price = float(entry_price)
            pos.stop_price = _round4(init_stop)
            pos.trail_percent = float(trail_pct)
            db.db.commit()

            self._trail_meta[rid] = _TrailMeta(
                do_not_trigger_before=pos.created_at + delay,
                best_price=float(entry_price),
            )
            log.info(
                "Trailing armed SHORT: runner=%s sym=%s entry=%.4f stop=%.4f pct=%.2f delay_until=%s",
                rid,
                pos.symbol,
                entry_price,
                pos.stop_price,
                trail_pct,
                (pos.created_at + delay).isoformat(),
            )

    def on_tick(self, *, user_id: int, runner, price: float, at: datetime) -> None:
        """
        Update trailing stops and flatten if hit.
        """
        at = _utc(at)
        rid = int(getattr(runner, "id"))

        with DBManager() as db:
            pos = db.get_open_position(runner_id=rid)
            if not pos:
                return

            side = self._infer_side(pos)
            pct = float(pos.trail_percent or 0.0)
            if pct <= 0.0:
                return  # no trailing stop for this position

            meta = self._trail_meta.get(rid)
            if not meta:
                meta = _TrailMeta(do_not_trigger_before=pos.created_at, best_price=float(pos.highest_price or pos.avg_price))
                self._trail_meta[rid] = meta

            # Update "best price" in the favorable direction
            if side == "LONG":
                meta.best_price = max(float(meta.best_price), float(price))
                pos.highest_price = float(meta.best_price)
                new_stop = max(float(pos.stop_price or -math.inf), float(meta.best_price) * (1.0 - pct / 100.0))
                new_stop = _round4(new_stop)

                # Guardrail: if somehow stop is not strictly below price, clamp and avoid triggering on this tick
                if new_stop >= price:
                    log.error(
                        "Trailing LONG guard: computed stop >= price (stop=%.6f price=%.6f). Clamping and skipping this tick.",
                        new_stop,
                        price,
                    )
                    pos.stop_price = _round4(min(price - self._eps, new_stop))
                    db.db.commit()
                    return

                # Persist any trail-up
                if pos.stop_price is None or new_stop > float(pos.stop_price):
                    pos.stop_price = new_stop
                    db.db.commit()
                    log.debug(
                        "Trail update LONG: runner=%s %s best=%.4f stop=%.4f",
                        rid, pos.symbol, meta.best_price, pos.stop_price
                    )

                # Activation delay: do not evaluate until next bar
                if at < meta.do_not_trigger_before:
                    return

                # Trigger?
                if price <= float(pos.stop_price):
                    self._exit_trailing_stop(db, pos, user_id, runner, price, at)
                return

            # SHORT
            meta.best_price = min(float(meta.best_price), float(price))
            new_stop = min(float(pos.stop_price or math.inf), float(meta.best_price) * (1.0 + pct / 100.0))
            new_stop = _round4(new_stop)

            if new_stop <= price:
                log.error(
                    "Trailing SHORT guard: computed stop <= price (stop=%.6f price=%.6f). Clamping and skipping this tick.",
                    new_stop,
                    price,
                )
                pos.stop_price = _round4(max(price + self._eps, new_stop))
                db.db.commit()
                return

            if pos.stop_price is None or new_stop < float(pos.stop_price):
                pos.stop_price = new_stop
                db.db.commit()
                log.debug("Trail update SHORT: runner=%s %s best=%.4f stop=%.4f", rid, pos.symbol, meta.best_price, pos.stop_price)

            if at < meta.do_not_trigger_before:
                return

            if price >= float(pos.stop_price):
                self._exit_trailing_stop(db, pos, user_id, runner, price, at)

    def sell_all(
        self,
        *,
        user_id: int,
        runner,
        symbol: str,
        price: float,
        decision: Dict[str, Any] | None,
        at: datetime,
        reason_override: Optional[str] = None,
    ) -> bool:
        at = _utc(at)
        symbol = (symbol or "").upper()
        rid = int(getattr(runner, "id"))

        with DBManager() as db:
            pos = db.get_open_position(runner_id=rid)
            if not pos:
                return False

            qty = float(pos.quantity or 0)
            if qty == 0:
                return False

            avg = float(pos.avg_price or 0.0)
            pnl = (float(price) - avg) * qty
            pnl_pct = 0.0 if avg == 0 else ((float(price) / avg) - 1.0) * 100.0

            # Account
            acct: Account = db.ensure_account(user_id=user_id, name="mock")
            notional = qty * float(price)
            acct.cash = float(acct.cash or 0.0) + notional
            acct.equity = float(acct.equity or 0.0)  # mark-to-market will update later

            # Order + ExecutedTrade
            ord = Order(
                user_id=user_id,
                runner_id=rid,
                symbol=symbol,
                side="SELL",
                order_type=str((decision or {}).get("order_type", "MKT")).upper(),
                quantity=int(qty),
                limit_price=float((decision or {}).get("limit_price") or 0) or None,
                stop_price=None,
                status="filled",
                created_at=at,
                filled_at=at,
                details=None,
            )
            db.db.add(ord)

            tr = ExecutedTrade(
                perm_id=None,
                user_id=user_id,
                runner_id=rid,
                symbol=symbol,
                buy_ts=pos.created_at,
                sell_ts=at,
                buy_price=_round4(avg),
                sell_price=_round4(price),
                quantity=qty,
                pnl_amount=_round4(pnl),
                pnl_percent=_round4(pnl_pct),
                strategy=str(getattr(runner, "strategy", "")),
                timeframe=str(int(getattr(runner, "time_frame", 5) or 5)) + "m",
            )
            db.db.add(tr)

            # Remove open position
            db.db.delete(pos)
            db.db.commit()

            # Clear meta
            self._trail_meta.pop(rid, None)

            log_sell(
                user_id=user_id,
                runner_id=rid,
                symbol=symbol,
                qty=qty,
                avg_price=avg,
                price=float(price),
                as_of=at,
                reason=(reason_override or (decision or {}).get("reason") or "strategy_sell"),
            )
            return True

    def mark_to_market_all(self, *, user_id: int, at: datetime) -> None:
        """
        Refresh account equity from last prices; cash unchanged.
        """
        at = _utc(at)
        with DBManager() as db:
            acct = db.ensure_account(user_id=user_id, name="mock")
            # Get all open positions
            from sqlalchemy import select
            from database.models import OpenPosition
            positions = db.db.execute(
                select(OpenPosition.symbol, OpenPosition.quantity, OpenPosition.avg_price)
                .where(OpenPosition.user_id == user_id)
            ).all()
            if not positions:
                # Equity equals cash when no positions
                acct.equity = float(acct.cash or 0.0)
                db.db.commit()
                return

            symbols = [row._mapping["symbol"] for row in positions]
            tf = 5  # use 5m for mark-to-market in sim window
            last_prices = self._mkt.get_last_close_for_symbols(symbols, minutes=tf, as_of=at, regular_hours_only=True)
            equity = float(acct.cash or 0.0)
            for row in positions:
                m = row._mapping
                s = m["symbol"]
                qty = float(m["quantity"] or 0)
                avg = float(m["avg_price"] or 0.0)
                px = float(last_prices.get(s, avg))
                equity += qty * px
            acct.equity = equity
            db.db.commit()

    # ─────────────────────────── Helpers ───────────────────────────

    @staticmethod
    def _infer_side(pos: OpenPosition) -> SideT:
        """
        LONG if quantity > 0, else SHORT (sim engine is long-only currently).
        """
        try:
            return "LONG" if float(pos.quantity or 0.0) > 0 else "SHORT"
        except Exception:
            return "LONG"

    def _exit_trailing_stop(
        self,
        db: DBManager,
        pos: OpenPosition,
        user_id: int,
        runner,
        price: float,
        at: datetime,
    ) -> None:
        """
        Execute a trailing-stop exit, with clear logging.
        """
        rid = int(getattr(runner, "id"))
        side = self._infer_side(pos)
        stop = float(pos.stop_price or 0.0)
        log.info(
            "Exit via trailing stop: runner=%s sym=%s side=%s price=%.4f stop=%.4f at=%s",
            rid, pos.symbol, side, float(price), stop, _utc(at).isoformat()
        )
        self.sell_all(
            user_id=user_id,
            runner=runner,
            symbol=pos.symbol,
            price=float(price),
            decision={"order_type": "MKT", "reason": "trailing_stop"},
            at=at,
            reason_override="trailing_stop",
        )

    # ─────────────────────────── Pure helpers (unit-test friendly) ───────────────────────────

    @staticmethod
    def compute_trail_update(
        *,
        side: SideT,
        price: float,
        trail_pct: float,
        prev_stop: Optional[float],
        prev_best: float,
        eps: float = 1e-6,
    ) -> Tuple[float, float]:
        """
        Pure math for trailing stops (no DB side effects).

        Returns: (new_stop, new_best)

        LONG : new_best = max(prev_best, price); new_stop = max(prev_stop, new_best * (1 - pct))
        SHORT: new_best = min(prev_best, price); new_stop = min(prev_stop, new_best * (1 + pct))

        Guarded so the stop always stays strictly on the correct side of price.
        """
        pct = float(trail_pct) / 100.0
        if side == "LONG":
            best = max(float(prev_best), float(price))
            candidate = best * (1.0 - pct)
            stop = candidate if prev_stop is None else max(float(prev_stop), candidate)
            if stop >= price:
                stop = min(price - eps, stop)
            return (_round4(stop), best)

        # SHORT
        best = min(float(prev_best), float(price))
        candidate = best * (1.0 + pct)
        stop = candidate if prev_stop is None else min(float(prev_stop), candidate)
        if stop <= price:
            stop = max(price + eps, stop)
        return (_round4(stop), best)

    @staticmethod
    def should_trigger_stop(*, side: SideT, price: float, stop: float) -> bool:
        if side == "LONG":
            return float(price) <= float(stop)
        return float(price) >= float(stop)
