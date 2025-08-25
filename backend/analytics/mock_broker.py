from __future__ import annotations
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from backend.ib_manager.market_data_manager import MarketDataManager


@dataclass
class _Order:
    ibkr_perm_id: int
    user_id: int
    runner_id: Optional[int]
    symbol: str
    action: str  # BUY | SELL
    order_type: str
    quantity: float
    limit_price: Optional[float]
    status: str  # Filled
    submitted_time: datetime
    last_updated: datetime


@dataclass
class _Position:
    symbol: str
    quantity: float
    avgCost: float
    account: str = "ANALYTICS"


class _IBView:
    def __init__(self, trades_provider):
        self._trades_provider = trades_provider

    def trades(self):
        # Return objects with .contract.symbol and .orderStatus.status
        res = []
        for o in self._trades_provider():
            contract = SimpleNamespace(symbol=o.symbol)
            orderStatus = SimpleNamespace(status=o.status)
            res.append(SimpleNamespace(contract=contract, orderStatus=orderStatus, order=SimpleNamespace(permId=o.ibkr_perm_id)))
        return res


class MockBusinessManager:
    """
    Drop-in replacement for IBBusinessManager in analytics mode.
    Executes orders instantly at current price and maintains in-memory positions.
    """

    def __init__(self, user, component: str = "analytics") -> None:
        self.user = user
        self.component = component
        self._orders: list[_Order] = []
        self._trades: list[dict] = []  # trades to be returned by sync_executed_trades
        self._positions: dict[str, _Position] = {}
        self._next_perm_id: int = int(os.getenv("ANALYTICS_PERM_ID_BASE", "500000"))
        self.mkt = MarketDataManager()
        self.ib = _IBView(lambda: self._orders)

    # ───────── helpers ─────────
    def _now(self) -> datetime:
        ts = os.getenv("SIM_TIME_EPOCH")
        if ts:
            try:
                return datetime.fromtimestamp(int(ts), tz=timezone.utc)
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _alloc_perm_id(self) -> int:
        self._next_perm_id += 1
        return self._next_perm_id

    # ───────── account/state reads used by sync_service ─────────
    async def get_account_information(self) -> dict:
        # Minimal snapshot
        return {
            "account": "ANALYTICS",
            "timestamp": self._now(),
            "total_cash_value": 0.0,
            "net_liquidation": 0.0,
            "available_funds": 0.0,
            "buying_power": 0.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "excess_liquidity": 0.0,
            "gross_position_value": 0.0,
        }

    def get_open_positions(self) -> list[dict]:
        return [vars(p) for p in self._positions.values()]

    async def sync_orders_from_ibkr(self, *, user_id: int) -> list[dict]:
        # Return all orders from "today" in simulated time
        now = self._now()
        start_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        out: list[dict] = []
        for o in self._orders:
            if o.submitted_time >= start_day:
                out.append({
                    "ibkr_perm_id": o.ibkr_perm_id,
                    "user_id": o.user_id,
                    "runner_id": o.runner_id,
                    "symbol": o.symbol,
                    "action": o.action,
                    "order_type": o.order_type,
                    "quantity": o.quantity,
                    "limit_price": o.limit_price,
                    "status": o.status,
                    "last_updated": o.last_updated,
                    "created_at": o.submitted_time,
                })
        return out

    def sync_executed_trades(self, *, user_id: int) -> list[dict]:
        # Drain trades queue for idempotency across calls
        trades = self._trades
        self._trades = []
        return trades

    # ───────── order management used by executor ─────────
    async def cancel_open_orders_for_symbol(self, symbol: str) -> list[int]:
        # All orders are filled instantly in analytics mode
        return []

    async def place_order_from_decision(
        self,
        *,
        decision: Dict[str, Any],
        user_id: int,
        runner_id: Optional[int],
        symbol: str,
        wait_fill: bool = True,
    ) -> dict | None:
        action = str(decision.get("action") or "").upper()
        if action not in {"BUY", "SELL"}:
            return None
        qty = float(decision.get("quantity") or 0)
        if qty <= 0:
            return None

        px = float(decision.get("limit_price") or self.mkt.get_current_price(symbol) or 0.0)
        if px <= 0:
            return None

        perm_id = self._alloc_perm_id()
        now = self._now()
        order = _Order(
            ibkr_perm_id=perm_id,
            user_id=user_id,
            runner_id=runner_id,
            symbol=symbol,
            action=action,
            order_type=str(decision.get("order_type") or "MKT").upper(),
            quantity=qty,
            limit_price=decision.get("limit_price"),
            status="Filled",
            submitted_time=now,
            last_updated=now,
        )
        self._orders.append(order)

        # Update positions
        pos = self._positions.get(symbol)
        if action == "BUY":
            if pos is None:
                pos = _Position(symbol=symbol, quantity=0.0, avgCost=0.0)
            new_qty = pos.quantity + qty
            pos.avgCost = ((pos.avgCost * pos.quantity) + (px * qty)) / new_qty if new_qty else 0.0
            pos.quantity = new_qty
            self._positions[symbol] = pos
        else:
            if pos is None:
                pos = _Position(symbol=symbol, quantity=0.0, avgCost=0.0)
            sell_qty = min(qty, pos.quantity)
            pos.quantity = max(0.0, pos.quantity - sell_qty)
            if pos.quantity == 0:
                pos.avgCost = 0.0
            self._positions[symbol] = pos

        # Record trade for sync_executed_trades
        self._trades.append({
            "user_id": user_id,
            "runner_id": runner_id,
            "perm_id": perm_id,
            "symbol": symbol,
            "action": action,
            "order_type": order.order_type,
            "quantity": qty,
            "price": px,
            "commission": 0.0,
            "fill_time": now,
            "account": "ANALYTICS",
        })

        return {
            "ibkr_perm_id": perm_id,
            "user_id": user_id,
            "runner_id": runner_id,
            "symbol": symbol,
            "action": action,
            "order_type": order.order_type,
            "quantity": qty,
            "limit_price": order.limit_price,
            "status": order.status,
            "submitted_time": now,
            "last_updated": now,
        }

    async def flat_position(self, symbol: str, *, user_id: int, runner_id: Optional[int] = None) -> dict | None:
        pos = self._positions.get(symbol)
        if not pos or pos.quantity <= 0:
            return None
        return await self.place_order_from_decision(
            decision={"action": "SELL", "quantity": pos.quantity, "order_type": "MKT"},
            user_id=user_id,
            runner_id=runner_id,
            symbol=symbol,
            wait_fill=True,
        )


