# backend/ib_manager/ib_connector.py
"""
IBBusinessManager – safe façade around ib_insync.IB()

Key points
──────────
• Every `connect()` call starts from a **fresh** IB() instance and
  closes sockets/tasks on failure – zero FD leaks.
• Skips connect attempts during IBKR’s nightly restart window
  (23:45-00:45 ET) and applies exponential back-off.
• Helpers for: account snapshot, open positions, placing / flattening
  orders, merged-fill sync and brutal “close-everything” failsafe.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from ib_insync import IB, LimitOrder, MarketOrder, Order, Stock, StopOrder, Trade, util

from backend.ib_manager.market_data_manager import MarketDataManager
from datetime import datetime, time, timezone, timedelta
import os
try:
    from zoneinfo import ZoneInfo
except Exception:
    from dateutil import tz as _tz
    ZoneInfo = lambda name: _tz.gettz(name)

log = logging.getLogger("IBKR-Business-Manager")
_per_user_locks: dict[int, asyncio.Lock] = {}   # user.id → lock
# ────────────────────────── env / constants ──────────────────────────
IB_CONNECTION_TIMEOUT = int(os.getenv("IB_CONNECTION_TIMEOUT", "90"))
WAIT_RTH_SEC          = int(os.getenv("ORDER_WAIT_RTH",  "10"))
WAIT_XRTH_SEC         = int(os.getenv("ORDER_WAIT_XRTH", "25"))
XRTH_TRADING_ENABLED  = os.getenv("XRTH_TRADING_ENABLED", "true").lower() == "true"

class IBBusinessManager:  # kept for import compatibility
	def __init__(self, user, component: str = "api") -> None:
		raise RuntimeError("IBBusinessManager is not available in analytics-only project. Use MockBusinessManager.")

class MaintenanceWindowError(RuntimeError):
	"""Raised when we are inside IBKR’s nightly maintenance window."""
	pass

# Legacy helper kept for import points that reference it; always false in analytics
async def _in_maintenance_window(now_utc=None) -> bool:
	"""
	IBKR nightly restart is officially 23:45–00:45 ET, but in practice things
	get flaky a bit before/after. We add a configurable buffer (minutes)
	around that window and skip connects entirely inside it.

	Env:
	  IB_MAINTENANCE_BUFFER_MIN (default 20)
	"""
	buf = int(os.getenv("IB_MAINTENANCE_BUFFER_MIN", "20"))

	et = ZoneInfo("America/New_York")
	now = (now_utc or datetime.now(timezone.utc)).astimezone(et).time()

	# Base window
	start = time(23, 45)  # 23:45
	end   = time(0, 45)   # 00:45

	# Apply ±buffer by working in minutes and wrapping midnight
	def _shift(t: time, delta_min: int) -> time:
		total = (t.hour * 60 + t.minute + delta_min) % (24 * 60)
		return time(total // 60, total % 60)

	start_b = _shift(start, -buf)  # earlier
	end_b   = _shift(end,   +buf)  # later

	# Window crosses midnight → we treat it as:  [start_b .. 24h) U [00:00 .. end_b]
	return (now >= start_b) or (now <= end_b)


# ---------------------------------------------------------------------
def _next_client_id(component: str, user_id: int) -> int:
    """Stable per-process client-id range so API-gateway & Scheduler never collide."""
    base = {"scheduler": 1_000, "api": 2_000}.get(component, 9_000)
    return base + user_id


# ---------------------------------------------------------------------
class IBBusinessManager:
    """
    Thin, **safe** wrapper around ib_insync.IB().
    Always call `connect()` once right before doing IB work – it is idempotent.
    """

    def __init__(self, user, component: str = "api") -> None:
        self.user       = user
        self.component  = component
        self.gw_host    = os.getenv("IB_GATEWAY_HOST", "ib-gateway-1")
        self.gw_port    = int(os.getenv("IB_GATEWAY_PORT", 4004))
        self._new_ib()

    # ─────────── IB lifecycle ───────────
    def _new_ib(self) -> None:
        log.debug(f"Creating new IB instance for user {self.user.username}")
        self.ib = IB()                       # brand-new socket/tasks pool
        log.debug(f"New IB instance created for user {self.user.username}")

    def _close_ib(self) -> None:
        log.debug(f"Closing IB connection for user {self.user.username}")
        try:
            if self.ib.isConnected():
                log.debug(f"Disconnecting IB for user {self.user.username}")
                self.ib.disconnect()
                log.debug(f"IB disconnected for user {self.user.username}")
            else:
                log.debug(f"IB was not connected for user {self.user.username}")
        except Exception as e:
            log.warning(f"Error during IB disconnect for user {self.user.username}: {e}")
        # Don't automatically create new IB instance - do it lazily when needed
  

    async def connect(self, max_retries: int = 3) -> None:
        """
        Idempotent and concurrency-safe.

        Multiple coroutines for the same user share an asyncio.Lock so only one
        socket handshake runs; the rest await the result.
        """
        log.debug(f"IB connect requested for user {self.user.username} (max_retries={max_retries})")
        
        if _in_maintenance_window():
            log.warning(f"IB connect blocked - maintenance window for user {self.user.username}")
            raise MaintenanceWindowError("IB Gateway in nightly maintenance window")

        if self.ib.isConnected():
            log.debug(f"IB already connected for user {self.user.username}")
            return

        lock = _per_user_locks.setdefault(self.user.id, asyncio.Lock())
        log.debug(f"Acquiring IB connection lock for user {self.user.username}")

        async with lock:
            if self.ib.isConnected():  # someone else connected meanwhile
                log.debug(f"IB connected by another process for user {self.user.username}")
                return

            # Retry logic with exponential backoff
            last_exception = None
            for attempt in range(max_retries):
                if attempt > 0:
                    wait_time = min(2 ** attempt, 10)  # Cap at 10 seconds
                    log.info(f"Retrying IB connection for {self.user.username} (attempt {attempt + 1}/{max_retries}) after {wait_time}s")
                    await asyncio.sleep(wait_time)

                log.debug(f"Starting IB connection attempt {attempt + 1}/{max_retries} for user {self.user.username}")
                self._close_ib()  # nuke any half-open state
                # Ensure we have a fresh IB instance
                if not hasattr(self, 'ib') or self.ib is None:
                    self._new_ib()
                base_client_id = _next_client_id(self.component, self.user.id)
                # Use process ID and timestamp to ensure uniqueness
                import time
                import os
                client_id = base_client_id + (os.getpid() % 100) + (int(time.time() * 1000) % 1000)

                log.info(
                    "IB connect → user=%s cid=%d host=%s:%d (attempt %d/%d)",
                    self.user.username, client_id, self.gw_host, self.gw_port, attempt + 1, max_retries
                )

                try:
                    log.debug(f"Calling connectAsync for user {self.user.username} with client_id={client_id}, host={self.gw_host}:{self.gw_port}, timeout={IB_CONNECTION_TIMEOUT}")
                    await self.ib.connectAsync(
                        host=self.gw_host,
                        port=self.gw_port,
                        clientId=client_id,
                        timeout=IB_CONNECTION_TIMEOUT,
                    )
                    log.debug(f"connectAsync completed for user {self.user.username}")
                    
                    if not self.ib.isConnected():
                        log.error(f"IB connectAsync succeeded but isConnected() returned False for user {self.user.username}")
                        raise ConnectionError("Connected=False after connectAsync()")
                    
                    log.debug(f"IB connection established for user {self.user.username}, testing with reqCurrentTimeAsync")
                    
                    # Test the connection with a simple request
                    try:
                        await asyncio.wait_for(self.ib.reqCurrentTimeAsync(), timeout=10.0)
                        log.info(f"IB connection established and tested successfully for user {self.user.username}")
                        return  # Success - exit retry loop
                    except asyncio.TimeoutError:
                        log.warning(f"IB connection established but time sync failed for user {self.user.username} - continuing anyway")
                        return  # Success - exit retry loop
                    except Exception as e:
                        log.warning(f"IB connection test failed for user {self.user.username}: {e} - continuing anyway")
                        return  # Success - exit retry loop
                        
                except Exception as e:
                    last_exception = e
                    log.warning(f"IB connect attempt {attempt + 1}/{max_retries} failed for user {self.user.username}: {type(e).__name__}: {e}")
                    self._close_ib()
                    if attempt == max_retries - 1:  # Last attempt
                        log.exception(f"All IB connect attempts failed for user {self.user.username}")
                        raise last_exception


    def disconnect(self) -> None:
        self._close_ib()

    async def health_check(self) -> bool:
        """Check if the IB connection is still alive and responsive."""
        log.debug(f"IB health check requested for user {self.user.username}")
        
        if not self.ib.isConnected():
            log.debug(f"IB health check failed - not connected for user {self.user.username}")
            return False
        
        try:
            log.debug(f"IB health check - calling reqCurrentTimeAsync for user {self.user.username}")
            await asyncio.wait_for(self.ib.reqCurrentTimeAsync(), timeout=5.0)
            log.debug(f"IB health check successful for user {self.user.username}")
            return True
        except Exception as e:
            log.warning(f"IB health check failed for user {self.user.username}: {type(e).__name__}: {e}")
            return False

    # ───────────────────── account helpers ─────────────────────
    async def get_account_information(self) -> dict:
        try:
            summary = await self.ib.accountSummaryAsync()
        except Exception:                    # network hiccup
            log.exception("accountSummary failed")
            return {}

        wanted = {
            "TotalCashValue",
            "CashBalance",
            "AccruedCash",
            "AvailableFunds",
            "ExcessLiquidity",
            "NetLiquidation",
            "RealizedPnL",
            "UnrealizedPnL",
            "GrossPositionValue",
            "BuyingPower",
        }
        acct: str | None = None
        result: Dict[str, Any] = {}
        for item in summary:
            if item.tag in wanted:
                key = f"{item.tag} ({item.currency})" if item.currency else item.tag
                result[key] = float(item.value)
            if not acct and item.account != "All":
                acct = item.account
        if acct:
            result["account"] = acct
        return result

    # live positions ----------------------------------------------------
    def get_open_positions(self) -> list[dict]:
        positions = self.ib.positions()
        return [
            {
                "symbol":   p.contract.symbol,
                "quantity": p.position,
                "avgCost":  p.avgCost,
                "account":  p.account,
            }
            for p in positions
        ]

    # ───────────────────── order helpers ─────────────────────
    async def flat_position(self, symbol: str, *, user_id: int, runner_id: int | None = None) -> dict | None:
        """
        Close any open position with a super‑tight 0.5% limit order.

        • Works for both long and short positions.
        • Waits up to WAIT_RTH_SEC (RTH) or WAIT_XRTH_SEC (extended) for the
        fill, then cancels the order and raises TimeoutError with a rich
        debug payload.
        """
        submitted_at = datetime.now(timezone.utc)

        if not self.ib.isConnected():
            await self.connect()

        # Ensure positions cache is fresh
        try:
            await self.ib.reqPositionsAsync()
        except Exception:
            log.debug("reqPositionsAsync failed (will use cached positions)", exc_info=True)

        pos = next(
            (p for p in self.ib.positions() if p.contract.symbol.upper() == symbol.upper()),
            None,
        )
        if not pos or pos.position == 0:
            return None  # nothing to flatten

        action = "SELL" if pos.position > 0 else "BUY"
        qty = abs(int(pos.position))

        contract = Stock(symbol.upper(), "SMART", "USD")
        await self.ib.qualifyContractsAsync(contract)

        mdm = MarketDataManager()
        session = await mdm.get_us_market_session()

        # Prefer Market orders during RTH for guaranteed fills
        if session == "open":
            order = MarketOrder(action, qty)
            order.tif = "DAY"
            order.outsideRth = False
        else:
            # Outside RTH: use a protective limit around last known price
            last_px = mdm.get_current_price(symbol) or float(pos.avgCost or 1.0)
            limit_px = round(last_px * (0.995 if action == "SELL" else 1.005), 2)
            order = LimitOrder(action, qty, limit_px, tif="GTC", outsideRth=True)

        if runner_id:
            order.orderRef = f"runner:{runner_id}"

        trade = self.ib.placeOrder(contract, order)
        timeout = WAIT_RTH_SEC if session == "open" else WAIT_XRTH_SEC

        filled, _ = await self._wait_for_fill(trade, timeout=timeout)

        if not filled:
            try:
                self.ib.cancelOrder(trade.order)
            except Exception:
                pass
            # For non-RTH we can retry once with a wider limit band
            if session != "open":
                widen_last = mdm.get_current_price(symbol) or float(pos.avgCost or 1.0)
                retry_limit = round(widen_last * (0.98 if action == "SELL" else 1.02), 2)
                retry_order = LimitOrder(action, qty, retry_limit, tif="GTC", outsideRth=True)
                if runner_id:
                    retry_order.orderRef = f"runner:{runner_id}"
                retry_trade = self.ib.placeOrder(contract, retry_order)
                retry_filled, _ = await self._wait_for_fill(retry_trade, timeout=min(5, timeout))
                if not retry_filled:
                    try:
                        self.ib.cancelOrder(retry_trade.order)
                    finally:
                        raise TimeoutError(
                            json.dumps(
                                self._timeout_debug(
                                    trade=trade,
                                    limit_px=getattr(order, "lmtPrice", None),
                                    symbol=symbol,
                                    waited=timeout,
                                    session="RTH" if session == "open" else "XRTH",
                                ),
                                default=str,
                            )
                        )
            else:
                # In RTH, a market order not filling within timeout is unexpected
                raise TimeoutError(
                    json.dumps(
                        self._timeout_debug(
                            trade=trade,
                            limit_px=None,
                            symbol=symbol,
                            waited=timeout,
                            session="RTH",
                        ),
                        default=str,
                    )
                )

        return {
            "user_id":        user_id,
            "runner_id":      runner_id,
            "symbol":         symbol,
            "action":         action,
            "order_type":     getattr(trade.order, "orderType", None) or ("MKT" if isinstance(order, MarketOrder) else "LMT"),
            "quantity":       qty,
            "limit_price":    getattr(order, "lmtPrice", None),
            "status":         trade.orderStatus.status,
            "perm_id":        trade.order.permId,
            "filled_qty":     trade.orderStatus.filled,
            "submitted_time": submitted_at,
            "avg_fill_price": trade.orderStatus.avgFillPrice,
        }

    async def place_order_from_decision(
        self,
        *,
        decision : dict,
        user_id  : int,
        runner_id: int | None,
        symbol   : str,
        wait_fill: bool = False
    ) -> dict | None:
        """
        Submit BUY or SELL order based on a decision dict.
        If *wait_fill* is True the coroutine waits for **Filled**. On timeout
        the order is cancelled and a TimeoutError (with diagnostic JSON) is raised.
        Always includes `user_id` in the returned payload.
        """
        submitted_at = datetime.now(timezone.utc)

        if not self.ib.isConnected():
            await self.connect()

        action       = decision["action"].upper()
        requested_ot = decision.get("order_type", "MKT").upper()
        limit_px_in  = decision.get("limit_price")

        contract = Stock(symbol, "SMART", "USD")
        await self.ib.qualifyContractsAsync(contract)

        mdm      = MarketDataManager()
        session  = await mdm.get_us_market_session()
        in_rth   = session == "open"

        # -------- quantity resolution (SELL prefers live IB position) --------
        quantity = int(decision.get("quantity") or 0)
        if action == "SELL":
            pos = next((p for p in self.ib.positions() if p.contract.symbol.upper() == symbol.upper()), None)
            live_qty = abs(int(pos.position)) if pos else 0
            if live_qty > 0:
                quantity = live_qty
            if quantity <= 0:
                # Nothing to sell
                return None

        # -------- construct order -------------------------------------------
        if requested_ot == "LMT":
            if limit_px_in is None:
                raise ValueError("limit_price required for LMT orders")
            parent = LimitOrder(action, quantity, limit_px_in)

        elif requested_ot == "MKT":
            # During XRTH we convert BUY market orders to protective limits.
            if action == "BUY" and not in_rth:
                last      = mdm.get_current_price(symbol) or 1.0
                cushion   = float(os.getenv("XRTH_MARKET_TO_LIMIT_PCT", "0.03"))
                limit_px_in = round(
                    last * (1 + cushion) if action == "BUY" else last * (1 - cushion),
                    2
                )
                parent = LimitOrder(action, quantity, limit_px_in)
                requested_ot = "LMT"
            else:
                parent = util.MarketOrder(action, quantity)

        else:
            raise ValueError(f"Unsupported order_type '{requested_ot}'")

        parent.tif        = "GTC"
        parent.outsideRth = not in_rth
        parent.transmit   = True
        if runner_id:
            parent.orderRef = f"runner:{runner_id}"

        parent_trade = self.ib.placeOrder(contract, parent)
        await asyncio.sleep(0.1)  # let status propagate

        # -------- optional wait-for-fill ------------------------------------
        if wait_fill:
            timeout = WAIT_RTH_SEC if in_rth else WAIT_XRTH_SEC
            filled_ok, _last = await self._wait_for_fill(parent_trade, timeout=timeout)
            if not filled_ok:
                try:
                    self.ib.cancelOrder(parent_trade.order)
                finally:
                    debug = self._timeout_debug(
                        trade    = parent_trade,
                        limit_px = getattr(parent, "lmtPrice", None),
                        symbol   = symbol,
                        waited   = timeout,
                        session  = "RTH" if in_rth else "XRTH",
                    )
                    raise TimeoutError(json.dumps(debug, default=str))

        await asyncio.sleep(0.2)  # make sure orderStatus is fresh

        return {
            "user_id"       : user_id,
            "runner_id"     : runner_id,
            "symbol"        : symbol,
            "action"        : action,
            "quantity"      : quantity,
            "order_type"    : requested_ot,
            "limit_price"   : getattr(parent, "lmtPrice", None),
            "ibkr_perm_id"  : parent_trade.order.permId,
            "status"        : parent_trade.orderStatus.status,
            "submitted_time": submitted_at,
            "filled_qty"    : parent_trade.orderStatus.filled,
            "avg_fill_price": parent_trade.orderStatus.avgFillPrice,
        }




# ───────────────────────── orders / trades ──────────────────────────
    async def sync_orders_from_ibkr(self, *, user_id: int) -> list[dict]:
        """
        Return every order IBKR still reports for this account, enriched with:

        • reliable timestamp
        • *original* quantity (never 0)
        • limit- and stop-prices whenever they apply
        """
        try:
            trades: list[Trade] = list(self.ib.trades())
            if not trades:
                log.debug("No orders found for user %d", user_id)
                return []

            out: list[dict] = []

            for tr in trades:
                pid = tr.order.permId
                if not pid:                       # sanity-guard
                    continue

                # ─────── timestamp ────────────────────────────────────────
                raw_ts = (
                    getattr(tr.orderStatus, "lastUpdateTime", None)
                    or getattr(getattr(tr.order, "orderState", None), "completedTime", None)
                )
                ts = None
                if raw_ts:
                    try:
                        ts = util.parseIBDatetime(raw_ts)
                    except Exception:
                        pass
                if ts is None and tr.log:
                    ts = tr.log[-1].time
                if ts is None:                    # still unknown → skip
                    log.debug("Skipping order %s – no timestamp", pid)
                    continue

                # ─────── runner FK (from orderRef) ────────────────────────
                tag = tr.order.orderRef or ""
                runner_id_val = (
                    int(tag.removeprefix("runner:")) if tag.startswith("runner:") else None
                )

                # ─────── robust original quantity ─────────────────────────
                total   = tr.order.totalQuantity or 0
                filled  = tr.orderStatus.filled    or 0
                remain  = tr.orderStatus.remaining or 0
                qty     = max(total, filled, filled + remain)

                if qty == 0:                      # final guard → sum fills
                    qty = sum(f.execution.shares for f in tr.fills) if tr.fills else 0

                # ─────── limit / stop prices ──────────────────────────────
                otype = tr.order.orderType.upper()

                # Limit price (only meaningful for LMT / STP LMT / TRAIL LIMIT)
                limit_px = getattr(tr.order, "lmtPrice", None)
                if limit_px in {None, 0} or (isinstance(limit_px, float) and math.isnan(limit_px)):
                    limit_px = None
                if otype not in {"LMT", "STP LMT", "TRAIL LIMIT"}:
                    limit_px = None

                # Stop price for every stop family
                if otype in {"STP", "STP LMT"}:
                    stop_px = tr.order.auxPrice
                elif otype.startswith("TRAIL"):
                    stop_px = getattr(tr.order, "trailStopPrice", None)
                else:
                    stop_px = None

                if stop_px in {None, 0} or (
                    isinstance(stop_px, float) and math.isnan(stop_px)
                ):
                    stop_px = None

                # ─────── final payload ────────────────────────────────────
                out.append(
                    {
                        "user_id":         user_id,
                        "runner_id":       runner_id_val,
                        "ibkr_perm_id":    pid,
                        "symbol":          tr.contract.symbol,
                        "action":          tr.order.action,
                        "order_type":      tr.order.orderType,
                        "quantity":        qty,            # ← never 0/None now
                        "limit_price":     limit_px,       # ← when relevant
                        "stop_price":      stop_px,        # ← when relevant
                        "status":          tr.orderStatus.status,
                        "filled_quantity": filled,
                        "avg_fill_price":  tr.orderStatus.avgFillPrice,
                        "account":         tr.order.account or "",
                        "last_updated":    ts,
                    }
                )

            log.debug("Fetched %d orders from IBKR for user %d", len(out), user_id)
            return out

        except Exception:
            log.exception("sync_orders_from_ibkr failed for user %d", user_id)
            return []




    # ─────────────────── orders / trades ────────────────────
    def sync_executed_trades(self, *, user_id: int) -> list[dict]:
        """
        Harvest NEW executions from the last 15 min, merge fills that share
        (permId, price), **sum the commissions**, and free the cached Trade
        objects to stop un-bounded memory growth.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=15)
        combined: dict[tuple[int, float], dict] = {}
        seen_exec_ids: set[str] = set()

        for tr in list(self.ib.trades()):            # copy – we will clear()
            pid = tr.order.permId or 0
            if not pid:
                continue

            tag = tr.order.orderRef or ""
            runner_id_val = int(tag[7:]) if tag.startswith("runner:") else None

            for f in tr.fills:
                if f.time < cutoff or f.execution.shares == 0:
                    continue
                if f.execution.execId in seen_exec_ids:
                    continue
                seen_exec_ids.add(f.execution.execId)

                px2 = round(float(f.execution.price), 2)
                key = (pid, px2)
                bucket = combined.setdefault(key, {
                    "user_id":    user_id,
                    "runner_id":  runner_id_val,
                    "perm_id":    pid,
                    "symbol":     tr.contract.symbol,
                    "action":     tr.order.action,
                    "order_type": tr.order.orderType,
                    "quantity":   0,
                    "price":      px2,
                    "commission": 0.0,
                    "fill_time":  f.time.astimezone(timezone.utc).replace(microsecond=0),
                    "account":    f.execution.acctNumber,
                })

                bucket["quantity"]   += f.execution.shares
                bucket["commission"] += abs(
                    getattr(getattr(f, "commissionReport", None), "commission", 0.0)
                )
                if f.time > bucket["fill_time"]:
                    bucket["fill_time"] = f.time

        out = list(combined.values())
        log.debug("Fetched %d merged executions – flushing IB trade cache", len(out))

        # —— clear IB’s internal list to avoid leaks ——
        try:
            self.ib.clearTrades()           # ib-insync ≥0.9.71
        except AttributeError:
            self.ib.trades().clear()        # older fallback

        return out



    async def cancel_open_orders_for_symbol(self, symbol: str) -> list[int]:
        """
        Cancel all currently open IBKR orders for this manager's user & given symbol.
        Returns the list of permIds that were successfully cancelled.

        Avoids IB error 10147 by skipping orders that have orderId == 0 or are already terminal.
        """
        trades = list(self.ib.trades())
        to_cancel = [
            tr for tr in trades
            if tr.contract.symbol == symbol
            and tr.orderStatus.status not in {"Filled", "Cancelled", "Inactive"}
            and getattr(tr.order, "orderId", 0) != 0                    # ← guard
        ]
        cancelled = []

        for tr in to_cancel:
            pid = tr.order.permId
            try:
                self.ib.cancelOrder(tr.order)
                log.info("Canceled IBKR order permId=%s symbol=%s", pid, symbol)
                cancelled.append(pid)
            except Exception:
                # Swallow 10147 "OrderId 0 that needs to be cancelled is not found"
                log.exception("Failed to cancel IBKR order permId=%s symbol=%s", pid, symbol)

        return cancelled





## ───────────────────────── not in use ─────────────────────────

    async def execute_order(
        self,
        symbol: str,
        action: str,
        quantity: int,
        order_type: str = "MKT",
        limit_price: float | None = None,
        stop_price: float | None = None,
        trailing_percent: float | None = None,
        trailing_amount: float | None = None,
        limit_offset: float | None = None,
        runner_id: int | None = None, 
    ) -> dict | None:
        """
        Places a generic order.

        Supported order types:
        - MKT (Market)
        - LMT (Limit)
        - STP (Stop)
        - TRAIL LIMIT (Trailing Limit, with trailing amount or percent)

        All orders are placed as GTC and outside regular trading hours.

        Returns a dict with order info or None on failure.
        """
        try:
            contract = Stock(symbol, "SMART", "USD")
            await self.ib.qualifyContractsAsync(contract)

            order: Order

            if order_type == "MKT":
                order = MarketOrder(action, quantity)

            elif order_type == "LMT":
                if limit_price is None:
                    raise ValueError("limit_price must be provided for LMT orders")
                order = LimitOrder(action, quantity, limit_price)

            elif order_type == "STP":
                if stop_price is None:
                    raise ValueError("stop_price must be provided for STP orders")
                order = StopOrder(action, quantity, stop_price)

            elif order_type == "TRAIL LIMIT":
                if trailing_amount is None and trailing_percent is None:
                    raise ValueError("Trailing amount or percent must be provided for TRAIL LIMIT")
                if limit_offset is None:
                    raise ValueError("limit_offset must be provided for TRAIL LIMIT")

                order = Order(
                    action=action,
                    orderType="TRAIL LIMIT",
                    totalQuantity=quantity,
                    trailStopPrice=None,  # Let IB compute dynamically
                    trailingPercent=trailing_percent if trailing_percent is not None else None,
                    auxPrice=trailing_amount if trailing_percent is None else None,
                    lmtPriceOffset=limit_offset,
                    tif="GTC",
                    outsideRth=True,
                )

            else:
                raise ValueError(f"Unsupported order type: {order_type}")

            order.tif = "GTC"
            order.outsideRth = True

            trade = self.ib.placeOrder(contract, order)

            for _ in range(50):  # Wait up to ~5 seconds for permId
                if trade.order.permId:
                    break
                await asyncio.sleep(0.1)

            result = {
                "symbol": symbol,
                "action": action,
                "order_type": order.orderType,
                "quantity": quantity,
                "runner_id":     runner_id, 
                "limit_price": getattr(order, "lmtPrice", None),
                "stop_price": getattr(order, "auxPrice", None),
                "trailing_percent": getattr(order, "trailingPercent", None),
                "trailing_amount": trailing_amount,
                "limit_offset": getattr(order, "lmtPriceOffset", None),
                "status": trade.orderStatus.status,
                "perm_id": trade.order.permId,
                "filled_quantity": trade.orderStatus.filled,
                "avg_fill_price": trade.orderStatus.avgFillPrice,
            }

            log.info("Executed order: %s", result)
            return result

        except Exception:
            log.exception("Failed to execute order for %s", symbol)
            return None
        

 
    async def sell_position_outside_rth_safe_limit(self, symbol: str) -> Optional[dict]:
        """
        Sell the full open position for a stock using a limit order outside RTH.
        The limit price is set slightly below the current price to improve fill odds.
        """
        try:
            log.info("Initiating outsideRTH limit SELL for user=%s, symbol=%s", self.user.id, symbol)

            if not self.ib.isConnected():
                log.warning("IB not connected — attempting reconnect")
                await self.connect()

            # Get open position
            positions = self.ib.positions()
            position = next((p for p in positions if p.contract.symbol == symbol), None)
            if not position or position.position <= 0:
                log.info("No open position or zero quantity for %s", symbol)
                return None

            quantity = int(position.position)
            contract = Stock(symbol, "SMART", "USD")
            await self.ib.qualifyContractsAsync(contract)

            # Use current price as base for safe limit
            mdm = MarketDataManager()
            price = mdm.get_current_price(symbol)
            if not price:
                log.warning("Unable to fetch price for %s — aborting sell", symbol)
                return None

            # Limit price slightly below current market price to increase fill chance
            limit_price = round(price * 0.98, 2)

            order = LimitOrder("SELL", quantity, limit_price, tif="GTC", outsideRth=True)
            trade = self.ib.placeOrder(contract, order)

            for _ in range(50):
                if trade.order.permId:
                    break
                await asyncio.sleep(0.1)

            result = {
                "symbol": symbol,
                "action": "SELL",
                "order_type": "LMT",
                "quantity": quantity,
                "limit_price": limit_price,
                "status": trade.orderStatus.status,
                "perm_id": trade.order.permId,
                "filled_quantity": trade.orderStatus.filled,
                "avg_fill_price": trade.orderStatus.avgFillPrice,
                "account": trade.order.account or "",
            }

            log.info("✓ Submitted outsideRTH LIMIT SELL for %s: %s", symbol, result)
            return result

        except Exception:
            log.exception("✗ Failed to submit outsideRTH limit sell for %s", symbol)
            return None

    async def close_all_positions_outside_rth(self) -> Optional[list[dict]]:
        """
        Close all open positions using an extremely aggressive limit order outside RTH.
        Returns a list of order-result dicts, or None if there were no positions.
        Added extensive logging to trace each step.
        """
        if not self.ib.isConnected():
            log.info("IB not connected — attempting to reconnect before closing positions")
            try:
                await self.connect()
                log.info("Reconnected to IB successfully")
            except Exception as e:
                log.error("Failed to reconnect to IB: %s", e)
                return None

        # 1️⃣ Fetch current open positions
        positions = self.ib.positions()
        log.info("Fetched %d position(s) from IBKR", len(positions))

        if not positions:
            log.info("No positions to close")
            return None

        results: list[dict] = []

        for pos in positions:
            sym = pos.contract.symbol
            qty = pos.position

            # Skip zero-quantity positions (shouldn't normally happen)
            if qty == 0:
                log.debug("Skipping symbol=%s because quantity=0", sym)
                continue

            action = "SELL" if qty > 0 else "BUY"
            quantity = abs(int(qty))
            log.info("Processing position: symbol=%s, quantity=%d, action=%s", sym, quantity, action)

            # 2️⃣ Qualify contract
            contract = Stock(sym, "SMART", "USD")
            try:
                await self.ib.qualifyContractsAsync(contract)
                log.debug("Contract qualified for %s", sym)
            except Exception as e:
                log.error("Failed to qualify contract for %s: %s", sym, e)
                continue

            # 3️⃣ Determine an extremely aggressive limit price
            last_px = MarketDataManager().get_current_price(sym) or 1.0
            log.debug("Last price fetched for %s: %s", sym, last_px)

            if action == "SELL":
                limit_px = 0.01
                log.debug("Aggressive sell-limit for %s set to %s (will cross book)", sym, limit_px)
            else:
                limit_px = round(last_px * 100, 2)
                log.debug("Aggressive buy-limit for %s set to %s (will cross book)", sym, limit_px)

            # 4️⃣ Place the limit order outside regular trading hours
            try:
                order = LimitOrder(action, quantity, limit_px, tif="GTC", outsideRth=True)
                log.info(
                    "Placing order: symbol=%s, action=%s, quantity=%d, limit_price=%s, outsideRth=True",
                    sym, action, quantity, limit_px
                )
                trade = self.ib.placeOrder(contract, order)
                log.debug("Order placed for %s, temporary order status=%s, permId=%s",
                          sym,
                          trade.orderStatus.status,
                          trade.order.permId)
            except Exception as e:
                log.error("Exception while placing order for %s: %s", sym, e)
                continue

            # 5️⃣ Wait for a terminal status (Filled, Cancelled, or Inactive)
            final_status = None
            for attempt in range(50):  # Wait up to ~10 seconds
                status = trade.orderStatus.status
                if status in {"Filled", "Cancelled", "Inactive"}:
                    final_status = status
                    log.debug("Order for %s reached terminal status '%s' after %d checks",
                              sym, status, attempt + 1)
                    break

                if attempt % 10 == 0:
                    # Every ~2 seconds, log that we're still waiting
                    log.debug("Waiting for order to fill for %s; current status='%s' (attempt %d/50)",
                              sym, status, attempt + 1)

                await asyncio.sleep(0.2)

            # If we never saw a terminal status, record the last-known status
            if final_status is None:
                final_status = trade.orderStatus.status
                log.warning(
                    "Order for %s did not reach a terminal status within timeout. Last status='%s'",
                    sym, final_status
                )

            # 6️⃣ Record the result
            filled_qty = trade.orderStatus.filled or 0
            result = {
                "symbol":      sym,
                "action":      action,
                "quantity":    quantity,
                "limit_price": limit_px,
                "status":      final_status,
                "perm_id":     trade.order.permId,
                "filled_qty":  filled_qty,
            }
            log.info(
                "Result for %s: action=%s, requested_qty=%d, filled_qty=%d, status=%s, perm_id=%s",
                sym, action, quantity, filled_qty, final_status, trade.order.permId
            )
            results.append(result)

        log.info("Finished attempting to close %d position(s)", len(results))
        return results


    # ───────────────────── private utils ─────────────────────
    async def _wait_for_fill(self, trade: Trade, *, timeout: float) -> Tuple[bool, str]:
        """True when Filled inside `timeout` seconds, False otherwise."""
        deadline = asyncio.get_running_loop().time() + timeout
        last     = trade.orderStatus.status
        while asyncio.get_running_loop().time() < deadline:
            last = trade.orderStatus.status
            if last == "Filled":
                return True, last
            if last in {"Cancelled", "Inactive"}:
                return False, last
            await asyncio.sleep(0.15)
        return False, last

    def _timeout_debug(
        self, *, trade: Trade, limit_px: float | None,
        symbol: str, waited: float, session: str
    ) -> Dict[str, Any]:
        mkt        = MarketDataManager()
        best_bid   = mkt.get_current_price(symbol)
        remaining  = trade.orderStatus.remaining or 0
        filled     = trade.orderStatus.filled or 0
        return {
            "explanation": f"Order not filled after {waited:.1f}s "
                           f"({session}, status={trade.orderStatus.status})",
            "limit_price":   limit_px,
            "filled_qty":    filled,
            "remaining_qty": remaining,
            "best_bid":      best_bid,
        }