from __future__ import annotations
import asyncio
import os
import uuid
import logging
from typing import Any, Dict, Sequence

from backend.ib_manager.market_data_manager import MarketDataManager
from database.db_manager import DBManager
import os
from ib_manager.ib_connector import IBBusinessManager
from backend.analytics.mock_broker import MockBusinessManager
from database.models import Runner, User
from utils import now_et, to_et

# Layered runner modules
from backend.runner.types import ExecutionStatus
from backend.runner.records import record_exec as _record_exec
from backend.runner.decision_builder import build_decision_info
from backend.runner.executor import execute_decision
from backend.runner.guards import global_sl_tp_decision
from backend.runner.time_exit import handle_time_exit_if_needed
from strategies.factory import select_strategy
from strategies.contracts import validate_decision, StrategyDecisionError

MKT = MarketDataManager()
log = logging.getLogger("runner-service")

_MAX_PARALLEL = int(os.getenv("RUNNER_PARALLELISM", "8"))
_SEM          = asyncio.Semaphore(_MAX_PARALLEL)

COMMISSION_BUFFER_PCT       = float(os.getenv("COMMISSION_BUFFER_PCT", "0.001"))
GLOBAL_SELL_LIMIT_WIGGLE    = float(os.getenv("GLOBAL_SELL_LIMIT_WIGGLE", "0.0005"))

# ─────────────────────── public API ───────────────────────
async def run_due_runners(user: User, db_unused, ib: IBBusinessManager) -> None:
    # Use provided db connection instead of creating new ones
    with DBManager() as shared_db:
        actives = shared_db.get_active_runners(user_id=user.id)

        if not actives:
            log.info("skip user=%s(id=%d) reason=no-active-runners", user.username, user.id)
            return

        log.info("tick user=%s actives=%d", user.username, len(actives))

        async def _wrap(r: Runner):
            async with _SEM:
                # Create separate DB connection per runner for thread safety
                with DBManager() as db:
                    # Always use mock broker for analytics simulation
                    broker = MockBusinessManager(user)
                    await _run_runner(r, user, db, broker)

        await asyncio.gather(*[_wrap(r) for r in actives])


# ───────────────────── main per-runner loop ─────────────────────
async def _run_runner(
    runner: Runner,
    user:   User,
    db:     DBManager,
    ib:     IBBusinessManager,
) -> None:
    cycle_seq = uuid.uuid4().hex
    log.debug("▶ runner-start %s(%d) user=%s", runner.name, runner.id, user.username)

    try:
        handled = await handle_time_exit_if_needed(
            runner=runner, user=user, db=db, ib=ib, cycle_seq=cycle_seq
        )
        if handled:
            return

        if not _is_runner_in_time(runner, user, db):
            _record_exec(
                db, user_id=user.id, runner_id=runner.id,
                status=ExecutionStatus.SKIPPED_NOT_IN_TIME,
                cycle_seq=cycle_seq, symbol=runner.stock,
                reason="not_in_time"
            )
            return

        # In analytics we do not check real broker open orders

        info, remaining_budget, err = await build_decision_info(runner, user, db, ib)
        if err:
            _record_exec(
                db, user_id=user.id, runner_id=runner.id,
                status=err.get("skip_status", ExecutionStatus.SKIPPED_BUILD_FAILED),
                cycle_seq=cycle_seq, symbol=runner.stock,
                reason=err.get("reason")
            )
            return

        decision = None
        if info.position:
            decision = global_sl_tp_decision(
                info,
                commission_buffer_pct=COMMISSION_BUFFER_PCT,
                limit_wiggle_pct=GLOBAL_SELL_LIMIT_WIGGLE,
            )

        if decision is None:
            strategy = select_strategy(runner)
            decision = (
                strategy.decide_sell(info) if info.position
                else strategy.decide_buy(info)
                if remaining_budget >= info.current_price
                else {"action": "NO_ACTION", "reason": "no_funds"}
            )
        else:
            strategy = select_strategy(runner)

        try:
            validated = validate_decision(decision, is_exit=bool(info.position))
        except StrategyDecisionError as ve:
            _record_exec(
                db, user_id=user.id, runner_id=runner.id,
                status=ExecutionStatus.SKIPPED_BUILD_FAILED,
                cycle_seq=cycle_seq, symbol=runner.stock,
                reason=f"strategy_decision_invalid: {ve}",
                strategy=getattr(strategy, "name", strategy.__class__.__name__),
                details=decision or {},
            )
            return

        await execute_decision(
            validated,
            strategy_name=getattr(strategy, "name", strategy.__class__.__name__),
            runner=runner, user=user, db=db, ib=ib,
            cycle_seq=cycle_seq, is_exit=bool(info.position)
        )

    except Exception as e:
        import traceback
        _record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.ERROR, cycle_seq=cycle_seq,
            symbol=runner.stock,
            reason=type(e).__name__,
            details={"error": str(e), "traceback": traceback.format_exc(limit=5)},
        )
        log.exception("runner failure %s(%d)", runner.name, runner.id)

    finally:
        exec_row = db.get_last_runner_execution(
            user_id=user.id, runner_id=runner.id, cycle_seq=cycle_seq
        )
        if not exec_row:
            log.info("◀ runner-end %s no executions recorded", runner.name)
        else:
            log.info(
                "◀ runner-end %s status=%s reason=%s strategy=%s details=%s",
                runner.name,
                exec_row.status,
                exec_row.reason   or "n/a",
                exec_row.strategy or "n/a",
                exec_row.details  or "{}",
            )

# ----------------------------------------------------------------
def _is_runner_in_time(runner: Runner, user: User, db: DBManager) -> bool:
    if runner.time_range_from and now_et() < to_et(runner.time_range_from):
        log.debug("skip runner=%s(%d) reason=not-started starts=%s",
                  runner.name, runner.id,
                  to_et(runner.time_range_from).strftime("%Y-%m-%d %H:%M:%S"))
        return False

    if "expired date" in runner.exit_strategy.lower():
        if runner.time_range_to:
            expiry = to_et(runner.time_range_to)
            if now_et() > expiry:
                log.info("skip runner=%s(%d) reason=expired expires=%s",
                         runner.name, runner.id, expiry.strftime("%Y-%m-%d %H:%M:%S"))
                db.update_runners_activation(user_id=user.id, ids=[runner.id], activation="inactive")
                return False
            log.debug("runner=%s(%d) expiry-ok until=%s",
                      runner.name, runner.id, expiry.strftime("%Y-%m-%d %H:%M:%S"))
    return True

# ─────────────────── public helpers kept intact ───────────────────
async def deactivate_runner(
    runner: Runner,
    user:   User,
    db:     DBManager,
    ib:     IBBusinessManager,
) -> None:
    from backend.sync_service import _sync_orders, _sync_executions, sync_positions
    from backend.runner.executor import cancel_all_open_orders_for_stock

    symbol    = runner.stock
    cycle_seq = uuid.uuid4().hex
    log.info("Deactivating runner=%s(%d) user=%s(%d)",
             runner.name, runner.id, user.username, user.id)

    if runner.activation == "closing":
        log.info("Runner already closing – skipping duplicate call")
        return
    db.update_runners_activation(user_id=user.id, ids=[runner.id], activation="closing")

    try:
        flat_res = await ib.flat_position(symbol, user_id=user.id, runner_id=runner.id)
    except TimeoutError as e:
        db.update_runners_activation(user_id=user.id, ids=[runner.id], activation="active")
        _record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.ORDER_NOT_FILLED,
            cycle_seq=cycle_seq, symbol=symbol,
            reason="not_filled", details=str(e),
        )
        log.error("Flat order timed-out – runner kept ACTIVE")
        return

    if flat_res:
        _record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.DEACTIVATE_FLAT,
            cycle_seq=cycle_seq, symbol=symbol,
            perm_id=flat_res.get("perm_id"),
            limit_price=flat_res.get("limit_price"),
            details=flat_res,
        )
        db.save_order(flat_res)
    else:
        _record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.DEACTIVATE_NO_POSITION,
            cycle_seq=cycle_seq, symbol=symbol,
        )

    await cancel_all_open_orders_for_stock(user, db, ib, symbol)
    await sync_positions(user, db, ib)
    await _sync_orders(user, db, ib)
    await _sync_executions(user, db, ib)
    db.update_runners_activation(user_id=user.id, ids=[runner.id], activation="inactive")
    log.info("Runner %s(%d) marked inactive", runner.name, runner.id)

async def deactivate_runners(
    *,
    user: User,
    db: DBManager,
    business_manager: IBBusinessManager,
    ids: Sequence[int]
) -> Dict[str, Any]:
    runners = db.get_runners_by_ids(user_id=user.id, ids=list(ids))
    if not runners:
        log.warning("No runners found for deactivation: ids=%s", ids)
        return {"succeeded": [], "failed": []}

    results = {"succeeded": [], "failed": []}
    for r in runners:
        try:
            await deactivate_runner(r, user, db, business_manager)
            results["succeeded"].append(r.id)
        except Exception as e:
            log.exception("Error deactivating runner=%s(%d)", r.name, r.id)
            results["failed"].append({"id": r.id, "error": str(e)})
    return results

# ───────── Test helper preserved (import path unchanged) ─────────
async def force_close_all_positions_and_orders(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    from backend.sync_service import sync_positions, _sync_orders
    log.warning("⚠ Force-closing all positions and orders for user=%s (%d)", user.username, user.id)

    trades = list(ib.ib.trades())
    open_orders = [tr for tr in trades if tr.orderStatus.status not in {"Filled", "Cancelled"}]
    log.info("🔍 Found %d open orders to cancel", len(open_orders))

    cancelled_ids = []
    for tr in open_orders:
        try:
            ib.ib.cancelOrder(tr.order)
            cancelled_ids.append(tr.order.permId)
            log.info("✅ Cancelled order permId=%s symbol=%s", tr.order.permId, tr.contract.symbol)
        except Exception:
            log.exception("❌ Failed to cancel order permId=%s", tr.order.permId)

    if cancelled_ids:
        removed = db.delete_orders_by_perm_ids(user_id=user.id, perm_ids=cancelled_ids)
        log.info("🗑 Removed %d canceled orders from DB", removed)

    log.debug("🔄 Remaining open orders after cancel: %s", ib.ib.openOrders())

    positions = ib.ib.positions()
    log.info("📦 Found %d open positions", len(positions))

    for pos in positions:
        symbol = pos.contract.symbol
        try:
            await ib.flat_position(symbol, user_id=user.id)
        except Exception:
            log.exception("Failed to flatten position for %s", symbol)

    await sync_positions(user, db, ib)
    await _sync_orders(user, db, ib)

    log.warning("🏁 Force-close finished for user=%s — all orders cancelled, all positions attempted to flatten", user.username)

__all__ = [
    "run_due_runners",
    "deactivate_runner",
    "deactivate_runners",
    "force_close_all_positions_and_orders",
    "ExecutionStatus",
    "_record_exec",
]
