from __future__ import annotations
import json
import logging
from typing import Dict, Any

from database.db_manager import DBManager, canonical_cycle_seq
from database.models import Runner, User
from ib_manager.ib_connector import IBBusinessManager
import os
from backend.runner.types import ExecutionStatus
from backend.runner.records import record_exec

log = logging.getLogger("runner-executor")

async def post_sell_refresh(user: User, db: DBManager, ib: IBBusinessManager) -> None:
    from backend.sync_service import sync_positions, _sync_orders, _sync_executions
    await sync_positions(user, db, ib)
    await _sync_orders(user, db, ib)
    await _sync_executions(user, db, ib)

async def cancel_all_open_orders_for_stock(
    user: User, db: DBManager, ib: IBBusinessManager, symbol: str
) -> None:
    cancelled_perm_ids = await ib.cancel_open_orders_for_symbol(symbol)
    if cancelled_perm_ids:
        db.delete_orders_by_perm_ids(user_id=user.id, perm_ids=cancelled_perm_ids)

    still_open = [
        tr for tr in ib.ib.trades()
        if tr.contract.symbol == symbol
           and tr.orderStatus.status not in {"Filled", "Cancelled"}
    ]
    log.debug("post-cancel → %d open IBKR orders left for %s", len(still_open), symbol)

async def execute_decision(
    decision: Dict[str, Any] | None,
    *,
    strategy_name: str,
    runner: Runner,
    user:   User,
    db:     DBManager,
    ib:     IBBusinessManager,
    cycle_seq: str,
    is_exit: bool,
) -> None:
    if not decision or decision.get("action") in (None, "", "NO_ACTION"):
        status = (ExecutionStatus.NO_SELL_ACTION if is_exit else ExecutionStatus.NO_BUY_ACTION)
        record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=status, cycle_seq=cycle_seq,
            symbol=runner.stock,
            reason=(decision or {}).get("reason", "no_signal"),
            strategy=strategy_name,
            details=decision or {},
        )
        return

    action = str(decision["action"]).upper()

    if action == "BUY" and not decision.get("trail_stop_order") and not decision.get("static_stop_order"):
        if os.getenv("ANALYTICS_MODE", "false").lower() != "true":
            record_exec(
                db, user_id=user.id, runner_id=runner.id,
                status=ExecutionStatus.SKIPPED_BUILD_FAILED,
                cycle_seq=cycle_seq, symbol=runner.stock,
                reason="missing_stop_loss", strategy=strategy_name,
            )
            return

    record_exec(
        db, user_id=user.id, runner_id=runner.id,
        status=ExecutionStatus.DECISION_MADE,
        cycle_seq=cycle_seq, symbol=runner.stock,
        reason=decision.get("reason", action),
        strategy=strategy_name, details=decision,
    )

    try:
        if action == "SELL":
            await cancel_all_open_orders_for_stock(user, db, ib, runner.stock)

        order = await ib.place_order_from_decision(
            decision   = decision,
            user_id    = user.id,
            runner_id  = runner.id,
            symbol     = runner.stock,
            wait_fill  = True
        )

    except TimeoutError as e:
        try:
            details = json.loads(str(e))
        except Exception:
            details = str(e)

        record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.ORDER_NOT_FILLED,
            cycle_seq=cycle_seq, symbol=runner.stock,
            reason="not_filled", strategy=strategy_name,
            details=details,
        )
        return

    if not order:
        record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.ORDER_PLACE_FAILED,
            cycle_seq=cycle_seq, symbol=runner.stock,
            reason="ib_error", strategy=strategy_name,
        )
        return

    db.save_order(order)
    db._update_runner_current_budget(runner_id=runner.id)

    perm_id = order.get("perm_id") or order.get("ibkr_perm_id")
    order_cycle = canonical_cycle_seq(perm_id) if perm_id else cycle_seq

    record_exec(
        db, user_id=user.id, runner_id=runner.id,
        status=ExecutionStatus.ORDER_PLACED,
        cycle_seq=order_cycle, symbol=runner.stock,
        perm_id=perm_id,
        limit_price=order.get("limit_price"),
        ts=order.get("submitted_time"),
        details=order, strategy=strategy_name,
    )

    if action == "SELL":
        await post_sell_refresh(user, db, ib)
