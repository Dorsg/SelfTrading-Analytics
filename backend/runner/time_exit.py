from __future__ import annotations
from database.db_manager import DBManager
from database.models import Runner, User
from ib_manager.ib_connector import IBBusinessManager
from backend.analytics.mock_broker import MockBusinessManager
from backend.runner.types import ExecutionStatus
from backend.runner.records import record_exec
from backend.runner.executor import post_sell_refresh, cancel_all_open_orders_for_stock
from backend.utils import now_et, to_et

async def handle_time_exit_if_needed(
    *,
    runner: Runner,
    user:   User,
    db:     DBManager,
    ib:     IBBusinessManager,
    cycle_seq: str,
) -> bool:
    if "expired date" not in (runner.exit_strategy or "").lower():
        return False
    if not runner.time_range_to:
        return False
    if now_et() <= to_et(runner.time_range_to):
        return False

    symbol = runner.stock.upper()
    # Analytics: use DB or mock broker positions
    ib_is_mock = isinstance(ib, MockBusinessManager)
    db_pos = db.get_open_position_for_stock(user_id=user.id, symbol=symbol)
    has_pos = bool(db_pos and db_pos.quantity)

    if has_pos:
        try:
            flat_res = await ib.flat_position(symbol, user_id=user.id, runner_id=runner.id)
            if flat_res:
                db.save_order(flat_res)
                record_exec(
                    db, user_id=user.id, runner_id=runner.id,
                    status=ExecutionStatus.TIME_EXIT_SELL,
                    cycle_seq=cycle_seq, symbol=symbol,
                    perm_id=flat_res.get("perm_id"),
                    limit_price=flat_res.get("limit_price"),
                    details=flat_res,
                    reason="expired_date",
                )
                await post_sell_refresh(user, db, ib)
        except TimeoutError as e:
            record_exec(
                db, user_id=user.id, runner_id=runner.id,
                status=ExecutionStatus.ORDER_NOT_FILLED,
                cycle_seq=cycle_seq, symbol=symbol,
                reason="expired_date_not_filled",
                details=str(e),
            )
            return True
    else:
        record_exec(
            db, user_id=user.id, runner_id=runner.id,
            status=ExecutionStatus.TIME_EXIT_NO_POSITION,
            cycle_seq=cycle_seq, symbol=symbol,
            reason="expired_date",
        )

    await cancel_all_open_orders_for_stock(user, db, ib, symbol)
    await post_sell_refresh(user, db, ib)
    db.update_runners_activation(user_id=user.id, ids=[runner.id], activation="inactive")
    return True
