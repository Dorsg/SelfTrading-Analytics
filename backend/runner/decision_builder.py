from __future__ import annotations
import logging
from types import SimpleNamespace
from sqlalchemy import func

from strategies.runner_decision_info import RunnerDecisionInfo
from backend.ib_manager.market_data_manager import MarketDataManager
from database.db_manager import DBManager
from database.models import ExecutedTrade, OpenPosition, Runner, User
from ib_manager.ib_connector import IBBusinessManager
from backend.utils import now_et, to_et

log = logging.getLogger("runner-decision-builder")
MKT = MarketDataManager()

async def build_decision_info(
    runner: Runner,
    user:   User,
    db:     DBManager,
    ib:     IBBusinessManager,
) -> tuple[RunnerDecisionInfo | None, float, dict | None]:
    symbol = runner.stock.upper()

    price = MKT.get_current_price(symbol)
    if price is None:
        return None, 0.0, {"reason": "no_price"}

    remaining_budget, position = calculate_budget(symbol, runner, user, db, ib)
    if remaining_budget <= 0 and not position:
        return None, 0.0, {
            "skip_status": "skipped_no_funds",
            "reason": "no_funds",
        }

    tf      = runner.time_frame or "1day"
    candles = await fetch_candles(symbol, tf) or []

    info = RunnerDecisionInfo(
        runner=runner,
        position=position,
        current_price=price,
        candles=candles,
        distance_from_time_limit=calculate_time_limit_distance(runner),
    )
    return info, remaining_budget, None

def calculate_budget(
    symbol: str,
    runner: Runner,
    user:   User,
    db:     DBManager,
    ib:     IBBusinessManager,
) -> tuple[float, OpenPosition | None]:
    # Use our DB snapshot; ignore real broker in analytics
    db_pos = db.get_open_position_for_stock(user_id=user.id, symbol=symbol)
    if db_pos:
        invested_cost = db_pos.avg_price * abs(db_pos.quantity)
        position = SimpleNamespace(quantity=abs(db_pos.quantity), avg_price=db_pos.avg_price, symbol=symbol)
    else:
        invested_cost = 0.0
        position = None

    live_px = MKT.get_current_price(symbol) or 0.0
    reserved_cash = 0.0
    for o in db.get_open_buy_orders_for_stock(user_id=user.id, symbol=symbol):
        px = o.limit_price if o.limit_price not in (None, 0) else live_px
        reserved_cash += px * o.quantity

    realised_pnl = (
        db.db.query(func.coalesce(func.sum(ExecutedTrade.pnl_amount), 0.0))
        .filter(
            ExecutedTrade.runner_id == runner.id,
            ExecutedTrade.pnl_amount.isnot(None),
        )
        .scalar()
        or 0.0
    )

    remaining_cash = runner.budget - invested_cost - reserved_cash + realised_pnl
    log.debug(
        "budget/live runner=%s invested=%.2f reserved=%.2f realised=%.2f → remaining=%.2f",
        runner.name, invested_cost, reserved_cash, realised_pnl, remaining_cash,
    )
    return remaining_cash, position

async def fetch_candles(symbol: str, time_frame: str, bars: int = 250) -> list[dict] | None:
    first_try = await MKT._get_candles(symbol, time_frame, bars)
    if first_try:
        return first_try

    log.warning("No candles for %s at requested time_frame='%s' (bars=%d)", symbol, time_frame, bars)
    short_30d = await MKT._get_candles(symbol, "1day", 30)
    if short_30d:
        return short_30d

    log.warning("No daily candles (30-day fallback) for %s", symbol)
    hourly = await MKT._get_candles(symbol, "60min", bars)
    if hourly:
        return hourly

    log.warning("No hourly candles (60min) for %s (bars=%d)", symbol, bars)
    fifteen = await MKT._get_candles(symbol, "15min", bars)
    if fifteen:
        return fifteen

    log.warning("All candle-fetch fallbacks failed for %s", symbol)
    return None

def calculate_time_limit_distance(runner: Runner) -> float | None:
    if "expired date" not in runner.exit_strategy.lower():
        return None
    if not runner.time_range_to:
        log.warning("time_range_to is missing for runner=%s(%d) with 'expired date' strategy",
                    runner.name, runner.id)
        return None

    now = now_et()
    end = to_et(runner.time_range_to)
    delta = max((end - now).total_seconds(), 0.0)
    log.debug("time-limit: runner=%s(%d) ends-in=%.0f seconds", runner.name, runner.id, delta)
    return delta
