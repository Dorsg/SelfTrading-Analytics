from __future__ import annotations
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Sequence
from sqlalchemy import text

from database.db_manager import DBManager, get_users_with_ib_safe
from database.models import User, SimulationState
from backend.runner_service import run_due_runners
from backend.analytics.mock_broker import MockBusinessManager
from backend.analytics.pnl_aggregator import compute_final_pnl_for_runner
from backend.analytics.result_writer import upsert_result

log = logging.getLogger("AnalyticsScheduler")


def _set_sim_time(ts: int) -> None:
    os.environ["SIM_TIME_EPOCH"] = str(ts)


def _get_earliest_timestamp() -> int | None:
    from sqlalchemy import select, func
    from database.db_core import engine
    from database.models import HistoricalMinuteBar, HistoricalDailyBar
    with engine.connect() as conn:
        t1 = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
        t2 = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
        candidates = [int(t.timestamp()) for t in [t1, t2] if isinstance(t, datetime)]
        return min(candidates) if candidates else None


async def _tick_users(ts_epoch: int) -> None:
    _set_sim_time(ts_epoch)
    users: Sequence[User] = await asyncio.to_thread(get_users_with_ib_safe)
    # If none, create a synthetic in-memory user for analytics runs
    if not users:
        with DBManager() as db:
            # Try to find any user, else create one
            u = db.get_user_by_username("analytics")
            if u is None:
                try:
                    u = db.create_user(username="analytics", email="analytics@example.com", password="analytics")
                except Exception:
                    u = db.get_user_by_username("analytics")
            users = [u] if u else []

    if not users:
        log.warning("No users available; skipping tick")
        return

    async def _run_for_user(u: User):
        bm = MockBusinessManager(u)
        # DB connection is created inside run_due_runners
        await run_due_runners(u, None, bm)
        # After runners tick, aggregate and write results per runner
        with DBManager() as db:
            runners = db.get_runners_by_user(user_id=u.id)
            for r in runners:
                amt, pct, trades, avg_pnl, avg_dur = compute_final_pnl_for_runner(runner_id=r.id)
                tf = str(r.time_frame or "").lower()
                tf = "1d" if tf in {"d", "1day", "1440"} else ("5m" if tf in {"5", "5min"} else str(tf))
                upsert_result(
                    symbol=r.stock,
                    strategy=str(r.strategy),
                    timeframe=tf,
                    start_ts=r.time_range_from,
                    end_ts=datetime.fromtimestamp(ts_epoch, tz=timezone.utc),
                    final_pnl_amount=amt,
                    final_pnl_percent=pct,
                    trades_count=trades,
                    max_drawdown=None,
                    avg_pnl_per_trade=avg_pnl,
                    avg_trade_duration_sec=avg_dur,
                )

    await asyncio.gather(*(_run_for_user(u) for u in users))


async def main() -> None:
    # Set up logging with file handler for sim_scheduler
    import logging.config
    import sys
    import os
    
    # Ensure logs directory exists
    os.makedirs('/app/logs', exist_ok=True)
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)-8s %(name)s: %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/sim_scheduler.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    log.info("Starting Analytics Simulation Scheduler")
    
    try:
        # Test database connection early
        with DBManager() as db:
            # Test basic query
            db.db.execute(text("SELECT 1")).scalar()
            log.info("Database connection successful")
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        raise

    pace_seconds = int(os.getenv("SIM_PACE_SECONDS", "0"))  # 0 = as fast as possible
    step_seconds = int(os.getenv("SIM_STEP_SECONDS", "300"))  # 5 minutes default
    
    log.info(f"Configuration: pace_seconds={pace_seconds}, step_seconds={step_seconds}")

    start_ts = os.getenv("SIM_START_EPOCH")
    if start_ts:
        ts = int(start_ts)
    else:
        earliest = _get_earliest_timestamp()
        if earliest is None:
            log.error("No historical data found. Exiting.")
            return
        ts = earliest

    end_ts_env = os.getenv("SIM_END_EPOCH")
    end_ts = int(end_ts_env) if end_ts_env else None

    # Bootstrap default user and runners if none exist
    with DBManager() as db:
        u = db.get_user_by_username("analytics")
        if u is None:
            try:
                u = db.create_user(username="analytics", email="analytics@example.com", password="analytics")
            except Exception:
                u = db.get_user_by_username("analytics")
        if u:
            existing = db.get_runners_by_user(user_id=u.id)
            if not existing:
                _bootstrap_runners(db, u)

    log.info("Analytics simulation initialized at %s", datetime.fromtimestamp(ts, tz=timezone.utc))
    
    # Initialize simulation state as stopped - user will start manually from UI
    with DBManager() as db:
        u = db.get_user_by_username("analytics")
        if u:
            st = db.db.query(SimulationState).filter(SimulationState.user_id == u.id).first()
            if not st:
                st = SimulationState(user_id=u.id, is_running="false")
                db.db.add(st)
            else:
                st.is_running = "false"
            st.last_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
            db.db.commit()

    tick_count = 0
    last_log_time = 0
    
    while True:
        # Respect persisted start/stop state
        with DBManager() as db:
            u = db.get_user_by_username("analytics")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == (u.id if u else -1)).first() if u else None
            if not st or st.is_running != "true":
                await asyncio.sleep(1.0)
                continue
        
        current_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        
        if end_ts and ts > end_ts:
            log.info("Reached end of simulation at %s", current_dt)
            break
            
        try:
            # Log every tick for now to see progress
            log.info("Processing tick #%d at %s (epoch: %d)", tick_count, current_dt, ts)
            await _tick_users(ts)
            tick_count += 1
            
            # Log progress more frequently for visibility
            if tick_count % 10 == 0 or (ts - last_log_time) >= 3600:  # Every 10 ticks or every hour
                log.info("Simulation progress: Tick #%d, Time: %s, Epoch: %d", tick_count, current_dt, ts)
                last_log_time = ts
                
        except Exception:
            log.exception("Tick failed at %s", current_dt)

        ts += step_seconds
        
        # Persist last ts every 10 ticks to reduce DB load
        if tick_count % 10 == 0:
            with DBManager() as db:
                u = db.get_user_by_username("analytics")
                if u:
                    st = db.db.query(SimulationState).filter(SimulationState.user_id == u.id).first()
                    if not st:
                        st = SimulationState(user_id=u.id, is_running="true")
                        db.db.add(st)
                    st.last_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                    db.db.commit()
                    log.debug("Persisted simulation time: %s", st.last_ts)
                
        if pace_seconds:
            await asyncio.sleep(pace_seconds)


def _bootstrap_runners(db: DBManager, user: User) -> None:
    from sqlalchemy import select
    from database.db_core import engine
    from database.models import HistoricalDailyBar

    # Strategies to include (exclude Fibonacci)
    strategies = [
        "test",
        "triple_top_break",
        "below_above",
        "chatgpt_5_strategy",
        "grok_4_strategy",
    ]
    timeframes = [(5, "5m"), (1440, "1d")]
    default_budget = float(os.getenv("SIM_DEFAULT_BUDGET", "10000"))
    symbol_limit = int(os.getenv("SIM_SYMBOL_LIMIT", "0"))  # 0 = all

    try:
        with engine.connect() as conn:
            syms = [r[0] for r in conn.execute(select(HistoricalDailyBar.symbol).distinct().order_by(HistoricalDailyBar.symbol.asc())).all()]
        log.info(f"Found {len(syms)} symbols for bootstrapping runners")
        
        if symbol_limit > 0:
            syms = syms[:symbol_limit]
            log.info(f"Limited to {len(syms)} symbols due to SIM_SYMBOL_LIMIT")

        created = 0
        errors = 0
        
        for sym in syms:
            for strat in strategies:
                for tf_val, tf_name in timeframes:
                    name = f"{sym}-{strat}-{tf_name}"
                    try:
                        db.create_runner(
                            user_id=user.id,
                            data={
                                "name": name,
                                "strategy": strat,
                                "budget": default_budget,
                                "stock": sym,
                                "time_frame": tf_val,
                                "time_range_from": None,
                                "time_range_to": None,
                                "exit_strategy": "hold_forever",
                                "parameters": {},
                            },
                        )
                        created += 1
                    except Exception as e:
                        errors += 1
                        log.debug(f"Failed to create runner {name}: {e}")
                        # Skip duplicates and continue
                        continue
        
        if created:
            log.info("Bootstrapped %d runners for analytics user (errors: %d)", created, errors)
        else:
            log.warning("No runners were created during bootstrap (errors: %d)", errors)
            
    except Exception as e:
        log.error(f"Failed to bootstrap runners: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())


