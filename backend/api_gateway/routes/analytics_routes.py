from __future__ import annotations
from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime, timezone
from sqlalchemy import select, func, desc

from database.db_core import engine
from database.db_manager import DBManager
from database.models import RunnerExecution, AnalyticsResult, HistoricalDailyBar, HistoricalMinuteBar, SimulationState

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _now_sim() -> Optional[int]:
    import os
    v = os.getenv("SIM_TIME_EPOCH")
    try:
        return int(v) if v else None
    except Exception:
        return None


@router.get("/progress")
def get_progress() -> dict:
    """
    Overall simulation progress based on SIM_TIME_EPOCH vs earliest/latest bars.
    Returns ticks processed and total ticks for both 5m and 1d.
    """
    with engine.connect() as conn:
        start_min = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
        end_min   = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
        start_day = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
        end_day   = conn.execute(select(func.max(HistoricalDailyBar.date))).scalar()

    sim_ts = _now_sim() or 0
    sim_dt = datetime.fromtimestamp(sim_ts, tz=timezone.utc) if sim_ts else None

    def _ticks(start: Optional[datetime], end: Optional[datetime], step_seconds: int) -> tuple[int, int, float]:
        if not (start and end):
            return (0, 0, 0.0)
        total = max(0, int((end - start).total_seconds() // step_seconds))
        cur   = 0 if not sim_dt else max(0, min(total, int((sim_dt - start).total_seconds() // step_seconds)))
        pct   = (cur / total * 100.0) if total > 0 else 0.0
        return (cur, total, round(pct, 2))

    cur5, tot5, pct5 = _ticks(start_min, end_min, 300)
    cur1d, tot1d, pct1d = _ticks(start_day, end_day, 86400)

    return {
        "sim_time_epoch": sim_ts,
        "sim_time_iso": sim_dt.isoformat() if sim_dt else None,
        "timeframes": {
            "5m":  {"ticks_done": cur5,  "ticks_total": tot5,  "percent": pct5},
            "1d":  {"ticks_done": cur1d, "ticks_total": tot1d, "percent": pct1d},
        },
    }


@router.get("/errors")
def list_errors(limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    """Return recent error/skip entries from runner executions."""
    with DBManager() as db:
        q = (
            db.db.query(RunnerExecution)
            .filter(RunnerExecution.status.in_(["error", "skipped_build_failed"]))
            .order_by(RunnerExecution.execution_time.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "time": r.execution_time,
                "runner_id": r.runner_id,
                "symbol": r.symbol,
                "status": r.status,
                "reason": r.reason,
                "details": r.details,
                "strategy": r.strategy,
            }
            for r in q
        ]


@router.get("/results")
def list_results(
    limit: int = Query(100, ge=1, le=1000),
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[dict]:
    with DBManager() as db:
        q = db.db.query(AnalyticsResult)
        if strategy:
            q = q.filter(AnalyticsResult.strategy == strategy)
        if symbol:
            q = q.filter(AnalyticsResult.symbol == symbol.upper())
        if timeframe:
            q = q.filter(AnalyticsResult.timeframe == timeframe)
        rows = (
            q.order_by(desc(AnalyticsResult.final_pnl_amount))
            .limit(limit)
            .all()
        )
        return [
            {
                "symbol": r.symbol,
                "strategy": r.strategy,
                "timeframe": r.timeframe,
                "start_ts": r.start_ts,
                "end_ts": r.end_ts,
                "final_pnl_amount": r.final_pnl_amount,
                "final_pnl_percent": r.final_pnl_percent,
                "trades_count": r.trades_count,
            }
            for r in rows
        ]


@router.get("/summary")
def get_summary() -> dict:
    with engine.connect() as conn:
        syms = conn.execute(select(func.count(func.distinct(HistoricalDailyBar.symbol)))).scalar() or 0
    with DBManager() as db:
        strategies = db.db.execute(select(func.count(func.distinct(AnalyticsResult.strategy)))).scalar() or 0
        timeframes = db.db.execute(select(func.count(func.distinct(AnalyticsResult.timeframe)))).scalar() or 0
        results    = db.db.execute(select(func.count(AnalyticsResult.id))).scalar() or 0
        errors     = db.db.execute(
            select(func.count(RunnerExecution.id)).where(RunnerExecution.status.in_(["error", "skipped_build_failed"]))
        ).scalar() or 0
    return {
        "symbols": int(syms),
        "strategies": int(strategies),
        "timeframes": int(timeframes),
        "results": int(results),
        "errors": int(errors),
    }


@router.post("/simulation/start")
def start_simulation() -> dict:
    """Mark simulation as running and persist last_ts from env if set."""
    import os
    sim_ts = os.getenv("SIM_TIME_EPOCH")
    val = datetime.fromtimestamp(int(sim_ts), tz=timezone.utc) if sim_ts else None
    with DBManager() as db:
        # single-user analytics – pick first user; else create
        users = db.get_users_with_ib()  # returns [] in analytics; use any user
        user = db.get_user_by_username("analytics") or (db.create_user(username="analytics", email="analytics@example.com", password="analytics"))
        st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
        if not st:
            st = SimulationState(user_id=user.id, is_running="true", last_ts=val)
            db.db.add(st)
        else:
            st.is_running = "true"
            if val:
                st.last_ts = val
        db.db.commit()
        return {"running": True, "last_ts": st.last_ts}


@router.post("/simulation/stop")
def stop_simulation() -> dict:
    with DBManager() as db:
        user = db.get_user_by_username("analytics")
        if not user:
            return {"running": False}
        st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
        if not st:
            st = SimulationState(user_id=user.id, is_running="false")
            db.db.add(st)
        else:
            st.is_running = "false"
        db.db.commit()
        return {"running": False}


@router.get("/simulation/state")
def get_simulation_state() -> dict:
    import os
    with DBManager() as db:
        user = db.get_user_by_username("analytics")
        st = None
        if user:
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
        sim_env = os.getenv("SIM_TIME_EPOCH")
        return {
            "running": (st.is_running == "true") if st else False,
            "last_ts": st.last_ts if st else None,
            "env_ts": int(sim_env) if sim_env else None,
        }


