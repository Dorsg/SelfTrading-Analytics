# FILE: backend/api_gateway/routes/analytics_routes.py
# DESCRIPTION: Minimal analytics API for server-side simulation

from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func, text
import os
import logging

from database.db_core import engine
from database.db_manager import DBManager
from database.models import (
    RunnerExecution,
    AnalyticsResult,
    HistoricalDailyBar,
    HistoricalMinuteBar,
    SimulationState,
    ExecutedTrade,
    Runner
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _now_sim() -> Optional[int]:
    try:
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None
            if st and st.last_ts:
                return int(st.last_ts.timestamp())
    except Exception:
        pass
    try:
        v = os.getenv("SIM_TIME_EPOCH")
        return int(v) if v else None
    except Exception:
        return None


@router.get("/database/status")
def get_database_status() -> dict:
    with engine.connect() as conn:
        daily = conn.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0
        minute = conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0
        start = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
        end = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
    with DBManager() as db:
        users = db.count_users()
        runners = db.count_runners()
    return {
        "data": {"daily_bars": int(daily), "minute_bars": int(minute),
                 "date_range": {"start": start.isoformat() if start else None,
                                "end": end.isoformat() if end else None}},
        "setup": {"users": users, "runners": runners},
        "ready": (daily > 0 and minute > 0 and users > 0 and runners > 0)
    }


@router.post("/simulation/start")
def start_simulation() -> dict:
    logger = logging.getLogger("api-gateway")
    try:
        # Discover 5m boundaries
        with engine.connect() as conn:
            min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
            max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()

        if not min_ts or not max_ts:
            raise HTTPException(status_code=400, detail="No historical minute data found")

        # Warmup-aware desired start (forward-only)
        warmup_bars = int(os.getenv("SIM_WARMUP_BARS", os.getenv("WARMUP_BARS", "30")))
        step_sec = int(os.getenv("SIM_STEP_SECONDS", "300"))  # default 5m

        min_epoch = int((min_ts if min_ts.tzinfo else min_ts.replace(tzinfo=timezone.utc)).timestamp())
        max_epoch = int((max_ts if max_ts.tzinfo else max_ts.replace(tzinfo=timezone.utc)).timestamp())
        desired_start_epoch = min(min_epoch + warmup_bars * step_sec, max_epoch)

        with DBManager() as db:
            user = db.get_or_create_user("analytics", "analytics@example.com", "analytics")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()

            if not st:
                st = SimulationState(
                    user_id=user.id,
                    is_running="true",
                    last_ts=datetime.fromtimestamp(desired_start_epoch, tz=timezone.utc),
                )
                db.db.add(st)
                db.db.commit()
                last_ts_epoch = desired_start_epoch
            else:
                # Forward-only: never move last_ts backward
                existing_epoch = (
                    int((st.last_ts if st.last_ts.tzinfo else st.last_ts.replace(tzinfo=timezone.utc)).timestamp())
                    if st.last_ts else None
                )
                new_epoch = desired_start_epoch if existing_epoch is None else max(existing_epoch, desired_start_epoch)
                st.is_running = "true"
                if existing_epoch != new_epoch:
                    st.last_ts = datetime.fromtimestamp(new_epoch, tz=timezone.utc)
                db.db.commit()
                last_ts_epoch = new_epoch

        # Enable auto-advance pacing toggle (does not touch time)
        try:
            import json
            with open("/tmp/sim_auto_advance.json", "w") as f:
                json.dump({"enabled": True, "pace_seconds": float(os.getenv("SIM_PACE_SECONDS", "0"))}, f)
        except Exception:
            pass

        return {"running": True, "last_ts": datetime.fromtimestamp(last_ts_epoch, tz=timezone.utc).isoformat()}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("start_simulation failed")
        raise HTTPException(status_code=500, detail=str(e))



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
    try:
        import json
        with open("/tmp/sim_auto_advance.json", "w") as f:
            json.dump({"enabled": False, "stopped": True}, f)
    except Exception:
        pass
    return {"running": False}


@router.get("/simulation/state")
def get_simulation_state() -> dict:
    with DBManager() as db:
        user = db.get_user_by_username("analytics")
        st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None
    running = False
    if st:
        running = str(st.is_running).lower() in {"true", "1"}
    return {
        "running": running,
        "last_ts": st.last_ts.isoformat() if st and st.last_ts else None,
    }


@router.get("/progress")
def get_progress() -> dict:
    with engine.connect() as conn:
        start_min = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
        end_min = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
        start_day = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
        end_day = conn.execute(select(func.max(HistoricalDailyBar.date))).scalar()

    sim_ts = _now_sim()
    sim_dt = datetime.fromtimestamp(sim_ts, tz=timezone.utc) if sim_ts else None

    def _ticks(start: Optional[datetime], end: Optional[datetime], step_seconds: int) -> tuple[int, int, float]:
        if not (start and end):
            return (0, 0, 0.0)
        total = max(0, int((end - start).total_seconds() // step_seconds))
        cur = 0 if not sim_dt else max(0, min(total, int((sim_dt - start).total_seconds() // step_seconds)))
        pct = (cur / total * 100.0) if total > 0 else 0.0
        return (cur, total, round(pct, 2))

    cur5, tot5, pct5 = _ticks(start_min, end_min, 300)
    cur1d, tot1d, pct1d = _ticks(start_day, end_day, 86400)

    # light stats
    with DBManager() as db:
        total_exec = db.count_executions()
        total_trades = db.count_trades()

    return {
        "sim_time_epoch": sim_ts,
        "sim_time_iso": sim_dt.isoformat() if sim_dt else None,
        "timeframes": {"5m": {"ticks_done": cur5, "ticks_total": tot5, "percent": pct5},
                       "1d": {"ticks_done": cur1d, "ticks_total": tot1d, "percent": pct1d}},
        "counters": {"executions_all_time": int(total_exec), "trades_all_time": int(total_trades)},
    }


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
        rows = q.order_by(AnalyticsResult.end_ts.desc().nullslast()).limit(limit).all()
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


@router.get("/errors")
def list_errors(limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    with DBManager() as db:
        rows = (
            db.db.query(RunnerExecution)
            .filter(
                (RunnerExecution.status == "error")
                | (RunnerExecution.status == "failed")
                | (RunnerExecution.status.like("skipped%"))
            )
            .order_by(RunnerExecution.execution_time.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "time": r.execution_time, "runner_id": r.runner_id, "symbol": r.symbol,
                "status": r.status, "reason": r.reason, "details": r.details, "strategy": r.strategy
            }
            for r in rows
        ]
