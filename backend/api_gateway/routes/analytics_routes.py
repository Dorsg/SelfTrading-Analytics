from __future__ import annotations
from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime, timezone
from sqlalchemy import select, func, desc, text
import os
import sqlite3

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


@router.get("/database/status")
def get_database_status() -> dict:
    """
    Check database readiness for analytics simulation.
    Returns status of database tables and data availability.
    """
    with engine.connect() as conn:
        try:
            # Check if tables exist and have data
            daily_count = conn.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0
            minute_count = conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0
            users_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar() or 0
            runners_count = conn.execute(text("SELECT COUNT(*) FROM runners")).scalar() or 0
            
            # Check if import is completed
            import_marker_exists = os.path.exists("/app/data/.import_completed")
            
            # Determine readiness
            has_data = daily_count > 0 and minute_count > 0
            has_setup = users_count > 0 and runners_count > 0
            is_ready = has_data and has_setup and import_marker_exists
            
            # Get date ranges for data
            start_date = None
            end_date = None
            if daily_count > 0:
                start_date = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
                end_date = conn.execute(select(func.max(HistoricalDailyBar.date))).scalar()
            
            # Calculate import progress if importing
            import_progress = None
            if not import_marker_exists:
                # Get actual target counts from SQLite source file
                sqlite_path = os.getenv("ANALYTICS_SQLITE_PATH", "/app/tools/finnhub_downloader/data/daily_bars.sqlite")
                
                try:
                    if os.path.exists(sqlite_path):
                        with sqlite3.connect(sqlite_path) as sqlite_conn:
                            cur = sqlite_conn.cursor()
                            
                            # Get actual total counts from source
                            cur.execute("SELECT COUNT(*) FROM daily_bars")
                            actual_daily_target = cur.fetchone()[0]
                            
                            cur.execute("SELECT COUNT(*) FROM minute_bars WHERE interval=5")
                            actual_minute_target = cur.fetchone()[0]
                    else:
                        # Fallback to estimates if SQLite not found
                        actual_daily_target = 500000
                        actual_minute_target = 2000000
                except Exception:
                    # Fallback to estimates on any error
                    actual_daily_target = 500000
                    actual_minute_target = 2000000
                
                # Calculate accurate progress percentages
                daily_progress = min(100, (daily_count / actual_daily_target) * 100) if actual_daily_target > 0 else 0
                minute_progress = min(100, (minute_count / actual_minute_target) * 100) if actual_minute_target > 0 else 0
                
                # If no data yet, show as starting (1% to indicate activity)
                if daily_count == 0 and minute_count == 0:
                    daily_progress = 1
                    minute_progress = 1
                
                import_progress = {
                    "daily_progress": round(daily_progress, 1),
                    "minute_progress": round(minute_progress, 1),
                    "overall_progress": round((daily_progress + minute_progress) / 2, 1),
                    "importing": True,
                    "targets": {
                        "daily_target": actual_daily_target,
                        "minute_target": actual_minute_target
                    }
                }

            return {
                "ready": is_ready,
                "import_completed": import_marker_exists,
                "import_progress": import_progress,
                "data": {
                    "daily_bars": daily_count,
                    "minute_bars": minute_count,
                    "has_data": has_data,
                    "date_range": {
                        "start": start_date.isoformat() if start_date else None,
                        "end": end_date.isoformat() if end_date else None,
                    }
                },
                "setup": {
                    "users": users_count,
                    "runners": runners_count,
                    "has_setup": has_setup,
                },
                "status": "ready" if is_ready else ("importing" if not import_marker_exists else "setup_needed")
            }
            
        except Exception as e:
            return {
                "ready": False,
                "error": str(e),
                "status": "error"
            }


