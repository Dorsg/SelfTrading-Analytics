from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timezone
from sqlalchemy import select, func, desc, text
import os
import sqlite3

from database.db_core import engine
from database.db_manager import DBManager
from database.models import RunnerExecution, AnalyticsResult, HistoricalDailyBar, HistoricalMinuteBar, SimulationState

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _bootstrap_runners_simple(db: DBManager, user) -> int:
    """Simple bootstrap function to create runners for analytics."""
    import os
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Simple strategy list
    strategies = ["test", "triple_top_break", "below_above"]
    timeframes = [(5, "5m"), (1440, "1d")]
    default_budget = float(os.getenv("SIM_DEFAULT_BUDGET", "10000"))
    symbol_limit = int(os.getenv("SIM_SYMBOL_LIMIT", "10"))  # Limit to 10 for testing

    try:
        with engine.connect() as conn:
            syms = [r[0] for r in conn.execute(select(HistoricalDailyBar.symbol).distinct().order_by(HistoricalDailyBar.symbol.asc())).all()]
        
        if symbol_limit > 0:
            syms = syms[:symbol_limit]
        
        logger.info(f"Creating runners for {len(syms)} symbols, {len(strategies)} strategies, {len(timeframes)} timeframes")

        created = 0
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
                        logger.debug(f"Failed to create runner {name}: {e}")
                        continue
        
        logger.info(f"Successfully created {created} runners")
        return created
        
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        raise


def _now_sim() -> Optional[int]:
    """Get current simulation time - prioritize database state over env var."""
    import os
    
    # First try to get from database (more reliable)
    try:
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if user:
                st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
                if st and st.last_ts:
                    return int(st.last_ts.timestamp())
    except Exception:
        pass
    
    # Fallback to environment variable
    try:
        v = os.getenv("SIM_TIME_EPOCH")
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

    # Get current simulation state
    with DBManager() as db:
        user = db.get_user_by_username("analytics")
        st = None
        if user:
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
        
        # Get runner execution stats
        try:
            runner_stats = db.db.execute(text("""
                SELECT 
                    COUNT(*) as total_executions,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_executions,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) as error_executions,
                    COUNT(CASE WHEN status = 'skipped_build_failed' THEN 1 END) as skipped_executions
                FROM runner_executions 
                WHERE execution_time > (NOW() - INTERVAL '1 day')
            """)).fetchone()
        except Exception:
            # Fallback if query fails
            runner_stats = (0, 0, 0, 0)
            
        # Get current runner info
        current_runner_info = None
        try:
            # Get most recent runner execution
            recent_execution = db.db.execute(text("""
                SELECT symbol, strategy, status, execution_time 
                FROM runner_executions 
                ORDER BY execution_time DESC 
                LIMIT 1
            """)).fetchone()
            
            if recent_execution:
                current_runner_info = f"{recent_execution[0]} - {recent_execution[1]} ({recent_execution[2]})"
                
        except Exception:
            pass

    return {
        "sim_time_epoch": sim_ts,
        "sim_time_iso": sim_dt.isoformat() if sim_dt else None,
        "sim_time_readable": sim_dt.strftime("%Y-%m-%d %H:%M:%S") if sim_dt else "Not started",
        "simulation_running": (st.is_running == "true") if st else False,
        "last_sim_ts": st.last_ts.isoformat() if st and st.last_ts else None,
        "current_runner_info": current_runner_info,
        "timeframes": {
            "5m":  {"ticks_done": cur5,  "ticks_total": tot5,  "percent": pct5},
            "1d":  {"ticks_done": cur1d, "ticks_total": tot1d, "percent": pct1d},
        },
        "execution_stats": {
            "total_executions": runner_stats[0] if runner_stats else 0,
            "completed_executions": runner_stats[1] if runner_stats else 0,
            "error_executions": runner_stats[2] if runner_stats else 0,
            "skipped_executions": runner_stats[3] if runner_stats else 0,
        }
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


@router.get("/logs/plain")
def get_plain_logs(
    hours_back: int = Query(24, ge=1, le=168),
    log_level: str = Query("INFO", regex="^(DEBUG|INFO|WARNING|ERROR)$"),
) -> dict:
    """Get plain text logs for errors and warnings."""
    import os
    import glob
    from datetime import datetime, timedelta
    
    # Get all log files
    log_dir = "/app/logs"
    log_files = []
    
    if os.path.exists(log_dir):
        log_files = glob.glob(os.path.join(log_dir, "*.log"))
    
    # If no logs in container, try host paths
    if not log_files:
        host_log_dir = "/root/projects/SelfTrading Analytics/logs"
        if os.path.exists(host_log_dir):
            log_files = glob.glob(os.path.join(host_log_dir, "*.log"))
    
    cutoff_time = datetime.now() - timedelta(hours=hours_back)
    log_entries = []
    
    for log_file in log_files:
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    # Parse log line format: [timestamp] LEVEL module: message
                    if '[' in line and ']' in line:
                        try:
                            # Extract timestamp and level
                            timestamp_str = line[1:line.find(']')]
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                            
                            if timestamp >= cutoff_time:
                                # Check if line contains the specified log level
                                if log_level in line:
                                    log_entries.append({
                                        "timestamp": timestamp.isoformat(),
                                        "level": log_level,
                                        "file": os.path.basename(log_file),
                                        "message": line.strip()
                                    })
                        except ValueError:
                            # Skip lines that don't match expected format
                            continue
        except Exception as e:
            continue
    
    # Sort by timestamp (newest first)
    log_entries.sort(key=lambda x: x["timestamp"], reverse=True)
    
    return {
        "log_entries": log_entries[:1000],  # Limit to 1000 entries
        "total_entries": len(log_entries),
        "hours_back": hours_back,
        "log_level": log_level,
        "files_searched": [os.path.basename(f) for f in log_files]
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


@router.get("/results/monthly-summary")
def get_monthly_summary() -> dict:
    """Get monthly average P&L by year for all strategies."""
    with DBManager() as db:
        # Get monthly P&L averages by year
        try:
            monthly_stats = db.db.execute(text("""
                SELECT 
                    EXTRACT(YEAR FROM end_ts) as year,
                    EXTRACT(MONTH FROM end_ts) as month,
                    COUNT(*) as result_count,
                    AVG(final_pnl_amount) as avg_pnl_amount,
                    AVG(final_pnl_percent) as avg_pnl_percent,
                    SUM(final_pnl_amount) as total_pnl_amount,
                    SUM(trades_count) as total_trades
                FROM analytics_results 
                WHERE end_ts IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM end_ts), EXTRACT(MONTH FROM end_ts)
                ORDER BY year DESC, month DESC
            """)).fetchall()
        except Exception:
            monthly_stats = []
        
        # Group by year
        yearly_data = {}
        for row in monthly_stats:
            year = int(row[0])
            month = int(row[1])
            if year not in yearly_data:
                yearly_data[year] = {}
            
            yearly_data[year][month] = {
                "result_count": row[2],
                "avg_pnl_amount": float(row[3]) if row[3] else 0.0,
                "avg_pnl_percent": float(row[4]) if row[4] else 0.0,
                "total_pnl_amount": float(row[5]) if row[5] else 0.0,
                "total_trades": int(row[6]) if row[6] else 0,
            }
        
        return {
            "monthly_summary": yearly_data,
            "total_years": len(yearly_data),
            "total_results": sum(sum(data["result_count"] for data in year_data.values()) for year_data in yearly_data.values())
        }


@router.get("/test/bootstrap")
def test_bootstrap() -> dict:
    """Test endpoint to bootstrap runners manually."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        with DBManager() as db:
            user = db.get_user_by_username("analytics") or (db.create_user(username="analytics", email="analytics@example.com", password="analytics"))
            
            # Clear existing runners for fresh start
            existing = db.get_runners_by_user(user_id=user.id)
            logger.info(f"Found {len(existing)} existing runners")
            
            if len(existing) == 0:
                created_count = _bootstrap_runners_simple(db, user)
                return {"success": True, "created_count": created_count, "user_id": user.id}
            else:
                return {"success": True, "message": "Runners already exist", "existing_count": len(existing)}
                
    except Exception as e:
        logger.error(f"Test bootstrap failed: {e}")
        return {"success": False, "error": str(e)}


@router.post("/simulation/force-tick")
def force_simulation_tick(fast: bool = False) -> dict:
    """Force one simulation tick manually."""
    import os
    import logging
    import asyncio
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get current simulation state
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if not user:
                return {"success": False, "error": "No analytics user found"}
            
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
            if not st:
                return {"success": False, "error": "No simulation state found"}
            
            # Get current time 
            current_ts = int(st.last_ts.timestamp()) if st.last_ts else None
            if not current_ts:
                return {"success": False, "error": "No current simulation time"}
            
            # Advance by one step
            step_seconds = int(os.getenv("SIM_STEP_SECONDS", "300"))
            new_ts = current_ts + step_seconds
            new_dt = datetime.fromtimestamp(new_ts, tz=timezone.utc)
            
            # Set environment variable for strategies to use
            os.environ["SIM_TIME_EPOCH"] = str(new_ts)
            
            # Update simulation state first
            st.last_ts = new_dt
            db.db.commit()
            
            if fast:
                # Fast mode: just advance time, minimal processing
                return {
                    "success": True,
                    "new_time": new_ts,
                    "new_time_readable": new_dt.strftime("%Y-%m-%d %H:%M:%S")
                }
            
            # Full mode: run all runners and process results
            try:
                from backend.analytics.mock_broker import MockBusinessManager
                from backend.runner_service import run_due_runners
                
                async def _run_tick():
                    bm = MockBusinessManager(user)
                    await run_due_runners(user, None, bm)
                    
                    # Process results for runners (every 10th tick only for performance)
                    if new_ts % (step_seconds * 10) == 0:
                        from backend.analytics.pnl_aggregator import compute_final_pnl_for_runner
                        from backend.analytics.result_writer import upsert_result
                        
                        runners = db.get_runners_by_user(user_id=user.id)
                        results_written = 0
                        
                        for r in runners[:10]:  # Process only first 10 runners for speed
                            try:
                                amt, pct, trades, avg_pnl, avg_dur = compute_final_pnl_for_runner(runner_id=r.id)
                                if trades > 0:
                                    tf = str(r.time_frame or "").lower()
                                    tf = "1d" if tf in {"d", "1day", "1440"} else ("5m" if tf in {"5", "5min"} else str(tf))
                                    upsert_result(
                                        symbol=r.stock,
                                        strategy=str(r.strategy),
                                        timeframe=tf,
                                        start_ts=r.time_range_from,
                                        end_ts=new_dt,
                                        final_pnl_amount=amt,
                                        final_pnl_percent=pct,
                                        trades_count=trades,
                                        max_drawdown=None,
                                        avg_pnl_per_trade=avg_pnl,
                                        avg_trade_duration_sec=avg_dur,
                                    )
                                    results_written += 1
                            except Exception:
                                continue
                        
                        return results_written
                    return 0
                
                results_count = asyncio.run(_run_tick())
                
                return {
                    "success": True,
                    "previous_time": current_ts,
                    "new_time": new_ts,
                    "new_time_readable": new_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "results_written": results_count
                }
                
            except Exception as e:
                logger.error(f"Force tick runner execution failed: {e}")
                return {
                    "success": True,  # Still success since time advanced
                    "error": f"Runner execution failed: {str(e)}",
                    "new_time": new_ts,
                    "new_time_readable": new_dt.strftime("%Y-%m-%d %H:%M:%S")
                }
                
    except Exception as e:
        logger.error(f"Force tick failed: {e}")
        return {"success": False, "error": str(e)}


@router.post("/simulation/auto-advance")
def toggle_auto_advance() -> dict:
    """Toggle automatic simulation advancement."""
    import os
    import json
    
    # Use a simple flag file to control auto-advancement
    flag_file = "/tmp/sim_auto_advance.json"
    
    try:
        # Check current state
        auto_advance_enabled = False
        if os.path.exists(flag_file):
            with open(flag_file, 'r') as f:
                data = json.load(f)
                auto_advance_enabled = data.get('enabled', False)
        
        # Toggle the state
        new_state = not auto_advance_enabled
        
        with open(flag_file, 'w') as f:
            json.dump({
                'enabled': new_state,
                'last_update': datetime.now(tz=timezone.utc).isoformat(),
                'pace_seconds': int(os.getenv("SIM_PACE_SECONDS", "1"))  # Use 1 second instead of 0 for stability
            }, f)
        
        return {
            "success": True,
            "auto_advance_enabled": new_state,
            "message": f"Auto-advance {'enabled' if new_state else 'disabled'}"
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/simulation/auto-advance/status")
def get_auto_advance_status() -> dict:
    """Get current auto-advance status."""
    import os
    import json
    
    flag_file = "/tmp/sim_auto_advance.json"
    
    try:
        if os.path.exists(flag_file):
            with open(flag_file, 'r') as f:
                data = json.load(f)
                return {
                    "enabled": data.get('enabled', False),
                    "last_update": data.get('last_update'),
                    "pace_seconds": data.get('pace_seconds', 1)
                }
        else:
            return {"enabled": False, "last_update": None, "pace_seconds": 1}
            
    except Exception:
        return {"enabled": False, "last_update": None, "pace_seconds": 1}


@router.get("/results/partial")
def get_partial_results(
    limit: int = Query(50, ge=1, le=500),
    days_back: int = Query(7, ge=1, le=365),
) -> dict:
    """Get partial results for development - shows recent results and progress."""
    with DBManager() as db:
        # Get recent results
        try:
            recent_results = db.db.execute(text(f"""
                SELECT 
                    symbol, strategy, timeframe, 
                    final_pnl_amount, final_pnl_percent, trades_count,
                    end_ts,
                    EXTRACT(EPOCH FROM (end_ts - start_ts))/86400 as days_duration
                FROM analytics_results 
                WHERE end_ts > (NOW() - INTERVAL '{days_back} days')
                ORDER BY end_ts DESC
                LIMIT {limit}
            """)).fetchall()
        except Exception:
            recent_results = []
        
        # Get execution stats for the same period
        try:
            execution_stats = db.db.execute(text(f"""
                SELECT 
                    COUNT(*) as total_executions,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) as errors,
                    COUNT(CASE WHEN status = 'skipped_build_failed' THEN 1 END) as skipped,
                    AVG(EXTRACT(EPOCH FROM (execution_time - created_at))) as avg_execution_time_seconds
                FROM runner_executions 
                WHERE execution_time > (NOW() - INTERVAL '{days_back} days')
            """)).fetchone()
        except Exception:
            execution_stats = (0, 0, 0, 0, 0)
        
        # Get current simulation state
        user = db.get_user_by_username("analytics")
        sim_state = None
        if user:
            sim_state = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
        
        return {
            "recent_results": [
                {
                    "symbol": r[0],
                    "strategy": r[1],
                    "timeframe": r[2],
                    "final_pnl_amount": float(r[3]) if r[3] else 0.0,
                    "final_pnl_percent": float(r[4]) if r[4] else 0.0,
                    "trades_count": int(r[5]) if r[5] else 0,
                    "end_ts": r[6].isoformat() if r[6] else None,
                    "days_duration": float(r[7]) if r[7] else 0.0,
                }
                for r in recent_results
            ],
            "execution_stats": {
                "total_executions": execution_stats[0] if execution_stats else 0,
                "completed": execution_stats[1] if execution_stats else 0,
                "errors": execution_stats[2] if execution_stats else 0,
                "skipped": execution_stats[3] if execution_stats else 0,
                "avg_execution_time_seconds": float(execution_stats[4]) if execution_stats and execution_stats[4] else 0.0,
            },
            "simulation_state": {
                "running": (sim_state.is_running == "true") if sim_state else False,
                "last_ts": sim_state.last_ts.isoformat() if sim_state and sim_state.last_ts else None,
            },
            "period_days": days_back,
            "results_count": len(recent_results)
        }


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
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Initialize simulation time to earliest data point if not set
    sim_ts = os.getenv("SIM_TIME_EPOCH")
    if not sim_ts:
        # Get earliest timestamp from historical data
        with engine.connect() as conn:
            earliest_minute = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
            if earliest_minute:
                sim_ts = str(int(earliest_minute.timestamp()))
                os.environ["SIM_TIME_EPOCH"] = sim_ts
                logger.info(f"Initialized SIM_TIME_EPOCH to earliest data: {sim_ts} ({earliest_minute})")
            else:
                # Fallback to a fixed date if no data
                sim_ts = "1597392000"  # 2020-08-14
                os.environ["SIM_TIME_EPOCH"] = sim_ts
                logger.info(f"Fallback: Set SIM_TIME_EPOCH to {sim_ts}")
    
    val = datetime.fromtimestamp(int(sim_ts), tz=timezone.utc) if sim_ts else None
    
    try:
        with DBManager() as db:
            # single-user analytics – pick first user; else create
            users = db.get_users_with_ib()  # returns [] in analytics; use any user
            user = db.get_user_by_username("analytics") or (db.create_user(username="analytics", email="analytics@example.com", password="analytics"))
            logger.info(f"Using analytics user: {user.username} (ID: {user.id})")
            
            # Ensure runners are set up for this user
            existing = db.get_runners_by_user(user_id=user.id)
            logger.info(f"Found {len(existing)} existing runners for user {user.id}")
            
            if not existing:
                logger.info("No runners found, bootstrapping...")
                try:
                    created_count = _bootstrap_runners_simple(db, user)
                    # Check if runners were created
                    after_bootstrap = db.get_runners_by_user(user_id=user.id)
                    logger.info(f"After bootstrap: {len(after_bootstrap)} runners created (expected: {created_count})")
                except Exception as e:
                    logger.error(f"Failed to bootstrap runners: {e}")
                    # Continue anyway - simulation can run without runners initially
            
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
            if not st:
                st = SimulationState(user_id=user.id, is_running="true", last_ts=val)
                db.db.add(st)
                logger.info("Created new simulation state")
            else:
                st.is_running = "true"
                if val:
                    st.last_ts = val
                logger.info("Updated existing simulation state")
            
            db.db.commit()
            logger.info("Simulation started successfully")
            
            # Enable auto-advance by default when starting simulation
            import json
            flag_file = "/tmp/sim_auto_advance.json"
            try:
                with open(flag_file, 'w') as f:
                    json.dump({
                        'enabled': True,
                        'last_update': datetime.now(tz=timezone.utc).isoformat(),
                        'pace_seconds': 0.1  # Very fast - 10 ticks per second
                    }, f)
                logger.info("Auto-advance enabled for fast simulation")
            except Exception as e:
                logger.warning(f"Failed to enable auto-advance: {e}")
            
            return {"running": True, "last_ts": st.last_ts}
            
    except Exception as e:
        logger.error(f"Failed to start simulation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start simulation: {str(e)}")


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
            
            # Check if import is completed - try multiple approaches
            import_marker_paths = [
                "/app/data/.import_completed",
                "./data/.import_completed", 
                "/root/projects/SelfTrading Analytics/data/.import_completed"
            ]
            import_marker_exists = any(os.path.exists(path) for path in import_marker_paths)
            
            # Also consider import complete if we have the full dataset
            # Based on logs: 50,411,392 minute bars = 100% complete
            import_complete_by_data = minute_count >= 50411392
            
            # Import is considered complete if marker exists OR we have full data
            import_completed = import_marker_exists or import_complete_by_data
            
            # Determine readiness - data import completion is the main requirement
            has_data = daily_count > 0 and minute_count > 0
            has_setup = users_count > 0 and runners_count > 0
            
            # System is ready if import is completed and we have data
            # Setup (users/runners) can be created on-demand
            is_ready = has_data and import_completed
            
            # Get date ranges for data
            start_date = None
            end_date = None
            if daily_count > 0:
                start_date = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
                end_date = conn.execute(select(func.max(HistoricalDailyBar.date))).scalar()
            
            # Calculate import progress only if import is not completed
            import_progress = None
            if not import_completed:
                # Get actual target counts from SQLite source file
                # Try multiple possible paths for the SQLite file
                sqlite_paths = [
                    os.getenv("ANALYTICS_SQLITE_PATH", "/app/tools/finnhub_downloader/data/daily_bars.sqlite"),
                    "/app/tools/finnhub_downloader/data/daily_bars.sqlite",
                    "./tools/finnhub_downloader/data/daily_bars.sqlite",
                    "/root/projects/SelfTrading Analytics/tools/finnhub_downloader/data/daily_bars.sqlite"
                ]
                
                actual_daily_target = None
                actual_minute_target = None
                
                for sqlite_path in sqlite_paths:
                    try:
                        if os.path.exists(sqlite_path):
                            with sqlite3.connect(sqlite_path) as sqlite_conn:
                                cur = sqlite_conn.cursor()
                                
                                # Get actual total counts from source
                                cur.execute("SELECT COUNT(*) FROM daily_bars")
                                actual_daily_target = cur.fetchone()[0]
                                
                                cur.execute("SELECT COUNT(*) FROM minute_bars WHERE interval=5")
                                actual_minute_target = cur.fetchone()[0]
                                break  # Found the file, use these values
                    except Exception as e:
                        continue  # Try next path
                
                # Use known values from the logs if SQLite not accessible
                # Based on recent logs: 20,400,000/50,411,392 = 40.5% for minute bars
                if actual_daily_target is None or actual_minute_target is None:
                    actual_daily_target = 2000000  # Conservative estimate for daily bars
                    actual_minute_target = 50411392  # From the logs - this is the actual total
                
                # Calculate accurate progress percentages
                daily_progress = min(100, (daily_count / actual_daily_target) * 100) if actual_daily_target > 0 else 0
                minute_progress = min(100, (minute_count / actual_minute_target) * 100) if actual_minute_target > 0 else 0
                
                # If no data yet, show as starting (1% to indicate activity)
                if daily_count == 0 and minute_count == 0:
                    daily_progress = 1
                    minute_progress = 1
                
                # Ensure we show progress even if targets are estimates
                if minute_count > 0 and minute_progress == 0:
                    minute_progress = max(1, min(99, (minute_count / 50411392) * 100))
                if daily_count > 0 and daily_progress == 0:
                    daily_progress = max(1, min(99, (daily_count / 2000000) * 100))
                
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
                "import_completed": import_completed,
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
                "status": "ready" if is_ready else ("importing" if not import_completed else ("setup_needed" if not has_setup else "waiting"))
            }
            
        except Exception as e:
            return {
                "ready": False,
                "error": str(e),
                "status": "error"
            }


