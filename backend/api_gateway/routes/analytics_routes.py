from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func, desc, text
import os
import sqlite3
import logging

from database.db_core import engine
from database.db_manager import DBManager
from database.models import RunnerExecution, AnalyticsResult, HistoricalDailyBar, HistoricalMinuteBar, SimulationState, ExecutedTrade, Runner

router = APIRouter(prefix="/analytics", tags=["analytics"])
@router.get("/debug/database-test")
async def test_database_connection() -> dict:
    """Test database connection and data availability."""
    logger = logging.getLogger("api-gateway")
    
    try:
        with engine.connect() as conn:
            logger.info("✅ Database connection successful")
            
            # Check if tables exist
            tables_exist = {}
            for table_name in ['historical_minute_bars', 'historical_daily_bars', 'users', 'runners']:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                    tables_exist[table_name] = result
                    logger.info(f"📊 Table {table_name}: {result} rows")
                except Exception as e:
                    tables_exist[table_name] = f"ERROR: {str(e)}"
                    logger.error(f"❌ Table {table_name} error: {e}")
            
            # Check available intervals in minute bars
            intervals = {}
            try:
                interval_results = conn.execute(text("""
                    SELECT interval_min, COUNT(*) 
                    FROM historical_minute_bars 
                    GROUP BY interval_min 
                    ORDER BY interval_min
                """)).all()
                intervals = dict(interval_results)
                logger.info(f"📈 Available intervals: {intervals}")
            except Exception as e:
                intervals = f"ERROR: {str(e)}"
                logger.error(f"❌ Intervals query error: {e}")
            
            # Sample symbols for 5min data
            sample_symbols = []
            try:
                symbol_results = conn.execute(text("""
                    SELECT DISTINCT symbol 
                    FROM historical_minute_bars 
                    WHERE interval_min = 5 
                    LIMIT 10
                """)).all()
                sample_symbols = [r[0] for r in symbol_results]
                logger.info(f"🏢 Sample 5min symbols: {sample_symbols}")
            except Exception as e:
                sample_symbols = f"ERROR: {str(e)}"
                logger.error(f"❌ Sample symbols error: {e}")
            
            return {
                "success": True,
                "database_connected": True,
                "tables": tables_exist,
                "intervals": intervals,
                "sample_symbols": sample_symbols
            }
            
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return {
            "success": False,
            "database_connected": False,
            "error": str(e)
        }

@router.get("/debug/comprehensive-test")
async def comprehensive_system_test() -> dict:
    """Comprehensive test of the entire system flow."""
    logger = logging.getLogger("api-gateway")
    
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tests": {}
    }
    
    # Test 1: Database connection and data
    try:
        with engine.connect() as conn:
            minute_count = conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0
            daily_count = conn.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0
            
            results["tests"]["database"] = {
                "connected": True,
                "minute_bars": minute_count,
                "daily_bars": daily_count,
                "has_data": minute_count > 0 and daily_count > 0
            }
            
            if minute_count > 0:
                earliest = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                latest = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                
                results["tests"]["data_range"] = {
                    "earliest": earliest.isoformat() if earliest else None,
                    "latest": latest.isoformat() if latest else None,
                    "earliest_readable": earliest.strftime("%Y-%m-%d %H:%M:%S UTC") if earliest else None,
                    "latest_readable": latest.strftime("%Y-%m-%d %H:%M:%S UTC") if latest else None,
                    "total_days": int((latest - earliest).days) if earliest and latest else 0
                }
                
                # Check intervals
                intervals = conn.execute(
                    select(HistoricalMinuteBar.interval_min, func.count())
                    .group_by(HistoricalMinuteBar.interval_min)
                ).all()
                results["tests"]["intervals"] = dict(intervals)
                
                # Sample symbols for 5min
                symbols = conn.execute(
                    select(HistoricalMinuteBar.symbol)
                    .where(HistoricalMinuteBar.interval_min == 5)
                    .distinct()
                    .limit(5)
                ).all()
                results["tests"]["sample_symbols"] = [s[0] for s in symbols]
            
    except Exception as e:
        results["tests"]["database"] = {"connected": False, "error": str(e)}
    
    # Test 2: Simulation time
    sim_ts = os.getenv("SIM_TIME_EPOCH")
    if sim_ts:
        try:
            sim_dt = datetime.fromtimestamp(int(sim_ts), tz=timezone.utc)
            results["tests"]["simulation_time"] = {
                "set": True,
                "timestamp": int(sim_ts),
                "datetime": sim_dt.isoformat(),
                "readable": sim_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            }
        except Exception as e:
            results["tests"]["simulation_time"] = {"set": True, "error": str(e)}
    else:
        results["tests"]["simulation_time"] = {"set": False}
    
    # Test 3: MarketDataManager query
    try:
        from backend.ib_manager.market_data_manager import MarketDataManager
        mkt = MarketDataManager()
        
        test_symbol = "AAPL"
        candles = await mkt._get_candles(test_symbol, "5", 10)
        
        results["tests"]["market_data"] = {
            "symbol": test_symbol,
            "candles_returned": len(candles),
            "sample_candles": candles[:2] if candles else [],
            "success": len(candles) > 0
        }
    except Exception as e:
        results["tests"]["market_data"] = {"error": str(e), "success": False}
    
    return results

@router.get("/debug/test-candles")
async def test_candles_query(symbol: str = "AAPL", timeframe: str = "5") -> dict:
    """Test endpoint to verify candle data query works."""
    from backend.ib_manager.market_data_manager import MarketDataManager
    from sqlalchemy import text
    
    try:
        # First check what's actually in the database
        with engine.connect() as conn:
            # Check if tables exist
            tables_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'historical%'
            """)).fetchall()
            
            # Check minute bars count and sample
            minute_count = conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0
            
            # Check what symbols exist
            symbols_sample = []
            if minute_count > 0:
                symbols_sample = [r[0] for r in conn.execute(
                    select(HistoricalMinuteBar.symbol).distinct().limit(10)
                ).fetchall()]
            
            # Check what intervals exist
            intervals_sample = []
            if minute_count > 0:
                intervals_sample = [r[0] for r in conn.execute(
                    select(HistoricalMinuteBar.interval_min).distinct()
                ).fetchall()]
            
            # Check specific symbol data
            symbol_data = []
            if minute_count > 0:
                symbol_data = conn.execute(
                    select(HistoricalMinuteBar.symbol, HistoricalMinuteBar.interval_min, func.count())
                    .where(HistoricalMinuteBar.symbol == symbol.upper())
                    .group_by(HistoricalMinuteBar.symbol, HistoricalMinuteBar.interval_min)
                ).fetchall()
        
        # Now test the MarketDataManager
        mkt = MarketDataManager()
        candles = await mkt._get_candles(symbol, timeframe, 10)
        
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "candles_found": len(candles),
            "sample_candles": candles[:3] if candles else [],
            "database_info": {
                "tables": [t[0] for t in tables_check],
                "minute_bars_count": minute_count,
                "symbols_sample": symbols_sample,
                "intervals_available": intervals_sample,
                "symbol_specific_data": [{"symbol": r[0], "interval": r[1], "count": r[2]} for r in symbol_data]
            },
            "success": True
        }
    except Exception as e:
        import traceback
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }

@router.get("/strategies/compare")
def compare_strategies() -> dict:
    """Compare strategy names available in analytics vs main app.
    Assumes both projects exist under /root/projects.
    Returns simple lists to manually verify parity.
    """
    import os, glob
    def list_strategy_names(root: str) -> list[str]:
        paths = glob.glob(os.path.join(root, "backend/strategies/*_strategy.py"))
        names = []
        for p in paths:
            base = os.path.basename(p)
            if base.endswith("_strategy.py"):
                names.append(base[:-3])  # file name without .py
        return sorted(names)

    analytics_root = "/root/projects/SelfTrading Analytics"
    main_root      = "/root/projects/SelfTrading"
    try:
        analytics = list_strategy_names(analytics_root)
    except Exception:
        analytics = []
    try:
        main = list_strategy_names(main_root)
    except Exception:
        main = []

    only_in_main = sorted([n for n in main if n not in analytics])
    only_in_analytics = sorted([n for n in analytics if n not in main])

    return {
        "analytics": analytics,
        "main": main,
        "only_in_main": only_in_main,
        "only_in_analytics": only_in_analytics,
        "parity": len(only_in_main) == 0 and len(only_in_analytics) == 0,
    }


def _bootstrap_runners_simple(db: DBManager, user) -> int:
    """Simple bootstrap function to create runners for analytics."""
    import os
    import logging
    
    logger = logging.getLogger("api-gateway")
    
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
    Overall simulation progress based on current sim-time vs earliest/latest bars.
    DB-agnostic (works on Postgres/SQLite). Also returns last/current runner info
    and a realistic ETA using the auto-advance pace if available.
    """
    from sqlalchemy import select, func, desc
    from datetime import datetime, timezone, timedelta
    import json
    import os

    # ── 0) Data range (for % complete)
    with engine.connect() as conn:
        start_min = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
        end_min   = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
        start_day = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
        end_day   = conn.execute(select(func.max(HistoricalDailyBar.date))).scalar()

    sim_ts = _now_sim() or 0
    sim_dt = datetime.fromtimestamp(sim_ts, tz=timezone.utc) if sim_ts else None
    
    # Enhanced logging for debugging
    logger = logging.getLogger("api-gateway")
    logger.info(f"📊 Progress Check - Sim time: {sim_dt} (ts: {sim_ts})")
    logger.info(f"📅 Data range: {start_min} to {end_min}")
    if start_min and end_min and sim_dt:
        if sim_dt < start_min:
            logger.warning(f"⚠️ Simulation time ({sim_dt}) is BEFORE data start ({start_min})")
        elif sim_dt > end_min:
            logger.warning(f"⚠️ Simulation time ({sim_dt}) is AFTER data end ({end_min})")
        else:
            logger.info(f"✅ Simulation time is within data range")

    def _ticks(start: Optional[datetime], end: Optional[datetime], step_seconds: int) -> tuple[int, int, float]:
        if not (start and end):
            return (0, 0, 0.0)
        total = max(0, int((end - start).total_seconds() // step_seconds))
        cur   = 0 if not sim_dt else max(0, min(total, int((sim_dt - start).total_seconds() // step_seconds)))
        pct   = (cur / total * 100.0) if total > 0 else 0.0
        return (cur, total, round(pct, 2))

    cur5, tot5, pct5     = _ticks(start_min, end_min, 300)
    cur1d, tot1d, pct1d  = _ticks(start_day, end_day, 86400)

    # ── 1) Simulation state
    with DBManager() as db:
        user = db.get_user_by_username("analytics")
        st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None

        # ── 2) Execution stats (last 24h), DB-agnostic (compute in Python)
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        statuses: list[str] = []
        if user:
            rows = (
                db.db.query(RunnerExecution.status, RunnerExecution.execution_time)
                .filter(RunnerExecution.execution_time >= cutoff)
                .all()
            )
            statuses = [r[0] for r in rows if r and r[0] is not None]

        total_exec   = len(statuses)
        completed    = sum(1 for s in statuses if s == "completed")
        errors       = sum(1 for s in statuses if s in {"error", "failed"})
        skipped      = sum(1 for s in statuses if str(s).startswith("skipped"))

        # All-time counters
        executions_all_time = 0
        trades_all_time = 0
        try:
            executions_all_time = db.db.query(func.count(RunnerExecution.id)).scalar() or 0
        except Exception:
            executions_all_time = 0
        try:
            trades_all_time = db.db.query(func.count(ExecutedTrade.id)).scalar() or 0
        except Exception:
            trades_all_time = 0

        # Fallback to analytics_results when runner_executions isn't populated
        if total_exec == 0:
            try:
                completed_from_results = (
                    db.db.query(AnalyticsResult)
                    .filter(AnalyticsResult.end_ts != None, AnalyticsResult.end_ts >= cutoff)
                    .count()
                )
            except Exception:
                completed_from_results = 0
            if completed_from_results > 0:
                total_exec = completed_from_results
                completed  = completed_from_results

        # ── 3) Current/last runner info (prefer runner_executions, else analytics_results)
        current_runner_info = None
        current_struct = None
        if user:
            try:
                re = (
                    db.db.query(
                        RunnerExecution.symbol,
                        RunnerExecution.strategy,
                        RunnerExecution.status,
                        Runner.time_frame,
                    )
                    .join(Runner, Runner.id == RunnerExecution.runner_id)
                    .order_by(RunnerExecution.execution_time.desc())
                    .first()
                )
                if re:
                    tf_val = str(re[3] or "").lower()
                    tf = "1d" if tf_val in {"d", "1day", "1440"} else ("5m" if tf_val in {"5", "5min", "5m"} else tf_val)
                    current_runner_info = f"{re[0]} - {re[1]} ({re[2]})"
                    current_struct = {
                        "symbol": re[0],
                        "strategy": re[1],
                        "status": re[2],
                        "timeframe": tf,
                    }
                else:
                    ar = (
                        db.db.query(AnalyticsResult.symbol, AnalyticsResult.strategy, AnalyticsResult.timeframe)
                        .order_by(AnalyticsResult.end_ts.desc())
                        .first()
                    )
                    if ar:
                        current_runner_info = f"{ar[0]} - {ar[1]} ({ar[2]})"
                        current_struct = {
                            "symbol": ar[0],
                            "strategy": ar[1],
                            "status": "completed",
                            "timeframe": ar[2],
                        }
            except Exception:
                pass

    # ── 4) ETA (use /tmp/sim_auto_advance.json pace if available; else assume ~2 ticks/sec)
    estimated_finish = None
    try:
        pace_seconds = None
        step_seconds = int(os.getenv("SIM_STEP_SECONDS", "300"))
        flag_path = "/tmp/sim_auto_advance.json"
        if os.path.exists(flag_path):
            with open(flag_path, "r") as f:
                data = json.load(f)
                pace_seconds = float(data.get("pace_seconds", 0)) or None
        # ticks/sec: if pace_seconds is 0/None => default to ~2 (matches UI timer of 500ms)
        ticks_per_sec = (1.0 / pace_seconds) if (pace_seconds and pace_seconds > 0) else 2.0

        if sim_dt and end_min and st and st.is_running == "true":
            remaining_sim_seconds = max(0.0, (end_min - sim_dt).total_seconds())
            sim_seconds_per_real_second = ticks_per_sec * float(step_seconds)
            if sim_seconds_per_real_second > 0:
                eta_real_seconds = remaining_sim_seconds / sim_seconds_per_real_second
                estimated_finish_dt = datetime.now(tz=timezone.utc) + timedelta(seconds=eta_real_seconds)
                estimated_finish = estimated_finish_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        estimated_finish = None

    return {
        "sim_time_epoch": sim_ts,
        "sim_time_iso": sim_dt.isoformat() if sim_dt else None,
        "sim_time_readable": sim_dt.strftime("%Y-%m-%d %H:%M:%S") if sim_dt else "Not started",
        "simulation_running": (st.is_running == "true") if st else False,
        "last_sim_ts": st.last_ts.isoformat() if st and st.last_ts else None,

        "current_runner_info": current_runner_info,
        "current": current_struct,
        "estimated_finish": estimated_finish,

        "timeframes": {
            "5m":  {"ticks_done": cur5,  "ticks_total": tot5,  "percent": pct5},
            "1d":  {"ticks_done": cur1d, "ticks_total": tot1d, "percent": pct1d},
        },
        "execution_stats": {
            "total_executions": total_exec,
            "completed_executions": completed,
            "error_executions": errors,
            "skipped_executions": skipped,
        },
        "counters": {
            "executions_all_time": int(executions_all_time),
            "trades_all_time": int(trades_all_time),
        },
        # Enhanced debugging and status information
        "data_range": {
            "start": start_min.isoformat() if start_min else None,
            "end": end_min.isoformat() if end_min else None,
            "start_readable": start_min.strftime("%Y-%m-%d %H:%M:%S UTC") if start_min else None,
            "end_readable": end_min.strftime("%Y-%m-%d %H:%M:%S UTC") if end_min else None,
            "total_days": int((end_min - start_min).days) if start_min and end_min else 0,
        },
        "simulation_status": {
            "time_position": "before_data" if sim_dt and start_min and sim_dt < start_min else
                           "after_data" if sim_dt and end_min and sim_dt > end_min else
                           "within_range" if sim_dt and start_min and end_min else "no_time_set",
            "days_simulated": int((sim_dt - start_min).days) if sim_dt and start_min else 0,
            "days_remaining": int((end_min - sim_dt).days) if sim_dt and end_min else 0,
        },
        "debug_info": {
            "sim_timestamp": sim_ts,
            "has_simulation_state": st is not None,
            "simulation_state_running": st.is_running if st else None,
            "env_sim_time": os.getenv("SIM_TIME_EPOCH"),
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
    log_level: str = Query("ERROR", regex="^(DEBUG|INFO|WARNING|ERROR)$"),
) -> dict:
    """
    Parse *.log files and return ONLY the requested severities.
    No synthetic INFO entries are injected anymore.
    If no matches are found, return an empty list.
    """
    import os, glob
    from datetime import datetime, timedelta, timezone

    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    want = log_level.upper()
    allowed_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}
    if want not in allowed_levels:
        want = "ERROR"

    log_dirs = ["/app/logs", "/root/projects/SelfTrading Analytics/logs"]
    log_files = []
    for d in log_dirs:
        if os.path.exists(d):
            log_files.extend(glob.glob(os.path.join(d, "*.log")))

    entries = []

    def _accept(line_upper: str) -> bool:
        if want == "ERROR":
            return "ERROR" in line_upper or "EXCEPTION" in line_upper or "TRACEBACK" in line_upper or "FAILED" in line_upper
        if want == "WARNING":
            return "WARNING" in line_upper
        if want == "INFO":
            # Include INFO but not noise from DEBUG
            return "INFO" in line_upper and "DEBUG" not in line_upper
        return "DEBUG" in line_upper

    for path in log_files:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if "[" not in line or "]" not in line:
                        continue
                    # Expect format: [YYYY-mm-dd HH:MM:SS,ms] LEVEL NAME: msg
                    try:
                        ts_str = line[1:line.find("]")]
                        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                        # stored without tz in file; compare in naive
                        if ts < cutoff_time.replace(tzinfo=None):
                            continue
                    except Exception:
                        continue

                    up = line.upper()
                    if _accept(up):
                        lvl = "ERROR" if "ERROR" in up or "EXCEPTION" in up or "TRACEBACK" in up or "FAILED" in up else (
                            "WARNING" if "WARNING" in up else ("INFO" if "INFO" in up else "DEBUG")
                        )
                        entries.append({
                            "timestamp": ts.isoformat(),
                            "level": lvl,
                            "file": os.path.basename(path),
                            "message": line.strip()
                        })
        except Exception:
            continue

    # Sort newest first, cap to 1000
    entries.sort(key=lambda x: x["timestamp"], reverse=True)
    return {
        "log_entries": entries[:1000],
        "total_entries": len(entries),
        "hours_back": hours_back,
        "log_level": want,
        "files_searched": [os.path.basename(f) for f in log_files] if log_files else [],
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
                    CAST(strftime('%Y', end_ts) AS INT) as year,
                    CAST(strftime('%m', end_ts) AS INT) as month,
                    COUNT(*) as result_count,
                    AVG(final_pnl_amount) as avg_pnl_amount,
                    AVG(final_pnl_percent) as avg_pnl_percent,
                    SUM(final_pnl_amount) as total_pnl_amount,
                    SUM(trades_count) as total_trades
                FROM analytics_results 
                WHERE end_ts IS NOT NULL
                GROUP BY CAST(strftime('%Y', end_ts) AS INT), CAST(strftime('%m', end_ts) AS INT)
                ORDER BY year DESC, month DESC
            """)).fetchall()
        except Exception:
            # Fallback for Postgres
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
    logger = logging.getLogger("api-gateway")
    
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
async def force_simulation_tick(fast: bool = False) -> dict:
    """Force one simulation tick manually."""
    import os
    import logging
    import asyncio
    
    logger = logging.getLogger("api-gateway")
    
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
                # Fast mode: advance time and run strategies but skip heavy result processing
                try:
                    from backend.analytics.mock_broker import MockBusinessManager
                    from backend.runner_service import run_due_runners
                    
                    bm = MockBusinessManager(user)
                    await run_due_runners(user, None, bm)
                    
                except Exception as e:
                    logger.debug(f"Fast tick runner execution failed: {e}")
                
                return {
                    "success": True,
                    "new_time": new_ts,
                    "new_time_readable": new_dt.strftime("%Y-%m-%d %H:%M:%S")
                }
            
            # Full mode: run all runners and process results
            try:
                from backend.analytics.mock_broker import MockBusinessManager
                from backend.runner_service import run_due_runners
                
                bm = MockBusinessManager(user)
                await run_due_runners(user, None, bm)
                
                # Process results for runners on every tick (write when trades exist)
                results_written = 0
                try:
                    from backend.analytics.pnl_aggregator import compute_final_pnl_for_runner
                    from backend.analytics.result_writer import upsert_result
                    runners = db.get_runners_by_user(user_id=user.id)
                    for r in runners:
                        try:
                            amt, pct, trades, avg_pnl, avg_dur = compute_final_pnl_for_runner(runner_id=r.id)
                            if trades and trades > 0:
                                tf = str(r.time_frame or "").lower()
                                tf = "1d" if tf in {"d", "1day", "1440"} else ("5m" if tf in {"5", "5min", "5m"} else str(tf))
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
                except Exception:
                    results_written = 0
                
                results_count = results_written
                
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
    """Recent partial results + execution stats; DB-agnostic and safe when stopped."""
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    with DBManager() as db:
        # Recent results (safe ORM)
        try:
            recents = (
                db.db.query(
                    AnalyticsResult.symbol,
                    AnalyticsResult.strategy,
                    AnalyticsResult.timeframe,
                    AnalyticsResult.final_pnl_amount,
                    AnalyticsResult.final_pnl_percent,
                    AnalyticsResult.trades_count,
                    AnalyticsResult.end_ts,
                    AnalyticsResult.start_ts,
                )
                .filter(AnalyticsResult.end_ts != None, AnalyticsResult.end_ts >= cutoff)
                .order_by(desc(AnalyticsResult.end_ts))
                .limit(limit)
                .all()
            )
        except Exception:
            recents = []

        recent_results = []
        for r in recents:
            end_ts = r[6]
            start_ts = r[7]
            days_duration = 0.0
            try:
                if end_ts and start_ts:
                    # Both columns are datetimes already
                    days_duration = max(0.0, (end_ts - start_ts).total_seconds() / 86400.0)
            except Exception:
                days_duration = 0.0

            recent_results.append({
                "symbol": r[0],
                "strategy": r[1],
                "timeframe": r[2],
                "final_pnl_amount": float(r[3]) if r[3] is not None else 0.0,
                "final_pnl_percent": float(r[4]) if r[4] is not None else 0.0,
                "trades_count": int(r[5]) if r[5] is not None else 0,
                "end_ts": end_ts.isoformat() if end_ts else None,
                "days_duration": round(days_duration, 3),
            })

        # Execution stats (safe ORM + Python)
        try:
            exec_rows = (
                db.db.query(
                    RunnerExecution.status,
                    RunnerExecution.execution_time,
                    RunnerExecution.created_at,
                )
                .filter(RunnerExecution.execution_time >= cutoff)
                .all()
            )
        except Exception:
            exec_rows = []

        total = len(exec_rows)
        completed = 0
        errors = 0
        skipped = 0
        durations = []

        for s, exec_time, created_at in exec_rows:
            if s == "completed":
                completed += 1
            elif s in {"error", "failed"}:
                errors += 1
            elif str(s or "").startswith("skipped"):
                skipped += 1
            # average execution time (seconds)
            try:
                if exec_time and created_at:
                    durations.append(max(0.0, (exec_time - created_at).total_seconds()))
            except Exception:
                pass

        avg_exec_time = (sum(durations) / len(durations)) if durations else 0.0

        # Sim state (safe)
        user = db.get_user_by_username("analytics")
        sim_state = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None

        return {
            "recent_results": recent_results,
            "execution_stats": {
                "total_executions": total,
                "completed": completed,
                "errors": errors,
                "skipped": skipped,
                "avg_execution_time_seconds": round(avg_exec_time, 3),
            },
            "counters": {
                "executions_all_time": int(db.db.query(func.count(RunnerExecution.id)).scalar() or 0),
                "trades_all_time": int(db.db.query(func.count(ExecutedTrade.id)).scalar() or 0),
            },
            "simulation_state": {
                "running": (sim_state.is_running == "true") if sim_state else False,
                "last_ts": sim_state.last_ts.isoformat() if sim_state and sim_state.last_ts else None,
            },
            "period_days": days_back,
            "results_count": len(recent_results),
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
        execs_all  = db.db.query(func.count(RunnerExecution.id)).scalar() or 0
        trades_all = db.db.query(func.count(ExecutedTrade.id)).scalar() or 0
    return {
        "symbols": int(syms),
        "strategies": int(strategies),
        "timeframes": int(timeframes),
        "results": int(results),
        "errors": int(errors),
        "executions_all_time": int(execs_all),
        "trades_all_time": int(trades_all),
    }


@router.post("/simulation/start")
def start_simulation() -> dict:
    """Mark simulation as running and persist last_ts from env if set."""
    import os
    import logging
    
    logger = logging.getLogger("api-gateway")
    
    # Always initialize simulation time to earliest data point for clean start
    logger.info("🚀 Initializing simulation time...")
    
    with engine.connect() as conn:
        # Get earliest timestamp from historical data
        earliest_minute = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
        latest_minute = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
        
        if earliest_minute and latest_minute:
            sim_ts = str(int(earliest_minute.timestamp()))
            os.environ["SIM_TIME_EPOCH"] = sim_ts
            logger.info(f"📅 Data range: {earliest_minute} to {latest_minute}")
            logger.info(f"🎯 Set SIM_TIME_EPOCH to earliest: {sim_ts} ({earliest_minute})")
        else:
            # Fallback to a fixed date if no data
            sim_ts = "1577836800"  # 2020-01-01
            os.environ["SIM_TIME_EPOCH"] = sim_ts
            logger.warning(f"⚠️ No historical data found! Fallback: Set SIM_TIME_EPOCH to {sim_ts}")
    
    # Clear any existing simulation state to start fresh
    try:
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if user:
                st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
                if st:
                    db.db.delete(st)
                    db.db.commit()
                    logger.info("🧹 Cleared existing simulation state for fresh start")
    except Exception as e:
        logger.warning(f"⚠️ Could not clear simulation state: {e}")
    
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
    logger = logging.getLogger("api-gateway")
    
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
        
        # Disable auto-advance flag file
        import json
        import os
        flag_file = "/tmp/sim_auto_advance.json"
        try:
            if os.path.exists(flag_file):
                with open(flag_file, 'w') as f:
                    json.dump({
                        'enabled': False,
                        'last_update': datetime.now(tz=timezone.utc).isoformat(),
                        'stopped_by': 'stop_simulation_api'
                    }, f)
                logger.info("Auto-advance disabled via stop simulation")
            else:
                # Create disabled flag file
                with open(flag_file, 'w') as f:
                    json.dump({'enabled': False}, f)
        except Exception as e:
            logger.warning(f"Failed to disable auto-advance: {e}")
        
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
    logger = logging.getLogger("api-gateway")
    
    try:
        with engine.connect() as conn:
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
        logger.error(f"Database status check failed: {e}")
        return {
            "ready": False,
            "error": str(e),
            "status": "error",
            "message": "Database connection failed. Check if PostgreSQL is running and accessible."
        }


