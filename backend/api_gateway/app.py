from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from logger_config import setup_logging
from database.db_core import wait_for_db_ready
from database.db_manager import DBManager
from database.models import (
    SimulationState,
    ExecutedTrade,
    OpenPosition,
    Runner,
    RunnerExecution,
    Account,
)
from sqlalchemy import select, func, text
from sqlalchemy.orm import Session

from backend.ib_manager.market_data_manager import MarketDataManager

# --------------------------------------------------------------------------------------
# App & logging
# --------------------------------------------------------------------------------------
setup_logging()
log = logging.getLogger("api-gateway")

app = FastAPI(title="SelfTrading Analytics API", version="1.0.0")

origins = [o.strip() for o in (os.getenv("API_CORS_ORIGINS", "*") or "*").split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PACE_FILE = "/tmp/sim_auto_advance.json"
HEARTBEAT_FILE = "/tmp/sim_scheduler.heartbeat"

ANALYTICS_USER = os.getenv("ANALYTICS_USERNAME", "analytics")
ANALYTICS_EMAIL = os.getenv("ANALYTICS_EMAIL", "analytics@example.com")
ANALYTICS_PASSWORD = os.getenv("ANALYTICS_PASSWORD", "analytics")

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _read_heartbeat() -> Optional[str]:
    try:
        if os.path.exists(HEARTBEAT_FILE):
            with open(HEARTBEAT_FILE, "r") as f:
                return f.read().strip()
    except Exception:
        pass
    return None

def _write_pace(enabled: bool, pace_seconds: Optional[float]) -> None:
    try:
        payload = {"enabled": bool(enabled)}
        if pace_seconds is not None:
            payload["pace_seconds"] = float(pace_seconds)
        with open(PACE_FILE, "w") as f:
            json.dump(payload, f)
    except Exception:
        log.exception("Failed to write pace file")

def _analytics_user_id(db: DBManager) -> int:
    u = db.get_or_create_user(ANALYTICS_USER, ANALYTICS_EMAIL, ANALYTICS_PASSWORD)
    return int(u.id)

def _ensure_state(db: DBManager, uid: int) -> SimulationState:
    st = db.ensure_simulation_state(user_id=uid)
    return st

def _weighted_pct(pnl_amount: float, cost_basis: float) -> float:
    if cost_basis == 0:
        return 0.0
    return (pnl_amount / cost_basis) * 100.0

def _tail_file(path: str, max_lines: int) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            return [ln.rstrip("\n") for ln in lines[-max_lines:]]
    except FileNotFoundError:
        return []
    except Exception:
        log.exception("Failed to read log file %s", path)
        return []

# --------------------------------------------------------------------------------------
# Pydantic schemas (requests / responses)
# --------------------------------------------------------------------------------------
class StartSimRequest(BaseModel):
    pace_seconds: Optional[float] = Field(default=None, description="Delay between ticks; 0 or null = full speed")

class StopSimRequest(BaseModel):
    disable_auto_advance: bool = Field(default=True)

class ResetRequest(BaseModel):
    hard: bool = Field(default=True, description="If true, also resets SimulationState.last_ts to NULL")
    reset_account: bool = Field(default=True, description="Reset mock Account cash/equity to starting cash")
    truncate_logs: bool = Field(default=False, description="Truncate analytics logs (errors_warnings, health gate)")
    clear_runner_executions: bool = Field(default=True)
    clear_executed_trades: bool = Field(default=True)
    clear_orders: bool = Field(default=True)
    clear_open_positions: bool = Field(default=True)
    clear_analytics_results: bool = Field(default=True)

class StatusResponse(BaseModel):
    is_running: bool
    last_ts: Optional[str]
    heartbeat_iso: Optional[str]
    auto_start: bool
    pace_seconds: Optional[float]

class ResultsResponse(BaseModel):
    as_of: str
    realized: Dict[str, Any]
    unrealized: Dict[str, Any]
    combined: Dict[str, Any]
    best_stocks: List[Dict[str, Any]]

class WarnsResponse(BaseModel):
    errors_warnings: List[str]
    health_gate: List[str]

# --------------------------------------------------------------------------------------
# Startup: DB readiness, light migrations, ensure analytics user/state, respect SIM_AUTO_START
# --------------------------------------------------------------------------------------
@app.on_event("startup")
def on_startup() -> None:
    wait_for_db_ready()
    try:
        # best-effort small schema tweaks shared in your codebase
        from backend.database.init_db import _apply_light_migrations
        _apply_light_migrations()
    except Exception:
        log.exception("Light migrations at API startup failed")

    with DBManager() as db:
        uid = _analytics_user_id(db)
        st = _ensure_state(db, uid)
        want_auto = (os.getenv("SIM_AUTO_START", "0") == "1")
        prev = str(st.is_running).lower() in {"true", "1"}
        if want_auto != prev:
            st.is_running = "true" if want_auto else "false"
            db.db.commit()
        log.info("API startup: analytics user id=%s, auto_start=%s, is_running_now=%s", uid, want_auto, st.is_running)

# --------------------------------------------------------------------------------------
# SIM CONTROL
# --------------------------------------------------------------------------------------
@app.get("/sim/status", response_model=StatusResponse)
def get_status():
    with DBManager() as db:
        uid = _analytics_user_id(db)
        st = _ensure_state(db, uid)
        hb = _read_heartbeat()
        pace = None
        try:
            if os.path.exists(PACE_FILE):
                with open(PACE_FILE, "r") as f:
                    data = json.load(f)
                    pace = float(data.get("pace_seconds", 0.0))
        except Exception:
            pass
        return StatusResponse(
            is_running=str(st.is_running).lower() in {"true", "1"},
            last_ts=st.last_ts.isoformat() if st.last_ts else None,
            heartbeat_iso=hb,
            auto_start=(os.getenv("SIM_AUTO_START", "0") == "1"),
            pace_seconds=pace
        )

@app.post("/sim/start", response_model=StatusResponse)
def start_sim(req: StartSimRequest):
    with DBManager() as db:
        uid = _analytics_user_id(db)
        st = _ensure_state(db, uid)
        st.is_running = "true"
        db.db.commit()
        _write_pace(True, req.pace_seconds)
        hb = _read_heartbeat()
        return StatusResponse(
            is_running=True,
            last_ts=st.last_ts.isoformat() if st.last_ts else None,
            heartbeat_iso=hb,
            auto_start=(os.getenv("SIM_AUTO_START", "0") == "1"),
            pace_seconds=req.pace_seconds
        )

@app.post("/sim/stop", response_model=StatusResponse)
def stop_sim(req: StopSimRequest):
    with DBManager() as db:
        uid = _analytics_user_id(db)
        st = _ensure_state(db, uid)
        st.is_running = "false"
        db.db.commit()
        if req.disable_auto_advance:
            _write_pace(False, None)
        hb = _read_heartbeat()
        return StatusResponse(
            is_running=False,
            last_ts=st.last_ts.isoformat() if st.last_ts else None,
            heartbeat_iso=hb,
            auto_start=(os.getenv("SIM_AUTO_START", "0") == "1"),
            pace_seconds=None
        )

# --------------------------------------------------------------------------------------
# RESET / PURGE (remove ALL simulation data; keep historical market data)
# --------------------------------------------------------------------------------------
@app.post("/sim/reset")
def reset_sim(req: ResetRequest):
    """
    Removes RunnerExecutions, ExecutedTrades, Orders, OpenPositions, AnalyticsResults for the analytics user,
    resets SimulationState (if hard), clears scheduler pace/heartbeat, optionally resets account balances,
    and (optionally) truncates warning/health logs. Historical bars are untouched.
    """
    deleted = {
        "runner_executions": 0,
        "executed_trades": 0,
        "orders": 0,
        "open_positions": 0,
        "analytics_results": 0,
    }
    with DBManager() as db:
        uid = _analytics_user_id(db)
        # Make sure state exists
        st = _ensure_state(db, uid)

        # Hard stop + optional last_ts reset
        st.is_running = "false"
        if req.hard:
            st.last_ts = None
        db.db.commit()

        # Clear pace/heartbeat
        try:
            if os.path.exists(PACE_FILE):
                os.remove(PACE_FILE)
        except Exception:
            pass
        try:
            if os.path.exists(HEARTBEAT_FILE):
                os.remove(HEARTBEAT_FILE)
        except Exception:
            pass

        # Purge rows (scoped to user)
        if req.clear_runner_executions:
            res = db.db.execute(text("DELETE FROM runner_executions WHERE user_id=:u"), {"u": uid})
            deleted["runner_executions"] = getattr(res, "rowcount", 0) or 0
        if req.clear_executed_trades:
            res = db.db.execute(text("DELETE FROM executed_trades WHERE user_id=:u"), {"u": uid})
            deleted["executed_trades"] = getattr(res, "rowcount", 0) or 0
        if req.clear_orders:
            res = db.db.execute(text("DELETE FROM orders WHERE user_id=:u"), {"u": uid})
            deleted["orders"] = getattr(res, "rowcount", 0) or 0
        if req.clear_open_positions:
            res = db.db.execute(text("DELETE FROM open_positions WHERE user_id=:u"), {"u": uid})
            deleted["open_positions"] = getattr(res, "rowcount", 0) or 0
        if req.clear_analytics_results:
            res = db.db.execute(text("DELETE FROM analytics_results"))
            deleted["analytics_results"] = getattr(res, "rowcount", 0) or 0

        db.db.commit()

        # Reset account (cash/equity) if requested
        if req.reset_account:
            acct = db.ensure_account(user_id=uid, name="mock")
            try:
                starting_cash = float(os.getenv("MOCK_STARTING_CASH", "10000000"))
                acct.cash = starting_cash
                acct.equity = starting_cash
                db.db.commit()
            except Exception:
                db.db.rollback()
                log.exception("Failed to reset account balances")

    # Optional: truncate logs
    if req.truncate_logs:
        try:
            lg_dir = os.getenv("LOG_DIR", "/root/projects/SelfTrading Analytics/logs")
            for fname in ("errors_warnings.log", "runner_health_gate.log"):
                p = os.path.join(lg_dir, fname)
                with open(p, "w", encoding="utf-8"):
                    pass
        except Exception:
            log.exception("Failed to truncate logs")

    return {"ok": True, "deleted": deleted}

# --------------------------------------------------------------------------------------
# RESULTS (works for partial runs too — includes UNREALIZED P&L on open positions)
# --------------------------------------------------------------------------------------
def _fetch_realized(session: Session, uid: int) -> Dict[str, Any]:
    # per-year (weighted %)
    year_rows = session.execute(
        select(
            func.extract("year", ExecutedTrade.sell_ts).label("yr"),
            func.sum(ExecutedTrade.pnl_amount).label("pnl"),
            func.sum(ExecutedTrade.buy_price * ExecutedTrade.quantity).label("cost"),
            func.count().label("trades"),
        ).where(ExecutedTrade.user_id == uid).group_by("yr").order_by("yr")
    ).all()
    by_year = []
    for r in year_rows:
        m = r._mapping
        pnl = float(m["pnl"] or 0.0)
        cost = float(m["cost"] or 0.0)
        by_year.append({
            "year": int(m["yr"]) if m["yr"] is not None else None,
            "trades": int(m["trades"] or 0),
            "pnl_amount": pnl,
            "pnl_pct": _weighted_pct(pnl, cost),
        })

    # per-timeframe
    tf_rows = session.execute(
        select(
            ExecutedTrade.timeframe,
            func.sum(ExecutedTrade.pnl_amount).label("pnl"),
            func.sum(ExecutedTrade.buy_price * ExecutedTrade.quantity).label("cost"),
            func.count().label("trades"),
        ).where(ExecutedTrade.user_id == uid).group_by(ExecutedTrade.timeframe).order_by(ExecutedTrade.timeframe.asc())
    ).all()
    by_timeframe = []
    for r in tf_rows:
        m = r._mapping
        pnl = float(m["pnl"] or 0.0)
        cost = float(m["cost"] or 0.0)
        by_timeframe.append({
            "timeframe": str(m["timeframe"] or ""),
            "trades": int(m["trades"] or 0),
            "pnl_amount": pnl,
            "pnl_pct": _weighted_pct(pnl, cost),
        })

    # per-strategy
    strat_rows = session.execute(
        select(
            ExecutedTrade.strategy,
            func.sum(ExecutedTrade.pnl_amount).label("pnl"),
            func.sum(ExecutedTrade.buy_price * ExecutedTrade.quantity).label("cost"),
            func.count().label("trades"),
        ).where(ExecutedTrade.user_id == uid).group_by(ExecutedTrade.strategy).order_by(ExecutedTrade.strategy.asc())
    ).all()
    by_strategy = []
    for r in strat_rows:
        m = r._mapping
        pnl = float(m["pnl"] or 0.0)
        cost = float(m["cost"] or 0.0)
        by_strategy.append({
            "strategy": str(m["strategy"] or ""),
            "trades": int(m["trades"] or 0),
            "pnl_amount": pnl,
            "pnl_pct": _weighted_pct(pnl, cost),
        })

    # by year-month (optional, useful for charts)
    ym_rows = session.execute(
        select(
            func.to_char(ExecutedTrade.sell_ts, "YYYY-MM").label("ym"),
            func.sum(ExecutedTrade.pnl_amount).label("pnl"),
            func.sum(ExecutedTrade.buy_price * ExecutedTrade.quantity).label("cost"),
            func.count().label("trades"),
        ).where(ExecutedTrade.user_id == uid).group_by("ym").order_by("ym")
    ).all()
    by_year_month = []
    for r in ym_rows:
        m = r._mapping
        pnl = float(m["pnl"] or 0.0)
        cost = float(m["cost"] or 0.0)
        by_year_month.append({
            "bucket": m["ym"],
            "trades": int(m["trades"] or 0),
            "pnl_amount": pnl,
            "pnl_pct": _weighted_pct(pnl, cost),
        })

    return {
        "by_year": by_year,
        "by_year_month": by_year_month,
        "by_timeframe": by_timeframe,
        "by_strategy": by_strategy,
    }

def _fetch_unrealized(session: Session, uid: int, as_of: datetime) -> Dict[str, Any]:
    """
    Compute unrealized P&L for open positions with mark-to-market using last close
    at the runner's timeframe.
    """
    pos_rows = (
        session.query(OpenPosition, Runner)
        .join(Runner, Runner.id == OpenPosition.runner_id)
        .filter(OpenPosition.user_id == uid)
        .all()
    )
    if not pos_rows:
        return {"total_positions": 0, "by_timeframe": [], "by_strategy": [], "by_symbol": []}

    # Group by timeframe/strategy/symbol and pull last prices in bulk
    by_tf: Dict[int, List[Dict[str, Any]]] = {}
    by_strategy: Dict[str, List[Dict[str, Any]]] = {}
    by_symbol_rows: List[Dict[str, Any]] = []

    # Prepare symbol->tf map for bulk fetch per tf
    tf_to_syms: Dict[int, List[str]] = {}
    meta: List[Dict[str, Any]] = []
    for (pos, runner) in pos_rows:
        tf = int(runner.time_frame or 5)
        s = (pos.symbol or "").upper()
        tf_to_syms.setdefault(tf, []).append(s)
        meta.append({
            "symbol": s,
            "timeframe": tf,
            "strategy": (runner.strategy or ""),
            "quantity": float(pos.quantity or 0.0),
            "avg_price": float(pos.avg_price or 0.0),
        })

    mkt = MarketDataManager()
    last_prices: Dict[tuple, float] = {}
    for tf, syms in tf_to_syms.items():
        prices = mkt.get_last_close_for_symbols(list(set(syms)), minutes=tf, as_of=as_of, regular_hours_only=(tf < 1440))
        for s, px in prices.items():
            last_prices[(s, tf)] = float(px)

    # Compute entries
    for row in meta:
        s, tf, strat = row["symbol"], row["timeframe"], row["strategy"]
        qty, avg = row["quantity"], row["avg_price"]
        last = last_prices.get((s, tf))
        if last is None or avg <= 0 or qty <= 0:
            continue
        pnl_amt = (last - avg) * qty
        pnl_pct = ((last / avg) - 1.0) * 100.0
        cost = avg * qty

        by_symbol_rows.append({
            "symbol": s,
            "timeframe": tf,
            "strategy": strat,
            "qty": qty,
            "avg_price": avg,
            "last_price": last,
            "pnl_amount": pnl_amt,
            "pnl_pct": pnl_pct,
        })

        by_tf.setdefault(tf, []).append({"pnl": pnl_amt, "cost": cost})
        by_strategy.setdefault(strat, []).append({"pnl": pnl_amt, "cost": cost})

    # Aggregate
    agg_tf = []
    for tf, items in sorted(by_tf.items(), key=lambda kv: kv[0]):
        pnl = sum(i["pnl"] for i in items)
        cost = sum(i["cost"] for i in items)
        agg_tf.append({"timeframe": str(tf), "pnl_amount": pnl, "pnl_pct": _weighted_pct(pnl, cost)})

    agg_strat = []
    for strat, items in sorted(by_strategy.items(), key=lambda kv: kv[0]):
        pnl = sum(i["pnl"] for i in items)
        cost = sum(i["cost"] for i in items)
        agg_strat.append({"strategy": strat, "pnl_amount": pnl, "pnl_pct": _weighted_pct(pnl, cost)})

    return {
        "total_positions": len(by_symbol_rows),
        "by_timeframe": agg_tf,
        "by_strategy": agg_strat,
        "by_symbol": by_symbol_rows,
    }

def _best_stocks(session: Session, uid: int, top_n: int = 25) -> List[Dict[str, Any]]:
    rows = session.execute(
        select(
            ExecutedTrade.symbol,
            ExecutedTrade.timeframe,
            ExecutedTrade.strategy,
            func.count().label("trades"),
            func.sum(ExecutedTrade.pnl_amount).label("pnl"),
            func.sum(ExecutedTrade.buy_price * ExecutedTrade.quantity).label("cost"),
        ).where(ExecutedTrade.user_id == uid)
         .group_by(ExecutedTrade.symbol, ExecutedTrade.timeframe, ExecutedTrade.strategy)
         .order_by(func.sum(ExecutedTrade.pnl_amount).desc())
         .limit(top_n * 3)  # overshoot then filter by pct below
    ).all()

    scored: List[Dict[str, Any]] = []
    for r in rows:
        m = r._mapping
        pnl = float(m["pnl"] or 0.0)
        cost = float(m["cost"] or 0.0)
        pct = _weighted_pct(pnl, cost)
        scored.append({
            "symbol": str(m["symbol"] or ""),
            "timeframe": str(m["timeframe"] or ""),
            "strategy": str(m["strategy"] or ""),
            "trades": int(m["trades"] or 0),
            "pnl_amount": pnl,
            "pnl_pct": pct,
        })
    # Sort by weighted % first, then amount
    scored.sort(key=lambda x: (x["pnl_pct"], x["pnl_amount"]), reverse=True)
    return scored[:top_n]

@app.get("/results", response_model=ResultsResponse)
def get_results(top_n: int = Query(25, ge=1, le=200)):
    """
    Returns:
    - realized: % P&L by year/time (year+year-month), timeframe, strategy (weighted by cost basis)
    - unrealized: open P&L aggregates (strategy/timeframe) + per-symbol rows
    - combined: realized + unrealized aggregates, same buckets where sensible
    - best_stocks: table of top symbols by weighted % P&L with strategy & timeframe
    """
    as_of = _now_utc()
    with DBManager() as db:
        uid = _analytics_user_id(db)
        realized = _fetch_realized(db.db, uid)
        unrealized = _fetch_unrealized(db.db, uid, as_of)

        # Combine timeframe buckets
        combo_tf: Dict[str, Dict[str, float]] = {}
        for r in realized["by_timeframe"]:
            combo_tf[str(r["timeframe"])] = {"pnl": float(r["pnl_amount"]), "cost_pct": r["pnl_pct"]}
        for u in unrealized["by_timeframe"]:
            key = str(u["timeframe"])
            combo_tf.setdefault(key, {"pnl": 0.0, "cost_pct": 0.0})
            combo_tf[key]["pnl"] += float(u["pnl_amount"])

        combined_by_timeframe = [
            {"timeframe": k, "pnl_amount": v["pnl"]} for k, v in sorted(combo_tf.items(), key=lambda kv: kv[0])
        ]

        # Combine strategy buckets
        combo_strat: Dict[str, float] = {}
        for r in realized["by_strategy"]:
            combo_strat[str(r["strategy"])] = float(r["pnl_amount"])
        for u in unrealized["by_strategy"]:
            combo_strat[str(u["strategy"])] = combo_strat.get(str(u["strategy"]), 0.0) + float(u["pnl_amount"])

        combined_by_strategy = [{"strategy": k, "pnl_amount": v} for k, v in sorted(combo_strat.items(), key=lambda kv: kv[0])]

        best = _best_stocks(db.db, uid, top_n=top_n)

        return ResultsResponse(
            as_of=as_of.isoformat(),
            realized=realized,
            unrealized=unrealized,
            combined={
                "by_timeframe": combined_by_timeframe,
                "by_strategy": combined_by_strategy,
            },
            best_stocks=best
        )

# --------------------------------------------------------------------------------------
# WARNINGS & ERRORS (log surfacing)
# --------------------------------------------------------------------------------------
@app.get("/warns", response_model=WarnsResponse)
def get_warns(max_lines: int = Query(200, ge=1, le=2000)):
    """
    Returns the last N lines from:
      - errors_warnings.log (root WARNING+)
      - runner_health_gate.log (exclusion/coverage diagnostics)
    """
    log_dir = os.getenv("LOG_DIR", "/root/projects/SelfTrading Analytics/logs")
    ew = _tail_file(os.path.join(log_dir, "errors_warnings.log"), max_lines)
    hg = _tail_file(os.path.join(log_dir, "runner_health_gate.log"), max_lines)
    return WarnsResponse(errors_warnings=ew, health_gate=hg)

# --------------------------------------------------------------------------------------
# ROOT
# --------------------------------------------------------------------------------------
@app.get("/")
def root():
    return {"service": "analytics-api", "ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
