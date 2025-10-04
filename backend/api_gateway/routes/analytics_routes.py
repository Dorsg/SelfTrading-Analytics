# FILE: backend/api_gateway/routes/analytics_routes.py
# DESCRIPTION: Minimal analytics API for server-side simulation

from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func, text, desc
import os
import logging
from fastapi.responses import Response

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


def _ensure_runners_if_needed(users_ct: int, runners_ct: int) -> int:
    """Idempotently create runners once bars exist. Returns runners count after ensure.
    Uses a file marker to avoid repeating heavy work on each call."""
    logger = logging.getLogger("api-gateway")
    try:
        marker = os.getenv("RUNNERS_MARKER", "/app/data/.runners_seeded")
        # If runners already exist, do not early-return; we allow backfill of newly added strategies/timeframes.
        # We still refresh marker for observability and continue to backfill missing combos.
        try:
            os.makedirs(os.path.dirname(marker), exist_ok=True)
            with open(marker, "w") as f:
                f.write("seeded")
        except Exception:
            pass
        if users_ct <= 0:
            return runners_ct
        # Previously this function was one-shot. We now always attempt idempotent backfill when invoked.
        # Proceed to create
        from database.models import HistoricalDailyBar
        from database.db_manager import DBManager
        with engine.connect() as conn:
            syms = [r[0] for r in conn.execute(select(HistoricalDailyBar.symbol).distinct()).fetchall()]
        if not syms:
            logger.info("ensure_runners: no symbols yet; will retry later")
            return runners_ct
        try:
            limit = int(os.getenv("SIM_SYMBOL_LIMIT", "0") or "0")
        except Exception:
            limit = 0
        if limit and len(syms) > limit:
            syms = syms[:limit]
        # Prefer dynamic discovery to keep in sync with available modules
        try:
            from backend.strategies.factory import list_available_strategy_keys as _list_strats
            strategies = _list_strats()
        except Exception:
            # Fallback to a static list if discovery fails
            strategies = [
                "chatgpt_5_strategy",
                "chatgpt_5_ultra_strategy",
                "grok_4_strategy",
                "gemini_2_5_pro_strategy",
                "claude_4_5_sonnet_strategy",
                "deepseek_v3_1_strategy",
            ]
        timeframes = [5, 1440]
        created = 0
        from database.models import Runner as RunnerModel
        with DBManager() as db:
            user = db.get_or_create_user("analytics", "analytics@example.com", "analytics")
            for sym in syms:
                for strat in strategies:
                    for tf in timeframes:
                        try:
                            exists = (
                                db.db.query(RunnerModel)
                                .filter(
                                    RunnerModel.user_id == user.id,
                                    RunnerModel.stock == sym,
                                    RunnerModel.strategy == strat,
                                    RunnerModel.time_frame == tf,
                                )
                                .first()
                            )
                            if exists:
                                continue
                            r = RunnerModel(
                                user_id=user.id,
                                name=f"{sym}-{strat}-{('5m' if tf==5 else '1d')}",
                                strategy=strat,
                                budget=float(os.getenv("SIM_START_CASH", "10000000")) * 10,
                                current_budget=0.0,
                                stock=sym,
                                time_frame=tf,
                                parameters={},
                                exit_strategy="hold_forever",
                                activation="active",
                            )
                            db.db.add(r)
                            created += 1
                        except Exception:
                            try:
                                db.db.rollback()
                            except Exception:
                                pass
                            continue
            try:
                if created:
                    db.db.commit()
            except Exception:
                try:
                    db.db.rollback()
                except Exception:
                    pass

        # Mark success (even if created==0, we attempted once; next calls will recount)
        try:
            os.makedirs(os.path.dirname(marker), exist_ok=True)
            with open(marker, "w") as f:
                f.write(f"created={created}")
        except Exception:
            pass
        # recount
        try:
            with DBManager() as db:
                return int(db.count_runners())
        except Exception:
            return runners_ct
    except Exception:
        logger.exception("ensure_runners failed")
        return runners_ct


@router.get("/database/status")
def get_database_status() -> dict:
    logger = logging.getLogger("api-gateway")
    daily = 0
    minute = 0
    start = None
    end = None
    users = 0
    runners = 0

    # DB counters (resilient)
    try:
        with engine.connect() as conn:
            daily = int(conn.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0)
            minute = int(conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0)
            start = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
            end = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
    except Exception:
        logger.debug("database/status: failed to read bar counters", exc_info=True)

    # Setup counts (resilient)
    try:
        with DBManager() as db:
            users = int(db.count_users())
            runners = int(db.count_runners())
    except Exception:
        logger.debug("database/status: failed to read setup counts", exc_info=True)

    ready = (daily > 0 and minute > 0 and users > 0 and runners > 0)

    return {
        "status": "ready" if ready else ("importing" if (daily > 0 or minute > 0) else "checking"),
        "data": {"daily_bars": daily, "minute_bars": minute,
                 "date_range": {"start": start.isoformat() if start else None,
                                "end": end.isoformat() if end else None}},
        "setup": {"users": users, "runners": runners},
        "ready": ready
    }

@router.post("/runners/backfill")
def backfill_runners() -> dict:
    """Idempotently ensure missing runners for all discovered strategies/timeframes.

    Returns the final runners count and whether a backfill was attempted.
    """
    logger = logging.getLogger("api-gateway")
    try:
        with DBManager() as db:
            users_ct = int(db.count_users())
            runners_ct_before = int(db.count_runners())
        final_ct = _ensure_runners_if_needed(users_ct, runners_ct_before)
        return {"ok": True, "runners_before": runners_ct_before, "runners_after": int(final_ct)}
    except Exception as e:
        logger.exception("runners/backfill failed")
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/simulation/start")
def start_simulation() -> dict:
    logger = logging.getLogger("api-gateway")
    logger.info("Received request: analytics.simulation.start")
    # Lightweight in-memory debounce to avoid rapid duplicate starts.
    if not hasattr(start_simulation, "_last_called"):
        start_simulation._last_called = 0
    try:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if now_ts - int(getattr(start_simulation, "_last_called", 0)) < 2:
            logger.info("start_simulation: debounced duplicate call")
            with DBManager() as db:
                user = db.get_user_by_username("analytics")
                st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None
                running = str(st.is_running).lower() in {"true", "1"} if st else False
                return {"running": running, "last_ts": st.last_ts.isoformat() if st and st.last_ts else None, "message": "debounced"}
        start_simulation._last_called = now_ts
    except Exception:
        pass

    # HARD GUARD: do not allow starting until import/setup is fully ready (3/3 checks)
    try:
        with engine.connect() as conn:
            daily_ct = int(conn.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0)
            minute_ct = int(conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0)
        with DBManager() as db:
            users_ct = int(db.count_users())
            runners_ct = int(db.count_runners())
        gate_daily = daily_ct > 0
        gate_minute = minute_ct > 0
        gate_setup = (users_ct > 0 and runners_ct > 0)
        gates_done = int(gate_daily) + int(gate_minute) + int(gate_setup)
        if gates_done < 3:
            logger.warning(
                "start_simulation blocked: import/setup not ready (gates=%d/3 daily=%d minute=%d users=%d runners=%d)",
                gates_done, daily_ct, minute_ct, users_ct, runners_ct,
            )
            raise HTTPException(status_code=409, detail={
                "error": "import_not_ready",
                "message": f"Import/setup incomplete ({gates_done}/3 checks). Finish import before starting.",
                "checks": {"daily": gate_daily, "minute": gate_minute, "setup": gate_setup,
                           "daily_bars": daily_ct, "minute_bars": minute_ct, "users": users_ct, "runners": runners_ct},
            })
    except HTTPException:
        raise
    except Exception:
        logger.exception("start_simulation: readiness check failed")
        raise HTTPException(status_code=500, detail="readiness check failed")

    try:
        # Backfill any missing runners for newly added strategies/timeframes before starting
        try:
            _ensure_runners_if_needed(users_ct, runners_ct)
        except Exception:
            logger.exception("start_simulation: runner backfill failed; continuing with existing runners")

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
                # No state -> create and start
                st = SimulationState(
                    user_id=user.id,
                    is_running="true",
                    last_ts=datetime.fromtimestamp(desired_start_epoch, tz=timezone.utc),
                )
                db.db.add(st)
                db.db.commit()
                last_ts_epoch = desired_start_epoch
                started_now = True
            else:
                # Forward-only: never move last_ts backward
                existing_epoch = (
                    int((st.last_ts if st.last_ts.tzinfo else st.last_ts.replace(tzinfo=timezone.utc)).timestamp())
                    if st.last_ts else None
                )
                new_epoch = desired_start_epoch if existing_epoch is None else max(existing_epoch, desired_start_epoch)
                was_running = str(st.is_running).lower() in {"true", "1"}
                if was_running:
                    # Idempotent: already running -> return current state without mutating time
                    last_ts_epoch = existing_epoch if existing_epoch is not None else new_epoch
                    logger.info("start_simulation: already running for user=%s", user.id)
                    return {"running": True, "last_ts": datetime.fromtimestamp(last_ts_epoch, tz=timezone.utc).isoformat(), "message": "already running"}
                # transition to running
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


@router.get("/simulation/import/status")
def get_import_status() -> dict:
    """Return a robust import status for the UI.

    This endpoint is resilient and informative:
      - Reads a best-effort container marker written by the importer
      - Derives database readiness and basic progress from table counts
      - Provides helpful details so the client can render context
    """
    logger = logging.getLogger("api-gateway")
    try:
        marker_path = os.getenv("IMPORT_MARKER", "/app/data/.import_completed")

        # Read marker (best-effort)
        marker_state = {
            "exists": os.path.exists(marker_path),
            "path": marker_path,
            "text": None,
        }
        if marker_state["exists"]:
            try:
                with open(marker_path, "r", encoding="utf-8", errors="ignore") as f:
                    marker_state["text"] = f.read().strip()
            except Exception:
                marker_state["text"] = None

        # Read DB status/counters
        daily_ct = 0
        minute_ct = 0
        min_ts = None
        max_ts = None
        try:
            with engine.connect() as conn:
                daily_ct = int(conn.execute(select(func.count()).select_from(HistoricalDailyBar)).scalar() or 0)
                minute_ct = int(conn.execute(select(func.count()).select_from(HistoricalMinuteBar)).scalar() or 0)
                min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                # Provide rough expected totals for UI progress: bars per distinct symbol * days * (6.5h*60/5)
                # We expose None when unknown; UI treats as unbounded.
                try:
                    distinct_syms = int(conn.execute(text("select count(distinct symbol) from historical_daily_bars")).scalar() or 0)
                except Exception:
                    distinct_syms = 0
        except Exception:
            logger.debug("Import status: failed to read DB counters", exc_info=True)
            distinct_syms = 0

        users_ct = 0
        runners_ct = 0
        try:
            with DBManager() as db:
                users_ct = db.count_users()
                runners_ct = db.count_runners()
        except Exception:
            logger.debug("Import status: failed to read setup counts", exc_info=True)

        # If bars exist and user exists but no runners yet, try to create them once
        if (daily_ct > 0 or minute_ct > 0) and users_ct > 0 and runners_ct == 0:
            runners_ct = _ensure_runners_if_needed(int(users_ct), int(runners_ct))

        # Expected totals for UI
        expected = {
            "users_total": 1,
            "runners_total": int(max(0, distinct_syms)) * 2 * 2 if distinct_syms else None,  # 2 strategies x 2 TFs
            "minute_bars_total": None,  # unknown precisely; UI treats None as unbounded
            "daily_bars_total": None,
        }

        # Determine readiness and coarse progress based on three gates:
        # 1) Daily bars present, 2) Minute bars present, 3) Users & runners configured
        gate_daily = daily_ct > 0
        gate_minute = minute_ct > 0
        gate_setup = (users_ct > 0 and runners_ct > 0)
        gates_total = 3
        gates_done = int(gate_daily) + int(gate_minute) + int(gate_setup)

        ready = (gates_done == gates_total)

        # Prefer explicit marker state for informational purposes, but map to UI states
        if ready:
            state = "ready"
        else:
            # If the importer has created the marker but DB isn't fully ready,
            # we treat this as "importing" (or recently completed in another container)
            state = "importing" if marker_state["exists"] else "pending"

        progress_percent = int((gates_done / gates_total) * 100)

        details = {
            "daily_bars": daily_ct,
            "minute_bars": minute_ct,
            "users": users_ct,
            "runners": runners_ct,
            "date_range": {
                "start": min_ts.isoformat() if min_ts else None,
                "end": max_ts.isoformat() if max_ts else None,
            },
            "marker": marker_state,
            "checks_done": gates_done,
            "checks_total": gates_total,
            "expected": expected,
        }

        resp = {
            "state": state,
            "progress_percent": progress_percent,
            # expose a simple processed/total for clients that only show a ratio
            "processed": gates_done,
            "total": gates_total,
            "details": details,
        }

        logger.debug("Import status response: %s", resp)
        return resp
    except Exception as e:
        logger.exception("Failed to compute import status")
        return {"state": "unknown", "error": str(e)}


@router.get('/simulation/heartbeat')
def sim_heartbeat():
    """Simple Server-Sent Event (SSE) style heartbeat endpoint (returns JSON for now).

    Intended for lightweight polling or SSE upgrade later. Returns the scheduler
    heartbeat iso timestamp and simulation running flag.
    """
    logger = logging.getLogger('api-gateway')
    try:
        hb = None
        try:
            if os.path.exists('/tmp/sim_scheduler.heartbeat'):
                with open('/tmp/sim_scheduler.heartbeat', 'r') as f:
                    hb = f.read().strip()
        except Exception:
            pass
        with DBManager() as db:
            user = db.get_user_by_username('analytics')
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None
            running = str(st.is_running).lower() in {'true', '1'} if st else False
            return {'heartbeat_iso': hb, 'running': running}
    except Exception as e:
        logger.exception('sim_heartbeat failed')
        return {'heartbeat_iso': None, 'running': False, 'error': str(e)}


@router.get('/debug/script')
def debug_inject_script() -> Response:
    """Return a small JS overlay script that the user can eval in browser DevTools.

    Usage in browser Console:
      fetch('/api/analytics/debug/script').then(r=>r.text()).then(t=>eval(t))

    This is a safe, temporary dev helper to visualize /progress and inject values
    into the page when the frontend bundle is stale.
    """
    js = r'''(function(){
  if(window.__simPatchActive){ console.log('simPatch active'); return; }
  window.__simPatchActive = true;
  function elText(e){ return (e && e.textContent||'').trim(); }
  function findLabelElt(label){
    const els = Array.from(document.querySelectorAll('div,span,small,label'));
    return els.find(e=>{const t=elText(e); return t && (t.startsWith(label) || t.toLowerCase().includes(label.toLowerCase()))});
  }
  function writeValueNearLabel(label, value){
    try{
      const lab = findLabelElt(label); if(!lab) return false;
      let parent = lab.parentElement || lab;
      const candidates = parent.querySelectorAll('div,span');
      for(const c of candidates){ if(c === lab) continue; if((c.textContent||'').trim().length<=4){ c.textContent = value; return true; } }
      const s = document.createElement('span'); s.textContent = value; s.style.marginLeft='8px'; parent.appendChild(s); return true;
    }catch(e){return false;}
  }
  const overlay = document.createElement('div');
  overlay.id = '__simPatchOverlay';
  Object.assign(overlay.style,{position:'fixed',right:'12px',bottom:'12px',zIndex:2147483647,background:'#fff',color:'#111',border:'1px solid #ccc',padding:'10px',width:'360px',fontFamily:'Arial,Helvetica,sans-serif',fontSize:'13px',boxShadow:'0 6px 18px rgba(0,0,0,0.12)',borderRadius:'8px'});
  overlay.innerHTML = '<b>Sim Live Debug</b><div id="__simPatchBody" style="margin-top:8px">loading...</div><div style="margin-top:8px"><button id="__simPatchStop">Stop</button> <button id="__simPatchRefresh">Refresh</button></div>';
  document.body.appendChild(overlay);
  const body = document.getElementById('__simPatchBody');
  let running = true, timer = null;
  async function fetchOnce(){
    try{
      const pRes = await fetch('/api/analytics/progress?_t='+Date.now(),{cache:'no-store'}).catch(()=>null);
      const p = pRes ? await pRes.json().catch(()=>null) : null;
      const percent = p ? (p.progress_percent ?? (p.timeframes && p.timeframes['5m'] && p.timeframes['5m'].percent) ?? null) : null;
      const simt = p?.sim_time_iso ?? p?.sim_time_epoch ?? null;
      const buys = p?.total_buys ?? (p&&p.counters? p.counters.trades_all_time : 0) ?? 0;
      const sells = p?.total_sells ?? 0;
      const eta = p?.estimated_finish_iso ?? null;
      body.innerHTML = ['<div><b>progress_percent</b>: '+(percent===null?'n/a':(Math.round((+percent+Number.EPSILON)*100)/100)+'%')+'</div>','<div><b>sim_time</b>: '+(simt||'n/a')+'</div>','<div><b>total_buys</b>: '+buys+'</div>','<div><b>total_sells</b>: '+sells+'</div>','<div><b>ETA</b>: '+(eta||'—')+'</div>','<pre style="margin-top:8px;max-height:220px;overflow:auto;background:#f3f6f9;padding:6px;border-radius:4px">'+JSON.stringify(p||{},null,2)+'</pre>'].join('');
      if(percent!==null){ writeValueNearLabel('Progress', (Math.round((+percent+Number.EPSILON)*100)/100) + '%'); }
      if(simt) writeValueNearLabel('Last TS', simt) || writeValueNearLabel('sim_time', simt);
      writeValueNearLabel('Total Buys', String(buys)); writeValueNearLabel('Total Sells', String(sells)); if(eta) writeValueNearLabel('Time to finish', new Date(eta).toLocaleString());
    }catch(err){ body.innerText = 'fetch error: '+(err&&err.message?err.message:String(err)); }
  }
  fetchOnce(); timer = setInterval(()=>{ if(!running) return; fetchOnce(); }, 1000);
  document.getElementById('__simPatchStop').onclick = function(){ running=false; clearInterval(timer); const overlayEl=document.getElementById('__simPatchOverlay'); if(overlayEl) overlayEl.remove(); window.__simPatchActive=false; };
  document.getElementById('__simPatchRefresh').onclick = function(){ try{ sessionStorage.removeItem('analytics_db_status_cache'); }catch(e){}; fetchOnce(); };
})();'''
    return Response(js, media_type='text/javascript')


@router.post("/simulation/force-tick")
def force_tick() -> dict:
    """Force a single simulation tick by advancing SimulationState.last_ts by SIM_STEP_SECONDS.

    This is a best-effort helper for the UI to request immediate progress during testing.
    It does not execute runner logic; it only advances the clock and writes a small
    progress snapshot so the UI can reflect time advancement immediately.
    """
    logger = logging.getLogger("api-gateway")
    try:
        step_sec = int(os.getenv("SIM_STEP_SECONDS", "300"))
        snap_path = os.getenv("SIM_PROGRESS_SNAPSHOT", "/app/data/sim_last_progress.json")
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if not user:
                raise HTTPException(status_code=404, detail="analytics user not found")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
            if not st:
                st = SimulationState(user_id=user.id, is_running="false", last_ts=None)
                db.db.add(st)
            # Advance last_ts by step_sec (create if missing)
            from datetime import datetime, timezone
            if st.last_ts:
                cur_epoch = int((st.last_ts if st.last_ts.tzinfo else st.last_ts.replace(tzinfo=timezone.utc)).timestamp())
            else:
                cur_epoch = int(datetime.now(timezone.utc).timestamp())
            new_epoch = cur_epoch + step_sec
            st.last_ts = datetime.fromtimestamp(new_epoch, tz=timezone.utc)
            db.db.commit()

        # Try to write a lightweight snapshot so progress endpoint can return it immediately
        try:
            import json
            # Compute a naive percent across historical minute data if available
            pct = None
            try:
                with engine.connect() as conn:
                    from database.models import HistoricalMinuteBar
                    min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                    max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                    if min_ts and max_ts:
                        min_epoch = int((min_ts if min_ts.tzinfo else min_ts.replace(tzinfo=timezone.utc)).timestamp())
                        max_epoch = int((max_ts if max_ts.tzinfo else max_ts.replace(tzinfo=timezone.utc)).timestamp())
                        total = max(1, max_epoch - min_epoch)
                        done = max(0, new_epoch - min_epoch)
                        pct = max(0.0, min(100.0, (done / total) * 100.0))
            except Exception:
                pct = None

            snap = {
                "sim_time_epoch": new_epoch,
                "sim_time_iso": st.last_ts.isoformat(),
                "timeframes": {"5m": {"ticks_done": 0, "ticks_total": 0, "percent": pct or 0.0}},
                "counters": {"executions_all_time": 0, "trades_all_time": 0},
                "progress_percent": pct or 0.0,
                "state": str(st.is_running)
            }
            tmp = f"{snap_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snap, f)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            try:
                os.replace(tmp, snap_path)
            except Exception:
                try:
                    os.rename(tmp, snap_path)
                except Exception:
                    logger.exception("Failed to write force-tick snapshot")
        except Exception:
            logger.exception("Failed to write snapshot after force-tick")

        return {"ok": True, "last_ts": st.last_ts.isoformat(), "progress_percent": pct}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("force_tick failed")
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


@router.post("/simulation/reset")
def api_reset_simulation() -> dict:
    """Reset execution data and simulation state (preserve imported bars)."""
    logger = logging.getLogger("api-gateway")
    try:
        deleted = {"runner_executions": 0, "executed_trades": 0, "orders": 0, "open_positions": 0, "analytics_results": 0}
        with DBManager() as db:
            user = db.get_or_create_user("analytics", "analytics@example.com", "analytics")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first()
            if not st:
                st = SimulationState(user_id=user.id, is_running="false", last_ts=None)
                db.db.add(st)
            else:
                st.is_running = "false"
                st.last_ts = None

            # Bulk delete execution artifacts (user-scoped where applicable)
            try:
                from sqlalchemy import text as sqltext
                res = db.db.execute(sqltext("DELETE FROM runner_executions WHERE user_id=:u"), {"u": user.id}); deleted["runner_executions"] = getattr(res, "rowcount", 0) or 0
                res = db.db.execute(sqltext("DELETE FROM executed_trades WHERE user_id=:u"), {"u": user.id}); deleted["executed_trades"] = getattr(res, "rowcount", 0) or 0
                res = db.db.execute(sqltext("DELETE FROM orders WHERE user_id=:u"), {"u": user.id}); deleted["orders"] = getattr(res, "rowcount", 0) or 0
                res = db.db.execute(sqltext("DELETE FROM open_positions WHERE user_id=:u"), {"u": user.id}); deleted["open_positions"] = getattr(res, "rowcount", 0) or 0
                # analytics_results not user-scoped
                res = db.db.execute(sqltext("DELETE FROM analytics_results")); deleted["analytics_results"] = getattr(res, "rowcount", 0) or 0
            except Exception:
                logger.exception("api_reset_simulation: delete operations failed")
                db.db.rollback()
                raise

            db.db.commit()

        # Remove snapshot and pace toggle
        snap_path = os.getenv("SIM_PROGRESS_SNAPSHOT", "/app/data/sim_last_progress.json")
        try:
            if os.path.exists(snap_path):
                os.remove(snap_path)
        except Exception:
            logger.debug("api_reset_simulation: failed to remove snapshot", exc_info=True)
        try:
            if os.path.exists("/tmp/sim_auto_advance.json"):
                os.remove("/tmp/sim_auto_advance.json")
        except Exception:
            pass

        return {"ok": True, "deleted": deleted}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("api_reset_simulation failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/simulation/state")
def get_simulation_state() -> dict:
    logger = logging.getLogger("api-gateway")
    logger.debug("get_simulation_state requested")
    try:
        snap_path = os.getenv("SIM_PROGRESS_SNAPSHOT", "/app/data/sim_last_progress.json")
        progress_percent = None
        snapshot_age = None
        try:
            if os.path.exists(snap_path):
                import json, time
                with open(snap_path, "r", encoding="utf-8", errors="ignore") as f:
                    data = json.load(f)
                    progress_percent = data.get("progress_percent") or data.get("progress")
                    # Prefer file modification time for snapshot age (reflects when scheduler wrote it).
                    try:
                        mtime = os.path.getmtime(snap_path)
                        snapshot_age = max(0, int(time.time() - mtime))
                    except Exception:
                        # Fallback: if snapshot contains a sim_time_epoch, compute relative age
                        if data.get("sim_time_epoch"):
                            try:
                                snapshot_age = max(0, int(time.time()) - int(data.get("sim_time_epoch")))
                            except Exception:
                                snapshot_age = None
                        else:
                            snapshot_age = None
        except Exception:
            logger.exception("Failed to read progress snapshot for simulation state")

        # Base simulation state
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None
        running = False
        last_ts = None
        if st:
            running = str(st.is_running).lower() in {"true", "1"}
            last_ts = st.last_ts.isoformat() if st and st.last_ts else None

        resp = {"running": running, "last_ts": last_ts}

        # Prefer snapshot percent, else compute percent from DB bounds if available
        if progress_percent is not None:
            try:
                resp["progress_percent"] = float(progress_percent)
            except Exception:
                resp["progress_percent"] = progress_percent
        else:
            try:
                # try to compute from historical minute bounds and SimulationState.last_ts
                with engine.connect() as conn:
                    min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                    max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                if min_ts and max_ts and st and st.last_ts:
                    min_epoch = int((min_ts if min_ts.tzinfo else min_ts.replace(tzinfo=timezone.utc)).timestamp())
                    max_epoch = int((max_ts if max_ts.tzinfo else max_ts.replace(tzinfo=timezone.utc)).timestamp())
                    cur_epoch = int((st.last_ts if st.last_ts.tzinfo else st.last_ts.replace(tzinfo=timezone.utc)).timestamp())
                    total = max(1, max_epoch - min_epoch)
                    done = max(0, cur_epoch - min_epoch)
                    resp["progress_percent"] = max(0.0, min(100.0, (done / total) * 100.0))
            except Exception:
                logger.debug("Could not compute progress_percent from DB bounds")

        # Add snapshot_age if found
        if snapshot_age is not None:
            resp["snapshot_age_seconds"] = snapshot_age

        # Try to enrich response with totals (buys/sells) and ETA from the latest snapshot if available.
        try:
            if os.path.exists(snap_path):
                import json
                with open(snap_path, "r", encoding="utf-8", errors="ignore") as f:
                    snap = json.load(f)
                    # prefer explicit totals written by the scheduler
                    if isinstance(snap, dict):
                        if snap.get("total_buys") is not None:
                            resp["total_buys"] = int(snap.get("total_buys"))
                        if snap.get("total_sells") is not None:
                            resp["total_sells"] = int(snap.get("total_sells"))
                        if snap.get("estimated_finish_seconds") is not None:
                            resp["eta_seconds"] = int(snap.get("estimated_finish_seconds"))
                        if snap.get("estimated_finish_iso"):
                            resp["estimated_finish_iso"] = snap.get("estimated_finish_iso")
        except Exception:
            # ignore snapshot enrichment failures
            pass

        # Include counters (total buys/sells/executions) from DB as a fallback
        try:
            with engine.connect() as conn:
                total_exec = conn.execute(select(func.count()).select_from(RunnerExecution)).scalar() or 0
                total_trades = conn.execute(select(func.count()).select_from(ExecutedTrade)).scalar() or 0
                # Count buys/sells separately when possible
                try:
                    total_buys_db = conn.execute(select(func.count()).select_from(ExecutedTrade).where(ExecutedTrade.buy_ts != None)).scalar() or 0
                except Exception:
                    total_buys_db = 0
                try:
                    total_sells_db = conn.execute(select(func.count()).select_from(ExecutedTrade).where(ExecutedTrade.sell_ts != None)).scalar() or 0
                except Exception:
                    total_sells_db = 0
            resp["counters"] = {"executions_all_time": int(total_exec), "trades_all_time": int(total_trades)}
            # only set totals if not already set from snapshot enrichment
            if "total_buys" not in resp:
                resp["total_buys"] = int(total_buys_db)
            if "total_sells" not in resp:
                resp["total_sells"] = int(total_sells_db)
        except Exception:
            # ignore errors here
            pass

        # If ETA not provided by snapshot, try to compute a best-effort ETA using SIM_PACE_SECONDS
        try:
            if resp.get("eta_seconds") is None:
                # compute from DB bounds and SimulationState.last_ts if available
                try:
                    with engine.connect() as conn:
                        min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                        max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                    if min_ts and max_ts and st and st.last_ts:
                        min_epoch = int((min_ts if min_ts.tzinfo else min_ts.replace(tzinfo=timezone.utc)).timestamp())
                        max_epoch = int((max_ts if max_ts.tzinfo else max_ts.replace(tzinfo=timezone.utc)).timestamp())
                        cur_epoch = int((st.last_ts if st.last_ts.tzinfo else st.last_ts.replace(tzinfo=timezone.utc)).timestamp())
                        total = max(1, max_epoch - min_epoch)
                        done = max(0, cur_epoch - min_epoch)
                        remaining = max(0, total - done)
                        step_sec = int(os.getenv("SIM_STEP_SECONDS", "300"))
                        pace = float(os.getenv("SIM_PACE_SECONDS", "0"))
                        if pace and step_sec > 0 and remaining > 0:
                            remaining_ticks = remaining / step_sec
                            est_secs = int(remaining_ticks * pace)
                            resp["eta_seconds"] = est_secs
                            resp["estimated_finish_iso"] = datetime.fromtimestamp(cur_epoch + est_secs, tz=timezone.utc).isoformat()
                except Exception:
                    pass
        except Exception:
            pass

        return resp
    except Exception as e:
        logger.exception("Failed to read simulation state")
        return {"running": False, "last_ts": None, "error": str(e)}


@router.get("/progress")
def get_progress() -> dict:
    logger = logging.getLogger("api-gateway")
    logger.debug("get_progress requested")
    
    try:
        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            st = db.db.query(SimulationState).filter(SimulationState.user_id == user.id).first() if user else None
            if not st:
                return {"state": "idle", "progress_percent": 0}

            running = str(st.is_running).lower() in {"true", "1"}
            cur_ts = int(st.last_ts.timestamp()) if st.last_ts else None

            min_ts, max_ts = None, None
            min_daily, max_daily = None, None
            with engine.connect() as conn:
                min_ts = conn.execute(select(func.min(HistoricalMinuteBar.ts))).scalar()
                max_ts = conn.execute(select(func.max(HistoricalMinuteBar.ts))).scalar()
                # Daily bounds for per-timeframe progress (1d)
                try:
                    min_daily = conn.execute(select(func.min(HistoricalDailyBar.date))).scalar()
                    max_daily = conn.execute(select(func.max(HistoricalDailyBar.date))).scalar()
                except Exception:
                    min_daily = None
                    max_daily = None

            if not min_ts or not max_ts:
                return {"state": "running" if running else "idle", "progress_percent": 0, "sim_time_iso": st.last_ts.isoformat() if st.last_ts else None}

            start_epoch = int(min_ts.timestamp())
            end_epoch = int(max_ts.timestamp())
            
            pct = 0
            # Compute 5m timeframe progress using continuous 5m ticks across bounds
            step_sec = int(os.getenv("SIM_STEP_SECONDS", "300"))
            tf5m = {"ticks_done": 0, "ticks_total": 0, "percent": 0.0}
            if cur_ts and step_sec > 0:
                total_span = max(1, end_epoch - start_epoch)
                done_span = max(0, cur_ts - start_epoch)
                tf5m_total = max(0, int(total_span // step_sec))
                tf5m_done = min(tf5m_total, max(0, int(done_span // step_sec)))
                tf5m_pct = (tf5m_done / tf5m_total * 100.0) if tf5m_total > 0 else 0.0
                tf5m = {"ticks_done": tf5m_done, "ticks_total": tf5m_total, "percent": tf5m_pct}
                pct = max(0.0, min(100.0, tf5m_pct))

            # Compute 1d timeframe progress by distinct trading days present in DB
            tf1d = {"ticks_done": 0, "ticks_total": 0, "percent": 0.0}
            if min_daily and max_daily and cur_ts:
                try:
                    cur_day = datetime.fromtimestamp(cur_ts, tz=timezone.utc).date()
                    with engine.connect() as conn:
                        total_days = conn.execute(text("SELECT COUNT(DISTINCT date) FROM historical_daily_bars")).scalar() or 0
                        done_days = conn.execute(text("SELECT COUNT(DISTINCT date) FROM historical_daily_bars WHERE date <= :d"), {"d": cur_day}).scalar() or 0
                    done_days = int(done_days)
                    total_days = int(total_days)
                    tf1d_pct = (done_days / total_days * 100.0) if total_days > 0 else 0.0
                    tf1d = {"ticks_done": done_days, "ticks_total": total_days, "percent": tf1d_pct}
                except Exception:
                    pass

            # Per-timeframe buys/sells counters
            try:
                with engine.connect() as conn:
                    q = text("""
                        WITH tf AS (
                            SELECT
                                CASE
                                    WHEN timeframe IN ('1440','1440m','1d','day','1D') THEN '1d'
                                    WHEN timeframe IN ('5','5m','5min','5MIN') THEN '5m'
                                    ELSE NULL
                                END AS tf,
                                buy_ts, sell_ts
                            FROM executed_trades
                        )
                        SELECT
                            SUM(CASE WHEN tf='5m' AND buy_ts IS NOT NULL THEN 1 ELSE 0 END) AS buys_5m,
                            SUM(CASE WHEN tf='5m' AND sell_ts IS NOT NULL THEN 1 ELSE 0 END) AS sells_5m,
                            SUM(CASE WHEN tf='1d' AND buy_ts IS NOT NULL THEN 1 ELSE 0 END) AS buys_1d,
                            SUM(CASE WHEN tf='1d' AND sell_ts IS NOT NULL THEN 1 ELSE 0 END) AS sells_1d
                        FROM tf
                    """)
                    r = conn.execute(q).mappings().first()
                    if r:
                        try:
                            tf5m["total_buys"] = int(r.get("buys_5m") or 0)
                            tf5m["total_sells"] = int(r.get("sells_5m") or 0)
                        except Exception:
                            pass
                        try:
                            tf1d["total_buys"] = int(r.get("buys_1d") or 0)
                            tf1d["total_sells"] = int(r.get("sells_1d") or 0)
                        except Exception:
                            pass
            except Exception:
                pass

            resp = {
                "sim_time_iso": st.last_ts.isoformat() if st.last_ts else None,
                "sim_time_epoch": cur_ts,
                "progress_percent": pct,
                "state": "running" if running else "idle",
                "min_epoch": start_epoch,
                "max_epoch": end_epoch,
                "current_runner_info": { "timeframe": f"{int(os.getenv('SIM_STEP_SECONDS', '300')) // 60}m" },
                "timeframes": {"5m": tf5m, "1d": tf1d}
            }

            # Enrich with ETA from snapshot when available
            try:
                snap_path = os.getenv("SIM_PROGRESS_SNAPSHOT", "/app/data/sim_last_progress.json")
                if os.path.exists(snap_path):
                    import json
                    with open(snap_path, "r", encoding="utf-8", errors="ignore") as f:
                        snap = json.load(f)
                        if isinstance(snap, dict):
                            if snap.get("estimated_finish_iso"):
                                resp["estimated_finish_iso"] = snap.get("estimated_finish_iso")
                            if snap.get("estimated_finish_seconds") is not None:
                                resp["estimated_finish_seconds"] = int(snap.get("estimated_finish_seconds"))
                            if snap.get("estimated_finish"):
                                resp["estimated_finish"] = snap.get("estimated_finish")
            except Exception:
                pass

            # Fallback ETA based on pace if not present
            try:
                if resp.get("estimated_finish_iso") is None and running and cur_ts:
                    step_sec = int(os.getenv("SIM_STEP_SECONDS", "300"))
                    pace = float(os.getenv("SIM_PACE_SECONDS", "0"))
                    if pace and step_sec > 0:
                        remaining = max(0, end_epoch - cur_ts)
                        remaining_ticks = remaining / step_sec
                        est_secs = int(remaining_ticks * pace)
                        resp["estimated_finish_seconds"] = est_secs
                        resp["estimated_finish_iso"] = datetime.fromtimestamp(cur_ts + est_secs, tz=timezone.utc).isoformat()
            except Exception:
                pass
            
            try:
                with engine.connect() as conn:
                    resp["total_buys"] = int(conn.execute(select(func.count()).select_from(ExecutedTrade).where(ExecutedTrade.buy_ts != None)).scalar() or 0)
                    resp["total_sells"] = int(conn.execute(select(func.count()).select_from(ExecutedTrade).where(ExecutedTrade.sell_ts != None)).scalar() or 0)
            except Exception:
                pass
            
            return resp

    except Exception as e:
        logger.exception("get_progress calculation failed")
        return {"state": "error", "error": str(e)}


@router.get("/results")
def list_results(
    limit: int = Query(100, ge=1, le=1000),
    strategy: Optional[str] = None,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> list[dict]:
    with DBManager() as db:
        q = db.db.query(ExecutedTrade).filter(ExecutedTrade.sell_ts != None)
        if strategy:
            q = q.filter(ExecutedTrade.strategy == strategy)
        if symbol:
            q = q.filter(ExecutedTrade.symbol == symbol.upper())
        if timeframe:
            q = q.filter(ExecutedTrade.timeframe == timeframe)
        rows = q.order_by(desc(ExecutedTrade.sell_ts)).limit(limit).all()
        return [
            {
                "symbol": r.symbol,
                "strategy": r.strategy,
                "timeframe": r.timeframe,
                "start_ts": r.buy_ts,
                "end_ts": r.sell_ts,
                "pnl_amount": r.pnl,
                "pnl_percent": r.pnl_pct,
            }
            for r in rows
        ]


@router.get("/results/summary")
def get_results_summary() -> dict:
    """Computes P&L summaries directly from ExecutedTrade rows."""
    with engine.connect() as conn:
        # P&L by Year
        by_year_q = text("""
            SELECT
                EXTRACT(YEAR FROM sell_ts) AS year,
                COALESCE(SUM(pnl_amount), 0) / NULLIF(SUM(buy_price * quantity), 0) * 100 AS weighted_pct,
                AVG(COALESCE(pnl_amount, 0) / NULLIF(buy_price * quantity, 0) * 100) AS avg_pct,
                CAST(COUNT(*) AS INT) AS trades
            FROM executed_trades
            WHERE sell_ts IS NOT NULL AND buy_price > 0 AND quantity > 0
            GROUP BY year
            ORDER BY year DESC
        """)
        pnl_by_year = [{"bucket": r.year, "weighted_pct": r.weighted_pct, "avg_pct": r.avg_pct, "trades": int(r.trades or 0)} for r in conn.execute(by_year_q).mappings()]

        # P&L by Timeframe
        by_tf_q = text("""
            WITH tf AS (
                SELECT
                    CASE
                        WHEN timeframe IN ('1440','1440m','1d','day','1D') THEN '1d'
                        WHEN timeframe IN ('5','5m','5min','5MIN') THEN '5m'
                        ELSE NULL
                    END AS timeframe_bucket,
                    pnl_amount, buy_price, quantity, pnl_percent
                FROM executed_trades
                WHERE sell_ts IS NOT NULL AND buy_price > 0 AND quantity > 0
            )
            SELECT timeframe_bucket,
                   COALESCE(SUM(pnl_amount), 0) / NULLIF(SUM(buy_price * quantity), 0) * 100 AS weighted_pct,
                   AVG(COALESCE(pnl_amount, 0) / NULLIF(buy_price * quantity, 0) * 100) AS avg_pct,
                   CAST(COUNT(*) AS INT) AS trades
            FROM tf
            WHERE timeframe_bucket IN ('1d','5m')
            GROUP BY timeframe_bucket
            ORDER BY timeframe_bucket
        """)
        pnl_by_timeframe = [{"bucket": r.timeframe_bucket, "weighted_pct": r.weighted_pct, "avg_pct": r.avg_pct, "trades": int(r.trades or 0)} for r in conn.execute(by_tf_q).mappings()]

        # P&L by Strategy (extended with win rate and avg trade duration days)
        by_strat_q = text("""
            SELECT
                strategy,
                COALESCE(SUM(pnl_amount), 0) / NULLIF(SUM(buy_price * quantity), 0) * 100 AS weighted_pct,
                AVG(COALESCE(pnl_amount, 0) / NULLIF(buy_price * quantity, 0) * 100) AS avg_pct,
                CAST(COUNT(*) AS INT) AS trades,
                100.0 * SUM(CASE WHEN COALESCE(pnl_amount,0) > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS win_rate_pct,
                AVG(CASE WHEN buy_ts IS NOT NULL AND sell_ts IS NOT NULL THEN EXTRACT(EPOCH FROM (sell_ts - buy_ts)) ELSE NULL END) / 86400.0 AS avg_trade_days
            FROM executed_trades
            WHERE sell_ts IS NOT NULL
              AND buy_price > 0 AND quantity > 0
              AND strategy IS NOT NULL
              AND TRIM(LOWER(strategy)) NOT LIKE '%test%'
            GROUP BY strategy
            ORDER BY weighted_pct DESC
        """)
        pnl_by_strategy_raw = {}
        for r in conn.execute(by_strat_q).mappings():
            pnl_by_strategy_raw[r.strategy] = {
                "weighted_pct": float(r.weighted_pct or 0.0),
                "avg_pct": float(r.avg_pct or 0.0),
                "trades": int(r.trades or 0),
                "win_rate_pct": float(r.win_rate_pct or 0.0),
                "avg_trade_days": float(r.avg_trade_days or 0.0),
            }

        # Seed with all active strategies to ensure they appear even with 0 trades
        with DBManager() as db:
            active_runners = db.db.query(Runner.strategy).distinct().all()
            all_strategies = {name for (name,) in active_runners if name and ('test' not in name.lower())}

        pnl_by_strategy = []
        for strat in sorted(list(all_strategies)):
            data = pnl_by_strategy_raw.get(strat, {"weighted_pct": 0.0, "avg_pct": 0.0, "trades": 0, "win_rate_pct": 0.0, "avg_trade_days": 0.0})
            # Ensure JSON-safe numbers
            data = {
                "weighted_pct": float(data.get("weighted_pct", 0.0)),
                "avg_pct": float(data.get("avg_pct", 0.0)),
                "trades": int(data.get("trades", 0)),
                "win_rate_pct": float(data.get("win_rate_pct", 0.0)),
                "avg_trade_days": float(data.get("avg_trade_days", 0.0)),
            }
            pnl_by_strategy.append({"bucket": strat, **data})

        # P&L by Year/Strategy/Timeframe (for detailed view)
        by_yst_q = text("""
            WITH base AS (
                SELECT
                    EXTRACT(YEAR FROM sell_ts) AS year,
                    CASE
                        WHEN timeframe IN ('1440','1440m','1d','day','1D') THEN '1d'
                        WHEN timeframe IN ('5','5m','5min','5MIN') THEN '5m'
                        ELSE NULL
                    END AS tf,
                    strategy,
                    pnl_amount, buy_price, quantity,
                    buy_ts, sell_ts
                FROM executed_trades
                WHERE sell_ts IS NOT NULL AND buy_price > 0 AND quantity > 0
                  AND strategy IS NOT NULL
            )
            SELECT
                year,
                tf AS timeframe,
                strategy,
                COALESCE(SUM(pnl_amount), 0) / NULLIF(SUM(buy_price * quantity), 0) * 100 AS weighted_pct,
                AVG(COALESCE(pnl_amount, 0) / NULLIF(buy_price * quantity, 0) * 100) AS avg_pct,
                CAST(COUNT(*) AS INT) AS trades,
                AVG(CASE WHEN buy_ts IS NOT NULL AND sell_ts IS NOT NULL THEN EXTRACT(EPOCH FROM (sell_ts - buy_ts)) ELSE NULL END) / 86400.0 AS avg_trade_days
            FROM base
            WHERE tf IN ('1d','5m')
            GROUP BY year, strategy, tf
            ORDER BY year DESC, strategy ASC, tf ASC
        """)
        pnl_by_year_strategy_time = []
        for r in conn.execute(by_yst_q).mappings():
            tf = (r.timeframe or '').strip()
            tf_label = '5 minutes' if tf == '5m' else ('1 day' if tf == '1d' else tf)
            pnl_by_year_strategy_time.append({
                "year": int(r.year) if r.year is not None else None,
                "strategy": r.strategy,
                "timeframe": tf,
                "timeframe_label": tf_label,
                "weighted_pct": float(r.weighted_pct or 0.0),
                "avg_pct": float(r.avg_pct or 0.0),
                "trades": int(r.trades or 0),
                "avg_trade_days": float(r.avg_trade_days or 0.0),
            })


    return {
        "pnl_by_year": [
            {"bucket": x["bucket"], "weighted_pct": float(x["weighted_pct"] or 0.0), "avg_pct": float(x["avg_pct"] or 0.0), "trades": int(x["trades"] or 0)}
            for x in pnl_by_year
        ],
        "pnl_by_timeframe": [
            {"bucket": x["bucket"], "weighted_pct": float(x["weighted_pct"] or 0.0), "avg_pct": float(x["avg_pct"] or 0.0), "trades": int(x["trades"] or 0)}
            for x in pnl_by_timeframe
        ],
        "pnl_by_strategy": pnl_by_strategy,
        "pnl_by_year_strategy_time": pnl_by_year_strategy_time,
    }


@router.get("/results/top-stocks")
def get_top_stocks(limit: int = Query(20, ge=1, le=100)) -> list[dict]:
    """Computes best-performing stocks directly from ExecutedTrade rows."""
    q = text(f"""
        SELECT
            symbol AS stock,
            CASE
                WHEN timeframe IN ('1440','1440m','1d','day','1D') THEN '1d'
                WHEN timeframe IN ('5','5m','5min','5MIN') THEN '5m'
                ELSE NULL
            END AS timeframe,
            strategy,
            COALESCE(SUM(pnl_amount), 0) / NULLIF(SUM(buy_price * quantity), 0) * 100 AS weighted_pct,
            AVG(COALESCE(pnl_amount, 0) / NULLIF(buy_price * quantity, 0) * 100) AS avg_pct,
            CAST(COUNT(*) AS INT) AS trades,
            100.0 * SUM(CASE WHEN COALESCE(pnl_amount,0) > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS win_rate_pct,
            AVG(CASE WHEN buy_ts IS NOT NULL AND sell_ts IS NOT NULL THEN EXTRACT(EPOCH FROM (sell_ts - buy_ts)) ELSE NULL END) / 86400.0 AS avg_trade_days
        FROM executed_trades
        WHERE sell_ts IS NOT NULL
          AND buy_price > 0 AND quantity > 0
          AND (strategy IS NULL OR TRIM(LOWER(strategy)) NOT LIKE '%test%')
        GROUP BY stock, timeframe, strategy
        HAVING timeframe IN ('1d','5m')
        ORDER BY weighted_pct DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"limit": limit}).mappings()
        # Normalize rows to ensure decimals become floats and add safe defaults
        out = []
        for r in rows:
            m = dict(r)
            try:
                m["weighted_pct"] = float(m.get("weighted_pct") or 0.0)
            except Exception:
                m["weighted_pct"] = 0.0
            try:
                m["avg_pct"] = float(m.get("avg_pct") or 0.0)
            except Exception:
                m["avg_pct"] = 0.0
            try:
                m["trades"] = int(m.get("trades") or 0)
            except Exception:
                m["trades"] = 0
            try:
                m["win_rate_pct"] = float(m.get("win_rate_pct") or 0.0)
            except Exception:
                m["win_rate_pct"] = 0.0
            try:
                m["avg_trade_days"] = float(m.get("avg_trade_days") or 0.0)
            except Exception:
                m["avg_trade_days"] = 0.0
            out.append(m)
        return out


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
