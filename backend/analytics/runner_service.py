from __future__ import annotations

import asyncio
import os
import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Set

from database.db_manager import DBManager
from database.models import Runner, OpenPosition
from backend.ib_manager.market_data_manager import MarketDataManager
from backend.broker.mock_broker import MockBroker
from backend.strategies.runner_decision_info import RunnerDecisionInfo
from backend.strategies.factory import select_strategy, resolve_strategy_key
from backend.strategies.contracts import validate_decision
from backend.analytics.health_gate import HealthGate

log = logging.getLogger("runner-service")
kpi = logging.getLogger("analytics-kpi")


@dataclass(slots=True)
class _RunnerCtx:
    runner: Any
    position: Optional[OpenPosition]
    price: float
    candles: List[Dict[str, Any]]


@dataclass(slots=True)
class RunnerView:
    id: int
    user_id: int
    name: str
    strategy: str
    budget: float
    current_budget: float
    stock: str
    time_frame: int
    parameters: dict
    exit_strategy: str
    activation: str
    cooldown_until: Optional[datetime]


class RunnerService:
    """
    Executes one decision tick across active runners with:
      • Stop cross → exit (broker-managed; never skip).
      • Per-(symbol, timeframe) health gate (HEALTHY→DEGRADED→EXCLUDED) w/ TTL.
      • Strategy signals only on bar advance to avoid same-bar flip-flops.
      • Partial disable per timeframe: excluded pairs are skipped; others run.
      • Bulk UPSERT for runner_executions (cycle_seq, user_id, symbol, strategy, timeframe).
      • NEW: optional global same-bar guard with configurable scope:
            SAME_BAR_SCOPE = ["symbol_tf" | "symbol_tf_strategy"]
         When enabled, at most one successful BUY per (key, bar_ts) is allowed.
    """

    def __init__(self) -> None:
        self.mkt = MarketDataManager()
        self.broker = MockBroker()
        self._cache_seq: Optional[int] = None
        self._candle_cache: Dict[Tuple[str, int, int], List[Dict[str, Any]]] = {}

        self._log_no_action = os.getenv("SIM_LOG_NO_ACTION", "0") == "1"
        self._thin_no_action_details = os.getenv("SIM_THIN_NO_ACTION_DETAILS", "1") == "1"

        self._unit_budget_usd = float(os.getenv("SIM_RUNNER_UNIT_BUDGET", "2000"))
        self._min_cash_floor = float(os.getenv("SIM_MIN_CASH", "5000000"))
        self._topup_cash_to = float(os.getenv("SIM_TOPUP_CASH_TO", "10000000"))

        self._skip_stale_price = os.getenv("SIM_SKIP_STALE_PRICE", "1") == "1"
        self._require_bar_advance = os.getenv("SIM_REQUIRE_BAR_ADVANCE", "1") == "1"

        self._last_bar_ts: Dict[Tuple[int, int], datetime] = {}
        self._regular_hours_only = os.getenv("SIM_REGULAR_HOURS_ONLY", "1") == "1"
        self._warn_no_data_once: Set[Tuple[str, int, str]] = set()

        # Health gate (tunable via env)
        ttl_days = int(os.getenv("HEALTH_TTL_DAYS", "5"))
        deg = int(os.getenv("HEALTH_DEGRADE_THRESHOLD", "3"))
        exc = int(os.getenv("HEALTH_EXCLUDE_THRESHOLD", "10"))
        window = int(os.getenv("HEALTH_WINDOW_DAYS", "5"))
        self.health = HealthGate(ttl_days=ttl_days, degrade_threshold=deg, exclude_threshold_sessions=exc, window_days=window)

        # simulation bootstrap start (for coverage checks)
        self._sim_boot_start: Optional[datetime] = None

        # ── NEW: global same-bar BUY guard (per tick) ────────────────────────────
        # Scope:
        #   "symbol_tf"          → one BUY per (symbol,timeframe,bar_ts) across ALL strategies
        #   "symbol_tf_strategy" → one BUY per (symbol,timeframe,bar_ts,strategy)
        # Default relaxed to symbol_tf_strategy to avoid over-constraining buys across all strategies
        self._same_bar_scope: str = (os.getenv("SAME_BAR_SCOPE", "symbol_tf_strategy") or "symbol_tf_strategy").strip().lower()
        self._same_bar_seen_seq: Optional[int] = None  # resets each tick (cycle_seq)
        self._same_bar_seen: Set[str] = set()
        # Thread-based lock to coordinate same-bar guard across worker threads
        self._same_bar_thread_lock = threading.Lock()

        # ── Parallelism: thread pool for blocking runner work ───────────────────
        try:
            conc_default = max(1, int(os.getenv("SIM_RUNNER_CONCURRENCY", "0")))
        except Exception:
            conc_default = 0
        if conc_default <= 0:
            try:
                cpu = os.cpu_count() or 4
            except Exception:
                cpu = 4
            conc_default = max(2, min(32, cpu * 2))
        self._executor = ThreadPoolExecutor(max_workers=conc_default, thread_name_prefix="runner-worker")

        # ── Budgeting controls ──────────────────────────────────────────────────
        # Per-runner compounding budget (unit budget), with auto-reset when depleted
        try:
            self._budget_reset_fraction = float(os.getenv("SIM_BUDGET_RESET_FRACTION", "0.25"))
        except Exception:
            self._budget_reset_fraction = 0.25
        # Strategy-provided quantity is ignored by default in analytics sim
        self._allow_strategy_quantity = (os.getenv("SIM_ALLOW_STRATEGY_QUANTITY", "0") == "1")

    # ───────────────────────── internals ─────────────────────────
    def _get_candles_cached(
        self,
        symbol: str,
        interval_min: int,
        as_of: datetime,
        lookback: int = 300
    ) -> List[Dict[str, Any]]:
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())
        sym = symbol.upper()
        key = (sym, int(interval_min), seq)

        if self._cache_seq != seq:
            self._cache_seq = seq
            self._candle_cache.clear()

        if key in self._candle_cache:
            return self._candle_cache[key]

        candles = self.mkt.get_candles_until(
            sym,
            int(interval_min),
            as_of,
            lookback=lookback,
            regular_hours_only=self._regular_hours_only,
        )
        self._candle_cache[key] = candles
        return candles

    def _prefetch_candles_for_runners(self, runners: List[RunnerView], as_of: datetime) -> None:
        if not runners:
            return
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())

        self._cache_seq = seq
        self._candle_cache.clear()

        syms_5 = sorted({r.stock for r in runners if int(r.time_frame or 5) == 5})
        syms_1d = sorted({r.stock for r in runners if int(r.time_frame or 5) == 1440})

        if syms_5:
            data5 = self.mkt.get_candles_bulk_until(
                syms_5, 5, as_of, lookback=300, regular_hours_only=self._regular_hours_only
            )
            for s, candles in data5.items():
                self._candle_cache[(s, 5, seq)] = candles
        if syms_1d:
            data1d = self.mkt.get_candles_bulk_until(
                syms_1d, 1440, as_of, lookback=300, regular_hours_only=False
            )
            for s, candles in data1d.items():
                self._candle_cache[(s, 1440, seq)] = candles

        for s in syms_5:
            self._candle_cache.setdefault((s, 5, seq), [])
        for s in syms_1d:
            self._candle_cache.setdefault((s, 1440, seq), [])

    @staticmethod
    def _last_candle_ts(candles: List[Dict[str, Any]]) -> Optional[datetime]:
        if not candles:
            return None
        ts = candles[-1].get("ts")
        if ts is None:
            return None
        return (ts if getattr(ts, "tzinfo", None) else ts.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)

    @staticmethod
    def _is_stale_candle(last_ts: Optional[datetime], tf_min: int, as_of: datetime) -> bool:
        if last_ts is None:
            return True
        if last_ts.date() < as_of.date():
            return True
        age_sec = (as_of - last_ts).total_seconds()
        return age_sec > (tf_min * 60 + 1)

    def _decide(self, ctx: _RunnerCtx, strategy_obj=None, is_exit: Optional[bool] = None) -> dict:
        info = RunnerDecisionInfo(
            runner=ctx.runner,
            position=ctx.position,
            current_price=ctx.price,
            candles=ctx.candles,
            distance_from_time_limit=None,
        )
        strat = strategy_obj or select_strategy(ctx.runner)
        choose_exit = (is_exit if is_exit is not None else (ctx.position is not None))
        raw = strat.decide_sell(info) if choose_exit else strat.decide_buy(info)
        decision = validate_decision(raw, is_exit=ctx.position is not None) or {"action": "NO_ACTION"}

        # Inject a static stop at BUY if strategy didn't provide any stop
        if (decision.get("action") or "NO_ACTION").upper() == "BUY":
            has_trail = isinstance(decision.get("trail_stop_order"), dict)
            has_static = isinstance(decision.get("static_stop_order"), dict)
            if not has_trail and not has_static:
                try:
                    params = getattr(ctx.runner, "parameters", {}) or {}
                    sl_pct = float(params.get("default_stop_loss_percent", 0.0) or 0.0)
                except Exception:
                    sl_pct = 0.0
                if sl_pct > 0:
                    decision["static_stop_order"] = {
                        "action": "SELL",
                        "order_type": "STOP",
                        "stop_price": round(ctx.price * (1.0 - sl_pct / 100.0), 4),
                    }
        return decision

    def _qty_from_budget(self, db: DBManager, r: RunnerView, price: float) -> int:
        try:
            if price is None or price <= 0:
                return 0
            # Use runner's current compounding budget for trade sizing
            qty = int(r.current_budget // max(price, 0.01))
            return max(qty, 0)
        except Exception:
            return 0

    @staticmethod
    def _snapshot_runner(r: Runner) -> RunnerView:
        try:
            return RunnerView(
                id=int(getattr(r, "id")),
                user_id=int(getattr(r, "user_id")),
                name=str(getattr(r, "name", "") or ""),
                strategy=str(getattr(r, "strategy", "") or ""),
                budget=float(getattr(r, "budget", 0.0) or 0.0),
                current_budget=float(getattr(r, "current_budget", 0.0) or 0.0),
                stock=str(getattr(r, "stock", "UNKNOWN") or "UNKNOWN").upper(),
                time_frame=int(getattr(r, "time_frame", 5) or 5),
                parameters=dict(getattr(r, "parameters", {}) or {}),
                exit_strategy=str(getattr(r, "exit_strategy", "hold_forever") or "hold_forever"),
                activation=str(getattr(r, "activation", "active") or "active"),
                cooldown_until=getattr(r, "cooldown_until", None),
            )
        except Exception:
            return RunnerView(
                id=int(getattr(r, "id", 0) or 0),
                user_id=int(getattr(r, "user_id", 0) or 0),
                name=str(getattr(r, "name", "") or ""),
                strategy=str(getattr(r, "strategy", "") or ""),
                budget=float(getattr(r, "budget", 0.0) or 0.0),
                current_budget=float(getattr(r, "current_budget", 0.0) or 0.0),
                stock=str(getattr(r, "stock", "UNKNOWN") or "UNKNOWN").upper(),
                time_frame=int(getattr(r, "time_frame", 5) or 5),
                parameters=dict(getattr(r, "parameters", {}) or {}),
                exit_strategy=str(getattr(r, "exit_strategy", "hold_forever") or "hold_forever"),
                activation=str(getattr(r, "activation", "active") or "active"),
                cooldown_until=None,
            )

    # ── NEW: same-bar key helper ────────────────────────────────────────────────
    def _same_bar_key(self, symbol: str, timeframe: int, bar_ts: Optional[datetime], strategy: str) -> Optional[str]:
        """
        Build the same-bar guard key according to scope. Returns None if bar_ts is None.
        """
        if bar_ts is None:
            return None
        ts_i = int(bar_ts.timestamp())
        sym = (symbol or "UNKNOWN").upper()
        strat = (strategy or "")
        if self._same_bar_scope == "symbol_tf_strategy":
            return f"{sym}:{int(timeframe)}:{ts_i}:{strat}"
        # default (legacy / broader): symbol + timeframe only
        return f"{sym}:{int(timeframe)}:{ts_i}"

    def _process_runner_sync(self, r: RunnerView, as_of: datetime, seq: int, et_day: str, positions_map: Optional[Dict[int, Dict[str, Any]]] = None) -> Tuple[Dict[str, int], Dict[str, Any]]:
        stats_delta = defaultdict(int)

        try:
            with DBManager() as db:
                uid = r.user_id
                rid = r.id
                tf = r.time_frame
                sym = r.stock

                # Pair-level exclusion gate
                excluded, ex_reason = self.health.is_excluded(sym, tf, now=as_of)
                if excluded:
                    stats_delta["excluded_pairs"] += 1
                    stats_delta["processed"] += 1
                    return stats_delta, {"runner_id": rid, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "skipped-excluded-universe", "reason": (ex_reason or "excluded"), "details": None, "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                # NEW: Cooldown gate
                if r.cooldown_until and as_of < r.cooldown_until:
                    stats_delta["skipped_cooldown"] += 1
                    stats_delta["processed"] += 1
                    return stats_delta, {"runner_id": rid, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "skipped-cooldown", "reason": "cooldown_active", "details": None, "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}


                # Fetch candles
                candles = self._get_candles_cached(sym, tf, as_of, lookback=300)
                if not candles:
                    self.health.note_no_data(sym=sym, tf=tf, now=as_of, et_day=et_day)
                    stats_delta["skipped_no_data"] += 1
                    stats_delta["processed"] += 1
                    return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "skipped-no-data", "reason": "insufficient_candles", "details": None if self._thin_no_action_details else json.dumps({"message": "no candles available at as_of", "tf": tf}, ensure_ascii=False), "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                last_ts = self._last_candle_ts(candles)
                
                # Broker tick for stop-loss (only if a position exists)
                price = float(candles[-1]["close"])
                has_position = bool((positions_map or {}).get(r.id))
                if has_position:
                    c = candles[-1]
                    retc = self.broker.on_bar(user_id=uid, runner=r, o=c["open"], h=c["high"], l=c["low"], c=c["close"], at=as_of)
                    stats_delta["stop_cross_exits"] += int(retc.get("stop_cross_exits", 0))
                    # The on_bar logic might have closed the position, so we need to re-check
                    if stats_delta["stop_cross_exits"] > 0:
                        has_position = False # It's closed now

                # Bar advance guard
                bar_key = (r.id, tf)
                prev_bar_ts = self._last_bar_ts.get(bar_key)
                bar_advanced = (prev_bar_ts is None) or (last_ts is not None and last_ts > prev_bar_ts)

                if not bar_advanced and self._require_bar_advance:
                    stats_delta["same_bar_skips"] += 1
                    stats_delta["no_action"] += 1
                    stats_delta["processed"] += 1
                    return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "completed", "reason": "skipped-same-bar", "details": None, "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                # Strategy decision
                # Avoid per-runner DB fetch of OpenPosition in hot path; use prefetch presence
                ctx = _RunnerCtx(runner=r, position=None, price=price, candles=candles)
                decision = self._decide(ctx, is_exit=has_position)
                action = (decision.get("action") or "NO_ACTION").upper()

                # Build details lazily only for actions that need it to reduce JSON overhead
                def _build_details_json() -> str:
                    payload = {
                        "price": round(ctx.price, 6),
                        "position_open": bool(has_position),
                        "timeframe_min": tf,
                        "last_ts": last_ts.isoformat() if last_ts else None,
                        "decision": {k: v for k, v in decision.items() if k != "action"},
                    }
                    try:
                        return json.dumps(payload, ensure_ascii=False)
                    except Exception:
                        return "{}"

                if action == "BUY" and not has_position:
                    sb_key = self._same_bar_key(sym, tf, last_ts, r.strategy)
                    should_skip_buy = False
                    if sb_key:
                        with self._same_bar_thread_lock:
                            if sb_key in self._same_bar_seen:
                                should_skip_buy = True
                            else:
                                self._same_bar_seen.add(sb_key)
                    
                    if should_skip_buy:
                        stats_delta["same_bar_skips"] += 1
                        stats_delta["no_action"] += 1
                        stats_delta["processed"] += 1
                        if last_ts: self._last_bar_ts[bar_key] = last_ts
                        return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "completed", "reason": "skipped-same-bar-guard", "details": None, "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                    # Ignore strategy-provided quantity unless explicitly allowed
                    qty = (int(decision.get("quantity") or 0) if self._allow_strategy_quantity else 0) or self._qty_from_budget(db, r, ctx.price)
                    if qty <= 0:
                        stats_delta["skipped_no_budget"] += 1
                        stats_delta["processed"] += 1
                        if last_ts: self._last_bar_ts[bar_key] = last_ts
                        return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "skipped-no-budget", "reason": "qty=0", "details": None, "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                    ok = self.broker.buy(user_id=uid, runner=r, symbol=sym, price=ctx.price, quantity=qty, decision=decision, at=as_of)
                    if not ok:
                        stats_delta["skipped_no_budget"] += 1
                        stats_delta["processed"] += 1
                        if last_ts: self._last_bar_ts[bar_key] = last_ts
                        return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "skipped-no-budget", "reason": "broker_rejected_buy", "details": _build_details_json(), "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                    # Arm trailing stop once if strategy specified it (idempotent)
                    try:
                        tspec = decision.get("trail_stop_order")
                        if isinstance(tspec, dict):
                            tp = tspec.get("trailing_percent")
                            if tp is None:
                                tp = tspec.get("trailing_amount")
                            tp = float(tp or 0.0)
                            if tp > 0.0:
                                self.broker.arm_trailing_stop_once(
                                    user_id=uid,
                                    runner=r,
                                    entry_price=ctx.price,
                                    trail_pct=tp,
                                    at=as_of,
                                )
                    except Exception:
                        log.exception("Failed to arm trailing stop for runner_id=%s", rid)

                    stats_delta["buys"] += 1
                    stats_delta["processed"] += 1
                    if last_ts: self._last_bar_ts[bar_key] = last_ts
                    self.health.mark_clean_pass(sym=sym, tf=tf)
                    return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "completed", "reason": "buy", "details": _build_details_json(), "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                elif action == "SELL" and has_position:
                    reason = str(decision.get("reason") or decision.get("explanation") or "strategy_sell")
                    pnl = self.broker.sell_all(user_id=uid, runner=r, symbol=sym, price=ctx.price, decision=decision, at=as_of, reason_override=reason)
                    
                    ok = pnl is not None
                    if ok:
                        stats_delta["sells"] += 1
                        self.health.mark_clean_pass(sym=sym, tf=tf)
                        # Update runner's compounding budget with auto-reset if depleted
                        try:
                            # Determine initial budget (persisted in parameters if available)
                            try:
                                params = dict(getattr(r, "parameters", {}) or {})
                            except Exception:
                                params = {}
                            initial_budget = float(params.get("initial_budget_usd", self._unit_budget_usd) or self._unit_budget_usd)
                            new_budget = float(r.current_budget) + float(pnl)
                            # Auto-reset when below threshold
                            if initial_budget > 0 and new_budget < (self._budget_reset_fraction * initial_budget):
                                new_budget = initial_budget
                            # Never allow negative
                            if new_budget < 0:
                                new_budget = 0.0
                            db.update_runner_budget(runner_id=rid, new_budget=new_budget)
                        except Exception:
                            log.exception("Failed to update runner budget for runner_id=%s", rid)
                    else:
                        stats_delta["errors"] += 1
                        self.health.note_error(sym=sym, tf=tf, now=as_of, et_day=et_day)
                    
                    stats_delta["processed"] += 1
                    if last_ts: self._last_bar_ts[bar_key] = last_ts
                    return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "completed" if ok else "error", "reason": "sell" if ok else "broker_sell_failed", "details": _build_details_json(), "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

                else: # NO_ACTION
                    stats_delta["no_action"] += 1
                    stats_delta["processed"] += 1
                    if last_ts: self._last_bar_ts[bar_key] = last_ts
                    self.health.mark_clean_pass(sym=sym, tf=tf)
                    return stats_delta, {"runner_id": r.id, "user_id": uid, "symbol": sym, "strategy": r.strategy, "status": "completed", "reason": str(decision.get("reason") or "no_action"), "details": None if self._thin_no_action_details else _build_details_json(), "execution_time": as_of, "cycle_seq": seq, "timeframe": tf}

        except Exception:
            stats_delta["errors"] += 1
            stats_delta["processed"] += 1
            log.exception("Runner %s tick failed", r.id)
            self.health.note_error(sym=r.stock, tf=r.time_frame, now=as_of, et_day=et_day)
            return stats_delta, {"runner_id": r.id, "user_id": r.user_id, "symbol": r.stock, "strategy": r.strategy, "status": "error", "reason": "exception", "details": "see logs", "execution_time": as_of, "cycle_seq": seq, "timeframe": r.time_frame}

    # ───────────────────────── public ─────────────────────────
    async def run_tick(self, as_of: datetime) -> dict:
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())

        # reset per-tick same-bar BUY guard
        if self._same_bar_seen_seq != seq:
            self._same_bar_seen_seq = seq
            self._same_bar_seen.clear()

        stats = defaultdict(int)
        exec_buffer: List[dict] = []

        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if not user:
                log.warning("No analytics user found yet.")
                return dict(stats)

            uid = int(getattr(user, "id"))
            
            try:
                acct = db.ensure_account(user_id=uid, name="mock")
                if float(getattr(acct, "cash", 0.0) or 0.0) < self._min_cash_floor:
                    setattr(acct, "cash", self._topup_cash_to)
                    db.db.commit()
            except Exception:
                log.exception("ensure_account failed for user_id=%s", uid)

            runners_orm = db.get_runners_by_user(user_id=uid, activation="active")

            # Initialize missing budgets to unit budget and persist initial budget in parameters
            for orm_runner in runners_orm:
                try:
                    if float(getattr(orm_runner, "current_budget", 0.0) or 0.0) <= 0.0:
                        # Ensure a parameters dict exists
                        params = dict(getattr(orm_runner, "parameters", {}) or {})
                        if "initial_budget_usd" not in params:
                            params["initial_budget_usd"] = float(self._unit_budget_usd)
                        setattr(orm_runner, "parameters", params)
                        setattr(orm_runner, "current_budget", float(self._unit_budget_usd))
                except Exception:
                    continue
            try:
                db.db.commit()
            except Exception:
                try:
                    db.db.rollback()
                except Exception:
                    pass

            runners: List[RunnerView] = [self._snapshot_runner(r) for r in runners_orm]
            positions_map = db.get_open_positions_map([rv.id for rv in runners])

        # On first tick, bootstrap coverage health
        if self._sim_boot_start is None:
            self._sim_boot_start = as_of
            self.health.bootstrap_coverage_scan(runners=runners, sim_start=self._sim_boot_start, market=self.mkt, now=as_of)

        self._prefetch_candles_for_runners(runners, as_of)

        try:
            from zoneinfo import ZoneInfo
            ny = ZoneInfo("America/New_York")
            et_day = as_of.astimezone(ny).date().isoformat()
        except Exception:
            et_day = as_of.date().isoformat()

        loop = asyncio.get_running_loop()
        # Execute blocking runner work in a thread pool for true CPU/IO parallelism
        futures = [loop.run_in_executor(self._executor, self._process_runner_sync, r, as_of, seq, et_day, positions_map) for r in runners]
        results = await asyncio.gather(*futures, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                log.error("Error in parallel runner processing: %s", res)
                continue
            stats_delta, exec_log = res
            if exec_log:
                exec_buffer.append(exec_log)
            for k, v in stats_delta.items():
                stats[k] += v

        # Bulk UPSERT executions
        if exec_buffer:
            try:
                with DBManager() as db:
                    db.bulk_upsert_runner_executions(exec_buffer)
            except Exception:
                log.exception("Bulk upsert of runner executions failed")

        # Mark-to-market after the tick
        try:
            with DBManager() as db:
                user = db.get_user_by_username("analytics")
                if user:
                    self.broker.mark_to_market_all(user_id=int(getattr(user, "id")), at=as_of)
        except Exception:
            log.exception("Mark-to-market after tick failed")

        log.debug(
            "tick@%s processed=%d buys=%d sells=%d no_action=%d skipped_no_data=%d skipped_no_budget=%d same_bar_skips=%d stop_cross=%d excluded=%d errors=%d",
            as_of.isoformat(),
            stats["processed"], stats["buys"], stats["sells"], stats["no_action"],
            stats["skipped_no_data"], stats["skipped_no_budget"],
            stats["same_bar_skips"], stats["stop_cross_exits"], stats["excluded_pairs"], stats["errors"],
        )
        kpi.info(
            "tick@%s processed=%d buys=%d sells=%d no_action=%d skipped_no_data=%d skipped_no_budget=%d same_bar_skips=%d stop_cross=%d excluded=%d errors=%d cooldown_skips=%d",
            as_of.isoformat(),
            stats["processed"], stats["buys"], stats["sells"], stats["no_action"],
            stats["skipped_no_data"], stats["skipped_no_budget"],
            stats["same_bar_skips"], stats["stop_cross_exits"], stats["excluded_pairs"], stats["errors"],
            stats["skipped_cooldown"]
        )
        return dict(stats)
