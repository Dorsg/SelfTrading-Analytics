from __future__ import annotations

import os
import json
import logging
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


class RunnerService:
    """
    Executes one decision tick across active runners with:
      • Stop cross → exit (broker-managed; never skip).
      • Per-(symbol, timeframe) health gate (HEALTHY→DEGRADED→EXCLUDED) w/ TTL.
      • Strategy signals only on bar advance to avoid same-bar flip-flops.
      • Partial disable per timeframe: excluded pairs are skipped; others run.
      • Bulk UPSERT for runner_executions (cycle_seq, user_id, symbol, strategy, timeframe).
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

    def _decide(self, ctx: _RunnerCtx, strategy_obj=None) -> dict:
        info = RunnerDecisionInfo(
            runner=ctx.runner,
            position=ctx.position,
            current_price=ctx.price,
            candles=ctx.candles,
            distance_from_time_limit=None,
        )
        strat = strategy_obj or select_strategy(ctx.runner)
        raw = strat.decide_buy(info) if ctx.position is None else strat.decide_sell(info)
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
            qty = int(self._unit_budget_usd // max(price, 0.01))
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
            )

    # ───────────────────────── public ─────────────────────────
    async def run_tick(self, as_of: datetime) -> dict:
        as_of = as_of.astimezone(timezone.utc)
        seq = int(as_of.timestamp())

        stats = {
            "processed": 0,
            "buys": 0,
            "sells": 0,
            "no_action": 0,
            "skipped_no_data": 0,
            "skipped_no_budget": 0,
            "same_bar_skips": 0,
            "stop_cross_exits": 0,
            "errors": 0,
            "excluded_pairs": 0,
        }

        exec_buffer: List[dict] = []

        with DBManager() as db:
            user = db.get_user_by_username("analytics")
            if not user:
                log.warning("No analytics user found yet.")
                return stats

            uid = int(getattr(user, "id"))

            # Ensure account exists and is funded for simulation
            try:
                acct = db.ensure_account(user_id=uid, name="mock")
                current_cash = float(getattr(acct, "cash", 0.0) or 0.0)
                if current_cash < self._min_cash_floor:
                    setattr(acct, "cash", self._topup_cash_to)
                    db.db.commit()
                    log.info(
                        "Top-upped mock account cash to $%.2f for user_id=%s (previous=%.2f, floor=%.2f)",
                        self._topup_cash_to, uid, current_cash, self._min_cash_floor
                    )
            except Exception:
                log.exception("ensure_account failed for user_id=%s", uid)

            runners_orm = db.get_runners_by_user(user_id=uid, activation="active")
            runners: List[RunnerView] = [self._snapshot_runner(r) for r in runners_orm]

            # On first tick, bootstrap coverage health
            if self._sim_boot_start is None:
                self._sim_boot_start = as_of
                self.health.bootstrap_coverage_scan(
                    runners=runners, sim_start=self._sim_boot_start, market=self.mkt, now=as_of
                )

            self._prefetch_candles_for_runners(runners, as_of)

            # helper: ET date string for health-gate day windows
            try:
                from zoneinfo import ZoneInfo  # type: ignore
                ny = ZoneInfo("America/New_York")
                et_day = as_of.astimezone(ny).date().isoformat()
            except Exception:
                et_day = as_of.date().isoformat()

            for r in runners:
                try:
                    rid = int(getattr(r, "id", 0) or 0)
                    tf = int(getattr(r, "time_frame", 5) or 5)
                    sym = r.stock

                    # Pair-level exclusion gate (coverage/errors/TTL)
                    excluded, ex_reason = self.health.is_excluded(sym, tf, now=as_of)
                    if excluded:
                        exec_buffer.append({
                            "runner_id": rid,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": r.strategy,
                            "status": "skipped-excluded-universe",
                            "reason": (ex_reason or "excluded"),
                            "details": None,
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["excluded_pairs"] += 1
                        stats["processed"] += 1
                        continue

                    if rid == 0:
                        exec_buffer.append({
                            "runner_id": 0,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": str(getattr(r, "strategy", "")),
                            "status": "skipped-invalid-runner",
                            "reason": "no_primary_key",
                            "details": json.dumps({"error": "runner row missing primary key"}, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    canon = resolve_strategy_key(getattr(r, "strategy", None))
                    if not canon:
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": str(getattr(r, "strategy", "")),
                            "status": "skipped-unknown-strategy",
                            "reason": "unknown_strategy",
                            "details": json.dumps({"strategy": getattr(r, "strategy", None)}, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    # fetch candles (once per runner)
                    candles = self._get_candles_cached(sym, tf, as_of, lookback=300)
                    if not candles:
                        # Note: treat as health event (no_data)
                        self.health.note_no_data(sym=sym, tf=tf, now=as_of, et_day=et_day)
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": r.strategy,
                            "status": "skipped-no-data",
                            "reason": "insufficient_candles",
                            "details": None if self._thin_no_action_details else json.dumps({"message": "no candles available at as_of", "tf": tf}, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["skipped_no_data"] += 1
                        stats["processed"] += 1
                        continue

                    last_ts = self._last_candle_ts(candles)
                    is_stale = self._skip_stale_price and self._is_stale_candle(last_ts, tf, as_of)

                    if is_stale:
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "skipped-stale-price",
                            "details": None if self._thin_no_action_details else json.dumps({
                                "message": "last candle is stale for timeframe",
                                "tf_min": tf,
                                "last_ts": (last_ts.isoformat() if last_ts else None),
                                "as_of": as_of.isoformat(),
                            }, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    # Fresh price → broker with *real* price now that we have candles
                    price = float(candles[-1]["close"])
                    try:
                        retc = self.broker.on_tick(user_id=uid, runner=r, price=price, at=as_of)
                    except Exception:
                        retc = {}
                    try:
                        stats["stop_cross_exits"] += int(retc.get("stop_cross_exits", 0))
                    except Exception:
                        pass

                    # refresh ORM after possible stop close
                    try:
                        db.db.expire_all()
                    except Exception:
                        pass

                    try:
                        pos: Optional[OpenPosition] = (
                            db.db.query(OpenPosition)
                            .filter(OpenPosition.runner_id == r.id)
                            .first()
                        )
                    except Exception:
                        log.exception("Failed to refresh position for runner %s", r.id)
                        pos = None

                    # same-bar guard
                    bar_key = (r.id, tf)
                    prev_bar_ts = self._last_bar_ts.get(bar_key)
                    bar_advanced = (prev_bar_ts is None) or (last_ts is not None and last_ts > prev_bar_ts)

                    if not bar_advanced and self._require_bar_advance:
                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "skipped-same-bar",
                            "details": None if self._thin_no_action_details else json.dumps({
                                "message": "bar has not advanced; ignoring strategy signals this tick",
                                "tf_min": tf,
                                "last_bar_ts": (last_ts.isoformat() if last_ts else None),
                                "prev_bar_ts": (prev_bar_ts.isoformat() if prev_bar_ts else None),
                                "as_of": as_of.isoformat(),
                            }, ensure_ascii=False),
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["same_bar_skips"] += 1
                        stats["no_action"] += 1
                        stats["processed"] += 1
                        continue

                    ctx = _RunnerCtx(runner=r, position=pos, price=price, candles=candles)
                    decision = self._decide(ctx)
                    action = (decision.get("action") or "NO_ACTION").upper()
                    explanation = decision.get("explanation")
                    checks = decision.get("checks")

                    details_payload = {
                        "price": round(ctx.price, 6),
                        "position_open": bool(ctx.position is not None),
                        "timeframe_min": tf,
                        "stale": False,
                        "last_ts": last_ts.isoformat() if last_ts else None,
                        "decision": {k: v for k, v in decision.items() if k not in {"action"}},
                        "checks": checks,
                    }
                    details_json = json.dumps(details_payload, ensure_ascii=False)

                    # BUY (no position; bar advanced)
                    if action == "BUY" and ctx.position is None:
                        qty = int(decision.get("quantity") or 0)
                        if qty <= 0:
                            qty = self._qty_from_budget(db, r, ctx.price)
                        if qty <= 0:
                            msg = {"reason": "qty=0", "explanation": explanation or "insufficient budget"}
                            exec_buffer.append({
                                "runner_id": r.id,
                                "user_id": uid,
                                "symbol": sym,
                                "strategy": r.strategy,
                                "status": "skipped-no-budget",
                                "reason": "qty=0",
                                "details": None if self._thin_no_action_details else json.dumps(msg, ensure_ascii=False),
                                "execution_time": as_of,
                                "cycle_seq": seq,
                                "timeframe": tf,
                            })
                            stats["skipped_no_budget"] += 1
                            stats["processed"] += 1
                            if last_ts is not None:
                                self._last_bar_ts[bar_key] = last_ts
                            continue

                        ok: bool = False
                        try:
                            ok = bool(self.broker.buy(
                                user_id=uid,
                                runner=r,
                                symbol=sym,
                                price=ctx.price,
                                quantity=qty,
                                decision=decision,
                                at=as_of,
                            ))
                        except Exception:
                            ok = False
                            self.health.note_error(sym=sym, tf=tf, now=as_of, et_day=et_day)
                            log.exception("Broker BUY failed for %s", sym)

                        if not ok:
                            exec_buffer.append({
                                "runner_id": r.id,
                                "user_id": uid,
                                "symbol": sym,
                                "strategy": r.strategy,
                                "status": "skipped-no-budget",
                                "reason": "broker_rejected_buy",
                                "details": None if self._thin_no_action_details else details_json,
                                "execution_time": as_of,
                                "cycle_seq": seq,
                                "timeframe": tf,
                            })
                            stats["skipped_no_budget"] += 1
                            stats["processed"] += 1
                            if last_ts is not None:
                                self._last_bar_ts[bar_key] = last_ts
                            continue

                        # Arm trailing stop once at BUY (broker-managed)
                        try:
                            params = getattr(r, "parameters", {}) or {}
                            trail_pct = float(params.get("trailing_stop_percent", 0.0) or 0.0)
                        except Exception:
                            trail_pct = 0.0
                        if trail_pct > 0.0:
                            self.broker.arm_trailing_stop_once(
                                user_id=uid,
                                runner=r,
                                entry_price=ctx.price,
                                trail_pct=trail_pct,
                                at=as_of,
                            )

                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": r.strategy,
                            "status": "completed",
                            "reason": "buy",
                            "details": details_json,
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        stats["buys"] += 1
                        stats["processed"] += 1
                        if last_ts is not None:
                            self._last_bar_ts[bar_key] = last_ts
                        self.health.mark_clean_pass(sym=sym, tf=tf)
                        continue

                    # SELL (strategy-driven)
                    if action == "SELL" and ctx.position is not None:
                        ok = False
                        try:
                            reason = str(decision.get("reason") or decision.get("explanation") or "strategy_sell")
                            ok = self.broker.sell_all(
                                user_id=uid,
                                runner=r,
                                symbol=sym,
                                price=ctx.price,
                                decision=decision,
                                at=as_of,
                                reason_override=reason,
                            )
                        except Exception:
                            ok = False
                            self.health.note_error(sym=sym, tf=tf, now=as_of, et_day=et_day)
                            log.exception("Broker SELL failed for %s", sym)

                        exec_buffer.append({
                            "runner_id": r.id,
                            "user_id": uid,
                            "symbol": sym,
                            "strategy": r.strategy,
                            "status": "completed" if ok else "error",
                            "reason": "sell" if ok else "broker_sell_failed",
                            "details": details_json,
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": tf,
                        })
                        if ok:
                            stats["sells"] += 1
                            self.health.mark_clean_pass(sym=sym, tf=tf)
                        else:
                            stats["errors"] += 1
                        stats["processed"] += 1
                        if last_ts is not None:
                            self._last_bar_ts[bar_key] = last_ts
                        continue

                    # NO_ACTION
                    exec_buffer.append({
                        "runner_id": r.id,
                        "user_id": uid,
                        "symbol": sym,
                        "strategy": r.strategy,
                        "status": "completed",
                        "reason": str(decision.get("reason") or "no_action"),
                        "details": None if self._thin_no_action_details else details_json,
                        "execution_time": as_of,
                        "cycle_seq": seq,
                        "timeframe": tf,
                    })
                    stats["no_action"] += 1
                    stats["processed"] += 1
                    if last_ts is not None:
                        self._last_bar_ts[bar_key] = last_ts
                    self.health.mark_clean_pass(sym=sym, tf=tf)

                except Exception:
                    # any unhandled error → health note + execution error row
                    try:
                        label = getattr(r, "name", None) or f"#{getattr(r, 'id', 'unknown')}"
                    except Exception:
                        label = "unknown"
                    log.exception("Runner %s tick failed", label)
                    try:
                        self.health.note_error(sym=r.stock, tf=int(getattr(r, "time_frame", 5) or 5), now=as_of, et_day=et_day)
                        exec_buffer.append({
                            "runner_id": int(getattr(r, "id", 0) or 0),
                            "user_id": uid,
                            "symbol": r.stock,
                            "strategy": (getattr(r, "strategy", "") or "unknown"),
                            "status": "error",
                            "reason": "exception",
                            "details": "see logs",
                            "execution_time": as_of,
                            "cycle_seq": seq,
                            "timeframe": int(getattr(r, "time_frame", 5) or 5),
                        })
                    except Exception:
                        pass
                    stats["errors"] += 1
                    stats["processed"] += 1

            # ── Bulk UPSERT executions (idempotent) ───────────────────────────────
            try:
                if exec_buffer:
                    # Helpful preview so we can see exactly what we're about to upsert
                    try:
                        ex0 = exec_buffer[0]
                        log.debug(
                            "Preparing bulk upsert: rows=%d example={runner_id=%s user_id=%s sym=%s strat=%s status=%s seq=%s tf=%s}",
                            len(exec_buffer),
                            ex0.get("runner_id"), ex0.get("user_id"), ex0.get("symbol"),
                            ex0.get("strategy"), ex0.get("status"), ex0.get("cycle_seq"),
                            ex0.get("timeframe"),
                        )
                    except Exception:
                        pass

                    # mirror to KPI logger
                    try:
                        kpi.info(
                            "tick=%s runners=%d buys=%d sells=%d no_action=%d skipped_no_data=%d same_bar_skips=%d stop_cross=%d excluded=%d errors=%d",
                            seq, len(runners),
                            stats["buys"], stats["sells"], stats["no_action"],
                            stats["skipped_no_data"], stats["same_bar_skips"], stats["stop_cross_exits"],
                            stats["excluded_pairs"], stats["errors"],
                        )
                    except Exception:
                        pass

                    db.bulk_upsert_runner_executions(exec_buffer)
            except Exception:
                try:
                    bad = exec_buffer[0] if exec_buffer else {}
                    log.exception(
                        "Bulk upsert of runner executions failed (rows=%d example={runner_id=%s sym=%s strat=%s status=%s seq=%s tf=%s})",
                        len(exec_buffer),
                        bad.get("runner_id"), bad.get("symbol"), bad.get("strategy"),
                        bad.get("status"), bad.get("cycle_seq"), bad.get("timeframe")
                    )
                except Exception:
                    log.exception("Bulk upsert of runner executions failed")

            # Mark-to-market after the tick to keep P&L sane
            try:
                self.broker.mark_to_market_all(user_id=uid, at=as_of)
            except Exception:
                log.exception("Mark-to-market after tick failed")

        log.debug(
            "tick@%s processed=%d buys=%d sells=%d no_action=%d skipped_no_data=%d skipped_no_budget=%d same_bar_skips=%d stop_cross=%d excluded=%d errors=%d",
            as_of.isoformat(),
            stats["processed"], stats["buys"], stats["sells"], stats["no_action"],
            stats["skipped_no_data"], stats["skipped_no_budget"],
            stats["same_bar_skips"], stats["stop_cross_exits"], stats["excluded_pairs"], stats["errors"],
        )
        return stats
