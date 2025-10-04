from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from typing import Optional, Iterable, Dict, Any, List
from datetime import datetime, timezone
import json
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.db_core import SessionLocal
from database.models import (
    User,
    Runner,
    SimulationState,
    Account,
    OpenPosition,
    Order,
    ExecutedTrade,
    RunnerExecution,
    AnalyticsResult,
)

# password hashing for user bootstrap
try:
    from passlib.context import CryptContext  # type: ignore
    _pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
except Exception:
    _pwd_ctx = None  # pragma: no cover

log = logging.getLogger("database.db_manager")
_exec_log = logging.getLogger("runner-executions")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class DBManager(AbstractContextManager["DBManager"]):
    """
    Thin session manager with explicit helpers used by the scheduler, runner service,
    broker, and API layer. Returns ORM rows bound to ONE live session so subsequent
    attribute access is safe during a tick.
    """

    def __init__(self) -> None:
        self._session: Session = SessionLocal()

    # Expose the Session as `.db` for existing callsites
    @property
    def db(self) -> Session:
        return self._session

    @property
    def engine(self):
        """
        Back-compat: provide an Engine for legacy callsites that still expect
        `DBManager.engine`. Preferred access is `self.db.bind` (SQLAlchemy Session→Engine).
        """
        try:
            eng = getattr(self.db, "bind", None)
            if eng is not None:
                return eng
        except Exception:
            pass
        # Final fallback to a globally constructed engine if available.
        try:
            from database.db_core import engine as core_engine  # type: ignore
            return core_engine
        except Exception:
            raise AttributeError("No SQLAlchemy engine available from DBManager")


    # Context manager plumbing
    def __enter__(self) -> "DBManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                # Best-effort rollback; keep callers' explicit commits intact otherwise
                self._session.rollback()
        finally:
            self._session.close()

    # ───────────────────────── Users & Accounts ─────────────────────────

    def get_user_by_username(self, username: str) -> Optional[User]:
        """
        Return the user by username while deferring heavy/optional columns.

        Defers `User.password_hash` to avoid selecting a non-existent column on
        legacy SQLite schemas where migrations haven't run yet. Accessing
        `user.password_hash` will trigger a lazy-load; most scheduler/runners
        never touch it.
        """
        from sqlalchemy.orm import defer

        return (
            self._session.query(User)
            .options(defer(User.password_hash))
            .filter(User.username == username)
            .first()
        )

    def count_users(self) -> int:
        """Return total number of users (robust to empty tables)."""
        try:
            return int(self._session.execute(select(func.count()).select_from(User)).scalar() or 0)
        except Exception:
            return 0

    def create_user(self, username: str, email: str, password: str) -> User:
        """
        Low-ceremony user creation with password hashing (bcrypt via passlib when available).
        Also ensures a mock account and simulation state exist.
        """
        if _pwd_ctx is not None:
            try:
                pw_hash = _pwd_ctx.hash(password)
            except Exception:
                pw_hash = password  # fallback – only used in dev containers
        else:
            pw_hash = password

        u = User(username=username, email=email, password_hash=pw_hash, created_at=_now_utc())
        self._session.add(u)
        self._session.commit()

        # Ensure account + simulation state
        self.ensure_account(user_id=u.id, name="mock")
        self.ensure_simulation_state(user_id=u.id)

        log.info("Created user '%s' (id=%s) with mock account and simulation state.", username, u.id)
        return u

    def get_or_create_user(self, username: str, email: str, password: str) -> User:
        """
        API gateway expects this. Idempotent: returns existing user or creates one.
        Also ensures account + simulation state.
        """
        u = self.get_user_by_username(username)
        if u:
            # Ensure bootstrap invariants even if user pre-existed
            self.ensure_account(user_id=u.id, name="mock")
            self.ensure_simulation_state(user_id=u.id)
            return u
        return self.create_user(username, email, password)

    def ensure_simulation_state(self, user_id: int) -> SimulationState:
        st = (
            self._session.query(SimulationState)
            .filter(SimulationState.user_id == user_id)
            .first()
        )
        if st:
            return st
        st = SimulationState(user_id=user_id, is_running="false", last_ts=None)
        self._session.add(st)
        self._session.commit()
        return st

    def ensure_account(self, user_id: int, name: str = "mock", cash: Optional[float] = None) -> Account:
        """
        Ensure a mock account exists.

        If the account must be created, the starting cash is:
        - the explicit `cash` argument when provided, else
        - the env `MOCK_STARTING_CASH` (default 10_000_000).

        If the account already exists and BOTH cash & equity are zero,
        and a `cash` value is provided, we backfill the balances to `cash`
        (to keep bootstrap idempotent without clobbering real balances).
        """
        acct = (
            self._session.query(Account)
            .filter(Account.user_id == user_id, Account.name == name)
            .first()
        )
        if acct:
            if cash is not None:
                try:
                    if float(acct.cash or 0.0) == 0.0 and float(acct.equity or 0.0) == 0.0:
                        acct.cash = float(cash)
                        acct.equity = float(cash)
                        self._session.commit()
                except Exception:
                    # Non-fatal: leave existing balances as-is
                    self._session.rollback()
            return acct

        initial_cash = float(cash if cash is not None else os.getenv("MOCK_STARTING_CASH", "10000000"))
        acct = Account(
            user_id=user_id,
            name=name,
            cash=initial_cash,
            equity=initial_cash,
            created_at=_now_utc(),
        )
        self._session.add(acct)
        self._session.commit()
        return acct

    # ───────────────────────── Runners ─────────────────────────

    def get_runners_by_user(
        self,
        user_id: int,
        activation: Optional[str] = None,
    ) -> List[Runner]:
        """
        Returns ATTACHED ORM Runner rows (never Core RowMappings / dicts).
        Keeping ordering stable helps debugging & determinism in sims.
        """
        q = self._session.query(Runner).filter(Runner.user_id == user_id)
        if activation:
            q = q.filter(Runner.activation == activation)
        return q.order_by(Runner.created_at.asc(), Runner.id.asc()).all()

    def count_runners(self, user_id: Optional[int] = None) -> int:
        """Return number of runners. Optionally filter by user_id."""
        try:
            q = select(func.count()).select_from(Runner)
            if user_id is not None:
                from database.models import Runner as _Runner
                q = select(func.count()).select_from(_Runner).where(_Runner.user_id == int(user_id))
            return int(self._session.execute(q).scalar() or 0)
        except Exception:
            return 0

    # ───────────────────────── Positions ─────────────────────────

    def get_open_position(self, runner_id: int) -> Optional[OpenPosition]:
        return (
            self._session.query(OpenPosition)
            .filter(OpenPosition.runner_id == runner_id)
            .first()
        )

    def get_open_positions_map(self, runner_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        Bulk fetch open positions for the provided runner IDs, returning a mapping:
            runner_id -> minimal position dict (for fast read-only decisions).

        This avoids issuing N queries per tick when thousands of runners are active.
        """
        out: Dict[int, Dict[str, Any]] = {}
        if not runner_ids:
            return out
        try:
            rows = (
                self._session.query(OpenPosition)
                .filter(OpenPosition.runner_id.in_(list({int(r) for r in runner_ids})))
                .all()
            )
            for p in rows:
                try:
                    out[int(getattr(p, "runner_id"))] = {
                        "symbol": str(getattr(p, "symbol", "")).upper(),
                        "quantity": float(getattr(p, "quantity", 0) or 0),
                        "avg_price": float(getattr(p, "avg_price", 0) or 0),
                        "created_at": getattr(p, "created_at", None),
                        "stop_price": (None if getattr(p, "stop_price", None) is None else float(getattr(p, "stop_price"))),
                        "trail_percent": (None if getattr(p, "trail_percent", None) is None else float(getattr(p, "trail_percent"))),
                        "highest_price": (None if getattr(p, "highest_price", None) is None else float(getattr(p, "highest_price"))),
                    }
                except Exception:
                    continue
        except Exception:
            # Non-fatal: return empty map on error to keep tick running
            return {}
        return out

    # ───────────────────────── Executions & results ─────────────────────────

    def bulk_upsert_runner_executions(self, rows):
        """
        Insert-or-update runner execution rows efficiently and idempotently.

        Conflict key (must match DB unique constraint / index):
            (cycle_seq, user_id, symbol, strategy, timeframe)

        Behavior:
        • Normalizes each row (uppercases symbol, ensures non-null strategy).
        • Collapses any duplicate rows within the same batch that target the SAME conflict key
        to avoid Postgres 'CardinalityViolation' ("row updated twice") during ON CONFLICT DO UPDATE.
        Winner selection priority: error > sell > buy > completed/no_action > skipped-*; then
        prefer richer 'details', then latest 'execution_time', finally last-write-wins.
        • Uses native ON CONFLICT for PostgreSQL / SQLite / MySQL where available.
        • Falls back to an UPDATE-then-INSERT loop for unknown dialects.
        • Mirrors a concise success/failure line to the "runner-executions" logger, and warns
        when dedup collapses rows.
        """
        import json
        import logging
        from datetime import datetime

        logger_db = logging.getLogger("database.db_manager")
        logger_exec = logging.getLogger("runner-executions")

        if not rows:
            logger_db.debug("bulk_upsert_runner_executions: nothing to upsert (0 rows)")
            return

        # ── Normalize/guard payload ────────────────────────────────────────────────
        def _norm(r):
            details = r.get("details")
            if isinstance(details, (dict, list)):
                try:
                    details = json.dumps(details, ensure_ascii=False)
                except Exception:
                    details = str(details)

            # Ensure symbol/strategy are non-null for a stable unique key
            sym = (r.get("symbol") or "UNKNOWN")
            try:
                sym = str(sym).upper()
            except Exception:
                sym = "UNKNOWN"

            strat = (r.get("strategy") or "unknown")
            try:
                strat = str(strat)
            except Exception:
                strat = "unknown"

            # execution_time expected to be TZ-aware datetime; if not, pass-through
            exec_time = r.get("execution_time")

            # timeframe: tolerate None safely (envs/tests)
            tf = r.get("timeframe", 5)
            try:
                tf = int(tf if tf is not None else 5)
            except Exception:
                tf = 5

            return {
                "runner_id": int(r.get("runner_id")),
                "user_id": int(r.get("user_id")),
                "symbol": sym,
                "strategy": strat,
                "status": (r.get("status") or None),
                "reason": (r.get("reason") or None),
                "details": details,
                "execution_time": exec_time,
                "cycle_seq": int(r.get("cycle_seq")),
                "timeframe": tf,
            }

        values = [_norm(r) for r in rows]

        # ── Collapse duplicates by conflict key *within the same batch* ────────────
        conflict_cols = ["cycle_seq", "user_id", "symbol", "strategy", "timeframe"]
        updatable_cols = ["runner_id", "status", "reason", "details", "execution_time"]

        def _severity(row: dict) -> int:
            st = (row.get("status") or "").lower()
            rs = (row.get("reason") or "").lower()
            # Higher number = more important
            if st == "error":
                return 50
            # Completed with meaningful actions outrank plain "completed/no_action"
            if rs == "sell":
                return 40
            if rs == "buy":
                return 30
            if st == "completed":
                # completed + no_action or other completes
                return 20
            if st.startswith("skipped"):
                return 10
            return 0

        def _better(a: dict, b: dict) -> dict:
            """Choose a winner between two rows targeting the same unique key."""
            sa, sb = _severity(a), _severity(b)
            if sb > sa:
                return b
            if sa > sb:
                return a
            # Tie-break 1: prefer the one with 'details'
            da, db = a.get("details") or "", b.get("details") or ""
            if db and not da:
                return b
            if da and not db:
                return a
            # Tie-break 2: prefer latest execution_time if both present
            ta, tb = a.get("execution_time"), b.get("execution_time")
            if isinstance(ta, datetime) and isinstance(tb, datetime):
                return b if tb >= ta else a
            # Final: last-write-wins (prefer 'b' as it arrived later)
            return b

        merged = {}
        dup_count: dict[tuple, int] = {}
        for v in values:
            key = (v["cycle_seq"], v["user_id"], v["symbol"], v["strategy"], v["timeframe"])
            if key in merged:
                dup_count[key] = dup_count.get(key, 1) + 1
                merged[key] = _better(merged[key], v)
            else:
                merged[key] = v
                dup_count[key] = 1

        deduped_values = list(merged.values())

        # Helpful preview + duplicate diagnostics
        dialect = (self.engine.dialect.name if getattr(self.engine, "dialect", None) else "unknown")
        try:
            ex0 = deduped_values[0]
            logger_db.debug(
                "bulk_upsert_runner_executions: preparing upsert rows=%d (deduped from %d) dialect=%s conflict=%s example=%s",
                len(deduped_values), len(values), dialect, ",".join(conflict_cols),
                {k: ex0.get(k) for k in ("runner_id", "user_id", "symbol", "strategy", "cycle_seq", "timeframe", "status")}
            )
        except Exception:
            pass

        # If we collapsed any duplicates, emit a compact warning with top examples
        if len(deduped_values) < len(values):
            try:
                # Build a small top list
                items = sorted(dup_count.items(), key=lambda kv: kv[1], reverse=True)
                tops = [f"key={k} x{n}" for (k, n) in items if n > 1][:5]
                logger_db.warning(
                    "bulk_upsert_runner_executions: collapsed duplicate keys (before=%d after=%d). Top duplicates: %s",
                    len(values), len(deduped_values), "; ".join(tops) if tops else "<none>"
                )
            except Exception:
                pass

        # ── Execute inside a single transaction ────────────────────────────────────
        table = RunnerExecution.__table__
        try:
            with self.engine.begin() as conn:
                if dialect == "postgresql":
                    from sqlalchemy.dialects.postgresql import insert as pg_insert
                    stmt = pg_insert(table).values(deduped_values)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_cols,
                        set_={c: getattr(stmt.excluded, c) for c in updatable_cols},
                    )
                    conn.execute(stmt)

                elif dialect == "sqlite":
                    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                    stmt = sqlite_insert(table).values(deduped_values)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_cols,
                        set_={c: getattr(stmt.excluded, c) for c in updatable_cols},
                    )
                    conn.execute(stmt)

                elif dialect.startswith("mysql"):
                    from sqlalchemy.dialects.mysql import insert as my_insert
                    stmt = my_insert(table).values(deduped_values)
                    stmt = stmt.on_duplicate_key_update(**{c: getattr(stmt.inserted, c) for c in updatable_cols})
                    conn.execute(stmt)

                else:
                    # Portable fallback: UPDATE then INSERT if no row was touched.
                    for row in deduped_values:
                        upd = (
                            table.update()
                            .where(table.c.cycle_seq == row["cycle_seq"])
                            .where(table.c.user_id == row["user_id"])
                            .where(table.c.symbol == row["symbol"])
                            .where(table.c.strategy == row["strategy"])
                            .where(table.c.timeframe == row["timeframe"])
                            .values({c: row[c] for c in updatable_cols})
                        )
                        res = conn.execute(upd)
                        if getattr(res, "rowcount", 0) == 0:
                            ins = table.insert().values(row)
                            conn.execute(ins)

            # Success logging (to dedicated executions log if configured)
            sample = {
                "runner_id": deduped_values[0]["runner_id"],
                "user_id": deduped_values[0]["user_id"],
                "symbol": deduped_values[0]["symbol"],
                "strategy": deduped_values[0]["strategy"],
                "cycle_seq": deduped_values[0]["cycle_seq"],
                "timeframe": deduped_values[0]["timeframe"],
                "status": deduped_values[0]["status"],
            }
            # Reduce log volume on hot path: success to DEBUG; warnings/errors stay higher
            logger_exec.debug(
                "UPSERT OK runner_executions: rows=%d dialect=%s conflict=%s example=%s",
                len(deduped_values), dialect, ",".join(conflict_cols), sample
            )

        except Exception:
            try:
                sample = {
                    k: deduped_values[0].get(k)
                    for k in ("runner_id", "user_id", "symbol", "strategy", "cycle_seq", "timeframe", "status")
                }
            except Exception:
                sample = {}
            logger_db.exception(
                "Bulk upsert failed (runner_executions). rows=%d dialect=%s conflict=%s example=%s",
                len(deduped_values), dialect, ",".join(conflict_cols), sample
            )
            raise



    def record_runner_execution(
        self,
        runner_id: int,
        user_id: int,
        symbol: str,
        strategy: str,
        status: str,
        reason: Optional[str] = None,
        details: Optional[str] = None,
        execution_time: Optional[datetime] = None,
        cycle_seq: Optional[int] = None,
        timeframe: Optional[int] = None,
    ) -> RunnerExecution:
        """
        Persist a per-tick execution record (single-row UPSERT).
        """
        if execution_time is None:
            execution_time = _now_utc()
        if cycle_seq is None:
            cycle_seq = int(execution_time.timestamp())

        # Prefer the bulk upsert path for performance; this is a single-row variant
        self.bulk_upsert_runner_executions([{
            "runner_id": runner_id,
            "user_id": user_id,
            "symbol": symbol.upper(),
            "strategy": strategy,
            "status": status,
            "reason": reason,
            "details": details,
            "cycle_seq": cycle_seq,
            "execution_time": execution_time,
            "timeframe": int(timeframe) if timeframe is not None else None,
        }])

        # Best-effort fetch of the upserted row (not strictly required)
        rec = (
            self._session.query(RunnerExecution)
            .filter(
                RunnerExecution.cycle_seq == cycle_seq,
                RunnerExecution.user_id == user_id,
                RunnerExecution.symbol == symbol.upper(),
                RunnerExecution.strategy == strategy,
                RunnerExecution.timeframe == (int(timeframe) if timeframe is not None else None),
            )
            .first()
        )
        return rec  # type: ignore[return-value]

    # ───────────────────────── Misc helpers (used by other parts) ─────────────────────────

    def count_minute_bars(self, *, symbol: str, interval_min: int, ts_lte: datetime) -> int:
        from database.models import HistoricalMinuteBar
        stmt = (
            select(func.count())
            .select_from(HistoricalMinuteBar)
            .where(HistoricalMinuteBar.symbol == symbol.upper())
            .where(HistoricalMinuteBar.interval_min == int(interval_min))
            .where(HistoricalMinuteBar.ts <= ts_lte)
        )
        return int(self._session.execute(stmt).scalar() or 0)

    def bulk_record_runner_executions(self, records: List[Dict[str, Any]]) -> None:
        """
        Fast path: insert many RunnerExecution rows in one commit.

        Expected keys per record:
          runner_id, user_id, symbol, strategy, status, reason, details,
          execution_time (datetime), cycle_seq (int)

        Notes:
          • `symbol` is uppercased here.
          • `created_at` is set to now.
          • Mirrors compact lines to the 'runner-executions' logger, but
            avoids per-row flush/commit overhead.
          • Summary rows and runner_id<=0 rows are NOT persisted (only logged).
          • Upsert de-dupes on (runner_id, symbol, strategy, execution_time).
        """
        if not records:
            return

        now = _now_utc()
        # Filter out rows that will violate FK or are summaries; still mirror them to logs
        clean_rows: List[dict] = []
        for rec in records:
            try:
                runner_id = int(rec.get("runner_id", 0) or 0)
                strategy = str(rec.get("strategy", "") or "")
                if runner_id <= 0 or strategy.lower() == "summary":
                    # mirror to log only; do not persist
                    try:
                        _exec_log.info(
                            "cycle=%s time=%s runner=%s user=%s symbol=%s strategy=%s status=%s reason=%s",
                            int(rec.get("cycle_seq", int(now.timestamp()))),
                            (rec.get("execution_time") or now).isoformat(),
                            runner_id,
                            int(rec.get("user_id", 0) or 0),
                            str(rec.get("symbol", "UNKNOWN")).upper(),
                            strategy,
                            str(rec.get("status", "")),
                            str(rec.get("reason", "") or ""),
                        )
                    except Exception:
                        pass
                    continue

                clean_rows.append(
                    {
                        "runner_id": runner_id,
                        "user_id": int(rec.get("user_id", 0) or 0),
                        "symbol": str(rec.get("symbol", "UNKNOWN")).upper(),
                        "strategy": strategy,
                        "status": str(rec.get("status", "")),
                        "reason": rec.get("reason"),
                        "details": rec.get("details"),
                        "cycle_seq": int(rec.get("cycle_seq", int(now.timestamp()))),
                        "execution_time": rec.get("execution_time") or now,
                        "created_at": now,
                    }
                )
            except Exception:
                continue

        if not clean_rows:
            return

        try:
            # Postgres upsert to keep idempotent on replays
            table = RunnerExecution.__table__
            stmt = pg_insert(table).values(clean_rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["runner_id", "symbol", "strategy", "execution_time"]
            )
            self._session.execute(stmt)
            self._session.commit()
        except Exception:
            self._session.rollback()
            raise

        # Mirror clean rows to the execution log
        try:
            for r in clean_rows:
                _exec_log.info(
                    "cycle=%s time=%s runner=%s user=%s symbol=%s strategy=%s status=%s reason=%s",
                    r["cycle_seq"],
                    r["execution_time"].isoformat(),
                    r["runner_id"],
                    r["user_id"],
                    r["symbol"],
                    r["strategy"],
                    r["status"],
                    (r.get("reason") or ""),
                )
        except Exception:
            pass
