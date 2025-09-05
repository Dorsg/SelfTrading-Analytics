from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from typing import Optional, Iterable, Dict, Any, List
from datetime import datetime, timezone

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
        return (
            self._session.query(User)
            .filter(User.username == username)
            .first()
        )

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

    # ───────────────────────── Positions ─────────────────────────

    def get_open_position(self, runner_id: int) -> Optional[OpenPosition]:
        return (
            self._session.query(OpenPosition)
            .filter(OpenPosition.runner_id == runner_id)
            .first()
        )

    # ───────────────────────── Executions & results ─────────────────────────

    def record_runner_execution(
        self,
        *,
        runner_id: int,
        user_id: int,
        symbol: str,
        strategy: str,
        status: str,
        reason: Optional[str] = None,
        details: Optional[str] = None,
        execution_time: Optional[datetime] = None,
        cycle_seq: Optional[int] = None,
    ) -> RunnerExecution:
        """
        Persist a per-tick execution record. Commits immediately to ensure logs
        are visible even if later work fails within the same tick.
        Also mirrors a compact line to the 'runner-executions' logger.
        """
        if execution_time is None:
            execution_time = _now_utc()
        if cycle_seq is None:
            cycle_seq = int(execution_time.timestamp())

        rec = RunnerExecution(
            runner_id=runner_id,
            user_id=user_id,
            symbol=symbol.upper(),
            strategy=strategy,
            status=status,
            reason=reason,
            details=details,
            cycle_seq=cycle_seq,
            execution_time=execution_time,
            created_at=_now_utc(),
        )
        self._session.add(rec)
        self._session.commit()

        # Mirror to dedicated log for easy human review
        try:
            _exec_log.info(
                "cycle=%s time=%s runner=%s user=%s symbol=%s strategy=%s status=%s reason=%s",
                cycle_seq,
                execution_time.isoformat(),
                runner_id,
                user_id,
                rec.symbol,
                strategy,
                status,
                (reason or "")
            )
        except Exception:
            pass

        return rec

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
        • Self-healing: if the ON CONFLICT target is missing in this DB (older schema),
            we fall back to a plain INSERT so we don't crash ticks.
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

        # Module-scoped one-time flag to avoid log spam when falling back
        global _RUNNER_EXEC_UPSERT_FALLBACK
        try:
            _RUNNER_EXEC_UPSERT_FALLBACK
        except NameError:
            _RUNNER_EXEC_UPSERT_FALLBACK = False  # type: ignore

        table = RunnerExecution.__table__

        # Try ON CONFLICT first (preferred, idempotent)
        try:
            stmt = pg_insert(table).values(clean_rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["runner_id", "symbol", "strategy", "execution_time"]
            )
            self._session.execute(stmt)
            self._session.commit()
        except Exception as e:
            # If the DB doesn't have the unique/exclusion constraint, Postgres raises:
            # psycopg2.errors.InvalidColumnReference (seen wrapped by SQLAlchemy).
            self._session.rollback()
            if not _RUNNER_EXEC_UPSERT_FALLBACK:
                log.warning(
                    "Upsert ON CONFLICT failed (likely missing unique key). "
                    "Falling back to plain INSERT for runner_executions. Error=%s",
                    getattr(e, "orig", e),
                )
                _RUNNER_EXEC_UPSERT_FALLBACK = True  # type: ignore
            # Plain INSERT (may create duplicates across replays, but keeps the run healthy)
            self._session.execute(table.insert(), clean_rows)
            self._session.commit()

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

