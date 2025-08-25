from __future__ import annotations

import json
import logging
from typing import Sequence

from sqlalchemy.exc import PendingRollbackError, OperationalError, DatabaseError
from sqlalchemy.orm import Session

import database.db_core as dbc
from database.db_utils import aware_utc_now, canonical_cycle_seq, new_session_with_retry
from database.services.users_mixin import UsersMixin, get_users_with_ib_safe as _get_users_with_ib_safe_impl
from database.services.snapshots_mixin import SnapshotsMixin
from database.services.positions_mixin import PositionsMixin
from database.services.runners_mixin import RunnersMixin
from database.services.orders_mixin import OrdersMixin
from database.services.trades_mixin import TradesMixin
from database.services.pnl_mixin import PnLMixin
from database.services.executions_mixin import ExecutionsMixin

logger = logging.getLogger(__name__)

# re-export for backward compatibility (external code imports from database.db_manager)
def get_users_with_ib_safe(max_attempts: int = 3, backoff: float = 2.0):
    return _get_users_with_ib_safe_impl(max_attempts=max_attempts, backoff=backoff)


class DBManager(
    UsersMixin,
    SnapshotsMixin,
    PositionsMixin,
    RunnersMixin,
    OrdersMixin,
    TradesMixin,
    PnLMixin,
    ExecutionsMixin,
):
    """
    Thin facade over a SQLAlchemy session (context-manager friendly).
    All public methods and their behaviors are preserved via mixins.
    """

    # ───────── lifecycle ─────────
    def __init__(self, db_session: Session | None = None) -> None:
        self._own_session = db_session is None
        self.db: Session = db_session or new_session_with_retry()

    def __enter__(self) -> "DBManager":
        return self

    def __exit__(self, exc_t, exc_v, tb) -> None:
        try:
            if exc_t:
                self._safe_rollback()
            else:
                ok = self._commit("Context commit")
                if not ok:
                    # ensure session is left clean
                    self._safe_rollback()
        finally:
            self.close()

    async def __aenter__(self) -> "DBManager":
        return self

    async def __aexit__(self, exc_t, exc_v, tb) -> None:
        self.__exit__(exc_t, exc_v, tb)

    def close(self) -> None:
        if self._own_session:
            self.db.close()

    # ───────── commit/rollback helpers ─────────
    def _safe_rollback(self) -> None:
        try:
            self.db.rollback()
        except Exception as exc:
            logger.warning("rollback failed: %s – rebuilding engine", exc)
            try:
                self.db.close()
            except Exception:
                pass
            try:
                dbc.rebuild_engine()
                # refresh session for future use if needed
                self.db = dbc.SessionLocal()
            except Exception:
                # As we are in exit path, swallow errors here
                pass

    def _commit(self, msg: str, *, retries: int = 3) -> bool:
        """
        Commit with automatic rollback & retry when the connection
        dies mid-transaction (OperationalError / PendingRollbackError / TimeoutError / invalidated DBAPIError).
        """
        attempt = 0
        while attempt <= retries:
            try:
                self.db.commit()
                logger.debug("%s – OK", msg)
                return True

            except PendingRollbackError:
                self.db.rollback()
                attempt += 1
                logger.warning("%s – session invalid (retry %d)", msg, attempt)

            except Exception as exc:
                if dbc._is_retryable_error(exc) and attempt < retries:
                    self.db.rollback()
                    attempt += 1
                    logger.warning(
                        "%s – transient %s (retry %d)",
                        msg, exc.__class__.__name__, attempt
                    )
                    
                    # Handle recovery mode with special backoff
                    if "in recovery mode" in str(exc).lower():
                        dbc._set_backoff_on_recovery(exc)
                        logger.warning(f"Database in recovery mode, applying extended backoff")
                        import time
                        time.sleep(min(attempt * 2.0, 10.0))  # Longer delay for recovery
                    else:
                        import time
                        time.sleep(min(attempt * 0.5, 2.0))
                    
                    dbc.rebuild_engine()
                    
                    # Re-establish fresh session after engine rebuild
                    try:
                        self.db.close()
                        self.db = dbc.SessionLocal()
                    except Exception as session_exc:
                        logger.warning(f"Failed to recreate session: {session_exc}")
                    
                    continue

                self.db.rollback()
                logger.exception("%s – FAILED", msg)
                return False

        logger.error("%s – FAILED after %d retries", msg, retries)
        return False

    # ───────── optional helper retained for compatibility ─────────
    def _infer_cycle_seq(self, perm_id: int) -> str:
        """
        Return the oldest recorded cycle_seq for this perm_id; if none exists,
        fall back to deterministic canonical_cycle_seq so future rows will group
        together even after restarts.
        """
        from database.models import RunnerExecution

        row = (
            self.db.query(RunnerExecution.cycle_seq)
            .filter(RunnerExecution.perm_id == perm_id)
            .order_by(RunnerExecution.id.asc())
            .first()
        )
        return row.cycle_seq if row else canonical_cycle_seq(perm_id)
