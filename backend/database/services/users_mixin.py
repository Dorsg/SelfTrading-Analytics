from __future__ import annotations

import logging
import time
from typing import Sequence

from psycopg2.errors import UndefinedTable
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import Session

import database.db_core as dbc
from api_gateway.security.auth import hash_password, verify_password
from database.models import User
from database.db_utils import with_retry

logger = logging.getLogger(__name__)


# ─── keep this near the top of the file with other imports ───
from psycopg2.errors import UndefinedTable
from sqlalchemy.exc import IntegrityError, ProgrammingError

import database.db_core as dbc
from api_gateway.security.auth import hash_password, verify_password
from database.models import User
from database.db_utils import with_retry

# Log-once guard for a missing database
_MISSING_DB_LOGGED = False


def get_users_with_ib_safe(max_attempts: int = 3, backoff: float = 2.0) -> list[User]:
    """
    Robust loader for the scheduler.

    • Uses a fresh SessionLocal() per attempt.
    • If the *database itself* is missing (SQLSTATE 3D000) → try to auto-create it once,
      rebuild the engine, and retry immediately.
    • If core tables aren't created yet (UndefinedTable) → return [] (tick does nothing).
    • Retries transient errors with exponential backoff.
    """
    global _MISSING_DB_LOGGED

    # Reduce debug logging frequency - only log every 10th call
    call_count = getattr(get_users_with_ib_safe, '_call_count', 0) + 1
    get_users_with_ib_safe._call_count = call_count
    
    if call_count % 10 == 1:  # Log first call and every 10th call
        logger.debug(f"get_users_with_ib_safe called with max_attempts={max_attempts}, backoff={backoff}")
    
    delay = backoff
    for attempt in range(1, max_attempts + 1):
        if call_count % 10 == 1:
            logger.debug(f"get_users_with_ib_safe attempt {attempt}/{max_attempts}")
        try:
            db = dbc.new_session_blocking()
            try:
                if call_count % 10 == 1:
                    logger.debug("Executing query to get users with IB credentials")
                rows = (
                    db.query(User)
                      .filter(User.ib_username.isnot(None),
                              User.ib_password.isnot(None))
                      .all()
                )
                if call_count % 10 == 1:
                    logger.debug(f"Query successful, found {len(rows)} users with IB credentials")
                # If we get here, DB is fine → clear the "missing DB" one-shot flag
                if _MISSING_DB_LOGGED:
                    _MISSING_DB_LOGGED = False
                return rows
            finally:
                db.close()

        # Schema hasn't been created yet – skip this tick quietly.
        except ProgrammingError as exc:
            logger.debug(f"ProgrammingError in get_users_with_ib_safe: {type(exc).__name__}: {exc}")
            if isinstance(exc.orig, UndefinedTable):
                logger.warning(
                    "get_users_with_ib_safe – core tables missing; "
                    "scheduler tick skipped (DB not initialised yet)"
                )
                return []
            # Some other SQL syntax/privilege error → bubble up / retry logic below.
            raise

        except Exception as exc:
            if call_count % 10 == 1 or attempt == max_attempts:  # Only log for every 10th call or final attempt
                logger.debug(f"Exception in get_users_with_ib_safe attempt {attempt}: {type(exc).__name__}: {exc}")
            # The *database name* in the DSN doesn't exist (SQLSTATE 3D000).
            if dbc._is_undefined_database_error(exc):
                if not _MISSING_DB_LOGGED:
                    logger.warning("Target Postgres database missing; attempting auto-create once")
                    _MISSING_DB_LOGGED = True
                try:
                    logger.debug("Attempting to create database and rebuild engine")
                    dbc._ensure_database_exists(dbc.DATABASE_URL)
                    dbc.rebuild_engine()
                    logger.debug("Database created and engine rebuilt, retrying immediately")
                    # try again immediately on the fresh engine
                    continue
                except Exception as create_exc:
                    logger.exception(f"Auto-create failed: {type(create_exc).__name__}: {create_exc}")
                    # Force table creation as a last resort
                    try:
                        from database.models import Base
                        logger.warning("Attempting emergency table creation as last resort...")
                        Base.metadata.create_all(bind=dbc.engine)
                        logger.info("Emergency table creation succeeded - retrying query")
                        continue  # Retry the operation
                    except Exception as emergency_exc:
                        logger.exception(f"Emergency table creation failed: {type(emergency_exc).__name__}: {emergency_exc}; scheduler will idle this tick")
                    return []

            # Transient / network / recovery errors → bounded retries + backoff.
            if not dbc._is_retryable_error(exc) or attempt == max_attempts:
                logger.error(f"Non-retryable error or max attempts reached in get_users_with_ib_safe: {type(exc).__name__}: {exc}")
                raise
            logger.warning(f"Retryable error in get_users_with_ib_safe attempt {attempt}: {type(exc).__name__}: {exc}")
            dbc.rebuild_engine()
            time.sleep(min(delay, 0.2))
            delay *= 2

    logger.error("get_users_with_ib_safe failed after all attempts")
    return []




class UsersMixin:
    """
    Users-related operations. Expects the concrete class to define:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
    """

    def get_user_by_username(self, username: str) -> User | None:
        logger.debug(f"get_user_by_username called for username: {username}")
        try:
            user = self.db.query(User).filter(User.username == username).first()
            logger.debug(f"get_user_by_username result: {'found' if user else 'not found'}")
            return user
        except Exception as e:
            logger.error(f"get_user_by_username failed for username {username}: {type(e).__name__}: {e}")
            raise

    def get_user_by_email(self, email: str) -> User | None:
        logger.debug(f"get_user_by_email called for email: {email}")
        try:
            user = self.db.query(User).filter(User.email == email).first()
            logger.debug(f"get_user_by_email result: {'found' if user else 'not found'}")
            return user
        except Exception as e:
            logger.error(f"get_user_by_email failed for email {email}: {type(e).__name__}: {e}")
            raise

    @with_retry()
    def get_users_with_ib(self) -> Sequence[User]:
        return (
            self.db.query(User)
            .filter(User.ib_username.isnot(None), User.ib_password.isnot(None))
            .all()
        )

    def create_user(
        self,
        *,
        username: str,
        email: str,
        password: str,
        ib_username: str | None = None,
        ib_password: str | None = None,
    ) -> User:
        if self.get_user_by_username(username) or self.get_user_by_email(email):
            raise ValueError("Username or e-mail already taken")

        user = User(
            username=username,
            email=email,
            hashed_password=hash_password(password),
            ib_username=ib_username,
            ib_password=ib_password,
        )
        self.db.add(user)
        self._commit("Create user")
        return user

    def authenticate(self, *, username: str, password: str) -> User | None:
        logger.debug(f"authenticate called for username: {username}")
        try:
            user = self.get_user_by_username(username)
            if user is None:
                logger.debug(f"Authentication failed - user not found: {username}")
                return None
            if not verify_password(password, user.hashed_password):
                logger.debug(f"Authentication failed - invalid password for user: {username}")
                return None
            logger.debug(f"Authentication successful for user: {username}")
            return user
        except Exception as e:
            logger.error(f"Authentication failed with exception for user {username}: {type(e).__name__}: {e}")
            raise
