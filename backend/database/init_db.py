from __future__ import annotations

import logging
from contextlib import suppress

from sqlalchemy import text
from database.db_core import engine
from database.db_manager import DBManager

log = logging.getLogger("app")



def _exec(conn, sql: str, params: dict | None = None) -> None:
    with suppress(Exception):
        conn.execute(text(sql), params or {})


def _column_is_nullable(conn, table: str, column: str) -> bool | None:
    sql = """
        SELECT is_nullable
          FROM information_schema.columns
         WHERE table_schema = current_schema()
           AND table_name = :t
           AND column_name = :c
        LIMIT 1
    """
    row = conn.execute(text(sql), {"t": table, "c": column}).fetchone()
    if not row:
        return None
    is_nullable = (row[0] or "").strip().upper()
    return is_nullable == "YES"


def _table_exists(conn, table: str) -> bool:
    sql = """
        SELECT 1
          FROM information_schema.tables
         WHERE table_schema = current_schema()
           AND table_name = :t
        LIMIT 1
    """
    return conn.execute(text(sql), {"t": table}).scalar() is not None



def _apply_light_migrations() -> None:
    """
    Idempotent light migrations to keep schemas/constraints consistent across processes.

    - Ensure runner_executions unique index (existing behavior in your app).
    - NEW: Deduplicate runners and enforce uniqueness on (user_id, stock, strategy, time_frame).
    """
    try:
        with DBManager() as db:
            # --- Existing migrations you already run elsewhere are safe to repeat ---
            # Ensure runner_executions.timeframe exists and unique index on conflict key
            try:
                db.db.execute(text(
                    "ALTER TABLE IF EXISTS runner_executions "
                    "    ADD COLUMN IF NOT EXISTS timeframe INT"
                ))
                db.db.commit()
            except Exception:
                db.db.rollback()

            try:
                db.db.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_runner_exec "
                    "ON runner_executions (cycle_seq, user_id, symbol, strategy, timeframe)"
                ))
                db.db.commit()
            except Exception:
                db.db.rollback()

            # --- NEW: sanitize + dedupe runners ---
            # 1) Normalize stock symbols to uppercase so the unique key is robust.
            try:
                res = db.db.execute(text(
                    "UPDATE runners SET stock = UPPER(stock) "
                    "WHERE stock <> UPPER(stock)"
                ))
                updated = getattr(res, "rowcount", 0) or 0
                db.db.commit()
                if updated:
                    log.info("Light migrations: uppercased %d runner symbols.", updated)
            except Exception:
                db.db.rollback()
                log.exception("Light migrations: failed uppercasing runner symbols")

            # 2) Delete duplicates, keep lowest id per (user_id, stock, strategy, time_frame).
            try:
                res = db.db.execute(text("""
                    WITH ranked AS (
                        SELECT
                            id,
                            ROW_NUMBER() OVER (
                                PARTITION BY user_id, stock, strategy, time_frame
                                ORDER BY id
                            ) AS rn
                        FROM runners
                    )
                    DELETE FROM runners r
                    USING ranked q
                    WHERE r.id = q.id
                      AND q.rn > 1
                """))
                removed = getattr(res, "rowcount", 0) or 0
                db.db.commit()
                if removed:
                    log.info("Light migrations: removed %d duplicate runners.", removed)
            except Exception:
                db.db.rollback()
                log.exception("Light migrations: failed removing duplicate runners")

            # 3) Enforce uniqueness going forward.
            try:
                db.db.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_runners_unique
                    ON runners (user_id, stock, strategy, time_frame)
                """))
                db.db.commit()
                log.info("Light migrations: ensured unique index ux_runners_unique.")
            except Exception:
                db.db.rollback()
                log.exception("Light migrations: failed creating ux_runners_unique")

            log.info("Light migrations completed.")
    except Exception:
        log.exception("Light migrations: fatal error")

