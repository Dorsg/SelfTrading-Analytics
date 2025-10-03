from __future__ import annotations

import logging
from contextlib import suppress

from sqlalchemy import text, inspect
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
    - Fix strategy name for chatgpt_5_strategy to its canonical key.
    """
    try:
        with engine.connect() as conn:
            # --- Ensure users.password_hash exists ---
            try:
                insp = inspect(conn)
                cols = [c["name"] for c in insp.get_columns("users")]
                if "password_hash" not in cols:
                    log.info("Light migrations: adding users.password_hash column...")
                    conn.execute(text("ALTER TABLE users ADD COLUMN password_hash VARCHAR(255)"))
                    conn.commit()
            except Exception:
                conn.rollback()
                log.exception("Light migrations: failed adding password_hash")

            # --- Existing migrations you already run elsewhere are safe to repeat ---
            # Ensure runner_executions.timeframe exists and unique index on conflict key
            try:
                conn.execute(text(
                    "ALTER TABLE IF EXISTS runner_executions "
                    "    ADD COLUMN IF NOT EXISTS timeframe INT"
                ))
                conn.commit()
            except Exception:
                conn.rollback()

            try:
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_runner_exec "
                    "ON runner_executions (cycle_seq, user_id, symbol, strategy, timeframe)"
                ))
                conn.commit()
            except Exception:
                conn.rollback()

            # --- NEW: sanitize + dedupe runners ---
            # 1) Normalize stock symbols to uppercase so the unique key is robust.
            try:
                res = conn.execute(text(
                    "UPDATE runners SET stock = UPPER(stock) "
                    "WHERE stock <> UPPER(stock)"
                ))
                updated = getattr(res, "rowcount", 0) or 0
                conn.commit()
                if updated:
                    log.info("Light migrations: uppercased %d runner symbols.", updated)
            except Exception:
                conn.rollback()
                log.exception("Light migrations: failed uppercasing runner symbols")

            # 2) Migration: align chatgpt_5_strategy alias to canonical key recognized by factory
            try:
                res = conn.execute(text(
                    """
                    UPDATE runners
                       SET strategy = 'chatgpt5strategy'
                     WHERE TRIM(LOWER(strategy)) IN ('chatgpt_5_strategy', 'chatgpt 5 strategy', 'chatgpt-5-strategy')
                    """
                ))
                updated_strat = getattr(res, "rowcount", 0) or 0
                conn.commit()
                if updated_strat:
                    log.info("Light migrations: aligned %d runners to 'chatgpt5strategy' canonical key.", updated_strat)
            except Exception:
                conn.rollback()
                log.exception("Light migrations: failed aligning chatgpt strategy name")


            # 3) Delete duplicates, keep lowest id per (user_id, stock, strategy, time_frame).
            try:
                # SQLite-compatible duplicate removal (no USING clause)
                res = conn.execute(text("""
                    DELETE FROM runners
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM runners
                        GROUP BY user_id, stock, strategy, time_frame
                    )
                """))
                removed = getattr(res, "rowcount", 0) or 0
                conn.commit()
                if removed:
                    log.info("Light migrations: removed %d duplicate runners.", removed)
            except Exception:
                conn.rollback()
                log.exception("Light migrations: failed removing duplicate runners (compat)")

            # 4) Enforce uniqueness going forward.
            try:
                conn.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_runners_unique
                    ON runners (user_id, stock, strategy, time_frame)
                """))
                conn.commit()
                log.info("Light migrations: ensured unique index ux_runners_unique.")
            except Exception:
                conn.rollback()
                log.exception("Light migrations: failed creating ux_runners_unique")

            log.info("Light migrations completed.")
    except Exception:
        log.exception("Light migrations: fatal error")

