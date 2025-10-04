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
        # Step 1: ensure users.password_hash exists and backfill from legacy hashed_password
        try:
            with engine.begin() as conn:
                insp = inspect(conn)
                cols = [c["name"] for c in insp.get_columns("users")]
                if "password_hash" not in cols:
                    log.info("Light migrations: adding users.password_hash column...")
                    conn.execute(text("ALTER TABLE users ADD COLUMN password_hash VARCHAR(255)"))
                if "hashed_password" in cols:
                    conn.execute(text(
                        "UPDATE users SET password_hash = COALESCE(password_hash, hashed_password) "
                        "WHERE password_hash IS NULL AND hashed_password IS NOT NULL"
                    ))
        except Exception:
            log.exception("Light migrations: failed adding/backfilling password_hash")

        # Step 2: ensure runner_executions.timeframe column exists (dialect-safe)
        try:
            with engine.begin() as conn:
                insp = inspect(conn)
                if insp.has_table("runner_executions"):
                    cols = {c["name"] for c in insp.get_columns("runner_executions")}
                    if "timeframe" not in cols:
                        conn.execute(text("ALTER TABLE runner_executions ADD COLUMN timeframe INT"))
        except Exception:
            log.exception("Light migrations: failed ensuring runner_executions.timeframe")

        # Step 3: ensure unique index on runner_executions conflict key
        try:
            with engine.begin() as conn:
                insp = inspect(conn)
                if insp.has_table("runner_executions"):
                    cols = {c["name"] for c in insp.get_columns("runner_executions")}
                    if "timeframe" in cols:
                        conn.execute(text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS ux_runner_exec "
                            "ON runner_executions (cycle_seq, user_id, symbol, strategy, timeframe)"
                        ))
        except Exception:
            log.exception("Light migrations: failed ensuring ux_runner_exec index")

        # Step 4: sanitize and dedupe runners
        # 4a) Uppercase symbols
        try:
            with engine.begin() as conn:
                res = conn.execute(text(
                    "UPDATE runners SET stock = UPPER(stock) WHERE stock <> UPPER(stock)"
                ))
                updated = getattr(res, "rowcount", 0) or 0
                if updated:
                    log.info("Light migrations: uppercased %d runner symbols.", updated)
        except Exception:
            log.exception("Light migrations: failed uppercasing runner symbols")

        # 4b) Align chatgpt strategy name safely (avoid unique conflicts)
        try:
            with engine.begin() as conn:
                # Dialect-agnostic delete of aliases that would conflict with an existing canonical row
                res_del = conn.execute(text(
                    """
                    DELETE FROM runners
                     WHERE TRIM(LOWER(strategy)) IN ('chatgpt5strategy', 'chatgpt 5 strategy', 'chatgpt-5-strategy')
                       AND EXISTS (
                           SELECT 1 FROM runners t
                            WHERE t.user_id = runners.user_id
                              AND t.stock = runners.stock
                              AND t.time_frame = runners.time_frame
                              AND TRIM(LOWER(t.strategy)) = 'chatgpt_5_strategy'
                       )
                    """
                ))
                removed = getattr(res_del, "rowcount", 0) or 0
                if removed:
                    log.info("Light migrations: removed %d conflicting chatgpt alias runners.", removed)

                # Then, update remaining alias rows to canonical where it won't create a duplicate
                res_upd = conn.execute(text(
                    """
                    UPDATE runners
                       SET strategy = 'chatgpt_5_strategy'
                     WHERE TRIM(LOWER(strategy)) IN ('chatgpt5strategy', 'chatgpt 5 strategy', 'chatgpt-5-strategy')
                       AND NOT EXISTS (
                           SELECT 1 FROM runners t
                            WHERE t.user_id = runners.user_id AND t.stock = runners.stock AND t.time_frame = runners.time_frame
                              AND TRIM(LOWER(t.strategy)) = 'chatgpt_5_strategy'
                       )
                    """
                ))
                updated_strat = getattr(res_upd, "rowcount", 0) or 0
                if updated_strat:
                    log.info("Light migrations: aligned %d runners to 'chatgpt_5_strategy' canonical key.", updated_strat)
        except Exception:
            log.exception("Light migrations: failed aligning chatgpt strategy name")

        # 4c) Delete duplicates (keep lowest id per key)
        try:
            with engine.begin() as conn:
                res = conn.execute(text("""
                    DELETE FROM runners
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM runners
                        GROUP BY user_id, stock, strategy, time_frame
                    )
                """))
                removed = getattr(res, "rowcount", 0) or 0
                if removed:
                    log.info("Light migrations: removed %d duplicate runners.", removed)
        except Exception:
            log.exception("Light migrations: failed removing duplicate runners (compat)")

        # 4d) Enforce uniqueness going forward
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_runners_unique
                    ON runners (user_id, stock, strategy, time_frame)
                """))
                log.info("Light migrations: ensured unique index ux_runners_unique.")
        except Exception:
            log.exception("Light migrations: failed creating ux_runners_unique")

        # 4e) Normalize executed_trades strategy names to canonical (reporting consistency)
        try:
            with engine.begin() as conn:
                insp = inspect(conn)
                if insp.has_table("executed_trades"):
                    cols = {c["name"] for c in insp.get_columns("executed_trades")}
                    if "strategy" in cols:
                        res_et = conn.execute(text(
                            """
                            UPDATE executed_trades
                               SET strategy = 'chatgpt_5_strategy'
                             WHERE TRIM(LOWER(strategy)) IN ('chatgpt5strategy', 'chatgpt 5 strategy', 'chatgpt-5-strategy')
                            """
                        ))
                        updated_et = getattr(res_et, "rowcount", 0) or 0
                        if updated_et:
                            log.info("Light migrations: normalized %d executed_trades to 'chatgpt_5_strategy'.", updated_et)
        except Exception:
            log.exception("Light migrations: failed normalizing executed_trades strategy names")

        log.info("Light migrations completed.")
    except Exception:
        log.exception("Light migrations: fatal error")

