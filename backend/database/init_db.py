from __future__ import annotations

import logging
from contextlib import suppress

from sqlalchemy import text
from database.db_core import engine

log = logging.getLogger("app")


def _exec(conn, sql: str, params: dict | None = None) -> None:
    """
    Execute one DDL/DML statement in AUTOCOMMIT mode and swallow errors so that:
      • migrations remain idempotent
      • a single failure does not leave the connection in an aborted state
    """
    with suppress(Exception):
        conn.execute(text(sql), params or {})


def _column_is_nullable(conn, table: str, column: str) -> bool | None:
    """
    Return True if column is nullable, False if not nullable, or None if unknown.
    Works on Postgres via information_schema.
    """
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
    Lightweight, idempotent migrations. Safe to run at every process start.
    Uses AUTOCOMMIT so any individual failure does not abort the whole run.
    """
    try:
        # Switch this Connection to AUTOCOMMIT so each statement is its own txn.
        with engine.connect() as raw_conn:
            conn = raw_conn.execution_options(isolation_level="AUTOCOMMIT")

            # ─────────────────────────────────────────────────────────────
            # orders.side (back-compat: 'action' relaxed to NULL)
            # NOTE: Postgres supports "ALTER TABLE IF EXISTS" (not IF NOT EXISTS).
            # ─────────────────────────────────────────────────────────────
            _exec(conn, "ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS side VARCHAR(8)")
            _exec(conn, "ALTER TABLE IF EXISTS orders ALTER COLUMN action DROP NOT NULL")

            # ─────────────────────────────────────────────────────────────
            # executed_trades: ensure essential columns exist
            # (buy_ts, sell_ts, prices, qty, pnl_*, strategy, timeframe)
            # ─────────────────────────────────────────────────────────────
            if _table_exists(conn, "executed_trades"):
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS user_id INTEGER")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS runner_id INTEGER")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS symbol VARCHAR(32)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS buy_ts TIMESTAMPTZ")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS sell_ts TIMESTAMPTZ")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS buy_price NUMERIC(18,6)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS sell_price NUMERIC(18,6)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS quantity NUMERIC(18,6)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS pnl_amount NUMERIC(18,6)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS pnl_percent NUMERIC(9,6)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS strategy VARCHAR(64)")
                _exec(conn, "ALTER TABLE executed_trades ADD COLUMN IF NOT EXISTS timeframe VARCHAR(16)")

                # CRITICAL: relax NOT NULL on perm_id for simulation
                nullable = _column_is_nullable(conn, "executed_trades", "perm_id")
                if nullable is False:
                    _exec(conn, "ALTER TABLE executed_trades ALTER COLUMN perm_id DROP NOT NULL")
                    log.info("Light migrations: relaxed executed_trades.perm_id to NULL (sim-safe).")

                # Helpful indexes
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_executed_trades_user_runner ON executed_trades (user_id, runner_id)")
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_executed_trades_sell_ts ON executed_trades (sell_ts)")
            else:
                log.warning("Light migrations: executed_trades table not found; skipping column checks.")

            # ─────────────────────────────────────────────────────────────
            # runners: ensure current_budget column exists
            # ─────────────────────────────────────────────────────────────
            if _table_exists(conn, "runners"):
                _exec(conn, "ALTER TABLE runners ADD COLUMN IF NOT EXISTS current_budget DOUBLE PRECISION DEFAULT 0")
                _exec(conn, "UPDATE runners SET current_budget = 0 WHERE current_budget IS NULL")
            else:
                log.warning("Light migrations: runners table not found; skipping current_budget check.")

            # ─────────────────────────────────────────────────────────────
            # open_positions: ensure 'account' column exists and is NOT NULL
            # ─────────────────────────────────────────────────────────────
            if _table_exists(conn, "open_positions"):
                _exec(conn, "ALTER TABLE open_positions ADD COLUMN IF NOT EXISTS account VARCHAR(50)")
                _exec(conn, "UPDATE open_positions SET account = 'mock' WHERE account IS NULL")
                _exec(conn, "ALTER TABLE open_positions ALTER COLUMN account SET NOT NULL")
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_open_positions_account ON open_positions (account)")
            else:
                log.warning("Light migrations: open_positions table not found; skipping account column check.")

            # ─────────────────────────────────────────────────────────────
            # runner_executions: ensure cycle_seq & execution_time (with indexes)
            # and add a unique key for de-dupe + backstop unique index (satisfies ON CONFLICT)
            # ─────────────────────────────────────────────────────────────
            if _table_exists(conn, "runner_executions"):
                _exec(conn, "ALTER TABLE runner_executions ADD COLUMN IF NOT EXISTS cycle_seq INTEGER")
                _exec(conn, "ALTER TABLE runner_executions ADD COLUMN IF NOT EXISTS execution_time TIMESTAMPTZ")
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_runner_exec_cycle ON runner_executions (cycle_seq)")
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_runner_exec_exec_time ON runner_executions (execution_time)")
                # ADD CONSTRAINT has no IF NOT EXISTS in Postgres; swallow duplicate errors safely
                _exec(conn, "ALTER TABLE runner_executions ADD CONSTRAINT uq_runner_exec_key UNIQUE (runner_id, symbol, strategy, execution_time)")
                _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_runner_exec_key_idx ON runner_executions (runner_id, symbol, strategy, execution_time)")
            else:
                log.warning("Light migrations: runner_executions table not found; skipping cycle_seq check.")

        # Human-visible summary
        log.info("Light migrations: ensured orders.side (VARCHAR(8)).")
        log.info("Light migrations: relaxed orders.action to NULL (back-compat with 'side').")
        log.info("Light migrations: ensured executed_trades columns (buy_ts, sell_ts, buy/sell_price, qty, pnl_*, strategy, timeframe).")
        log.info("Light migrations: ensured runners.current_budget (DOUBLE PRECISION).")
        log.info("Light migrations: ensured open_positions.account (NOT NULL, default 'mock').")
        log.info("Light migrations: ensured runner_executions.cycle_seq & execution_time (with indexes + unique key/index).")

    except Exception:
        # If anything outside _exec() blows up (e.g., connection issue), surface once.
        log.exception("Light migrations failed")
