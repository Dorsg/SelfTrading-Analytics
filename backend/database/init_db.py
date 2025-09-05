from __future__ import annotations

import logging
from contextlib import suppress

from sqlalchemy import text
from database.db_core import engine

log = logging.getLogger("app")


def _exec(conn, sql: str, params: dict | None = None) -> None:
    """Execute a statement and swallow errors so migrations are idempotent."""
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
    """
    try:
        with engine.begin() as conn:
            # ─────────────────────────────────────────────────────────────
            # orders.side (back-compat: 'action' relaxed to NULL)
            # ─────────────────────────────────────────────────────────────
            _exec(conn, "ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS side VARCHAR(8)")
            _exec(conn, "ALTER TABLE IF EXISTS orders ALTER COLUMN action DROP NOT NULL")

            # ─────────────────────────────────────────────────────────────
            # executed_trades: ensure essential columns exist
            # (buy_ts, sell_ts, prices, qty, pnl_*, strategy, timeframe)
            # Types chosen to be broadly compatible; adjust if your DDL differs.
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

                # ─────────────────────────────────────────────────────────
                # CRITICAL: relax NOT NULL on perm_id for simulation
                # Keep the column if present, just allow NULLs.
                # ─────────────────────────────────────────────────────────
                nullable = _column_is_nullable(conn, "executed_trades", "perm_id")
                if nullable is False:
                    _exec(conn, "ALTER TABLE executed_trades ALTER COLUMN perm_id DROP NOT NULL")
                    log.info("Light migrations: relaxed executed_trades.perm_id to NULL (sim-safe).")

                # Optional helpful indexes (idempotent CREATE INDEX IF NOT EXISTS)
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_executed_trades_user_runner ON executed_trades (user_id, runner_id)")
                _exec(conn, "CREATE INDEX IF NOT EXISTS ix_executed_trades_sell_ts ON executed_trades (sell_ts)")
            else:
                log.warning("Light migrations: executed_trades table not found; skipping column checks.")

        # Mirror the same lines that your logs already show for visibility
        log.info("Light migrations: ensured orders.side (VARCHAR(8)).")
        log.info("Light migrations: relaxed orders.action to NULL (back-compat with 'side').")
        log.info("Light migrations: ensured executed_trades columns (buy_ts, sell_ts, buy/sell_price, qty, pnl_*, strategy, timeframe).")

    except Exception:
        log.exception("Light migrations failed")
