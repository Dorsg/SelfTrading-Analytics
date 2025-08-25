#!/usr/bin/env python3
"""
Database health monitoring script.
Checks for data persistence and alerts on issues.
"""

import os
import sys
import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine, text, inspect

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL_DOCKER", "postgresql://postgres:password@db:5432/new_self_trading_db")

def check_database_health():
    """Comprehensive database health check."""
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Check 1: Database connectivity
            result = conn.execute(text("SELECT version()"))
            db_version = result.scalar()
            logger.info(f"Database connected: {db_version}")
            
            # Check 2: Marker table (indicates persistence)
            try:
                result = conn.execute(text("SELECT version, initialized_at FROM db_init_marker WHERE id = 1"))
                row = result.fetchone()
                if row:
                    version, init_time = row
                    logger.info(f"Database persistence marker: Version {version}, Initialized at {init_time}")
                    
                    # Alert if database was recently re-initialized (potential data loss)
                    if init_time and (datetime.now(timezone.utc) - init_time).total_seconds() < 300:
                        logger.warning("WARNING: Database was initialized less than 5 minutes ago!")
                else:
                    logger.warning("No persistence marker found - database may be fresh")
            except Exception as e:
                logger.warning(f"Could not check persistence marker: {e}")
            
            # Check 3: Core tables
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            required_tables = ["users", "runners", "account_snapshots", "open_positions", "orders", "executed_trades"]
            
            missing_tables = [t for t in required_tables if t not in tables]
            if missing_tables:
                logger.error(f"CRITICAL: Missing tables: {missing_tables}")
                return False
            else:
                logger.info("All required tables exist")
            
            # Check 4: Data counts
            for table in ["users", "runners"]:
                try:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    logger.info(f"Table '{table}' has {count} rows")
                    
                    if table == "users" and count == 0:
                        logger.warning("WARNING: No users in database - possible data loss!")
                except Exception as e:
                    logger.error(f"Could not count rows in {table}: {e}")
            
            # Check 5: WAL status (write-ahead logging)
            try:
                result = conn.execute(text("SELECT pg_current_wal_lsn()"))
                wal_position = result.scalar()
                logger.info(f"WAL position: {wal_position}")
            except Exception:
                pass
                
            return True
            
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    if check_database_health():
        logger.info("Database health check PASSED")
        sys.exit(0)
    else:
        logger.error("Database health check FAILED")
        sys.exit(1)
