#!/usr/bin/env python3
"""
Database initialization safeguard module.
Ensures database and tables exist with proper data integrity checks.
"""

import logging
import os
import time
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import OperationalError, ProgrammingError
from database.models import Base

logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL_DOCKER")
MAX_RETRIES = 30
RETRY_DELAY = 2

def wait_for_db():
    """Wait for database to be ready."""
    url = DATABASE_URL.replace("new_self_trading_db", "postgres")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            engine = create_engine(url, isolation_level="AUTOCOMMIT")
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            logger.info("Database server is ready")
            return True
        except Exception as e:
            logger.warning(f"Database not ready (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                logger.error("Database server failed to become ready")
                return False

def ensure_database_exists():
    """Ensure the application database exists."""
    # Connect to postgres database
    url = DATABASE_URL.replace("new_self_trading_db", "postgres")
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    
    try:
        with engine.connect() as conn:
            # Check if database exists
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": "new_self_trading_db"}
            )
            exists = result.scalar() is not None
            
            if not exists:
                logger.warning("Database 'new_self_trading_db' does not exist - creating it")
                conn.execute(text('CREATE DATABASE "new_self_trading_db"'))
                logger.info("Database created successfully")
            else:
                logger.info("Database 'new_self_trading_db' already exists")
                
    finally:
        engine.dispose()

def ensure_tables_exist():
    """Ensure all required tables exist."""
    engine = create_engine(DATABASE_URL)
    
    try:
        # Check if tables exist
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        required_tables = ["users", "runners", "account_snapshots", "open_positions", "orders", "executed_trades"]
        missing_tables = [t for t in required_tables if t not in existing_tables]
        
        if missing_tables:
            logger.warning(f"Missing tables detected: {missing_tables}")
            logger.info("Creating all database tables...")
            Base.metadata.create_all(bind=engine)
            
            # Verify creation
            inspector = inspect(engine)
            new_tables = inspector.get_table_names()
            created = [t for t in required_tables if t in new_tables]
            logger.info(f"Tables created successfully: {created}")
        else:
            logger.info("All required tables already exist")
            
        # Log table counts for verification
        with engine.connect() as conn:
            for table in ["users", "runners"]:
                if table in existing_tables:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        logger.info(f"Table '{table}' has {count} rows")
                    except Exception:
                        pass
                        
    finally:
        engine.dispose()

def init_database_safe():
    """Initialize database with all safeguards."""
    logger.info("Starting database initialization safeguard...")
    
    # Step 1: Wait for database server
    if not wait_for_db():
        raise RuntimeError("Database server is not available")
    
    # Step 2: Ensure database exists
    # In production we should not auto-create the DB; rely on init scripts.
    allow_create = os.getenv("DB_ALLOW_AUTO_CREATE_DB", "false").lower() == "true"
    if allow_create:
        ensure_database_exists()
    
    # Step 3: Ensure tables exist
    allow_tables = os.getenv("DB_ALLOW_AUTO_CREATE_TABLES", "false").lower() == "true"
    if allow_tables:
        ensure_tables_exist()
    
    logger.info("Database initialization safeguard completed successfully")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_database_safe()
