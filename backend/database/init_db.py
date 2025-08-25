import time
import logging
import os
from sqlalchemy.exc import OperationalError
from sqlalchemy import inspect, text
from database.models import Base
import database.db_core as dbc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_tables():
    max_retries = 10
    delay_seconds = 3

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt} of {max_retries}: Ensuring database and tables exist...")
            
            # First ensure the database exists
            try:
                dbc._ensure_database_exists(dbc.DATABASE_URL)
                dbc.rebuild_engine()
                logger.info("Database creation/verification completed.")
            except Exception as db_exc:
                logger.warning(f"Database creation warning: {db_exc}")
            
            # Ensure app_user exists with proper password and privileges
            try:
                db_name = os.getenv("POSTGRES_DB", "new_self_trading_db")
                app_password = os.getenv("DB_PASSWORD")
                if not app_password:
                    logger.warning("DB_PASSWORD not set in environment; skipping user password sync")
                with dbc.engine.begin() as conn:
                    if app_password:
                        try:
                            conn.execute(text("ALTER ROLE app_user WITH PASSWORD :pwd"), {"pwd": app_password})
                            logger.info("Updated app_user password")
                        except Exception:
                            conn.execute(text("CREATE ROLE app_user LOGIN PASSWORD :pwd"), {"pwd": app_password})
                            logger.info("Created app_user with password")
                    # Grants (idempotent)
                    conn.execute(text("GRANT CONNECT ON DATABASE \"" + db_name + "\" TO app_user"))
                    conn.execute(text("GRANT USAGE, CREATE ON SCHEMA public TO app_user"))
                    conn.execute(text("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user"))
                    conn.execute(text("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO app_user"))
                    conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user"))
                    conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO app_user"))
                    logger.info("Granted privileges to app_user")
            except Exception as grant_exc:
                logger.warning(f"Privilege grant step encountered an issue: {grant_exc}")

            # Check if tables exist
            insp = inspect(dbc.engine)
            if insp.has_table("users"):
                logger.info("Tables already exist, verifying schema...")
                # Verify we can query the users table
                with dbc.engine.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
                    logger.info(f"Users table verified with {result} users.")
                # Re-apply grants to ensure existing tables are covered
                try:
                    with dbc.engine.begin() as conn:
                        conn.execute(text("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user"))
                        conn.execute(text("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO app_user"))
                except Exception as e:
                    logger.warning(f"Grant refresh failed on existing tables: {e}")
                return
            else:
                logger.info("Tables missing, creating schema...")
                Base.metadata.create_all(bind=dbc.engine)
                logger.info("Table creation completed successfully.")
                
                # Verify creation worked
                insp_verify = inspect(dbc.engine)
                if insp_verify.has_table("users"):
                    logger.info("Schema creation verified - users table exists.")
                else:
                    raise RuntimeError("Table creation appeared to succeed but users table not found!")
                # Final pass to ensure privileges on freshly created objects
                try:
                    with dbc.engine.begin() as conn:
                        conn.execute(text("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user"))
                        conn.execute(text("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO app_user"))
                except Exception as e:
                    logger.warning(f"Grant pass after create_all failed: {e}")
                return
                
        except OperationalError as e:
            logger.warning(f"DB not ready yet: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)
            else:
                logger.error("Failed to connect to DB after all retries.")
                raise
        except Exception as e:
            logger.exception(f"Unexpected error during table creation: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)
            else:
                logger.error("Failed to create tables after all retries.")
                raise

    raise RuntimeError("Database initialization failed after all attempts")


if __name__ == "__main__":
    create_tables()
