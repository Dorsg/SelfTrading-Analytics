"""
logger_config.py
───────────────────────────────────────────────────────────────────────────────
• Rotating file handlers per component
• Root logger also writes WARN/ERROR to errors_warnings.log
• Stdlib warnings routed into logging
• Lightweight CPU/RAM watchdog (process-local)
• Rate-limited spammy sources (ib_insync 1100/1102, etc.)
"""

from __future__ import annotations
import logging, logging.config, os, time
from pathlib import Path
import coloredlogs

# ───────────────────────── directories ──────────────────────────
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ───────────────────────── env toggles ──────────────────────────
DISABLE_STDOUT  = os.getenv("NO_STDOUT_LOG", "0") == "1"
LOG_LEVEL       = os.getenv("LOG_LEVEL",      "INFO").upper()

# Watchdog tuning (env-overridable)
WATCHDOG_CPU_PCT  = float(os.getenv("WATCHDOG_CPU_PCT",  "90"))
WATCHDOG_MEM_PCT  = float(os.getenv("WATCHDOG_MEM_PCT",  "90"))
WATCHDOG_INTERVAL = int  (os.getenv("WATCHDOG_INTERVAL", "10"))

# Spam rate-limit (seconds) for very noisy third-party loggers
IB_SPAM_SUPPRESS_SEC = float(os.getenv("IB_SPAM_SUPPRESS_SEC", "30"))

# Enhanced debugging for IB and DB operations
IB_DEBUG_ENABLED = os.getenv("IB_DEBUG_ENABLED", "true").lower() == "true"
DB_DEBUG_ENABLED = os.getenv("DB_DEBUG_ENABLED", "true").lower() == "true"

# Log rotation settings
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(50 * 1024 * 1024)))  # 50MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "10"))  # Keep 10 rotated files

# ───────────────────────── handler groups ───────────────────────
root_handlers = (["console"] if not DISABLE_STDOUT else []) + ["file_root"]
root_handlers_with_err = root_handlers + ["file_warn_error"]

# ───────────────────────── logging config ───────────────────────
LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,

    "formatters": {
        "detailed": {
            "format": "[%(asctime)s] %(levelname)-7s %(name)s: %(message)s",
        },
    },

    "handlers": {
        # ─── generic ───
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "detailed",
        },
        "file_root": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "app.log",
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
            "formatter": "detailed",
            "encoding": "utf-8",
        },
        "file_warn_error": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "errors_warnings.log",
            "maxBytes": LOG_MAX_BYTES // 2,
            "backupCount": LOG_BACKUP_COUNT,
            "formatter": "detailed",
            "encoding": "utf-8",
            "level": "WARNING",
        },

        # ─── component-specific ───
        "file_scheduler":            {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "scheduler.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_runner_service":       {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "runner-service.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_basic_strategy":       {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "basic-strategy.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_chatgpt_5_strategy": {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "chatgpt-5-strategy.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_grok_4_strategy": {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "grok-4-strategy.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_below_above_strategy": {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "below-above-strategy.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_fibonacci_strategy":   {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "fibonacci-strategy.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_ibkr_business_manager":{"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "ibkr-business-manager.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_sync_service":         {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "sync-service.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_market_data_manager":  {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "market-data-manager.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_api_gateway":          {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "api-gateway.log","maxBytes": LOG_MAX_BYTES,"backupCount": LOG_BACKUP_COUNT,"formatter": "detailed","encoding": "utf-8"},
        "file_ib_debug":             {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "ib-debug.log","maxBytes": LOG_MAX_BYTES * 2,"backupCount": LOG_BACKUP_COUNT + 5,"formatter": "detailed","encoding": "utf-8"},
        "file_db_debug":             {"class": "logging.handlers.RotatingFileHandler","filename": LOG_DIR / "db-debug.log","maxBytes": LOG_MAX_BYTES * 2,"backupCount": LOG_BACKUP_COUNT + 5,"formatter": "detailed","encoding": "utf-8"},
    },

    "loggers": {
        "": {  # root
            "level": LOG_LEVEL,
            "handlers": root_handlers_with_err,
        },
        "Scheduler":               {"level": "INFO","handlers": ["file_scheduler", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "runner-service":          {"level": "INFO","handlers": ["file_runner_service", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "basic-strategy":          {"level": "INFO","handlers": ["file_basic_strategy", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "chatgpt-5-strategy":    {"level": "DEBUG","handlers": ["file_chatgpt_5_strategy", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "grok-4-strategy":    {"level": "DEBUG","handlers": ["file_grok_4_strategy", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "below-above-strategy":    {"level": "DEBUG","handlers": ["file_below_above_strategy", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "fibonacci-strategy":      {"level": "DEBUG","handlers": ["file_fibonacci_strategy", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "IBKR-Business-Manager":   {"level": "DEBUG" if IB_DEBUG_ENABLED else "INFO","handlers": ["file_ibkr_business_manager", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "sync-service":            {"level": "DEBUG" if DB_DEBUG_ENABLED else "INFO","handlers": ["file_sync_service", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "api-gateway":             {"level": "DEBUG" if DB_DEBUG_ENABLED else "INFO","handlers": ["file_api_gateway", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "market-data-manager":     {"level": "DEBUG","handlers": ["file_market_data_manager", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        
        # Enhanced IB debugging
        "ib_insync":               {"level": "DEBUG" if IB_DEBUG_ENABLED else "WARNING","handlers": ["file_ib_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "ib_insync.client":        {"level": "DEBUG" if IB_DEBUG_ENABLED else "WARNING","handlers": ["file_ib_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "ib_insync.wrapper":       {"level": "DEBUG" if IB_DEBUG_ENABLED else "WARNING","handlers": ["file_ib_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "ib_insync.ib":            {"level": "DEBUG" if IB_DEBUG_ENABLED else "WARNING","handlers": ["file_ib_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        
        # Enhanced database debugging
        "sqlalchemy":              {"level": "DEBUG" if DB_DEBUG_ENABLED else "WARNING","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "sqlalchemy.engine":       {"level": "DEBUG" if DB_DEBUG_ENABLED else "WARNING","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "sqlalchemy.pool":         {"level": "DEBUG" if DB_DEBUG_ENABLED else "WARNING","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "sqlalchemy.orm":          {"level": "DEBUG" if DB_DEBUG_ENABLED else "WARNING","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "psycopg2":                {"level": "DEBUG" if DB_DEBUG_ENABLED else "WARNING","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        
        # Database service modules
        "database":                {"level": "DEBUG" if DB_DEBUG_ENABLED else "INFO","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "database.db_core":        {"level": "DEBUG" if DB_DEBUG_ENABLED else "INFO","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
        "database.services":       {"level": "DEBUG" if DB_DEBUG_ENABLED else "INFO","handlers": ["file_db_debug", "file_warn_error"] + ([] if DISABLE_STDOUT else ["console"]),"propagate": False,},
    },

    "root": {
        "level": "WARNING",
        "handlers": root_handlers_with_err
    },

    "lastResort": {
        "class": "logging.FileHandler",
        "filename": str(LOG_DIR / "errors_warnings.log"),
        "level": "WARNING",
        "formatter": "detailed",
        "encoding": "utf-8",
    },
}

# ───────────────────────── spam rate-limit filter ───────────────
class RateLimitFilter(logging.Filter):
    """
    Drops repeated log records from the same logger for `per_seconds`.
    Useful to tame ib_insync 1100/1102 spam.
    """
    def __init__(self, per_seconds: float = 30.0, name: str = ""):
        super().__init__(name)
        self.per_seconds = per_seconds
        self._last = 0.0

    def filter(self, record: logging.LogRecord) -> bool:
        now = time.time()
        if now - self._last >= self.per_seconds:
            self._last = now
            return True
        return False

# --- new: maintenance-aware pattern rate limiter (logging-only) ---
class IBMaintenancePatternFilter(logging.Filter):
    """Suppresses extremely noisy repeated IB connectivity messages.

    - Always rate-limits repeated messages matching given patterns
    - Uses a much longer interval while inside the nightly IB maintenance window
    - Logging-only; no side effects on application logic
    """
    def __init__(self, patterns: tuple[str, ...], normal_seconds: float = 30.0, maintenance_seconds: float = 300.0):
        super().__init__(name="ib_maintenance_filter")
        self.patterns = patterns
        self.normal_seconds = float(normal_seconds)
        self.maintenance_seconds = float(maintenance_seconds)
        self._last_by_pattern: dict[str, float] = {}

    @staticmethod
    def _in_maintenance_window() -> bool:
        # Mirror logic from ib_connector, kept local to avoid imports
        try:
            from zoneinfo import ZoneInfo  # py3.9+
            tz = ZoneInfo("America/New_York")
        except Exception:  # pragma: no cover
            try:
                from dateutil import tz as _tz
                tz = _tz.gettz("America/New_York")
            except Exception:
                tz = None
        from datetime import datetime, time, timezone
        import os as _os
        buf = int(_os.getenv("IB_MAINTENANCE_BUFFER_MIN", "20"))
        now_et = datetime.now(timezone.utc).astimezone(tz) if tz else datetime.utcnow()
        start = time(23, 45)
        end = time(0, 45)
        def _shift(t: time, delta_min: int) -> time:
            total = (t.hour * 60 + t.minute + delta_min) % (24 * 60)
            return time(total // 60, total % 60)
        start_b = _shift(start, -buf)
        end_b = _shift(end, +buf)
        now_t = now_et.time()
        return (now_t >= start_b) or (now_t <= end_b)

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if not msg:
            return True
        now = time.time()
        per = self.maintenance_seconds if self._in_maintenance_window() else self.normal_seconds
        for pat in self.patterns:
            if pat in msg:
                last = self._last_by_pattern.get(pat, 0.0)
                if now - last >= per:
                    self._last_by_pattern[pat] = now
                    return True
                else:
                    return False
        return True

# ───────────────────────── watchdog (internal) ──────────────────
def _start_resource_watchdog() -> None:
    """
    Log one WARNING whenever *this* process crosses CPU or RSS
    thresholds (env-tunable).  Silent if psutil is missing.
    """
    import threading, time as _t, os as _os, logging as _logging

    log = _logging.getLogger("resource-watchdog")

    try:
        import psutil
    except ImportError:
        log.info("psutil not installed – resource watchdog disabled")
        return

    proc = psutil.Process(_os.getpid())

    def _loop() -> None:
        while True:
            try:
                cpu_pct = proc.cpu_percent(interval=0) / psutil.cpu_count()
                mem_pct = proc.memory_info().rss / psutil.virtual_memory().total * 100
                if cpu_pct >= WATCHDOG_CPU_PCT or mem_pct >= WATCHDOG_MEM_PCT:
                    log.warning(
                        "High resource usage – CPU %.1f%%  RSS %.1f%% "
                        "(thresholds %s/%s)",
                        cpu_pct, mem_pct, WATCHDOG_CPU_PCT, WATCHDOG_MEM_PCT,
                    )
            except Exception:
                log.exception("Resource watchdog failure")
            _t.sleep(WATCHDOG_INTERVAL)

    threading.Thread(target=_loop, name="ResourceWatchdog", daemon=True).start()

def force_propagate_all_loggers():
    for name in logging.root.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.propagate = True

# ───────────────────────── public entrypoint ───────────────────
def setup_logging() -> None:
    """Initialise logging once per process."""
    logging.config.dictConfig(LOGGING_CONFIG)
    coloredlogs.install(
        level=LOG_LEVEL,
        fmt="[%(asctime)s] %(levelname)-7s %(name)s: %(message)s",
    )

    # capture stdlib warnings and silence known SQLAlchemy deprecation noise
    import warnings
    logging.captureWarnings(True)
    try:
        from sqlalchemy.exc import SADeprecationWarning  # type: ignore
        warnings.filterwarnings("ignore", category=SADeprecationWarning)
    except Exception:
        warnings.filterwarnings("ignore", message=".*engine_connect.*", category=DeprecationWarning)

    # Enhanced database debugging - only quiet if debug is disabled
    if not DB_DEBUG_ENABLED:
        logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    else:
        # Enable full SQLAlchemy debugging
        logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)
        logging.getLogger("sqlalchemy.orm").setLevel(logging.DEBUG)

    # Always rate-limit noisy ib_insync connectivity spam; add stronger suppression during maintenance
    ib_wrap = logging.getLogger("ib_insync.wrapper")
    ib_client = logging.getLogger("ib_insync.client")
    patterns = (
        "Error 1100",  # Connectivity between IBKR and TWS has been lost
        "Connectivity between IBKR and TWS has been lost",
        "Peer closed connection",
    )
    for lg in (ib_wrap, ib_client):
        lg.addFilter(RateLimitFilter(IB_SPAM_SUPPRESS_SEC))
        lg.addFilter(IBMaintenancePatternFilter(patterns=patterns, normal_seconds=30.0, maintenance_seconds=300.0))

    if IB_DEBUG_ENABLED:
        logging.getLogger(__name__).info("IB debug enabled – spam filters active to reduce log noise")

    force_propagate_all_loggers()
    # start watchdog
    _start_resource_watchdog()
