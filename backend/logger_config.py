"""
Analytics logger configuration
──────────────────────────────────────────────────────────────────────────────
• Rotating file handlers mirroring the main app
• Root logger also writes WARN/ERROR to errors_warnings.log
• Stdlib warnings routed into logging
• Safe if coloredlogs is missing
"""

from __future__ import annotations
import logging, logging.config, os, time
from pathlib import Path

# Optional pretty console logs
try:
    import coloredlogs  # type: ignore
except Exception:  # pragma: no cover
    coloredlogs = None  # type: ignore

# ───────────────────────── directories ──────────────────────────
DEFAULT_LOG_DIR = Path(os.getenv("LOG_DIR", "/app/logs"))
DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)

# ───────────────────────── env toggles ──────────────────────────
DISABLE_STDOUT  = os.getenv("NO_STDOUT_LOG", "0") == "1"
LOG_LEVEL       = os.getenv("LOG_LEVEL",      "INFO").upper()

# Rotation settings
LOG_MAX_BYTES   = int(os.getenv("LOG_MAX_BYTES", str(50 * 1024 * 1024)))
LOG_BACKUP_COUNT= int(os.getenv("LOG_BACKUP_COUNT", "10"))

def _handler_file(path: Path, level: str | None = None) -> dict:
    h = {
        "class": "logging.handlers.RotatingFileHandler",
        "filename": str(path),
        "maxBytes": LOG_MAX_BYTES,
        "backupCount": LOG_BACKUP_COUNT,
        "formatter": "detailed",
        "encoding": "utf-8",
    }
    if level:
        h["level"] = level
    return h

LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,

    "formatters": {
        "detailed": {"format": "[%(asctime)s] %(levelname)-7s %(name)s: %(message)s"},
    },

    "handlers": {
        # generic
        "console": {"class": "logging.StreamHandler", "formatter": "detailed"},
        "file_root": _handler_file(DEFAULT_LOG_DIR / "app.log"),
        "file_warn_error": _handler_file(DEFAULT_LOG_DIR / "errors_warnings.log", level="WARNING"),

        # components
        "file_scheduler": _handler_file(DEFAULT_LOG_DIR / "sim_scheduler.log"),
        "file_runner_service": _handler_file(DEFAULT_LOG_DIR / "runner-service.log"),
        "file_basic_strategy": _handler_file(DEFAULT_LOG_DIR / "basic-strategy.log"),
        "file_below_above_strategy": _handler_file(DEFAULT_LOG_DIR / "below-above-strategy.log"),
        "file_chatgpt_5_strategy": _handler_file(DEFAULT_LOG_DIR / "chatgpt-5-strategy.log"),
        "file_grok_4_strategy": _handler_file(DEFAULT_LOG_DIR / "grok-4-strategy.log"),
        "file_sync_service": _handler_file(DEFAULT_LOG_DIR / "sync-service.log"),
        "file_api_gateway": _handler_file(DEFAULT_LOG_DIR / "api_gateway.log"),
        "file_db_debug": _handler_file(DEFAULT_LOG_DIR / "db-debug.log"),
    },

    "loggers": {
        "": {"level": LOG_LEVEL, "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_root", "file_warn_error"]},
        "AnalyticsScheduler": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_scheduler", "file_warn_error"], "propagate": False},
        "runner-service": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_runner_service", "file_warn_error"], "propagate": False},
        "basic-strategy": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_basic_strategy", "file_warn_error"], "propagate": False},
        "below-above-strategy": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_below_above_strategy", "file_warn_error"], "propagate": False},
        "chatgpt-5-strategy": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_chatgpt_5_strategy", "file_warn_error"], "propagate": False},
        "grok-4-strategy": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_grok_4_strategy", "file_warn_error"], "propagate": False},
        "sync-service": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_sync_service", "file_warn_error"], "propagate": False},
        "api-gateway": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_api_gateway", "file_warn_error"], "propagate": False},
        "market-data-manager": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_api_gateway", "file_warn_error"], "propagate": False},
        "runner-decision-builder": {"level": "INFO", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_runner_service", "file_warn_error"], "propagate": False},
        "sqlalchemy": {"level": os.getenv("DB_DEBUG_ENABLED", "false").lower() == "true" and "DEBUG" or "WARNING", "handlers": ["file_db_debug", "file_warn_error"] + (["console"] if not DISABLE_STDOUT else []), "propagate": False},
    },

    "root": {"level": "WARNING", "handlers": (["console"] if not DISABLE_STDOUT else []) + ["file_warn_error", "file_root"]},
}


def setup_logging() -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    # capture stdlib warnings
    import warnings
    logging.captureWarnings(True)
    # colored console if available
    if coloredlogs:
        coloredlogs.install(level=LOG_LEVEL, fmt="[%(asctime)s] %(levelname)-7s %(name)s: %(message)s")


