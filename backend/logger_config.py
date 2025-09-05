import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "/root/projects/SelfTrading Analytics/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _mk_handler(filename: str, level: int) -> RotatingFileHandler:
    fh = RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))),
        backupCount=int(os.getenv("LOG_BACKUP_COUNT", "5")),
        encoding="utf-8",
    )
    # NOTE: Use %(asctime)s and %(msecs)03d — strftime-based %f is not supported here.
    fmt = logging.Formatter(
        fmt="[%(asctime)s,%(msecs)03d] %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(fmt)
    fh.setLevel(level)
    return fh


def setup_logging() -> None:
    root_level = getattr(logging, os.getenv("LOG_LEVEL", "DEBUG").upper(), logging.DEBUG)
    logging.basicConfig(level=root_level)

    # Core logs
    loggers = {
        "AnalyticsScheduler": ("sim_scheduler.log", logging.DEBUG),
        "runner-service": ("runner-service.log", logging.DEBUG),
        "mock-broker": (
            "mock-broker.log",
            getattr(logging, os.getenv("LOG_MOCK_BROKER_LEVEL", "INFO").upper(), logging.INFO),
        ),
        "grok-4-strategy": ("grok-4-strategy.log", logging.DEBUG),
        "chatgpt-5-strategy": ("chatgpt-5-strategy.log", logging.INFO),
        "api-gateway": ("api_gateway.log", logging.INFO),
        "errors_warnings": ("errors_warnings.log", logging.WARNING),
        "app": ("app.log", logging.INFO),
        "trades": ("trades.log", logging.INFO),

        # Data / session clock visibility
        "market-data-manager": ("market_data_manager.log", logging.INFO),

        # Runner execution mirroring
        "runner-executions": ("runner_executions.log", logging.INFO),

        # Universe + importer (added sinks)
        "universe": ("universe.log", logging.INFO),
        "analytics-importer": ("analytics_importer.log", logging.INFO),
        # Back-compat if any code still uses underscore variant
        "analytics_importer": ("analytics_importer.log", logging.INFO),
    }

    for name, (file, level) in loggers.items():
        lg = logging.getLogger(name)
        lg.setLevel(level)
        if not any(
            isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith(file)
            for h in lg.handlers
        ):
            lg.addHandler(_mk_handler(file, level))

    # Ensure all WARNING+ logs from any logger also go to errors_warnings.log
    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)
    ew_file = "errors_warnings.log"
    if not any(
        isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith(ew_file)
        for h in root_logger.handlers
    ):
        root_logger.addHandler(_mk_handler(ew_file, logging.WARNING))

    # Quiet noisy libs unless explicitly raised
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
