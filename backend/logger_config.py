import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(os.getenv("LOG_DIR", "/root/projects/SelfTrading Analytics/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)


class TruncatingRotatingFileHandler(RotatingFileHandler):
    """Rotate by truncating the same file (sliding window behavior).

    When rollover is triggered, this handler truncates the target file to
    zero length and continues writing to the same filename instead of
    renaming/creating backups.
    """

    def doRollover(self):
        try:
            if self.stream:
                self.stream.close()
                self.stream = None
        except Exception:
            pass

        try:
            enc = getattr(self, "encoding", None)
            with open(self.baseFilename, "w", encoding=enc) as f:
                # opening with 'w' truncates the file
                pass
        except Exception:
            try:
                with open(self.baseFilename, "r+") as f:
                    f.truncate(0)
            except Exception:
                pass

        if not getattr(self, "delay", False):
            self.stream = self._open()


def _mk_handler(filename: str, level: int) -> TruncatingRotatingFileHandler:
    fh = TruncatingRotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))),
        backupCount=int(os.getenv("LOG_BACKUP_COUNT", "0")),
        encoding="utf-8",
    )
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

    loggers = {
        "AnalyticsScheduler": ("sim_scheduler.log", logging.DEBUG),
        "runner-service": ("runner-service.log", logging.DEBUG),
        "mock-broker": ("mock-broker.log", getattr(logging, os.getenv("LOG_MOCK_BROKER_LEVEL", "INFO").upper(), logging.INFO)),
        "grok-4-strategy": ("grok-4-strategy.log", logging.DEBUG),
        "chatgpt-5-strategy": ("chatgpt-5-strategy.log", logging.INFO),
        "api-gateway": ("api_gateway.log", logging.INFO),
        "app": ("app.log", logging.INFO),
        "trades": ("trades.log", logging.INFO),
        "market-data-manager": ("market_data_manager.log", logging.INFO),
        "runner-executions": ("runner_executions.log", logging.INFO),
        "analytics-kpi": ("analytics_kpi.log", logging.INFO),
        # New: dedicated health-gate sink for exclusion/coverage diagnostics
        "runner-health-gate": ("runner_health_gate.log", logging.INFO),
    }

    for name, (file, level) in loggers.items():
        lg = logging.getLogger(name)
        lg.setLevel(level)
        if not any(
            isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith(file)
            for h in lg.handlers
        ):
            lg.addHandler(_mk_handler(file, level))

    root_logger = logging.getLogger()
    root_logger.setLevel(root_level)
    ew_file = "errors_warnings.log"
    if not any(
        isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith(ew_file)
        for h in root_logger.handlers
    ):
        root_logger.addHandler(_mk_handler(ew_file, logging.WARNING))

    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
