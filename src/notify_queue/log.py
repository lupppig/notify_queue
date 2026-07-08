"""Centralised logging configuration: stdout + rotating log file with component tags."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_FORMAT = "%(asctime)s [%(component)s] %(name)s %(levelname)s %(message)s"


class _ComponentFilter(logging.Filter):
    """Inject a ``component`` field into every log record."""

    def __init__(self, component: str) -> None:
        super().__init__()
        self.component = component

    def filter(self, record: logging.LogRecord) -> bool:
        record.component = self.component  # type: ignore[attr-defined]
        return True


def setup_logging(component: str = "app", level: int = logging.INFO) -> None:
    """Configure the root logger with a stdout handler and a rotating file handler.

    Every log line is tagged with *component* (e.g. ``[scheduler]``,
    ``[worker]``) so entries from different processes are easy to trace
    in the single unified ``logs/notify_queue.log`` file.  The file rotates
    at 5 MB with up to 3 backups.
    """
    LOG_DIR.mkdir(exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    if root.handlers:
        return

    comp_filter = _ComponentFilter(component)
    formatter = logging.Formatter(LOG_FORMAT)

    console = logging.StreamHandler()
    console.addFilter(comp_filter)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = RotatingFileHandler(
        LOG_DIR / "notify_queue.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.addFilter(comp_filter)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
