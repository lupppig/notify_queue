"""Centralised logging configuration: stdout + rotating log file with component tags."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_FORMAT = "%(asctime)s [%(component)s] %(name)s %(levelname)s %(message)s"

_FILE_HANDLER_ATTR = "_nq_file_handler_installed"


class _ComponentFilter(logging.Filter):
    """Inject a ``component`` field into every log record."""

    def __init__(self, component: str) -> None:
        super().__init__()
        self.component = component

    def filter(self, record: logging.LogRecord) -> bool:
        record.component = self.component  # type: ignore[attr-defined]
        return True


def setup_logging(
    component: str = "app",
    level: int = logging.INFO,
    log_file: str = "notify_queue.log",
) -> None:
    """Configure the root logger with a stdout handler and a rotating file handler.

    Every log line is tagged with *component* (e.g. ``[scheduler]``,
    ``[worker]``) so entries from different processes are easy to trace
    in the unified log file.  The file rotates at 5 MB with up to 3 backups.

    Pass a custom *log_file* name for isolated log streams (e.g. tests).
    """
    LOG_DIR.mkdir(exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    comp_filter = _ComponentFilter(component)

    # Ensure our file handler is attached exactly once, even if a framework
    # (e.g. uvicorn) has already added its own handlers to the root logger.
    if not getattr(root, _FILE_HANDLER_ATTR, False):
        formatter = logging.Formatter(LOG_FORMAT)

        # Add a console handler only if none exist yet (avoids duplicate
        # stdout lines when uvicorn has already configured one).
        if not root.handlers:
            console = logging.StreamHandler()
            console.addFilter(comp_filter)
            console.setFormatter(formatter)
            root.addHandler(console)
        else:
            for handler in root.handlers:
                handler.addFilter(comp_filter)
                handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(
            LOG_DIR / log_file,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
        )
        file_handler.addFilter(comp_filter)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

        setattr(root, _FILE_HANDLER_ATTR, True)
