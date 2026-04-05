"""
Logging utilities for observability.
Provides daily rotating log files based on Sydney timezone.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path


class ServerLogHandler(logging.Handler):
    """Custom logging handler that writes to daily log files based on Sydney time."""

    def __init__(self, log_dir: str = None):
        super().__init__()
        self.log_file = None
        self.current_day_start = None
        if log_dir:
            self.log_dir = Path(log_dir)
        else:
            self.log_dir = Path(__file__).parent.parent / "logging" / "logs" / "web_server_logs"
        self._update_log_file()

    def _get_sydney_day_start_timestamp(self) -> int:
        now = datetime.now()
        sydney_tz_offset = 10
        local_offset = now.astimezone().utcoffset().total_seconds() / 3600
        offset_diff = sydney_tz_offset - local_offset
        sydney_now = now + timedelta(hours=offset_diff)
        sydney_midnight = sydney_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(sydney_midnight.timestamp())

    def _get_log_filename(self) -> str:
        day_ts = self._get_sydney_day_start_timestamp()
        return f"{day_ts}.log"

    def _update_log_file(self):
        day_ts = self._get_sydney_day_start_timestamp()
        if self.current_day_start != day_ts:
            if self.log_file:
                self.log_file.close()
            self.current_day_start = day_ts
            self.log_dir.mkdir(parents=True, exist_ok=True)
            log_path = self.log_dir / self._get_log_filename()
            self.log_file = open(log_path, "a", encoding="utf-8")

    def emit(self, record):
        self._update_log_file()
        try:
            msg = self.format(record)
            self.log_file.write(msg + "\n")
            self.log_file.flush()
        except Exception:
            self.handleError(record)

    def close(self):
        if self.log_file:
            self.log_file.close()
        super().close()


def setup_server_logging(log_dir: str = None):
    """Configure logging with daily rotating file and console output."""
    log_handler = ServerLogHandler(log_dir)
    log_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(log_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    root_logger.addHandler(console_handler)