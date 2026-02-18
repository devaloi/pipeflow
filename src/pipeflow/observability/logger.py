"""Structured JSON logging for pipeline observability."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any


class JSONFormatter(logging.Formatter):
    """Format log records as JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "pipeline"):
            log_data["pipeline"] = record.pipeline  # type: ignore[attr-defined]
        if hasattr(record, "stage"):
            log_data["stage"] = record.stage  # type: ignore[attr-defined]
        if record.exc_info and record.exc_info[1]:
            log_data["error"] = str(record.exc_info[1])
        return json.dumps(log_data)


def get_logger(name: str = "pipeflow", json_format: bool = True) -> logging.Logger:
    """Get a configured logger instance."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        if json_format:
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
