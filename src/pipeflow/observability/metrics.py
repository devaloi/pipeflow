"""Metrics collection for pipeline runs."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class PipelineMetrics:
    """Collects metrics during a pipeline run."""

    records_extracted: int = 0
    records_transformed: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    records_loaded: int = 0
    errors: list[dict[str, object]] = field(default_factory=list)
    _start_time: float = field(default=0.0, repr=False)
    _end_time: float = field(default=0.0, repr=False)

    def start(self) -> None:
        self._start_time = time.time()

    def stop(self) -> None:
        self._end_time = time.time()

    @property
    def duration(self) -> float:
        if self._end_time == 0.0:
            return time.time() - self._start_time
        return self._end_time - self._start_time

    def to_dict(self) -> dict[str, object]:
        return {
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_valid": self.records_valid,
            "records_invalid": self.records_invalid,
            "records_loaded": self.records_loaded,
            "duration_seconds": round(self.duration, 3),
            "error_count": len(self.errors),
        }
