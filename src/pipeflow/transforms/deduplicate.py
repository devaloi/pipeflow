"""Deduplicate records by key columns."""

from __future__ import annotations

from typing import Any

from pipeflow.types import Record


class DeduplicateTransform:
    """Remove duplicate records based on one or more key columns.

    Keeps the first occurrence of each unique key combination.
    """

    def __init__(self, key: list[str]) -> None:
        self.key = key
        # Seen keys grow unboundedly — intentional for correctness.
        # For very large datasets, consider adding an LRU eviction strategy.
        self._seen: set[tuple[Any, ...]] = set()

    def apply(self, record: Record) -> Record | None:
        key_values = tuple(record.get(k) for k in self.key)
        if key_values in self._seen:
            return None
        self._seen.add(key_values)
        return record

    def reset(self) -> None:
        """Reset seen keys for reuse."""
        self._seen.clear()
