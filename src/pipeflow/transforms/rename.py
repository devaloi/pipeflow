"""Rename columns transform."""

from __future__ import annotations

from pipeflow.types import Record


class RenameTransform:
    """Rename record keys based on a mapping."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping  # old_name -> new_name

    def apply(self, record: Record) -> Record:
        return {self.mapping.get(k, k): v for k, v in record.items()}
