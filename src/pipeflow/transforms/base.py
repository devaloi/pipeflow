"""Transform protocol — all transforms implement this interface."""

from __future__ import annotations

from typing import Any, Protocol


Record = dict[str, Any]


class Transform(Protocol):
    """Protocol for record transforms. Return None to filter out."""

    def apply(self, record: Record) -> Record | None:
        """Apply the transform to a single record."""
        ...
