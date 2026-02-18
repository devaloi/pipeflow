"""Loader protocol — all loaders implement this interface."""

from __future__ import annotations

from typing import Any, Protocol, Sequence


Record = dict[str, Any]


class Loader(Protocol):
    """Protocol for data loaders."""

    def load(self, records: Sequence[Record]) -> int:
        """Load records to the destination. Returns count loaded."""
        ...

    def close(self) -> None:
        """Clean up resources."""
        ...
