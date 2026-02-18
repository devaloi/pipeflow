"""Extractor protocol — all extractors implement this interface."""

from __future__ import annotations

from typing import Any, Iterator, Protocol


Record = dict[str, Any]


class Extractor(Protocol):
    """Protocol for data extractors that yield records."""

    def extract(self) -> Iterator[Record]:
        """Yield records from the source."""
        ...
