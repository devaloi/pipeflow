"""Extractor protocol — all extractors implement this interface."""

from __future__ import annotations

from typing import Iterator, Protocol

from pipeflow.types import Record


class Extractor(Protocol):
    """Protocol for data extractors that yield records."""

    def extract(self) -> Iterator[Record]:
        """Yield records from the source."""
        ...
