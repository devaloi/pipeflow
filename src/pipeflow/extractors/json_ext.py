"""JSON / JSONL file extractor."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator


Record = dict[str, Any]


class JSONExtractor:
    """Extract records from a JSON array file or JSONL (one JSON object per line)."""

    def __init__(self, path: str, format: str = "json") -> None:
        self.path = Path(path)
        self.format = format

    def extract(self) -> Iterator[Record]:
        """Yield records from the file."""
        if self.format == "jsonl":
            yield from self._extract_jsonl()
        else:
            yield from self._extract_json()

    def _extract_json(self) -> Iterator[Record]:
        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for item in data:
                yield item
        elif isinstance(data, dict):
            yield data
        else:
            raise ValueError(f"Unexpected JSON root type: {type(data).__name__}")

    def _extract_jsonl(self) -> Iterator[Record]:
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
