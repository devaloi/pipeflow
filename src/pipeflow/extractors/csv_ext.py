"""CSV file extractor — streams records via csv.DictReader."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterator


Record = dict[str, Any]


class CSVExtractor:
    """Extract records from a CSV file using streaming DictReader."""

    def __init__(
        self,
        path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> None:
        self.path = Path(path)
        self.delimiter = delimiter
        self.encoding = encoding

    def extract(self) -> Iterator[Record]:
        """Yield records from the CSV file."""
        with open(self.path, "r", newline="", encoding=self.encoding) as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                yield dict(row)
