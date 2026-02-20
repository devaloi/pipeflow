"""CSV file writer loader."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import IO, Sequence

from pipeflow.types import Record


class CSVWriterLoader:
    """Write records to a CSV file."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._file: IO[str] | None = None
        self._writer: csv.DictWriter[str] | None = None
        self._header_written = False

    def load(self, records: Sequence[Record]) -> int:
        """Append records to the CSV file. Returns count written."""
        if not records:
            return 0

        if self._file is None:
            self._file = open(self.path, "w", newline="", encoding="utf-8")

        if self._writer is None:
            fieldnames = list(records[0].keys())
            self._writer = csv.DictWriter(self._file, fieldnames=fieldnames)

        if not self._header_written:
            self._writer.writeheader()
            self._header_written = True

        count = 0
        for record in records:
            self._writer.writerow(record)
            count += 1

        self._file.flush()
        return count

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
            self._writer = None
