"""SQLite loader with auto-create table and upsert support."""

from __future__ import annotations

import sqlite3
from typing import Any, Sequence

from pipeflow.types import Record


class SQLiteLoader:
    """Load records into a SQLite database."""

    def __init__(
        self,
        database: str,
        table: str,
        mode: str = "insert",
        conflict_key: str | None = None,
        batch_size: int = 100,
    ) -> None:
        self.database = database
        self.table = table
        self.mode = mode
        self.conflict_key = conflict_key
        self.batch_size = batch_size
        self._conn: sqlite3.Connection | None = None
        self._table_created = False

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.database)
        return self._conn

    def _ensure_table(self, columns: list[str]) -> None:
        """Auto-create table from column names if it doesn't exist."""
        if self._table_created:
            return
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        if self.conflict_key and self.conflict_key in columns:
            # Add UNIQUE constraint on the conflict key for upsert support
            self.conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{self.table}" '
                f'({col_defs}, UNIQUE("{self.conflict_key}"))'
            )
        else:
            self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{self.table}" ({col_defs})')
        self.conn.commit()
        self._table_created = True

    def load(self, records: Sequence[Record]) -> int:
        """Load a batch of records. Returns count loaded."""
        if not records:
            return 0

        columns = list(records[0].keys())
        self._ensure_table(columns)

        placeholders = ", ".join("?" for _ in columns)
        col_names = ", ".join(f'"{c}"' for c in columns)

        if self.mode == "upsert" and self.conflict_key:
            update_cols = [c for c in columns if c != self.conflict_key]
            update_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
            sql = (
                f'INSERT INTO "{self.table}" ({col_names}) VALUES ({placeholders}) '
                f'ON CONFLICT("{self.conflict_key}") DO UPDATE SET {update_clause}'
            )
        else:
            sql = f'INSERT INTO "{self.table}" ({col_names}) VALUES ({placeholders})'

        count = 0
        for record in records:
            values = [self._serialize(record.get(c)) for c in columns]
            self.conn.execute(sql, values)
            count += 1
        self.conn.commit()
        return count

    @staticmethod
    def _serialize(value: Any) -> Any:
        """Convert value to a SQLite-compatible type."""
        if value is None:
            return None
        if isinstance(value, (int, float, str)):
            return value
        return str(value)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
