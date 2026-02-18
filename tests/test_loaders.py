"""Tests for SQLite and CSV loaders."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from pipeflow.loaders.sqlite import SQLiteLoader
from pipeflow.loaders.csv_writer import CSVWriterLoader


class TestSQLiteLoader:
    def test_basic_insert(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        loader = SQLiteLoader(database=db_path, table="users")
        records = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
        ]
        count = loader.load(records)
        loader.close()

        assert count == 2
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT * FROM users").fetchall()
        conn.close()
        assert len(rows) == 2

    def test_auto_create_table(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        loader = SQLiteLoader(database=db_path, table="auto_table")
        loader.load([{"col1": "val1", "col2": "val2"}])
        loader.close()

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("PRAGMA table_info(auto_table)")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()
        assert "col1" in columns
        assert "col2" in columns

    def test_upsert_mode(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        loader = SQLiteLoader(
            database=db_path,
            table="users",
            mode="upsert",
            conflict_key="email",
        )

        # First need to create table with unique constraint
        conn = sqlite3.connect(db_path)
        conn.execute(
            'CREATE TABLE users ("name" TEXT, "email" TEXT UNIQUE)'
        )
        conn.commit()
        conn.close()

        loader._table_created = True  # Skip auto-create
        loader.load([{"name": "Alice", "email": "alice@example.com"}])
        loader.load([{"name": "Alice Updated", "email": "alice@example.com"}])
        loader.close()

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT name, email FROM users").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "Alice Updated"

    def test_empty_records(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        loader = SQLiteLoader(database=db_path, table="empty")
        count = loader.load([])
        loader.close()
        assert count == 0

    def test_multiple_batches(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        loader = SQLiteLoader(database=db_path, table="data")
        loader.load([{"id": "1", "val": "a"}])
        loader.load([{"id": "2", "val": "b"}])
        loader.close()

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT * FROM data").fetchall()
        conn.close()
        assert len(rows) == 2


class TestCSVWriterLoader:
    def test_basic_write(self, tmp_path: Path) -> None:
        out_path = str(tmp_path / "out.csv")
        loader = CSVWriterLoader(path=out_path)
        records = [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
        ]
        count = loader.load(records)
        loader.close()

        assert count == 2
        with open(out_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"

    def test_round_trip(self, tmp_path: Path) -> None:
        """Write then read back — should be identical."""
        out_path = str(tmp_path / "roundtrip.csv")
        original = [
            {"x": "1", "y": "2"},
            {"x": "3", "y": "4"},
        ]
        loader = CSVWriterLoader(path=out_path)
        loader.load(original)
        loader.close()

        with open(out_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            roundtripped = [dict(row) for row in reader]
        assert roundtripped == original

    def test_empty_records(self, tmp_path: Path) -> None:
        out_path = str(tmp_path / "empty.csv")
        loader = CSVWriterLoader(path=out_path)
        count = loader.load([])
        loader.close()
        assert count == 0

    def test_multiple_batches(self, tmp_path: Path) -> None:
        out_path = str(tmp_path / "multi.csv")
        loader = CSVWriterLoader(path=out_path)
        loader.load([{"a": "1"}])
        loader.load([{"a": "2"}])
        loader.close()

        with open(out_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
