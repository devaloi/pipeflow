"""Tests for the CLI module."""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from pipeflow.cli import main


class TestCLIRun:
    def test_run_csv_to_sqlite(self, tmp_path: Path) -> None:
        # Create data file
        data_csv = tmp_path / "data.csv"
        with open(data_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["name", "email", "age"])
            w.writeheader()
            w.writerows([
                {"name": "Alice", "email": "alice@example.com", "age": "30"},
                {"name": "Bob", "email": "bob@example.com", "age": "25"},
            ])

        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: test_run
extract:
  type: csv
  path: {data_csv}
load:
  type: sqlite
  database: {db_path}
  table: users
""")

        result = main(["run", str(config_yaml)])
        assert result == 0
        assert db_path.exists()

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM users").fetchall()
        conn.close()
        assert len(rows) == 2

    def test_run_missing_config(self) -> None:
        result = main(["run", "/nonexistent/config.yaml"])
        assert result == 1


class TestCLIValidate:
    def test_validate_valid_config(self, tmp_path: Path) -> None:
        config = tmp_path / "pipeline.yaml"
        config.write_text("""
name: valid_test
extract:
  type: csv
  path: ./data.csv
load:
  type: sqlite
  database: ./out.db
  table: data
""")
        result = main(["validate", str(config)])
        assert result == 0

    def test_validate_invalid_config(self, tmp_path: Path) -> None:
        config = tmp_path / "bad.yaml"
        config.write_text("not a valid config")
        result = main(["validate", str(config)])
        assert result == 1


class TestCLIInspect:
    def test_inspect_csv(self, sample_csv: Path) -> None:
        result = main(["inspect", str(sample_csv)])
        assert result == 0

    def test_inspect_json(self, sample_json: Path) -> None:
        result = main(["inspect", str(sample_json)])
        assert result == 0

    def test_inspect_missing_file(self) -> None:
        result = main(["inspect", "/nonexistent/file.csv"])
        assert result == 1

    def test_no_command(self) -> None:
        result = main([])
        assert result == 1
