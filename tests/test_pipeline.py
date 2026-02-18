"""End-to-end pipeline tests."""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from pipeflow.config import load_config
from pipeflow.pipeline import Pipeline


class TestPipelineEndToEnd:
    def test_csv_to_sqlite_basic(self, sample_csv: Path, tmp_path: Path) -> None:
        """CSV extract → SQLite load, no transforms."""
        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: basic_pipeline
extract:
  type: csv
  path: {sample_csv}
load:
  type: sqlite
  database: {db_path}
  table: users
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert metrics["records_extracted"] == 5
        assert metrics["records_loaded"] == 5
        assert metrics["error_count"] == 0

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM users").fetchall()
        conn.close()
        assert len(rows) == 5

    def test_csv_with_transforms_and_validation(
        self, sample_csv: Path, tmp_path: Path
    ) -> None:
        """CSV → rename + cast + filter → validate → SQLite."""
        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: transform_pipeline
extract:
  type: csv
  path: {sample_csv}
transforms:
  - type: cast
    columns:
      age: int
  - type: filter
    condition: "age >= 18"
validate:
  model: UserRecord
  fields:
    name:
      type: str
    email:
      type: str
    age:
      type: int
    city:
      type: str
load:
  type: sqlite
  database: {db_path}
  table: users
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        # Diana (age 17) should be filtered out
        assert metrics["records_extracted"] == 5
        assert metrics["records_loaded"] == 4
        assert metrics["records_valid"] == 4

    def test_csv_to_csv_roundtrip(self, sample_csv: Path, tmp_path: Path) -> None:
        """CSV extract → CSV load."""
        out_csv = tmp_path / "out.csv"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: csv_roundtrip
extract:
  type: csv
  path: {sample_csv}
load:
  type: csv
  path: {out_csv}
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert metrics["records_loaded"] == 5
        with open(out_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 5

    def test_json_to_sqlite(self, sample_json: Path, tmp_path: Path) -> None:
        """JSON extract → SQLite load."""
        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: json_pipeline
extract:
  type: json
  path: {sample_json}
load:
  type: sqlite
  database: {db_path}
  table: records
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert metrics["records_extracted"] == 3
        assert metrics["records_loaded"] == 3

    def test_deduplicate_pipeline(self, tmp_path: Path) -> None:
        """Test deduplication in pipeline."""
        data_csv = tmp_path / "dupes.csv"
        with open(data_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["email", "name"])
            w.writeheader()
            w.writerows([
                {"email": "alice@example.com", "name": "Alice"},
                {"email": "alice@example.com", "name": "Alice Dup"},
                {"email": "bob@example.com", "name": "Bob"},
            ])

        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: dedup_pipeline
extract:
  type: csv
  path: {data_csv}
transforms:
  - type: deduplicate
    key: email
load:
  type: sqlite
  database: {db_path}
  table: users
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert metrics["records_extracted"] == 3
        assert metrics["records_loaded"] == 2  # one duplicate removed

    def test_derive_transform_pipeline(self, tmp_path: Path) -> None:
        """Test derive transform in pipeline."""
        data_csv = tmp_path / "names.csv"
        with open(data_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["first", "last"])
            w.writeheader()
            w.writerows([
                {"first": "Alice", "last": "Smith"},
                {"first": "Bob", "last": "Jones"},
            ])

        out_csv = tmp_path / "out.csv"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: derive_pipeline
extract:
  type: csv
  path: {data_csv}
transforms:
  - type: derive
    expression: "full_name = first + ' ' + last"
load:
  type: csv
  path: {out_csv}
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert metrics["records_loaded"] == 2
        with open(out_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert rows[0]["full_name"] == "Alice Smith"

    def test_validation_rejects_bad_records(self, tmp_path: Path) -> None:
        """Records failing validation go to error list, not loaded."""
        data_json = tmp_path / "data.json"
        data_json.write_text(json.dumps([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": "not_a_number"},
            {"name": "Charlie", "age": 25},
        ]))

        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: validation_pipeline
extract:
  type: json
  path: {data_json}
validate:
  model: Record
  fields:
    name:
      type: str
    age:
      type: int
load:
  type: sqlite
  database: {db_path}
  table: records
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert metrics["records_extracted"] == 3
        assert metrics["records_valid"] == 2
        assert metrics["records_invalid"] == 1
        assert metrics["records_loaded"] == 2
        assert metrics["error_count"] == 1

    def test_metrics_has_duration(self, sample_csv: Path, tmp_path: Path) -> None:
        db_path = tmp_path / "out.db"
        config_yaml = tmp_path / "pipeline.yaml"
        config_yaml.write_text(f"""
name: timing_test
extract:
  type: csv
  path: {sample_csv}
load:
  type: sqlite
  database: {db_path}
  table: data
""")
        config = load_config(config_yaml)
        pipeline = Pipeline(config)
        metrics = pipeline.run()

        assert "duration_seconds" in metrics
        assert metrics["duration_seconds"] >= 0  # type: ignore[operator]
