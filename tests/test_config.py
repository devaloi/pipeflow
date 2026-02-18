"""Tests for YAML config loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeflow.config import PipelineConfig, load_config


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "pipeline.yaml"
    p.write_text(content, encoding="utf-8")
    return p


class TestLoadConfig:
    def test_minimal_config(self, tmp_path: Path) -> None:
        yaml_text = """
name: test_pipeline
extract:
  type: csv
  path: ./data.csv
load:
  type: sqlite
  database: ./out.db
  table: test
"""
        path = _write_yaml(tmp_path, yaml_text)
        config = load_config(path)
        assert config.name == "test_pipeline"
        assert config.extract.type == "csv"
        assert config.extract.path == "./data.csv"
        assert config.load.type == "sqlite"
        assert config.transforms == []
        assert config.validate is None

    def test_full_config(self, tmp_path: Path) -> None:
        yaml_text = """
name: full_pipeline
extract:
  type: csv
  path: ./data.csv
  delimiter: ";"
transforms:
  - type: rename
    mapping: {"Full Name": "name"}
  - type: cast
    columns: {age: int}
  - type: filter
    condition: "age >= 18"
  - type: deduplicate
    key: email
validate:
  model: UserRecord
  fields:
    name: {type: str}
    email: {type: str}
load:
  type: sqlite
  database: ./out.db
  table: users
  mode: upsert
  conflict_key: email
  batch_size: 50
"""
        path = _write_yaml(tmp_path, yaml_text)
        config = load_config(path)
        assert config.name == "full_pipeline"
        assert config.extract.delimiter == ";"
        assert len(config.transforms) == 4
        assert config.transforms[0].type == "rename"
        assert config.validate is not None
        assert config.validate.model == "UserRecord"
        assert config.load.mode == "upsert"
        assert config.load.batch_size == 50

    def test_missing_file(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/pipeline.yaml")

    def test_invalid_yaml(self, tmp_path: Path) -> None:
        path = _write_yaml(tmp_path, "just a string")
        with pytest.raises(ValueError, match="expected a YAML mapping"):
            load_config(path)

    def test_missing_required_fields(self, tmp_path: Path) -> None:
        yaml_text = """
name: bad_pipeline
"""
        path = _write_yaml(tmp_path, yaml_text)
        with pytest.raises(Exception):
            load_config(path)

    def test_default_values(self, tmp_path: Path) -> None:
        yaml_text = """
name: defaults
extract:
  type: csv
  path: ./data.csv
load:
  type: csv
  path: ./out.csv
"""
        path = _write_yaml(tmp_path, yaml_text)
        config = load_config(path)
        assert config.extract.delimiter == ","
        assert config.extract.encoding == "utf-8"
        assert config.load.mode == "insert"
        assert config.load.batch_size == 100
