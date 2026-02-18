"""Shared test fixtures."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a sample CSV file with 5 rows."""
    data = [
        {"name": "Alice", "email": "alice@example.com", "age": "30", "city": "NYC"},
        {"name": "Bob", "email": "bob@example.com", "age": "25", "city": "LA"},
        {"name": "Charlie", "email": "charlie@example.com", "age": "35", "city": "Chicago"},
        {"name": "Diana", "email": "diana@example.com", "age": "17", "city": "Boston"},
        {"name": "Eve", "email": "eve@example.com", "age": "28", "city": "Seattle"},
    ]
    path = tmp_path / "data.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "email", "age", "city"])
        writer.writeheader()
        writer.writerows(data)
    return path


@pytest.fixture
def sample_json(tmp_path: Path) -> Path:
    """Create a sample JSON file with an array of records."""
    data = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
    path = tmp_path / "data.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    """Create a sample JSONL file."""
    records = [
        {"name": "Alice", "score": 95},
        {"name": "Bob", "score": 87},
    ]
    path = tmp_path / "data.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return path


@pytest.fixture
def sample_records() -> list[dict[str, Any]]:
    """Return sample records for testing transforms/loaders."""
    return [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]
