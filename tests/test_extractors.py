"""Tests for CSV and JSON extractors."""

from __future__ import annotations

from pathlib import Path

from pipeflow.extractors.csv_ext import CSVExtractor
from pipeflow.extractors.json_ext import JSONExtractor


class TestCSVExtractor:
    def test_basic_csv(self, sample_csv: Path) -> None:
        ext = CSVExtractor(path=str(sample_csv))
        records = list(ext.extract())
        assert len(records) == 5
        assert records[0]["name"] == "Alice"
        assert records[0]["email"] == "alice@example.com"

    def test_all_fields_present(self, sample_csv: Path) -> None:
        ext = CSVExtractor(path=str(sample_csv))
        records = list(ext.extract())
        for rec in records:
            assert set(rec.keys()) == {"name", "email", "age", "city"}

    def test_custom_delimiter(self, tmp_path: Path) -> None:
        tsv = tmp_path / "data.tsv"
        tsv.write_text("a\tb\n1\t2\n3\t4\n", encoding="utf-8")
        ext = CSVExtractor(path=str(tsv), delimiter="\t")
        records = list(ext.extract())
        assert len(records) == 2
        assert records[0] == {"a": "1", "b": "2"}

    def test_streaming_generator(self, sample_csv: Path) -> None:
        ext = CSVExtractor(path=str(sample_csv))
        gen = ext.extract()
        first = next(gen)
        assert first["name"] == "Alice"


class TestJSONExtractor:
    def test_json_array(self, sample_json: Path) -> None:
        ext = JSONExtractor(path=str(sample_json), format="json")
        records = list(ext.extract())
        assert len(records) == 3
        assert records[0]["name"] == "Alice"

    def test_jsonl(self, sample_jsonl: Path) -> None:
        ext = JSONExtractor(path=str(sample_jsonl), format="jsonl")
        records = list(ext.extract())
        assert len(records) == 2
        assert records[0]["name"] == "Alice"
        assert records[1]["score"] == 87

    def test_single_json_object(self, tmp_path: Path) -> None:
        import json
        p = tmp_path / "single.json"
        p.write_text(json.dumps({"key": "value"}))
        ext = JSONExtractor(path=str(p), format="json")
        records = list(ext.extract())
        assert len(records) == 1
        assert records[0]["key"] == "value"

    def test_jsonl_empty_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "sparse.jsonl"
        p.write_text('{"a": 1}\n\n{"b": 2}\n\n')
        ext = JSONExtractor(path=str(p), format="jsonl")
        records = list(ext.extract())
        assert len(records) == 2
